/**
 * `defineJob` — one durable long-running operation per actor, as
 * convention over `defineActor` + the `tasks:` primitive. The layer
 * encodes the four invariants everyone would otherwise get wrong:
 *
 *  1. `start` is idempotent under retry (a non-pending job returns its
 *     current info and never restarts).
 *  2. Attempt counting distinguishes crash-resume (counts against
 *     `maxAttempts`) from pause-resume (free).
 *  3. Detached code never touches live state — every JobHandle member is
 *     a serialized turn inside.
 *  4. Progress is change-feed-visible without write amplification
 *     (mutated, pushed, not persisted).
 *
 * Blessed pattern: one actor per run — key = your run id. The directory's
 * single-activation guarantee IS the "exactly one runner" guarantee.
 */
import { defineActor } from '../define';
import { decodePrincipal, encodePrincipalValue } from '../guards';
import type { ActorContext, ActorDefinition, ActorTaskContext } from '../types';
import { JobCancelledError, JobFailedError, JobNotDoneError, JobStateError } from './errors';
import {
    JOB_PAUSED,
    type JobControl,
    type JobHandle,
    type JobInfo,
    type JobMethodTable,
    type JobOptions,
    type JobProgress,
    type JobStatus,
    type JobStreamTable
} from './types';

/** The single task name every job runs under. */
const RUN = 'run';
/** The retention reminder (reserved — never reaches user onReminder). */
const RETAIN = '$sigx:job:retain';
const DEFAULT_MAX_ATTEMPTS = 3;

const TERMINAL: readonly JobStatus[] = ['completed', 'failed', 'cancelled'];

interface JobState<Extra extends object> {
    status: JobStatus;
    input: unknown;
    checkpoint: unknown;
    result: unknown;
    error: { message: string } | null;
    progress: JobProgress | null;
    attempts: number;
    startedAt: number | null;
    finishedAt: number | null;
    resumeData: unknown;
    /**
     * The ENCODED principal recorded at enqueue (rfc-server-v4 §7).
     *
     * A job outlives the request that started it — there is no live caller
     * for a crash-resume three hours later to authorize — so the run body
     * reads this snapshot as `job.principal` instead of re-deciding.
     * Persisted, so it survives deactivation and resume; `null` when the
     * starter was anonymous or the app configured no codec.
     */
    principal: string | null;
    extra: Extra;
}

function toInfo<Extra extends object>(state: JobState<Extra>, key: string): JobInfo<Extra> {
    return {
        key,
        status: state.status,
        progress: state.progress,
        attempts: state.attempts,
        startedAt: state.startedAt,
        finishedAt: state.finishedAt,
        error: state.error,
        extra: state.extra
    };
}

function message(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
}

export function defineJob<In, Out, C = unknown, Extra extends object = Record<never, never>>(
    options: JobOptions<In, Out, C, Extra>
): ActorDefinition<JobState<Extra>, JobMethodTable<In, Out, Extra>, JobStreamTable<Extra>> {
    const maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    type S = JobState<Extra>;
    type Ctx = ActorContext<S>;

    /**
     * Terminal bookkeeping + optional retention + `onSettled`, inside one
     * turn. THE single terminal transition — `doCancel`, the `maxAttempts`
     * give-up, completion and body failure all land here — which is what
     * lets `onSettled` promise "every terminal transition" without each
     * caller remembering to fire it.
     */
    async function finish(
        c: Ctx,
        status: Extract<JobStatus, 'completed' | 'failed' | 'cancelled'>,
        patch: Partial<Pick<S, 'result' | 'error'>>
    ): Promise<void> {
        c.state.status = status;
        c.state.finishedAt = Date.now();
        c.state.resumeData = null;
        Object.assign(c.state, patch);
        await c.save();
        if (options.retainMs !== undefined) {
            await c.reminders.set(RETAIN, { due: options.retainMs });
        }
        if (options.onSettled) {
            // After the save, and never fatal: the transition is already
            // durable, so a throwing handler must not be able to unwind it —
            // that would leave a job the runtime believes is terminal and the
            // caller believes is not.
            try {
                await options.onSettled(control(c), toInfo(c.snapshot() as S, c.key));
            } catch (error) {
                if (typeof process !== 'undefined' && process.env.NODE_ENV !== 'production') {
                    console.error(
                        `[sigx actors] onSettled threw for job "${options.type}/${c.key}" ` +
                            `(status "${status}") — the terminal transition stands:`,
                        error
                    );
                }
            }
        }
    }

    /** Shared by the `cancel` method and `JobControl.cancel`. */
    async function doCancel(ctx: Ctx): Promise<void> {
        const s = ctx.state;
        if (s.status === 'pending' || TERMINAL.includes(s.status)) return;
        // Marked FIRST, so status flips the moment the caller returns; the
        // run's terminal turns all guard on `status === 'running'` and so
        // cannot overwrite this.
        await finish(ctx, 'cancelled', {});
        await ctx.tasks.cancel(RUN); // request; no-op when paused
    }

    /** Shared by the `resume` method and `JobControl.resume`. */
    async function doResume(ctx: Ctx, data: unknown, strict: boolean): Promise<void> {
        const s = ctx.state;
        if (s.status !== 'paused') {
            if (strict) {
                throw new JobStateError(
                    `job "${options.type}/${ctx.key}" is "${s.status}" — only a paused job resumes.`
                );
            }
            return;
        }
        s.status = 'running';
        s.resumeData = data ?? null;
        await ctx.save();
        await ctx.tasks.start(RUN, s.input);
    }

    function control(ctx: Ctx): JobControl<Extra> {
        return {
            info: () => toInfo(ctx.snapshot() as S, ctx.key),
            resume: (data?: unknown) => doResume(ctx, data, false),
            cancel: () => doCancel(ctx),
            reminders: ctx.reminders
        };
    }

    return defineActor<S, JobMethodTable<In, Out, Extra>, JobStreamTable<Extra>>({
        type: options.type,
        kind: 'job',
        ...(options.authorize ? { authorize: options.authorize } : {}),
        ...(options.allowAnonymous !== undefined ? { allowAnonymous: options.allowAnonymous } : {}),
        ...(options.idleAfterMs !== undefined ? { idleAfterMs: options.idleAfterMs } : {}),
        ...(options.placement ? { placement: options.placement } : {}),
        persistence: 'explicit',
        state: (key): S => ({
            status: 'pending',
            input: null,
            checkpoint: null,
            result: null,
            error: null,
            progress: null,
            attempts: 0,
            startedAt: null,
            finishedAt: null,
            resumeData: null,
            principal: null,
            extra: options.state ? options.state(key) : ({} as Extra)
        }),
        methods: (ctx) => ({
            async start(input: In): Promise<JobInfo<Extra>> {
                const s = ctx.state;
                if (s.status !== 'pending') return toInfo(ctx.snapshot() as S, ctx.key);
                s.status = 'running';
                s.input = input;
                s.attempts = 1;
                s.startedAt = Date.now();
                // Snapshot the ENQUEUEING caller. `start` is the entry point
                // that authorized, so this is the identity the whole run is
                // attributable to — including resumes that happen on another
                // host, days later, with nobody waiting.
                s.principal = encodePrincipalValue(ctx.principal) ?? null;
                await ctx.save();
                await ctx.tasks.start(RUN, input);
                return toInfo(ctx.snapshot() as S, ctx.key);
            },
            status: (): JobInfo<Extra> => toInfo(ctx.snapshot() as S, ctx.key),
            async cancel(): Promise<JobInfo<Extra>> {
                await doCancel(ctx);
                return toInfo(ctx.snapshot() as S, ctx.key);
            },
            async resume(data?: unknown): Promise<JobInfo<Extra>> {
                await doResume(ctx, data, true);
                return toInfo(ctx.snapshot() as S, ctx.key);
            },
            async result(): Promise<Out> {
                const s = ctx.state;
                if (s.status === 'completed') return s.result as Out;
                if (s.status === 'failed') throw new JobFailedError(s.error?.message);
                if (s.status === 'cancelled') throw new JobCancelledError();
                throw new JobNotDoneError(s.status);
            },
            async discard(): Promise<void> {
                const s = ctx.state;
                if (!TERMINAL.includes(s.status)) {
                    throw new JobStateError(
                        `job "${options.type}/${ctx.key}" is "${s.status}" — only a terminal ` +
                            `job can be discarded (cancel it first).`
                    );
                }
                if (options.retainMs !== undefined) await ctx.reminders.clear(RETAIN);
                await ctx.clearState();
                ctx.deactivate();
            }
        }),
        streams: (ctx) => ({
            async *watch(): AsyncIterable<JobInfo<Extra>> {
                for await (const s of ctx.changes({ initial: true })) {
                    yield toInfo(s, ctx.key);
                }
            }
        }),
        tasks: (tctx: ActorTaskContext<S>) => ({
            async [RUN](input: unknown) {
                // Crash-resumes arrive with the core restart counter > 0;
                // pause-resumes are a FRESH task start (restarts 0).
                const restarts = tctx.tasks.list().find((t) => t.name === RUN)?.restarts ?? 0;
                const attempt = restarts + 1;
                let refused = false;
                let resumedFrom: C | undefined;
                let resumeData: unknown;
                let principal: string | null = null;
                await tctx.turn(async (c) => {
                    const s = c.state;
                    if (s.status !== 'running') {
                        // Cancelled/paused/terminal while the task was being
                        // (re)started — nothing to run.
                        refused = true;
                        return;
                    }
                    if (attempt > s.attempts) s.attempts = attempt;
                    if (s.attempts > maxAttempts) {
                        refused = true;
                        await finish(c, 'failed', {
                            error: {
                                message: `gave up after ${maxAttempts} attempts (crash-resume loop)`
                            }
                        });
                        return;
                    }
                    resumedFrom = (s.checkpoint ?? undefined) as C | undefined;
                    resumeData = s.resumeData ?? undefined;
                    principal = s.principal;
                    s.resumeData = null;
                    if (attempt > 1) await c.save();
                });
                if (refused) return;

                const job: JobHandle<In, C, Extra> = {
                    key: tctx.key,
                    input: input as In,
                    signal: tctx.abortSignal,
                    attempt,
                    resumedFrom,
                    resumeData,
                    principal: decodePrincipal(principal ?? undefined),
                    progress: (p) =>
                        tctx.turn((c) => {
                            if (c.state.status === 'running') c.state.progress = p;
                        }),
                    checkpoint: (cp) =>
                        tctx.turn(async (c) => {
                            if (c.state.status !== 'running') return;
                            c.state.checkpoint = cp;
                            await c.save();
                        }),
                    update: (fn) =>
                        tctx.turn((c) => {
                            // Same late-write guard as progress: a winding-
                            // down body must not mutate a cancelled/finished
                            // job's public shape.
                            if (c.state.status === 'running') fn(c.state.extra);
                        }),
                    pause: async (cp) => {
                        await tctx.turn(async (c) => {
                            if (c.state.status !== 'running') return;
                            c.state.checkpoint = cp;
                            c.state.status = 'paused';
                            await c.save();
                        });
                        return JOB_PAUSED;
                    },
                    reminders: tctx.reminders
                };

                try {
                    const out = await options.run(job, input as In);
                    if (out === JOB_PAUSED) return;
                    if (tctx.abortSignal.aborted) {
                        // The body returned because it was winding down, not
                        // because the work is done. 'cancelled' was already
                        // recorded by doCancel; a deactivation abort leaves
                        // 'running' so the task ledger resumes the run —
                        // recording a wind-down return as 'completed' here
                        // would erase the resume.
                        return;
                    }
                    await tctx.turn(async (c) => {
                        if (c.state.status !== 'running') return;
                        c.state.progress = null;
                        await finish(c, 'completed', { result: out });
                    });
                } catch (error) {
                    if (tctx.abortSignal.aborted) {
                        // 'cancelled': doCancel already recorded the status.
                        // Deactivation reasons: leave 'running' — the task
                        // ledger resumes the run on the next activation.
                        return;
                    }
                    await tctx.turn(async (c) => {
                        if (c.state.status !== 'running') return;
                        await finish(c, 'failed', { error: { message: message(error) } });
                    });
                }
            }
        }),
        onReminder: async (ctx, name) => {
            if (name === RETAIN) {
                const s = ctx.state;
                if (TERMINAL.includes(s.status)) {
                    await ctx.clearState();
                    ctx.deactivate();
                }
                return;
            }
            if (options.onReminder) await options.onReminder(control(ctx), name);
        }
    });
}
