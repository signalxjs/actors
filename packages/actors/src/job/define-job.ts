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
import type { ActorContext, ActorDefinition, ActorTaskContext, TaskResumeEntry } from '../types';
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

/**
 * Build `JobInfo` from an already-DETACHED state — the change feed's
 * snapshots. The method paths use `liveInfo` instead (#229): re-cloning a
 * feed snapshot would be waste, and cloning live state to read 8 fields
 * was the O(state-size) read the `jobs/status-read` ladder measured at
 * 681× across 0→2000 checkpoint rows.
 */
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
     * `JobInfo` off the LIVE state, without a whole-state clone (#229).
     *
     * Every method-path read used `toInfo(ctx.snapshot())` — a full
     * encode+revive of the entire state, checkpoint and result included,
     * to build 8 fields that exclude both. Safe to read live here: a
     * method body IS a serialized turn. The detachment contract is
     * unchanged — nothing returned aliases live state:
     *
     *  - the scalars cannot alias;
     *  - `progress`/`error` are fresh shallow objects (all-primitive by
     *    their declared types);
     *  - `extra` — the one arbitrary user object — goes through the host
     *    codec, but ONLY when the definition declares `state:`. The
     *    common checkpointing job has none, and pays nothing.
     */
    function liveInfo(ctx: Ctx): JobInfo<Extra> {
        const s = ctx.state;
        return {
            key: ctx.key,
            status: s.status,
            progress: s.progress ? { ...s.progress } : null,
            attempts: s.attempts,
            startedAt: s.startedAt,
            finishedAt: s.finishedAt,
            error: s.error ? { message: s.error.message } : null,
            extra: options.state ? ctx.snapshot(s.extra) : ({} as Extra)
        };
    }

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
                await options.onSettled(control(c), liveInfo(c));
            } catch (error) {
                if (__DEV__) {
                    console.error(
                        `[sigx actors] onSettled threw for job "${options.type}/${c.key}" ` +
                            `(status "${status}") — the terminal transition stands:`,
                        error
                    );
                }
            }
        }
    }

    /**
     * Launch the run AFTER `running` is durable — and take the transition
     * back if the launch rejects (#316). The state record is the ledger
     * (#309), so a `running` that outlives a failed `tasks.start` (task
     * liveness could not reach storage, say) is a run the caller was told
     * never started but the next activation resumes anyway — at-least-once
     * from the side nobody asked for. `#startTask`'s own rollback covers
     * stored ledgers only; for a derived ledger the definition that saved
     * the state is the one that must unsave it, which is this. `prior`
     * holds exactly the fields the caller wrote on the way to `running`,
     * at their pre-transition values.
     *
     * The revert is best-effort: if its save fails too, live state is put
     * back in step with the durable `running` (rather than a `pending`
     * activation over a `running` record), which is the previous contract —
     * the run resumes on the next activation, at-least-once. Either way
     * the caller sees the original rejection.
     */
    async function launch(ctx: Ctx, prior: Partial<S>): Promise<void> {
        const s = ctx.state;
        try {
            await ctx.tasks.start(RUN, s.input);
        } catch (error) {
            // Snapshot the same keys `prior` names, so the two writers
            // cannot drift: whatever a caller adds to `prior` is what the
            // fallback below restores.
            const running: Record<string, unknown> = {};
            for (const k of Object.keys(prior) as (keyof S)[]) running[k] = s[k];
            Object.assign(s, prior);
            try {
                await ctx.save();
            } catch {
                Object.assign(s, running);
            }
            throw error;
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
        const prior: Partial<S> = { status: 'paused', resumeData: s.resumeData };
        s.status = 'running';
        s.resumeData = data ?? null;
        await ctx.save();
        await launch(ctx, prior);
    }

    function control(ctx: Ctx): JobControl<Extra> {
        return {
            info: () => liveInfo(ctx),
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
        ...(options.internal !== undefined ? { internal: options.internal } : {}),
        ...(options.idleAfterMs !== undefined ? { idleAfterMs: options.idleAfterMs } : {}),
        ...(options.placement ? { placement: options.placement } : {}),
        persistence: 'explicit',
        // The state record IS the ledger (#309): a job is `running` exactly
        // while its one task should be in flight, `attempts` is what the
        // ledger's `restarts` counted (attempt = restarts + 1, so the run
        // that resumes this state has been restarted `attempts` times once
        // it starts), and `input` is right here. Paused, pending and
        // terminal jobs have nothing to resume — which also closes the gap
        // a separate record left open: a crash between the `start()` save
        // and the ledger write used to leave a job `running` forever.
        //
        // The bumped count is made durable by the run's FIRST turn below
        // (`if (attempt > 1) await c.save()`), before `options.run` is
        // called — where the ledger did it with its own CAS before launch.
        // A host death between activation and that turn re-derives the
        // same count once; no user code runs in that window, and the
        // contract is at-least-once either way. `maxAttempts` still ends a
        // crash loop, because every resume that gets as far as running
        // user code has already saved its attempt.
        resumeTasks: (s): Record<string, TaskResumeEntry> =>
            s.status === 'running'
                ? {
                      // `start()` sets both before it saves `running`.
                      [RUN]: { input: s.input, startedAt: s.startedAt!, restarts: s.attempts - 1 }
                  }
                : {},
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
                if (s.status !== 'pending') return liveInfo(ctx);
                // A pending job is the fresh state — nothing writes these five
                // fields before `start` does — so the revert's target is known.
                const prior: Partial<S> = {
                    status: 'pending',
                    input: null,
                    attempts: 0,
                    startedAt: null,
                    principal: null
                };
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
                await launch(ctx, prior);
                return liveInfo(ctx);
            },
            status: (): JobInfo<Extra> => liveInfo(ctx),
            async cancel(): Promise<JobInfo<Extra>> {
                await doCancel(ctx);
                return liveInfo(ctx);
            },
            async resume(data?: unknown): Promise<JobInfo<Extra>> {
                await doResume(ctx, data, true);
                return liveInfo(ctx);
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
            async *watch(opts?: { throttleMs?: number }): AsyncIterable<JobInfo<Extra>> {
                // Forwarded, not defaulted: `ctx.changes` owns the
                // validation and the window semantics, and an undefined
                // knob normalizes to the old one-per-turn contract there.
                for await (const s of ctx.changes({
                    initial: true,
                    throttleMs: opts?.throttleMs
                })) {
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
