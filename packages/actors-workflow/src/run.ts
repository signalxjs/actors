/**
 * `defineWorkflow` — one actor per run.
 *
 * Four rules are inherited from the workload this is derived from
 * (`perf/aks/src/workflow`), each of which was learned from a cluster
 * rather than reasoned out, and none of which should be relaxed without a
 * measurement saying so:
 *
 * 1. **Methods record, `advance` acts.** Every event — a start, a wake — is
 *    a turn that appends what happened and arms a zero-delay timer. The
 *    `advance` turn then drives the cursor until the run blocks or ends.
 *    The hop exists because `ctx.actor` carries the call chain and A→B→A
 *    is a deadlock on a non-reentrant actor: a timer tick has an empty
 *    chain, so `advance` never runs inside anyone else's call.
 *
 * 2. **A turn never awaits another host.** Not yet load-bearing here (this
 *    version has no child runs), but the rule is why `advance` is a timer
 *    hop rather than a call, and it is what a fan-out will need.
 *
 * 3. **Every wake is token-fenced.** A wake carries `seq`, and only the
 *    token in state is honoured, so a duplicate reminder tick, a timer
 *    re-armed after migration, and a touch racing a real wake are all
 *    harmless.
 *
 * 4. **Reminder firing is at-most-once**, so a lost wake is an outcome
 *    rather than a bug in the plan. A touch — `status()`, an activation —
 *    re-arms anything overdue.
 *
 * Rule 4 is where this differs from its ancestor, deliberately. The perf
 * engine re-armed only for a subset of statuses, so a run whose wake had
 * gone missing in the other statuses was invisible to every touch it had —
 * 9 runs in 2 986 were stranded by a host kill on the cluster, 7 of them
 * in exactly those states (#409). Here `nudge` re-arms for EVERY
 * non-terminal status, and the fence in rule 3 is what makes that safe:
 * an advance armed against a stale wake changes nothing.
 */
import { defineActor, type ActorContext, type TimerHandle } from '@sigx/actors';
import { applyRunEntry, emptyRun, type RunEntry, type RunState } from './log';
import type { Json, RunInfo, TaskHandler, WorkflowDefinition, WorkflowNode } from './types';

export interface WorkflowOptions {
    /** Definitions this deploy knows, keyed by name and version. */
    definitions: readonly WorkflowDefinition[];
    /** Task handlers, by the name a `task` node uses. */
    handlers: Record<string, TaskHandler>;
    /**
     * A delay at or above this rides a DURABLE reminder and the run leaves
     * memory; below it, a volatile timer. Default 30 s, matching the
     * runtime's own reminder cadence — a durable wake cannot be more
     * precise than the tick that delivers it, so a short delay wants the
     * timer and a long one wants to stop holding a host.
     */
    timerThresholdMs?: number;
    /** Actor type name, if one deploy needs several workflow engines. */
    type?: string;
}

const REMINDER_WAKE = 'wake';

/** `methods`, `onActivate` and `onReminder` all receive the SAME ctx object
 *  for an activation, so this is how the hooks reach the closure that owns
 *  the engine. A WeakMap rather than a field, so nothing keeps the ctx
 *  alive past its activation. */
interface RunEngine {
    wake(seq: number): Promise<void>;
    nudge(): void;
}
const engines = new WeakMap<object, RunEngine>();

/** `${var}` substitution, the only templating a definition gets. A missing
 *  variable substitutes empty rather than throwing: a definition is data
 *  and may name a variable a branch never set, and failing a run for that
 *  at the moment of use is worse than passing what the author wrote. */
function substitute(input: Json | undefined, vars: Record<string, Json>): Json {
    if (input === undefined) return null;
    if (typeof input === 'string') {
        const whole = /^\$\{([^}]+)\}$/.exec(input);
        // A whole-string reference keeps the variable's TYPE; an embedded
        // one is necessarily a string.
        if (whole) return vars[whole[1]!] ?? null;
        return input.replace(/\$\{([^}]+)\}/g, (_, name: string) => {
            const v = vars[name];
            return v === undefined || v === null ? '' : String(v);
        });
    }
    if (Array.isArray(input)) return input.map((v) => substitute(v, vars));
    if (typeof input === 'object' && input !== null) {
        return Object.fromEntries(
            Object.entries(input).map(([k, v]) => [k, substitute(v, vars)])
        );
    }
    return input;
}

function compare(left: Json | undefined, op: string, right: Json | undefined): boolean {
    if (op === 'exists') return left !== undefined && left !== null;
    if (op === 'eq') return left === right;
    if (op === 'ne') return left !== right;
    // The ordered comparisons are only meaningful on numbers; anything
    // else is false rather than a coercion nobody predicted.
    if (typeof left !== 'number' || typeof right !== 'number') return false;
    if (op === 'lt') return left < right;
    if (op === 'lte') return left <= right;
    if (op === 'gt') return left > right;
    return op === 'gte' ? left >= right : false;
}

/** Full jitter: a random point in [0, base × 2^(n-1)], capped. Jitter is
 *  not decoration — without it every branch of a fan-out that failed
 *  together retries together, which is the herd the backoff exists to
 *  avoid. */
function backoff(attempt: number, base: number, max: number): number {
    const window = Math.min(base * 2 ** Math.max(0, attempt - 1), max);
    return Math.floor(Math.random() * window);
}

export interface WorkflowEngine {
    /** Register these with `defineActorApp({ actors })`. */
    actors: readonly [ReturnType<typeof defineActor>];
    /** The run actor, for typed clients: `host.actor(engine.run, id)`. */
    run: ReturnType<typeof defineActor>;
}

export function defineWorkflow(options: WorkflowOptions) {
    const threshold = options.timerThresholdMs ?? 30_000;
    const byKey = new Map<string, WorkflowDefinition>();
    for (const d of options.definitions) byKey.set(`${d.name}@${d.version}`, d);
    /** Latest version per name, for a start that does not pin one. */
    const latest = new Map<string, number>();
    for (const d of options.definitions) {
        const seen = latest.get(d.name);
        if (seen === undefined || d.version > seen) latest.set(d.name, d.version);
    }

    const run = defineActor({
        type: options.type ?? 'WorkflowRun',
        state: emptyRun,
        persistence: 'explicit',
        applyEntry: applyRunEntry,
        methods: (ctx: ActorContext<RunState>) => {
            const s = ctx.state;
            const append = (entry: RunEntry): Promise<void> => ctx.append(entry);
            /**
             * What THIS activation has armed, and for which wake.
             *
             * Load-bearing, not bookkeeping: `nudge` runs on every touch,
             * and re-arming a volatile timer that is already counting
             * restarts it. A caller polling `status()` faster than the
             * delay would then push the wake back on every poll and the
             * run would never advance — a livelock caused entirely by
             * watching it. So a wake is armed once per activation per
             * `seq`, and a touch only arms what is genuinely missing.
             */
            let armedSeq = -1;
            let wakeTimer: TimerHandle | null = null;

            const definition = (): WorkflowDefinition => {
                const def = byKey.get(`${s.def}@${s.version}`);
                if (!def) {
                    // A run pinned to a version this deploy no longer
                    // carries. Failing loudly beats guessing at another
                    // version: the run's remaining path is unknowable.
                    throw new Error(
                        `[workflow] run ${ctx.key} is pinned to ${s.def}@${s.version}, which this deploy does not have`
                    );
                }
                return def;
            };

            /** Arm the zero-delay hop that drives the run. Idempotent by
             *  timer name, so recording twice cannot advance twice. */
            const armAdvance = (): void => {
                ctx.timer('advance', () => advance(), { due: 0 });
            };

            /** `after`: the node to move to on waking, or null to
             *  re-enter the current one (a retry backoff). */
            const sleep = async (ms: number, after: string | null): Promise<void> => {
                const seq = s.seq + 1;
                const until = Date.now() + ms;
                const durable = ms >= threshold;
                await append({ t: 'sleep', until, seq, durable, after });
                armedSeq = seq;
                if (durable) {
                    // `due` is ms FROM NOW, not an epoch — `until` is kept
                    // in state because that is what a touch on another
                    // host has to compare against, and the two are not
                    // interchangeable.
                    await ctx.reminders.set(REMINDER_WAKE, { due: ms });
                    // Nothing is holding this run now: the reminder will
                    // bring it back. Staying resident for a long delay is
                    // what stops a fleet from holding a million of them.
                    ctx.deactivate();
                } else {
                    wakeTimer = ctx.timer('wake', () => wake(seq), { due: ms });
                }
            };

            /** A wake fired. Fenced: anything but the current token is a
             *  duplicate or a survivor of a migration, and does nothing. */
            const wake = async (seq: number): Promise<void> => {
                if (!s.wake || s.wake.seq !== seq) return;
                // Read BEFORE the entry clears the wake.
                const after = s.wake.after;
                wakeTimer = null;
                await append({ t: 'wake', seq });
                if (after !== null) await moveTo(after);
                else armAdvance();
            };

            /**
             * Re-arm whatever this run is owed. Called by every touch.
             *
             * The condition is EVERY non-terminal status, which is the
             * #409 fix: a run whose wake went missing — a volatile timer
             * that died with its host, a durable wake whose at-most-once
             * delivery was lost — is otherwise invisible to every touch,
             * and nothing else will ever look at it. Re-arming when
             * nothing was owed is harmless: `advance` re-reads the cursor,
             * and a stale wake cannot fire past the fence.
             */
            const nudge = (): void => {
                if (s.status === 'completed' || s.status === 'failed') return;
                if (s.wake) {
                    const { seq, until, durable } = s.wake;
                    const left = until - Date.now();
                    if (left <= 0) {
                        // Overdue: the wake was lost, or its delivery was
                        // the at-most-once one that did not arrive. Take
                        // it now — this is the recovery path, and the
                        // fence makes a duplicate harmless.
                        void wake(seq);
                        return;
                    }
                    // Still owed. Arm ONLY if this activation has not —
                    // a fresh one after a host died has not, and every
                    // later touch of the same activation has.
                    if (armedSeq === seq) return;
                    armedSeq = seq;
                    if (durable) void ctx.reminders.set(REMINDER_WAKE, { due: left });
                    else wakeTimer = ctx.timer('wake', () => wake(seq), { due: left });
                    return;
                }
                armAdvance();
            };

            const finish = async (
                status: 'completed' | 'failed',
                error?: string
            ): Promise<void> => {
                await append({
                    t: 'end',
                    status,
                    at: Date.now(),
                    ...(error !== undefined ? { error } : {})
                });
                // The compaction point: the log is folded away into one
                // record, so a terminal run costs one read to load.
                await ctx.save();
            };

            const runTask = async (id: string, node: Extract<WorkflowNode, { type: 'task' }>) => {
                const handler = options.handlers[node.handler];
                if (!handler) {
                    await finish('failed', `no handler registered for '${node.handler}'`);
                    return;
                }
                const attempts = node.retry?.maxAttempts ?? 1;
                const n = s.attempt + 1;
                // Recorded BEFORE the call: a host that dies here re-runs
                // the attempt rather than skipping one it may have
                // finished. That is what makes a task at-least-once, and
                // why `idempotencyKey` is part of the handler contract.
                await append({ t: 'attempt', node: id, n });
                try {
                    const out = await handler(substitute(node.input, s.vars), {
                        runId: ctx.key,
                        nodeId: id,
                        attempt: n,
                        idempotencyKey: `${ctx.key}:${id}`
                    });
                    if (node.assignTo) await append({ t: 'vars', set: { [node.assignTo]: out } });
                    await moveTo(node.next);
                } catch (error) {
                    const message = error instanceof Error ? error.message : String(error);
                    if (n >= attempts) {
                        await finish('failed', message);
                        return;
                    }
                    // Retry as a sleep, so a backoff survives the host
                    // dying during it exactly as any other delay does.
                    // null: come back to THIS node, attempts intact.
                    await sleep(backoff(n, node.retry!.backoffMs, node.retry?.maxBackoffMs ?? 30_000), null);
                }
            };

            const moveTo = async (next: string | undefined): Promise<void> => {
                if (!next) {
                    await finish('completed');
                    return;
                }
                await append({ t: 'move', to: next, seq: s.seq });
                armAdvance();
            };

            /** Drive from the cursor until the run blocks or ends. One
             *  node per turn: the loop is the timer, not a `while`, so a
             *  long workflow never holds its host. */
            const advance = async (): Promise<void> => {
                if (s.status !== 'running') return;
                const def = definition();
                const node = def.nodes[s.cursor];
                if (!node) {
                    await finish('failed', `no node '${s.cursor}' in ${def.name}@${def.version}`);
                    return;
                }
                switch (node.type) {
                    case 'task':
                        await runTask(s.cursor, node);
                        return;
                    case 'delay':
                        // The delay's whole job is to move on afterwards;
                        // an end-of-flow delay simply completes.
                        await sleep(node.ms, node.next ?? '');
                        return;
                    case 'branch': {
                        const taken = compare(s.vars[node.var], node.op, node.value);
                        await moveTo(taken ? node.then : node.else);
                        return;
                    }
                    case 'end':
                        await finish(node.status ?? 'completed', node.reason);
                        return;
                }
            };

            engines.set(ctx, { wake, nudge });

            return {
                /**
                 * Begin, or report what already began. Idempotent by run
                 * id: the id IS the idempotency key, so a caller that
                 * retries a start gets the original run rather than a
                 * second one.
                 */
                async start(name: string, vars: Record<string, Json> = {}, version?: number) {
                    if (s.status !== 'running' || s.cursor !== '') return info();
                    const v = version ?? latest.get(name);
                    if (v === undefined) throw new Error(`[workflow] no definition named '${name}'`);
                    const def = byKey.get(`${name}@${v}`);
                    if (!def) throw new Error(`[workflow] no definition '${name}@${v}'`);
                    await append({ t: 'start', def: name, version: v, at: Date.now(), vars });
                    await append({ t: 'move', to: def.start, seq: s.seq });
                    armAdvance();
                    return info();
                },

                /** Read the run, and re-arm anything it is owed. The touch
                 *  is not a side effect to be tidied away: it is the
                 *  recovery path for a wake that was never delivered. */
                status(): RunInfo {
                    nudge();
                    return info();
                },

            };

            function info(): RunInfo {
                return {
                    id: ctx.key,
                    definition: s.def,
                    version: s.version,
                    status: s.status,
                    cursor: s.cursor,
                    vars: { ...s.vars },
                    startedAt: s.startedAt,
                    transitions: s.transitions,
                    ...(s.endedAt !== undefined ? { endedAt: s.endedAt } : {}),
                    ...(s.error !== undefined ? { error: s.error } : {})
                };
            }
        },
        async onReminder(ctx: ActorContext<RunState>, name: string) {
            if (name !== REMINDER_WAKE) return;
            const engine = engines.get(ctx);
            if (!engine) return;
            const seq = ctx.state.wake?.seq;
            // No wake in state: the reminder outlived what it was for —
            // the run was already woken by a touch, or has moved on. The
            // fence would reject it anyway; `nudge` is what makes sure the
            // run is not left owed something.
            if (seq === undefined) engine.nudge();
            else await engine.wake(seq);
        },
        onActivate(ctx: ActorContext<RunState>) {
            // Every activation is a touch, and this is the one that
            // matters: a run re-placed on a survivor after its host died
            // arrives here with whatever it was owed still recorded, and
            // nothing else would ever look at it.
            engines.get(ctx)?.nudge();
        }
    });

    return { actors: [run] as const, run };
}
