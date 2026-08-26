/**
 * `WorkflowRun` — one actor per run; the engine (#297).
 *
 * ## Event-driven, not a task loop
 *
 * A run is a state machine advanced by EVENTS: `start`, a signal, a child
 * finishing, a timer or reminder firing. Every event is one method turn
 * that records what happened, saves, and arms a zero-delay `advance`
 * timer; the `advance` turn then drives the run from its cursor until it
 * blocks (sleeps, waits, joins) or ends, saving after every node. That is
 * how the engines this imitates work (event → decide → schedule), and it
 * is what lets a sleeping run leave memory: nothing is held open.
 *
 * `tasks:` was the other candidate and is deliberately NOT used. A running
 * task costs a `$sigx:tasks` ledger record (one more CAS per start and
 * settle) and a 60 s liveness reminder in the shard table PER RUN, and an
 * activation with a live task cannot deactivate — which for a workload
 * whose point is "how many sleeping runs can a cluster hold" both doubles
 * the reminder-shard traffic and hides the very ceiling being measured.
 *
 * ## Why every event goes through a timer hop
 *
 * `ctx.actor` carries the call chain, and A→B→A is an `ActorDeadlockError`
 * on a non-reentrant actor. A parent starting a child from ITS advance
 * turn, and that child completing synchronously and calling the parent's
 * `childDone` from inside `start`, would be exactly that cycle. A timer
 * tick has an empty chain (`callChain: [self]`, nothing inherited), so
 * `advance` never runs inside anyone else's call — one uniform rule:
 * methods record, `advance` acts.
 *
 * ## Sleeping, and the wake protocol
 *
 * A sleep shorter than `WF_TIMER_THRESHOLD_MS` rides a volatile
 * `ctx.timer` (re-armed on the next activation; a host death loses it
 * until something touches the run); at or above it, a durable reminder
 * (one shard CAS to arm, one to fire, survives host loss). Every wake
 * carries a `token` minted from `seq`, and only the token in state is
 * honoured — so a duplicate reminder tick, a re-armed timer after a
 * migration, or a signal racing a timeout are all harmless.
 *
 * Reminder firing is AT-MOST-ONCE (`host/reminders.ts` deletes the entry
 * before it dispatches), so a lost wake is a real outcome, not a bug in
 * the plan: an overdue durable wake found by a touch — `status()`, an
 * activation, the parent's join watchdog — counts as `wakesLost` and
 * advances anyway. Arming a reminder can also FAIL (three CAS conflicts
 * and the runtime throws); the engine retries with jitter, counts
 * `reminderSetFailures`, and falls back to a volatile timer.
 *
 * Both are findings the rig exists to produce, so both are counted rather
 * than designed around.
 *
 * ## A turn never waits on another host
 *
 * The first cluster run wedged the fleet (#302): a parent's turn awaited
 * `child.start()` on other hosts, each child's last turn awaited
 * `parent.childDone()`, every finishing run awaited `ctx.publish()` to the
 * singleton aggregator — all through a bounded per-peer fetch pool, with
 * no deadline on a call made from a tick. Once the pool held only calls
 * whose target turns were waiting on the pool, three idle hosts sat on
 * 50 000 queued turns for forty minutes. So the rule here is the one every
 * real engine follows: a turn RECORDS the intent (children to start, an
 * event to publish, a parent to notify), saves, and lets the call go out
 * detached; a lost call is repaired by the join watchdog, the sweep, or the
 * notify-retry wake — never by a turn that holds a connection open.
 */
import { topic } from '@sigx/actors';
import type { TimerHandle } from '@sigx/actors';
import { defineActor } from '../actors.app.ts';
import { config, REMINDER_JOIN_CHECK, REMINDER_WAKE } from './config.ts';
import { workflowCounters as C } from './counters.ts';
import { WorkflowDefinition } from './definition.actor.ts';
import { WORKER_KEY, workerFor } from './workers.ts';
import {
    TERMINAL,
    type CompletionEvent,
    type NodeDef,
    type NodeId,
    type RunStats,
    type RunStatus,
    type RunSummary,
    type StartSpec,
    type TaskSpec,
    type Wake,
    type WakeReason,
    type WorkflowDef
} from './types.ts';

export const workflowEvents = (runId: string) =>
    topic<CompletionEvent>('workflow-events', runId);

interface NodeRecord {
    attempts: number;
    status: 'running' | 'done' | 'failed';
    startedAt: number;
    endedAt: number | null;
}

interface ChildRecord {
    nodeId: NodeId;
    workflow: string;
    version: number | undefined;
    status: 'running' | 'done' | 'failed';
    startedAt: number;
}

interface RunState {
    v: 1;
    workflow: string;
    version: number;
    template: string;
    tag: string | null;
    input: Record<string, unknown>;
    parent: { runId: string; nodeId: NodeId } | null;
    status: RunStatus;
    startedAt: number;
    endedAt: number | null;
    /** Transition counter; also the wake-token source. */
    seq: number;
    cursor: NodeId | null;
    vars: Record<string, unknown>;
    nodes: Record<NodeId, NodeRecord>;
    /** Completion order — what a compensation walks backwards. */
    doneOrder: NodeId[];
    wake: Wake | null;
    inbox: Record<string, unknown[]>;
    children: Record<string, ChildRecord>;
    pendingJoin: { nodeId: NodeId; expected: string[]; next: NodeId } | null;
    compensation: { stack: NodeId[]; index: number } | null;
    notifyParent: 'pending' | 'sent' | null;
    error: string | null;
    stats: RunStats;
}

const emptyStats = (): RunStats => ({
    nodeMs: {},
    wakeLagMs: [],
    wakes: { timers: 0, reminders: 0, fallback: 0, lost: 0, stale: 0 },
    signals: { delivered: 0, buffered: 0, late: 0, timedOut: 0 },
    attempts: 0,
    failures: 0,
    compensations: 0,
    children: 0,
    transitions: 0
});

const initialState = (): RunState => ({
    v: 1,
    workflow: '',
    version: 0,
    template: '',
    tag: null,
    input: {},
    parent: null,
    status: 'pending',
    startedAt: 0,
    endedAt: null,
    seq: 0,
    cursor: null,
    vars: {},
    nodes: {},
    doneOrder: [],
    wake: null,
    inbox: {},
    children: {},
    pendingJoin: null,
    compensation: null,
    notifyParent: null,
    error: null,
    stats: emptyStats()
});

type Outcome =
    | { kind: 'next'; to: NodeId }
    | { kind: 'sleep'; ms: number; reason: WakeReason }
    | { kind: 'wait' }
    | { kind: 'join' }
    | { kind: 'end' }
    | { kind: 'fail'; error: string };

const message = (error: unknown): string =>
    error instanceof Error ? error.message : String(error);

/** Per-activation engine — what the hooks reach through `engines`. */
interface Engine {
    wake(token: number, via: 'timer' | 'reminder'): Promise<void>;
    joinCheck(): Promise<void>;
    nudge(): void;
}

// `methods`, `onActivate` and `onReminder` all receive the SAME ctx object
// for an activation, so this is how the hooks reach the closure that owns
// the timer handles and the cached definition.
const engines = new WeakMap<object, Engine>();

export const WorkflowRun = defineActor({
    type: 'WorkflowRun',
    // Public on purpose: the load generator starts and signals runs bare.
    allowAnonymous: true,
    persistence: 'explicit',
    idleAfterMs: config.idleAfterMs,
    state: initialState,
    methods: (ctx) => {
        const s = ctx.state;
        let def: WorkflowDef | null = null;
        let wakeTimer: TimerHandle | null = null;
        let driving = false;

        const save = async (): Promise<void> => {
            C.saves++;
            await ctx.save();
        };

        const loadDef = async (): Promise<WorkflowDef> => {
            if (def) return def;
            const got = await ctx.actor(WorkflowDefinition, s.workflow).get(s.version);
            def = got.def;
            return def;
        };

        const node = (d: WorkflowDef, id: NodeId): NodeDef => {
            const n = d.nodes[id];
            if (!n) throw new Error(`[workflow] ${s.workflow}@${s.version} has no node '${id}'`);
            return n;
        };

        const record = (id: NodeId): NodeRecord =>
            (s.nodes[id] ??= { attempts: 0, status: 'running', startedAt: Date.now(), endedAt: null });

        const sample = (type: string, ms: number): void => {
            (s.stats.nodeMs[type] ??= []).push(Math.round(ms));
        };

        const summary = (): RunSummary => ({
            runId: ctx.key,
            status: s.status,
            cursor: s.cursor,
            seq: s.seq,
            startedAt: s.startedAt,
            endedAt: s.endedAt,
            wake: s.wake ? { ...s.wake } : null,
            attempts: s.stats.attempts,
            transitions: s.stats.transitions,
            error: s.error
        });

        // ---- scheduling ------------------------------------------------

        const armAdvance = (): void => {
            ctx.timer('advance', () => drive(), { due: 0, keepAlive: true });
        };

        const armTimer = (w: Wake, rearm = false): void => {
            if (rearm) C.timersRearmed++;
            else C.timersArmed++;
            const token = w.token;
            wakeTimer = ctx.timer(REMINDER_WAKE, () => engine.wake(token, 'timer'), {
                due: Math.max(0, w.due - Date.now()),
                keepAlive: true
            });
        };

        /** Set a reminder through the runtime's 3-attempt CAS, retried
         *  with jitter on top; false when it never took. */
        const setReminder = async (
            name: string,
            opts: { due: number; period?: number }
        ): Promise<boolean> => {
            for (let attempt = 1; ; attempt++) {
                try {
                    await ctx.reminders.set(name, opts);
                    C.remindersSet++;
                    return true;
                } catch (error) {
                    if (attempt >= config.reminderSetAttempts) {
                        C.reminderSetFailures++;
                        if (__DEV__) {
                            console.warn(
                                `[workflow] ${ctx.key}: reminder '${name}' not armed after ` +
                                    `${attempt} attempts: ${message(error)}`
                            );
                        }
                        return false;
                    }
                    await new Promise((r) => setTimeout(r, 50 + Math.random() * 150 * attempt));
                }
            }
        };

        const clearReminder = (name: string): void => {
            void ctx.reminders.clear(name).catch(() => {});
        };

        const cancelWake = (): void => {
            wakeTimer?.cancel();
            wakeTimer = null;
            if (s.wake?.kind === 'reminder') clearReminder(REMINDER_WAKE);
            s.wake = null;
        };

        /** Record the wake, save, arm it — and leave memory if durable. */
        const sleep = async (nodeId: NodeId, ms: number, reason: WakeReason): Promise<void> => {
            cancelWake();
            const w: Wake = {
                token: ++s.seq,
                nodeId,
                due: Date.now() + ms,
                // A notify-retry is ALWAYS volatile: if the host dies with
                // it, the parent's join watchdog touches the child, and the
                // touch re-arms it. Making it durable would cost every child
                // completion two shard CASes for a guarantee the watchdog
                // already gives.
                kind: reason === 'notify-retry' || ms < config.timerThresholdMs ? 'timer' : 'reminder',
                reason
            };
            s.wake = w;
            if (reason === 'delay' || reason === 'retry') s.status = 'sleeping';
            // Counted BEFORE the save: a durable sleep deactivates right
            // after arming, and a bump after the save would never be stored.
            if (w.kind === 'timer') s.stats.wakes.timers++;
            else s.stats.wakes.reminders++;
            await save();
            if (w.kind === 'timer') {
                armTimer(w);
                return;
            }
            // Relative to the SAME nominal due the timer kind uses — the save
            // and any set retry above already spent part of `ms`, and that
            // must not be read back as reminder lag.
            const armed = await setReminder(REMINDER_WAKE, { due: Math.max(0, w.due - Date.now()) });
            if (!armed) {
                w.kind = 'timer-fallback';
                s.stats.wakes.reminders--;
                s.stats.wakes.fallback++;
                await save();
                armTimer(w);
                return;
            }
            if (config.deactivateOnSleep) ctx.deactivate();
        };

        // ---- node execution --------------------------------------------

        const callWorker = async (id: NodeId, spec: TaskSpec, attempt: number): Promise<void> => {
            const started = performance.now();
            await ctx.actor(workerFor(spec.worker), WORKER_KEY).run({
                seed: `${ctx.key}:${id}:${attempt}`,
                ms: spec.ms,
                failureRate: spec.failureRate ?? 0
            });
            sample('task', performance.now() - started);
        };

        const runTask = async (id: NodeId, n: NodeDef & { type: 'task' }): Promise<Outcome> => {
            const rec = record(id);
            rec.status = 'running';
            rec.attempts++;
            s.stats.attempts++;
            C.taskAttempts++;
            // The "activity scheduled" record: a crash from here re-runs
            // the attempt on the next activation — at-least-once, like an
            // activity in any real engine.
            await save();
            try {
                await callWorker(id, n, rec.attempts);
                return { kind: 'next', to: n.next };
            } catch (error) {
                s.stats.failures++;
                C.taskFailures++;
                if (n.retry && rec.attempts < n.retry.maxAttempts) {
                    return { kind: 'sleep', ms: n.retry.backoffMs * rec.attempts, reason: 'retry' };
                }
                rec.status = 'failed';
                rec.endedAt = Date.now();
                return { kind: 'fail', error: message(error) };
            }
        };

        /** A branch of task nodes, in sequence; retries are immediate
         *  (no backoff sleep — the run's turn is what holds the join). */
        const runBranch = async (d: WorkflowDef, branch: NodeId[]): Promise<void> => {
            for (const id of branch) {
                const n = node(d, id);
                if (n.type !== 'task') throw new Error(`[workflow] parallel branch node '${id}' is not a task`);
                const rec = record(id);
                for (;;) {
                    rec.attempts++;
                    s.stats.attempts++;
                    C.taskAttempts++;
                    try {
                        await callWorker(id, n, rec.attempts);
                        rec.status = 'done';
                        rec.endedAt = Date.now();
                        s.doneOrder.push(id);
                        break;
                    } catch (error) {
                        s.stats.failures++;
                        C.taskFailures++;
                        if (n.retry && rec.attempts < n.retry.maxAttempts) continue;
                        rec.status = 'failed';
                        rec.endedAt = Date.now();
                        throw error;
                    }
                }
            }
        };

        const runParallel = async (
            d: WorkflowDef,
            id: NodeId,
            n: NodeDef & { type: 'parallel' }
        ): Promise<Outcome> => {
            const rec = record(id);
            const started = performance.now();
            await save();
            const results = await Promise.allSettled(n.branches.map((b) => runBranch(d, b)));
            sample('parallel', performance.now() - started);
            const failed = results.find((r) => r.status === 'rejected');
            if (failed) {
                rec.status = 'failed';
                rec.endedAt = Date.now();
                return { kind: 'fail', error: message((failed as PromiseRejectedResult).reason) };
            }
            return { kind: 'next', to: n.next };
        };

        const runFanoutTasks = async (
            id: NodeId,
            n: NodeDef & { type: 'fanout' }
        ): Promise<Outcome> => {
            const spec = n.task;
            if (!spec) throw new Error(`[workflow] fanout '${id}' in tasks mode has no task spec`);
            const rec = record(id);
            const units = Array.from({ length: n.width }, (_, i) => `${id}#${i}`);
            for (const unit of units) {
                const u = record(unit);
                u.attempts++;
            }
            s.stats.attempts += n.width;
            C.taskAttempts += n.width;
            const started = performance.now();
            await save();
            const results = await Promise.allSettled(
                units.map(async (unit) => {
                    await callWorker(unit, spec, 1);
                    const u = record(unit);
                    u.status = 'done';
                    u.endedAt = Date.now();
                })
            );
            sample('fanout', performance.now() - started);
            const failed = results.filter((r) => r.status === 'rejected');
            s.stats.failures += failed.length;
            C.taskFailures += failed.length;
            if (failed.length > 0) {
                rec.status = 'failed';
                rec.endedAt = Date.now();
                return { kind: 'fail', error: message((failed[0] as PromiseRejectedResult).reason) };
            }
            return { kind: 'next', to: n.next };
        };

        const startChild = async (childId: string): Promise<void> => {
            const c = s.children[childId];
            if (!c) return;
            await ctx.actor(WorkflowRun, childId).start({
                workflow: c.workflow,
                version: c.version,
                template: c.workflow,
                parent: { runId: ctx.key, nodeId: c.nodeId },
                ...(s.tag !== null ? { tag: s.tag } : {})
            });
        };

        const startChildren = async (
            id: NodeId,
            width: number,
            child: { workflow: string; version?: number },
            next: NodeId
        ): Promise<Outcome> => {
            record(id);
            const expected: string[] = [];
            for (let i = 0; i < width; i++) {
                const childId = `${ctx.key}.${id}.${i}`;
                s.children[childId] = {
                    nodeId: id,
                    workflow: child.workflow,
                    version: child.version,
                    status: 'running',
                    startedAt: Date.now()
                };
                expected.push(childId);
            }
            s.pendingJoin = { nodeId: id, expected, next };
            s.status = 'blocked';
            s.stats.children += width;
            // Recorded BEFORE any child starts: a crash between here and
            // the starts leaves records the join watchdog can repair.
            await save();
            // DETACHED (#302): the turn ends here; the starts go out without
            // holding it, and a start that never lands is re-issued by the
            // join watchdog. `start()` is idempotent, so a duplicate is free.
            for (const childId of expected) {
                void startChild(childId).then(
                    () => C.childStarts++,
                    () => C.childStartFailures++
                );
            }
            await setReminder(REMINDER_JOIN_CHECK, {
                due: config.childStaleMs,
                period: config.joinCheckPeriodMs
            });
            return { kind: 'join' };
        };

        /** Take the next queued signal for a wait node, if any. */
        const deliverSignal = (n: NodeDef & { type: 'wait' }): boolean => {
            const queue = s.inbox[n.signal];
            if (!queue || queue.length === 0) return false;
            s.vars[`signal:${n.signal}`] = queue.shift();
            s.stats.signals.delivered++;
            C.signalsDelivered++;
            return true;
        };

        const evalBranch = (n: NodeDef & { type: 'branch' }): boolean => {
            const left = s.vars[n.var] ?? s.input[n.var];
            switch (n.op) {
                case 'gt':
                    return Number(left) > Number(n.value);
                case 'lt':
                    return Number(left) < Number(n.value);
                case 'eq':
                    return left === n.value;
            }
        };

        const execute = async (d: WorkflowDef, id: NodeId, n: NodeDef): Promise<Outcome> => {
            switch (n.type) {
                case 'task':
                    return runTask(id, n);
                case 'delay':
                    record(id);
                    return { kind: 'sleep', ms: n.ms, reason: 'delay' };
                case 'branch':
                    record(id);
                    return { kind: 'next', to: evalBranch(n) ? n.then : n.else };
                case 'parallel':
                    return runParallel(d, id, n);
                case 'fanout':
                    if (n.mode === 'tasks') return runFanoutTasks(id, n);
                    if (!n.child) throw new Error(`[workflow] fanout '${id}' has no child`);
                    return startChildren(id, n.width, n.child, n.next);
                case 'subworkflow':
                    return startChildren(id, 1, { workflow: n.workflow, version: n.version }, n.next);
                case 'wait':
                    record(id);
                    if (deliverSignal(n)) return { kind: 'next', to: n.next };
                    return { kind: 'wait' };
                case 'end':
                    return { kind: 'end' };
            }
        };

        // ---- the drive loop --------------------------------------------

        /** Mark `from` done and move the cursor — along the definition, or
         *  along the compensation stack when one is being walked. */
        const moveTo = async (from: NodeId, to: NodeId): Promise<void> => {
            const rec = record(from);
            rec.status = 'done';
            rec.endedAt = Date.now();
            if (!s.doneOrder.includes(from)) s.doneOrder.push(from);
            if (s.compensation) {
                s.compensation.index++;
                s.cursor = s.compensation.stack[s.compensation.index] ?? null;
            } else {
                s.cursor = to;
            }
            await save();
        };

        const onFailure = async (d: WorkflowDef, error: string): Promise<void> => {
            s.error = error;
            if (d.onFailure === 'compensate' && !s.compensation) {
                const stack: NodeId[] = [];
                for (const id of [...s.doneOrder].reverse()) {
                    const n = d.nodes[id];
                    if (n?.type === 'task' && n.compensate) stack.push(n.compensate);
                }
                if (stack.length > 0) {
                    s.compensation = { stack, index: 0 };
                    s.status = 'compensating';
                    s.cursor = stack[0] as NodeId;
                    s.stats.compensations += stack.length;
                    C.compensations += stack.length;
                    await save();
                    return;
                }
            }
            await finish('failed');
        };

        const resolveJoin = async (d: WorkflowDef): Promise<boolean> => {
            const pj = s.pendingJoin;
            if (!pj) {
                s.status = s.compensation ? 'compensating' : 'running';
                return true;
            }
            let failed = false;
            for (const childId of pj.expected) {
                const c = s.children[childId];
                if (!c || c.status === 'running') return false;
                if (c.status === 'failed') failed = true;
            }
            s.pendingJoin = null;
            clearReminder(REMINDER_JOIN_CHECK);
            s.status = s.compensation ? 'compensating' : 'running';
            if (failed) {
                const rec = record(pj.nodeId);
                rec.status = 'failed';
                rec.endedAt = Date.now();
                await onFailure(d, 'child run failed');
            } else {
                sample('join', Date.now() - record(pj.nodeId).startedAt);
                await moveTo(pj.nodeId, pj.next);
            }
            return !TERMINAL.has(s.status);
        };

        const driveInner = async (): Promise<void> => {
            const d = await loadDef();
            for (;;) {
                if (s.status === 'blocked') {
                    if (!(await resolveJoin(d))) return;
                    continue;
                }
                if (s.status === 'waiting') {
                    const cursor = s.cursor;
                    const n = cursor ? node(d, cursor) : null;
                    if (!n || n.type !== 'wait' || !cursor) throw new Error('[workflow] waiting off a wait node');
                    if (!deliverSignal(n)) return;
                    cancelWake();
                    s.status = 'running';
                    sample('wait', Date.now() - record(cursor).startedAt);
                    await moveTo(cursor, n.next);
                    continue;
                }
                if (s.status !== 'running' && s.status !== 'compensating') return;
                const cursor = s.cursor;
                if (cursor === null) {
                    await finish(s.compensation ? 'compensated' : 'completed');
                    return;
                }
                const n = node(d, cursor);
                const outcome = await execute(d, cursor, n);
                s.stats.transitions++;
                C.transitions++;
                switch (outcome.kind) {
                    case 'next':
                        await moveTo(cursor, outcome.to);
                        break;
                    case 'sleep':
                        await sleep(cursor, outcome.ms, outcome.reason);
                        return;
                    case 'wait':
                        s.status = 'waiting';
                        await sleep(cursor, (n as NodeDef & { type: 'wait' }).timeoutMs, 'signal-timeout');
                        return;
                    case 'join':
                        return;
                    case 'end':
                        await finish(s.compensation ? 'compensated' : 'completed');
                        return;
                    case 'fail':
                        await onFailure(d, outcome.error);
                        if (TERMINAL.has(s.status)) return;
                        break;
                }
            }
        };

        /** Every entry into the machine from a tick: one failure funnel. */
        const guarded = async (fn: () => Promise<void>): Promise<void> => {
            if (driving) return;
            driving = true;
            try {
                await fn();
            } catch (error) {
                if (__DEV__) console.error(`[workflow] ${ctx.key}: run failed:`, error);
                if (!TERMINAL.has(s.status)) {
                    s.error = message(error);
                    try {
                        await finish('failed');
                    } catch {
                        // a save conflict here means the activation is
                        // already discarded — nothing left to record
                    }
                }
            } finally {
                driving = false;
            }
        };

        const drive = (): Promise<void> => guarded(driveInner);

        // ---- completion ------------------------------------------------

        /** Detached (#302): the completion event leaves without holding
         *  the turn; a failure is counted, never awaited. */
        const publishEvent = (): void => {
            const event: CompletionEvent = {
                runId: ctx.key,
                workflow: s.workflow,
                version: s.version,
                template: s.template,
                tag: s.tag,
                parentRunId: s.parent?.runId ?? null,
                status: s.status,
                startedAt: s.startedAt,
                endedAt: s.endedAt ?? Date.now(),
                error: s.error,
                stats: ctx.snapshot(s.stats)
            };
            void ctx.publish(workflowEvents(ctx.key), event).then(
                (report) => {
                    C.publishes++;
                    if (report.failures.length > 0) C.publishFailures++;
                },
                () => {
                    C.publishFailures++;
                }
            );
        };

        /**
         * The parent is told through a detached call (#302). The turn
         * arms a notify-retry wake FIRST and lets the call go; success
         * clears the wake, and if the wake fires the call is simply made
         * again — `childDone` is idempotent on the parent's side.
         */
        const notifyParent = async (): Promise<void> => {
            const parent = s.parent;
            if (!parent || s.notifyParent !== 'pending') return;
            await sleep(s.cursor ?? 'end', config.notifyRetryMs, 'notify-retry');
            void ctx.actor(WorkflowRun, parent.runId)
                .childDone(ctx.key, s.status)
                .then(
                    () => ctx.timer('notified', () => markNotified(), { due: 0 }),
                    () => {
                        C.childDoneRetries++;
                    }
                );
        };

        /** A turn of its own: the detached call may settle after the
         *  notifying turn has long returned. */
        const markNotified = async (): Promise<void> => {
            if (s.notifyParent !== 'pending') return;
            // Whichever attempt landed, the parent knows: any notify-retry
            // wake still armed is now redundant.
            if (s.wake?.reason === 'notify-retry') cancelWake();
            s.notifyParent = 'sent';
            await save();
            if (config.deactivateOnSleep) ctx.deactivate();
        };

        const finish = async (status: RunStatus): Promise<void> => {
            cancelWake();
            if (s.pendingJoin) clearReminder(REMINDER_JOIN_CHECK);
            s.status = status;
            s.endedAt = Date.now();
            s.cursor = null;
            if (s.parent) s.notifyParent = 'pending';
            await save();
            C.runsFinished++;
            publishEvent();
            await notifyParent();
            if (config.deactivateOnSleep && s.notifyParent !== 'pending') ctx.deactivate();
        };

        // ---- wakes -----------------------------------------------------

        const applyWake = async (w: Wake): Promise<void> => {
            const d = await loadDef();
            s.wake = null;
            wakeTimer = null;
            switch (w.reason) {
                case 'delay': {
                    s.stats.wakeLagMs.push(Math.max(0, Date.now() - w.due));
                    sample('delay', Date.now() - record(w.nodeId).startedAt);
                    s.status = s.compensation ? 'compensating' : 'running';
                    const n = node(d, w.nodeId);
                    await moveTo(w.nodeId, n.type === 'delay' ? n.next : w.nodeId);
                    return;
                }
                case 'retry':
                    s.status = s.compensation ? 'compensating' : 'running';
                    await save();
                    return;
                case 'signal-timeout': {
                    s.stats.signals.timedOut++;
                    C.signalTimeouts++;
                    s.status = 'running';
                    const n = node(d, w.nodeId);
                    sample('wait', Date.now() - record(w.nodeId).startedAt);
                    await moveTo(w.nodeId, n.type === 'wait' ? n.onTimeout : w.nodeId);
                    return;
                }
                case 'notify-retry':
                    C.childDoneRetries++;
                    await notifyParent();
                    return;
            }
        };

        const engine: Engine = {
            wake: async (token, via) => {
                if (via === 'timer') C.timersFired++;
                else C.remindersFired++;
                const w = s.wake;
                if (!w || w.token !== token) {
                    C.wakesStale++;
                    s.stats.wakes.stale++;
                    return;
                }
                await guarded(async () => {
                    await applyWake(w);
                    if (w.reason !== 'notify-retry') await driveInner();
                });
            },
            joinCheck: async () => {
                C.joinChecks++;
                const pj = s.pendingJoin;
                if (!pj || TERMINAL.has(s.status)) {
                    clearReminder(REMINDER_JOIN_CHECK);
                    return;
                }
                const now = Date.now();
                for (const childId of pj.expected) {
                    const c = s.children[childId];
                    if (!c || c.status !== 'running' || now - c.startedAt < config.childStaleMs) continue;
                    C.joinRepairs++;
                    // Idempotent: a child that did start ignores this; one
                    // whose start was lost gets it now. The status() call is
                    // the nudge that recovers a child stuck on a lost wake.
                    try {
                        await startChild(childId);
                        await ctx.actor(WorkflowRun, childId).status();
                    } catch {
                        C.childStartFailures++;
                    }
                }
                armAdvance();
            },
            nudge: () => {
                if (TERMINAL.has(s.status)) {
                    if (s.notifyParent === 'pending' && s.wake && !wakeTimer) armTimer(s.wake, true);
                    return;
                }
                const w = s.wake;
                if (w) {
                    if (w.kind === 'reminder') {
                        if (Date.now() - w.due > config.staleWakeMs && !wakeTimer) {
                            C.wakesLost++;
                            s.stats.wakes.lost++;
                            w.kind = 'timer-fallback';
                            armTimer(w, true);
                        }
                    } else if (!wakeTimer) {
                        armTimer(w, true);
                    }
                    return;
                }
                // No wake and not driving: a crash mid-drive, or a join
                // to re-check. Either way the advance turn sorts it out.
                if (!driving && (s.status === 'running' || s.status === 'compensating' || s.status === 'blocked')) {
                    armAdvance();
                }
            }
        };
        engines.set(ctx, engine);

        // A fresh activation of an in-flight run: re-arm what the previous
        // activation held in memory. Runs as the first tick, not inline —
        // the factory is not a turn.
        if (s.status !== 'pending') {
            ctx.timer('activate', () => engine.nudge(), { due: 0 });
        }

        return {
            async start(spec: StartSpec): Promise<RunSummary> {
                if (s.status !== 'pending') return summary();
                s.workflow = spec.workflow;
                s.template = spec.template;
                s.tag = spec.tag ?? null;
                s.input = spec.input ?? {};
                s.parent = spec.parent ?? null;
                s.startedAt = Date.now();
                const got = await ctx.actor(WorkflowDefinition, spec.workflow).get(spec.version);
                def = got.def;
                s.version = got.version;
                s.cursor = def.start;
                s.status = 'running';
                await save();
                C.runsStarted++;
                armAdvance();
                return summary();
            },
            async signal(name: string, payload: unknown): Promise<{ accepted: boolean }> {
                if (TERMINAL.has(s.status)) {
                    C.signalsLate++;
                    s.stats.signals.late++;
                    return { accepted: false };
                }
                (s.inbox[name] ??= []).push(payload);
                let matches = false;
                if (s.status === 'waiting' && s.cursor) {
                    const n = (await loadDef()).nodes[s.cursor];
                    matches = n?.type === 'wait' && n.signal === name;
                }
                if (!matches) {
                    s.stats.signals.buffered++;
                    C.signalsBuffered++;
                }
                await save();
                if (matches) armAdvance();
                return { accepted: true };
            },
            async childDone(
                childId: string,
                status: RunStatus
            ): Promise<{ accepted: boolean; duplicate?: boolean }> {
                C.childDoneCalls++;
                const c = s.children[childId];
                if (!c) return { accepted: false };
                if (c.status !== 'running') {
                    C.childDoneDuplicates++;
                    return { accepted: true, duplicate: true };
                }
                c.status = status === 'completed' ? 'done' : 'failed';
                await save();
                if (s.status === 'blocked') armAdvance();
                return { accepted: true };
            },
            async cancel(): Promise<RunSummary> {
                if (TERMINAL.has(s.status)) return summary();
                if (s.status !== 'pending') {
                    await guarded(() => finish('cancelled'));
                }
                return summary();
            },
            /** Also the nudge: an overdue wake found here is advanced. */
            async status(): Promise<RunSummary> {
                engine.nudge();
                return summary();
            },
            async vars(): Promise<Record<string, unknown>> {
                return ctx.snapshot(s.vars);
            },
            /**
             * TEST HOOK — drop whatever is armed for the current wake while
             * leaving the wake in state: what a host death between "shard
             * persisted" and "dispatched" looks like from the run's side.
             * Perf rig only; no production caller.
             */
            async debugDropWake(): Promise<void> {
                wakeTimer?.cancel();
                wakeTimer = null;
                await ctx.reminders.clear(REMINDER_WAKE).catch(() => {});
            }
        };
    },
    onReminder: async (ctx, name) => {
        const engine = engines.get(ctx);
        if (!engine) return;
        if (name === REMINDER_WAKE) {
            const token = ctx.state.wake?.token;
            if (token === undefined) {
                C.remindersFired++;
                C.wakesStale++;
                return;
            }
            await engine.wake(token, 'reminder');
        } else if (name === REMINDER_JOIN_CHECK) {
            await engine.joinCheck();
        }
    }
});
