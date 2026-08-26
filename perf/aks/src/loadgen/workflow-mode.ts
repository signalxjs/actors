/**
 * MODE=workflow — the workflow-engine workload (#297), as a function the
 * load generator calls. Lives beside `loop.ts` rather than inside
 * `loadgen.mjs` because it is the one mode with real structure: open-loop
 * arrivals, a cursor-driven drain of the aggregator, a final sweep that
 * classifies what did not finish.
 *
 * ## Open loop, on purpose
 *
 * The other modes are closed loops: N workers, each re-issuing on
 * completion. A workflow engine sees ARRIVALS — orders placed, approvals
 * requested — and an engine that falls behind does not slow its callers
 * down, it accumulates a backlog. So this mode starts runs at
 * `WF_START_RATE` per second (Poisson by default, `WF_ARRIVAL=fixed` for
 * evenly spaced) regardless of how fast they finish, bounded only by
 * `WF_MAX_INFLIGHT` — and when THAT bound defers a start, the deferral is
 * counted and timed rather than hidden inside a slower rate. A closed
 * loop would throttle itself to whatever the cluster could absorb and
 * report the reminder-shard backlog as a lower ops/s.
 *
 * ## Tracking completions costs one call per poll, not one per run
 *
 * Every run's completion lands in the `WorkflowStats` aggregator (a topic
 * subscriber), which this polls by cursor every `WF_POLL_MS`. NOT
 * `status()` per run: that is O(runs) calls per tick, and every one of
 * them re-activates a run that deliberately deactivated while sleeping —
 * the sleeping-runs scenario would be measuring its own poll. NOT `watch`
 * streams either: one cross-host stream per run pins a fetch-pool
 * connection for its lifetime (#194), the exact cliff a load generator
 * must not manufacture. `status()` is called exactly once per run that is
 * still in flight when the drain window closes — the sweep — and that
 * call is also the nudge that recovers a lost wake, so the sweep reports
 * both what was stuck and what a touch unstuck.
 *
 * Latencies are HOST-side (`endedAt − startedAt`, both stamped on hosts),
 * so the poll interval never inflates them; `observedMs` is the client's
 * view including it, reported separately and informational.
 *
 * Output: ONE JSON line per rate rung on stdout, everything else on
 * stderr — the contract every mode shares.
 */
import { Samples } from './histogram.ts';
import {
    allDefinitions,
    DEFAULT_KNOBS,
    pickTemplate,
    templateWeights
} from '../workflow/templates.ts';

export interface WireResult {
    data?: unknown;
    error?: string;
}

export interface WorkflowModeIo {
    /** One wire call; `error` is the kind string, `data` the payload. */
    call(type: string, method: string, args: unknown[]): Promise<WireResult>;
    log(...args: unknown[]): void;
    target: string;
    runId: string;
}

interface DrainedEvent {
    seq: number;
    runId: string;
    template: string;
    status: string;
    startedAt: number;
    endedAt: number;
    parent: boolean;
    error?: string;
}

const num = (name: string, fallback: number): number => {
    const raw = process.env[name];
    if (raw === undefined || raw === '') return fallback;
    const value = Number(raw);
    if (!Number.isFinite(value) || value < 0) {
        console.error(`[loadgen] ${name} must be a non-negative number, got '${raw}'`);
        process.exit(1);
    }
    return value;
};

const TERMINAL = new Set(['completed', 'failed', 'compensated', 'cancelled']);

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const round = (n: number) => Math.round(n * 1000) / 1000;
type Pct = { count: number; p50: number; p90: number; p99: number; max: number } | null;
const pct = (p: { count: number; p50: number; p90: number; p99: number; max: number } | null | undefined): Pct =>
    p && p.count > 0
        ? { count: p.count, p50: round(p.p50), p90: round(p.p90), p99: round(p.p99), max: round(p.max) }
        : null;

export async function runWorkflowMode(io: WorkflowModeIo): Promise<never> {
    const { call, log, target, runId } = io;
    const knobs = {
        taskMs: num('WF_TASK_MS', DEFAULT_KNOBS.taskMs),
        delayMs: num('WF_DELAY_MS', DEFAULT_KNOBS.delayMs),
        fanoutWidth: num('WF_FANOUT_WIDTH', DEFAULT_KNOBS.fanoutWidth),
        fanoutMode: (process.env.WF_FANOUT_MODE === 'tasks' ? 'tasks' : 'children') as 'tasks' | 'children',
        failureRate: num('WF_FAILURE_RATE', DEFAULT_KNOBS.failureRate),
        signalTimeoutMs: num('WF_SIGNAL_TIMEOUT_MS', DEFAULT_KNOBS.signalTimeoutMs),
        retryMax: num('WF_RETRY_MAX', DEFAULT_KNOBS.retryMax),
        retryBackoffMs: num('WF_RETRY_BACKOFF_MS', DEFAULT_KNOBS.retryBackoffMs),
        version: num('WF_SEED_VERSION', 1)
    };
    const mix = process.env.WF_MIX ?? 'order:50,approval:20,etl:20,saga:10';
    const weights = templateWeights(mix);
    const arrival = process.env.WF_ARRIVAL === 'fixed' ? 'fixed' : 'poisson';
    const startRate = num('WF_START_RATE', 25);
    const maxInflight = num('WF_MAX_INFLIGHT', 5000);
    const pollMs = num('WF_POLL_MS', 2000);
    const signalDelayMs = num('WF_SIGNAL_DELAY_MS', 2000);
    const signalSkipRatio = num('WF_SIGNAL_SKIP_RATIO', 0.2);
    const durationS = num('DURATION_S', 60);
    const drainS = num(
        'WF_DRAIN_S',
        Math.max(120, Math.ceil((3 * knobs.signalTimeoutMs + knobs.delayMs) / 1000))
    );
    const reportIntervalS = num('REPORT_INTERVAL_S', 10);
    const rungs = (process.env.SWEEP ?? '')
        .split(',')
        .map((s) => Number(s.trim()))
        .filter((n) => Number.isFinite(n) && n > 0);
    if (rungs.length === 0) rungs.push(startRate);

    // ---- seed ---------------------------------------------------------
    for (const def of allDefinitions(knobs)) {
        const r = await call('WorkflowDefinition', 'put', [def.name, def]);
        if (r.error) {
            log(`seeding ${def.name}@${def.version} failed: ${r.error}`);
            process.exit(1);
        }
    }
    const reset = await call('WorkflowStats', 'reset', ['all']);
    if (reset.error) {
        log(`WorkflowStats.reset failed: ${reset.error}`);
        process.exit(1);
    }
    log(`seeded ${allDefinitions(knobs).length} definitions @v${knobs.version}; mix=${mix}`);

    for (const rate of rungs) {
        const result = await oneRung(rate);
        console.log(JSON.stringify(result));
    }
    // Always 0 once the rows are out. A start that 504s under overload
    // and a run left stuck are MEASUREMENTS — the row carries them and
    // `testenv.mjs wf-load` exits 1 on `stuck` — whereas a non-zero exit
    // here marks the k8s Job failed and the whole run PARTIAL, which is
    // what a generator pod DYING means, and is how an overloaded rung came
    // to be refused as a harness fault rather than reported as a ceiling.
    process.exit(0);

    // ---- one rung -----------------------------------------------------
    async function oneRung(rate: number) {
        const tag = `${runId}-r${rate}`;
        const errors = new Map<string, number>();
        const tally = (kind: string) => errors.set(kind, (errors.get(kind) ?? 0) + 1);
        /** runId → { template, startedAtClient } */
        const inflight = new Map<string, { template: string; startedAtClient: number }>();
        /** Runs whose start() errored at the client — the run may still
         *  have started (a 504 is the deadline, not the outcome). */
        const startFailed = new Set<string>();
        /** Failed runs by (normalised) reason. */
        const failedByError: Record<string, number> = {};
        const latency = new Map<string, Samples>(); // template → host-side
        const observed = new Map<string, Samples>(); // template → client-side
        const startLatency = new Samples();
        const deferredMs = new Samples();
        const samplesFor = (map: Map<string, Samples>, key: string): Samples => {
            let s = map.get(key);
            if (!s) map.set(key, (s = new Samples()));
            return s;
        };
        const counts: Record<string, number> = {
            started: 0,
            startFailures: 0,
            startsDeferred: 0,
            completed: 0,
            failed: 0,
            compensated: 0,
            cancelled: 0,
            childEvents: 0,
            unknownEvents: 0,
            droppedEvents: 0,
            signalsSent: 0,
            signalFailures: 0,
            signalsSkipped: 0
        };
        const byTemplate: Record<string, Record<string, number>> = {};
        const bump = (template: string, status: string) => {
            const t = (byTemplate[template] ??= {});
            t[status] = (t[status] ?? 0) + 1;
        };
        const pending = new Set<Promise<unknown>>(); // in-flight start/signal promises
        let n = 0;

        const startOne = async (template: string): Promise<void> => {
            const id = `${tag}-${template}-${n++}`;
            const startedAtClient = Date.now();
            inflight.set(id, { template, startedAtClient });
            counts.started = (counts.started ?? 0) + 1;
            const t0 = performance.now();
            const r = await call('WorkflowRun', 'start', [
                id,
                { workflow: template, version: knobs.version, template, input: { n }, tag }
            ]);
            startLatency.record(performance.now() - t0);
            if (r.error) {
                counts.startFailures++;
                counts.started--;
                inflight.delete(id);
                startFailed.add(id);
                tally(`start:${r.error}`);
                return;
            }
            if (template === 'approval') {
                if (Math.random() < signalSkipRatio) {
                    counts.signalsSkipped++;
                    return;
                }
                const delay = signalDelayMs * (0.5 + Math.random());
                const p = sleep(delay).then(async () => {
                    const s = await call('WorkflowRun', 'signal', [id, 'approve', { by: 'loadgen' }]);
                    if (s.error) {
                        counts.signalFailures++;
                        tally(`signal:${s.error}`);
                    } else {
                        counts.signalsSent++;
                    }
                });
                pending.add(p);
                p.finally(() => pending.delete(p));
            }
        };

        const started = performance.now();
        const arrivalsEnd = started + durationS * 1000;
        const drainEnd = arrivalsEnd + drainS * 1000;
        let cursor = 0;
        let lastReport = started;

        const applyEvent = (e: DrainedEvent) => {
            if (e.status === 'failed' && e.error) {
                // Strip the run/node ids so the same cause buckets together.
                const key = e.error.replace(/[A-Za-z0-9_.-]*-r\d+-[A-Za-z0-9_.-]+/g, '<run>').slice(0, 80);
                failedByError[key] = (failedByError[key] ?? 0) + 1;
            }
            const rec = inflight.get(e.runId);
            if (rec) {
                inflight.delete(e.runId);
                counts[e.status] = (counts[e.status] ?? 0) + 1;
                bump(e.template, e.status);
                samplesFor(latency, e.template).record(e.endedAt - e.startedAt);
                samplesFor(observed, e.template).record(Date.now() - rec.startedAtClient);
            } else if (e.parent) {
                counts.childEvents++;
                bump(e.template, e.status);
                samplesFor(latency, e.template).record(e.endedAt - e.startedAt);
            } else if (startFailed.has(e.runId)) {
                // The deadline expired at the client but the run ran: the
                // engine did the work, the caller only lost the ack.
                counts.startFailedButRan = (counts.startFailedButRan ?? 0) + 1;
                bump(e.template, e.status);
            } else {
                counts.unknownEvents++;
            }
        };

        const drain = async () => {
            for (;;) {
                const r = await call('WorkflowStats', 'drain', ['all', tag, cursor, 5000]);
                if (r.error) {
                    tally(`drain:${r.error}`);
                    return;
                }
                const { cursor: next, events, dropped } = r.data as {
                    cursor: number;
                    events: DrainedEvent[];
                    dropped: number;
                };
                counts.droppedEvents = (counts.droppedEvents ?? 0) + dropped;
                for (const e of events) applyEvent(e);
                cursor = next;
                if (events.length < 5000) return;
            }
        };

        const report = () => {
            log(
                `t=${new Date().toISOString()} rate=${rate} started=${counts.started} ` +
                    `inflight=${inflight.size} completed=${counts.completed} failed=${counts.failed} ` +
                    `compensated=${counts.compensated} deferred=${counts.startsDeferred} ` +
                    `errors=${[...errors.values()].reduce((a, b) => a + b, 0)}`
            );
        };

        log(`run: mode=workflow rate=${rate}/s arrival=${arrival} duration=${durationS}s drain<=${drainS}s target=${target}`);

        // Arrivals and the drain poll run side by side.
        let polling = true;
        const poller = (async () => {
            while (polling) {
                await sleep(pollMs);
                await drain();
                if (performance.now() - lastReport >= reportIntervalS * 1000) {
                    lastReport = performance.now();
                    report();
                }
            }
        })();

        while (performance.now() < arrivalsEnd) {
            const gap = arrival === 'poisson' ? (-Math.log(1 - Math.random()) / rate) * 1000 : 1000 / rate;
            await sleep(gap);
            if (inflight.size >= maxInflight) {
                counts.startsDeferred++;
                const t0 = performance.now();
                while (inflight.size >= maxInflight && performance.now() < arrivalsEnd) await sleep(20);
                deferredMs.record(performance.now() - t0);
                if (performance.now() >= arrivalsEnd) break;
            }
            const template = pickTemplate(weights, Math.random());
            const p = startOne(template);
            pending.add(p);
            p.finally(() => pending.delete(p));
        }
        const arrivalsMs = performance.now() - started;
        log(`arrivals done: ${counts.started} started in ${Math.round(arrivalsMs)}ms; draining ${inflight.size}`);

        // Drain: let signals land and runs finish.
        while (performance.now() < drainEnd) {
            if (inflight.size === 0 && pending.size === 0) break;
            await sleep(Math.min(pollMs, 500));
        }
        polling = false;
        await poller;
        await Promise.allSettled(pending);
        await drain();
        const drainMs = performance.now() - started - arrivalsMs;

        // ---- the sweep: one status() per run still in flight ---------
        const stuck: Record<string, number> = { sleeping: 0, waiting: 0, blocked: 0, running: 0, other: 0, total: 0 };
        let completedUnreported = 0;
        let sweepUnpollable = 0;
        /** The last status the sweep saw for each unfinished run. */
        const lastStatus = new Map<string, string>();
        const sweep = async () => {
            // Deleting the current entry mid-iteration is safe on a Map.
            for (const [id, rec] of inflight) {
                const r = await call('WorkflowRun', 'status', [id]);
                if (r.error) {
                    sweepUnpollable++;
                    tally(`sweep:${r.error}`);
                    continue;
                }
                const s = (r.data as { status: string }).status;
                lastStatus.set(id, s);
                if (TERMINAL.has(s)) {
                    // Terminal on the host, never seen here: the topic
                    // delivery to the aggregator was lost.
                    completedUnreported++;
                    inflight.delete(id);
                    counts[s] = (counts[s] ?? 0) + 1;
                    bump(rec.template, s);
                }
            }
        };
        if (inflight.size > 0) {
            log(`sweeping ${inflight.size} unfinished run(s) — the status() touch also nudges lost wakes`);
            await sweep();
            // A touch may have un-stuck some; give them one poll to finish.
            if (inflight.size > 0) {
                await sleep(Math.max(pollMs, 2000));
                await drain();
                await sweep();
            }
            for (const [id] of inflight) {
                const s = lastStatus.get(id) ?? 'other';
                const slot = s in stuck ? s : 'other';
                stuck[slot] = (stuck[slot] ?? 0) + 1;
                stuck.total = (stuck.total ?? 0) + 1;
            }
        }

        const snap = await call('WorkflowStats', 'snapshot', ['all']);
        interface Snap {
            nodeMs: Record<string, NonNullable<Pct>>;
            wakeLagMs: NonNullable<Pct>;
            sums: Record<string, number>;
        }
        const stats = (snap.data as Snap | undefined) ?? null;
        const nodeMs: Record<string, Pct> = {};
        for (const [type, p] of Object.entries(stats?.nodeMs ?? {})) nodeMs[type] = pct(p);
        const latencyMs: Record<string, Pct> = {};
        for (const [t, s] of latency) latencyMs[t] = pct(s.percentiles());
        const observedMs: Record<string, Pct> = {};
        for (const [t, s] of observed) observedMs[t] = pct(s.percentiles());
        const wallS = (arrivalsMs + drainMs) / 1000;
        const errorTotal = [...errors.values()].reduce((a, b) => a + b, 0);

        return {
            runId,
            mode: 'workflow',
            target,
            tag,
            rate,
            arrival,
            mix,
            knobs: { ...knobs, maxInflight, pollMs, signalDelayMs, signalSkipRatio },
            durationMs: Math.round(arrivalsMs),
            drainMs: Math.round(drainMs),
            started: counts.started,
            startFailures: counts.startFailures,
            startsDeferred: counts.startsDeferred,
            deferredMs: pct(deferredMs.percentiles()),
            startMs: pct(startLatency.percentiles()),
            completed: counts.completed,
            failed: counts.failed,
            failedByError,
            startFailedButRan: counts.startFailedButRan ?? 0,
            compensated: counts.compensated,
            cancelled: counts.cancelled,
            childRuns: counts.childEvents,
            completedUnreported,
            droppedEvents: counts.droppedEvents,
            unknownEvents: counts.unknownEvents,
            sweepUnpollable,
            stuck,
            runsStartedPerSec: round(counts.started / (arrivalsMs / 1000)),
            runsCompletedPerSec: round(counts.completed / wallS),
            byTemplate,
            latencyMs,
            observedMs,
            nodeMs,
            wakeLagMs: pct(stats?.wakeLagMs),
            transitions: stats?.sums?.transitions ?? null,
            transitionsPerSec: stats ? round(stats.sums.transitions / wallS) : null,
            timersFired: stats?.sums?.timers ?? null,
            remindersFired: stats?.sums?.reminders ?? null,
            wakesFallback: stats?.sums?.fallback ?? null,
            wakesLost: stats?.sums?.lost ?? null,
            wakesStale: stats?.sums?.stale ?? null,
            signalsSent: counts.signalsSent,
            signalsSkipped: counts.signalsSkipped,
            signalFailures: counts.signalFailures,
            signalsDelivered: stats?.sums?.signalsDelivered ?? null,
            signalsBuffered: stats?.sums?.signalsBuffered ?? null,
            signalsLate: stats?.sums?.signalsLate ?? null,
            signalTimeouts: stats?.sums?.signalTimeouts ?? null,
            taskAttempts: stats?.sums?.attempts ?? null,
            taskFailures: stats?.sums?.failures ?? null,
            compensations: stats?.sums?.compensations ?? null,
            errors: { total: errorTotal, byKind: Object.fromEntries(errors) }
        };
    }
}
