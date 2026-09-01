/**
 * The job path's serialize costs (#227, the measurement half of #124's
 * remaining report).
 *
 * `jobs/status-read` prices the JobInfo clone: every job read runs
 * `toInfo(ctx.snapshot())` — a full encode+revive of the whole state,
 * checkpoint and result included, to build 8 fields that exclude both. The
 * ladder should fall roughly linearly with checkpoint size today; a fix
 * that builds `JobInfo` without the whole-state clone flattens it.
 *
 * `jobs/checkpoint-growth` is the reporter's composite, end to end on a
 * real `defineJob`: a growing checkpoint, one durable save per step, with
 * and without a `job.watch()` consumer. The `watch=1 − watch=0` delta is
 * the change-feed term (boundary snapshot + `toInfo` per emission); the
 * `watch=0` curve is the save encode alone, to be read against
 * `state/save-growth`'s `mem` arm.
 */
import {
    closedLoop,
    LATENCY_NOISE_FLOOR_MS,
    meanUs,
    sweepConcurrency,
    TURN_NOISE_FLOOR_US
} from '../loop.ts';
import {
    benchCall,
    countingStorage,
    createBenchHost,
    stringifyStorage,
    textStorage
} from '../host-fixture.ts';
import { settleGc } from '../memory.ts';
import { openSubscribers, type Subscribers } from '../subscribers.ts';
import {
    BenchLifecycleJob,
    BenchParkedJob,
    BenchStatusJob,
    BenchStepJob,
    resetStepChannel,
    sendStep,
    sendStop,
    settledLifecycle
} from '../job-fixtures.ts';
import { memoryStorage } from '@sigx/actors/host';
import type { ActorRef, ActorStorage, Host } from '@sigx/actors/host';
import type { Metric, RunContext, Scenario } from '../types.ts';

/** Same rungs as `state/dirty-size`, so the two ladders read together. */
const ROWS_LADDER = [0, 200, 2000] as const;

/**
 * `start()` resolves when the run is durably recorded and LAUNCHED, not
 * when it has done anything — the detached body still has to reach its
 * first checkpoint. Poll with a real timer: `manualScheduler` governs the
 * host's background work, not the event loop.
 */
async function waitForStatus(host: Host, ref: ActorRef, wanted: string): Promise<void> {
    const call = benchCall();
    for (let i = 0; i < 5000; i++) {
        const info = (await host.dispatch(ref, 'status', [], call)) as { status: string };
        if (info.status === wanted) return;
        await new Promise((resolve) => setTimeout(resolve, 1));
    }
    throw new Error(`job ${ref.type}/${ref.key} never reached status "${wanted}".`);
}

const statusRead: Scenario = {
    name: 'jobs/status-read',
    description: 'status() closed loop over a paused job, by checkpoint size — the JobInfo clone',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        const ladder = ctx.quick ? ROWS_LADDER.slice(0, 2) : ROWS_LADDER;
        for (const rows of ladder) {
            const fixture = await createBenchHost({ actors: [BenchStatusJob] });
            try {
                const ref = { type: BenchStatusJob.type, key: `rows${rows}` };
                const call = benchCall();
                await fixture.host.dispatch(ref, 'start', [rows], call);
                await waitForStatus(fixture.host, ref, 'paused');
                // Rungs are read against each other — start each from a quiet
                // heap rather than under the previous rung's GC debt.
                await settleGc();
                const outcome = await closedLoop({
                    call: () => fixture.host.dispatch(ref, 'status', [], call),
                    concurrency: 1,
                    durationMs: ctx.durationMs,
                    latency: true
                });
                metrics.push(
                    {
                        name: `rows=${rows}/status_per_sec`,
                        value: outcome.opsPerSec,
                        unit: 'ops/s',
                        direction: 'higher'
                    },
                    {
                        name: `rows=${rows}/p99_ms`,
                        value: outcome.percentiles!.p99,
                        unit: 'ms',
                        direction: 'lower',
                        noiseFloor: LATENCY_NOISE_FLOOR_MS
                    }
                );
            } finally {
                await fixture.stop();
            }
        }
        return metrics;
    }
};

/**
 * 300 steps rather than `state/dirty-growth`'s 500: every step here is a
 * durable save plus the command-channel round trip, so the run costs more
 * per step and the head/tail contrast needs less length to show.
 */
const CHECKPOINT_STEPS = 300;
const CHECKPOINT_WINDOW = 60;

/** Keys are minted per arm per round — a channel must never be reused. */
let runSeq = 0;

const checkpointGrowth: Scenario = {
    name: 'jobs/checkpoint-growth',
    description: 'per-step cost of a checkpointing job as state grows — unwatched, watched, throttled',
    async run(ctx: RunContext): Promise<Metric[]> {
        const steps = ctx.quick ? 60 : CHECKPOINT_STEPS;
        const window = ctx.quick ? 15 : CHECKPOINT_WINDOW;
        const metrics: Metric[] = [];
        // `throttled` is `watch=1` with `watch({ throttleMs })` (#231).
        // Under `manualScheduler` the window never closes mid-run, so the
        // arm measures the throttle's FLOOR — a burst entirely inside one
        // window: the leading snapshot and nothing else. The gap between
        // it and `watch=1` is exactly what opting in buys such a burst.
        // `watch=0,text` is `watch=0` against a storage that takes JSON text
        // (#238), which is the shape a real deployment has: pg, redis and
        // surreal all want the string. Its delta vs `watch=0,stringify` is
        // the adapter walk removed, on the checkpointing-job workload the
        // whole #124 thread was reported against. Both new arms are
        // UNWATCHED, so they read against `watch=0` and against each other
        // without the change feed in the way.
        const arms = [
            { label: 'watch=0', count: 0, args: [] as unknown[], storage: undefined },
            { label: 'watch=0,stringify', count: 0, args: [] as unknown[], storage: stringifyStorage },
            { label: 'watch=0,text', count: 0, args: [] as unknown[], storage: textStorage },
            { label: 'watch=1', count: 1, args: [] as unknown[], storage: undefined },
            { label: 'watch=throttled', count: 1, args: [{ throttleMs: 1000 }], storage: undefined }
        ] as { label: string; count: number; args: unknown[]; storage?: () => ActorStorage }[];
        for (const arm of arms) {
            const key = `run-${arm.label}-${runSeq++}`;
            const fixture = await createBenchHost({
                actors: [BenchStepJob],
                ...(arm.storage ? { storage: arm.storage() } : {})
            });
            const ref = { type: BenchStepJob.type, key };
            let subs: Subscribers | undefined;
            try {
                await fixture.host.dispatch(ref, 'start', [null], benchCall());
                // Watcher opened BEFORE the first step, so change tracking is
                // installed for the whole run — a job is explicit-persistence
                // and pays nothing until someone opens `watch()`.
                subs = openSubscribers(fixture.host, ref, arm.count, () => sendStep(key), arm.args);
                // The arms are read against each other — same GC hygiene as
                // `state/save-growth`.
                await settleGc();
                const timings: number[] = [];
                for (let i = 0; i < steps; i++) {
                    // Only the two windows are timed — same shape and same
                    // rationale as `state/dirty-growth`.
                    const timed = i < window || i >= steps - window;
                    if (!timed) {
                        await sendStep(key);
                        continue;
                    }
                    const t0 = performance.now();
                    await sendStep(key);
                    timings.push(performance.now() - t0);
                }
                metrics.push(
                    {
                        name: `${arm.label}/head_step_us`,
                        value: meanUs(timings.slice(0, window)),
                        unit: 'µs',
                        direction: 'lower',
                        noiseFloor: TURN_NOISE_FLOOR_US
                    },
                    {
                        name: `${arm.label}/tail_step_us`,
                        value: meanUs(timings.slice(-window)),
                        unit: 'µs',
                        direction: 'lower',
                        noiseFloor: TURN_NOISE_FLOOR_US
                    }
                );
                // A throttled consumer's wake-up mutation is DEFERRED into
                // the open window, so the unwind dance needs the window
                // closed between the abort and the assertion: abort, park
                // one more boundary, close the window (the trailing emit is
                // the wake-up), THEN prove the consumers released.
                if (arm.args.length > 0) {
                    subs.abort();
                    await sendStep(key);
                    fixture.clock.advance(2000);
                }
                await subs.unwind();
            } finally {
                subs?.abort();
                // Always delivered, even after a failure mid-run: a body
                // parked on its command channel would otherwise still be
                // parked while `stop()` deactivates the host.
                sendStop(key);
                await fixture.stop();
                resetStepChannel(key);
            }
        }
        return metrics;
    }
};

/**
 * Runs per exact pass — enough that a per-run integer divides out cleanly
 * and a one-off (the first activation's cold load) cannot hide in it.
 */
const LIFECYCLE_EXACT_RUNS = 25;

/** Keys are minted per fixture per round — a completed job must never be re-started. */
let lifecycleSeq = 0;

/** One whole lifecycle: `start()` → trivial body → terminal, on a fresh key. */
async function runLifecycle(host: Host, key: string): Promise<void> {
    const ref: ActorRef = { type: BenchLifecycleJob.type, key };
    const settled = settledLifecycle(key);
    await host.dispatch(ref, 'start', [null], benchCall());
    await settled;
}

const lifecycle: Scenario = {
    name: 'jobs/lifecycle',
    description: 'start() → terminal on a trivial job, fresh key per run — the bookkeeping a workflow run costs',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        // The exact half first, on its own fixture: N serial runs, then a
        // stop that waits for the detached ledger/reminder clears, then the
        // totals divided out. Every term is deterministic under serial
        // dispatch on one host — the runtime asks the store for the same
        // things on any machine — so these gate (#307). A fix that folds a
        // round trip away LOWERS them and reads `improved`.
        {
            const counted = countingStorage(memoryStorage());
            const fixture = await createBenchHost({
                actors: [BenchLifecycleJob],
                storage: counted.storage
            });
            try {
                for (let i = 0; i < LIFECYCLE_EXACT_RUNS; i++) {
                    await runLifecycle(fixture.host, `exact-${lifecycleSeq++}`);
                }
                // The ledger and reminder clears run DETACHED after
                // `onSettled`, and `host.stop()` does not wait for them: the
                // run leaves the task table before its bookkeeping finishes,
                // so the deactivation grace has nothing to await. On memory
                // storage every step is a microtask, so one macrotask turn
                // drains the last run's tail deterministically — without it
                // the totals come up exactly one clear short.
                await new Promise((resolve) => setImmediate(resolve));
            } finally {
                await fixture.stop();
            }
            const perRun = (n: number): number => n / LIFECYCLE_EXACT_RUNS;
            metrics.push(
                {
                    name: 'storage_loads_per_run',
                    value: perRun(counted.counts.loads),
                    unit: 'ops',
                    direction: 'lower',
                    exact: true
                },
                {
                    name: 'storage_saves_per_run',
                    value: perRun(counted.counts.saves),
                    unit: 'ops',
                    direction: 'lower',
                    exact: true
                },
                {
                    name: 'storage_clears_per_run',
                    value: perRun(counted.counts.clears),
                    unit: 'ops',
                    direction: 'lower',
                    exact: true
                }
            );
        }
        // The timed half. `mem` is the suite's usual floor; `text` is the
        // shape a real deployment has (pg, redis, surreal all take JSON
        // text) and what the lifecycle costs there in CPU alone — no
        // network, so a round trip is a JSON walk, not an RTT.
        const arms = [
            { label: 'mem', storage: undefined },
            { label: 'text', storage: textStorage }
        ] as { label: string; storage?: () => ActorStorage }[];
        const concurrencies = ctx.quick ? [1, 16] : [1, 16, 64];
        for (const arm of arms) {
            const fixture = await createBenchHost({
                actors: [BenchLifecycleJob],
                ...(arm.storage ? { storage: arm.storage() } : {})
            });
            try {
                await settleGc();
                const swept = await sweepConcurrency({
                    call: () => runLifecycle(fixture.host, `${arm.label}-${lifecycleSeq++}`),
                    concurrencies,
                    durationMs: ctx.durationMs
                });
                for (const m of swept) metrics.push({ ...m, name: `${arm.label}/${m.name}` });
            } finally {
                await fixture.stop();
            }
        }
        return metrics;
    }
};

/**
 * How many jobs to leave running before probing. Two non-zero rungs are
 * enough: the term under test is linear in N, and 5 000 already puts ~300
 * entries into the probe's shard.
 */
const RUNNING_LADDER = [0, 1000, 5000] as const;
/** Probes per rung; only the second half is timed, so the first rung's JIT warm-up is not the number. */
const RUNNING_PROBES = 40;

const manyRunning: Scenario = {
    name: 'jobs/many-running',
    description: 'start() with N jobs already running — the reminder-shard rewrite every start pays',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        const ladder = ctx.quick ? [0, 200] : RUNNING_LADDER;
        for (const n of ladder) {
            const counted = countingStorage(memoryStorage());
            const fixture = await createBenchHost({
                actors: [BenchParkedJob],
                storage: counted.storage
            });
            try {
                // Sequential: an activation storm would measure itself, and
                // every start serializes on the reminder writer chain anyway.
                for (let i = 0; i < n; i++) {
                    await fixture.host.dispatch(
                        { type: BenchParkedJob.type, key: `parked-${i}` },
                        'start',
                        [null],
                        benchCall()
                    );
                }
                await settleGc();
                const timings: number[] = [];
                for (let i = 0; i < RUNNING_PROBES; i++) {
                    const probe: ActorRef = { type: BenchParkedJob.type, key: `probe-${i}` };
                    const t0 = performance.now();
                    await fixture.host.dispatch(probe, 'start', [null], benchCall());
                    timings.push(performance.now() - t0);
                    // Every probe but the last is cancelled again, outside
                    // the timed section, so each start sees exactly `n`
                    // running jobs rather than `n` plus its predecessors.
                    // The cancel's ledger and shard clears run detached
                    // after the method returns; one macrotask drains them
                    // on memory storage (see `jobs/lifecycle`). The last
                    // probe stays, so `reminderWrite()` below is ITS start.
                    if (i < RUNNING_PROBES - 1) {
                        await fixture.host.dispatch(probe, 'cancel', [], benchCall());
                        await new Promise((resolve) => setImmediate(resolve));
                    }
                }
                // The last probe's shard write, decoded HERE — after the
                // timed loop, so the instrumentation is not in `start_us`.
                // Its entry count is a pure function of fixed key strings
                // through FNV-1a — the same shard, the same neighbours, on
                // any machine — so it gates. Bytes carry a wall-clock
                // `nextDue` per entry and stay informational.
                const write = counted.counts.reminderWrite();
                if (!write) throw new Error('a job start wrote no reminder shard — the liveness reminder is missing.');
                metrics.push(
                    {
                        name: `n=${n}/start_us`,
                        value: meanUs(timings.slice(RUNNING_PROBES / 2)),
                        unit: 'µs',
                        direction: 'lower',
                        noiseFloor: TURN_NOISE_FLOOR_US
                    },
                    {
                        name: `n=${n}/shard_entries_per_start`,
                        value: write.entries,
                        unit: 'entries',
                        direction: 'lower',
                        exact: true
                    },
                    {
                        name: `n=${n}/shard_bytes_per_start`,
                        value: write.bytes,
                        unit: 'B',
                        direction: 'lower',
                        informational: true
                    }
                );
            } finally {
                // Aborts every parked body; each returns on its signal and
                // the runtime records nothing for a wind-down.
                await fixture.stop();
            }
        }
        return metrics;
    }
};

export const jobScenarios: Scenario[] = [statusRead, checkpointGrowth, lifecycle, manyRunning];
