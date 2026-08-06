/**
 * Persistence and the change feed — where the codec walks live.
 *
 * `ctx.save()` encodes the whole state through `@sigx/serialize`;
 * `memoryStorage` stores that tree by reference (the seam's ownership
 * contract, #25) and `structuredClone`s only on the way out at load. The
 * change feed is worse per turn: `#snapshot()` is
 * `revive(encode(raw))`, two full deep walks, computed once per mutating
 * turn as soon as ANYONE is subscribed.
 *
 * So these scenarios are sized by payload, not by call count: holding the
 * call shape fixed and varying the state isolates how much of the cost is
 * per-call overhead and how much scales with the state. `state/explicit-save`
 * does that for the codec (tiny vs ~200 rows); `state/dirty-size` and
 * `state/dirty-growth` do it for the change-tracking walk, which until #124
 * nothing measured at all.
 */
import { Growing, Large, makeTrackedActor, Tiny, WriteBehind } from '../actors.ts';
import { closedLoop, LATENCY_NOISE_FLOOR_MS, sweepConcurrency } from '../loop.ts';
import { benchCall, createBenchHost, requireStreamDispatch } from '../host-fixture.ts';
import type { ActorRef, Host } from '@sigx/actors/host';
import type { Metric, RunContext, Scenario } from '../types.ts';

const CONCURRENCIES = [1, 64] as const;

/** How long teardown may take before we call the stream path broken. */
const TEARDOWN_TIMEOUT_MS = 2000;

/** `LATENCY_NOISE_FLOOR_MS` in the microseconds the per-turn metrics use. */
const TURN_NOISE_FLOOR_US = LATENCY_NOISE_FLOOR_MS * 1000;

/**
 * `true` if every promise settled in time, `false` on timeout. The timer is
 * always cleared — an un-cleared `setTimeout` would hold the event loop open
 * past the end of the run.
 */
async function settledWithin(promises: readonly Promise<unknown>[], ms: number): Promise<boolean> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<false>((resolve) => {
        timer = setTimeout(() => resolve(false), ms);
    });
    try {
        return await Promise.race([Promise.allSettled(promises).then(() => true), timeout]);
    } finally {
        clearTimeout(timer);
    }
}

interface Subscribers {
    /** Teardown-safe abort for a `finally`; never throws, never waits. */
    abort(): void;
    /** Abort, wake the parked consumers, and prove they unwound. */
    unwind(): Promise<void>;
}

/**
 * `unwind()` belongs on the SUCCESS path only, and the `finally` deliberately
 * does not call it.
 *
 * It is an assertion, not cleanup: releasing the consumers is
 * `fixture.stop()`'s job either way — `host.stop()` deactivates, and both
 * `deactivate()` and `forceStop()` call `#closeSubs()`, which marks every sub
 * done and wakes a parked `next()` (`activation.ts:773`, `:786`, `:1850`).
 * What `unwind()` adds is the proof that the runtime releases them from the
 * CONSUMER side, on abort plus a wake-up mutation, without needing the host
 * torn down underneath it.
 *
 * Putting it in `finally` would run that assertion against a host whose
 * measurement has just thrown — and its failure, or a dispatch failure from
 * its own wake-up call, would replace the exception that actually broke the
 * round. Losing the real error to a teardown check is a bad trade.
 */

/**
 * Open `count` change feeds on `ref` and drain each in the background: an
 * unconsumed feed fills its 16-slot buffer and starts dropping, which would
 * silently stop measuring the fan-out.
 *
 * Unwinding them is the fiddly half, which is why this is shared rather than
 * copied per scenario. A consumer parked in `ctx.changes()` is waiting for
 * the NEXT change, and abort alone does not wake it — so `unwind()` aborts,
 * then calls `wake` to push one more mutation, which lets each consumer see
 * the aborted signal, break, and have `for await` call `return()` on the
 * generator.
 *
 * If that does not happen we FAIL rather than carry on: the scenario runs
 * again every round, and iterators left parked on a host we are about to
 * drop would contaminate every later measurement with work nobody is
 * accounting for.
 */
function openSubscribers(
    host: Host,
    ref: ActorRef,
    count: number,
    wake: () => Promise<unknown>
): Subscribers {
    const controller = new AbortController();
    const drains: Promise<void>[] = [];
    if (count > 0) {
        const openStream = requireStreamDispatch(host);
        for (let i = 0; i < count; i++) {
            const stream = openStream(ref, 'watch', [], benchCall({ abortSignal: controller.signal }));
            drains.push(
                (async () => {
                    try {
                        for await (const _ of stream) {
                            if (controller.signal.aborted) break;
                        }
                    } catch {
                        // Aborted at teardown — expected.
                    }
                })()
            );
        }
    }
    return {
        abort: () => controller.abort(),
        async unwind(): Promise<void> {
            controller.abort();
            await wake();
            if (!(await settledWithin(drains, TEARDOWN_TIMEOUT_MS))) {
                throw new Error(
                    `${count} change-feed consumer(s) did not unwind within ` +
                        `${TEARDOWN_TIMEOUT_MS}ms after abort + a wake-up mutation — ` +
                        `ctx.changes() is not releasing parked consumers.`
                );
            }
        }
    };
}

const explicitSave: Scenario = {
    name: 'state/explicit-save',
    description: 'ctx.save() per turn against memoryStorage — codec walk + load-side clone only',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        for (const [label, def] of [
            ['tiny', Tiny],
            ['large', Large]
        ] as const) {
            const fixture = await createBenchHost({ actors: [def] });
            try {
                const ref = { type: def.type, key: 'saver' };
                const call = benchCall();
                await fixture.host.dispatch(ref, 'incrementAndSave', [1], call);
                const outcome = await closedLoop({
                    call: () => fixture.host.dispatch(ref, 'incrementAndSave', [1], call),
                    concurrency: 1,
                    durationMs: ctx.durationMs,
                    latency: true
                });
                metrics.push(
                    {
                        name: `${label}/saves_per_sec`,
                        value: outcome.opsPerSec,
                        unit: 'ops/s',
                        direction: 'higher'
                    },
                    {
                        name: `${label}/p99_ms`,
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
 * Write-behind installs a deep watch on state, so every mutating turn pays
 * the watcher and schedules a debounced flush. Compared against
 * `dispatch/warm-actor`'s `increment`, the gap is what the watch costs;
 * compared against `state/explicit-save`, it is what coalescing buys.
 */
const writeBehind: Scenario = {
    name: 'state/write-behind',
    description: 'mutating turns under write-behind persistence (deep watch + debounced flush)',
    async run(ctx: RunContext): Promise<Metric[]> {
        const fixture = await createBenchHost({ actors: [WriteBehind] });
        try {
            const ref = { type: WriteBehind.type, key: 'wb' };
            const call = benchCall();
            await fixture.host.dispatch(ref, 'increment', [1], call);
            return await sweepConcurrency({
                call: () => fixture.host.dispatch(ref, 'increment', [1], call),
                concurrencies: CONCURRENCIES,
                durationMs: ctx.durationMs
            });
        } finally {
            await fixture.stop();
        }
    }
};

/**
 * The change feed's cost is per TURN and per SUBSCRIBER-set, not per
 * subscriber: one snapshot is computed and pushed to all of them. Sweeping
 * K is how we confirm that — if throughput falls off with K, the snapshot
 * is not actually being shared.
 */
const changesFanout: Scenario = {
    name: 'streams/changes-fanout',
    description: 'mutating turns with K live ctx.changes() subscribers (K = 0, 1, 16)',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        for (const subscribers of [0, 1, 16]) {
            const fixture = await createBenchHost({ actors: [Tiny] });
            const ref = { type: Tiny.type, key: 'streamed' };
            const call = benchCall();
            const bump = (): Promise<unknown> =>
                fixture.host.dispatch(ref, 'increment', [1], call);
            let subs: Subscribers | undefined;
            try {
                await bump();
                subs = openSubscribers(fixture.host, ref, subscribers, bump);

                const outcome = await closedLoop({
                    call: bump,
                    concurrency: 1,
                    durationMs: ctx.durationMs,
                    latency: false
                });
                metrics.push({
                    name: `subs=${subscribers}/ops_per_sec`,
                    value: outcome.opsPerSec,
                    unit: 'ops/s',
                    direction: 'higher'
                });
                await subs.unwind();
            } finally {
                subs?.abort();
                await fixture.stop();
            }
        }
        return metrics;
    }
};

/**
 * The ladder for #124. Built once at module scope rather than per round:
 * each row count is one actor `type`, and a scenario's `run()` is called
 * again for every round.
 */
const ROW_LADDER = [0, 200, 2000] as const;
const TRACKED = ROW_LADDER.map((rows) => ({ rows, def: makeTrackedActor(rows) }));

/**
 * What change tracking costs as state GROWS (#124).
 *
 * The reported pathology is that the per-boundary cost scales with the size
 * of the state object rather than the size of the change, and nothing in
 * this suite measured that: `state/write-behind` and `streams/changes-fanout`
 * both pin state at `{ count: 0 }`, and `Large` is only ever exercised
 * through `save()` and heap footprint — never through the tracking walk.
 *
 * Two arms per size, and the DELTA between them is the point. Both use
 * identical write-behind actors whose flush never fires, so:
 *
 *   subs=0  the deep tracking walk alone
 *   subs=1  the walk PLUS `#snapshot()`, a full encode+revive of the state
 *
 * Those are two separate O(state-size) costs on the same boundary, and only
 * the first is what an upstream deep-tracking primitive can fix — which is
 * exactly why they are measured apart.
 */
const dirtySize: Scenario = {
    name: 'state/dirty-size',
    description: 'mutating turns vs state SIZE, with and without a change subscriber',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        const ladder = ctx.quick ? TRACKED.slice(0, 2) : TRACKED;
        for (const { rows, def } of ladder) {
            for (const subscribers of [0, 1]) {
                const fixture = await createBenchHost({ actors: [def] });
                const ref = { type: def.type, key: 'tracked' };
                const call = benchCall();
                const bump = (): Promise<unknown> =>
                    fixture.host.dispatch(ref, 'increment', [1], call);
                let subs: Subscribers | undefined;
                try {
                    await bump();
                    subs = openSubscribers(fixture.host, ref, subscribers, bump);
                    const outcome = await closedLoop({
                        call: bump,
                        concurrency: 1,
                        durationMs: ctx.durationMs,
                        latency: false
                    });
                    metrics.push({
                        name: `rows=${rows}/subs=${subscribers}/ops_per_sec`,
                        value: outcome.opsPerSec,
                        unit: 'ops/s',
                        direction: 'higher'
                    });
                    await subs.unwind();
                } finally {
                    subs?.abort();
                    await fixture.stop();
                }
            }
        }
        return metrics;
    }
};

/**
 * Turns timed at each end of a growth run; the middle is run untimed.
 *
 * A fifth of the run at each end, rather than a handful of turns: `head` is
 * a small number and a narrow window put it at ±56% spread on a loaded
 * machine, which is a gated metric that cries wolf. 100 samples an end costs
 * nothing extra — the turns run either way — and is worth more than the
 * wider head/tail separation a narrower window would buy.
 */
const GROWTH_STEPS = 500;
const GROWTH_WINDOW = 100;

/** Mean of a window of per-turn timings, in microseconds. */
function meanUs(ms: readonly number[]): number {
    return (ms.reduce((sum, v) => sum + v, 0) / ms.length) * 1000;
}

/**
 * The shape #124 was actually reported against: a job actor whose state
 * accumulates a step's output per turn. Per-step cost was measured drifting
 * from ~13 ms to ~33 ms across a 50-step run — tracking the growth of the
 * graph, not anything the steps do.
 *
 * `head` is the first window of turns (near-empty state), `tail` the last
 * (near-full). `growth_ratio` between them is the number that has to
 * collapse, and it is INFORMATIONAL: it is a quotient of two measurements
 * on the same machine, so it inherits both their errors, and `head` — a
 * handful of microseconds — carries most of that. The two absolute figures
 * are what gate; the ratio is there to be read.
 */
const dirtyGrowth: Scenario = {
    name: 'state/dirty-growth',
    description: 'per-turn cost as state accumulates — a job actor appending a step per turn',
    async run(ctx: RunContext): Promise<Metric[]> {
        const steps = ctx.quick ? 100 : GROWTH_STEPS;
        const window = ctx.quick ? 20 : GROWTH_WINDOW;
        const fixture = await createBenchHost({ actors: [Growing] });
        const ref = { type: Growing.type, key: 'run' };
        const call = benchCall();
        const step = (): Promise<unknown> => fixture.host.dispatch(ref, 'appendStep', [], call);
        let subs: Subscribers | undefined;
        try {
            // Subscribed BEFORE the first step, so tracking is installed for
            // the whole run — a `defineJob` actor is explicit-persistence and
            // pays nothing until someone opens `job.watch()`.
            subs = openSubscribers(fixture.host, ref, 1, step);
            const timings: number[] = [];
            for (let i = 0; i < steps; i++) {
                // Only the two windows are timed: a clock read per turn is
                // real distortion at head-window sizes.
                const timed = i < window || i >= steps - window;
                if (!timed) {
                    await step();
                    continue;
                }
                const t0 = performance.now();
                await step();
                timings.push(performance.now() - t0);
            }
            const head = meanUs(timings.slice(0, window));
            const tail = meanUs(timings.slice(-window));
            await subs.unwind();
            return [
                {
                    name: 'head/turn_us',
                    value: head,
                    unit: 'µs',
                    direction: 'lower',
                    noiseFloor: TURN_NOISE_FLOOR_US
                },
                {
                    name: 'tail/turn_us',
                    value: tail,
                    unit: 'µs',
                    direction: 'lower',
                    noiseFloor: TURN_NOISE_FLOOR_US
                },
                {
                    name: 'growth_ratio',
                    value: tail / head,
                    unit: 'x',
                    direction: 'lower',
                    informational: true
                }
            ];
        } finally {
            subs?.abort();
            await fixture.stop();
        }
    }
};

export const stateScenarios: Scenario[] = [
    explicitSave,
    writeBehind,
    changesFanout,
    dirtySize,
    dirtyGrowth
];
