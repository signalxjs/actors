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
import {
    Growing,
    GrowingSaver,
    Large,
    makeTrackedActor,
    makeWatchableActor,
    Tiny,
    WriteBehind
} from '../actors.ts';
import {
    closedLoop,
    LATENCY_NOISE_FLOOR_MS,
    meanUs,
    sweepConcurrency,
    TURN_NOISE_FLOOR_US
} from '../loop.ts';
import { benchCall, createBenchHost, stringifyStorage, textStorage } from '../host-fixture.ts';
import { settleGc } from '../memory.ts';
import {
    openSubscribers,
    settledWithin,
    TEARDOWN_TIMEOUT_MS,
    type Subscribers
} from '../subscribers.ts';
import type { ActorStorage } from '@sigx/actors/host';
import type { Metric, RunContext, Scenario } from '../types.ts';

const CONCURRENCIES = [1, 64] as const;

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

/**
 * The reporter's actual save shape from #124 (#227): growing state × one
 * DURABLE save per turn — `job.checkpoint()` in a loop. `state/dirty-growth`
 * deliberately never saves (it prices the walk + snapshot); this one prices
 * the codec on the save path, attributed across three arms that differ in
 * exactly one variable each:
 *
 *   mem        explicit persistence, no subscriber, `memoryStorage`: no
 *              tracking is installed, so a turn is body + ONE
 *              `encodeWithHandlers` walk + a by-reference store.
 *   stringify  same, `stringifyStorage`: the delta vs `mem` is the second
 *              full walk (`JSON.stringify`) every real adapter pays.
 *   text       same, `textStorage`: the adapter opts into `saveText`, so the
 *              host emits the JSON in ONE walk (#238) and the adapter walks
 *              nothing. `stringify − text` is what that removed; `text − mem`
 *              is what a string still costs over storing the tree by
 *              reference. Reported per round in the same run as `stringify`,
 *              which is the only way to read the pair without trusting two
 *              machines to agree.
 *   mem+sub    `memoryStorage` with one `ctx.changes()` subscriber: the
 *              delta vs `mem` is the deepTrack walk plus the boundary
 *              `#snapshot()` (encode + revive) the subscriber forces.
 *
 * All four are O(state size) per turn; what the arms buy is knowing which
 * term dominates — the fix for each lives in a different place (the host,
 * an upstream serialize API, the change feed).
 */
const saveGrowth: Scenario = {
    name: 'state/save-growth',
    description: 'per-turn cost of append + ctx.save() as state grows — by storage and subscriber',
    async run(ctx: RunContext): Promise<Metric[]> {
        const steps = ctx.quick ? 100 : GROWTH_STEPS;
        const window = ctx.quick ? 20 : GROWTH_WINDOW;
        const metrics: Metric[] = [];
        // Storage constructed per arm, inside run(): a scenario re-runs
        // every round and must not accumulate state across rounds.
        const arms: { label: string; storage?: () => ActorStorage; subscribers: number }[] = [
            { label: 'mem', subscribers: 0 },
            { label: 'stringify', storage: stringifyStorage, subscribers: 0 },
            { label: 'text', storage: textStorage, subscribers: 0 },
            { label: 'mem+sub', subscribers: 1 }
        ];
        for (const arm of arms) {
            const fixture = await createBenchHost({
                actors: [GrowingSaver],
                ...(arm.storage ? { storage: arm.storage() } : {})
            });
            const ref = { type: GrowingSaver.type, key: 'run' };
            const call = benchCall();
            const step = (): Promise<unknown> =>
                fixture.host.dispatch(ref, 'appendStepAndSave', [], call);
            let subs: Subscribers | undefined;
            try {
                subs = openSubscribers(fixture.host, ref, arm.subscribers, step);
                // The arms are read against each other, so each starts from a
                // quiet heap: without this, an arm's head window pays the GC
                // debt of whichever arm ran before it — measured at 3× on
                // `mem/head_turn_us`, which runs right after `mem+sub`'s
                // 500-row trees become garbage.
                await settleGc();
                const timings: number[] = [];
                for (let i = 0; i < steps; i++) {
                    const timed = i < window || i >= steps - window;
                    if (!timed) {
                        await step();
                        continue;
                    }
                    const t0 = performance.now();
                    await step();
                    timings.push(performance.now() - t0);
                }
                metrics.push(
                    {
                        name: `${arm.label}/head_turn_us`,
                        value: meanUs(timings.slice(0, window)),
                        unit: 'µs',
                        direction: 'lower',
                        noiseFloor: TURN_NOISE_FLOOR_US
                    },
                    {
                        name: `${arm.label}/tail_turn_us`,
                        value: meanUs(timings.slice(-window)),
                        unit: 'µs',
                        direction: 'lower',
                        noiseFloor: TURN_NOISE_FLOOR_US
                    }
                );
                await subs.unwind();
            } finally {
                subs?.abort();
                await fixture.stop();
            }
        }
        return metrics;
    }
};

const WATCHABLE = ROW_LADDER.map((rows) => ({ rows, def: makeWatchableActor(rows) }));

/**
 * What a `$live` watch costs, by state size (#129).
 *
 * This is the path `useActorState(…, { live: true })` and every wire watch
 * take, and it is NOT `streams/changes-fanout`: a watch re-invokes a read
 * method on change rather than consuming the state, so `createSharedWatch`'s
 * pump reads `{ done }` off the feed and never touches the value.
 *
 * Which is the point of measuring it against `state/dirty-size`'s subs=1 arm:
 * a subscriber that discards the snapshot should not pay for one. `throttleMs
 * 0` so the number is the per-turn cost rather than the throttle's.
 */
const liveWatch: Scenario = {
    name: 'streams/live-watch',
    description: 'mutating turns under one $live watch (dispatchWatch), by state size',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        for (const { rows, def } of ctx.quick ? WATCHABLE.slice(0, 2) : WATCHABLE) {
            const fixture = await createBenchHost({ actors: [def] });
            const controller = new AbortController();
            const watchRef = { type: def.type, key: 'watched' };
            const call = benchCall();
            const bump = (): Promise<unknown> =>
                fixture.host.dispatch(watchRef, 'increment', [1], call);
            let drain: Promise<void> | undefined;
            try {
                await bump();
                const watch = fixture.host.dispatchWatch?.(
                    watchRef,
                    'total',
                    [],
                    benchCall({ abortSignal: controller.signal }),
                    { throttleMs: 0 }
                );
                if (!watch) throw new Error('host.dispatchWatch is missing — no $live path to measure.');
                drain = (async () => {
                    try {
                        for await (const _ of watch) {
                            if (controller.signal.aborted) break;
                        }
                    } catch {
                        // Aborted at teardown — expected.
                    }
                })();

                const outcome = await closedLoop({
                    call: bump,
                    concurrency: 1,
                    durationMs: ctx.durationMs,
                    latency: false
                });
                metrics.push({
                    name: `rows=${rows}/ops_per_sec`,
                    value: outcome.opsPerSec,
                    unit: 'ops/s',
                    direction: 'higher'
                });

                controller.abort();
                await bump();
                if (!(await settledWithin([drain], TEARDOWN_TIMEOUT_MS))) {
                    throw new Error(
                        `the $live watch consumer did not unwind within ${TEARDOWN_TIMEOUT_MS}ms ` +
                            `after abort + a wake-up mutation.`
                    );
                }
            } finally {
                controller.abort();
                await fixture.stop();
            }
        }
        return metrics;
    }
};

export const stateScenarios: Scenario[] = [
    explicitSave,
    writeBehind,
    changesFanout,
    dirtySize,
    dirtyGrowth,
    saveGrowth,
    liveWatch
];
