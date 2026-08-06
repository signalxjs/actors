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
 * So these scenarios are sized by payload, not by call count: tiny vs
 * ~200-row state on the same call shape isolates how much of the cost is
 * per-call fixed overhead and how much scales with the state.
 */
import { Large, Tiny, WriteBehind } from '../actors.ts';
import { closedLoop, LATENCY_NOISE_FLOOR_MS, sweepConcurrency } from '../loop.ts';
import { benchCall, createBenchHost, requireStreamDispatch } from '../host-fixture.ts';
import type { Metric, RunContext, Scenario } from '../types.ts';

const CONCURRENCIES = [1, 64] as const;

/** How long teardown may take before we call the stream path broken. */
const TEARDOWN_TIMEOUT_MS = 2000;

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
            const controller = new AbortController();
            try {
                const ref = { type: Tiny.type, key: 'streamed' };
                const call = benchCall();
                await fixture.host.dispatch(ref, 'increment', [1], call);

                // Drain each subscriber in the background: an unconsumed
                // change feed fills its 16-slot buffer and starts dropping,
                // which would silently stop measuring the fan-out.
                const drains: Promise<void>[] = [];
                const openStream = requireStreamDispatch(fixture.host);
                for (let i = 0; i < subscribers; i++) {
                    const stream = openStream(
                        ref,
                        'watch',
                        [],
                        benchCall({ abortSignal: controller.signal })
                    );
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

                const outcome = await closedLoop({
                    call: () => fixture.host.dispatch(ref, 'increment', [1], call),
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

                // Unwinding the subscribers needs care. A consumer parked in
                // ctx.changes() is waiting for the NEXT change, and abort
                // alone does not wake it — so abort, then push
                // one more mutation to wake each consumer, which then sees
                // the aborted signal and breaks, letting `for await` call
                // return() on the generator.
                //
                // If that does not happen we must FAIL, not carry on: the
                // scenario runs again every round, and iterators left parked
                // on a host we are about to drop would contaminate every
                // later measurement with work nobody is accounting for.
                controller.abort();
                await fixture.host.dispatch(ref, 'increment', [1], call);
                if (!(await settledWithin(drains, TEARDOWN_TIMEOUT_MS))) {
                    throw new Error(
                        `${subscribers} change-feed consumer(s) did not unwind within ` +
                            `${TEARDOWN_TIMEOUT_MS}ms after abort + a wake-up mutation — ` +
                            `ctx.changes() is not releasing parked consumers.`
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

export const stateScenarios: Scenario[] = [explicitSave, writeBehind, changesFanout];
