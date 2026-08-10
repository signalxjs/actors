/**
 * The per-principal watch split (#121), as invariants — the laptop-priced
 * counterpart of Tier 3's `sockets/principal-cliff` (#180).
 *
 * A live read that consults `ctx.principal` gets one watch loop PER
 * distinct identity; every loop's seed and every re-read is a serialized
 * turn on the actor's single queue. The costs that collapsed the AKS rig
 * between 100 and 250 identities are therefore COUNTS that hold by
 * construction here: P identities cost exactly P loops, P seed turns, and
 * P read turns per publish, while the same P identities watching an
 * identity-blind read cost exactly 1 of each. All of those gate `exact` —
 * the establishment fix #180 calls for moves `seed_turns` and
 * `read_turns_per_publish` down, and this scenario is what shows it (and
 * what catches a regression quietly re-splitting or re-serializing).
 *
 * Establishment latency under load rides along informationally: one
 * process, one CPU, so it describes the mechanism (a new identity's seed
 * queues behind O(P) re-reads), not a capacity.
 *
 * `throttleMs: 0` deliberately — at the client-fixed 50 ms every count
 * would acquire a scheduler dependency and stop being deterministic by
 * construction. Establishment is sequential and each initial value is
 * awaited, so #121's discovery settles on the first seed and every later
 * identity resolves straight to a qualified key.
 */
import { defineActor } from '@sigx/actors';
import type { ActorCallContext } from '@sigx/actors/host';
import { createBenchHost } from '../host-fixture.ts';
import type { Metric, RunContext, Scenario } from '../types.ts';

const PrincipalCounter = defineActor({
    type: 'PrincipalCounter',
    allowAnonymous: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        /** Identity-blind: every subscriber shares one loop. */
        current() {
            return ctx.state.count;
        },
        /** Consults the principal: one loop per distinct identity (#121). */
        mine() {
            const me = (ctx.principal as { id: string } | null)?.id ?? 'nobody';
            return `${me}:${ctx.state.count}`;
        },
        async increment() {
            ctx.state.count += 1;
            await ctx.save();
            return ctx.state.count;
        }
    })
});

const SIZES = [1, 25, 100] as const;
const QUICK_SIZES = [1, 25] as const;
/** Publish rounds per size — enough for a median and a spread. */
const ROUNDS = 10;

function callAs(id: string, signal: AbortSignal): ActorCallContext {
    return { callChain: [], callId: `bench-${id}`, principal: id, abortSignal: signal };
}

const principalFanout: Scenario = {
    name: 'live/principal-fanout',
    description: 'watch loops and turns vs distinct principals — the #121 split, counted (#180)',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        const sizes = ctx.quick ? QUICK_SIZES : SIZES;
        for (const p of sizes) {
            const fixture = await createBenchHost({ actors: [PrincipalCounter] });
            try {
                const host = fixture.host;
                const ref = { type: 'PrincipalCounter', key: 'split' } as const;
                let reads = 0;
                const unobserve = host.observeTurns((_ref, method) => {
                    if (method === 'mine') reads += 1;
                });

                const abort = new AbortController();
                const writer = callAs('writer', abort.signal);

                // --- Establishment: P distinct identities, sequential,
                // each initial value awaited (see header).
                const iterators: AsyncIterator<unknown>[] = [];
                for (let i = 0; i < p; i++) {
                    const iterator = host
                        .dispatchWatch!(ref, 'mine', [], callAs(`u${i}`, abort.signal), {
                            throttleMs: 0
                        })
                        [Symbol.asyncIterator]();
                    iterators.push(iterator);
                    await iterator.next();
                }

                metrics.push(
                    {
                        name: `p=${p}/watch_loops`,
                        value: host.stats().watchLoops ?? -1,
                        unit: 'count',
                        direction: 'lower',
                        // The key is a pure function of (method, throttle,
                        // encoded args, encoded principal) — no RNG, no
                        // clock, no host ids — and discovery settled before
                        // subscriber 2 joined. One loop per identity: the
                        // draining base entry plus P−1 qualified (#121's
                        // immutable-key trade-off).
                        exact: true
                    },
                    {
                        name: `p=${p}/seed_turns`,
                        value: reads,
                        unit: 'turns',
                        direction: 'lower',
                        // One seed invoke per created loop, over a fully
                        // awaited sequence. P today; the #180 establishment
                        // fix moves how these are SCHEDULED, and any change
                        // to how many run lands here.
                        exact: true
                    }
                );

                // --- Steady state: publish, then drain one value from
                // every subscriber — the deterministic completion barrier
                // (throttle 0: each dirty loop re-reads exactly once).
                const perPublish: number[] = [];
                for (let round = 0; round < ROUNDS; round++) {
                    reads = 0;
                    await host.dispatch(ref, 'increment', [], writer);
                    for (const iterator of iterators) await iterator.next();
                    perPublish.push(reads);
                }
                perPublish.sort((a, b) => a - b);
                metrics.push(
                    {
                        name: `p=${p}/read_turns_per_publish`,
                        value: perPublish[Math.floor(perPublish.length / 2)] as number,
                        unit: 'turns',
                        direction: 'lower',
                        // One re-read per loop per mutation, barriered by
                        // the drain — no timers, no background work under
                        // the manual scheduler. THIS is the O(P) the #180
                        // fix exists to amortize.
                        exact: true
                    },
                    {
                        // Guards the claim that the median is deterministic:
                        // the day this is non-zero, the metric above stops
                        // being safe to gate on and we must hear about it
                        // (the `microtask_turns_spread` precedent).
                        name: `p=${p}/read_turns_per_publish_spread`,
                        value:
                            (perPublish[perPublish.length - 1] as number) -
                            (perPublish[0] as number),
                        unit: 'turns',
                        direction: 'lower',
                        exact: true,
                        noiseFloor: 0.5
                    }
                );

                // --- Establishment under load, informational: a NEW
                // identity's seed queues behind the P re-reads a publish
                // just requested — the serialized-establishment mechanism
                // that collapsed the cluster, timed on one CPU. The publish
                // is awaited and the pumps given their microtask hops
                // first, so every loop has ENQUEUED its re-read before the
                // late subscriber's seed joins the queue.
                await host.dispatch(ref, 'increment', [], writer);
                for (let hop = 0; hop < 10; hop++) await Promise.resolve();
                const late = host
                    .dispatchWatch!(ref, 'mine', [], callAs('late', abort.signal), {
                        throttleMs: 0
                    })
                    [Symbol.asyncIterator]();
                const seedStart = performance.now();
                await late.next();
                const seedMs = performance.now() - seedStart;
                for (const iterator of iterators) await iterator.next();
                iterators.push(late);
                metrics.push({
                    name: `p=${p}/seed_latency_under_load_ms`,
                    value: Math.round(seedMs * 1000) / 1000,
                    unit: 'ms',
                    direction: 'lower',
                    informational: true
                });

                // --- Throughput, informational: publish+drain round trips.
                const sliceMs = Math.max(200, Math.min(1000, ctx.durationMs / 8));
                const until = performance.now() + sliceMs;
                let publishes = 0;
                while (performance.now() < until) {
                    await host.dispatch(ref, 'increment', [], writer);
                    for (const iterator of iterators) await iterator.next();
                    publishes += 1;
                }
                metrics.push({
                    name: `p=${p}/publishes_per_sec`,
                    value: Math.round((publishes / sliceMs) * 1000),
                    unit: 'ops/s',
                    direction: 'higher',
                    informational: true
                });

                unobserve();
                abort.abort();
            } finally {
                await fixture.stop();
            }
        }

        // --- The control arm: the SAME distinct identities watching the
        // identity-blind read. One variable changes (does the read touch
        // `ctx.principal`), and the counts collapse to 1 — the pair that
        // makes this scenario demonstrate #180 rather than just count.
        const p = sizes[sizes.length - 1] as number;
        const fixture = await createBenchHost({ actors: [PrincipalCounter] });
        try {
            const host = fixture.host;
            const ref = { type: 'PrincipalCounter', key: 'shared' } as const;
            let reads = 0;
            const unobserve = host.observeTurns((_ref, method) => {
                if (method === 'current') reads += 1;
            });
            const abort = new AbortController();
            const writer = callAs('writer', abort.signal);

            const iterators: AsyncIterator<unknown>[] = [];
            for (let i = 0; i < p; i++) {
                const iterator = host
                    .dispatchWatch!(ref, 'current', [], callAs(`u${i}`, abort.signal), {
                        throttleMs: 0
                    })
                    [Symbol.asyncIterator]();
                iterators.push(iterator);
                await iterator.next();
            }

            reads = 0;
            await host.dispatch(ref, 'increment', [], writer);
            for (const iterator of iterators) await iterator.next();

            metrics.push(
                {
                    // `anon=` (not a bare `anonymous/` prefix): metric names
                    // shaped like scenario names confuse the gate audit in
                    // `markdown.test.ts`, which extracts scenario names by
                    // regex from these sources.
                    name: `anon=${p}/watch_loops`,
                    value: host.stats().watchLoops ?? -1,
                    unit: 'count',
                    direction: 'lower',
                    // Never observed consulting the principal, so every
                    // identity shares the single base-key loop.
                    exact: true
                },
                {
                    name: `anon=${p}/read_turns_per_publish`,
                    value: reads,
                    unit: 'turns',
                    direction: 'lower',
                    // One shared loop, one re-read, however many identities
                    // subscribe.
                    exact: true
                }
            );

            unobserve();
            abort.abort();
        } finally {
            await fixture.stop();
        }
        return metrics;
    }
};

export const livePrincipalScenarios: Scenario[] = [principalFanout];
