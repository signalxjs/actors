/**
 * Does it scale to 100 silos?
 *
 * These scenarios do NOT measure throughput — every silo shares one CPU
 * here, so a 100-silo number would just be 100 silos contending for one
 * core. They measure the **algorithmic shape**: how much work the runtime
 * asks of the cluster providers, and how each per-decision cost grows, as a
 * function of N. That is what decides whether 100 silos on 100 VMs works,
 * and an O(N²) shows up here for free rather than on a bill.
 *
 * Reading them: a metric that stays FLAT as N goes 1 → 100 scales. One that
 * grows with N is a shared bottleneck, and one that grows with N per silo is
 * an O(N²) waiting to happen.
 */
import { defineActor } from '@sigx/actors';
import { consistentHashPolicy, randomPlacementPolicy } from '@sigx/actors/cluster';
import { createCluster, selfPolicy } from '../cluster-harness.ts';
import { benchCall } from '../silo-fixture.ts';
import { closedLoop, LATENCY_NOISE_FLOOR_MS } from '../loop.ts';
import type { Metric, RunContext, Scenario } from '../types.ts';

/**
 * The reminder shard keys, `p0`..`p15`.
 *
 * Duplicated rather than imported because `reminder-shards.ts` is internal
 * and widening the package's public API from a benchmark would be the tail
 * wagging the dog. Safe to duplicate precisely because that file pins the
 * count as COMPAT-CRITICAL storage identity — "never change either" — so
 * this cannot drift without a deliberate migration that would touch both.
 */
const REMINDER_SHARDS = Array.from({ length: 16 }, (_v, i) => `p${i}`);

const Counted = defineActor({
    type: 'ClusterBench',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        noop() {
            return 0;
        },
        increment(by: number) {
            ctx.state.count += by;
            return ctx.state.count;
        }
    })
});

/** The sweep. 1 is the control; 100 is the question. */
const SIZES = [1, 2, 10, 50, 100] as const;
const QUICK_SIZES = [1, 2, 10] as const;

function sizesFor(ctx: RunContext): readonly number[] {
    return ctx.quick ? QUICK_SIZES : SIZES;
}

/**
 * One silo joining an N-silo cluster: how many provider calls does that
 * cost, across the whole cluster?
 *
 * Every silo must learn about the change, so the notification count is
 * expected to grow with N — that part is inherent. What matters is what a
 * notification COSTS: `memoryClusterHub` answers from a local map, while the
 * Redis provider turns each one into a `refresh()` — one `SMEMBERS`, then
 * one `HGET` per id that came back. After a join the set holds `n + 1`
 * silos, so a refresh is `n + 2` round trips and the join costs
 * `notify × (n + 2)`.
 */
const membershipFanout: Scenario = {
    name: 'cluster/membership-fanout',
    description: 'provider calls across the cluster caused by ONE silo joining, vs N',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        for (const n of sizesFor(ctx)) {
            const harness = await createCluster(n, { actors: [Counted], policy: selfPolicy });
            try {
                harness.counter.reset();
                await harness.addSilo();
                const notify = harness.counter.counts['membership.notify'];
                metrics.push(
                    {
                        name: `n=${n}/notifications`,
                        value: notify,
                        unit: 'count',
                        direction: 'lower',
                        noiseFloor: 0.5
                    },
                    {
                        // What the same join would cost against Redis, using
                        // the provider's own refresh shape: one SMEMBERS plus
                        // one HGET per id it returns. The set holds n + 1
                        // silos once the newcomer has joined, so each refresh
                        // is n + 2 round trips — NOT n + 1, which would
                        // undercount by one HGET per notified silo. Derived
                        // from the provider source, not measured here.
                        name: `n=${n}/redis_ops_modelled`,
                        value: notify * (n + 2),
                        unit: 'count',
                        direction: 'lower',
                        noiseFloor: 0.5
                    },
                    {
                        name: `n=${n}/membership_calls`,
                        value: harness.counter.sum('membership.'),
                        unit: 'count',
                        direction: 'lower',
                        noiseFloor: 0.5,
                        informational: true
                    }
                );
            } finally {
                await harness.stop();
            }
        }
        return metrics;
    }
};

/**
 * The single most important scaling property: activating a grain must cost
 * the same whether the cluster has 1 silo or 100. If this grows with N, the
 * directory is a shared bottleneck that gets worse as you add capacity —
 * the opposite of scaling.
 */
const directoryOpsPerActivation: Scenario = {
    name: 'cluster/directory-ops-per-activation',
    description: 'directory calls per cold activation vs N — must stay flat',
    async run(ctx: RunContext): Promise<Metric[]> {
        const grains = ctx.quick ? 50 : 200;
        const metrics: Metric[] = [];
        for (const n of sizesFor(ctx)) {
            const harness = await createCluster(n, { actors: [Counted], policy: selfPolicy });
            try {
                const silo = harness.silos[0] as (typeof harness.silos)[number];
                const call = benchCall();
                harness.counter.reset();
                for (let i = 0; i < grains; i++) {
                    await silo.dispatch({ type: Counted.type, key: `g${i}` }, 'noop', [], call);
                }
                metrics.push(
                    {
                        name: `n=${n}/directory_ops`,
                        value: harness.counter.sum('directory.') / grains,
                        unit: 'count',
                        direction: 'lower',
                        noiseFloor: 0.1
                    },
                    {
                        name: `n=${n}/membership_views`,
                        value: harness.counter.counts['membership.view'] / grains,
                        unit: 'count',
                        direction: 'lower',
                        noiseFloor: 0.1,
                        informational: true
                    }
                );
            } finally {
                await harness.stop();
            }
        }
        return metrics;
    }
};

/**
 * `choose()` runs on every route-cache miss and is O(active silos) in both
 * shipped policies (each filters the view, then scores every silo). This
 * prices that per-decision cost across the sweep.
 */
const placementChoose: Scenario = {
    name: 'cluster/placement-choose',
    description: 'placement choose() cost per decision vs N, random vs consistent-hash',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        for (const n of sizesFor(ctx)) {
            const harness = await createCluster(n, { actors: [Counted], policy: selfPolicy });
            try {
                const placement = harness.placements[0] as (typeof harness.placements)[number];
                const view = harness.hub.providers().membership.view();
                const self = placement.descriptor();
                for (const [label, policy] of [
                    ['random', randomPlacementPolicy()],
                    ['hash', consistentHashPolicy()]
                ] as const) {
                    let i = 0;
                    const outcome = await closedLoop({
                        call: async () => policy.choose({ type: Counted.type, key: `k${i++}` }, view, self),
                        concurrency: 1,
                        durationMs: Math.min(ctx.durationMs, 200),
                        latency: false
                    });
                    metrics.push({
                        name: `n=${n}/${label}/choose_per_sec`,
                        value: outcome.opsPerSec,
                        unit: 'ops/s',
                        direction: 'higher'
                    });
                }
            } finally {
                await harness.stop();
            }
        }
        return metrics;
    }
};

/**
 * The reminder ceiling, made visible.
 *
 * The reminder table is split into a FIXED 16 shards (pinned forever —
 * the shard count is storage identity), and each shard is owned by exactly
 * one silo via rendezvous hashing. So the number of silos doing any reminder
 * work at all is bounded by 16, no matter how many you run: at N=100, ~84
 * silos own nothing. This scenario reports that directly, because it is a
 * ceiling you cannot configure your way out of.
 */
const reminderShardOwnership: Scenario = {
    name: 'cluster/reminder-shard-ownership',
    description: 'silos owning at least one of the 16 reminder shards vs N — expect min(N, 16)',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        const shards = REMINDER_SHARDS;
        for (const n of sizesFor(ctx)) {
            const harness = await createCluster(n, { actors: [Counted], policy: selfPolicy });
            try {
                const owners = new Set<string>();
                let maxPerSilo = 0;
                for (const placement of harness.placements) {
                    let owned = 0;
                    for (const shard of shards) {
                        if (placement.ownsReminderShard(shard)) owned++;
                    }
                    if (owned > 0) owners.add(placement.identity.siloId);
                    if (owned > maxPerSilo) maxPerSilo = owned;
                }
                metrics.push(
                    {
                        name: `n=${n}/silos_with_work`,
                        value: owners.size,
                        unit: 'count',
                        direction: 'higher',
                        noiseFloor: 0.5
                    },
                    {
                        name: `n=${n}/idle_silos`,
                        value: n - owners.size,
                        unit: 'count',
                        direction: 'lower',
                        noiseFloor: 0.5
                    },
                    {
                        // Load balance across the silos that do have work.
                        // Perfectly even at N>=16 would be 1.
                        name: `n=${n}/max_shards_on_one_silo`,
                        value: maxPerSilo,
                        unit: 'count',
                        direction: 'lower',
                        noiseFloor: 0.5,
                        informational: true
                    }
                );
            } finally {
                await harness.stop();
            }
        }
        return metrics;
    }
};

/**
 * Locality — the thing that actually decides cross-silo traffic.
 *
 * Nothing routes a caller toward the owner of a grain, so under the default
 * random policy an inbound request lands on the owner roughly 1/N of the
 * time. At N=100 that means ~99% of calls take a network hop. This measures
 * the real ratio rather than assuming it, because it is the multiplier on
 * every latency number in a real deployment.
 */
const locality: Scenario = {
    name: 'cluster/locality',
    description: 'fraction of calls whose receiving silo also OWNS the grain, vs N',
    async run(ctx: RunContext): Promise<Metric[]> {
        const grains = ctx.quick ? 60 : 300;
        const metrics: Metric[] = [];
        for (const n of sizesFor(ctx)) {
            for (const [label, policy] of [
                ['random', randomPlacementPolicy()],
                ['hash', consistentHashPolicy()]
            ] as const) {
                const harness = await createCluster(n, { actors: [Counted], policy });
                try {
                    const call = benchCall();
                    let localHits = 0;
                    for (let i = 0; i < grains; i++) {
                        // Spread callers across silos, as a load balancer would.
                        const silo = harness.silos[i % n] as (typeof harness.silos)[number];
                        const before = silo.stats().activations;
                        await silo.dispatch({ type: Counted.type, key: `g${i}` }, 'noop', [], call);
                        // The receiving silo gained an activation ⇒ it owns
                        // the grain ⇒ no network hop was needed.
                        if (silo.stats().activations > before) localHits++;
                    }
                    metrics.push({
                        name: `n=${n}/${label}/local_fraction`,
                        value: localHits / grains,
                        unit: 'ratio',
                        direction: 'higher',
                        noiseFloor: 0.02
                    });
                } finally {
                    await harness.stop();
                }
            }
        }
        return metrics;
    }
};

/**
 * The per-call cost of the cluster layer, at a fixed small N so the numbers
 * mean something: a locally-owned call (one extra Map lookup over a
 * single-node dispatch) against a peer-owned one (envelope, HMAC, codec,
 * transport), and what the HMAC alone costs.
 */
const crossSiloCall: Scenario = {
    name: 'cluster/local-vs-cross-silo',
    description: 'dispatch to a locally-owned grain vs a peer-owned one, with and without HMAC',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        for (const [label, secret] of [
            ['hmac', 'bench-secret'],
            ['no-hmac', null]
        ] as const) {
            const harness = await createCluster(2, {
                actors: [Counted],
                policy: selfPolicy,
                secret
            });
            try {
                const [a, b] = harness.silos as [
                    (typeof harness.silos)[number],
                    (typeof harness.silos)[number]
                ];
                const call = benchCall();
                // selfPolicy: whichever silo first touches a key owns it.
                const localRef = { type: Counted.type, key: 'local' };
                const remoteRef = { type: Counted.type, key: 'remote' };
                await a.dispatch(localRef, 'noop', [], call);
                await b.dispatch(remoteRef, 'noop', [], call);

                for (const [kind, ref] of [
                    ['local', localRef],
                    ['cross', remoteRef]
                ] as const) {
                    const outcome = await closedLoop({
                        call: () => a.dispatch(ref, 'noop', [], call),
                        concurrency: 1,
                        durationMs: Math.min(ctx.durationMs, 300),
                        latency: true
                    });
                    metrics.push(
                        {
                            name: `${label}/${kind}/ops_per_sec`,
                            value: outcome.opsPerSec,
                            unit: 'ops/s',
                            direction: 'higher'
                        },
                        {
                            name: `${label}/${kind}/p99_ms`,
                            value: outcome.percentiles!.p99,
                            unit: 'ms',
                            direction: 'lower',
                            noiseFloor: LATENCY_NOISE_FLOOR_MS
                        }
                    );
                }
            } finally {
                await harness.stop();
            }
        }
        return metrics;
    }
};

export const clusterScenarios: Scenario[] = [
    membershipFanout,
    directoryOpsPerActivation,
    placementChoose,
    reminderShardOwnership,
    locality,
    crossSiloCall
];
