/**
 * Does it scale to 100 hosts?
 *
 * These scenarios do NOT measure throughput — every host shares one CPU
 * here, so a 100-host number would just be 100 hosts contending for one
 * core. They measure the **algorithmic shape**: how much work the runtime
 * asks of the cluster providers, and how each per-decision cost grows, as a
 * function of N. That is what decides whether 100 hosts on 100 VMs works,
 * and an O(N²) shows up here for free rather than on a bill.
 *
 * Reading them: a metric that stays FLAT as N goes 1 → 100 scales. One that
 * grows with N is a shared bottleneck, and one that grows with N per host is
 * an O(N²) waiting to happen.
 */
import { defineActor } from '@sigx/actors';
import { hashRouteToken } from '@sigx/actors/client';
import {
    consistentHashPolicy,
    preferLocalPolicy,
    randomPlacementPolicy
} from '@sigx/actors/cluster';
import { createCluster, selfPolicy } from '../cluster-harness.ts';
import { benchCall } from '../host-fixture.ts';
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

/**
 * Below this much change, a placement ratio has not moved.
 *
 * These ratios are quantized by the actor: over the 48-200 actors a sweep
 * places, ONE landing on a different host is worth 0.005-0.02. The floor was
 * 0.02 — exactly one actor — and a CI A/B of two identical commits duly
 * reported `local_fraction` 0.00 → 0.0208 as an improvement, clearing it by
 * 0.0008. Placement here is `randomPlacementPolicy()`, so a couple of actors
 * landing elsewhere is the expected behaviour of the thing being measured,
 * not evidence about the code. 0.05 covers that.
 */
const PLACEMENT_RATIO_NOISE_FLOOR = 0.05;

/**
 * `{ exact: true }` for the placement arms that are deterministic, `{}` for
 * the rest — spread into a metric so the distinction is made once, by policy,
 * rather than repeated per metric where it would drift.
 *
 * `preferLocalPolicy()` activates on whichever host received the call, so it
 * consults neither the RNG nor the per-run host ids. Every edge here is a pure
 * function too — two counters and a hash of the actor key — so an arm pairing
 * them is reproducible anywhere, and its numbers are a statement about the
 * ROUTING DESIGN rather than about this machine. `edgehash+prefer-local` is
 * the one that matters most: its entire claim is `hops_per_call = 0` at every
 * N, and a change that quietly broke perfect locality would move nothing a
 * timing comparison can see.
 *
 * The other two arms are `randomPlacementPolicy()`, which is exactly what it
 * says. Their ratios drift run to run and must never gate — measured across
 * four runs on two machines, every `prefer-local` figure was bit-identical
 * while the random ones moved.
 *
 * Not applied to `consistentHashPolicy` either, for a subtler reason: it is
 * deterministic given the host ids, and the ids are minted per run. Steady
 * within a run, different across two — the worst possible shape for a gate.
 */
function exactIfDeterministic(label: string): { exact?: true } {
    return label.endsWith('prefer-local') ? { exact: true } : {};
}

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

/**
 * What a load balancer does with the routing token: hash the STRING it sees,
 * with its own algorithm, over its own pool. Deliberately NOT the cluster's
 * rendezvous hash — the whole point of the composition is that the edge and
 * the cluster agree on nothing but stability.
 */
function edgeSlot(key: string, n: number): number {
    const token = hashRouteToken(Counted.type, key);
    let h = 0;
    for (let i = 0; i < token.length; i++) h = (Math.imul(h, 31) + token.charCodeAt(i)) >>> 0;
    return h % n;
}

/** The sweep. 1 is the control; 100 is the question. */
const SIZES = [1, 2, 10, 50, 100] as const;
const QUICK_SIZES = [1, 2, 10] as const;

function sizesFor(ctx: RunContext): readonly number[] {
    return ctx.quick ? QUICK_SIZES : SIZES;
}

/**
 * One host joining an N-host cluster: how many provider calls does that
 * cost, across the whole cluster?
 *
 * Every host must learn about the change, so the notification count is
 * expected to grow with N — that part is inherent. What matters is what a
 * notification COSTS: `memoryClusterHub` answers from a local map, while the
 * Redis provider turns each one into a `refresh()` — one `SMEMBERS`, then
 * one `HGET` per id that came back. After a join the set holds `n + 1`
 * hosts, so a refresh is `n + 2` round trips and the join costs
 * `notify × (n + 2)`.
 */
const membershipFanout: Scenario = {
    name: 'cluster/membership-fanout',
    description: 'provider calls across the cluster caused by ONE host joining, vs N',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        for (const n of sizesFor(ctx)) {
            const harness = await createCluster(n, { actors: [Counted], policy: selfPolicy });
            try {
                harness.counter.reset();
                await harness.addHost();
                const notify = harness.counter.counts['membership.notify'];
                metrics.push(
                    {
                        name: `n=${n}/notifications`,
                        value: notify,
                        unit: 'count',
                        direction: 'lower',
                        // Provider calls under `selfPolicy` — no randomness
                        // anywhere in the path, so this is an invariant and
                        // an O(N²) fan-out shows up as an exact failure.
                        exact: true,
                        noiseFloor: 0.5
                    },
                    {
                        // What the same join would cost against Redis, using
                        // the provider's own refresh shape: one SMEMBERS plus
                        // one HGET per id it returns. The set holds n + 1
                        // hosts once the newcomer has joined, so each refresh
                        // is n + 2 round trips — NOT n + 1, which would
                        // undercount by one HGET per notified host. Derived
                        // from the provider source, not measured here.
                        name: `n=${n}/redis_ops_modelled`,
                        value: notify * (n + 2),
                        unit: 'count',
                        direction: 'lower',
                        // A pure function of `notifications` above.
                        exact: true,
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
 * The single most important scaling property: activating an actor must cost
 * the same whether the cluster has 1 host or 100. If this grows with N, the
 * directory is a shared bottleneck that gets worse as you add capacity —
 * the opposite of scaling.
 */
const directoryOpsPerActivation: Scenario = {
    name: 'cluster/directory-ops-per-activation',
    description: 'directory calls per cold activation vs N — must stay flat',
    async run(ctx: RunContext): Promise<Metric[]> {
        const actors = ctx.quick ? 50 : 200;
        const metrics: Metric[] = [];
        for (const n of sizesFor(ctx)) {
            const harness = await createCluster(n, { actors: [Counted], policy: selfPolicy });
            try {
                const host = harness.hosts[0] as (typeof harness.hosts)[number];
                const call = benchCall();
                harness.counter.reset();
                for (let i = 0; i < actors; i++) {
                    await host.dispatch({ type: Counted.type, key: `g${i}` }, 'noop', [], call);
                }
                metrics.push(
                    {
                        name: `n=${n}/directory_ops`,
                        value: harness.counter.sum('directory.') / actors,
                        unit: 'count',
                        direction: 'lower',
                        // The single most important scaling property, and an
                        // invariant: a fixed actor loop under `selfPolicy`.
                        // An extra directory round-trip per activation moves
                        // it from 2 to 3 and fails the check — which is the
                        // whole reason this gate exists.
                        exact: true,
                        noiseFloor: 0.1
                    },
                    {
                        name: `n=${n}/membership_views`,
                        value: harness.counter.counts['membership.view'] / actors,
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
 * `choose()` runs on every route-cache miss and is O(active hosts) in both
 * shipped policies (each filters the view, then scores every host). This
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
 * one host via rendezvous hashing. So the number of hosts doing any reminder
 * work at all is bounded by 16, no matter how many you run: at N=100, ~84
 * hosts own nothing. This scenario reports that directly, because it is a
 * ceiling you cannot configure your way out of.
 */
const reminderShardOwnership: Scenario = {
    name: 'cluster/reminder-shard-ownership',
    description: 'hosts owning at least one of the 16 reminder shards vs N — expect min(N, 16)',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        const shards = REMINDER_SHARDS;
        for (const n of sizesFor(ctx)) {
            const harness = await createCluster(n, { actors: [Counted], policy: selfPolicy });
            try {
                const owners = new Set<string>();
                let maxPerHost = 0;
                for (const placement of harness.placements) {
                    let owned = 0;
                    for (const shard of shards) {
                        if (placement.ownsReminderShard(shard)) owned++;
                    }
                    if (owned > 0) owners.add(placement.identity.hostId);
                    if (owned > maxPerHost) maxPerHost = owned;
                }
                metrics.push(
                    {
                        name: `n=${n}/hosts_with_work`,
                        value: owners.size,
                        unit: 'count',
                        direction: 'higher',
                        noiseFloor: 0.5
                    },
                    {
                        name: `n=${n}/idle_hosts`,
                        value: n - owners.size,
                        unit: 'count',
                        direction: 'lower',
                        noiseFloor: 0.5
                    },
                    {
                        // Load balance across the hosts that do have work.
                        // Perfectly even at N>=16 would be 1.
                        name: `n=${n}/max_shards_on_one_host`,
                        value: maxPerHost,
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
 * Locality — the thing that actually decides cross-host traffic.
 *
 * Nothing routes a caller toward the owner of an actor, so under the default
 * random policy an inbound request lands on the owner roughly 1/N of the
 * time. At N=100 that means ~99% of calls take a network hop. This measures
 * the real ratio rather than assuming it, because it is the multiplier on
 * every latency number in a real deployment.
 *
 * COLD PLACEMENT ONLY: each actor is touched exactly once, and a local hit
 * is inferred from the receiving host gaining an activation. That makes this
 * probe structurally 1.00 under any caller-affinity policy (`preferLocal`
 * always activates on the receiver), and blind to the route cache — it
 * cannot evaluate a WARM steady state. `cluster/locality-routed` below
 * measures the composition that fixes locality; a warm probe over
 * `placement.counters()` is stage 4 of the locality-routing RFC.
 */
const locality: Scenario = {
    name: 'cluster/locality',
    description: 'fraction of calls whose receiving host also OWNS the actor, vs N',
    async run(ctx: RunContext): Promise<Metric[]> {
        const actors = ctx.quick ? 60 : 300;
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
                    for (let i = 0; i < actors; i++) {
                        // Spread callers across hosts, as a load balancer would.
                        const host = harness.hosts[i % n] as (typeof harness.hosts)[number];
                        const before = host.stats().activations;
                        await host.dispatch({ type: Counted.type, key: `g${i}` }, 'noop', [], call);
                        // The receiving host gained an activation ⇒ it owns
                        // the actor ⇒ no network hop was needed.
                        if (host.stats().activations > before) localHits++;
                    }
                    metrics.push({
                        name: `n=${n}/${label}/local_fraction`,
                        value: localHits / actors,
                        unit: 'ratio',
                        direction: 'higher',
                        noiseFloor: PLACEMENT_RATIO_NOISE_FLOOR
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
const crossHostCall: Scenario = {
    name: 'cluster/local-vs-cross-host',
    description: 'dispatch to a locally-owned actor vs a peer-owned one, with and without HMAC',
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
                const [a, b] = harness.hosts as [
                    (typeof harness.hosts)[number],
                    (typeof harness.hosts)[number]
                ];
                const call = benchCall();
                // selfPolicy: whichever host first touches a key owns it.
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

/**
 * Locality when the EDGE hashes the routing token — the composition the
 * locality-routing RFC argues for, measured rather than asserted.
 *
 * Two phases, because the cold probe above cannot answer this: it infers a
 * hit from a new activation on the receiver, which makes `preferLocal`
 * structurally 1.00 whatever the edge does. So phase 1 establishes ownership
 * (cold, activation deltas are meaningful), and phase 2 asks the question
 * that actually matters — in the STEADY state, does the edge send a call to
 * the host that owns the actor?
 *
 * Three arms, chosen to measure the RFC's claim AND its central argument:
 *
 * - `roundrobin+random`  — today's default. The 1/N baseline.
 * - `edgehash+hash`      — the ANTI-PATTERN. The edge's hash and the
 *   cluster's rendezvous hash are different functions over different sets;
 *   the RFC argues they cannot be made to agree, and this is what that costs.
 * - `edgehash+prefer-local` — the recommended shape. The LB becomes the
 *   placement: whoever receives the first call activates it, and the same
 *   key hashes back to that host forever.
 *
 * The edge hashes the REAL `hashRouteToken` off the shipped client entry —
 * the same token the client puts in the request line — so this measures the
 * actual composition, not a stand-in for it.
 */
const localityRouted: Scenario = {
    name: 'cluster/locality-routed',
    description: 'local fraction in the STEADY state, by edge strategy × placement policy',
    async run(ctx: RunContext): Promise<Metric[]> {
        const actors = ctx.quick ? 60 : 300;
        const metrics: Metric[] = [];
        for (const n of sizesFor(ctx)) {
            for (const [label, policy, edge] of [
                ['roundrobin+random', randomPlacementPolicy(), (i: number) => i % n],
                [
                    'edgehash+hash',
                    consistentHashPolicy(),
                    (_i: number, key: string) => edgeSlot(key, n)
                ],
                [
                    'edgehash+prefer-local',
                    preferLocalPolicy(),
                    (_i: number, key: string) => edgeSlot(key, n)
                ]
            ] as const) {
                const harness = await createCluster(n, { actors: [Counted], policy });
                try {
                    const call = benchCall();
                    const ref = (i: number) => ({ type: Counted.type, key: `g${i}` });

                    // Phase 1 — establish ownership. Cold, so an activation
                    // delta identifies the owner exactly.
                    const owner: number[] = Array.from({ length: actors }, () => -1);
                    for (let i = 0; i < actors; i++) {
                        const at = edge(i, `g${i}`);
                        const before = harness.hosts.map((s) => s.stats().activations);
                        await harness.hosts[at]!.dispatch(ref(i), 'noop', [], call);
                        owner[i] = harness.hosts.findIndex(
                            (s, j) => s.stats().activations > before[j]!
                        );
                    }

                    // Phase 2 — the steady state. Same edge decision, but now
                    // the actors are already placed.
                    let localHits = 0;
                    let placed = 0;
                    for (let i = 0; i < actors; i++) {
                        const at = edge(i, `g${i}`);
                        await harness.hosts[at]!.dispatch(ref(i), 'noop', [], call);
                        if (owner[i] === -1) continue; // never observed; not counted
                        placed++;
                        if (owner[i] === at) localHits++;
                    }

                    metrics.push({
                        name: `n=${n}/${label}/local_fraction`,
                        value: placed === 0 ? 0 : localHits / placed,
                        unit: 'ratio',
                        direction: 'higher',
                        ...exactIfDeterministic(label),
                        noiseFloor: PLACEMENT_RATIO_NOISE_FLOOR
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
 * Locality in the WARM steady state, and what it costs to get there.
 *
 * The two probes above both measure PLACEMENT — where an actor first lands.
 * Neither can answer the question a running cluster actually has: of the
 * calls I am making right now, how many take a network hop? So this one
 * warms up first, then measures.
 *
 * Read from `remoteDispatches` against a known call count, NOT from
 * `routedLocal`. That is a trap worth stating: `dispatcherFor` returns the
 * local dispatcher directly when the host already holds the claim, BEFORE
 * the routing path that increments `routedLocal` — so in the warm state
 * being measured here, a local hit increments nothing at all. Dividing
 * `routedLocal` by `routedLocal + remoteDispatches` would therefore report
 * near-zero locality for a perfectly local cluster. The denominator has to
 * come from outside the counters, and it does: this scenario issues the
 * calls, so it knows.
 *
 * The arms exist to answer one open question — should `preferLocalPolicy()`
 * be the default? Edge-hash + prefer-local is the shape the routing RFC
 * recommends, and it wins on locality. The case AGAINST making it the
 * default is that it inherits whatever distribution the load balancer has,
 * and a load balancer is not always fair: during a rolling deploy, or a bad
 * health check, one host briefly takes most of the traffic. Prefer-local
 * then places most of the actors there, and they do NOT move back. `skew/*`
 * is that scenario, and `ownership_spread` is what it costs.
 */
const localityWarm: Scenario = {
    name: 'cluster/locality-warm',
    description: 'hops per call in the STEADY state, and how evenly actors ended up spread',
    async run(ctx: RunContext): Promise<Metric[]> {
        const actors = ctx.quick ? 60 : 240;
        const metrics: Metric[] = [];

        // Edges are FACTORIES so each arm gets fresh state, and so
        // round-robin can be what it actually is.
        //
        // Subtle and load-bearing: a real round-robin balancer distributes by
        // ARRIVAL ORDER, so the same actor lands on a different host each
        // time it is called. Deriving the host from the actor index instead
        // (`i % n`) is a stable per-actor assignment — edge hashing wearing a
        // round-robin costume — and it makes prefer-local look perfect for
        // free, which is exactly the claim under test.
        type Edge = (i: number, key: string) => number;
        const roundRobin = (n: number): Edge => {
            let next = 0;
            return () => next++ % n;
        };
        /** A lopsided balancer — a rolling deploy, or a bad health check:
         *  four calls in five land on host 0, the fifth round-robins over
         *  ALL hosts. The remainder needs its own counter: reusing `next`
         *  advances it by five each time, so it would only ever visit every
         *  fifth host (hosts 1 and 6 at n=10) and concentrate the leftover
         *  traffic that this arm is supposed to spread. */
        const skewed = (n: number): Edge => {
            let next = 0;
            let rest = 0;
            return () => (next++ % 5 === 0 ? rest++ % n : 0);
        };
        const byGrain = (n: number): Edge => (_i, key) => edgeSlot(key, n);

        for (const n of sizesFor(ctx)) {
            for (const [label, policy, makeEdge] of [
                ['roundrobin+random', randomPlacementPolicy(), roundRobin],
                ['roundrobin+prefer-local', preferLocalPolicy(), roundRobin],
                ['edgehash+prefer-local', preferLocalPolicy(), byGrain],
                ['skew+random', randomPlacementPolicy(), skewed],
                ['skew+prefer-local', preferLocalPolicy(), skewed]
            ] as const) {
                const deterministic = exactIfDeterministic(label);
                const harness = await createCluster(n, { actors: [Counted], policy });
                try {
                    const call = benchCall();
                    const ref = (i: number) => ({ type: Counted.type, key: `g${i}` });
                    // ONE edge across both phases: a balancer does not reset
                    // when the cluster finishes warming up.
                    const edge = makeEdge(n);

                    // Warm up: place every actor. Unmeasured — this is the
                    // cold cost the other two scenarios already report.
                    for (let i = 0; i < actors; i++) {
                        await harness.hosts[edge(i, `g${i}`)]!.dispatch(
                            ref(i),
                            'noop',
                            [],
                            call
                        );
                    }

                    // How evenly did ownership land? Activations ARE
                    // ownership here: nothing has deactivated yet.
                    const owned = harness.hosts.map((s) => s.stats().activations);
                    const mean = owned.reduce((a, b) => a + b, 0) / n;
                    const spread = mean === 0 ? 1 : Math.max(...owned) / mean;

                    // Measure: the same edge, now against placed actors —
                    // the steady state a running cluster is in.
                    //
                    // Visited on a STRIDE coprime with `actors`, not in
                    // order. Iterating actors sequentially keeps a
                    // round-robin counter in lockstep with actor identity,
                    // which quietly turns it back into a stable per-actor
                    // assignment — and then prefer-local scores 1.00 purely
                    // because `actors % n === 0`. Real traffic does not
                    // arrive in actor order, and the stride is the cheapest
                    // way to say so deterministically.
                    const before = harness.placements.map(
                        (p) => p.counters().remoteDispatches
                    );
                    const STRIDE = 97; // coprime with both actor counts
                    for (let step = 0; step < actors; step++) {
                        const i = (step * STRIDE) % actors;
                        await harness.hosts[edge(i, `g${i}`)]!.dispatch(
                            ref(i),
                            'noop',
                            [],
                            call
                        );
                    }
                    const hops = harness.placements.reduce(
                        (total, p, at) => total + (p.counters().remoteDispatches - before[at]!),
                        0
                    );

                    metrics.push(
                        {
                            name: `n=${n}/${label}/hops_per_call`,
                            value: hops / actors,
                            unit: 'ratio',
                            direction: 'lower',
                            ...deterministic,
                            noiseFloor: PLACEMENT_RATIO_NOISE_FLOOR
                        },
                        {
                            name: `n=${n}/${label}/local_fraction`,
                            value: Math.max(0, 1 - hops / actors),
                            unit: 'ratio',
                            direction: 'higher',
                            ...deterministic,
                            noiseFloor: PLACEMENT_RATIO_NOISE_FLOOR
                        },
                        {
                            // 1.0 is perfectly even; n is "one host owns
                            // everything". The cost of caller affinity under
                            // a balancer that is not itself even.
                            name: `n=${n}/${label}/ownership_spread`,
                            value: spread,
                            unit: 'ratio',
                            direction: 'lower',
                            ...deterministic,
                            noiseFloor: 0.05
                        }
                    );
                } finally {
                    await harness.stop();
                }
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
    localityRouted,
    localityWarm,
    crossHostCall
];
