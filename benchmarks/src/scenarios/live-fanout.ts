/**
 * Live fan-out: what n subscribers to one REMOTE read cost, as invariants.
 *
 * The #111 contract — cross-host watch streams scale with HOSTS, not
 * subscribers — is a set of counts that hold by construction once placement
 * is pinned: n same-key subscribers on one host open exactly ONE stream to
 * the owner (`remote_watch_streams`), the owner serves exactly ONE inbound
 * watch (`inbound_watch_streams`), and the other n−1 attaches joined
 * (`coalesced_joins`). All three are exact gates: a change that quietly
 * opened per-subscriber streams again fails the check on a shared runner
 * where no timing could.
 *
 * A second arm pins the #138 contract on top: the relay carries the caller's
 * principal in that key unless the method declares `principalIndependent`,
 * so P DISTINCT identities cost ONE stream on a declared read
 * (`declared=P/*`) and P on an undeclared one (`undeclared=P/*`). Both are
 * exact — the undeclared row pins the conservative default, so a change that
 * starts sharing an undeclared read fails here rather than serving one
 * identity's view to another.
 *
 * Emission throughput is reported too, but informational only — one
 * process hosts every "host", so it describes contention, not capacity.
 */
import { defineActor } from '@sigx/actors';
import type { PlacementPolicy } from '@sigx/actors/cluster';
import { createCluster } from '../cluster-harness.ts';
import type { Metric, RunContext, Scenario } from '../types.ts';

/**
 * Every activation goes to the FIRST other active host, by membership
 * insertion order — deterministic by construction (no RNG, no per-run host
 * ids in the decision), which is what lets these counts gate. The
 * `topics/fanout` precedent.
 */
const peerPolicy: PlacementPolicy = {
    name: 'peer',
    choose: (_ref, view, self) =>
        view.hosts.find((h) => h.hostId !== self.hostId && h.status === 'active') ?? self
};

const WatchedCounter = defineActor({
    type: 'WatchedCounter',
    allowAnonymous: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        read() {
            return ctx.state.count;
        },
        async increment() {
            ctx.state.count += 1;
            await ctx.save();
            return ctx.state.count;
        }
    })
});

/**
 * The #138 arm. `shared` promises its result ignores `ctx.principal`, so the
 * relay may drop the principal from its coalescing key; `perUser` is the
 * same read UNDECLARED, so the pair differ in exactly one thing.
 */
const SharedFeed = defineActor({
    type: 'SharedFeed',
    allowAnonymous: true,
    watches: { shared: { principalIndependent: true } },
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        shared() {
            return ctx.state.count;
        },
        perUser() {
            return ctx.state.count;
        },
        async increment() {
            ctx.state.count += 1;
            await ctx.save();
            return ctx.state.count;
        }
    })
});

const SIZES = [1, 4, 16] as const;
const QUICK_SIZES = [1, 4] as const;

/** Distinct identities on the declared arm — the population #138 collapses. */
const PRINCIPALS = 16;
const QUICK_PRINCIPALS = 4;

const liveFanout: Scenario = {
    name: 'cluster/live-fanout',
    description: 'cross-host watch streams vs subscriber count — one stream per key (#111)',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        const sizes = ctx.quick ? QUICK_SIZES : SIZES;
        for (const n of sizes) {
            const harness = await createCluster(2, {
                actors: [WatchedCounter],
                policy: peerPolicy
            });
            try {
                const caller = harness.hosts[0]!;
                const ref = { type: 'WatchedCounter', key: 'live' } as const;
                // Activate on the peer (peerPolicy pins it there) so every
                // watch below crosses the hop.
                await caller.actor(WatchedCounter, 'live').increment();

                const before = {
                    remote: harness.placements[0]!.counters().remoteWatches,
                    joined: harness.placements[0]!.counters().coalescedWatches,
                    inbound: harness.placements[1]!.counters().inboundWatches
                };

                const aborts: AbortController[] = [];
                const iterators: AsyncIterator<unknown>[] = [];
                for (let i = 0; i < n; i++) {
                    const abort = new AbortController();
                    aborts.push(abort);
                    const iterator = caller
                        .dispatchWatch!(
                            ref,
                            'read',
                            [],
                            {
                                callChain: [],
                                callId: `bench-${i}`,
                                abortSignal: abort.signal
                            },
                            { throttleMs: 0 }
                        )
                        [Symbol.asyncIterator]();
                    iterators.push(iterator);
                    // Await each initial value: the counts below must
                    // describe ESTABLISHED subscriptions, not races.
                    await iterator.next();
                }

                metrics.push(
                    {
                        name: `n=${n}/remote_watch_streams`,
                        value: harness.placements[0]!.counters().remoteWatches - before.remote,
                        unit: 'count',
                        direction: 'lower',
                        // Placement is pinned and the key is a pure function
                        // of (actor, method, throttle, args, principal) — no
                        // RNG, no clock, no host ids. Always 1.
                        exact: true
                    },
                    {
                        name: `n=${n}/inbound_watch_streams`,
                        value: harness.placements[1]!.counters().inboundWatches - before.inbound,
                        unit: 'count',
                        direction: 'lower',
                        // The owner's mirror of the same invariant.
                        exact: true
                    },
                    {
                        name: `n=${n}/coalesced_joins`,
                        value:
                            harness.placements[0]!.counters().coalescedWatches - before.joined,
                        unit: 'count',
                        direction: 'higher',
                        // Every attach past the first joins: n−1.
                        exact: true
                    }
                );

                // Emissions: mutate sequentially and drain the first
                // subscriber. One process, both hosts sharing this CPU —
                // contention, so it never gates.
                const sliceMs = Math.max(200, Math.min(1000, ctx.durationMs / 8));
                const until = performance.now() + sliceMs;
                let emissions = 0;
                while (performance.now() < until) {
                    await caller.actor(WatchedCounter, 'live').increment();
                    await iterators[0]!.next();
                    emissions += 1;
                }
                metrics.push({
                    name: `n=${n}/emissions_per_sec`,
                    value: Math.round((emissions / sliceMs) * 1000),
                    unit: 'ops/s',
                    direction: 'higher',
                    informational: true
                });

                for (const abort of aborts) abort.abort();
            } finally {
                await harness.stop();
            }
        }
        metrics.push(...(await principalArms(ctx)));
        return metrics;
    }
};

/**
 * P DISTINCT principals, on a declared read and on an undeclared one.
 *
 * The relay keys its coalesced stream on the caller's principal unless the
 * method declared `principalIndependent` (#138) — so the declared arm costs
 * ONE cross-host stream for the whole identity population and the undeclared
 * arm costs P. Both are `exact`: the key is a pure function of (actor,
 * method, throttle, encoded args) plus that declaration, with the placement
 * pinned by `peerPolicy` — no RNG, no clock, no per-run host ids.
 *
 * The undeclared row is not decoration: it pins the CONSERVATIVE default, so
 * a change that starts sharing an undeclared read fails the gate rather than
 * quietly serving one identity's view to another.
 */
async function principalArms(ctx: RunContext): Promise<Metric[]> {
    const metrics: Metric[] = [];
    const p = ctx.quick ? QUICK_PRINCIPALS : PRINCIPALS;
    for (const method of ['shared', 'perUser'] as const) {
        const label = method === 'shared' ? 'declared' : 'undeclared';
        const harness = await createCluster(2, {
            actors: [SharedFeed],
            policy: peerPolicy
        });
        try {
            const caller = harness.hosts[0]!;
            const ref = { type: 'SharedFeed', key: 'live' } as const;
            await caller.actor(SharedFeed, 'live').increment();

            const before = {
                remote: harness.placements[0]!.counters().remoteWatches,
                joined: harness.placements[0]!.counters().coalescedWatches,
                inbound: harness.placements[1]!.counters().inboundWatches
            };

            const aborts: AbortController[] = [];
            for (let i = 0; i < p; i++) {
                const abort = new AbortController();
                aborts.push(abort);
                const iterator = caller
                    .dispatchWatch!(
                        ref,
                        method,
                        [],
                        {
                            callChain: [],
                            callId: `bench-${label}-${i}`,
                            // One distinct encoded identity per subscriber.
                            principal: `user-${i}`,
                            abortSignal: abort.signal
                        },
                        { throttleMs: 0 }
                    )
                    [Symbol.asyncIterator]();
                await iterator.next();
            }

            metrics.push(
                {
                    name: `${label}=${p}/remote_watch_streams`,
                    value: harness.placements[0]!.counters().remoteWatches - before.remote,
                    unit: 'count',
                    direction: 'lower',
                    exact: true
                },
                {
                    name: `${label}=${p}/inbound_watch_streams`,
                    value: harness.placements[1]!.counters().inboundWatches - before.inbound,
                    unit: 'count',
                    direction: 'lower',
                    exact: true
                },
                {
                    name: `${label}=${p}/coalesced_joins`,
                    value: harness.placements[0]!.counters().coalescedWatches - before.joined,
                    unit: 'count',
                    direction: 'higher',
                    exact: true
                }
            );

            for (const abort of aborts) abort.abort();
        } finally {
            await harness.stop();
        }
    }
    return metrics;
}

export const liveFanoutScenarios: Scenario[] = [liveFanout];
