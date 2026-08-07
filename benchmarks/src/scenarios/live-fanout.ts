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

const SIZES = [1, 4, 16] as const;
const QUICK_SIZES = [1, 4] as const;

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

                // Emissions: mutate sequentially and drain subscriber #0.
                // One process, both hosts sharing this CPU — contention, so
                // it never gates.
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
        return metrics;
    }
};

export const liveFanoutScenarios: Scenario[] = [liveFanout];
