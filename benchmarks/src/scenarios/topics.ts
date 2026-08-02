/**
 * Topics: what one publish COSTS, as invariants.
 *
 * A publish is S ordinary dispatches — one per subscribing type — and both
 * halves of that sentence are checkable by construction: the subscriber set
 * is a pure function of the registry, and the placement arms here are
 * deterministic (`self` pins every subscriber to the publishing host,
 * `peer` pins every one to a peer). So `deliveries_per_publish` and
 * `remote_dispatches_per_publish` are exact gates — a change that quietly
 * fanned out twice, or started proxying local deliveries over the wire,
 * fails the check on a shared runner where no timing could.
 *
 * Throughput is reported too, but informational only — this is one process
 * hosting every "host", so the number describes contention, not capacity.
 */
import { defineActor, topic, type AnyActorDefinition } from '@sigx/actors';
import type { PlacementPolicy } from '@sigx/actors/cluster';
import { createCluster, selfPolicy } from '../cluster-harness.ts';
import type { Metric, RunContext, Scenario } from '../types.ts';

/**
 * Every activation goes to the FIRST other active host, by membership
 * insertion order — deterministic by construction (no RNG, no per-run host
 * ids in the decision), which is what lets its remote-dispatch count gate.
 */
const peerPolicy: PlacementPolicy = {
    name: 'peer',
    choose: (_ref, view, self) =>
        view.hosts.find((h) => h.hostId !== self.hostId && h.status === 'active') ?? self
};

const BENCH_TOPIC = topic('bench-topic', 'k');

/** S subscriber types, each declaring one subscription to the bench topic. */
function subscribers(s: number): AnyActorDefinition[] {
    return Array.from({ length: s }, (_v, i) =>
        defineActor({
            type: `TopicSub${i}`,
            unguarded: true,
            state: () => ({ seen: 0 }),
            methods: (ctx) => ({
                seen() {
                    return ctx.state.seen;
                }
            }),
            subscriptions: {
                'bench-topic': (ctx) => {
                    ctx.state.seen += 1;
                }
            }
        })
    );
}

const SIZES = [1, 4, 16] as const;
const QUICK_SIZES = [1, 4] as const;

const ARMS: { label: string; policy: PlacementPolicy }[] = [
    // Everything local: the publish must not touch the wire at all (0).
    { label: 'local', policy: selfPolicy },
    // Everything remote: exactly one internal dispatch per subscriber (S).
    { label: 'peer', policy: peerPolicy }
];

const fanout: Scenario = {
    name: 'topics/fanout',
    description: 'dispatches per publish vs subscriber-type count, local and cross-host',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        const sizes = ctx.quick ? QUICK_SIZES : SIZES;
        for (const arm of ARMS) {
            for (const s of sizes) {
                const harness = await createCluster(2, {
                    actors: subscribers(s),
                    policy: arm.policy
                });
                try {
                    const publisher = harness.hosts[0]!;
                    const placement = harness.placements[0]!;
                    // Warm: activate every subscriber and claim directory
                    // entries, so the measured publish is the steady state.
                    await publisher.publish(BENCH_TOPIC, 0);

                    const remoteBefore = placement.counters().remoteDispatches;
                    const report = await publisher.publish(BENCH_TOPIC, 1);
                    const remote = placement.counters().remoteDispatches - remoteBefore;

                    metrics.push(
                        {
                            name: `${arm.label}/s=${s}/deliveries_per_publish`,
                            value: report.delivered,
                            unit: 'count',
                            direction: 'lower',
                            // The subscriber set is the registry — no RNG, no
                            // clock, no host ids. A drift here is a real
                            // behavioural change in the fan-out. No noiseFloor:
                            // the exact branch of the comparer never reads one.
                            exact: true
                        },
                        {
                            name: `${arm.label}/s=${s}/remote_dispatches_per_publish`,
                            value: remote,
                            unit: 'count',
                            direction: 'lower',
                            // Deterministic on BOTH arms: `self` pins every
                            // subscriber local (0), `peer` pins every one to
                            // a peer (S). Neither consults randomness.
                            exact: true
                        }
                    );

                    // Throughput: sequential awaited publishes for a slice of
                    // the budget. One process, every host sharing this CPU —
                    // a contention figure, so it never gates.
                    const sliceMs = Math.max(200, Math.min(1000, ctx.durationMs / 8));
                    const until = performance.now() + sliceMs;
                    let publishes = 0;
                    while (performance.now() < until) {
                        await publisher.publish(BENCH_TOPIC, publishes);
                        publishes += 1;
                    }
                    metrics.push({
                        name: `${arm.label}/s=${s}/publishes_per_sec`,
                        value: Math.round((publishes / sliceMs) * 1000),
                        unit: 'ops/s',
                        direction: 'higher',
                        informational: true
                    });
                } finally {
                    await harness.stop();
                }
            }
        }
        return metrics;
    }
};

export const topicScenarios: Scenario[] = [fanout];
