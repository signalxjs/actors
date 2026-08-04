/**
 * Context-bag propagation across a cluster (#246), end to end: the
 * `.with({ bag })` escape hatch sets it, the envelope's `bag` field carries
 * it host-to-host, `ctx.actor` relays it through an intermediate actor, and
 * `ctx.publish` hands it to every topic subscriber. The wire.test suite
 * covers the guard-stamped edge; this one covers the hops.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor, topic } from '@sigx/actors';
import { createCluster, selfPolicy, type ClusterHarness } from './harness';

const activity = topic<string>('bag-activity');

const Prober = defineActor({
    type: 'Prober',
    allowAnonymous: true,
    state: () => ({ seen: null as Record<string, string> | null }),
    subscriptions: {
        'bag-activity': async (ctx, _event) => {
            ctx.state.seen = { ...ctx.bag };
        }
    },
    methods: (ctx) => ({
        async bagOf() {
            return { ...ctx.bag };
        },
        async seen() {
            return ctx.state.seen;
        }
    })
});

const Relay = defineActor({
    type: 'BagRelay',
    allowAnonymous: true,
    state: () => ({}),
    methods: (ctx) => ({
        async relay(key: string) {
            return (ctx.actor(Prober, key) as { bagOf(): Promise<Record<string, string>> }).bagOf();
        },
        async announce() {
            await ctx.publish(activity, 'went by');
        }
    })
});

let running: ClusterHarness | null = null;
afterEach(async () => {
    await running?.stop();
    running = null;
});

describe('context bag across the cluster', () => {
    it('crosses a host-to-host hop via the envelope', async () => {
        const cluster = await createCluster(2, { actors: [Prober], policy: selfPolicy });
        running = cluster;
        // selfPolicy: host 1's first call makes it the owner.
        await cluster.hosts[1]!.actor(Prober, 'k').bagOf();
        const seen = await cluster.hosts[0]!
            .actor(Prober, 'k')
            .with({ bag: { user: 'ada', 'corr-id': 'r-9' } })
            .bagOf();
        expect(seen).toEqual({ user: 'ada', 'corr-id': 'r-9' });
        // Absence stays absence on the next call.
        await expect(cluster.hosts[0]!.actor(Prober, 'k').bagOf()).resolves.toEqual({});
    });

    it('is relayed by ctx.actor through an uninstrumented hop', async () => {
        const cluster = await createCluster(2, { actors: [Prober, Relay], policy: selfPolicy });
        running = cluster;
        // Prober#far owned by host 1; Relay activates on host 0. Only the
        // RELAY call carries the bag — the Prober hop must inherit it.
        await cluster.hosts[1]!.actor(Prober, 'far').bagOf();
        const seen = await cluster.hosts[0]!
            .actor(Relay, 'r')
            .with({ bag: { user: 'ada' } })
            .relay('far');
        expect(seen).toEqual({ user: 'ada' });
    });

    it('reaches topic subscribers through ctx.publish', async () => {
        const cluster = await createCluster(1, { actors: [Prober, Relay] });
        running = cluster;
        // The topic key defaults to 'default', which is also the subscriber
        // activation's key; publish activates it on demand.
        await cluster.hosts[0]!
            .actor(Relay, 'r')
            .with({ bag: { user: 'grace' } })
            .announce();
        await expect(cluster.hosts[0]!.actor(Prober, 'default').seen()).resolves.toEqual({
            user: 'grace'
        });
    });
});
