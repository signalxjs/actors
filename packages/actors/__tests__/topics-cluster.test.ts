/**
 * Topics across hosts (#239) — a publish is ordinary dispatches through
 * placement, so a remotely-owned subscriber rides the internal transport
 * (HMAC and all) with zero topic-specific wire machinery. These tests pin
 * exactly that: remote delivery, the cost model (one remote dispatch per
 * remote subscriber), payloads surviving the codec, and a dead peer
 * becoming a `failures` entry instead of a publisher exception.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor, topic, type TopicEvent } from '@sigx/actors';
import type { PlacementPolicy } from '@sigx/actors/cluster';
import { createCluster, type ClusterHarness } from './harness';

const chat = topic<unknown>('chat-messages', 'room-1');

/** Pin every activation to a peer — the publish must cross the wire. */
const peerPolicy: PlacementPolicy = {
    choose: (_ref, view, self) =>
        view.hosts.find((h) => h.hostId !== self.hostId && h.status === 'active') ?? self
};

const Feed = defineActor({
    type: 'Feed',
    allowAnonymous: true,
    state: () => ({ log: [] as unknown[], events: [] as TopicEvent[] }),
    methods: (ctx) => ({
        async log() {
            return ctx.snapshot().log;
        }
    }),
    subscriptions: {
        'chat-messages': async (ctx, event) => {
            ctx.state.log.push(event.payload);
            ctx.state.events.push(event);
            await ctx.save();
        }
    }
});

let cluster: ClusterHarness | null = null;

afterEach(async () => {
    await cluster?.stop();
    cluster = null;
});

describe('topics across hosts', () => {
    it('delivers to a subscriber owned by a peer, one remote dispatch per remote subscriber', async () => {
        cluster = await createCluster(2, {
            actors: [Feed],
            typePolicies: { Feed: peerPolicy }
        });
        const before = cluster.placements[0]!.counters().remoteDispatches;
        const report = await cluster.hosts[0]!.publish(chat, 'over the wire');
        expect(report).toEqual({ subscribers: 1, delivered: 1, failures: [] });
        // The cost model: exactly one remote dispatch for the one remote
        // subscriber — the invariant the bench scenario will gate.
        expect(cluster.placements[0]!.counters().remoteDispatches - before).toBe(1);
        // The activation really lives on the peer.
        expect(cluster.hosts[1]!.stats().perType['Feed']).toBe(1);
        expect(cluster.hosts[0]!.stats().perType['Feed']).toBeUndefined();
        await expect(cluster.hosts[1]!.actor(Feed, 'room-1').log()).resolves.toEqual([
            'over the wire'
        ]);
    });

    it('round-trips rich payloads through the wire codec', async () => {
        cluster = await createCluster(2, {
            actors: [Feed],
            typePolicies: { Feed: peerPolicy }
        });
        const payload = {
            when: new Date('2026-08-02T00:00:00Z'),
            tags: new Map([['a', 1n]]),
            set: new Set(['x'])
        };
        const report = await cluster.hosts[0]!.publish(chat, payload);
        expect(report.delivered).toBe(1);
        const [stored] = (await cluster.hosts[1]!.actor(Feed, 'room-1').log()) as [
            typeof payload
        ];
        expect(stored.when).toBeInstanceOf(Date);
        expect(stored.when.toISOString()).toBe('2026-08-02T00:00:00.000Z');
        expect(stored.tags).toBeInstanceOf(Map);
        expect(stored.tags.get('a')).toBe(1n);
        expect(stored.set).toBeInstanceOf(Set);
    });

    it('an unreachable subscriber host is a failures entry, never a publisher throw', async () => {
        cluster = await createCluster(2, {
            actors: [Feed],
            typePolicies: { Feed: peerPolicy },
            retries: 0,
            retryBackoffMs: 1
        });
        // Claim the subscriber on host 1, then partition it: the address
        // stops resolving while its membership heartbeat stays alive, so
        // the delivery cannot fail over to a survivor.
        await cluster.hosts[0]!.publish(chat, 'first');
        cluster.unbind(1);
        const report = await cluster.hosts[0]!.publish(chat, 'second');
        expect(report.delivered).toBe(0);
        expect(report.failures).toHaveLength(1);
        expect(report.failures[0]!.type).toBe('Feed');
        // An unreachable-but-alive owner surfaces as a terminal 'activation'
        // error after bounded retries — the same contract ordinary calls pin
        // in cluster.test.ts ("unreachable-but-alive peers…").
        expect(report.failures[0]!.kind).toBe('activation');
    });
});
