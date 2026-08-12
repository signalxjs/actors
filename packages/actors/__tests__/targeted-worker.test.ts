/**
 * Member enumeration + targeted worker calls (#213): `members()` is the
 * queryable membership view (the downstream `livePods()` replacement), and
 * `dispatchOn()`/`workerOn()` invoke a stateless worker ON a chosen member —
 * the generalized `$sigx:host#stats` mechanism. One attempt, no retry, no
 * route cache, no directory: the caller picked the host, so a miss is an
 * answer, not a condition to route around.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor, defineWorker } from '@sigx/actors';
import { workerOn } from '@sigx/actors/cluster';
import { addCounters, createCounters, type ClusterCounterTotals } from '../src/cluster/counters';
import { createCluster, type ClusterHarness } from './harness';

const Counter = defineActor({
    type: 'Counter',
    allowAnonymous: true,
    state: () => ({ n: 0 }),
    methods: (ctx) => ({
        async bump() {
            return ++ctx.state.n;
        }
    })
});

const Relay = defineWorker({
    type: 'Relay',
    allowAnonymous: true,
    methods: () => ({
        async double(n: number) {
            return n * 2;
        },
        async boom() {
            throw new Error('relay boom');
        }
    })
});

let cluster: ClusterHarness | null = null;
afterEach(async () => {
    await cluster?.stop();
    cluster = null;
});

/** Host 0: Counter only. Hosts 1 and 2: Relay — so targeting must pick ONE. */
function topo(
    extra: Partial<Parameters<typeof createCluster>[1]> = {}
): Promise<ClusterHarness> {
    return createCluster(3, {
        actors: [Counter],
        actorsFor: (i) => (i === 0 ? [Counter] : [Relay]),
        ...extra
    });
}

describe('members()', () => {
    it('answers the active view, self included, filtered by registration', async () => {
        cluster = await topo();
        const p0 = cluster.placements[0]!;
        const all = p0.members!();
        expect(all.map((h) => h.hostId).sort()).toEqual(
            cluster.placements.map((p) => p.identity.hostId).sort()
        );
        const relays = p0.members!({ registers: 'Relay' });
        expect(relays.map((h) => h.hostId).sort()).toEqual(
            [cluster.placements[1]!, cluster.placements[2]!].map((p) => p.identity.hostId).sort()
        );
    });

    it('a legacy descriptor (no types) matches any registers filter', async () => {
        cluster = await topo();
        await cluster.hub.providers().membership.join({
            hostId: 's.legacy',
            epoch: 1,
            address: 'http://host0.test',
            status: 'active'
        });
        const relays = cluster.placements[0]!.members!({ registers: 'Relay' });
        expect(relays.map((h) => h.hostId)).toContain('s.legacy');
    });

    it('a departed host leaves the answer', async () => {
        cluster = await topo();
        const departed = cluster.placements[2]!.identity.hostId;
        cluster.crash(2);
        expect(
            cluster.placements[0]!.members!({ registers: 'Relay' }).map((h) => h.hostId)
        ).not.toContain(departed);
    });
});

describe('dispatchOn()', () => {
    it('executes on the TARGETED host — by descriptor and by hostId — and counts', async () => {
        cluster = await topo();
        const [p0, p1, p2] = [cluster.placements[0]!, cluster.placements[1]!, cluster.placements[2]!];
        await expect(
            p0.dispatchOn!(p2.descriptor(), { type: 'Relay', key: 'k' }, 'double', [21])
        ).resolves.toBe(42);
        expect(p2.counters().inboundDispatches).toBe(1);
        expect(p1.counters().inboundDispatches).toBe(0);
        await expect(
            p0.dispatchOn!(p1.identity.hostId, { type: 'Relay', key: 'k' }, 'double', [4])
        ).resolves.toBe(8);
        expect(p1.counters().inboundDispatches).toBe(1);
        expect(p0.counters().targetedDispatches).toBe(2);
    });

    it('a self-target dispatches in-process, no wire hop', async () => {
        const urls: string[] = [];
        cluster = await topo({ onRequest: (url) => urls.push(url) });
        const p2 = cluster.placements[2]!;
        await expect(
            p2.dispatchOn!(p2.identity.hostId, { type: 'Relay', key: 'k' }, 'double', [3])
        ).resolves.toBe(6);
        expect(urls.filter((u) => u.includes('Relay'))).toEqual([]);
    });

    it('a departed target answers unreachable, without retry', async () => {
        cluster = await topo();
        const p0 = cluster.placements[0]!;
        const gone = cluster.placements[2]!.descriptor();
        cluster.crash(2);
        await expect(
            p0.dispatchOn!(gone, { type: 'Relay', key: 'k' }, 'double', [1])
        ).rejects.toMatchObject({ kind: 'unreachable' });
        expect(p0.counters().retries).toBe(0);
    });

    it('a target that does not register the type answers wrong-host, not re-routed', async () => {
        cluster = await topo();
        const p1 = cluster.placements[1]!;
        await expect(
            p1.dispatchOn!(
                cluster.placements[0]!.identity.hostId,
                { type: 'Relay', key: 'k' },
                'double',
                [1]
            )
        ).rejects.toMatchObject({ kind: 'wrong-host' });
        expect(p1.counters().retries).toBe(0);
    });

    it('refuses a stateful actor type when the definition is resolvable', async () => {
        cluster = await topo();
        await expect(
            cluster.placements[0]!.dispatchOn!(
                cluster.placements[1]!.identity.hostId,
                { type: 'Counter', key: 'c' },
                'bump',
                []
            )
        ).rejects.toThrow(/stateless/);
    });

    it('worker errors propagate to the targeted caller', async () => {
        cluster = await topo();
        await expect(
            cluster.placements[0]!.dispatchOn!(
                cluster.placements[2]!.identity.hostId,
                { type: 'Relay', key: 'k' },
                'boom',
                []
            )
        ).rejects.toThrow(/relay boom/);
    });
});

describe('workerOn()', () => {
    it('is a typed proxy over dispatchOn', async () => {
        cluster = await topo();
        const relay = workerOn(cluster.placements[0]!, cluster.placements[2]!.descriptor(), Relay);
        await expect(relay.double(5)).resolves.toBe(10);
        expect(cluster.placements[2]!.counters().inboundDispatches).toBe(1);
    });

    it('is not thenable, and method members are stable', async () => {
        cluster = await topo();
        const relay = workerOn(cluster.placements[0]!, cluster.placements[2]!.descriptor(), Relay);
        // `await relay` / `Promise.resolve(relay)` must NOT dispatch a
        // method literally named "then".
        const resolved = await Promise.resolve(relay);
        expect(resolved).toBe(relay);
        expect(cluster.placements[0]!.counters().targetedDispatches).toBe(0);
        // One function per method, not a fresh allocation per touch.
        expect(relay.double).toBe(relay.double);
    });
});

describe('addCounters mixed-version hardening', () => {
    it('tolerates a report missing newer fields — sums to the number, never NaN', () => {
        const legacy = { ...createCounters() } as Partial<ClusterCounterTotals>;
        delete legacy.targetedDispatches;
        const mine = createCounters();
        mine.targetedDispatches = 3;
        const sum = addCounters(mine, legacy as ClusterCounterTotals);
        expect(sum.targetedDispatches).toBe(3);
        for (const [key, value] of Object.entries(sum)) {
            expect(Number.isNaN(value), `${key} is NaN`).toBe(false);
        }
    });
});
