/**
 * Stateless workers in a CLUSTER: always-local dispatch with zero directory
 * traffic — no claims, no lookups, no releases — `locate()` answering local
 * on every host, invisibility to fencing and rebalancing, and the contrast
 * with a stateful type on the same hosts.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { defineActor, defineWorker } from '@sigx/actors';
import { createHost } from '@sigx/actors/host';
import {
    clusterPlacement,
    memoryClusterHub,
    type ClusterMembership
} from '@sigx/actors/cluster';
import { createCluster, quiet, type ClusterHarness } from './harness';

const NUL = String.fromCharCode(0);

const Work = defineWorker({
    type: 'Work',
    allowAnonymous: true,
    maxLocal: 2,
    methods: (ctx) => ({
        async double(n: number) {
            return n * 2;
        },
        async who() {
            return ctx.key;
        }
    })
});

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

let cluster: ClusterHarness | null = null;
afterEach(async () => {
    await cluster?.stop();
    cluster = null;
});

describe('stateless workers in a cluster', () => {
    it('worker dispatch performs ZERO directory operations, on every host', async () => {
        cluster = await createCluster(2, { actors: [Work, Counter] });
        for (const host of cluster.hosts) {
            for (let i = 0; i < 5; i++) {
                await expect(host.actor(Work, 'any').double(i)).resolves.toBe(i * 2);
            }
            await expect(host.actor(Work, 'other').who()).resolves.toBe('other');
        }
        for (const p of cluster.placements) {
            const c = p.counters();
            expect(c.directoryClaims).toBe(0);
            expect(c.directoryLookups).toBe(0);
            expect(c.directoryReleases).toBe(0);
            expect(c.claimConflicts).toBe(0);
            expect(c.claimed).toBe(0);
            expect(c.remoteDispatches).toBe(0);
        }
        // Nothing was ever written to the shared directory for the worker…
        await expect(cluster.hub.directory.lookup(`Work${NUL}any`)).resolves.toBeNull();
        // …while a stateful actor on the same hosts claims as usual (on
        // whichever host the policy placed it).
        await cluster.hosts[0]!.actor(Counter, 'c').bump();
        const claims = cluster.placements.reduce((n, p) => n + p.counters().directoryClaims, 0);
        expect(claims).toBeGreaterThan(0);
        await expect(cluster.hub.directory.lookup(`Counter${NUL}c`)).resolves.not.toBeNull();
    });

    it('locate() answers local for a worker on EVERY host — no 421 redirects', async () => {
        cluster = await createCluster(3, { actors: [Work] });
        for (const p of cluster.placements) {
            await expect(p.locate({ type: 'Work', key: 'any' })).resolves.toMatchObject({
                local: true
            });
            expect(p.counters().locateRemote).toBe(0);
        }
    });

    it('a fenced host keeps serving workers', async () => {
        cluster = await createCluster(2, { actors: [Work, Counter] });
        const fenced = cluster.hosts[1]!;
        // Warm the pool BEFORE the crash, and prove it re-dispatches after.
        await expect(fenced.actor(Work, 'k').double(2)).resolves.toBe(4);
        cluster.crash(1); // membership drops host 1 → it self-fences
        await vi.waitFor(() =>
            expect(cluster!.placements[1]!.counters().status).toBe('fenced')
        );
        // Fencing defends the single-activation invariant (it refuses LOCAL
        // stateful activation at the claim point). Workers have no such
        // invariant, so the fenced host keeps serving pure compute — warm
        // pools and fresh keys alike.
        await expect(fenced.actor(Work, 'k').double(3)).resolves.toBe(6);
        await expect(fenced.actor(Work, 'fresh').double(4)).resolves.toBe(8);
    });

    it('a WORKER-ONLY host rejoins instead of fencing forever (#272)', async () => {
        // The split web/engine deployment: the web tier joins the cluster
        // registering nothing but workers. It holds no directory claim by
        // construction, so its membership lapse cost the cluster nothing —
        // and the fence it used to take was permanent, leaving a zombie pod
        // that only a process restart could clear.
        cluster = await createCluster(2, {
            actors: [Work, Counter],
            actorsFor: (i) => (i === 1 ? [Work] : [Work, Counter])
        });
        const placement = cluster.placements[1]!;
        const hostId = placement.identity.hostId;
        cluster.hub.kill(hostId); // membership drops it → onSelfSuspect

        await vi.waitFor(() => expect(placement.counters().rejoins).toBe(1));
        // Same identity, deliberately: no directory entry anywhere names a
        // host that never claims, so nothing stale can be resurrected by
        // re-registering it.
        expect(placement.identity.hostId).toBe(hostId);
        expect(placement.counters().status).not.toBe('fenced');
        expect(placement.view().hosts.map((h) => h.hostId)).toContain(hostId);
        // It served pure compute throughout, and is addressable again.
        await expect(cluster.hosts[1]!.actor(Work, 'k').double(21)).resolves.toBe(42);
        await expect(
            cluster.placements[0]!.dispatchOn!(hostId, { type: 'Work', key: 'k' }, 'double', [4])
        ).resolves.toBe(8);
    });

    it('the rejoin keeps retrying while the store is down, and stops at stop()', async () => {
        // The lapse and the outage are the same event: the store that
        // dropped us is the store we have to re-register with. A single
        // attempt would fence-by-another-name.
        const hub = memoryClusterHub();
        const providers = hub.providers();
        let down = false;
        const attempts: string[] = [];
        const membership: ClusterMembership = {
            ...providers.membership,
            leave: async () => {
                if (down) {
                    attempts.push('leave');
                    throw new Error('store unreachable');
                }
                return providers.membership.leave();
            },
            join: async (descriptor) => {
                if (down) {
                    attempts.push('join');
                    throw new Error('store unreachable');
                }
                return providers.membership.join(descriptor);
            }
        };
        const placement = clusterPlacement({
            membership,
            directory: providers.directory,
            advertise: 'http://worker.test',
            retryBackoffMs: 1
        });
        const host = createHost({ actors: [Work], placement, defaults: quiet });
        await host.start();
        try {
            down = true;
            hub.kill(placement.identity.hostId);
            await vi.waitFor(() => expect(attempts.length).toBeGreaterThan(0));
            expect(placement.counters().rejoinAttempts).toBe(1);
            expect(placement.counters().rejoins).toBe(0);
            expect(placement.counters().status).not.toBe('fenced');
            // Pure compute never stopped, which is the whole point of not
            // fencing a host that has nothing to fence.
            await expect(host.actor(Work, 'k').double(3)).resolves.toBe(6);

            down = false;
            await vi.waitFor(() => expect(placement.counters().rejoins).toBe(1), {
                timeout: 5000
            });
            expect(placement.view().hosts.map((h) => h.hostId)).toContain(
                placement.identity.hostId
            );
        } finally {
            await host.stop({ timeoutMs: 1000 });
        }
    });

    it('a join that lands AFTER stop() is undone, not counted', async () => {
        // `stop()` sets the flag and wakes the backoff sleep, but the loop
        // may be parked inside `join()` at that moment — and `stop()`'s own
        // `leave()` can run first. Without the check after the await, that
        // join re-registers a host that is shutting down, heartbeat and all,
        // and leaves a stale entry behind forever.
        const hub = memoryClusterHub();
        const providers = hub.providers();
        // A pass-through membership whose `join` is swapped for a GATED one
        // once the host is up, so only the rejoin parks.
        const membership: ClusterMembership = { ...providers.membership };
        const placement = clusterPlacement({
            membership,
            directory: providers.directory,
            advertise: 'http://worker.test'
        });
        const host = createHost({ actors: [Work], placement, defaults: quiet });
        await host.start();
        const hostId = placement.identity.hostId;
        let releaseJoin: (() => void) | null = null;
        let joining: Promise<void> | null = null;
        membership.join = (descriptor) => {
            joining = new Promise<void>((resolve) => {
                releaseJoin = () => void providers.membership.join(descriptor).then(resolve);
            });
            return joining;
        };

        hub.kill(hostId);
        await vi.waitFor(() => expect(releaseJoin).not.toBeNull());
        // Shutdown overtakes the in-flight join.
        await host.stop({ timeoutMs: 1000 });
        releaseJoin!();
        await joining;

        await vi.waitFor(() =>
            expect(hub.providers().membership.view().hosts.map((h) => h.hostId)).not.toContain(
                hostId
            )
        );
        // Never back, so never counted.
        expect(placement.counters().rejoins).toBe(0);
    });

    it('a host registering ANY stateful type still fences — the invariant it defends', async () => {
        // The contrast that makes the rule readable: the rejoin is not "a
        // fence is survivable", it is "there was nothing to fence". One
        // stateful registration is enough to bring the terminal fence back,
        // whether or not that type is currently activated here.
        cluster = await createCluster(2, { actors: [Work, Counter] });
        const placement = cluster.placements[1]!;
        cluster.hub.kill(placement.identity.hostId);
        await vi.waitFor(() => expect(placement.counters().status).toBe('fenced'));
        expect(placement.counters().rejoinAttempts).toBe(0);
        expect(placement.counters().selfFences).toBe(1);
    });

    it('rebalance() never moves workers — a pool holds no claims', async () => {
        cluster = await createCluster(2, { actors: [Work] });
        const host = cluster.hosts[0]!;
        for (let i = 0; i < 6; i++) await host.actor(Work, `k${i}`).double(i);
        const report = await cluster.placements[0]!.rebalance({ minIdleMs: 0 });
        expect(report.moved).toBe(0);
    });
});
