/**
 * Stateless workers in a CLUSTER: always-local dispatch with zero directory
 * traffic — no claims, no lookups, no releases — `locate()` answering local
 * on every host, invisibility to fencing and rebalancing, and the contrast
 * with a stateful type on the same hosts.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { defineActor, defineWorker } from '@sigx/actors';
import { createCluster, type ClusterHarness } from './harness';

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

    it('rebalance() never moves workers — a pool holds no claims', async () => {
        cluster = await createCluster(2, { actors: [Work] });
        const host = cluster.hosts[0]!;
        for (let i = 0; i < 6; i++) await host.actor(Work, `k${i}`).double(i);
        const report = await cluster.placements[0]!.rebalance({ minIdleMs: 0 });
        expect(report.moved).toBe(0);
    });
});
