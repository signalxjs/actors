/**
 * placement.rebalance() and cluster({ rebalance }) (#241).
 *
 * The contract: one round is total (a report, never a throw), sheds only
 * this host's own excess down to the cluster mean, bounded by maxMoves,
 * skips busy/held/recently-active actors, and never acts on missing data —
 * unreachable peers are excluded from the mean, and no answering peer at
 * all means no action. Every migration goes through the existing
 * `migrate()` primitive, so claims release and state survives re-placement.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import { preferLocalPolicy } from '@sigx/actors/cluster';
import { createCluster, selfPolicy, type ClusterHarness } from './harness';

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ n: 0 }),
    methods: (ctx) => ({
        async bump() {
            ctx.state.n += 1;
            await ctx.save();
            return ctx.state.n;
        },
        async n() {
            return ctx.state.n;
        }
    })
});

let cluster: ClusterHarness | null = null;

afterEach(async () => {
    await cluster?.stop();
    cluster = null;
});

/** Seed `count` activations on host `i` (Counter pins to the caller). */
async function seed(harness: ClusterHarness, i: number, count: number): Promise<void> {
    for (let k = 0; k < count; k++) await harness.hosts[i]!.actor(Counter, `${i}-${k}`).bump();
}

describe('placement.rebalance()', () => {
    it('sheds down to the mean, bounded by maxMoves, releasing claims', async () => {
        cluster = await createCluster(2, {
            actors: [Counter],
            typePolicies: { Counter: preferLocalPolicy() }
        });
        await seed(cluster, 0, 30);
        // own 30, peer 0 → mean 15, excess 15, bounded to maxMoves 10.
        const report = await cluster.placements[0]!.rebalance({ minIdleMs: 0 });
        expect(report).toEqual({ own: 30, peers: 1, mean: 15, moved: 10 });
        expect(cluster.hosts[0]!.stats().activations).toBe(20);
        const counters = cluster.placements[0]!.counters();
        expect(counters.rebalanceRounds).toBe(1);
        expect(counters.rebalanceMigrations).toBe(10);
        // A shed actor's claim is gone: the directory answers null for
        // exactly the migrated keys.
        const directoryKey = (k: string) => ['Counter', k].join(String.fromCharCode(0));
        let released = 0;
        for (let k = 0; k < 30; k++) {
            if ((await cluster.hub.directory.lookup(directoryKey(`0-${k}`))) === null) released++;
        }
        expect(released).toBe(10);
    });

    it('a shed actor reactivates on demand with state intact', async () => {
        cluster = await createCluster(2, {
            actors: [Counter],
            typePolicies: { Counter: preferLocalPolicy() }
        });
        await seed(cluster, 0, 8);
        const report = await cluster.placements[0]!.rebalance({
            minIdleMs: 0,
            threshold: 1.0,
            maxMoves: 4
        });
        expect(report.moved).toBe(4);
        // Every key still answers, wherever it now lives, with its state.
        for (let k = 0; k < 8; k++) {
            await expect(cluster.hosts[1]!.actor(Counter, `0-${k}`).n()).resolves.toBe(1);
        }
    });

    it('reports balanced without moving when within threshold', async () => {
        cluster = await createCluster(2, { actors: [Counter], policy: selfPolicy });
        await seed(cluster, 0, 5);
        await seed(cluster, 1, 5);
        const report = await cluster.placements[0]!.rebalance({ minIdleMs: 0 });
        expect(report.reason).toBe('balanced');
        expect(report.moved).toBe(0);
        expect(cluster.hosts[0]!.stats().activations).toBe(5);
    });

    it('never acts on missing data: all peers unreachable → no-peers, no moves', async () => {
        cluster = await createCluster(2, {
            actors: [Counter],
            typePolicies: { Counter: preferLocalPolicy() },
            retries: 0
        });
        await seed(cluster, 0, 20);
        cluster.unbind(1);
        const report = await cluster.placements[0]!.rebalance({
            minIdleMs: 0,
            timeoutMs: 200
        });
        expect(report.reason).toBe('no-peers');
        expect(report.moved).toBe(0);
        expect(cluster.hosts[0]!.stats().activations).toBe(20);
        // Still counted: a partitioned host's cadence must stay observable.
        expect(cluster.placements[0]!.counters().rebalanceRounds).toBe(1);
    });

    it('skips recently-active actors: minIdleMs filters to no-candidates', async () => {
        cluster = await createCluster(2, {
            actors: [Counter],
            typePolicies: { Counter: preferLocalPolicy() }
        });
        await seed(cluster, 0, 12);
        const report = await cluster.placements[0]!.rebalance({ minIdleMs: 60_000 });
        expect(report.reason).toBe('no-candidates');
        expect(cluster.hosts[0]!.stats().activations).toBe(12);
    });

    it('a single host is a no-peers no-op', async () => {
        cluster = await createCluster(1, { actors: [Counter], policy: selfPolicy });
        await seed(cluster, 0, 5);
        const report = await cluster.placements[0]!.rebalance({ minIdleMs: 0 });
        expect(report.reason).toBe('no-peers');
    });
});

describe('cluster({ rebalance })', () => {
    it('runs rounds on the cadence and stops with the app', async () => {
        cluster = await createCluster(2, {
            actors: [Counter],
            typePolicies: { Counter: preferLocalPolicy() },
            rebalance: { intervalMs: 100, minIdleMs: 0, maxMoves: 5 }
        });
        await seed(cluster, 0, 20);
        // Two or three ticks: 20→15→12-ish; assert it moved without being
        // asked and stayed bounded per round.
        await new Promise((r) => setTimeout(r, 350));
        const shed = 20 - cluster.hosts[0]!.stats().activations;
        expect(shed).toBeGreaterThanOrEqual(5);
        const counters = cluster.placements[0]!.counters();
        expect(counters.rebalanceMigrations).toBe(shed);
        expect(counters.rebalanceRounds).toBeGreaterThanOrEqual(2);
        // Stop cancels the loop: counters freeze.
        await cluster.stop();
        const frozen = cluster.placements[0]!.counters().rebalanceRounds;
        await new Promise((r) => setTimeout(r, 250));
        expect(cluster.placements[0]!.counters().rebalanceRounds).toBe(frozen);
        cluster = null;
    });
});
