/**
 * Milestone 9 — the cluster-wide half of the ops surface.
 *
 * `clusterStats()` could always aggregate TOPOLOGY, because `HostReport`
 * carried it. It could not aggregate behaviour, because the report carried
 * no metrics, no health and no actors — so a dashboard showed one host's
 * `calls.total` and `errors.byKind` directly beneath cluster-wide totals,
 * with nothing saying which was which.
 *
 * Two claims are load-bearing here and both are asserted below.
 *
 * **Percentiles are re-derived, never averaged.** `HistogramSnapshot` is
 * p50/p90/p99 with no buckets, and the mean of two hosts' p99s is not the
 * p99 of anything. The digest carries bucket COUNTS so the cluster's
 * distribution can be summed and its percentiles taken from the result.
 *
 * **Coverage is published, not assumed.** A host with no `metrics()`, or
 * one on a build that predates the digest, contributes nothing — and totals
 * that quietly cover two thirds of the fleet look exactly like totals that
 * cover all of it. `totals.metrics.hosts` is the denominator that makes the
 * difference visible.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import { clusterStats } from '@sigx/actors/cluster';
import { createMetricsAccumulator, health, metrics, type MetricsPlugin } from '@sigx/actors/host';
import { createCluster, selfPolicy, type ClusterHarness } from './harness';

let running: ClusterHarness | null = null;
afterEach(async () => {
    await running?.stop();
    running = null;
});

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        }
    })
});

/** N hosts, each with its own `metrics()`, each activating its own actors. */
async function instrumented(
    count: number,
    only?: (index: number) => boolean
): Promise<{ cluster: ClusterHarness; plugins: MetricsPlugin[] }> {
    const plugins: MetricsPlugin[] = [];
    const cluster = await createCluster(count, {
        actors: [Counter],
        policy: selfPolicy,
        plugins: (i) => {
            if (only && !only(i)) return [];
            const plugin = metrics();
            plugins[i] = plugin;
            return [plugin, health()];
        }
    });
    return { cluster, plugins };
}


/**
 * Make one peer answer like a build that predates a field.
 *
 * Strips the named keys from its `$sigx:host#stats` response, which is what
 * a mid-rolling-deploy host genuinely sends: still `v: 1`, just without the
 * optional fields this build added. Simulated on the wire rather than by
 * configuring the peer differently, because the thing under test is
 * precisely how the COLLECTOR handles a payload it did not fully expect.
 */
function olderPeer(
    host: string,
    drop: readonly string[]
): (inner: typeof globalThis.fetch) => typeof globalThis.fetch {
    return (inner) => async (input, init) => {
        const response = await inner(input, init);
        const url = new Request(input, init).url;
        if (!url.includes(host) || !url.includes('stats')) return response;
        const body = (await response.json()) as { data?: Record<string, unknown> };
        for (const key of drop) delete body.data?.[key];
        return new Response(JSON.stringify(body), {
            status: response.status,
            headers: response.headers
        });
    };
}

describe('cluster ops: cluster-wide metrics', () => {
    it('sums calls, storage and churn across every host', async () => {
        const { cluster } = await instrumented(3);
        running = cluster;
        // Different traffic per host, so totals that actually read only ONE
        // of them would be indistinguishable from totals that read all.
        for (let i = 0; i < 3; i++) {
            for (let n = 0; n <= i; n++) {
                await cluster.hosts[i]!.actor(Counter, `s${i}-${n}`).increment(1);
            }
        }

        const report = await clusterStats(cluster.placements[0]!);
        const totals = report.totals.metrics;
        expect(totals).not.toBeNull();
        expect(totals!.hosts).toBe(3);
        // 1 + 2 + 3 calls, each saving once and creating an actor.
        expect(totals!.calls.total).toBe(6);
        expect(totals!.storage.saves).toBe(6);
        expect(totals!.activations.created).toBe(6);
        // A capped breakdown still adds up to the total it belongs to.
        const byType = Object.values(totals!.byType).reduce((n, row) => n + row.calls, 0);
        expect(byType).toBe(totals!.calls.total);
    });

    it('derives cluster latency from MERGED buckets, not averaged percentiles', async () => {
        const { cluster, plugins } = await instrumented(2);
        running = cluster;
        for (let n = 0; n < 40; n++) await cluster.hosts[0]!.actor(Counter, `fast-${n}`).increment(1);
        await cluster.hosts[1]!.actor(Counter, 'slow').increment(1);

        const report = await clusterStats(cluster.placements[0]!);
        const merged = report.totals.metrics!.turnMs!;
        // Every sample from both hosts is present — which is what makes
        // these the CLUSTER's percentiles rather than one host's.
        const counts = plugins.map((p) => p.snapshot().turnMs!.count);
        expect(merged.count).toBe(counts[0]! + counts[1]!);
        // And the result is bounded by observations that really happened,
        // which an average of percentiles is not required to be.
        const slowest = Math.max(...plugins.map((p) => p.snapshot().turnMs!.maxMs));
        expect(merged.p99Ms).toBeLessThanOrEqual(slowest + 1);
        expect(merged.meanMs).toBeGreaterThan(0);
    });

    it('publishes how many hosts reported, so a partial picture cannot pass as whole', async () => {
        // Two of three carry `metrics()`. The totals are then a LOWER
        // BOUND, and the denominator is the only way to know it.
        const { cluster } = await instrumented(3, (i) => i !== 2);
        running = cluster;
        for (let i = 0; i < 3; i++) await cluster.hosts[i]!.actor(Counter, `s${i}`).increment(1);

        const report = await clusterStats(cluster.placements[0]!);
        expect(report.totals.hosts).toBe(3);
        expect(report.totals.metrics!.hosts).toBe(2);
        // Not an error and not a partial fan-out: the third host ANSWERED,
        // it simply has no metrics to give.
        expect(report.partial).toBe(false);
        expect(report.hosts.some((s) => s.metrics === undefined)).toBe(true);
    });

    it('reports null rather than zeroes when nothing anywhere is instrumented', async () => {
        // A wall of zeroes reads as a cluster serving no traffic, which is
        // a different and much more alarming fact.
        const cluster = await createCluster(2, { actors: [Counter], policy: selfPolicy });
        running = cluster;
        await cluster.hosts[0]!.actor(Counter, 'a').increment(1);
        const report = await clusterStats(cluster.placements[0]!);
        expect(report.totals.metrics).toBeNull();
    });

    it('keeps a foreign bucket layout out of the distribution but not out of the counts', async () => {
        const { cluster } = await instrumented(1);
        running = cluster;
        await cluster.hosts[0]!.actor(Counter, 'a').increment(1);
        const report = await clusterStats(cluster.placements[0]!);
        const digest = report.hosts[0]!.metrics!;

        const accumulator = createMetricsAccumulator();
        expect(accumulator.add({ ...digest, layout: 'll-9-99' })).toBe('counts-only');
        // Its calls still count; only its incomparable buckets are dropped.
        // Dropping the host entirely would be a worse lie than dropping its
        // percentiles.
        expect(accumulator.totals().calls.total).toBe(digest.calls.total);
        expect(accumulator.totals().turnMs).toBeNull();
    });

    it('merges oneWayFailures, and a digest predating the field folds in as 0', async () => {
        const { cluster } = await instrumented(1);
        running = cluster;
        await cluster.hosts[0]!.actor(Counter, 'a').increment(1);
        const report = await clusterStats(cluster.placements[0]!);
        const digest = report.hosts[0]!.metrics!;

        const accumulator = createMetricsAccumulator();
        accumulator.add({ ...digest, calls: { ...digest.calls, oneWayFailures: 3 } });
        // An older host's digest has no key at all — additive within v1.
        const { oneWayFailures: _absent, ...legacyCalls } = digest.calls;
        accumulator.add({ ...digest, calls: legacyCalls });
        expect(accumulator.totals().calls.oneWayFailures).toBe(3);
    });

    it('rejects a digest from a future wire version outright', async () => {
        const accumulator = createMetricsAccumulator();
        expect(accumulator.add({ v: 2 } as never)).toBe('rejected');
        expect(accumulator.add(null)).toBe('rejected');
        expect(accumulator.totals().calls.total).toBe(0);
    });

    it('never lets a peer subtract from a cluster total', async () => {
        // Counters arrive over a wire. One negative — from a buggy build, a
        // truncated payload or a hostile one — would otherwise produce a
        // figure that cannot describe anything that happened.
        const { cluster } = await instrumented(1);
        running = cluster;
        await cluster.hosts[0]!.actor(Counter, 'a').increment(1);
        const report = await clusterStats(cluster.placements[0]!);
        const good = report.hosts[0]!.metrics!;

        const accumulator = createMetricsAccumulator();
        accumulator.add(good);
        accumulator.add({
            ...good,
            calls: { total: -1e9, failed: -5, streams: Number.NaN },
            storage: { loads: -1, saves: -1, clears: -1, conflicts: -1 },
            activations: { created: -3, destroyed: -3, byReason: { idle: -7 } },
            errors: { byKind: { 'call-timeout': -2 } },
            byType: { Counter: { calls: -10, failed: -10 } }
        });
        const totals = accumulator.totals();
        expect(totals.calls.total).toBe(good.calls.total);
        expect(totals.calls.failed).toBeGreaterThanOrEqual(0);
        expect(totals.storage.loads).toBeGreaterThanOrEqual(0);
        expect(totals.activations.created).toBeGreaterThanOrEqual(0);
        expect(totals.errors.byKind['call-timeout'] ?? 0).toBeGreaterThanOrEqual(0);
        expect(totals.byType.Counter!.calls).toBeGreaterThanOrEqual(0);
    });

    it('carries every host’s health, not just the collector’s', async () => {
        const { cluster } = await instrumented(2);
        running = cluster;
        const report = await clusterStats(cluster.placements[0]!);
        // The health tally covers the fleet: a single-host read cannot see
        // that a PEER is out of rotation.
        expect(report.totals.health.ready + report.totals.health.notReady).toBe(2);
        expect(report.totals.health.unknown).toBe(0);
        expect(report.hosts.every((s) => s.health !== undefined)).toBe(true);
    });

    it('reports readiness even where the health() ENDPOINT is not mounted', async () => {
        // `health()` serves a route; the checks themselves come from the
        // registry, and `cluster()` always contributes one. So a host with
        // no health endpoint still has a readiness answer to give, which is
        // the whole reason this rides `HostReport` rather than an HTTP hop
        // to each peer.
        const { cluster } = await instrumented(2, (i) => i === 0);
        running = cluster;
        const report = await clusterStats(cluster.placements[0]!);
        expect(report.totals.health.unknown).toBe(0);
        expect(report.hosts.every((s) => s.health?.checks.cluster !== undefined)).toBe(true);
    });

    it('counts a peer that sent no health at all as unknown, not as ready', async () => {
        // The mid-rolling-deploy case: a peer on a build that predates the
        // field. Counting it as ready would report a fleet healthier than
        // the one that was measured.
        const cluster = await createCluster(2, {
            actors: [Counter],
            policy: selfPolicy,
            plugins: () => [metrics()],
            wrapFetch: olderPeer('host1', ['health'])
        });
        running = cluster;
        const report = await clusterStats(cluster.placements[0]!);
        expect(report.hosts).toHaveLength(2);
        // It answered, so it is neither unreachable nor a partial fan-out.
        expect(report.unreachable).toEqual([]);
        expect(report.partial).toBe(false);
        expect(report.totals.health.unknown).toBe(1);
    });
});

describe('cluster ops: the detail flag', () => {
    it('leaves the actor list off unless detail asks for it', async () => {
        const { cluster } = await instrumented(2);
        running = cluster;
        for (let n = 0; n < 5; n++) await cluster.hosts[0]!.actor(Counter, `g${n}`).increment(1);

        const cheap = await clusterStats(cluster.placements[0]!);
        // Actor KEYS are the one field on this wire that can be personal
        // data, and the walk is O(activations) on every host at once.
        expect(cheap.hosts.every((s) => s.activations === undefined)).toBe(true);
        // The digest, by contrast, always travels — it is what makes the
        // totals mean anything, and it is the cheap part.
        expect(cheap.hosts.every((s) => s.metrics !== undefined)).toBe(true);

        const detailed = await clusterStats(cluster.placements[0]!, { detail: { activations: 3 } });
        const self = detailed.hosts.find((s) => s.hostId === detailed.from)!;
        expect(self.activations!.length).toBeLessThanOrEqual(3);
        expect(self.activations![0]!.type).toBe('Counter');
    });

    it('asks only the named hosts for the expensive parts', async () => {
        const { cluster } = await instrumented(3);
        running = cluster;
        for (let i = 0; i < 3; i++) await cluster.hosts[i]!.actor(Counter, `g${i}`).increment(1);

        const target = cluster.placements[1]!.identity.hostId;
        const report = await clusterStats(cluster.placements[0]!, {
            detail: { activations: 5, hosts: [target] }
        });
        // Drill-down is almost always ONE host; asking all N for actor
        // lists at 1 Hz is the cost that actually matters.
        expect(report.hosts.find((s) => s.hostId === target)!.activations).toBeDefined();
        expect(
            report.hosts.filter((s) => s.hostId !== target).every((s) => s.activations === undefined)
        ).toBe(true);
    });

    it('clamps a hostile request at the RESPONDER', async () => {
        const { cluster } = await instrumented(1);
        running = cluster;
        for (let n = 0; n < 30; n++) await cluster.hosts[0]!.actor(Counter, `g${n}`).increment(1);

        // The request arrives over a wire. HMAC proves WHO is asking; it
        // says nothing about whether walking a billion actors is a
        // reasonable thing to be asked for.
        const report = cluster.placements[0]!.report({
            metrics: true,
            activations: 1e9,
            methods: 1e9,
            errors: 1e9
        });
        expect(report.activations!.length).toBeLessThanOrEqual(200);
        expect(report.metrics!.errors.recent!.length).toBeLessThanOrEqual(32);
        expect(Object.keys(report.metrics!.byMethod).length).toBeLessThanOrEqual(256);
    });

    it('ignores nonsense in a request rather than failing the read', async () => {
        const { cluster } = await instrumented(1);
        running = cluster;
        const report = cluster.placements[0]!.report({
            metrics: true,
            activations: Number.NaN,
            errors: -5
        } as never);
        // The endpoint that explains a broken host must not be the thing
        // that breaks on a malformed question.
        expect(report.v).toBe(1);
        expect(report.activations).toBeUndefined();
        expect(report.metrics!.errors.recent).toBeUndefined();
    });

    it('does not put the digest in the LOCAL ops section as well', async () => {
        const { cluster } = await instrumented(1);
        running = cluster;
        // `ops.metrics` sits right beside `ops.cluster` in the same
        // snapshot; carrying buckets here too would ship the same numbers
        // twice on a route that gets polled once a second.
        const local = cluster.placements[0]!.report();
        expect(local.metrics).toBeUndefined();
        expect(local.activations).toBeUndefined();
        expect(local.health).toBeDefined();
    });
});

describe('cluster ops: cost', () => {
    it('reading the stats does not move the counters it reads — with detail on', async () => {
        const { cluster, plugins } = await instrumented(2);
        running = cluster;
        await cluster.hosts[0]!.actor(Counter, 'a').increment(1);

        await clusterStats(cluster.placements[0]!, { detail: true });
        const before = cluster.placements.map((p) => ({ ...p.counters() }));
        const callsBefore = plugins.map((p) => p.snapshot().calls.total);
        await clusterStats(cluster.placements[0]!, { detail: true });
        const after = cluster.placements.map((p) => ({ ...p.counters() }));

        for (let i = 0; i < before.length; i++) {
            expect(after[i]!.remoteDispatches).toBe(before[i]!.remoteDispatches);
            expect(after[i]!.inboundDispatches).toBe(before[i]!.inboundDispatches);
            expect(after[i]!.directoryLookups).toBe(before[i]!.directoryLookups);
        }
        // …and `metrics()` did not count the fan-out as traffic either.
        expect(plugins.map((p) => p.snapshot().calls.total)).toEqual(callsBefore);
    });

    it('stays small enough to poll', async () => {
        const { cluster } = await instrumented(3);
        running = cluster;
        for (let i = 0; i < 3; i++) {
            for (let n = 0; n < 10; n++) {
                await cluster.hosts[i]!.actor(Counter, `s${i}-${n}`).increment(1);
            }
        }
        const size = JSON.stringify(await clusterStats(cluster.placements[0]!)).length;
        // A budget, not a measurement. This rides an internal wire once a
        // second per host, and a breakdown that grows without anyone
        // noticing is exactly how that stops being affordable.
        expect(size).toBeLessThan(24_000);
    });
});
