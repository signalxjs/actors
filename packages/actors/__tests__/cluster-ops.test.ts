/**
 * Milestone 7 — the ops surface: `clusterStats()`, the cluster counters, and
 * cluster readiness.
 *
 * Two invariants are load-bearing here and worth stating up front.
 *
 * **Counting.** Every counter moves on exactly the ONE host where its event
 * happened, and the two sides of a cross-host call get different names
 * (`remoteDispatches` on the caller, `inboundDispatches` on the owner). They
 * are reported side by side and never summed — the gap between them is
 * itself the signal. There is a test asserting the arithmetic.
 *
 * **`metrics()` is not doubled.** `compositePlacement.bind()` hands the
 * placement the RAW local dispatcher, and `dispatchInbound` calls it
 * directly — so `useDispatch` middleware never sees an inbound hop. A
 * cross-host call is therefore counted ONCE by `metrics()`, on the caller,
 * while the owner sees the turn. That split is asserted below so a future
 * refactor cannot silently turn it into a double count.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import {
    clusterStats,
    HOST_STATS_SYMBOL,
    signAuth,
    HOST_CALL_HEADER,
    encodeEnvelope,
    type ClusterStatsReport,
    type HostReport
} from '@sigx/actors/cluster';
import {
    health,
    metrics,
    ops,
    type ActorPlugin,
    type MetricsPlugin,
    type OpsSnapshot
} from '@sigx/actors/host';
import { reminderShardKeys } from '../src/host/reminder-shards';
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
        },
        async get() {
            return ctx.state.count;
        }
    })
});

describe('cluster ops: clusterStats', () => {
    it('reports every host, and the totals are the sum of the parts', async () => {
        const cluster = await createCluster(3, { actors: [Counter], policy: selfPolicy });
        running = cluster;
        // selfPolicy: each host activates its own, so the population spreads.
        for (let i = 0; i < 3; i++) {
            for (let n = 0; n < i + 1; n++) {
                await cluster.hosts[i]!.actor(Counter, `s${i}-${n}`).increment(1);
            }
        }

        const report = await clusterStats(cluster.placements[0]!);
        expect(report.hosts).toHaveLength(3);
        expect(report.partial).toBe(false);
        expect(report.unreachable).toEqual([]);
        expect(report.from).toBe(cluster.placements[0]!.identity.hostId);
        expect(report.view.size).toBe(3);
        expect(report.view.active).toBe(3);

        const expected = cluster.hosts.reduce((sum, s) => sum + s.stats().activations, 0);
        expect(report.totals.activations).toBe(expected);
        expect(report.totals.activations).toBe(6);
        expect(report.totals.perType).toEqual({ Counter: 6 });
        expect(report.totals.hosts).toBe(3);

        // Each host reports its OWN gauges, not the collector's.
        for (const host of report.hosts) {
            const index = cluster.placements.findIndex((p) => p.identity.hostId === host.hostId);
            expect(host.stats.activations).toBe(cluster.hosts[index]!.stats().activations);
            expect(host.status).toBe('active');
            expect(host.v).toBe(1);
        }
    });

    it('the reminder shard map is a partition — every shard owned exactly once', async () => {
        const cluster = await createCluster(3, { actors: [Counter] });
        running = cluster;
        const report = await clusterStats(cluster.placements[0]!);

        const shards = reminderShardKeys();
        expect(Object.keys(report.reminderShards).sort()).toEqual([...shards].sort());
        for (const shard of shards) {
            // Exactly one claimant is the healthy case: none means nothing
            // ticks that shard, two means views have diverged.
            expect(report.reminderShards[shard]).toHaveLength(1);
        }
        // The #82 finding, made visible: how many hosts do reminder work.
        const working = new Set(Object.values(report.reminderShards).flat());
        expect(working.size).toBeGreaterThan(1);
        expect(working.size).toBeLessThanOrEqual(3);
    });

    it('a single-host cluster owns all 16 shards and needs no network', async () => {
        const cluster = await createCluster(1, {
            actors: [Counter],
            onRequest: () => expect.unreachable('a solo fan-out must not hit the wire')
        });
        running = cluster;
        const report = await clusterStats(cluster.placements[0]!);
        expect(report.hosts).toHaveLength(1);
        expect(report.hosts[0]!.reminderShards).toHaveLength(16);
    });

    it('a crashed host is LISTED, not thrown — the report still arrives', async () => {
        const cluster = await createCluster(3, { actors: [Counter], policy: selfPolicy });
        running = cluster;
        await cluster.hosts[1]!.actor(Counter, 'gone').increment(1);
        const dead = cluster.placements[1]!.identity.hostId;
        cluster.unbind(1); // address stops resolving, membership still lists it

        const report = await clusterStats(cluster.placements[0]!, { timeoutMs: 200 });
        expect(report.partial).toBe(true);
        expect(report.hosts.map((s) => s.hostId)).not.toContain(dead);
        expect(report.unreachable).toHaveLength(1);
        expect(report.unreachable[0]).toMatchObject({ hostId: dead, reason: 'unreachable' });
        // Totals are a lower bound, and `partial` is how you know.
        expect(report.totals.hosts).toBe(2);
        // Shards only the dead host claimed come back EMPTY rather than
        // vanishing — a missing key reads as "not measured".
        const orphaned = Object.entries(report.reminderShards).filter(([, o]) => o.length === 0);
        expect(orphaned.length).toBeGreaterThan(0);
    });

    it('bounds a hung peer with timeoutMs instead of hanging the operator', async () => {
        const cluster = await createCluster(2, {
            actors: [Counter],
            // A peer that never answers. Real fetch rejects when its signal
            // aborts, so the stall has to honour the signal too — otherwise
            // the test would prove nothing about the timeout.
            wrapFetch: (inner) => (input, init) => {
                const request = new Request(input, init);
                if (request.url.includes('host1.test') && request.url.includes('stats')) {
                    const signal = init?.signal ?? request.signal;
                    return new Promise<Response>((_resolve, reject) => {
                        signal?.addEventListener('abort', () =>
                            reject(
                                Object.assign(new Error('aborted'), { name: 'AbortError' })
                            )
                        );
                    });
                }
                return inner(input, init);
            }
        });
        running = cluster;
        const report = await clusterStats(cluster.placements[0]!, { timeoutMs: 30 });
        expect(report.partial).toBe(true);
        expect(report.unreachable[0]!.reason).toBe('timeout');
        // The healthy host still answered — one sick peer does not lose the
        // rest of the report.
        expect(report.hosts).toHaveLength(1);
    });

    it('reading stats does not move the counters it reads', async () => {
        const cluster = await createCluster(2, { actors: [Counter] });
        running = cluster;
        const before = cluster.placements[0]!.counters();
        await clusterStats(cluster.placements[0]!);
        const after = cluster.placements[0]!.counters();
        // The fan-out goes straight to the transport, never through routing.
        expect(after.remoteDispatches).toBe(before.remoteDispatches);
        expect(after.directoryLookups).toBe(before.directoryLookups);
        expect(after.routeCacheMisses).toBe(before.routeCacheMisses);
    });
});

describe('cluster ops: counters', () => {
    it('counts a cross-host call ONCE on each side, under different names', async () => {
        const cluster = await createCluster(2, { actors: [Counter], policy: selfPolicy });
        running = cluster;
        // Owned by host 1 (selfPolicy), called through host 0 → a real hop.
        await cluster.hosts[1]!.actor(Counter, 'remote').increment(1);
        const callerBefore = cluster.placements[0]!.counters().remoteDispatches;
        const ownerBefore = cluster.placements[1]!.counters().inboundDispatches;

        await cluster.hosts[0]!.actor(Counter, 'remote').increment(1);

        expect(cluster.placements[0]!.counters().remoteDispatches).toBe(callerBefore + 1);
        expect(cluster.placements[1]!.counters().inboundDispatches).toBe(ownerBefore + 1);
        // The owner never counts it as an outbound, and the caller never as
        // an inbound — that is what makes them safe to report side by side.
        expect(cluster.placements[1]!.counters().remoteDispatches).toBe(0);
        expect(cluster.placements[0]!.counters().inboundDispatches).toBe(0);
    });

    it('claims, releases and the claimed gauge track the directory', async () => {
        const cluster = await createCluster(2, { actors: [Counter], policy: selfPolicy });
        running = cluster;
        await cluster.hosts[0]!.actor(Counter, 'a').increment(1);
        await cluster.hosts[0]!.actor(Counter, 'b').increment(1);

        const counters = cluster.placements[0]!.counters();
        expect(counters.directoryClaims).toBe(2);
        expect(counters.directoryReleases).toBe(0);
        expect(counters.claimed).toBe(2);

        await cluster.hosts[0]!.deactivate({ type: 'Counter', key: 'a' });
        const after = cluster.placements[0]!.counters();
        expect(after.directoryReleases).toBe(1);
        expect(after.claimed).toBe(1);
    });

    it('the route cache misses once and then hits', async () => {
        const cluster = await createCluster(2, { actors: [Counter], policy: selfPolicy });
        running = cluster;
        await cluster.hosts[1]!.actor(Counter, 'hot').increment(1);

        await cluster.hosts[0]!.actor(Counter, 'hot').increment(1);
        const first = cluster.placements[0]!.counters();
        expect(first.routeCacheMisses).toBeGreaterThan(0);
        expect(first.directoryLookups).toBeGreaterThan(0);

        await cluster.hosts[0]!.actor(Counter, 'hot').increment(1);
        const second = cluster.placements[0]!.counters();
        expect(second.routeCacheHits).toBe(first.routeCacheHits + 1);
        // A cache hit costs no directory round-trip — that is the point.
        expect(second.directoryLookups).toBe(first.directoryLookups);
        expect(second.routeCacheSize).toBeGreaterThan(0);
    });

    it('a lost activation race is counted as a claim conflict AND a redirect', async () => {
        const cluster = await createCluster(2, { actors: [Counter], policy: selfPolicy });
        running = cluster;
        // Racing front doors on one key: both hosts try to claim, one loses
        // the directory race, throws wrong-host at itself, and re-routes to
        // the winner. Both calls still succeed.
        const results = await Promise.all([
            cluster.hosts[0]!.actor(Counter, 'shared').increment(1),
            cluster.hosts[1]!.actor(Counter, 'shared').increment(1)
        ]);
        expect([...results].sort()).toEqual([1, 2]);

        const all = cluster.placements.map((p) => p.counters());
        const sum = (pick: (c: (typeof all)[number]) => number): number =>
            all.reduce((total, c) => total + pick(c), 0);
        // Exactly one host lost the claim, consumed its own 421 and retried.
        expect(sum((c) => c.claimConflicts)).toBe(1);
        expect(sum((c) => c.wrongHostRedirects)).toBe(1);
        expect(sum((c) => c.retries)).toBe(1);
        // Convergence: nobody exhausted their attempts.
        expect(sum((c) => c.routingFailures)).toBe(0);
        // Both counts land on the SAME host — the loser refused its own
        // activation and then re-routed.
        const loser = all.find((c) => c.claimConflicts === 1)!;
        expect(loser.wrongHostRedirects).toBe(1);
    });

    it('membership changes are counted — the store-load signal from #82', async () => {
        const cluster = await createCluster(3, { actors: [Counter] });
        running = cluster;
        // Host 0 was present for hosts 1 and 2 joining.
        expect(cluster.placements[0]!.counters().membershipChanges).toBeGreaterThanOrEqual(2);
        expect(cluster.placements[0]!.counters().membershipVersion).toBeGreaterThan(0);
    });

    it('a departed host is swept, and the sweep is counted', async () => {
        const cluster = await createCluster(2, { actors: [Counter], policy: selfPolicy });
        running = cluster;
        await cluster.hosts[1]!.actor(Counter, 'orphan').increment(1);
        cluster.crash(1);
        await new Promise((r) => setTimeout(r, 20));

        const counters = cluster.placements[0]!.counters();
        expect(counters.hostSweeps).toBeGreaterThanOrEqual(1);
        expect(counters.sweptEntries).toBeGreaterThanOrEqual(1);
    });

    it('an HMAC rejection increments authFailures — otherwise a 403 is silent', async () => {
        const cluster = await createCluster(2, { actors: [Counter], secret: 'right' });
        running = cluster;
        expect(cluster.placements[1]!.counters().authFailures).toBe(0);

        const symbol = 'Counter#increment';
        const response = await cluster.fetch(
            `http://host1.test/_sigx/host/${encodeURIComponent(symbol)}`,
            {
                method: 'POST',
                headers: {
                    'content-type': 'application/json',
                    [HOST_CALL_HEADER]: encodeEnvelope(
                        { callChain: [], callId: 'c1.forged' },
                        's.attacker'
                    ),
                    'x-sigx-cluster-auth': await signAuth('wrong', symbol, 'c1.forged')
                },
                body: JSON.stringify({ args: ['x', 1] })
            }
        );
        expect(response.status).toBe(403);
        expect(cluster.placements[1]!.counters().authFailures).toBe(1);
    });
});

describe('cluster ops: readiness', () => {
    it('reports not-ready while DRAINING, and still serves — the LB contract', async () => {
        const cluster = await createCluster(2, {
            actors: [Counter],
            plugins: () => [health()]
        });
        running = cluster;
        const app = cluster.apps[0]!;
        const probe = (): Promise<Response> => {
            const route = app.routes.find((r) =>
                r.match(new Request('http://host0.test/_sigx/health/ready'))
            )!;
            return route.handle(new Request('http://host0.test/_sigx/health/ready'), app.host!);
        };

        expect((await probe()).status).toBe(200);

        // beginStop announces `leaving` BEFORE the drain — which is exactly
        // the window a balancer must stop sending traffic in, and the host
        // is still very much alive and answering.
        await cluster.placements[0]!.beginStop?.();

        const draining = await probe();
        expect(draining.status).toBe(503);
        const body = await draining.json();
        expect(body.checks.cluster.ready).toBe(false);
        expect(body.checks.cluster.detail).toMatch(/leaving/);
        // Still serving calls throughout the handoff.
        await expect(cluster.hosts[0]!.actor(Counter, 'x').increment(1)).resolves.toBe(1);
    });

    it('a FENCED host reports not-ready, though its published status still says active', async () => {
        const cluster = await createCluster(2, {
            actors: [Counter],
            plugins: () => [health()]
        });
        running = cluster;
        // Self-suspect: membership lost. `#fence()` deliberately leaves the
        // PUBLISHED status alone, so without this the balancer would keep
        // feeding a host that refuses every activation — a black hole.
        cluster.hub.kill(cluster.placements[0]!.identity.hostId);
        await new Promise((r) => setTimeout(r, 20));

        expect(cluster.placements[0]!.descriptor().status).toBe('active');
        expect(cluster.placements[0]!.counters().status).toBe('fenced');

        const app = cluster.apps[0]!;
        const route = app.routes.find((r) =>
            r.match(new Request('http://host0.test/_sigx/health/ready'))
        )!;
        const response = await route.handle(
            new Request('http://host0.test/_sigx/health/ready'),
            app.host!
        );
        expect(response.status).toBe(503);
        expect((await response.json()).checks.cluster.detail).toMatch(/fenced/);
    });

    it('a FENCED host fails LIVENESS too — fenced is terminal, restart is the medicine', async () => {
        // Leaving = draining = alive (restart would abort the handoff).
        // Fenced = membership lost for THIS host identity, every activation
        // refused, and no way back — a fenced pod that stays "live" is a
        // zombie no orchestrator will ever fix (#141: an outage of the
        // membership store left the whole cluster dead until a human
        // restarted the pods).
        const cluster = await createCluster(2, {
            actors: [Counter],
            plugins: () => [health()]
        });
        running = cluster;

        const probeLive = async (index: number) => {
            const app = cluster.apps[index]!;
            const request = new Request(`http://host${index}.test/_sigx/health`);
            const route = app.routes.find((r) => r.match(request))!;
            return route.handle(request, app.host!);
        };

        // Draining stays live: beginStop announces leaving, liveness 200.
        await cluster.placements[1]!.beginStop?.();
        expect((await probeLive(1)).status).toBe(200);

        // Fenced fails liveness.
        cluster.hub.kill(cluster.placements[0]!.identity.hostId);
        await new Promise((r) => setTimeout(r, 20));
        expect(cluster.placements[0]!.counters().status).toBe('fenced');
        const live = await probeLive(0);
        expect(live.status).toBe(503);
        expect((await live.json()).status).toBe('fatal');
    });
});

describe('cluster ops: the metrics split', () => {
    it('counts a cross-host call ONCE, on the caller — it is split, not doubled', async () => {
        const plugins: MetricsPlugin[] = [metrics(), metrics()];
        const cluster = await createCluster(2, {
            actors: [Counter],
            policy: selfPolicy,
            plugins: (i) => [plugins[i]!]
        });
        running = cluster;
        // Place it on host 1, then call it from host 0.
        await cluster.hosts[1]!.actor(Counter, 'split').increment(1);
        const callerBefore = plugins[0]!.snapshot().calls.total;
        const ownerBefore = plugins[1]!.snapshot().calls.total;
        const ownerTurnsBefore = plugins[1]!.snapshot().turnMs?.count ?? 0;

        await cluster.hosts[0]!.actor(Counter, 'split').increment(1);

        // The caller counted the call...
        expect(plugins[0]!.snapshot().calls.total).toBe(callerBefore + 1);
        // ...and the owner did NOT: `dispatchInbound` calls the RAW local
        // dispatcher, so an inbound hop never passes through `useDispatch`.
        expect(plugins[1]!.snapshot().calls.total).toBe(ownerBefore);
        // But the turn really ran there, and `observeTurns` saw it.
        expect(plugins[1]!.snapshot().turnMs?.count ?? 0).toBe(ownerTurnsBefore + 1);
    });
});

describe('cluster ops: the ops endpoint over a real cluster', () => {
    /** `ops()` wired the way an app actually wires it: the fan-out thunk
     *  closes over the placement `cluster()` built. Late-bound because the
     *  placement does not exist until the harness has built the host. */
    const opsFor = (index: number, harness: () => ClusterHarness): ActorPlugin =>
        ops({
            secret: 'ops-secret',
            cluster: (signal) => clusterStats(harness().placements[index]!, { signal })
        });

    const read = async (cluster: ClusterHarness, index: number, path: string) => {
        const app = cluster.apps[index]!;
        const request = new Request(`http://host.test${path}`, {
            headers: { authorization: 'Bearer ops-secret' }
        });
        const route = app.routes.find((candidate) => candidate.match(request))!;
        return route.handle(request, cluster.hosts[index]!);
    };

    it('serves the whole cluster over /_sigx/ops/cluster', async () => {
        let harness: ClusterHarness | null = null;
        const cluster = await createCluster(3, {
            actors: [Counter],
            policy: selfPolicy,
            plugins: (i) => [metrics(), opsFor(i, () => harness!)]
        });
        harness = cluster;
        running = cluster;
        for (let i = 0; i < 3; i++) {
            await cluster.hosts[i]!.actor(Counter, `actor-${i}`).increment(1);
        }

        const response = await read(cluster, 0, '/_sigx/ops/cluster');
        expect(response.status).toBe(200);
        const report = (await response.json()) as ClusterStatsReport;
        // The same answer `clusterStats()` gives in process — the route is a
        // transport for it, not a second implementation.
        expect(report.hosts).toHaveLength(3);
        expect(report.partial).toBe(false);
        expect(report.totals.activations).toBe(3);
        expect(report.from).toBe(cluster.placements[0]!.identity.hostId);
    });

    it('marks the report partial when a peer is down, rather than failing', async () => {
        let harness: ClusterHarness | null = null;
        const cluster = await createCluster(3, {
            actors: [Counter],
            policy: selfPolicy,
            plugins: (i) => [opsFor(i, () => harness!)]
        });
        harness = cluster;
        running = cluster;
        // Address stops resolving but membership still lists it — the case
        // where the fan-out MUST degrade rather than throw, since a
        // dashboard that goes blank when one host dies is worse than useless.
        cluster.unbind(2);

        const response = await read(cluster, 0, '/_sigx/ops/cluster');
        expect(response.status).toBe(200);
        const report = (await response.json()) as ClusterStatsReport;
        expect(report.partial).toBe(true);
        expect(report.unreachable).toHaveLength(1);
        expect(report.unreachable[0]!.reason).toBe('unreachable');
    });

    it('carries the local cluster report in the per-host snapshot', async () => {
        let harness: ClusterHarness | null = null;
        const cluster = await createCluster(2, {
            actors: [Counter],
            policy: selfPolicy,
            plugins: (i) => [opsFor(i, () => harness!)]
        });
        harness = cluster;
        running = cluster;
        await cluster.hosts[0]!.actor(Counter, 'local').increment(1);

        const body = (await (await read(cluster, 0, '/_sigx/ops')).json()) as OpsSnapshot;
        const section = body.ops.cluster as HostReport;
        expect(section.hostId).toBe(cluster.placements[0]!.identity.hostId);
        expect(section.status).toBe('active');
        // `counters` is what makes the routing panels possible at all.
        expect(section.counters.membershipVersion).toBeGreaterThan(0);
        // And cluster readiness reaches the ops snapshot through the same
        // seam `health()` reads, with no wiring.
        expect(body.health.checks.cluster).toEqual({
            ready: true,
            fatal: false,
            detail: 'active'
        });
    });
});
