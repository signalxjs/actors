/**
 * One realistic dashboard state, shared by every renderer that draws a frame.
 *
 * Modelled on what `pnpm --filter counter-example cluster:serve` actually
 * produces — two hosts with one draining, a hot actor, an unclaimed reminder
 * shard, and a method that does not exist being called one time in seven. A
 * fixture of zeroes would make every screen look fine, which is precisely the
 * failure mode these suites exist to catch.
 *
 * It lives in the MONITOR package rather than in either renderer, and both
 * import it. That is not tidiness: the claim this whole layering makes is
 * that a terminal and a browser over one data layer cannot disagree about
 * what the cluster is doing, and two renderers asserting against two fixtures
 * could not detect it if they did.
 *
 * Deliberately deterministic (fixed timestamps, a fixed pseudo-series), so a
 * frame snapshot diff means the RENDERING changed and nothing else.
 */
import { DashboardState, type HostView, type MonitorSnapshot, type MonitorSource } from '../src/index';

const stats = {
    activations: 30,
    queued: 2,
    perType: { Counter: 30 },
    transitional: { activating: 0, deactivating: 0 }
};

export const host = (over: Partial<HostView> = {}): HostView => ({
    hostId: 's.2sme5hx2',
    address: 'http://127.0.0.1:5391',
    status: 'active',
    uptimeMs: 142_000,
    stats,
    counters: null,
    reminderShards: ['p0'],
    membershipVersion: 5,
    transports: ['http'],
    // A local run has no node to report; the node tests set one explicitly.
    meta: null,
    // Every host's own digest and readiness — what milestone 9 put on the
    // wire, and what a drill-down renders.
    metrics: {
        v: 1,
        layout: 'll-4-26',
        windowMs: 142_000,
        calls: { total: 593, failed: 26, streams: 0 },
        latency: { count: 120, sumUs: 36_000, minUs: 60, maxUs: 900, idx: [40, 48], n: [90, 30] },
        queue: null,
        turn: null,
        storageLatency: null,
        errors: { byKind: { 'method-not-found': 68 }, recent: [] },
        activations: { created: 40, destroyed: 8, byReason: { idle: 8 } },
        storage: { loads: 158, saves: 2860, clears: 0, conflicts: 0 },
        byType: { Counter: { calls: 593, failed: 26 } },
        byMethod: { 'Counter#increment': { calls: 989, failed: 0 } }
    },
    health: { ready: true, fatal: false, checks: { cluster: { ready: true, detail: 'active' } } },
    activations: null,
    ...over
});

const hist = (p99: number) => ({
    count: 120,
    minMs: 0,
    maxMs: p99,
    meanMs: p99 / 4,
    p50Ms: p99 / 8,
    p90Ms: p99 / 2,
    p99Ms: p99
});

export const demoSnapshot: MonitorSnapshot = {
    at: 1_700_000_000_000,
    hosts: [
        host(),
        host({
            hostId: 's.ikfugf49',
            status: 'leaving',
            stats: { ...stats, activations: 2, queued: 0 },
            // A draining host answers live-200 / ready-503. Showing it as
            // ready would make the READY column decorative.
            health: {
                ready: false,
                fatal: false,
                checks: { cluster: { ready: false, detail: 'leaving — draining, take out of rotation' } }
            },
            metrics: null
        })
    ],
    cluster: {
        from: 's.2sme5hx2',
        view: { version: 5, size: 2, active: 1 },
        totals: {
            hosts: 2,
            activations: 32,
            queued: 2,
            perType: { Counter: 32 },
            counters: {
                routedLocal: 1280, remoteDispatches: 1280, remoteStreams: 1, remoteWatches: 0, coalescedWatches: 0, retries: 15,
                routingFailures: 0, targetedDispatches: 0, inboundDispatches: 1280, inboundStreams: 0, inboundWatches: 0,
                routeCacheHits: 238, routeCacheMisses: 44, directoryLookups: 44,
                directoryClaims: 37, claimConflicts: 4, directoryReleases: 0,
                directoryEvictions: 0, hostSweeps: 2, sweptEntries: 4,
                wrongHostRedirects: 3, unreachableRetries: 0, drainingRetries: 0,
                authFailures: 0, transportFallbacks: 0, membershipChanges: 5,
                selfFences: 0, rejoinAttempts: 0, rejoins: 0, rebalanceRounds: 0, rebalanceMigrations: 0, claimed: 32, routeCacheSize: 39, locates: 44, locateRemote: 12
            },
            // Cluster-wide, from the merged digests — which is the whole
            // point of milestone 9, and what the Overview must label as
            // such rather than printing beside one host's numbers.
            metrics: {
                hosts: 2,
                layoutMismatch: [],
                calls: { total: 1186, failed: 52, streams: 0, oneWayFailures: 0 },
                latencyMs: hist(608 / 1000),
                queueMs: hist(272 / 1000),
                turnMs: hist(320 / 1000),
                storageLatencyMs: hist(0.4),
                errors: { byKind: { 'method-not-found': 136 } },
                activations: { created: 80, destroyed: 16, byReason: { idle: 16 } },
                storage: { loads: 316, saves: 5720, clears: 0, conflicts: 0 },
                byType: { Counter: { calls: 1186, failed: 52 } },
                byMethod: {
                    'Counter#increment': { calls: 1978, failed: 0 },
                    'Counter#nope': { calls: 94, failed: 94 }
                }
            },
            health: { ready: 1, notReady: 1, fatal: 0, unknown: 0 }
        },
        reminderShards: Object.fromEntries(
            Array.from({ length: 16 }, (_, i) => [`p${i}`, i === 3 ? [] : ['s.2sme5hx2']])
        ),
        unreachable: []
    },
    metrics: {
        windowMs: 1000,
        calls: { total: 593, failed: 26, streams: 0 },
        latencyMs: hist(608 / 1000),
        queueMs: hist(272 / 1000),
        turnMs: hist(320 / 1000),
        byType: {},
        byMethod: {
            'Counter#increment': { calls: 989, failed: 0, latencyMs: null, queueMs: null, turnMs: hist(0.216) },
            'Counter#nope': { calls: 47, failed: 47, latencyMs: null, queueMs: null, turnMs: hist(0.136) }
        },
        errors: {
            byKind: { 'method-not-found': 68 },
            recent: [
                {
                    at: 1_700_000_000_000,
                    type: 'Counter',
                    method: 'nope',
                    kind: 'method-not-found',
                    message: 'actor type "Counter" has no method "nope"'
                }
            ]
        },
        activations: { created: 40, destroyed: 8, byReason: { idle: 8 } },
        storage: { loads: 158, saves: 2860, clears: 0, conflicts: 0, latencyMs: hist(0.4) },
        gauges: null
    },
    activations: [
        { type: 'Counter', key: 'cart', queued: 0, ageMs: 167_000, idleMs: 167_000, keptAlive: false, tasks: 0, watchLoops: 0, watchSubscribers: 0 },
        { type: 'Counter', key: 'cold-0', queued: 0, ageMs: 161_000, idleMs: 2000, keptAlive: false, tasks: 0, watchLoops: 0, watchSubscribers: 0 },
        { type: 'Counter', key: 'cold-1', queued: 3, ageMs: 167_000, idleMs: 2000, keptAlive: true, tasks: 2, watchLoops: 0, watchSubscribers: 0 }
    ],
    health: {
        live: true,
        ready: true,
        fatal: false,
        uptimeMs: 242_000,
        checks: { cluster: { ready: true, detail: 'active' } },
        host: null
    },
    partial: false
};

/** A source that is never polled — these states are loaded directly. */
export const inertSource: MonitorSource = {
    kind: 'http',
    label: 'http://127.0.0.1:5391',
    snapshot: () => Promise.reject(new Error('unused')),
    close: () => Promise.resolve()
};

/**
 * The fixture as a live `DashboardState`, sparkline history included.
 *
 * The series are a fixed sine rather than random, and carry one gap, so the
 * `·` a gap draws is part of every rendered frame.
 *
 * Filled to CAPACITY rather than part-way, so the frames show a dashboard
 * that has been watching for a while — which is both the steady state and
 * the one where the layout is under real pressure.
 */
export function demoState(snapshot: MonitorSnapshot = demoSnapshot): DashboardState {
    const state = new DashboardState({ source: inertSource });
    state.view.snapshot = snapshot;
    state.view.lastOk = snapshot.at;
    for (let i = 0; i < state.calls.capacity; i++) {
        state.calls.push(i % 7 === 0 ? null : 400 + Math.round(Math.sin(i) * 180));
        state.failures.push(i % 5 === 0 ? 6 : 2);
        state.queued.push(i % 3);
        state.activations.push(30 + (i % 4));
    }
    return state;
}
