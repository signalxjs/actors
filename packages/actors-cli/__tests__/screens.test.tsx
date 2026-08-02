/** @jsxImportSource @sigx/terminal */
/**
 * The screens, actually RENDERED.
 *
 * The pure layout functions are tested next door; this file exists to prove
 * the JSX layer over them produces terminal output at all — that the
 * intrinsics resolve to `@sigx/terminal`'s rather than the DOM's, and that
 * each screen survives the states it will really meet: no snapshot yet, a
 * failed poll, a partial fan-out, a fenced host.
 *
 * Assertions are on the TEXT, not on the styling. Colour is a theme token
 * resolved at paint time, and pinning it here would fail the first time
 * somebody changed a theme rather than the first time something broke.
 */
import { describe, expect, it } from 'vitest';
import { render, renderNodeToLines } from '@sigx/terminal';
import { cursorModel } from './fixture';
import {
    ClusterScreen,
    GrainsScreen,
    HealthScreen,
    OverviewScreen,
    HostsScreen
} from '../src/dashboard/screens';
import { DashboardState } from '../src/dashboard/state';
import type { ClusterView, MonitorSnapshot, MonitorSource, HostView } from '../src/source/types';

const emptyStats = {
    activations: 3,
    queued: 1,
    perType: { Cart: 3 },
    transitional: { activating: 0, deactivating: 0 }
};

const host = (over: Partial<HostView> = {}): HostView => ({
    hostId: 'host-a',
    address: 'http://a:3000',
    status: 'active',
    uptimeMs: 65_000,
    stats: emptyStats,
    counters: null,
    reminderShards: ['p0'],
    membershipVersion: 4,
    transports: ['http'],
    metrics: null,
    health: { ready: true, fatal: false, checks: {} },
    activations: null,
    ...over
});

/** Cluster totals with the metrics/health tallies a host build reports. */
const clusterTotals = (over: Partial<ClusterView['totals']> = {}): ClusterView['totals'] => ({
    hosts: 2,
    activations: 3,
    queued: 1,
    perType: {},
    counters: {} as never,
    metrics: null,
    health: { ready: 2, notReady: 0, fatal: 0, unknown: 0 },
    ...over
});

const hist = (p99: number) => ({
    count: 12,
    minMs: 0,
    maxMs: p99,
    meanMs: p99 / 2,
    p50Ms: p99 / 4,
    p90Ms: p99 / 2,
    p99Ms: p99
});

function snapshot(over: Partial<MonitorSnapshot> = {}): MonitorSnapshot {
    return {
        at: 1_700_000_000_000,
        hosts: [host()],
        cluster: null,
        metrics: {
            windowMs: 1000,
            calls: { total: 120, failed: 4, streams: 1 },
            latencyMs: hist(80),
            queueMs: hist(30),
            turnMs: hist(50),
            byType: {},
            byMethod: {
                'Cart#checkout': {
                    calls: 40,
                    failed: 2,
                    latencyMs: null,
                    queueMs: null,
                    turnMs: hist(90)
                }
            },
            errors: {
                byKind: { 'call-timeout': 3, '(unknown)': 1 },
                recent: [
                    {
                        at: 1_700_000_000_000,
                        type: 'Cart',
                        method: 'checkout',
                        kind: 'call-timeout',
                        message: 'deadline exceeded'
                    }
                ]
            },
            activations: { created: 9, destroyed: 6, byReason: { idle: 6 } },
            storage: { loads: 12, saves: 8, clears: 0, conflicts: 2, latencyMs: hist(4) },
            gauges: null
        },
        activations: [
            { type: 'Cart', key: 'user-42', queued: 7, ageMs: 812_004, idleMs: 0, keptAlive: false, tasks: 0 }
        ],
        health: { live: true, ready: true, fatal: false, uptimeMs: 65_000, checks: {}, host: null },
        partial: false,
        ...over
    };
}

const inertSource: MonitorSource = {
    kind: 'http',
    label: 'test',
    snapshot: () => Promise.reject(new Error('unused')),
    close: () => Promise.resolve()
};

/** A state pre-loaded with a snapshot, without going through the poll loop. */
function stateWith(over: Partial<MonitorSnapshot> = {}): DashboardState {
    const state = new DashboardState({ source: inertSource });
    state.view.snapshot = snapshot(over);
    state.view.lastOk = snapshot().at;
    return state;
}

/**
 * Render a screen to its LINE ARRAY.
 *
 * `draw` used to join before returning, and every assertion here was a
 * `toContain` over the joined string — which cannot tell twenty lines from
 * one. That is exactly how the screens shipped with every row concatenated
 * onto a single line while this suite stayed green. The join now happens at
 * the call site, so structure is assertable.
 */
function lines(node: unknown): string[] {
    const container = { type: 'element', tag: 'box', props: {}, children: [] } as never;
    render(node as never, container);
    // Strip SGR so assertions are about text, not styling.
    return renderNodeToLines(container).map((line) =>
        line.replace(new RegExp(`${String.fromCharCode(27)}\\[[0-9;]*m`, 'g'), '')
    );
}

/** Non-empty rendered lines, trimmed. */
function rows(node: unknown): string[] {
    return lines(node)
        .map((line) => line.trim())
        .filter((line) => line.length > 0);
}

function draw(node: unknown): string {
    return lines(node).join('\n');
}

describe('screens render', () => {
    it('draws the overview with real numbers', () => {
        const out = draw(<OverviewScreen state={stateWith()} />);
        expect(out).toContain('activations');
        expect(out).toContain('120');
        // The queue/turn split, which is the panel's whole reason to exist.
        expect(out).toContain('queue');
        expect(out).toContain('turn');
        expect(out).toContain('high queue = a hot actor');
    });

    it('says "connecting" rather than drawing zeroes before the first poll', () => {
        const state = new DashboardState({ source: inertSource });
        // Zeroes would be a claim about the host; this is a claim about us.
        expect(draw(<OverviewScreen state={state} />)).toContain('connecting');
    });

    it('leads with PARTIAL, and keeps showing the numbers underneath', () => {
        const out = draw(<OverviewScreen state={stateWith({ partial: true })} />);
        expect(out).toContain('PARTIAL');
        expect(out).toContain('LOWER BOUND');
        expect(out).toContain('120');
    });

    it('says the numbers are stale when a poll has failed', () => {
        const state = stateWith();
        state.view.error = 'connection refused';
        const out = draw(<OverviewScreen state={state} />);
        // The snapshot is still on screen, so it has to say why.
        expect(out).toContain('last good snapshot');
        expect(out).toContain('connection refused');
    });

    it('calls out a fenced host, which a load balancer cannot see', () => {
        const state = stateWith({ hosts: [host({ status: 'fenced' })] });
        const out = draw(<HostsScreen state={state} cursor={cursorModel(0)} />);
        expect(out).toContain('FENCED');
        expect(out).toContain('refusing activations');
        expect(out).toContain('host-a');
    });

    it('puts each host on its own line, under a header', () => {
        const state = stateWith({
            hosts: [host(), host({ hostId: 'host-b' }), host({ hostId: 'host-c' })]
        });
        const out = rows(<HostsScreen state={state} cursor={cursorModel(0)} />);
        const header = out.findIndex((line) => line.includes('HOST') && line.includes('STATUS'));
        expect(header).toBeGreaterThan(-1);
        // Three hosts, three lines after the header and its rule — not one
        // line holding all three, which is what shipped.
        expect(out[header + 2]).toContain('host-a');
        expect(out[header + 3]).toContain('host-b');
        expect(out[header + 4]).toContain('host-c');
        expect(out[header + 2]).not.toContain('host-b');
        expect(out[header]).not.toContain('host-a');
    });

    it('lists unreachable peers with their classified reason', () => {
        const state = stateWith({
            partial: true,
            cluster: {
                from: 'host-a',
                view: { version: 4, size: 2, active: 1 },
                totals: clusterTotals(),
                reminderShards: { p0: ['host-a'] },
                unreachable: [
                    { hostId: 'host-b', address: 'http://b:3000', reason: 'timeout', message: 'no answer' }
                ]
            }
        });
        const out = draw(<HostsScreen state={state} cursor={cursorModel(0)} />);
        expect(out).toContain('unreachable');
        expect(out).toContain('host-b');
        expect(out).toContain('timeout');
    });

    it('keeps every host on its own line whichever row is selected', () => {
        // A cursor that changes nothing on screen is the same as no cursor.
        // Where the selection SHOWS is asserted in `layout.test.tsx`, which
        // can see the viewport; here it is enough that selecting a row does
        // not disturb the structure around it.
        const state = stateWith({
            hosts: [host(), host({ hostId: 'host-b' }), host({ hostId: 'host-c' })]
        });
        for (const index of [0, 1, 2]) {
            const out = rows(<HostsScreen state={state} cursor={cursorModel(index)} />);
            const header = out.findIndex((line) => line.includes('HOST') && line.includes('STATUS'));
            expect(out[header + 2]).toContain('host-a');
            expect(out[header + 3]).toContain('host-b');
            expect(out[header + 4]).toContain('host-c');
        }
    });

    it('draws the actor table and the slowest methods', () => {
        const out = draw(<GrainsScreen state={stateWith()} cursor={cursorModel(0)} />);
        expect(out).toContain('user-42');
        expect(out).toContain('Cart#checkout');
        expect(out).toContain('slowest methods');
    });

    it('says single-node rather than drawing an empty cluster', () => {
        // An empty cluster panel would read as a cluster with nothing in it.
        expect(draw(<ClusterScreen state={stateWith()} />)).toContain('single-node');
    });

    it('shows routing counters side by side and flags unclaimed shards', () => {
        const state = stateWith({
            cluster: {
                from: 'host-a',
                view: { version: 4, size: 2, active: 2 },
                totals: clusterTotals({
                    counters: {
                        routedLocal: 10,
                        remoteDispatches: 7,
                        remoteStreams: 0,
                        remoteWatches: 0,
                        inboundDispatches: 5,
                        inboundStreams: 0,
                        inboundWatches: 0,
                        retries: 0,
                        routingFailures: 0,
                        routeCacheHits: 9,
                        routeCacheMisses: 1,
                        locates: 0,
                        locateRemote: 0,
                        directoryLookups: 1,
                        directoryClaims: 2,
                        claimConflicts: 0,
                        directoryReleases: 0,
                        directoryEvictions: 0,
                        hostSweeps: 0,
                        sweptEntries: 0,
                        wrongHostRedirects: 0,
                        unreachableRetries: 0,
                        drainingRetries: 0,
                        authFailures: 0,
                        transportFallbacks: 0,
                        membershipChanges: 1,
                        selfFences: 0,
                        rebalanceRounds: 0,
                        rebalanceMigrations: 0,
                        claimed: 3,
                        routeCacheSize: 3
                    }
                }),
                reminderShards: { p0: ['host-a'], p1: [], p2: ['host-a', 'host-b'] },
                unreachable: []
            }
        });
        const out = draw(<ClusterScreen state={state} />);
        // Reported side by side and never summed — the gap IS the signal.
        expect(out).toContain('remoteDispatches');
        expect(out).toContain('inboundDispatches');
        expect(out).toContain('UNCLAIMED');
        expect(out).toContain('claimed twice');
        expect(out).toContain('reminder shards');
    });

    it('distinguishes alive-but-draining from unhealthy', () => {
        const state = stateWith({
            health: {
                live: true,
                ready: false,
                fatal: false,
                uptimeMs: 1000,
                checks: { cluster: { ready: false, detail: 'leaving — draining' } },
                host: null
            }
        });
        const out = draw(<HealthScreen state={state} />);
        // The distinction the whole health endpoint exists for.
        expect(out).toContain('drain it, do not restart it');
        expect(out).toContain('leaving');
    });

    it('shows why calls fail, not just that they did', () => {
        const out = draw(<HealthScreen state={stateWith()} />);
        expect(out).toContain('call-timeout');
        expect(out).toContain('recent failures');
        expect(out).toContain('deadline exceeded');
        expect(out).toContain('etag conflicts');
    });
});
