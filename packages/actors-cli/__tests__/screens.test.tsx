/** @jsxImportSource @sigx/terminal */
/**
 * The screens, actually RENDERED.
 *
 * The pure layout functions are tested next door; this file exists to prove
 * the JSX layer over them produces terminal output at all — that the
 * intrinsics resolve to `@sigx/terminal`'s rather than the DOM's, and that
 * each screen survives the states it will really meet: no snapshot yet, a
 * failed poll, a partial fan-out, a fenced silo.
 *
 * Assertions are on the TEXT, not on the styling. Colour is a theme token
 * resolved at paint time, and pinning it here would fail the first time
 * somebody changed a theme rather than the first time something broke.
 */
import { describe, expect, it } from 'vitest';
import { render, renderNodeToLines } from '@sigx/terminal';
import {
    ClusterScreen,
    GrainsScreen,
    HealthScreen,
    OverviewScreen,
    SilosScreen
} from '../src/dashboard/screens';
import { DashboardState } from '../src/dashboard/state';
import type { MonitorSnapshot, MonitorSource, SiloView } from '../src/source/types';

const emptyStats = {
    activations: 3,
    queued: 1,
    perType: { Cart: 3 },
    transitional: { activating: 0, deactivating: 0 }
};

const silo = (over: Partial<SiloView> = {}): SiloView => ({
    siloId: 'silo-a',
    address: 'http://a:3000',
    status: 'active',
    uptimeMs: 65_000,
    stats: emptyStats,
    counters: null,
    reminderShards: ['p0'],
    membershipVersion: 4,
    transports: ['http'],
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
        silos: [silo()],
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
            { type: 'Cart', key: 'user-42', queued: 7, ageMs: 812_004, idleMs: 0, keptAlive: false }
        ],
        health: { live: true, ready: true, uptimeMs: 65_000, checks: {}, silo: null },
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

/** Render a screen to plain lines. */
function draw(node: unknown): string {
    const container = { type: 'element', tag: 'box', props: {}, children: [] } as never;
    render(node as never, container);
    return renderNodeToLines(container).join('\n');
}

describe('screens render', () => {
    it('draws the overview with real numbers', () => {
        const out = draw(<OverviewScreen state={stateWith()} />);
        expect(out).toContain('activations');
        expect(out).toContain('120');
        // The queue/turn split, which is the panel's whole reason to exist.
        expect(out).toContain('queue');
        expect(out).toContain('turn');
        expect(out).toContain('high queue = a hot grain');
    });

    it('says "connecting" rather than drawing zeroes before the first poll', () => {
        const state = new DashboardState({ source: inertSource });
        // Zeroes would be a claim about the silo; this is a claim about us.
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

    it('calls out a fenced silo, which a load balancer cannot see', () => {
        const state = stateWith({ silos: [silo({ status: 'fenced' })] });
        const out = draw(<SilosScreen state={state} cursor={0} />);
        expect(out).toContain('FENCED');
        expect(out).toContain('refusing activations');
        expect(out).toContain('silo-a');
    });

    it('lists unreachable peers with their classified reason', () => {
        const state = stateWith({
            partial: true,
            cluster: {
                from: 'silo-a',
                view: { version: 4, size: 2, active: 1 },
                totals: { silos: 2, activations: 3, queued: 1, perType: {}, counters: {} as never },
                reminderShards: { p0: ['silo-a'] },
                unreachable: [
                    { siloId: 'silo-b', address: 'http://b:3000', reason: 'timeout', message: 'no answer' }
                ]
            }
        });
        const out = draw(<SilosScreen state={state} cursor={0} />);
        expect(out).toContain('unreachable');
        expect(out).toContain('silo-b');
        expect(out).toContain('timeout');
    });

    it('marks the selected row, and only that one', () => {
        // The cursor is why `DataTable` exists rather than upstream's
        // `Table`; a cursor that renders nowhere is the same as no cursor.
        const state = stateWith({
            silos: [silo(), silo({ siloId: 'silo-b' }), silo({ siloId: 'silo-c' })]
        });
        const out = draw(<SilosScreen state={state} cursor={1} />).split('\n');
        const marked = out.filter((line) => line.includes('▸'));
        expect(marked).toHaveLength(1);
        expect(marked[0]).toContain('silo-b');
    });

    it('draws the grain table and the slowest methods', () => {
        const out = draw(<GrainsScreen state={stateWith()} cursor={0} />);
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
                from: 'silo-a',
                view: { version: 4, size: 2, active: 2 },
                totals: {
                    silos: 2,
                    activations: 3,
                    queued: 1,
                    perType: {},
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
                        directoryLookups: 1,
                        directoryClaims: 2,
                        claimConflicts: 0,
                        directoryReleases: 0,
                        directoryEvictions: 0,
                        siloSweeps: 0,
                        sweptEntries: 0,
                        wrongHostRedirects: 0,
                        unreachableRetries: 0,
                        drainingRetries: 0,
                        authFailures: 0,
                        transportFallbacks: 0,
                        membershipChanges: 1,
                        selfFences: 0,
                        claimed: 3,
                        routeCacheSize: 3
                    }
                },
                reminderShards: { p0: ['silo-a'], p1: [], p2: ['silo-a', 'silo-b'] },
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
                uptimeMs: 1000,
                checks: { cluster: { ready: false, detail: 'leaving — draining' } },
                silo: null
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
