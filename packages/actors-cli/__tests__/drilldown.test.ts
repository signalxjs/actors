/**
 * The drill-down, and the labelling around it.
 *
 * Two failures this covers, both from #121:
 *
 * **The cursor led nowhere.** Selecting a silo moved a highlight and
 * changed nothing else, because the fan-out carried no per-silo detail to
 * open. Opening one now has to actually ASK for that detail — and only
 * while it is open, because a detail poll makes every silo walk its
 * activation table.
 *
 * **Scope was unstated.** One silo's `calls.total` printed directly under
 * cluster-wide totals, so `calls total 382` beneath `cluster silos 2` read
 * as a cluster figure when it was one silo's.
 */
import { describe, expect, it } from 'vitest';
import { DashboardState } from '../src/dashboard/state';
import { renderStats } from '../src/commands/stats';
import type { MonitorSnapshot, MonitorSource, SnapshotOptions } from '../src/source/types';

const emptyStats = {
    activations: 2,
    queued: 0,
    perType: { Counter: 2 },
    transitional: { activating: 0, deactivating: 0 }
};

const silo = (siloId: string): MonitorSnapshot['silos'][number] => ({
    siloId,
    address: `http://${siloId}`,
    status: 'active',
    uptimeMs: 1000,
    stats: emptyStats,
    counters: null,
    reminderShards: [],
    membershipVersion: 1,
    transports: ['http'],
    metrics: null,
    health: null,
    activations: null
});

function snapshot(over: Partial<MonitorSnapshot> = {}): MonitorSnapshot {
    return {
        at: 1_700_000_000_000,
        silos: [silo('s.a'), silo('s.b')],
        cluster: null,
        metrics: null,
        activations: null,
        health: null,
        partial: false,
        ...over
    };
}

const clusterView = (over: Partial<NonNullable<MonitorSnapshot['cluster']>['totals']> = {}) => ({
    from: 's.a',
    view: { version: 1, size: 2, active: 2 },
    totals: {
        silos: 2,
        activations: 4,
        queued: 0,
        perType: {},
        counters: {} as never,
        metrics: null,
        health: { ready: 2, notReady: 0, fatal: 0, unknown: 0 },
        ...over
    },
    reminderShards: {},
    unreachable: []
});

/** A source that records what each poll asked for. */
function recordingSource(): { source: MonitorSource; asked: (SnapshotOptions | undefined)[] } {
    const asked: (SnapshotOptions | undefined)[] = [];
    return {
        asked,
        source: {
            kind: 'http',
            label: 'test',
            snapshot: (_signal?: AbortSignal, options?: SnapshotOptions) => {
                asked.push(options);
                return Promise.resolve(snapshot());
            },
            close: () => Promise.resolve()
        }
    };
}

describe('the drill-down asks for detail, and only then', () => {
    it('polls cheaply with nothing open', async () => {
        const { source, asked } = recordingSource();
        const state = new DashboardState({ source });
        await state.poll();
        // A detail poll makes EVERY silo walk its activation table. The
        // list view must not be paying for a panel nobody has opened.
        expect(asked[0]).toEqual({});
    });

    it('asks for one silo’s detail once a drill-down is open', async () => {
        const { source, asked } = recordingSource();
        const state = new DashboardState({ source });
        await state.poll();
        state.focus('s.b');
        // Focusing polls immediately: the detail was not in the snapshot
        // already on screen, and a second of "no grains" reads as a broken
        // panel rather than a pending request.
        await new Promise((resolve) => setTimeout(resolve, 0));
        expect(state.view.focus).toBe('s.b');
        expect(asked.at(-1)).toEqual({ detail: true, siloId: 's.b' });
    });

    it('goes back to the cheap poll when the drill-down closes', async () => {
        const { source, asked } = recordingSource();
        const state = new DashboardState({ source });
        state.focus('s.b');
        await new Promise((resolve) => setTimeout(resolve, 0));
        state.focus(null);
        await state.poll();
        expect(asked.at(-1)).toEqual({});
    });

    it('does not re-poll when the same silo is focused twice', async () => {
        const { source, asked } = recordingSource();
        const state = new DashboardState({ source });
        state.focus('s.b');
        await new Promise((resolve) => setTimeout(resolve, 0));
        const polls = asked.length;
        state.focus('s.b');
        await new Promise((resolve) => setTimeout(resolve, 0));
        expect(asked.length).toBe(polls);
    });
});

describe('renderStats says which scope a number belongs to', () => {
    it('marks single-silo figures as single-silo', () => {
        const out = renderStats(
            snapshot({
                cluster: clusterView(),
                metrics: {
                    windowMs: 1000,
                    calls: { total: 382, failed: 3, streams: 0 },
                    latencyMs: null,
                    queueMs: null,
                    turnMs: null,
                    byType: {},
                    byMethod: {},
                    errors: { byKind: { 'call-timeout': 3 }, recent: [] },
                    activations: { created: 4, destroyed: 0, byReason: {} },
                    storage: { loads: 0, saves: 0, clears: 0, conflicts: 0, latencyMs: null },
                    gauges: null
                }
            }),
            'http://silo-a'
        ).join('\n');
        // The exact confusion from the issue: `calls total 382` sitting
        // under `cluster silos 2` with nothing saying it is one silo's.
        expect(out).toContain('cluster');
        expect(out).toMatch(/calls — silo s\.a ONLY/);
        expect(out).toMatch(/errors — silo s\.a ONLY/);
    });

    it('marks cluster-wide figures as cluster-wide, with the denominator', () => {
        const out = renderStats(
            snapshot({
                cluster: clusterView({
                    metrics: {
                        silos: 2,
                        layoutMismatch: [],
                        calls: { total: 764, failed: 6, streams: 0 },
                        latencyMs: { count: 10, minMs: 0, maxMs: 2, meanMs: 1, p50Ms: 1, p90Ms: 2, p99Ms: 2 },
                        queueMs: null,
                        turnMs: null,
                        storageLatencyMs: null,
                        errors: { byKind: { 'call-timeout': 6 } },
                        activations: { created: 8, destroyed: 0, byReason: {} },
                        storage: { loads: 0, saves: 0, clears: 0, conflicts: 0 },
                        byType: {},
                        byMethod: {}
                    }
                })
            }),
            'http://silo-a'
        ).join('\n');
        expect(out).toMatch(/calls — cluster-wide \(2 of 2 silos reporting\)/);
        expect(out).toContain('764');
    });

    it('says so when only some silos reported metrics', () => {
        const out = renderStats(
            snapshot({
                cluster: clusterView({
                    silos: 3,
                    metrics: {
                        silos: 2,
                        layoutMismatch: [],
                        calls: { total: 764, failed: 0, streams: 0 },
                        latencyMs: null,
                        queueMs: null,
                        turnMs: null,
                        storageLatencyMs: null,
                        errors: { byKind: {} },
                        activations: { created: 0, destroyed: 0, byReason: {} },
                        storage: { loads: 0, saves: 0, clears: 0, conflicts: 0 },
                        byType: {},
                        byMethod: {}
                    }
                })
            }),
            'http://silo-a'
        ).join('\n');
        // Totals covering two thirds of the fleet look exactly like totals
        // covering all of it, so the denominator has to be said out loud.
        expect(out).toMatch(/2 of 3 silos reported metrics/);
        expect(out).toContain('LOWER BOUND');
    });

    it('attributes the grain list to the silo it came from', () => {
        const out = renderStats(
            snapshot({
                cluster: clusterView(),
                activations: [
                    { type: 'Counter', key: 'cart', queued: 1, ageMs: 1000, idleMs: 0, keptAlive: false, tasks: 0 }
                ]
            }),
            'http://silo-a'
        ).join('\n');
        expect(out).toMatch(/hottest grains — silo s\.a/);
    });
});
