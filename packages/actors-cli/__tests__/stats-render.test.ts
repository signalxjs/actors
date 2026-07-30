/**
 * `renderStats` — the non-interactive output.
 *
 * Pure by construction so it can be tested without a terminal or a silo,
 * which matters because this is the output CI and ssh sessions actually
 * see. The assertions are about what must never be silently omitted: a
 * partial fan-out, an unclaimed reminder shard, a silo that is not active.
 */
import { describe, expect, it } from 'vitest';
import { renderStats } from '../src/commands/stats';
import { count, durationMs, ellipsis, percent, uptime } from '../src/model/format';
import type { MonitorSnapshot } from '@sigx/actors-cli/source';

const emptyStats = {
    activations: 0,
    queued: 0,
    perType: {},
    transitional: { activating: 0, deactivating: 0 }
};

function snapshot(overrides: Partial<MonitorSnapshot> = {}): MonitorSnapshot {
    return {
        at: 1_700_000_000_000,
        silos: [
            {
                siloId: 'silo-a',
                address: 'http://a:3000',
                status: 'active',
                uptimeMs: 65_000,
                stats: { ...emptyStats, activations: 4, queued: 2 },
                counters: null,
                reminderShards: [],
                membershipVersion: null,
                transports: null,
                metrics: null,
                health: null,
                activations: null
            }
        ],
        cluster: null,
        metrics: null,
        activations: null,
        health: null,
        partial: false,
        ...overrides
    };
}

const render = (s: MonitorSnapshot): string => renderStats(s, 'http://a:3000').join('\n');

describe('renderStats', () => {
    it('leads with the PARTIAL warning, not a footnote', () => {
        const output = renderStats(snapshot({ partial: true }), 'x');
        const index = output.findIndex((line) => line.includes('PARTIAL'));
        expect(index).toBeGreaterThan(-1);
        // Before any totals: the numbers below it are lower bounds and still
        // look plausible, which is exactly why the caveat cannot trail them.
        expect(index).toBeLessThan(output.findIndex((line) => line.includes('silos')));
        expect(output[index]).toMatch(/LOWER BOUND/);
    });

    it('flags a silo that is not active', () => {
        const output = render(
            snapshot({
                silos: [
                    { ...snapshot().silos[0]!, status: 'leaving' },
                    { ...snapshot().silos[0]!, siloId: 'silo-b', status: 'fenced' }
                ]
            })
        );
        expect(output).toMatch(/! silo-a {2}leaving/);
        expect(output).toMatch(/! silo-b {2}fenced/);
    });

    it('does not flag an active or single-node silo', () => {
        expect(render(snapshot())).not.toMatch(/! silo-a/);
        expect(render(snapshot({ silos: [{ ...snapshot().silos[0]!, status: 'unknown' }] }))).not.toMatch(
            /! silo-a/
        );
    });

    it('shows in-flight slots, which the totals do not count', () => {
        const output = render(
            snapshot({
                silos: [
                    {
                        ...snapshot().silos[0]!,
                        stats: { ...emptyStats, transitional: { activating: 7, deactivating: 2 } }
                    }
                ]
            })
        );
        expect(output).toMatch(/in flight: 7 activating, 2 deactivating/);
    });

    it('calls out unclaimed and double-claimed reminder shards', () => {
        const output = render(
            snapshot({
                cluster: {
                    from: 'silo-a',
                    view: { version: 3, size: 2, active: 2 },
                    totals: {
                        silos: 2,
                        activations: 0,
                        queued: 0,
                        perType: {},
                        counters: {} as never,
                        metrics: null,
                        health: { ready: 2, notReady: 0, fatal: 0, unknown: 0 }
                    },
                    reminderShards: { p0: ['silo-a'], p1: [], p2: ['silo-a', 'silo-b'] },
                    unreachable: []
                }
            })
        );
        // An empty shard means those reminders are simply not firing.
        expect(output).toMatch(/1 reminder shard\(s\) unclaimed: p1/);
        // Two claimants is safe but means views diverged.
        expect(output).toMatch(/1 reminder shard\(s\) claimed twice/);
    });

    it('says how to get metrics rather than silently showing nothing', () => {
        expect(render(snapshot())).toMatch(/add \.use\(metrics\(\)\)/);
    });

    it('ranks methods by p99 turn time', () => {
        const hist = (p99: number) => ({
            count: 10,
            minMs: 0,
            maxMs: p99,
            meanMs: p99 / 2,
            p50Ms: p99 / 2,
            p90Ms: p99,
            p99Ms: p99
        });
        const output = render(
            snapshot({
                metrics: {
                    windowMs: 1000,
                    calls: { total: 20, failed: 1, streams: 0 },
                    latencyMs: hist(5),
                    queueMs: hist(1),
                    turnMs: hist(9),
                    byType: {},
                    byMethod: {
                        'Cart#fast': { calls: 10, failed: 0, latencyMs: null, queueMs: null, turnMs: hist(1) },
                        'Cart#slow': { calls: 10, failed: 0, latencyMs: null, queueMs: null, turnMs: hist(90) }
                    },
                    errors: { byKind: { 'call-timeout': 3 }, recent: [] },
                    activations: { created: 0, destroyed: 0, byReason: {} },
                    storage: { loads: 0, saves: 0, clears: 0, conflicts: 0, latencyMs: null },
                    gauges: null
                }
            })
        );
        const slow = output.indexOf('Cart#slow');
        const fast = output.indexOf('Cart#fast');
        expect(slow).toBeGreaterThan(-1);
        expect(slow).toBeLessThan(fast);
        expect(output).toMatch(/call-timeout/);
        // Queue and turn side by side — the comparison IS the diagnosis.
        expect(output).toMatch(/queue {8}p50/);
        expect(output).toMatch(/turn {9}p50/);
    });
});

describe('format', () => {
    it('keeps counts short enough to compare in a column', () => {
        expect(count(0)).toBe('0');
        expect(count(950)).toBe('950');
        expect(count(12_400)).toBe('12.4k');
        expect(count(3_100_000)).toBe('3.1M');
        expect(count(Number.NaN)).toBe('—');
    });

    it('picks a unit that makes latencies comparable at a glance', () => {
        expect(durationMs(0.812)).toBe('812µs');
        expect(durationMs(31.4)).toBe('31.4ms');
        expect(durationMs(2500)).toBe('2.5s');
        expect(durationMs(0)).toBe('0');
    });

    it('formats elapsed time', () => {
        expect(uptime(45_000)).toBe('45s');
        expect(uptime(65_000)).toBe('1m05s');
        expect(uptime(15_120_000)).toBe('4h12m');
        expect(uptime(280_800_000)).toBe('3d 6h');
        expect(uptime(-1)).toBe('—');
    });

    it('reports a percentage only when there is a denominator', () => {
        expect(percent(1, 4)).toBe('25%');
        expect(percent(0, 0)).toBe('—');
    });

    it('marks a truncated key rather than cutting it silently', () => {
        expect(ellipsis('short', 10)).toBe('short');
        expect(ellipsis('a-very-long-grain-key', 8)).toBe('a-very-…');
        expect(ellipsis('x', 0)).toBe('');
    });
});
