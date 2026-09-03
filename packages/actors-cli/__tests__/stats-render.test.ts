/**
 * `renderStats` — the non-interactive output.
 *
 * Pure by construction so it can be tested without a terminal or a host,
 * which matters because this is the output CI and ssh sessions actually
 * see. The assertions are about what must never be silently omitted: a
 * partial fan-out, an unclaimed reminder shard, a host that is not active.
 */
import { describe, expect, it } from 'vitest';
import { renderStats } from '../src/commands/stats';
import { count, durationMs, ellipsis, percent, uptime } from '@sigx/actors-monitor/format';
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
        hosts: [
            {
                hostId: 'host-a',
                address: 'http://a:3000',
                status: 'active',
                uptimeMs: 65_000,
                stats: { ...emptyStats, activations: 4, queued: 2 },
                counters: null,
                reminderShards: [],
                membershipVersion: null,
                transports: null,
                meta: null,
                metrics: null,
                health: null,
                activations: null,
                sockets: null
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

/** The `sockets` ops section a host with a socket mount publishes (#166). */
const sockets = {
    connectionsOpened: 7,
    connectionsClosed: 4,
    connectionsRefused: 2,
    callsStarted: 40,
    callsFailed: 3,
    subscriptionsOpened: 9,
    subscriptionsClosed: 5,
    protocolBreaches: 1,
    lifetimeCloses: 2,
    deliveries: 1200,
    deliveryBytes: 48_000,
    throttleQuantized: 6,
    subscriptionsShed: 3,
    open: 3,
    inFlight: 1,
    subscriptions: 4,
    bufferedBytes: 512,
    lifetimeMs: { count: 4, minMs: 2, maxMs: 21, meanMs: 11.5, p50Ms: 2, p90Ms: 21, p99Ms: 21 }
};

describe('renderStats', () => {
    it('leads with the PARTIAL warning, not a footnote', () => {
        const output = renderStats(snapshot({ partial: true }), 'x');
        const index = output.findIndex((line) => line.includes('PARTIAL'));
        expect(index).toBeGreaterThan(-1);
        // Before any totals: the numbers below it are lower bounds and still
        // look plausible, which is exactly why the caveat cannot trail them.
        expect(index).toBeLessThan(output.findIndex((line) => line.includes('hosts')));
        expect(output[index]).toMatch(/LOWER BOUND/);
    });

    it('flags a host that is not active', () => {
        const output = render(
            snapshot({
                hosts: [
                    { ...snapshot().hosts[0]!, status: 'leaving' },
                    { ...snapshot().hosts[0]!, hostId: 'host-b', status: 'fenced' }
                ]
            })
        );
        expect(output).toMatch(/! host-a {2}leaving/);
        expect(output).toMatch(/! host-b {2}fenced/);
    });

    it('does not flag an active or single-node host', () => {
        expect(render(snapshot())).not.toMatch(/! host-a/);
        expect(render(snapshot({ hosts: [{ ...snapshot().hosts[0]!, status: 'unknown' }] }))).not.toMatch(
            /! host-a/
        );
    });

    it('shows in-flight slots, which the totals do not count', () => {
        const output = render(
            snapshot({
                hosts: [
                    {
                        ...snapshot().hosts[0]!,
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
                    from: 'host-a',
                    view: { version: 3, size: 2, active: 2 },
                    totals: {
                        hosts: 2,
                        activations: 0,
                        queued: 0,
                        perType: {},
                        counters: {} as never,
                        metrics: null,
                        health: { ready: 2, notReady: 0, fatal: 0, unknown: 0 }
                    },
                    reminderShards: { p0: ['host-a'], p1: [], p2: ['host-a', 'host-b'] },
                    unreachable: []
                }
            })
        );
        // An empty shard means those reminders are simply not firing.
        expect(output).toMatch(/1 reminder shard\(s\) unclaimed: p1/);
        // Two claimants is safe but means views diverged.
        expect(output).toMatch(/1 reminder shard\(s\) claimed twice/);
    });

    it('says how many nodes the hosts span, and which node each is on (#51)', () => {
        const base = snapshot().hosts[0]!;
        const output = render(
            snapshot({
                hosts: [
                    { ...base, meta: { node: 'node-1' } },
                    { ...base, hostId: 'host-b', meta: { node: 'node-1' } },
                    { ...base, hostId: 'host-c', meta: { node: 'node-2' } }
                ],
                cluster: {
                    from: 'host-a',
                    view: { version: 3, size: 3, active: 3 },
                    totals: {
                        hosts: 3,
                        activations: 0,
                        queued: 0,
                        perType: {},
                        counters: {} as never,
                        metrics: null,
                        health: { ready: 3, notReady: 0, fatal: 0, unknown: 0 }
                    },
                    reminderShards: { p0: ['host-a'] },
                    unreachable: []
                }
            })
        );
        // `hosts 3` over `nodes 2` — packing vs spread, without kubectl.
        expect(output).toMatch(/hosts {8}3/);
        expect(output).toMatch(/nodes {8}2/);
        expect(output).toMatch(/host-b .*node node-1/);
        expect(output).toMatch(/host-c .*node node-2/);
    });

    it('prints no nodes line when no host reports one', () => {
        const output = render(
            snapshot({
                cluster: {
                    from: 'host-a',
                    view: { version: 3, size: 1, active: 1 },
                    totals: {
                        hosts: 1,
                        activations: 0,
                        queued: 0,
                        perType: {},
                        counters: {} as never,
                        metrics: null,
                        health: { ready: 1, notReady: 0, fatal: 0, unknown: 0 }
                    },
                    reminderShards: { p0: ['host-a'] },
                    unreachable: []
                }
            })
        );
        expect(output).not.toMatch(/nodes/);
        expect(output).not.toMatch(/node /);
    });

    it('prints a host-labelled sockets section when the host reported one (#166)', () => {
        const output = render(snapshot({ hosts: [{ ...snapshot().hosts[0]!, sockets }] }));
        // One host's own, and the heading says so — the fan-out carries no
        // socket digest, so this is never a cluster total.
        expect(output).toMatch(/sockets — host host-a ONLY/);
        expect(output).toMatch(/open {9}3 \(1 in flight\)/);
        expect(output).toMatch(/subs {9}4 \(6 throttle-quantized\)/);
        expect(output).toMatch(/deliveries {3}1\.2k frames {2}~48 kB/);
        expect(output).toMatch(/buffered {5}512 B/);
        expect(output).toMatch(/connections {2}7 opened {2}4 closed {2}2 refused/);
        expect(output).toMatch(/evicted {6}2 lifetime {2}1 protocol breach/);
        expect(output).toMatch(/shed {9}3 subscriptions/);
        expect(output).toMatch(/lifetime {5}p50 2ms {2}p90 21ms {2}p99 21ms/);
    });

    it('prints the shed row only when a subscription was shed (#258)', () => {
        // Off by default and inert without an adapter's buffer gauge — a
        // zero would read as "nothing was slow" on a host that cannot say.
        const output = render(
            snapshot({ hosts: [{ ...snapshot().hosts[0]!, sockets: { ...sockets, subscriptionsShed: 0 } }] })
        );
        expect(output).not.toMatch(/shed/);
    });

    it('prints no sockets section for a host that reported none', () => {
        // "Said nothing" is not "no sockets": a host without a socket mount
        // must not read as one with zero sessions.
        expect(render(snapshot())).not.toMatch(/sockets/);
    });

    it('draws unknown buffered bytes as a gap, not as zero (#208)', () => {
        const output = render(
            snapshot({
                hosts: [{ ...snapshot().hosts[0]!, sockets: { ...sockets, bufferedBytes: null, lifetimeMs: null, lifetimeCloses: 0, protocolBreaches: 0 } }]
            })
        );
        expect(output).toMatch(/buffered {5}—/);
        // Nothing closed, nothing evicted: neither row claims a measurement.
        expect(output).not.toMatch(/lifetime {5}p50/);
        expect(output).not.toMatch(/evicted/);
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
        expect(ellipsis('a-very-long-actor-key', 8)).toBe('a-very-…');
        expect(ellipsis('x', 0)).toBe('');
    });
});
