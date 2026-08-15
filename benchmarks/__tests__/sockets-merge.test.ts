// @vitest-environment node
/**
 * The socket run's REPORT — the part a human acts on without ever seeing
 * the cluster (#184). Same reason the rest of `benchmarks/__tests__` exists:
 * the measurements need a deployment, the arithmetic over them does not,
 * and the arithmetic is where a wrong number gets published from.
 *
 * Two things here have already been wrong in production runs, which is why
 * they are pinned rather than assumed:
 *
 *  - rows are SUMMED across pods, so a merge that drops or double-counts a
 *    pod does not lose a line, it moves every total;
 *  - rates and percentiles are NOT summable, and treating them as if they
 *    were is the easiest possible mistake to make here.
 */
import { describe, expect, it } from 'vitest';
import { mergeRows, runWsLoad } from '../../perf/aks/deploy/ws-load.mjs';
import { deliveryMetrics } from '../src/scenarios/sockets.ts';

/** One generator pod's line for a rung, with the fields merge touches. */
const row = (over: Record<string, unknown> = {}) => ({
    n: 1000,
    mode: 'hot',
    read: 'current',
    principal: 'anon',
    connected: 1000,
    socketsExpected: 1000,
    connectFailures: 0,
    deliveries: 60_000,
    durationMs: 60_000,
    publishes: 60,
    publishFailures: 0,
    subscriptionErrors: 0,
    drops: 0,
    seqMismatches: 0,
    maxBufferedBytes: 0,
    clientRssMb: 100,
    latencyMs: null,
    ...over
});

describe('mergeRows', () => {
    it('sums counts across pods and reports the total as totalConnections', () => {
        const [merged] = mergeRows([row(), row(), row()]);
        expect(merged!.pods).toBe(3);
        expect(merged!.connected).toBe(3000);
        expect(merged!.totalConnections).toBe(3000);
        expect(merged!.deliveries).toBe(180_000);
    });

    it('recomputes the rate rather than summing per-pod rates', () => {
        // Each pod measured 1000 msg/s over its own 60 s window. Summing the
        // rates would say 3000/s; the truth is 180 000 delivered over the
        // window they overlap within, which is the same 60 s.
        const [merged] = mergeRows([row(), row(), row()]);
        expect(merged!.deliveriesPerSec).toBe(3000);

        // …and a pod whose window ran long must widen the denominator, not
        // be averaged away.
        const [skewed] = mergeRows([row(), row({ durationMs: 90_000 })]);
        expect(skewed!.durationMs).toBe(90_000);
        expect(skewed!.deliveriesPerSec).toBeCloseTo(120_000 / 90, 3);
    });

    it('takes latency from the publishing pod alone and says how many it had', () => {
        const latencyMs = { count: 100, p50: 60, p90: 70, p99: 80, max: 90 };
        const [merged] = mergeRows([row({ latencyMs }), row(), row()]);
        // Only the publishing pod carries probes; merging percentiles across
        // pods would produce the p99 of nothing.
        expect(merged!.latencyMs).toEqual(latencyMs);
        expect(merged!.latencyFromPods).toBe(1);
    });

    it('takes the MAX of the buffer high-water mark, never the sum', () => {
        const [merged] = mergeRows([
            row({ maxBufferedBytes: 0 }),
            row({ maxBufferedBytes: 4096 }),
            row({ maxBufferedBytes: 128 })
        ]);
        expect(merged!.maxBufferedBytes).toBe(4096);
    });

    it('keeps rungs separate and orders them by size', () => {
        const merged = mergeRows([row({ n: 5000 }), row({ n: 1000 }), row({ n: 5000 })]);
        expect(merged.map((m) => m.n)).toEqual([1000, 5000]);
        expect(merged[0]!.pods).toBe(1);
        expect(merged[1]!.pods).toBe(2);
    });

    it('stamps every row partial, so a short run cannot be read as a result', () => {
        const merged = mergeRows([row(), row({ n: 5000 })], { partial: true });
        expect(merged.every((m) => m.partial === true)).toBe(true);
        // …and does not stamp it otherwise, or every row would carry it.
        expect(mergeRows([row()])[0]).not.toHaveProperty('partial');
    });

    it('does not divide by zero when a rung published nothing', () => {
        const [merged] = mergeRows([row({ publishes: 0, deliveries: 0, durationMs: 0 })]);
        expect(merged!.deliveriesPerPublish).toBe(0);
        expect(merged!.deliveriesPerSec).toBe(0);
    });

    it('sums connect failures, which is how the identity cliff is detected', () => {
        // `sockets/principal-cliff` reads `max_healthy_identities` off the
        // highest rung with ZERO failures, so a failure on any pod has to
        // survive the merge.
        const [merged] = mergeRows([row(), row({ connectFailures: 30, connected: 970 })]);
        expect(merged!.connectFailures).toBe(30);
        expect(merged!.connected).toBe(1970);
    });
});

describe('runWsLoad option guards', () => {
    /**
     * The image is forwarded to `helm` AFTER the explicit `--set image.*`,
     * so a tag smuggled through `values` would win — and the run would
     * measure a build neither the caller nor the recorded `INFRA_SHAPE`
     * names. Refusing is the only safe answer; picking a winner means one
     * of the two callers is silently wrong.
     *
     * Reached before any `kubectl`, so this needs no cluster.
     */
    it.each(['image.tag', 'image.repository'])(
        'refuses %s smuggled through values',
        async (key) => {
            await expect(
                runWsLoad({
                    context: 'ctx',
                    namespace: 'ns',
                    chartDir: '/nowhere',
                    imageRepository: 'registry/img',
                    imageTag: 'abc123',
                    workload: 'w',
                    values: { [key]: 'sneaky', mode: 'hot' }
                })
            ).rejects.toThrow(/imageRepository\/imageTag, not through values/);
        }
    );
});

describe('deliveryMetrics (#252)', () => {
    const base = {
        merged: [],
        peakOpen: 0,
        peakSubscriptions: 0,
        hosts: 3,
        delta: {},
        partial: false
    };

    it('reports what the HOSTS sent, alongside the generator count', () => {
        const metrics = deliveryMetrics({
            ...base,
            delta: { deliveries: 48_000, deliveryBytes: 2_400_000 }
        });
        expect(metrics.map((m) => m.name)).toEqual(['host_deliveries', 'host_delivery_bytes']);
        expect(metrics[0]).toMatchObject({ value: 48_000, informational: true });
    });

    it('omits the host counters when the run did not carry them', () => {
        // Absent rather than 0: a zero would say "the hosts delivered
        // nothing", which is a claim about the runtime rather than about an
        // older image that has no such counter.
        expect(deliveryMetrics({ ...base, delta: {} })).toEqual([]);
    });

    it('omits the buffered gauge when no host could report one', () => {
        // THE #208 RULE. A 0 here asserts "the hosts are not buffering" from
        // an absence of data, which is exactly what left #182 unresolved.
        expect(
            deliveryMetrics({ ...base, delta: { deliveries: 1 }, peakBufferedBytes: null }).some(
                (m) => m.name === 'peak_host_buffered_bytes'
            )
        ).toBe(false);
    });

    it('reports the buffered gauge when a host did — including a real zero', () => {
        const zero = deliveryMetrics({ ...base, delta: { deliveries: 1 }, peakBufferedBytes: 0 });
        expect(zero.find((m) => m.name === 'peak_host_buffered_bytes')).toMatchObject({ value: 0 });
        const deep = deliveryMetrics({ ...base, delta: { deliveries: 1 }, peakBufferedBytes: 9_001 });
        expect(deep.find((m) => m.name === 'peak_host_buffered_bytes')).toMatchObject({
            value: 9_001,
            direction: 'lower'
        });
    });
});
