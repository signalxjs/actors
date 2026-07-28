/**
 * The bucketing primitive behind `metrics()`.
 *
 * Tested against the source directly rather than through `metrics()`: both
 * bugs below need inputs (a 2-hour turn, four billion samples) that cannot
 * be produced through the public API in a test, and exporting the class
 * just to test it would widen the package's API for no user's benefit.
 */
import { describe, expect, it } from 'vitest';
import { Histogram } from '../src/silo/histogram';

describe('Histogram', () => {
    it('reports exact min/max/mean and plausible percentiles', () => {
        const h = new Histogram();
        for (let i = 1; i <= 1000; i++) h.record(i / 1000); // 1µs … 1ms
        const s = h.snapshot();
        expect(s.count).toBe(1000);
        expect(s.minMs).toBeCloseTo(0.001, 6);
        expect(s.maxMs).toBeCloseTo(1, 6);
        expect(s.meanMs).toBeCloseTo(0.5005, 3);
        // Bucketed, so ~6% relative error is expected and acceptable.
        expect(s.p50Ms).toBeGreaterThan(0.45);
        expect(s.p50Ms).toBeLessThanOrEqual(0.51);
        expect(s.p99Ms).toBeGreaterThan(0.9);
        expect(s.p99Ms).toBeLessThanOrEqual(1);
    });

    it('keeps sub-microsecond resolution', () => {
        // An uncontended turn is ~0.5µs. At millisecond resolution every
        // healthy turn would land in bucket zero and p99 would read 0 —
        // useless exactly where the system is behaving.
        const h = new Histogram();
        for (let i = 0; i < 100; i++) h.record(0.0005);
        expect(h.snapshot().p50Ms).toBeGreaterThan(0);
    });

    it('files an absurdly long duration as slow, not fast', () => {
        // `Math.clz32` coerces to uint32, so a duration past 2^32µs (~71min)
        // used to wrap and be bucketed as one of the FASTEST samples — the
        // exact opposite of the truth, and the one sample you most want.
        const h = new Histogram();
        h.record(1); // 1ms
        h.record(2 * 60 * 60 * 1000); // 2 hours
        const s = h.snapshot();
        expect(s.maxMs).toBe(2 * 60 * 60 * 1000);
        // p99 of two samples is the slow one; it must not read as ~1ms.
        expect(s.p99Ms).toBeGreaterThan(1000);
    });

    it('accumulates one hot bucket exactly', () => {
        // Partial guard only, and worth saying so: the bug this area had was
        // a Uint32 bucket wrapping at 2^32 samples (~under an hour at 1-2M
        // ops/s), and driving 4 billion records in a unit test is not
        // feasible. This checks exact accumulation at a reachable scale; the
        // actual fix — Float64 buckets, exact to 2^53 — is argued at the
        // field in histogram.ts.
        const h = new Histogram();
        for (let i = 0; i < 200_000; i++) h.record(0.5);
        const s = h.snapshot();
        expect(s.count).toBe(200_000);
        expect(s.p50Ms).toBeGreaterThan(0.4);
        expect(s.p50Ms).toBeLessThan(0.6);
        expect(s.meanMs).toBeCloseTo(0.5, 3);
    });

    it('clamps a negative duration rather than corrupting buckets', () => {
        const h = new Histogram();
        h.record(-5); // only reachable via a clock step
        const s = h.snapshot();
        expect(s.count).toBe(1);
        expect(s.minMs).toBe(0);
        expect(s.p50Ms).toBe(0);
    });

    it('reset() clears everything', () => {
        const h = new Histogram();
        h.record(1);
        h.reset();
        expect(h.snapshot()).toMatchObject({ count: 0, minMs: 0, maxMs: 0, p99Ms: 0 });
    });
});
