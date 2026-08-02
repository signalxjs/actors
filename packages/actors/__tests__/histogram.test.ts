/**
 * The bucketing primitive behind `metrics()`.
 *
 * Tested against the source directly rather than through `metrics()`: both
 * bugs below need inputs (a 2-hour turn, four billion samples) that cannot
 * be produced through the public API in a test, and exporting the class
 * just to test it would widen the package's API for no user's benefit.
 */
import { describe, expect, it } from 'vitest';
import {
    HISTOGRAM_LAYOUT,
    Histogram,
    digestSnapshot,
    emptyHistogramDigest,
    mergeHistogramDigests
} from '../src/host/histogram';

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

describe('histogram digests', () => {
    it('round-trips: a digest re-derives its own snapshot exactly', () => {
        const h = new Histogram();
        for (let i = 0; i < 1000; i++) h.record((i % 50) + 0.5);
        // If these ever disagree, one of the two percentile walks has
        // drifted from the other — which is how a cluster-wide p99 stops
        // meaning the same thing as the per-host one it generalises.
        expect(digestSnapshot(h.digest())).toEqual(h.snapshot());
    });

    it('MERGES buckets rather than averaging percentiles', () => {
        // The whole reason buckets are on the wire. One host serves a
        // thousand fast calls, the other serves a single catastrophic one.
        const fast = new Histogram();
        for (let i = 0; i < 999; i++) fast.record(1);
        const slow = new Histogram();
        slow.record(1000);

        const merged = digestSnapshot(mergeHistogramDigests([fast.digest(), slow.digest()]));
        expect(merged.count).toBe(1000);
        // 999 of 1000 samples are at 1ms, so the cluster's p99 IS ~1ms.
        expect(merged.p99Ms).toBeLessThan(2);
        // What averaging the two hosts' p99s would have claimed: about
        // 500ms — a latency figure describing no call that ever happened.
        const averaged = (fast.snapshot().p99Ms + slow.snapshot().p99Ms) / 2;
        expect(averaged).toBeGreaterThan(400);
        expect(Math.abs(merged.p99Ms - averaged)).toBeGreaterThan(100);
    });

    it('merges in the other direction too — a real tail is not smoothed away', () => {
        // Half the fleet is slow: the cluster p90 must land in the SLOW
        // band, where averaging the two p90s would put it in the middle.
        const a = new Histogram();
        for (let i = 0; i < 500; i++) a.record(1);
        const b = new Histogram();
        for (let i = 0; i < 500; i++) b.record(1000);
        const merged = digestSnapshot(mergeHistogramDigests([a.digest(), b.digest()]));
        expect(merged.p90Ms).toBeGreaterThan(500);
        expect((a.snapshot().p90Ms + b.snapshot().p90Ms) / 2).toBeLessThan(600);
    });

    it('keeps the mean exact, not a mean of means', () => {
        const many = new Histogram();
        for (let i = 0; i < 900; i++) many.record(1);
        const few = new Histogram();
        for (let i = 0; i < 100; i++) few.record(11);
        const merged = digestSnapshot(mergeHistogramDigests([many.digest(), few.digest()]));
        // (900*1 + 100*11) / 1000 = 2. A mean of means would say 6.
        expect(merged.meanMs).toBeCloseTo(2, 6);
    });

    it('is sparse enough to poll', () => {
        const h = new Histogram();
        for (let i = 0; i < 1000; i++) h.record(0.5 + (i % 20) / 10);
        const digest = h.digest();
        // 384 buckets exist; a real distribution occupies a handful, and
        // this rides an internal wire once a second per host.
        expect(digest.idx.length).toBeLessThan(60);
        expect(JSON.stringify(digest).length).toBeLessThan(1024);
        expect(digest.idx).toEqual([...digest.idx].sort((x, y) => x - y));
    });

    it('ignores a hostile digest instead of indexing into it', () => {
        const good = new Histogram();
        for (let i = 0; i < 10; i++) good.record(1);
        const merged = mergeHistogramDigests([
            good.digest(),
            // Every one of these would either throw away a write silently
            // or reach outside the array.
            { count: 5, sumUs: 1, minUs: 0, maxUs: 1, idx: [-1, 1e9, 2.5, Number.NaN], n: [1, 1, 1, 1] },
            { count: 1, sumUs: 0, minUs: 0, maxUs: 0, idx: [0], n: [Number.NaN] },
            null,
            undefined
        ]);
        // The valid host's samples survive; nothing crashed and no bucket
        // outside the layout was written.
        expect(merged.idx.every((i) => Number.isInteger(i) && i >= 0)).toBe(true);
        expect(digestSnapshot(merged).p50Ms).toBeGreaterThan(0);
    });

    it('merges nothing into the empty snapshot rather than NaN', () => {
        const merged = mergeHistogramDigests([]);
        expect(merged).toEqual(emptyHistogramDigest());
        expect(digestSnapshot(merged)).toMatchObject({ count: 0, meanMs: 0, p99Ms: 0 });
        expect(digestSnapshot(null)).toMatchObject({ count: 0, p50Ms: 0 });
        expect(digestSnapshot(new Histogram().digest())).toMatchObject({ count: 0 });
    });

    it('states the bucket layout, so two builds cannot be merged blindly', () => {
        expect(HISTOGRAM_LAYOUT).toBe('ll-4-26');
    });
});
