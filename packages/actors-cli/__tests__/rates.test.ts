/**
 * Rate derivation.
 *
 * The case that matters is a counter going BACKWARDS. Core exposes
 * monotonic totals with no windowing, so a dashboard must diff them — and
 * `metrics().reset()`, a silo restart, or a peer dropping out of a fan-out
 * all make the previous total meaningless. Reporting a negative rate, or a
 * huge positive one from treating the new value as the delta, is worse than
 * reporting nothing: both look like real traffic.
 */
import { describe, expect, it } from 'vitest';
import { rateBetween, RateTracker, Series } from '@sigx/actors-cli/source';

describe('rateBetween', () => {
    it('converts a delta into per-second', () => {
        expect(rateBetween({ at: 1000, value: 100 }, { at: 2000, value: 200 })).toBe(100);
        expect(rateBetween({ at: 1000, value: 100 }, { at: 3000, value: 200 })).toBe(50);
        expect(rateBetween({ at: 1000, value: 0 }, { at: 1500, value: 10 })).toBe(20);
    });

    it('returns a GAP when the counter went backwards', () => {
        // reset() ran, or the silo restarted. The previous total describes a
        // different lifetime, so there is no rate to report for this window.
        expect(rateBetween({ at: 1000, value: 500 }, { at: 2000, value: 10 })).toBeNull();
    });

    it('returns a gap rather than dividing by zero', () => {
        expect(rateBetween({ at: 1000, value: 10 }, { at: 1000, value: 20 })).toBeNull();
        // A clock stepped backwards must not produce a negative rate either.
        expect(rateBetween({ at: 2000, value: 10 }, { at: 1000, value: 20 })).toBeNull();
    });

    it('reports zero for a counter that did not move', () => {
        // Distinct from a gap: nothing happened IS a measurement.
        expect(rateBetween({ at: 1000, value: 42 }, { at: 2000, value: 42 })).toBe(0);
    });
});

describe('RateTracker', () => {
    it('has no rate for the first reading', () => {
        const tracker = new RateTracker();
        // Reporting the running total as a rate would put a spike at the
        // left edge of every graph on startup.
        expect(tracker.observe('calls', 1000, 5000)).toBeNull();
        expect(tracker.observe('calls', 2000, 5100)).toBe(100);
    });

    it('keeps series independent', () => {
        const tracker = new RateTracker();
        tracker.observe('a', 1000, 0);
        tracker.observe('b', 1000, 0);
        expect(tracker.observe('a', 2000, 10)).toBe(10);
        expect(tracker.observe('b', 2000, 50)).toBe(50);
    });

    it('recovers on the reading AFTER a reset', () => {
        const tracker = new RateTracker();
        tracker.observe('calls', 1000, 900);
        expect(tracker.observe('calls', 2000, 5)).toBeNull(); // reset
        // The new baseline is remembered, so the next window is real again
        // rather than a second gap.
        expect(tracker.observe('calls', 3000, 25)).toBe(20);
    });

    it('reports whether a reading was a reset', () => {
        const tracker = new RateTracker();
        tracker.observe('calls', 1000, 900);
        expect(tracker.lastWasReset('calls', 2000, 5)).toBe(true);
        expect(tracker.lastWasReset('calls', 2000, 1000)).toBe(false);
    });

    it('retains only the series still present, so a shrinking cluster cannot leak', () => {
        const tracker = new RateTracker();
        for (const id of ['s1', 's2', 's3']) tracker.observe(id, 1000, 0);
        expect(tracker.size).toBe(3);
        tracker.retain(['s1', 's3']);
        expect(tracker.size).toBe(2);
        // A silo that left and came back starts fresh rather than diffing
        // against its pre-departure total.
        expect(tracker.observe('s2', 2000, 100)).toBeNull();
    });

    it('forgets one series', () => {
        const tracker = new RateTracker();
        tracker.observe('a', 1000, 10);
        tracker.forget('a');
        expect(tracker.observe('a', 2000, 20)).toBeNull();
    });
});

describe('Series', () => {
    it('is bounded and keeps the newest', () => {
        const series = new Series(3);
        for (const value of [1, 2, 3, 4, 5]) series.push(value);
        expect(series.values()).toEqual([3, 4, 5]);
        expect(series.latest()).toBe(5);
    });

    it('records gaps as holes rather than dropping them', () => {
        const series = new Series(5);
        series.push(10);
        series.push(null);
        series.push(20);
        // A dropped sample would join 10 to 20 with a straight line, which
        // reads as "nothing happened" rather than "we lost this window".
        expect(series.values()).toEqual([10, null, 20]);
    });

    it('peaks over non-gap values only', () => {
        const series = new Series(5);
        series.push(null);
        series.push(7);
        series.push(null);
        expect(series.peak()).toBe(7);
        expect(new Series(3).peak()).toBe(0);
    });

    it('clamps a nonsensical capacity instead of behaving strangely', () => {
        expect(new Series(0).capacity).toBe(1);
        expect(new Series(-5).capacity).toBe(1);
    });
});
