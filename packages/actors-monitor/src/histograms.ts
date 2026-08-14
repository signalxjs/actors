/**
 * A latency histogram as three comparable points.
 *
 * The one shape core produces that a renderer has to make a judgement about:
 * `null` for an absent or empty histogram rather than three zeroed points,
 * because zero asserts "we measured, and it was fast" while null says "no
 * reading". `metrics({ histograms: false })` and a host that has served
 * nothing are both the latter, and drawing either as a flat row of zeroes is
 * the same lie a missing metrics plugin tells when it renders as 0 calls/s.
 *
 * Scaling is deliberately NOT here: it is a drawing decision (one axis across
 * latency, queue and turn, because the comparison IS the diagnosis) and each
 * renderer owns the arithmetic in its own units — terminal cells, CSS
 * percentages.
 */
import type { HistogramSnapshot } from '@sigx/actors/host';

export interface PercentilePoint {
    /** `p50` / `p90` / `p99`. */
    label: string;
    /** Milliseconds, or null for "no reading". */
    value: number | null;
}

/** p50 / p90 / p99, or three nulls for an absent or empty histogram. */
export function percentilePoints(snapshot: HistogramSnapshot | null | undefined): PercentilePoint[] {
    const empty = !snapshot || snapshot.count === 0;
    return [
        { label: 'p50', value: empty ? null : snapshot.p50Ms },
        { label: 'p90', value: empty ? null : snapshot.p90Ms },
        { label: 'p99', value: empty ? null : snapshot.p99Ms }
    ];
}

/**
 * The largest value across a set of histograms — the scale a shared axis
 * needs to cover.
 *
 * Every percentile is offered, not just p99: the axis has to cover the
 * tallest point actually drawn. Returns 0 when nothing has a reading, which
 * a renderer should treat as "draw no bars" rather than dividing by it.
 */
export function percentileCeiling(
    snapshots: readonly (HistogramSnapshot | null | undefined)[]
): number {
    let ceiling = 0;
    for (const snapshot of snapshots) {
        if (!snapshot || snapshot.count === 0) continue;
        for (const value of [snapshot.p50Ms, snapshot.p90Ms, snapshot.p99Ms]) {
            if (Number.isFinite(value) && value > ceiling) ceiling = value;
        }
    }
    return ceiling;
}
