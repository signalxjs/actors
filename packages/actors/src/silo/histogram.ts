/**
 * A bounded log-linear histogram — the counting primitive behind `metrics()`.
 *
 * Constraints that decide the design: it records on every turn, so `record()`
 * must be O(1) with no allocation; it runs for the life of a process, so
 * memory must be bounded regardless of how many samples arrive; and it must
 * bring no dependency (issue #38: "pull-based counters first; no
 * metrics-library dependency").
 *
 * Keeping raw samples — what the benchmark suite does — is wrong here for
 * exactly that reason: bounded runs can afford exact percentiles, a
 * long-lived server cannot.
 *
 * Values are recorded in MICROSECONDS. An uncontended turn is ~0.5µs, so a
 * millisecond-resolution histogram would put nearly every healthy turn in
 * bucket zero and report p99 = 0 — useless precisely where the system is
 * behaving. Percentiles are reported back in milliseconds for consistency
 * with the rest of the API.
 */

/** Sub-buckets per power of two. 16 → worst-case ~6% bucket-relative error. */
const SUB_BITS = 4;
const SUB = 1 << SUB_BITS;
/**
 * Highest octave tracked: 2^26 µs ≈ 67s. Anything slower is already a
 * catastrophe and lands in the final bucket; `max` is tracked exactly
 * anyway, so nothing is lost that matters.
 */
const MAX_OCTAVE = 26;
const BUCKETS = SUB + (MAX_OCTAVE - SUB_BITS + 1) * SUB;

export interface HistogramSnapshot {
    count: number;
    /** Milliseconds. Exact, not bucketed. */
    minMs: number;
    maxMs: number;
    meanMs: number;
    /** Milliseconds, bucketed — accurate to ~6%. */
    p50Ms: number;
    p90Ms: number;
    p99Ms: number;
}

const EMPTY: HistogramSnapshot = {
    count: 0,
    minMs: 0,
    maxMs: 0,
    meanMs: 0,
    p50Ms: 0,
    p90Ms: 0,
    p99Ms: 0
};

/** Above this, `Math.clz32` can no longer describe the value (uint32). */
const CLZ32_LIMIT = 2 ** 32;

/**
 * Bucket for a microsecond value: linear below `SUB` (so single-microsecond
 * turns keep full resolution), log-linear above it.
 */
function bucketOf(us: number): number {
    if (us < SUB) return us;
    // Guard BEFORE clz32: it coerces to uint32, so a duration past 2^32µs
    // (~71 min) would wrap and be filed as a FAST turn — the exact opposite
    // of the truth, and precisely the sample you most want to see.
    if (us >= CLZ32_LIMIT) return BUCKETS - 1;
    // 31 - clz32 is floor(log2(us)) — the octave.
    const octave = 31 - Math.clz32(us);
    if (octave > MAX_OCTAVE) return BUCKETS - 1;
    const sub = (us >>> (octave - SUB_BITS)) - SUB;
    return (octave - SUB_BITS + 1) * SUB + sub;
}

/** Representative µs value for a bucket — the bottom of its range. */
function valueOf(bucket: number): number {
    if (bucket < SUB) return bucket;
    const octave = ((bucket - SUB) / SUB | 0) + SUB_BITS;
    const sub = (bucket - SUB) % SUB;
    return (SUB + sub) << (octave - SUB_BITS);
}

export class Histogram {
    /**
     * Float64, not Uint32. A hot bucket would wrap at 2^32 samples, which at
     * ~1-2M ops/s is under an hour — the counts would silently reset and
     * corrupt every percentile while `#count` kept climbing. Float64 holds
     * exact integers to 2^53, which at the same rate is ~140 years. The
     * extra 1.8KB per histogram is not worth a wrong p99.
     */
    readonly #buckets = new Float64Array(BUCKETS);
    #count = 0;
    #sumUs = 0;
    #minUs = Infinity;
    #maxUs = 0;

    /** Record a duration in MILLISECONDS. */
    record(ms: number): void {
        // Negative can only come from a clock adjustment mid-turn; clamp
        // rather than corrupt the buckets.
        const us = ms > 0 ? Math.round(ms * 1000) : 0;
        this.#count++;
        this.#sumUs += us;
        if (us < this.#minUs) this.#minUs = us;
        if (us > this.#maxUs) this.#maxUs = us;
        this.#buckets[bucketOf(us)]!++;
    }

    reset(): void {
        this.#buckets.fill(0);
        this.#count = 0;
        this.#sumUs = 0;
        this.#minUs = Infinity;
        this.#maxUs = 0;
    }

    snapshot(): HistogramSnapshot {
        if (this.#count === 0) return EMPTY;
        const at = (q: number): number => {
            const target = q * this.#count;
            let seen = 0;
            for (let i = 0; i < BUCKETS; i++) {
                seen += this.#buckets[i] as number;
                if (seen >= target) return valueOf(i) / 1000;
            }
            return this.#maxUs / 1000;
        };
        return {
            count: this.#count,
            minMs: this.#minUs / 1000,
            maxMs: this.#maxUs / 1000,
            meanMs: this.#sumUs / this.#count / 1000,
            p50Ms: at(0.5),
            p90Ms: at(0.9),
            p99Ms: at(0.99)
        };
    }
}
