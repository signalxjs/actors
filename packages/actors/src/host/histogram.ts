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

/**
 * Bucket layout id — the shape of `#buckets`, derived from the constants
 * above rather than written out, because the failure it guards against is
 * somebody editing `SUB_BITS` and forgetting a hand-maintained string.
 *
 * A bucket INDEX only means anything within one layout, and a digest
 * arrives over a wire from a peer that may be running a different build. A
 * mismatch has to be detectable, or a rolling deploy silently produces a
 * merged distribution assembled from two different axes.
 */
export const HISTOGRAM_LAYOUT = `ll-${SUB_BITS}-${MAX_OCTAVE}`;

/**
 * A histogram's raw distribution, in a shape that can be SUMMED.
 *
 * This exists because `HistogramSnapshot` cannot be merged: averaging two
 * hosts' p99s is not the cluster's p99 and is not any other statistic
 * either. Percentiles have to be re-derived from the combined counts, so
 * the counts are what travels.
 *
 * **Sparse** — `idx` holds the occupied bucket indices ascending, `n` the
 * counts parallel to it. Of 384 buckets a real host occupies a few dozen,
 * so this is several times smaller than a dense array on a payload that a
 * dashboard polls once a second per host.
 *
 * `sumUs`/`minUs`/`maxUs` are exact rather than bucketed, so a merged mean
 * is a real mean rather than a mean of means.
 */
export interface HistogramDigest {
    count: number;
    sumUs: number;
    minUs: number;
    maxUs: number;
    /** Occupied bucket indices, ascending. */
    idx: number[];
    /** Sample counts, parallel to `idx`. */
    n: number[];
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
        return summarize(
            (bucket) => this.#buckets[bucket] as number,
            this.#count,
            this.#sumUs,
            this.#minUs,
            this.#maxUs
        );
    }

    /**
     * The raw distribution, for merging across hosts.
     *
     * Sparse, and built here rather than on `record()` — recording stays
     * allocation-free; this is snapshot-time work.
     */
    digest(): HistogramDigest {
        const idx: number[] = [];
        const n: number[] = [];
        for (let i = 0; i < BUCKETS; i++) {
            const value = this.#buckets[i] as number;
            if (value > 0) {
                idx.push(i);
                n.push(value);
            }
        }
        return {
            count: this.#count,
            sumUs: this.#sumUs,
            minUs: this.#count === 0 ? 0 : this.#minUs,
            maxUs: this.#maxUs,
            idx,
            n
        };
    }
}

/** A digest with nothing in it — the shape a merge of nothing produces. */
export function emptyHistogramDigest(): HistogramDigest {
    return { count: 0, sumUs: 0, minUs: 0, maxUs: 0, idx: [], n: [] };
}

/**
 * Sum digests bucket-wise.
 *
 * This is the whole point of shipping buckets: the merged distribution is
 * the cluster's real distribution, so percentiles taken from it are the
 * cluster's real percentiles. Averaging per-host percentiles instead — the
 * only thing `HistogramSnapshot` allows — produces a number that is not the
 * p99 of anything.
 *
 * Indices arrive over a wire, so every one of them is BOUNDS-CHECKED rather
 * than used as an offset. A peer that is buggy, mid-upgrade or hostile must
 * cost its own contribution and nothing else.
 */
export function mergeHistogramDigests(
    digests: Iterable<HistogramDigest | null | undefined>
): HistogramDigest {
    const buckets = new Float64Array(BUCKETS);
    let count = 0;
    let sumUs = 0;
    let minUs = Infinity;
    let maxUs = 0;
    let any = false;

    for (const digest of digests) {
        if (!digest || !Array.isArray(digest.idx) || !Array.isArray(digest.n)) continue;
        if (!Number.isFinite(digest.count) || digest.count <= 0) continue;
        any = true;
        count += digest.count;
        sumUs += Number.isFinite(digest.sumUs) ? digest.sumUs : 0;
        if (Number.isFinite(digest.minUs) && digest.minUs < minUs) minUs = digest.minUs;
        if (Number.isFinite(digest.maxUs) && digest.maxUs > maxUs) maxUs = digest.maxUs;
        const pairs = Math.min(digest.idx.length, digest.n.length);
        for (let i = 0; i < pairs; i++) {
            const bucket = digest.idx[i] as number;
            const value = digest.n[i] as number;
            // A non-integer, negative or out-of-range index would either
            // throw away the write silently (a fractional offset) or reach
            // past the array; either way it must not become a data point.
            if (!Number.isInteger(bucket) || bucket < 0 || bucket >= BUCKETS) continue;
            if (!Number.isFinite(value) || value <= 0) continue;
            buckets[bucket]! += value;
        }
    }

    if (!any) return emptyHistogramDigest();
    const idx: number[] = [];
    const n: number[] = [];
    for (let i = 0; i < BUCKETS; i++) {
        const value = buckets[i] as number;
        if (value > 0) {
            idx.push(i);
            n.push(value);
        }
    }
    return { count, sumUs, minUs: minUs === Infinity ? 0 : minUs, maxUs, idx, n };
}

/**
 * Percentiles re-derived from a (usually merged) distribution.
 *
 * The counterpart to `Histogram.snapshot()`, sharing its walk so the two
 * can never drift: same buckets, same rounding, same answer.
 */
export function digestSnapshot(digest: HistogramDigest | null | undefined): HistogramSnapshot {
    if (!digest || !Number.isFinite(digest.count) || digest.count <= 0) return EMPTY;
    const counts = new Float64Array(BUCKETS);
    const pairs = Math.min(digest.idx?.length ?? 0, digest.n?.length ?? 0);
    for (let i = 0; i < pairs; i++) {
        const bucket = digest.idx[i] as number;
        const value = digest.n[i] as number;
        if (!Number.isInteger(bucket) || bucket < 0 || bucket >= BUCKETS) continue;
        if (!Number.isFinite(value) || value <= 0) continue;
        counts[bucket]! += value;
    }
    return summarize(
        (bucket) => counts[bucket] as number,
        digest.count,
        digest.sumUs,
        digest.minUs,
        digest.maxUs
    );
}

/**
 * The one percentile walk, shared by the live histogram and a merged
 * digest — duplicating it is how a cluster-wide p99 quietly stops matching
 * the per-host one it is supposed to generalise.
 */
function summarize(
    countAt: (bucket: number) => number,
    count: number,
    sumUs: number,
    minUs: number,
    maxUs: number
): HistogramSnapshot {
    const at = (q: number): number => {
        const target = q * count;
        let seen = 0;
        for (let i = 0; i < BUCKETS; i++) {
            seen += countAt(i);
            if (seen >= target) return valueOf(i) / 1000;
        }
        return maxUs / 1000;
    };
    return {
        count,
        minMs: minUs / 1000,
        maxMs: maxUs / 1000,
        meanMs: sumUs / count / 1000,
        p50Ms: at(0.5),
        p90Ms: at(0.9),
        p99Ms: at(0.99)
    };
}
