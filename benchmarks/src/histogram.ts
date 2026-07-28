/**
 * Latency samples with exact percentiles up to `capacity`, then reservoir
 * sampling.
 *
 * Bucketed histograms trade accuracy for unbounded capacity; we don't need
 * that. A benchmark run is seconds long, so a preallocated Float64Array
 * holds every sample (1M × 8B = 8MB) and percentiles are exact. Past
 * capacity we switch to Vitter's Algorithm R, which keeps a uniform sample
 * of the stream — percentiles stay unbiased, they just get noisier.
 *
 * The PRNG is seeded and deterministic on purpose: two runs of the same
 * scenario must differ because the CODE differs, never because the
 * reservoir rolled differently.
 */

export interface Percentiles {
    count: number;
    min: number;
    max: number;
    mean: number;
    p50: number;
    p90: number;
    p99: number;
    p999: number;
}

const DEFAULT_CAPACITY = 1 << 20;

/** xorshift32 — tiny, fast, and reproducible from a fixed seed. */
function makeRng(seed: number): () => number {
    let s = seed | 0 || 1;
    return () => {
        s ^= s << 13;
        s ^= s >>> 17;
        s ^= s << 5;
        // >>> 0 makes it unsigned; / 2^32 maps to [0, 1).
        return (s >>> 0) / 4294967296;
    };
}

export class Samples {
    readonly #buf: Float64Array;
    readonly #rng = makeRng(0x5eed);
    /** Samples currently held (≤ capacity). */
    #held = 0;
    /** Samples ever recorded — the reservoir denominator. */
    #seen = 0;
    #min = Infinity;
    #max = -Infinity;
    #sum = 0;

    constructor(capacity: number = DEFAULT_CAPACITY) {
        this.#buf = new Float64Array(capacity);
    }

    get count(): number {
        return this.#seen;
    }

    record(value: number): void {
        this.#seen++;
        this.#sum += value;
        if (value < this.#min) this.#min = value;
        if (value > this.#max) this.#max = value;
        if (this.#held < this.#buf.length) {
            this.#buf[this.#held++] = value;
            return;
        }
        // Algorithm R: the n-th sample survives with probability capacity/n.
        const j = Math.floor(this.#rng() * this.#seen);
        if (j < this.#buf.length) this.#buf[j] = value;
    }

    /**
     * min/max/mean come from the full stream; percentiles from the held
     * sample (exact while `count <= capacity`).
     */
    percentiles(): Percentiles {
        if (this.#seen === 0) {
            return { count: 0, min: 0, max: 0, mean: 0, p50: 0, p90: 0, p99: 0, p999: 0 };
        }
        const held = this.#buf.subarray(0, this.#held).slice();
        held.sort();
        const at = (q: number): number => {
            // Nearest-rank on a 0-indexed sorted array.
            const idx = Math.min(held.length - 1, Math.max(0, Math.ceil(q * held.length) - 1));
            return held[idx] as number;
        };
        return {
            count: this.#seen,
            min: this.#min,
            max: this.#max,
            mean: this.#sum / this.#seen,
            p50: at(0.5),
            p90: at(0.9),
            p99: at(0.99),
            p999: at(0.999)
        };
    }
}
