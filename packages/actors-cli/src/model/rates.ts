/**
 * Turning cumulative counters into rates.
 *
 * Core reports monotonic totals since start with no windowing, deliberately:
 * a counter you read is cheap and a windowed one is a decision the runtime
 * should not make for you. But "12,483,201 calls" tells an operator nothing,
 * and "1,240 calls/s" tells them everything, so somebody has to diff two
 * snapshots — and it is fiddly enough that every consumer should not.
 *
 * The fiddly part is that a counter going DOWN is not an anomaly to clamp.
 * It means `metrics().reset()` ran, or the silo restarted, or (in a cluster
 * view) a peer dropped out of the fan-out. In every case the correct answer
 * is "no rate for this interval", not a negative one and not a huge positive
 * one from treating the new value as the delta.
 *
 * No terminal imports — a web dashboard needs exactly this.
 */

/** A per-second rate, or null when the interval could not produce one. */
export type Rate = number | null;

export interface RateSample {
    /** Epoch-ms of the reading. */
    at: number;
    value: number;
}

/**
 * Per-second rate between two readings.
 *
 * `null` when the counter moved backwards (a reset or a restart — the
 * previous total is meaningless, so report a GAP rather than invent a
 * number), or when no time passed (two reads inside the same millisecond
 * would divide by zero and report Infinity).
 */
export function rateBetween(previous: RateSample, current: RateSample): Rate {
    const elapsedMs = current.at - previous.at;
    if (elapsedMs <= 0) return null;
    if (current.value < previous.value) return null;
    return ((current.value - previous.value) * 1000) / elapsedMs;
}

/**
 * Tracks several named counters across snapshots.
 *
 * Deliberately holds only the PREVIOUS reading per series rather than a
 * window: the dashboard polls at a fixed interval and wants the
 * instantaneous rate, and a running average would smooth away exactly the
 * spike you opened the tool to look at. Sparklines keep their own history.
 */
export class RateTracker {
    #previous = new Map<string, RateSample>();

    /**
     * Record a reading and return the rate since the last one.
     *
     * The FIRST reading for a series always returns null — there is no
     * interval yet, and reporting the total as a rate would put a spike at
     * the left edge of every graph on startup.
     */
    observe(series: string, at: number, value: number): Rate {
        const previous = this.#previous.get(series);
        this.#previous.set(series, { at, value });
        if (!previous) return null;
        return rateBetween(previous, { at, value });
    }

    /** Was this series' last movement a reset (counter went backwards)? */
    lastWasReset(series: string, at: number, value: number): boolean {
        const previous = this.#previous.get(series);
        return previous !== undefined && at > previous.at && value < previous.value;
    }

    /** Forget a series — e.g. a silo that left the cluster. */
    forget(series: string): void {
        this.#previous.delete(series);
    }

    /** Forget every series not in `keep`, so a shrinking cluster does not leak. */
    retain(keep: Iterable<string>): void {
        const wanted = new Set(keep);
        // Deleting during Map iteration is specified to be safe: a removed
        // entry is simply not visited. No snapshot copy needed.
        for (const series of this.#previous.keys()) {
            if (!wanted.has(series)) this.#previous.delete(series);
        }
    }

    get size(): number {
        return this.#previous.size;
    }
}

/**
 * A bounded history for sparklines.
 *
 * `push(null)` records a gap rather than dropping the sample, so a reset or
 * an unreachable poll leaves a visible hole in the graph instead of a
 * straight line that reads as "nothing happened".
 */
export class Series {
    readonly capacity: number;
    #values: Rate[] = [];

    constructor(capacity = 60) {
        this.capacity = Math.max(1, Math.floor(capacity));
    }

    push(value: Rate): void {
        this.#values.push(value);
        if (this.#values.length > this.capacity) this.#values.shift();
    }

    values(): readonly Rate[] {
        return this.#values;
    }

    /** Highest non-gap value, or 0 for an empty or all-gap series. */
    peak(): number {
        let peak = 0;
        for (const value of this.#values) {
            if (value !== null && value > peak) peak = value;
        }
        return peak;
    }

    latest(): Rate {
        return this.#values.length === 0 ? null : this.#values[this.#values.length - 1]!;
    }

    /** Drop the history — for a "start watching from now" reset. */
    clear(): void {
        this.#values = [];
    }
}
