/** The scenario contract and the result shapes written to JSON. */
import type { Calibration } from './calibrate.ts';
import type { BenchEnv } from './env.ts';

/**
 * `lower` = smaller is better (latency, bytes, GC pause). `higher` =
 * bigger is better (ops/sec). The comparer needs this to know which
 * direction of a delta is a regression; getting it wrong inverts the
 * verdict, so every metric must declare it.
 */
export type Direction = 'lower' | 'higher';

export interface Metric {
    /** Unique within a scenario, e.g. `c=64/ops_per_sec`. */
    name: string;
    value: number;
    unit: string;
    direction: Direction;
    /**
     * Informational metrics are recorded and printed but never fail a
     * comparison — sample counts, GC collection counts, and anything whose
     * run-to-run variance is structurally high.
     */
    informational?: boolean;
    /**
     * Absolute difference, in this metric's own unit, below which a change
     * is not meaningful. REQUIRED for any metric that can sit at or below
     * zero (heap slopes, retained bytes): percentage change against a
     * baseline of ~0 is meaningless, and against a NEGATIVE baseline it
     * silently inverts — a leak appearing where there was none would be
     * reported as an improvement. When this is set, the comparer falls back
     * to absolute differences instead of guessing.
     */
    noiseFloor?: number;
}

export interface RunContext {
    /** Seconds of measurement per concurrency level. */
    durationMs: number;
    /** Shrink work so a full pass takes seconds — for smoke-testing. */
    quick: boolean;
}

export interface Scenario {
    /** Stable id, `group/name`; also the CLI filter target. */
    name: string;
    description: string;
    /**
     * One repetition. The runner calls this once as warmup (discarded) and
     * then `--runs` times, taking the median of each metric. Must be
     * self-contained: create and tear down its own silos.
     */
    run(ctx: RunContext): Promise<Metric[]>;
}

export interface AggregatedMetric extends Metric {
    /** Median across repetitions — the reported value. */
    value: number;
    samples: number[];
    min: number;
    max: number;
    /** Interquartile range as a fraction of the median; the noise estimate. */
    spread: number;
}

export interface ScenarioResult {
    name: string;
    description: string;
    metrics: AggregatedMetric[];
    error?: string;
}

export interface BenchResult {
    /** Bump when the result shape changes so old baselines are rejected. */
    formatVersion: 1;
    createdAt: string;
    env: BenchEnv;
    runs: number;
    durationMs: number;
    quick: boolean;
    /**
     * Actor-free CPU probe, sampled once per interleaved round. Any change
     * here is the machine, not the code — it is what separates a real
     * regression from a laptop that got busy.
     */
    calibration: Calibration;
    scenarios: ScenarioResult[];
}
