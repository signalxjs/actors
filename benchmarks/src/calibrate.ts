/**
 * A machine-drift probe.
 *
 * Running two identical suites back to back produced 20-40% swings across
 * EVERY scenario — not a code change, just a machine that had warmed up.
 * Without a way to see that, a comparison confidently reports a dozen
 * regressions and the suite becomes worse than useless.
 *
 * So: run a fixed, actor-free workload before and after the scenarios. It
 * touches none of the code under test, so any change in ITS score is the
 * machine. Comparing the probe against the baseline's probe answers "were
 * these two runs measured under the same conditions?"; comparing start
 * against end answers "did the machine drift DURING this run?".
 *
 * THE PROBE MUST MATCH THE WORKLOAD'S SHAPE. The first version was a tight
 * integer loop — pure ALU, no allocation — and it was useless: it moved 4%
 * while the actor scenarios moved 25%. What degrades under sustained load
 * here is not raw arithmetic but promise churn, allocation and the GC and
 * memory traffic behind them. So the probe is now an async microtask loop
 * that allocates a small object per iteration, which is exactly the shape
 * of one turn.
 *
 * This does not fix drift — nothing in-process can. It makes drift visible
 * instead of letting it masquerade as a regression.
 */

/** Iterations between clock reads; keeps the probe's own overhead low. */
const BATCH = 500;

export async function calibrationScore(durationMs = 60): Promise<number> {
    const deadline = performance.now() + durationMs;
    let ops = 0;
    let sink: { a: number; b: string } | null = null;
    do {
        for (let i = 0; i < BATCH; i++) {
            // One microtask + one short-lived allocation — a turn in
            // miniature, minus the actor runtime.
            sink = await Promise.resolve({ a: i, b: 'probe' });
            ops++;
        }
    } while (performance.now() < deadline);
    // Consume `sink` so the loop cannot be optimized away. Never true.
    if (sink !== null && sink.b === 'never') process.stdout.write('');
    return ops;
}

export interface Calibration {
    /**
     * One probe score per interleaved round, in order.
     *
     * Sampled THROUGHOUT the suite rather than just at the ends, because
     * the dominant noise source turned out to be other processes competing
     * for the CPU — which comes and goes. Two 60ms probes at the start and
     * end can land in quiet windows and report a calm machine while the
     * scenarios, running for seconds each, sat through the contention. One
     * sample per round gives the probe the same exposure as the scenarios.
     */
    samples: number[];
}

function median(values: readonly number[]): number {
    if (values.length === 0) return 0;
    const sorted = [...values].sort((a, b) => a - b);
    const mid = sorted.length >> 1;
    return sorted.length % 2
        ? (sorted[mid] as number)
        : ((sorted[mid - 1] as number) + (sorted[mid] as number)) / 2;
}

/** The representative machine speed for this run. */
export function calibrationMedian(calibration: Calibration): number {
    return median(calibration.samples);
}

/**
 * Spread of the probe across the run, as a fraction of its median. High
 * spread means the machine was busy with something else — the whole run is
 * untrustworthy, not just one scenario.
 */
export function instability(calibration: Calibration): number {
    const { samples } = calibration;
    if (samples.length < 2) return 0;
    const mid = median(samples);
    if (mid === 0) return 0;
    return (Math.max(...samples) - Math.min(...samples)) / mid;
}
