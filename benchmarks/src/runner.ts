/**
 * Repetition and aggregation.
 *
 * One measurement is an anecdote. Every scenario runs once as warmup (JIT
 * tiering, first-call deopt and lazy module init all land there and are
 * discarded), then `runs` more times; we report the MEDIAN, which shrugs off
 * the single slow repetition that a background process will eventually
 * cause, where a mean would not.
 *
 * Repetitions are INTERLEAVED across scenarios — round 1 of every scenario,
 * then round 2, and so on — rather than finishing one scenario before
 * starting the next. This matters more than it looks: the calibration probe
 * showed a laptop losing 16% of its clock to throttling over a single suite,
 * which under sequential execution is charged entirely to whichever
 * scenarios happen to run last. That would quietly corrupt the dispatch
 * ladder, whose whole value is comparing scenarios against each other.
 * Interleaving spreads the drift across all of them, so each scenario's
 * median samples the same range of machine conditions.
 *
 * `spread` (IQR / median) rides along as the honesty check: if a scenario's
 * own repetitions disagree by more than the threshold a comparison uses,
 * that scenario cannot detect a regression of that size and the number
 * should not be trusted.
 */
import { observeGc, settleGc } from './memory.ts';
import type {
    AggregatedMetric,
    Metric,
    RunContext,
    Scenario,
    ScenarioResult
} from './types.ts';

function median(sorted: readonly number[]): number {
    const n = sorted.length;
    if (n === 0) return 0;
    const mid = n >> 1;
    return n % 2
        ? (sorted[mid] as number)
        : ((sorted[mid - 1] as number) + (sorted[mid] as number)) / 2;
}

function quantile(sorted: readonly number[], q: number): number {
    if (sorted.length === 0) return 0;
    const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil(q * sorted.length) - 1));
    return sorted[idx] as number;
}

function aggregate(perRun: readonly Metric[][]): AggregatedMetric[] {
    // Preserve first-run metric order; scenarios emit them in a meaningful
    // sequence (ascending concurrency) and the report should keep it.
    const order: string[] = [];
    const byName = new Map<string, { meta: Metric; values: number[] }>();
    for (const run of perRun) {
        for (const m of run) {
            let entry = byName.get(m.name);
            if (!entry) {
                entry = { meta: m, values: [] };
                byName.set(m.name, entry);
                order.push(m.name);
            }
            entry.values.push(m.value);
        }
    }

    return order.map((name) => {
        const { meta, values } = byName.get(name) as { meta: Metric; values: number[] };
        const sorted = [...values].sort((a, b) => a - b);
        const med = median(sorted);
        const lo = sorted[0] as number;
        const hi = sorted[sorted.length - 1] as number;

        // Noise estimate. With the handful of repetitions a benchmark can
        // afford, IQR is computed from two order statistics out of five and
        // badly understates dispersion — identical code kept producing
        // "regressions" that the gate waved through because the measured
        // IQR was tiny while the samples actually ranged 20%. The full range
        // is the honest estimator at this sample size. It does not blunt
        // real regressions: a genuine slowdown moves ALL the samples
        // together, so the range stays narrow while the median shifts.
        const dispersion =
            sorted.length < 8 ? hi - lo : quantile(sorted, 0.75) - quantile(sorted, 0.25);

        const aggregated: AggregatedMetric = {
            name,
            unit: meta.unit,
            direction: meta.direction,
            value: med,
            samples: values,
            min: lo,
            max: hi,
            spread: med === 0 ? 0 : Math.abs(dispersion / med)
        };
        if (meta.informational) aggregated.informational = true;
        if (meta.noiseFloor !== undefined) aggregated.noiseFloor = meta.noiseFloor;
        return aggregated;
    });
}

export interface RunnerOptions extends RunContext {
    runs: number;
    /** Add a dedicated GC-profiling pass. On by default. */
    gcProfile: boolean;
    /**
     * Machine-speed probe, sampled once per round so it sees the same CPU
     * contention the scenarios do.
     */
    probe?(): Promise<number>;
    onProgress?(message: string): void;
}

export interface SuiteOutcome {
    results: ScenarioResult[];
    calibrationSamples: number[];
}

interface Collected {
    scenario: Scenario;
    perRun: Metric[][];
    error?: string;
}

function describeError(error: unknown): string {
    return error instanceof Error ? `${error.message}\n${error.stack ?? ''}` : String(error);
}

export async function runSuite(
    scenarios: readonly Scenario[],
    options: RunnerOptions
): Promise<SuiteOutcome> {
    const { runs, gcProfile, probe, onProgress, ...ctx } = options;
    const calibrationSamples: number[] = [];
    const log = onProgress ?? ((): void => {});

    const collected: Collected[] = scenarios.map((scenario) => ({ scenario, perRun: [] }));

    // Warmup pass — discarded. Also surfaces a broken scenario before we
    // spend the whole measurement budget on it.
    for (const entry of collected) {
        try {
            await entry.scenario.run(ctx);
        } catch (error) {
            entry.error = describeError(error);
        }
        log(`warmup ${entry.scenario.name}`);
    }

    for (let round = 1; round <= runs; round++) {
        if (probe) calibrationSamples.push(await probe());
        for (const entry of collected) {
            if (entry.error) continue;
            try {
                // Start each repetition from a quiet heap so one
                // repetition's garbage lands in its own GC totals.
                await settleGc();
                entry.perRun.push(await entry.scenario.run(ctx));
            } catch (error) {
                entry.error = describeError(error);
            }
        }
        log(`round ${round}/${runs} complete`);
    }

    if (gcProfile) {
        // A dedicated pass, because GCProfiler costs ~3% and that is the
        // same order as the regression threshold — folding it into the
        // measured runs would make every throughput number 3% pessimistic
        // and add noise to the one signal it provides.
        for (const entry of collected) {
            if (entry.error || entry.perRun.length === 0) continue;
            try {
                await settleGc();
                const gc = observeGc();
                await entry.scenario.run(ctx);
                const totals = gc.stop();
                // All informational: this pass produces ONE sample, so these
                // have no measured spread and the noise gate cannot soften
                // them — gating on them made identical code report 5-8%
                // "regressions" in GC pause every run. They stay as
                // attribution aids (a throughput drop with a matching
                // GC-pause jump names its own cause), and `printComparison`
                // surfaces large moves under Diagnostics without failing.
                const gcMetrics: Metric[] = [
                    {
                        name: 'gc/pause_ms',
                        value: totals.pauseMs,
                        unit: 'ms',
                        direction: 'lower',
                        informational: true
                    },
                    {
                        name: 'gc/collections',
                        value: totals.collections,
                        unit: 'count',
                        direction: 'lower',
                        informational: true
                    },
                    {
                        name: 'gc/major_collections',
                        value: totals.majorCollections,
                        unit: 'count',
                        direction: 'lower',
                        informational: true
                    }
                ];
                for (const run of entry.perRun) run.push(...gcMetrics);
            } catch (error) {
                entry.error = describeError(error);
            }
        }
        log('gc pass complete');
    }

    const results = collected.map(({ scenario, perRun, error }) => {
        const result: ScenarioResult = {
            name: scenario.name,
            description: scenario.description,
            metrics: error ? [] : aggregate(perRun)
        };
        if (error) result.error = error;
        return result;
    });
    return { results, calibrationSamples };
}
