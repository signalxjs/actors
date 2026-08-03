/**
 * Console tables, result persistence, and baseline comparison.
 *
 * The comparison rule is deliberately conservative: a metric is only called
 * a regression when it moves against its `direction` by more than the
 * threshold AND by more than the scenario's own measured noise. A perf
 * suite that cries wolf gets ignored, and an ignored suite is worse than
 * none — so when the run-to-run spread is wider than the delta, the honest
 * answer is "cannot tell", and that is what it prints.
 */
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { calibrationMedian, instability } from './calibrate.ts';
import { envMismatch, INFRA_SHAPE_MISMATCH } from './env.ts';
import type { AggregatedMetric, BenchResult } from './types.ts';

export const RESULTS_DIR = fileURLToPath(new URL('../results/', import.meta.url));
export const BASELINE_PATH = fileURLToPath(new URL('../baselines/local.json', import.meta.url));

/**
 * Default regression threshold: 10% against the metric's good direction.
 *
 * Calibrated, not guessed. Running identical code against its own baseline
 * (5 runs each) reliably produced ~5-6% swings in tail-latency metrics
 * under queueing — p99 at c=64 is inherently noisier than throughput, and a
 * 5% gate failed on unchanged code every time. A suite that cries wolf gets
 * ignored, so the floor sits above the measured noise. Throughput metrics
 * are far steadier; tighten with `--threshold=5` on a quiet machine with
 * `--runs=9` when chasing something small.
 */
export const DEFAULT_THRESHOLD = 0.1;

/** A metric value in its own unit, scaled for reading. */
export function fmt(value: number, unit: string): string {
    if (unit === 'bytes') {
        if (Math.abs(value) >= 1024 * 1024) return `${(value / 1024 / 1024).toFixed(2)} MiB`;
        if (Math.abs(value) >= 1024) return `${(value / 1024).toFixed(1)} KiB`;
        return `${value.toFixed(0)} B`;
    }
    if (unit === 'ops/s') {
        if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
        if (value >= 1000) return `${(value / 1000).toFixed(1)}k`;
        return value.toFixed(1);
    }
    if (unit === 'ms') {
        if (value < 1) return `${(value * 1000).toFixed(1)}µs`;
        return `${value.toFixed(3)}ms`;
    }
    return `${value.toFixed(2)} ${unit}`;
}

function pad(text: string, width: number): string {
    return text.length >= width ? text : text + ' '.repeat(width - text.length);
}

function padStart(text: string, width: number): string {
    return text.length >= width ? text : ' '.repeat(width - text.length) + text;
}

export function printResult(result: BenchResult): void {
    console.log('');
    console.log(
        `${result.env.node} ${result.env.platform}/${result.env.arch} · ${result.env.cpuModel}`
    );
    console.log(
        `conditions: ${result.env.conditions} · commit ${result.env.commit}${
            result.env.dirty ? ' (dirty)' : ''
        } · ${result.runs} runs × ${result.durationMs}ms${result.quick ? ' · QUICK' : ''}`
    );
    const jitter = instability(result.calibration);
    if (jitter > MACHINE_INSTABILITY_LIMIT) {
        console.log(
            `⚠ THE MACHINE WAS BUSY: the actor-free probe varied ${(jitter * 100).toFixed(0)}% ` +
                `across rounds, so something else was competing for the CPU. ` +
                `Repetitions are interleaved and reported as medians, which absorbs ` +
                `some of this — but treat small differences as meaningless. Close ` +
                `other applications and re-run before trusting anything under ~20%.`
        );
    }

    for (const scenario of result.scenarios) {
        console.log('');
        console.log(`▸ ${scenario.name} — ${scenario.description}`);
        if (scenario.error) {
            console.log(`  FAILED: ${scenario.error.split('\n')[0]}`);
            continue;
        }
        const nameWidth = Math.max(...scenario.metrics.map((m) => m.name.length), 10);
        for (const m of scenario.metrics) {
            // A metric centred on zero has an unbounded relative spread —
            // `±20190%` is technically true and completely unreadable, so
            // anything past 100% just says so.
            const noisy =
                m.spread > 1 ? '  (noise ≫ signal)' : m.spread > 0.1 ? `  ±${(m.spread * 100).toFixed(0)}%` : '';
            console.log(
                `    ${pad(m.name, nameWidth)}  ${padStart(fmt(m.value, m.unit), 12)}${noisy}`
            );
        }
    }
}

export function saveResult(result: BenchResult, path?: string): string {
    const target = path ?? `${RESULTS_DIR}${result.createdAt.replace(/[:.]/g, '-')}.json`;
    mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, `${JSON.stringify(result, null, 2)}\n`);
    return target;
}

export function loadResult(path: string): BenchResult {
    const parsed = JSON.parse(readFileSync(path, 'utf8')) as BenchResult;
    if (parsed.formatVersion !== 1) {
        throw new Error(
            `baseline at ${path} has formatVersion ${parsed.formatVersion}; expected 1. ` +
                `Re-record it with \`pnpm bench:baseline\`.`
        );
    }
    return parsed;
}

type Verdict = 'regressed' | 'improved' | 'unchanged' | 'inconclusive' | 'new';

/** Informational metrics only appear as diagnostics past this much change. */
export const DIAGNOSTIC_THRESHOLD = 0.25;

/**
 * Probe variation across rounds past this much means the machine was busy.
 * Measured on a developer laptop with a browser and a music player running,
 * the probe routinely swings 15%+ — which is precisely the situation the
 * warning exists to name.
 */
export const MACHINE_INSTABILITY_LIMIT = 0.12;

/**
 * Probe difference against the baseline past this much invalidates it.
 *
 * Set at HALF the regression threshold, deliberately: if the machine alone
 * moved by half of what we are willing to call a regression, we can no
 * longer say which half of a 10% move was the code. Observed in practice —
 * a run with a -6.2% probe delta produced two 12-16% "regressions" on
 * identical code, which a 7% limit let through and a 5% limit correctly
 * suppresses.
 */
export const MACHINE_DRIFT_LIMIT = DEFAULT_THRESHOLD / 2;

export interface Comparison {
    scenario: string;
    metric: string;
    unit: string;
    informational?: boolean;
    /** Compared exactly — see `Metric.exact`. Never downgraded by drift. */
    exact?: boolean;
    baseline: number;
    current: number;
    /**
     * Signed fraction; positive always means "better", per `direction`.
     * `null` when a relative change is not defined (baseline at or below
     * zero) — read `absoluteDiff` instead.
     */
    delta: number | null;
    absoluteDiff: number;
    verdict: Verdict;
}

function compareMetric(
    scenario: string,
    current: AggregatedMetric,
    baseline: AggregatedMetric | undefined,
    threshold: number
): Comparison {
    if (!baseline) {
        return {
            scenario,
            metric: current.name,
            unit: current.unit,
            baseline: 0,
            current: current.value,
            delta: null,
            absoluteDiff: 0,
            verdict: 'new'
        };
    }

    const absoluteDiff = current.value - baseline.value;
    // Positive `improvement` always means better, whichever direction the
    // metric runs in.
    const improvement = current.direction === 'higher' ? absoluteDiff : -absoluteDiff;
    const floor = current.noiseFloor ?? 0;
    const base = {
        scenario,
        metric: current.name,
        unit: current.unit,
        baseline: baseline.value,
        current: current.value,
        absoluteDiff,
        ...(current.informational ? { informational: true } : {})
    };

    // An invariant, not a measurement: no threshold, no noise gate. Both
    // exist to absorb variance this metric does not have, and applying them
    // would hide the one class of change a shared runner can prove.
    if (current.exact && !current.informational) {
        return {
            ...base,
            exact: true,
            delta: baseline.value > 0 ? improvement / baseline.value : null,
            verdict:
                absoluteDiff === 0 ? 'unchanged' : improvement < 0 ? 'regressed' : 'improved'
        };
    }

    if (current.informational) {
        // Still compute the change so `printComparison` can surface a big
        // move as a diagnostic; it just never counts as a regression.
        return {
            ...base,
            delta: baseline.value > 0 ? improvement / baseline.value : null,
            verdict: 'unchanged'
        };
    }
    if (Math.abs(absoluteDiff) <= floor) {
        return { ...base, delta: null, verdict: 'unchanged' };
    }

    // A percentage against a baseline of zero is undefined, and against a
    // NEGATIVE baseline it flips sign — so once the baseline is not safely
    // positive, judge on the absolute difference alone. Metrics that can
    // land there are required to carry a noiseFloor, which the branch above
    // has already applied.
    if (baseline.value <= 0) {
        return {
            ...base,
            delta: null,
            verdict: improvement < 0 ? 'regressed' : 'improved'
        };
    }

    const delta = improvement / baseline.value;
    // The noise gate is the wider of the two runs' own spreads: a delta
    // smaller than the measurement's own scatter is not evidence.
    const noise = Math.max(current.spread, baseline.spread);

    let verdict: Verdict;
    if (Math.abs(delta) <= threshold) verdict = 'unchanged';
    else if (Math.abs(delta) <= noise) verdict = 'inconclusive';
    else verdict = delta < 0 ? 'regressed' : 'improved';

    return { ...base, delta, verdict };
}

export interface CompareOutcome {
    comparisons: Comparison[];
    regressions: Comparison[];
    /**
     * The subset of `regressions` on `exact` metrics — invariants that
     * MOVED. Machine-independent, so unlike every other row here this one
     * carries the same weight on a shared CI runner as on a quiet desk, and
     * is the only thing the Bench workflow is willing to fail on.
     */
    exactRegressions: Comparison[];
    envWarnings: string[];
    /**
     * How much slower/faster the bare-CPU probe was than the baseline's.
     * If this is materially non-zero, every scenario moved for a reason that
     * has nothing to do with the code.
     */
    machineDelta: number;
    /**
     * Mismatches that make the comparison meaningless rather than merely
     * suspect — currently a differing deployment shape. Non-empty means the
     * caller must refuse to print a verdict.
     */
    fatalMismatch: string[];
}

export function compare(
    current: BenchResult,
    baseline: BenchResult,
    threshold = DEFAULT_THRESHOLD
): CompareOutcome {
    const envWarnings = envMismatch(current.env, baseline.env);
    const baseByScenario = new Map(baseline.scenarios.map((s) => [s.name, s]));
    const comparisons: Comparison[] = [];

    for (const scenario of current.scenarios) {
        if (scenario.error) continue;
        const base = baseByScenario.get(scenario.name);
        const baseMetrics = new Map(base?.metrics.map((m) => [m.name, m]) ?? []);
        for (const metric of scenario.metrics) {
            comparisons.push(
                compareMetric(scenario.name, metric, baseMetrics.get(metric.name), threshold)
            );
        }
    }

    // Median of the per-round probes: the mean would follow whichever round
    // happened to collide with a background process.
    const baseProbe = calibrationMedian(baseline.calibration);
    const machineDelta =
        baseProbe === 0 ? 0 : (calibrationMedian(current.calibration) - baseProbe) / baseProbe;

    // If the machine itself moved materially, we cannot attribute anything
    // to the code. Downgrade every regression to inconclusive rather than
    // reporting two dozen false failures — a run that blames the developer
    // for their laptop being busy is a run nobody will trust twice.
    const untrustworthy = Math.abs(machineDelta) > MACHINE_DRIFT_LIMIT;
    if (untrustworthy) {
        for (const c of comparisons) {
            // Exact metrics are exempt: a busy machine changes how long a
            // dispatch takes, never how many directory calls it makes. They
            // are precisely what stays readable when nothing else is.
            if (c.exact) continue;
            if (c.verdict === 'regressed' || c.verdict === 'improved') c.verdict = 'inconclusive';
        }
    }

    const regressions = comparisons.filter((c) => c.verdict === 'regressed');
    return {
        fatalMismatch: envWarnings.filter((w) => w.startsWith(INFRA_SHAPE_MISMATCH)),
        comparisons,
        regressions,
        exactRegressions: regressions.filter((c) => c.exact),
        envWarnings,
        machineDelta
    };
}

export function printComparison(outcome: CompareOutcome, threshold: number): void {
    const { comparisons, regressions, exactRegressions, envWarnings, machineDelta } = outcome;

    // First, and separately from everything else: these are not timings and
    // carry no caveat. A machine cannot make a dispatch take more microtask
    // turns — only a code change can.
    if (exactRegressions.length > 0) {
        console.log('');
        console.log('EXACT METRICS MOVED — these are invariants, not measurements:');
        for (const c of exactRegressions) {
            console.log(
                `  ✗ ${c.scenario} ${c.metric}: ${fmt(c.baseline, c.unit)} → ` +
                    `${fmt(c.current, c.unit)}`
            );
        }
    }

    if (envWarnings.length > 0) {
        console.log('');
        console.log('⚠ BASELINE FROM A DIFFERENT ENVIRONMENT — numbers are not comparable:');
        for (const warning of envWarnings) console.log(`    ${warning}`);
    }

    if (Math.abs(machineDelta) > MACHINE_DRIFT_LIMIT) {
        console.log('');
        console.log(
            `⚠ THE MACHINE, NOT THE CODE: the actor-free CPU probe scored ` +
                `${(Math.abs(machineDelta) * 100).toFixed(0)}% ` +
                `${machineDelta < 0 ? 'LOWER' : 'higher'} than when the baseline was ` +
                `recorded. Expect roughly that much shift in every scenario below, in ` +
                `the same direction, for reasons unrelated to your change. Let the ` +
                `machine idle and re-record the baseline before reading anything into ` +
                `these results.`
        );
    }

    const interesting = comparisons.filter(
        (c) =>
            !c.informational &&
            // Exact moves were listed above, on their own terms. Repeating
            // them under a heading about thresholds files an invariant with
            // the measurements.
            !(c.exact && (c.verdict === 'regressed' || c.verdict === 'improved')) &&
            (c.verdict === 'regressed' ||
                c.verdict === 'improved' ||
                c.verdict === 'inconclusive')
    );

    console.log('');
    if (Math.abs(machineDelta) > MACHINE_DRIFT_LIMIT) {
        console.log('No verdict: the machine differed too much from the baseline to attribute anything.');
    }
    if (interesting.length === 0) {
        const compared = comparisons.filter((c) => c.verdict !== 'new').length;
        console.log(`✓ no change beyond ±${(threshold * 100).toFixed(0)}% (${compared} metrics)`);
    } else {
        console.log(`Changes beyond ±${(threshold * 100).toFixed(0)}%:`);
        for (const c of interesting) {
            const mark =
                c.verdict === 'regressed' ? '✗' : c.verdict === 'improved' ? '✓' : '?';
            const change =
                c.delta === null
                    ? `${c.absoluteDiff >= 0 ? '+' : ''}${fmt(c.absoluteDiff, c.unit)}`
                    : `${c.delta >= 0 ? '+' : ''}${(c.delta * 100).toFixed(1)}%`;
            const note = c.verdict === 'inconclusive' ? '  (within run-to-run noise)' : '';
            console.log(
                `  ${mark} ${c.scenario} ${c.metric}: ` +
                    `${fmt(c.baseline, c.unit)} → ${fmt(c.current, c.unit)} ` +
                    `(${change})${note}`
            );
        }
    }

    // Diagnostics never fail a run — but a GC-pause jump alongside a
    // throughput drop is usually the explanation, so it must not vanish.
    // GC counters only. p999/max are informational too, but they are single
    // extreme samples that routinely swing 10x on a busy machine — listing
    // them buries the one diagnostic that actually explains a throughput
    // move. They stay in the JSON for eyeballing.
    const diagnostics = comparisons.filter(
        (c) =>
            c.informational &&
            c.metric.startsWith('gc/') &&
            c.delta !== null &&
            Math.abs(c.delta) > DIAGNOSTIC_THRESHOLD
    );
    if (diagnostics.length > 0) {
        console.log('');
        console.log('Diagnostics (not regressions — single-sample, high variance):');
        for (const c of diagnostics) {
            const d = c.delta as number;
            console.log(
                `  · ${c.scenario} ${c.metric}: ${fmt(c.baseline, c.unit)} → ` +
                    `${fmt(c.current, c.unit)} (${d >= 0 ? '+' : ''}${(d * 100).toFixed(0)}%)`
            );
        }
    }

    const added = comparisons.filter((c) => c.verdict === 'new');
    if (added.length > 0) console.log(`  ${added.length} metric(s) not in the baseline`);
    if (regressions.length > 0) {
        console.log('');
        console.log(`${regressions.length} regression(s).`);
    }
}
