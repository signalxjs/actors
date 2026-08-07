// @vitest-environment node
/**
 * The interleaved A/B verdict (#98).
 *
 * Same rationale as markdown.test.ts: this report is read by a human who
 * never sees the numbers behind it, so everything asserted here is a way it
 * could mislead — a verdict called on unanimity the spread should have
 * refused, an exact metric quietly varying between rounds of the same code,
 * a row matched in four rounds of five wearing a five-round verdict.
 */
import { describe, expect, it } from 'vitest';
import {
    buildReport,
    failures,
    markdownReport,
    MIN_EFFECT_PCT,
    NOISY_SPREAD_PCT,
    type RoundRecord
} from '../src/ab-verdict.ts';
import type { BenchEnv } from '../src/env.ts';
import type { AggregatedMetric, BenchResult, ScenarioResult } from '../src/types.ts';

const ENV: BenchEnv = {
    node: 'v24.0.0',
    nodeMajor: 24,
    platform: 'linux',
    arch: 'x64',
    cpuModel: 'CI vCPU',
    cpuCount: 4,
    totalMemBytes: 16 * 1024 ** 3,
    conditions: 'production',
    commit: 'abc1234',
    dirty: false
};

function metric(name: string, value: number, extra: Partial<AggregatedMetric> = {}) {
    return {
        name,
        value,
        unit: 'ops/s',
        direction: 'higher',
        samples: [value],
        min: value,
        max: value,
        spread: 0,
        ...extra
    } satisfies AggregatedMetric;
}

function scenario(name: string, metrics: AggregatedMetric[]): ScenarioResult {
    return { name, description: `${name} description`, metrics };
}

function result(scenarios: ScenarioResult[], commit = 'abc1234'): BenchResult {
    return {
        formatVersion: 1,
        createdAt: '2026-08-01T00:00:00.000Z',
        env: { ...ENV, commit },
        runs: 1,
        durationMs: 400,
        quick: false,
        calibration: { samples: [1000] },
        scenarios
    };
}

/**
 * Rounds where one throughput metric takes the given per-round values.
 * Direction 'higher', so head > base reads as improvement.
 */
function rounds(basePerRound: number[], headPerRound: number[], extra: Partial<AggregatedMetric> = {}): RoundRecord[] {
    return basePerRound.map((baseValue, i) => ({
        index: i,
        order: i % 2 === 0 ? ['base', 'head'] : ['head', 'base'],
        base: result([scenario('s/x', [metric('m', baseValue, extra)])], 'baseaaa'),
        head: result([scenario('s/x', [metric('m', headPerRound[i]!, extra)])], 'headaaa')
    }));
}

function only(records: RoundRecord[]) {
    const report = buildReport(records);
    expect(report.rows).toHaveLength(1);
    return { report, row: report.rows[0]! };
}

describe('interleaved A/B verdicts', () => {
    it('calls a unanimous move that clears the floor and the spread', () => {
        // +8% every round on steady sides: sign test unanimous, median well
        // past MIN_EFFECT_PCT and past both spreads.
        const { report, row } = only(rounds([1000, 1005, 995, 1002, 998], [1080, 1085, 1075, 1082, 1078]));
        expect(row.verdict).toBe('improved');
        expect(failures(report, false)).toHaveLength(0);

        const down = only(rounds([1000, 1005, 995, 1002, 998], [920, 925, 915, 922, 918]));
        expect(down.row.verdict).toBe('regressed');
        // Timings never fail the PR path...
        expect(failures(down.report, false)).toHaveLength(0);
        // ...and DO fail the deliberate --enforce proof run.
        expect(failures(down.report, true)).toHaveLength(1);
    });

    it('refuses unanimity below the effect floor', () => {
        // +1.5% every round — unanimous, meaningless. The floor refuses it.
        const { row } = only(rounds([1000, 1000, 1000, 1000, 1000], [1015, 1015, 1015, 1015, 1015]));
        expect(Math.abs(row.medianDeltaPct)).toBeLessThan(MIN_EFFECT_PCT);
        expect(row.verdict).toBe('no change');
    });

    it('refuses a median its own spread covers', () => {
        // Median +6% but the base side wobbles 8% run to run: unanimity plus
        // a floor of max(3, spread) refuses the call.
        const { row } = only(rounds([1000, 1080, 1000, 1080, 1000], [1060, 1145, 1060, 1145, 1060]));
        expect(row.verdict).toBe('no change');
    });

    it('reads a side that cannot hold still as noisy, whatever the median says', () => {
        // Base swings 25% — beyond NOISY_SPREAD_PCT — so even a huge
        // unanimous delta claims nothing.
        const { row } = only(rounds([1000, 1250, 1000, 1250, 1000], [2000, 2500, 2000, 2500, 2000]));
        expect(row.baseSpreadPct).toBeGreaterThan(NOISY_SPREAD_PCT);
        expect(row.verdict).toBe('noisy');
    });

    it('does not call mixed signs', () => {
        // Deltas alternate +4% / −3% while both sides stay inside the noisy
        // limit — the sign test is what refuses this one.
        const { row } = only(rounds([1000, 1000, 1000, 1000, 1000], [1040, 970, 1040, 970, 1040]));
        expect(row.headSpreadPct).toBeLessThan(NOISY_SPREAD_PCT);
        expect(row.verdict).toBe('no change');
    });

    it('carries exact verdicts at zero tolerance, and only regressions fail', () => {
        const exact = { unit: 'count', direction: 'lower', exact: true } as const;
        const same = only(rounds([5, 5, 5], [5, 5, 5], exact));
        expect(same.row.verdict).toBe('unchanged');
        expect(failures(same.report, false)).toHaveLength(0);

        // direction 'lower': head 6 > base 5 is a regression — of ONE unit,
        // no threshold, no noise gate.
        const moved = only(rounds([5, 5, 5], [6, 6, 6], exact));
        expect(moved.row.verdict).toBe('regressed');
        expect(failures(moved.report, false)).toHaveLength(1);

        const better = only(rounds([5, 5, 5], [4, 4, 4], exact));
        expect(better.row.verdict).toBe('improved');
        expect(failures(better.report, false)).toHaveLength(0);
    });

    it('fails an exact metric that varies between rounds of the SAME code', () => {
        // The head answered 5, 5, 6 across three rounds of one commit: the
        // metric is not deterministic by construction, which breaks the
        // Metric.exact contract and outranks whatever delta it reported.
        const exact = { unit: 'count', direction: 'lower', exact: true } as const;
        const { report, row } = only(rounds([5, 5, 5], [5, 5, 6], exact));
        expect(row.verdict).toBe('nondeterministic');
        expect(failures(report, false)).toHaveLength(1);
    });

    it('reads informational rows as info and never fails them', () => {
        const { report, row } = only(
            rounds([1000, 1000, 1000], [500, 500, 500], { informational: true })
        );
        expect(row.verdict).toBe('info');
        expect(failures(report, true)).toHaveLength(0);
    });

    it('gives no verdict to a row that missed a round', () => {
        const records = rounds([1000, 1000, 1000], [1100, 1100, 1100]);
        // Round 2's head lost the metric (scenario crashed, filter drift…).
        records[1] = {
            ...records[1]!,
            head: result([scenario('s/x', [])], 'headaaa')
        };
        const report = buildReport(records);
        expect(report.rows).toHaveLength(0);
        expect(report.partial).toHaveLength(1);
        expect(report.partial[0]).toContain('matched in 2/3 rounds');
    });

    it('renders the failure bold in markdown, with the full table collapsed', () => {
        const exact = { unit: 'count', direction: 'lower', exact: true } as const;
        const records = rounds([5, 5, 5], [6, 6, 6], exact);
        // A second, boring metric so the collapsed section exists.
        for (const record of records) {
            (record.base as BenchResult).scenarios.push(
                scenario('s/quiet', [metric('steady', 1000)])
            );
            (record.head as BenchResult).scenarios.push(
                scenario('s/quiet', [metric('steady', 1001)])
            );
        }
        const report = buildReport(records);
        const markdown = markdownReport(report, false, 'a title');
        expect(markdown).toContain('a title');
        expect(markdown).toContain('**🔴 regressed**');
        expect(markdown).toContain('<details><summary>Every metric</summary>');
        expect(markdown).toContain('counterbalanced');
    });

    it('refuses payloads that are not bench results', () => {
        const report = buildReport([
            { index: 0, order: ['base', 'head'], base: { nope: true }, head: { nope: true } },
            { index: 1, order: ['head', 'base'], base: { nope: true }, head: { nope: true } }
        ]);
        expect(report.fatalMismatch.length).toBeGreaterThan(0);
    });
});
