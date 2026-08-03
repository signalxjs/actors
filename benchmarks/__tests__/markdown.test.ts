// @vitest-environment node
/**
 * The A/B report a PR comment carries.
 *
 * Worth testing even though the benchmarks themselves are not: this is the
 * only part of the suite whose output a HUMAN reads and acts on without ever
 * seeing the numbers behind it, and it runs unattended in CI where nobody
 * notices a wrong cell. Everything asserted here is a way the report could
 * mislead — a fabricated delta, a silent truncation, a scenario that
 * disappeared because it crashed.
 */
import { readdirSync, readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { markdownComparison } from '../src/markdown.ts';
import { compare } from '../src/report.ts';
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

function result(scenarios: ScenarioResult[], probe = 1000, commit = 'abc1234'): BenchResult {
    return {
        formatVersion: 1,
        createdAt: '2026-08-01T00:00:00.000Z',
        env: { ...ENV, commit },
        runs: 3,
        durationMs: 400,
        quick: false,
        // Identical probes on both sides: the machine held still, so verdicts
        // are attributable to the code. A drifting probe is its own test.
        calibration: { samples: [probe, probe, probe] },
        scenarios
    };
}

function scenario(name: string, metrics: AggregatedMetric[]): ScenarioResult {
    return { name, description: `${name} description`, metrics };
}

/**
 * The merge-queue gate list, and the scenarios it is supposed to describe,
 * read out of the workflow and the scenario sources.
 */
function gateSets(): {
    declared: string[];
    scenarioNames: string[];
    gated: Map<string, string>;
} {
    const workflow = readFileSync(
        new URL('../../.github/workflows/bench.yml', import.meta.url),
        'utf8'
    );
    const declared = (/^\s*BENCH_GATE_SCENARIOS:\s*'(.+)'\s*$/m.exec(workflow)?.[1] ?? '')
        .split(/\s+/)
        .filter(Boolean);

    const dir = new URL('../src/scenarios/', import.meta.url);
    const scenarioNames: string[] = [];
    /** Scenario id → the file that declares it, for every gated scenario. */
    const gated = new Map<string, string>();
    for (const file of readdirSync(dir).filter((f) => f.endsWith('.ts') && f !== 'index.ts')) {
        const src = readFileSync(new URL(file, dir), 'utf8');
        // A scenario's id line starts its block; the next one ends it. Crude,
        // but it attributes each `exact` flag to the scenario that emits it —
        // which "does this FILE contain a flag?" cannot do, and `cluster.ts`
        // holds eight scenarios of which only some gate.
        const marks = [...src.matchAll(/^\s*name:\s*'([\w-]+\/[\w-]+)',$/gm)];
        for (const [i, mark] of marks.entries()) {
            const name = mark[1] as string;
            scenarioNames.push(name);
            const block = src.slice(mark.index, marks[i + 1]?.index ?? src.length);
            // The literal flag, or the helper that applies it.
            if (/exact:\s*true|exactIfDeterministic\s*\(/.test(block)) gated.set(name, file);
        }
    }
    return { declared, scenarioNames, gated };
}

/** Render an A/B the way the workflow does, and hand back the markdown. */
function report(before: BenchResult, after: BenchResult, threshold = 0.1): string {
    return markdownComparison(compare(after, before, threshold), before, after, {
        threshold,
        title: 'base `abc1234` (before) → head `def5678` (after).'
    });
}

describe('markdownComparison', () => {
    it('leads with the verdict and names both refs', () => {
        const before = result([scenario('dispatch/warm-actor', [metric('c=1/ops_per_sec', 100)])]);
        const after = result(
            [scenario('dispatch/warm-actor', [metric('c=1/ops_per_sec', 100)])],
            1000,
            'def5678'
        );

        const md = report(before, after);
        expect(md).toContain('### Benchmark A/B');
        expect(md).toContain('base `abc1234` (before) → head `def5678` (after).');
        expect(md).toContain('✅ No change beyond ±10% (1 metrics compared).');
        expect(md).toContain('<sub>before: `abc1234`');
        expect(md).toContain('<sub>after: `def5678`');
    });

    it('reports a drop in a higher-is-better metric as a regression', () => {
        const before = result([scenario('dispatch/warm-actor', [metric('c=1/ops_per_sec', 100)])]);
        const after = result([scenario('dispatch/warm-actor', [metric('c=1/ops_per_sec', 70)])]);

        const md = report(before, after);
        expect(md).toContain('**Changes beyond ±10%**');
        expect(md).toMatch(/\| `dispatch\/warm-actor` \| `c=1\/ops_per_sec` \|.*-30\.0% \| 🔴 regressed \|/);
    });

    it('calls a delta inside the run-to-run spread inconclusive, not a regression', () => {
        // A metric whose own scatter is wider than the move it made: the honest
        // answer is "cannot tell", and a CI comment claiming otherwise is how
        // a perf report gets ignored.
        const noisy = { spread: 0.5 };
        const before = result([
            scenario('wire/endpoint-roundtrip', [metric('c=1/ops_per_sec', 100, noisy)])
        ]);
        const after = result([
            scenario('wire/endpoint-roundtrip', [metric('c=1/ops_per_sec', 80, noisy)])
        ]);

        const md = report(before, after);
        expect(md).toContain('🟡 inconclusive — within run-to-run noise');
        expect(md).not.toContain('🔴 regressed');
    });

    it('warns that the machine moved, and downgrades every verdict, when the probe drifted', () => {
        const before = result([scenario('dispatch/warm-actor', [metric('c=1/ops_per_sec', 100)])]);
        // Probe 30% slower on the "after" side — far past MACHINE_DRIFT_LIMIT,
        // so nothing below it can be attributed to the code.
        const after = result(
            [scenario('dispatch/warm-actor', [metric('c=1/ops_per_sec', 70)])],
            700
        );

        const md = report(before, after);
        expect(md).toContain('> [!WARNING]');
        expect(md).toContain('**The machine, not the code.**');
        expect(md).toContain('🟡 inconclusive');
        expect(md).not.toContain('🔴 regressed');
        // The banner must describe what `compare()` actually did: it downgrades
        // `regressed`/`improved` TIMINGS, leaves `unchanged`/`new` alone, and
        // exempts exact metrics entirely. Overstating any of that would have a
        // reader mistrust rows that were never in question — or, worse, wave
        // away an invariant that really did move.
        expect(md).toContain('**on a timing** has been downgraded');
        expect(md).toContain('**Exact metrics are exempt and still stand**');
    });

    it('renders a metric the "before" side never measured as new, with no delta', () => {
        // The regression this pins: `compare()` leaves the baseline at 0 for a
        // new metric, which rendered as a confident "0.0 → 120.0 (+0.0)".
        const before = result([scenario('dispatch/warm-actor', [metric('c=1/ops_per_sec', 100)])]);
        const after = result([
            scenario('dispatch/warm-actor', [
                metric('c=1/ops_per_sec', 100),
                metric('c=64/ops_per_sec', 120)
            ])
        ]);

        const md = report(before, after);
        expect(md).toContain('**New** — measured on the "after" side only');
        expect(md).toContain('`dispatch/warm-actor c=64/ops_per_sec`');
        expect(md).toMatch(/\| `c=64\/ops_per_sec` \| — \| 120\.0 \| — \| 🆕 new \|/);
        expect(md).not.toMatch(/`c=64\/ops_per_sec` \| 0\.0/);
    });

    it('lists a metric that stopped being measured as stale', () => {
        const before = result([
            scenario('dispatch/warm-actor', [
                metric('c=1/ops_per_sec', 100),
                metric('c=512/ops_per_sec', 90)
            ])
        ]);
        const after = result([scenario('dispatch/warm-actor', [metric('c=1/ops_per_sec', 100)])]);

        const md = report(before, after);
        expect(md).toContain('**Stale**');
        expect(md).toContain('`dispatch/warm-actor c=512/ops_per_sec`');
    });

    it('does not call a crashed scenario\'s metrics "renamed or removed"', () => {
        // A scenario that threw reports no metrics, which looks identical to a
        // rename from the outside. Listing it under **Stale** sends the reader
        // hunting for a diff that does not exist — it failed, and it already
        // has its own section.
        const before = result([scenario('mem/leak-streams', [metric('retained_per_stream', 3)])]);
        const after = result([
            { name: 'mem/leak-streams', description: 'x', metrics: [], error: 'boom' }
        ]);

        const md = report(before, after);
        expect(md).toContain('**Scenarios that failed to run:**');
        expect(md).not.toContain('**Stale**');
    });

    it('ranks a move it cannot express as a percentage first, not last', () => {
        // Heap slopes and retained bytes sit at or below zero, where a
        // percentage inverts — so `compare()` judges them on the absolute
        // difference and leaves `delta` null. Treating that as a 0% move sorts
        // a real leak below rounding noise and lets truncation drop it.
        const leak = { unit: 'bytes', direction: 'lower', noiseFloor: 1 } as const;
        const before = result([
            scenario('mem/leak-activate-deactivate', [metric('retained_per_actor', 0, leak)]),
            scenario('dispatch/warm-actor', [metric('c=1/ops_per_sec', 100)])
        ]);
        const after = result([
            scenario('mem/leak-activate-deactivate', [metric('retained_per_actor', 4096, leak)]),
            scenario('dispatch/warm-actor', [metric('c=1/ops_per_sec', 70)])
        ]);

        const md = report(before, after);
        const headline = md.split('<details>')[0] as string;
        const rows = headline.split('\n').filter((l) => l.startsWith('| `'));
        // Both regressed, so verdict priority ties and magnitude decides: the
        // unquantifiable 4 KiB leak must not sort below a -30% throughput move.
        expect(rows[0]).toContain('retained_per_actor');
        expect(rows[0]).toContain('🔴 regressed');
        // And with no percentage to show, it reports the absolute difference.
        expect(rows[0]).toContain('+4.0 KiB');
    });

    it('names a scenario that crashed instead of letting it vanish from the table', () => {
        const before = result([scenario('mem/leak-streams', [metric('retained_per_stream', 3)])]);
        const after = result([
            { name: 'mem/leak-streams', description: 'x', metrics: [], error: 'boom\nat foo' }
        ]);

        const md = report(before, after);
        expect(md).toContain('**Scenarios that failed to run:**');
        expect(md).toContain('`mem/leak-streams`: boom');
    });

    it('surfaces a big GC move as a diagnostic, never as a regression', () => {
        const gc = { unit: 'ms', direction: 'lower', informational: true } as const;
        const before = result([
            scenario('dispatch/warm-actor', [
                metric('c=1/ops_per_sec', 100),
                metric('gc/pause_ms', 10, gc)
            ])
        ]);
        const after = result([
            scenario('dispatch/warm-actor', [
                metric('c=1/ops_per_sec', 100),
                metric('gc/pause_ms', 20, gc)
            ])
        ]);

        const md = report(before, after);
        expect(md).toContain('**Diagnostics**');
        expect(md).toContain('`gc/pause_ms`');
        expect(md).not.toContain('🔴 regressed');
    });

    it('stays inside the comment size limit and says what it dropped', () => {
        // GitHub rejects an issue comment past 65 536 characters. The default
        // suite already emits ~360 metrics; this is deliberately far past that.
        const many = (offset: number) =>
            Array.from({ length: 60 }, (_, i) =>
                scenario(
                    `group-${i}/scenario-with-a-realistically-long-name`,
                    Array.from({ length: 40 }, (_, j) =>
                        metric(`concurrency=${j}/ops_per_sec`, 100 + offset + j)
                    )
                )
            );

        const md = report(result(many(0)), result(many(1)));
        expect(md.length).toBeLessThan(65_536);
        expect(md).toMatch(/_… and \d+ more metric\(s\), all smaller movers/);
        // The headline must survive the trim — only the appendix is bounded.
        expect(md).toContain('### Benchmark A/B');
        expect(md).toContain('<sub><b>Timings are informational</b>');
    });

    describe('exact metrics', () => {
        const exact = { unit: 'count', direction: 'lower', exact: true } as const;

        it('fails on a one-unit move that no threshold would catch', () => {
            // directory_ops 2 → 3 is +50%, but the point is that it would fail
            // at ANY threshold: it is an invariant, so one unit is the whole
            // signal. This is the regression a shared runner can prove and a
            // timing table never could.
            const before = result([
                scenario('cluster/directory-ops-per-activation', [metric('n=100/directory_ops', 2, exact)])
            ]);
            const after = result([
                scenario('cluster/directory-ops-per-activation', [metric('n=100/directory_ops', 3, exact)])
            ]);

            const outcome = compare(after, before, 0.1);
            expect(outcome.exactRegressions).toHaveLength(1);
            expect(outcome.exactRegressions[0]?.metric).toBe('n=100/directory_ops');

            const md = markdownComparison(outcome, before, after, { threshold: 0.1 });
            expect(md).toContain('> [!CAUTION]');
            expect(md).toContain('**An exact metric moved.**');
            expect(md).toContain('**This fails the Bench check.**');
        });

        it('is not silenced by a threshold that would swallow the same move', () => {
            // 10 → 11 microtask turns is +10%, under the 25% CI threshold. A
            // thresholded comparison calls that "unchanged"; an invariant has
            // no threshold to hide behind.
            const turns = { unit: 'turns', direction: 'lower', exact: true } as const;
            const before = result([
                scenario('dispatch/warm-turns', [metric('microtask_turns', 10, turns)])
            ]);
            const after = result([
                scenario('dispatch/warm-turns', [metric('microtask_turns', 11, turns)])
            ]);

            expect(compare(after, before, 0.25).exactRegressions).toHaveLength(1);
        });

        it('survives a machine-drift downgrade that voids every timing verdict', () => {
            // The drift rule exists because a busy machine changes timings. It
            // cannot change how many directory calls an activation makes, so
            // exempting exact metrics is what keeps the gate readable exactly
            // when nothing else is.
            const before = result([
                scenario('cluster/directory-ops-per-activation', [
                    metric('n=1/directory_ops', 2, exact),
                    metric('n=1/ops_per_sec', 100)
                ])
            ]);
            const after = result(
                [
                    scenario('cluster/directory-ops-per-activation', [
                        metric('n=1/directory_ops', 3, exact),
                        metric('n=1/ops_per_sec', 60)
                    ])
                ],
                700 // probe 30% down — far past the drift limit
            );

            const outcome = compare(after, before, 0.1);
            expect(outcome.exactRegressions).toHaveLength(1);
            // ...while the timing beside it was correctly voided.
            const timing = outcome.comparisons.find((c) => c.metric === 'n=1/ops_per_sec');
            expect(timing?.verdict).toBe('inconclusive');

            // And the banner must not claim to have downgraded the invariant
            // it just let through — a reader who trusts "everything was
            // downgraded" would wave away the one row that still counts.
            const md = markdownComparison(outcome, before, after, { threshold: 0.1 });
            expect(md).toContain('> [!WARNING]');
            expect(md).toContain('**Exact metrics are exempt and still stand**');
            expect(md).toContain('**This fails the Bench check.**');
        });

        it('does not fail when an invariant moves the GOOD way', () => {
            const before = result([
                scenario('cluster/membership-fanout', [metric('n=10/notifications', 20, exact)])
            ]);
            const after = result([
                scenario('cluster/membership-fanout', [metric('n=10/notifications', 10, exact)])
            ]);

            const outcome = compare(after, before, 0.1);
            expect(outcome.exactRegressions).toHaveLength(0);
            const md = markdownComparison(outcome, before, after, { threshold: 0.1 });
            expect(md).toContain('> [!IMPORTANT]');
            expect(md).toContain('nothing fails');
            expect(md).not.toContain('**This fails the Bench check.**');
        });

        it('reports an exact move once, not also among the noisy rows', () => {
            const before = result([
                scenario('cluster/directory-ops-per-activation', [metric('n=1/directory_ops', 2, exact)])
            ]);
            const after = result([
                scenario('cluster/directory-ops-per-activation', [metric('n=1/directory_ops', 3, exact)])
            ]);

            const md = markdownComparison(compare(after, before, 0.1), before, after, {
                threshold: 0.1
            });
            const rows = md
                .split('<details>')[0]
                ?.split('\n')
                .filter((l) => l.includes('`n=1/directory_ops`'));
            expect(rows).toHaveLength(1);
        });

        it('names only real scenarios in the merge-queue gate list', () => {
            // The queue run passes BENCH_GATE_SCENARIOS as a scenario filter,
            // because `merge_group` supports no `paths` filter and re-measuring
            // the whole suite on every merge would tax it to re-check timings
            // that cannot fail anything.
            //
            // A filter is a SUBSTRING match, so a renamed or misspelt scenario
            // does not error — it matches nothing, and the queue silently gates
            // on a smaller set than anyone believes. That is the failure worth
            // a test: it is invisible in a green run.
            const { declared, scenarioNames } = gateSets();
            expect(declared.length).toBeGreaterThan(0);
            for (const name of declared) expect(scenarioNames).toContain(name);
        });

        it('covers every scenario that flags an exact metric', () => {
            // The other direction: flag metrics in a scenario the list omits
            // and they stop gating on the queue, again silently. Per SCENARIO,
            // not per file — `cluster.ts` declares eight and only some gate, so
            // a file-level check would pass while a newly flagged scenario sat
            // outside the queue's filter.
            const { declared, gated } = gateSets();
            expect(gated.size).toBeGreaterThan(0);
            for (const [name, file] of gated) {
                expect(
                    declared,
                    `${name} (${file}) emits an exact metric but is not in ` +
                        `BENCH_GATE_SCENARIOS, so it does not gate on the merge queue`
                ).toContain(name);
            }
        });

        it('leaves an unchanged invariant alone', () => {
            const before = result([
                scenario('dispatch/warm-turns', [metric('microtask_turns', 10, { unit: 'turns', direction: 'lower', exact: true })])
            ]);
            const after = result([
                scenario('dispatch/warm-turns', [metric('microtask_turns', 10, { unit: 'turns', direction: 'lower', exact: true })])
            ]);

            const outcome = compare(after, before, 0.1);
            expect(outcome.exactRegressions).toHaveLength(0);
            const md = markdownComparison(outcome, before, after, { threshold: 0.1 });
            expect(md).not.toContain('An exact metric moved');
            expect(md).toContain('✅ No change beyond ±10%');
        });
    });

    it('caps the "what moved" table and keeps regressions over borderline noise', () => {
        // A runner that got busy mid-job puts half the suite in the
        // `inconclusive` band at once. The headline must stay bounded, and the
        // one row worth acting on must not be the row that gets dropped.
        const noisy = { spread: 0.9 };
        const before = result([
            scenario('dispatch/warm-actor', [metric('c=1/ops_per_sec', 100)]),
            scenario(
                'noise/everything',
                Array.from({ length: 200 }, (_, i) => metric(`m${i}/ops_per_sec`, 100, noisy))
            )
        ]);
        const after = result([
            scenario('dispatch/warm-actor', [metric('c=1/ops_per_sec', 55)]),
            scenario(
                'noise/everything',
                Array.from({ length: 200 }, (_, i) => metric(`m${i}/ops_per_sec`, 80, noisy))
            )
        ]);

        const md = report(before, after);
        // Everything before `<details>` is the headline; the appendix that
        // follows is bounded separately.
        const changeRows = (md.split('<details>')[0] as string)
            .split('\n')
            .filter((l) => l.startsWith('| `'));
        expect(changeRows.length).toBeLessThanOrEqual(60);
        expect(md).toMatch(/_… and \d+ more row\(s\) past the threshold/);
        // The only real regression outranks 200 inconclusive rows.
        expect(md).toContain('🔴 regressed');
        expect(md).toMatch(/`dispatch\/warm-actor` \| `c=1\/ops_per_sec` \|.*🔴 regressed/);
    });

    it('keeps the biggest movers when it truncates', () => {
        const filler = Array.from({ length: 40 }, (_, i) =>
            scenario(
                `group-${i}/scenario-with-a-realistically-long-name`,
                Array.from({ length: 40 }, (_, j) => metric(`concurrency=${j}/ops_per_sec`, 100))
            )
        );
        const before = result([
            scenario('dispatch/warm-actor', [metric('c=1/ops_per_sec', 100)]),
            ...filler
        ]);
        const after = result([
            scenario('dispatch/warm-actor', [metric('c=1/ops_per_sec', 40)]),
            ...filler
        ]);

        const md = report(before, after);
        expect(md).toMatch(/_… and \d+ more metric\(s\)/);
        // -60% is the largest move in the set, so it sorts to the top of the
        // appendix and cannot be the row that gets dropped.
        expect(md).toContain('-60.0%');
    });
});
