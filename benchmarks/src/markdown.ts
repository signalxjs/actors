/**
 * The comparison, rendered as a report to post on a pull request.
 *
 * The same `CompareOutcome` `printComparison` puts on a terminal — no second,
 * laxer set of rules for CI. What differs is only what a PR comment can carry:
 * it is size-bounded (GitHub rejects a comment past 65 536 characters, and the
 * default suite produces ~360 metrics), and it leads with the verdict because
 * the reader is a reviewer, not the person who ran it.
 *
 * Deliberately generic about the two sides — "before" and "after", never
 * "baseline" and "current". The A/B job's two sides are a base ref and a head
 * ref, neither of which is the committed baseline.
 */
import { instability } from './calibrate.ts';
import {
    type Comparison,
    type CompareOutcome,
    DIAGNOSTIC_THRESHOLD,
    MACHINE_DRIFT_LIMIT,
    MACHINE_INSTABILITY_LIMIT,
    fmt
} from './report.ts';
import type { BenchResult } from './types.ts';

/**
 * A GitHub issue comment is rejected past 65 536 characters. The report is
 * built so the headline always fits and only the every-metric appendix is
 * trimmed — the JSON artifact carries every row regardless.
 */
const MAX_REPORT_CHARS = 60_000;

/** Longest `new`/`stale` list rendered inline before it is summarized. */
const MAX_NAME_LIST = 20;

/**
 * Longest "what moved" table before it, too, is capped.
 *
 * Normally a handful of rows — but on a runner that got busy mid-job, half
 * the suite lands in the `inconclusive` band at once, and the headline has to
 * stay bounded independently of the appendix that follows it.
 */
const MAX_CHANGE_ROWS = 60;

/** Regressions first: what a truncation may drop is the least actionable. */
const VERDICT_PRIORITY: Record<Comparison['verdict'], number> = {
    regressed: 0,
    improved: 1,
    inconclusive: 2,
    unchanged: 3,
    new: 4
};

/**
 * How big a move was, for ordering only — never shown.
 *
 * `delta` is null whenever a percentage would lie: a baseline at or below
 * zero, which is exactly the case for the heap slopes and retained-bytes
 * metrics that carry a `noiseFloor`. Those judge on the absolute difference
 * instead, and an absolute difference in bytes cannot be ranked against one
 * in ops/s — so rather than pretend such a row moved by 0% and let a leak get
 * sorted below rounding noise, a move we cannot quantify ranks FIRST. A row
 * that merely came back `unchanged` did not move at all.
 */
function magnitude(c: Comparison): number {
    if (c.verdict === 'new') return -1;
    if (c.delta !== null) return Math.abs(c.delta);
    return c.verdict === 'regressed' || c.verdict === 'improved' ? Infinity : 0;
}

const TABLE_HEAD = [
    '| scenario | metric | before | after | delta | |',
    '| --- | --- | ---: | ---: | ---: | --- |'
];

const MARK: Record<Comparison['verdict'], string> = {
    regressed: '🔴',
    improved: '🟢',
    inconclusive: '🟡',
    unchanged: '·',
    new: '🆕'
};

export interface MarkdownOptions {
    threshold: number;
    /** One line naming which ref is which; rendered under the heading. */
    title?: string;
}

/** `+12.3%`, or an absolute difference where a percentage is undefined. */
function changeOf(c: Comparison): string {
    // A metric the "before" side never measured has nothing to be a delta
    // FROM. `compare()` leaves the baseline at 0 for those, which would render
    // as a confident "+0.0" — the one reading a reviewer must not get.
    if (c.verdict === 'new') return '—';
    if (c.delta === null) {
        return `${c.absoluteDiff >= 0 ? '+' : ''}${fmt(c.absoluteDiff, c.unit)}`;
    }
    return `${c.delta >= 0 ? '+' : ''}${(c.delta * 100).toFixed(1)}%`;
}

function row(c: Comparison, note = ''): string {
    const before = c.verdict === 'new' ? '—' : fmt(c.baseline, c.unit);
    return (
        `| \`${c.scenario}\` | \`${c.metric}\` | ${before} | ` +
        `${fmt(c.current, c.unit)} | ${changeOf(c)} | ${MARK[c.verdict]} ${c.verdict}${note} |`
    );
}

/** `a, b, c, … and 40 more` — an unbounded list would blow the size budget. */
function nameList(names: readonly string[]): string {
    const shown = names.slice(0, MAX_NAME_LIST).map((n) => `\`${n}\``);
    const rest = names.length - shown.length;
    return shown.join(', ') + (rest > 0 ? `, … and ${rest} more` : '');
}

/** How a run was taken. The machine line is shared — both halves ran on it. */
function stamp(result: BenchResult): string {
    return (
        `\`${result.env.commit}\`${result.env.dirty ? ' (dirty)' : ''} · ` +
        `${result.runs} runs × ${result.durationMs}ms${result.quick ? ' · quick' : ''}`
    );
}

/**
 * Metric ids present in `before` that `after` no longer measures — a rename
 * or a removal, which is worth naming because such a metric silently stops
 * being compared.
 *
 * A scenario that CRASHED on the "after" run reports no metrics either, but
 * that is not the same thing at all and must not be listed here: it is a
 * failure, it has its own section, and calling it "renamed or removed" sends
 * the reader looking for a diff that does not exist.
 */
export function staleMetrics(before: BenchResult, after: BenchResult): string[] {
    const measured = new Set<string>();
    const crashed = new Set<string>();
    for (const scenario of after.scenarios) {
        if (scenario.error) {
            crashed.add(scenario.name);
            continue;
        }
        for (const metric of scenario.metrics) measured.add(`${scenario.name} ${metric.name}`);
    }
    const stale: string[] = [];
    for (const scenario of before.scenarios) {
        if (scenario.error || crashed.has(scenario.name)) continue;
        for (const metric of scenario.metrics) {
            const id = `${scenario.name} ${metric.name}`;
            if (!measured.has(id)) stale.push(id);
        }
    }
    return stale;
}

/**
 * The every-metric table, trimmed to whatever is left of the size budget.
 *
 * Sorted by the size of the move, so the rows a truncation drops are the ones
 * nobody was going to read — scenario order would instead lop off `cluster/*`
 * for being last in the alphabet.
 */
function appendix(
    comparisons: readonly Comparison[],
    head: readonly string[],
    footer: readonly string[]
): string[] {
    const sorted = [...comparisons].sort((a, b) => magnitude(b) - magnitude(a));

    const open = [
        '',
        '<details><summary>Every metric, biggest mover first</summary>',
        '',
        ...TABLE_HEAD
    ];
    const close = ['', '</details>'];
    /** Room a truncation notice needs, reserved before any row is admitted. */
    const NOTICE_BUDGET = 200;
    const charge = (lines: readonly string[]): number =>
        lines.reduce((n, line) => n + line.length + 1, 0);

    let spent = charge(head) + charge(open) + charge(close) + charge(footer) + NOTICE_BUDGET;
    const rows: string[] = [];
    for (const c of sorted) {
        const rendered = row(c);
        if (spent + rendered.length + 1 > MAX_REPORT_CHARS) break;
        spent += rendered.length + 1;
        rows.push(rendered);
    }

    const dropped = sorted.length - rows.length;
    if (dropped > 0) {
        rows.push(
            '',
            `_… and ${dropped} more metric(s), all smaller movers — the full ` +
                `comparison is in the run's JSON artifact._`
        );
    }
    return [...open, ...rows, ...close];
}

export function markdownComparison(
    outcome: CompareOutcome,
    before: BenchResult,
    after: BenchResult,
    options: MarkdownOptions
): string {
    const { comparisons, envWarnings, machineDelta } = outcome;
    const thresholdPct = (options.threshold * 100).toFixed(0);
    const lines: string[] = ['### Benchmark A/B'];
    if (options.title) lines.push('', options.title);

    if (Math.abs(machineDelta) > MACHINE_DRIFT_LIMIT) {
        lines.push(
            '',
            '> [!WARNING]',
            `> **The machine, not the code.** The actor-free CPU probe scored ` +
                `${(Math.abs(machineDelta) * 100).toFixed(0)}% ` +
                `${machineDelta < 0 ? 'lower' : 'higher'} on the "after" run than on the ` +
                `"before" one, so the runner itself moved between the two halves. Expect ` +
                `roughly that much shift in every row below, in the same direction, for ` +
                `reasons unrelated to this change — so every \`regressed\` and ` +
                `\`improved\` verdict **on a timing** has been downgraded to ` +
                `\`inconclusive\` rather than blaming the change for a busy runner. ` +
                `Re-running the job draws a fresh runner; a quiet one produces a verdict. ` +
                `**Exact metrics are exempt and still stand**: a busy machine changes how ` +
                `long a dispatch takes, never how many directory calls it makes.`
        );
    }
    const jitter = Math.max(instability(before.calibration), instability(after.calibration));
    if (jitter > MACHINE_INSTABILITY_LIMIT) {
        lines.push(
            '',
            '> [!NOTE]',
            `> The probe varied ${(jitter * 100).toFixed(0)}% across rounds *within* a run — ` +
                `something else was competing for the CPU. Treat small differences as ` +
                `meaningless.`
        );
    }
    if (envWarnings.length > 0) {
        lines.push('', '**The two runs came from different environments:**');
        for (const warning of envWarnings) lines.push(`- ${warning}`);
    }

    // Above everything else, and without a caveat attached: these are the
    // rows a shared runner can prove, and the only ones that fail the check.
    const exactMoves = comparisons.filter(
        (c) => c.exact && (c.verdict === 'regressed' || c.verdict === 'improved')
    );
    if (exactMoves.length > 0) {
        const regressed = exactMoves.filter((c) => c.verdict === 'regressed');
        lines.push(
            '',
            regressed.length > 0 ? '> [!CAUTION]' : '> [!IMPORTANT]',
            `> **An exact metric moved.** These are algorithmic invariants — directory ` +
                `calls per activation, microtask turns per dispatch — not measurements. ` +
                `The same code produces the same value on any machine under any load, so ` +
                `unlike every other number here this one is not noise and carries no ` +
                `threshold.` +
                (regressed.length > 0
                    ? ` **This fails the Bench check.** If the change is intended, the ` +
                      `expected value moved with it and that is worth saying in the PR.`
                    : ' Moving in the good direction, so nothing fails.')
        );
        lines.push('', ...TABLE_HEAD);
        for (const c of exactMoves) lines.push(row(c));
    }

    const compared = comparisons.filter((c) => c.verdict !== 'new');
    const interesting = comparisons.filter(
        (c) =>
            !c.informational &&
            // Exact moves have their own section above; repeating them here,
            // among rows carrying a "within noise" caveat, would file them
            // under the same doubt.
            !exactMoves.includes(c) &&
            (c.verdict === 'regressed' || c.verdict === 'improved' || c.verdict === 'inconclusive')
    );

    lines.push('');
    if (interesting.length === 0) {
        lines.push(`✅ No change beyond ±${thresholdPct}% (${compared.length} metrics compared).`);
    } else {
        // Severity first, then size of the move: a reviewer reads the top of
        // this table and stops, so the order has to put the actionable rows
        // there — and a truncation must not drop a regression to keep a
        // borderline `inconclusive`.
        const ranked = [...interesting].sort(
            (a, b) =>
                VERDICT_PRIORITY[a.verdict] - VERDICT_PRIORITY[b.verdict] ||
                magnitude(b) - magnitude(a)
        );
        lines.push(
            `**Changes beyond ±${thresholdPct}%** (of ${compared.length} metrics compared):`,
            '',
            ...TABLE_HEAD
        );
        for (const c of ranked.slice(0, MAX_CHANGE_ROWS)) {
            lines.push(row(c, c.verdict === 'inconclusive' ? ' — within run-to-run noise' : ''));
        }
        if (ranked.length > MAX_CHANGE_ROWS) {
            lines.push(
                '',
                `_… and ${ranked.length - MAX_CHANGE_ROWS} more row(s) past the ` +
                    `threshold, smaller moves than those above. When this many metrics ` +
                    `move at once it is usually the runner, not the change._`
            );
        }
    }

    // GC counters only, and only on a big move. A GC-pause jump next to a
    // throughput drop is usually the explanation, so it must not vanish — but
    // p999/max are single extreme samples that swing 10x on a shared runner
    // and listing those would bury it. They stay in the JSON.
    const diagnostics = comparisons.filter(
        (c) =>
            c.informational &&
            c.metric.startsWith('gc/') &&
            c.delta !== null &&
            Math.abs(c.delta) > DIAGNOSTIC_THRESHOLD
    );
    if (diagnostics.length > 0) {
        lines.push(
            '',
            '**Diagnostics** — never regressions, but often the explanation:',
            '',
            ...TABLE_HEAD
        );
        for (const c of diagnostics) lines.push(row(c));
    }

    const added = comparisons.filter((c) => c.verdict === 'new');
    if (added.length > 0) {
        lines.push(
            '',
            `**New** — measured on the "after" side only, so nothing compares them ` +
                `(${added.length}): ` +
                nameList(added.map((c) => `${c.scenario} ${c.metric}`))
        );
    }
    const stale = staleMetrics(before, after);
    if (stale.length > 0) {
        lines.push(
            '',
            `**Stale** — on the "before" side but not measured now, so renamed or ` +
                `removed (${stale.length}): ${nameList(stale)}`
        );
    }

    // A scenario that threw reported no metrics at all, so it cannot show up
    // as a regression — it would silently vanish from the table instead.
    const failed = after.scenarios.filter((s) => s.error);
    if (failed.length > 0) {
        lines.push('', '**Scenarios that failed to run:**');
        for (const s of failed) lines.push(`- \`${s.name}\`: ${s.error?.split('\n')[0]}`);
    }

    const footer = [
        '',
        `<sub>before: ${stamp(before)}</sub><br>`,
        `<sub>after: ${stamp(after)}</sub><br>`,
        `<sub>${after.env.node} ${after.env.platform}/${after.env.arch} · ` +
            `${after.env.cpuModel} (${after.env.cpuCount} vCPU) · conditions ` +
            `${after.env.conditions}</sub><br>`,
        `<sub><b>Timings are informational</b> — a shared CI runner cannot gate on one, ` +
            `and a delta smaller than the metric's own run-to-run spread reads ` +
            `\`inconclusive\` rather than \`regressed\`. Reproduce anything interesting ` +
            `locally with \`pnpm bench:baseline\` / \`pnpm bench:compare\` on a quiet ` +
            `machine. <b>Exact metrics do gate</b>: they are invariants rather than ` +
            `measurements, so a shared runner judges them exactly as well as a quiet one.` +
            `</sub>`
    ];

    return `${[...lines, ...appendix(comparisons, lines, footer), ...footer].join('\n')}\n`;
}
