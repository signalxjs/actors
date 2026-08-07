/**
 * CLI over `ab-verdict.ts`: read the rounds file `ab.ts` wrote, print the
 * verdict table, optionally write the markdown report, and exit with the
 * gate's answer.
 *
 * Exit code, mirroring `compare-files.ts`'s posture exactly:
 *
 * - **0** for any timing verdict, however bad it looks — the PR path is
 *   advisory on timings and must never block a merge on one.
 * - **1** when an `exact` metric regressed or came back nondeterministic:
 *   invariants, so a change there is behavioural on any machine.
 * - **1** under `--enforce` when a timing row read `regressed` with every
 *   round agreeing — the deliberate dispatch proof run, never the PR path.
 * - **1** when it cannot answer honestly (unreadable file, refused
 *   comparison, fewer than 2 rounds).
 *
 * Usage:
 *   node --conditions=production benchmarks/src/ab-report.ts \
 *     --rounds-file=<path> [--markdown=<path>] [--title=<text>] [--enforce]
 */
import fs from 'node:fs';
import { buildReport, failures, markdownReport, type RoundRecord } from './ab-verdict.ts';

function argValue(name: string): string | undefined {
    const prefix = `--${name}=`;
    return process.argv.find((a) => a.startsWith(prefix))?.slice(prefix.length);
}

function fail(message: string): never {
    console.error(`[ab-report] ${message}`);
    process.exit(1);
}

function pct(value: number): string {
    if (!Number.isFinite(value)) return 'n/a';
    return `${value >= 0 ? '+' : ''}${value.toFixed(1)}%`;
}

function main(): void {
    const roundsFile = argValue('rounds-file');
    if (!roundsFile) fail('missing --rounds-file=<path>');
    if (!fs.existsSync(roundsFile)) fail(`rounds file not found: ${roundsFile}`);

    const parsed = JSON.parse(fs.readFileSync(roundsFile, 'utf8')) as {
        rounds?: RoundRecord[];
    };
    const rounds = parsed.rounds ?? [];
    if (rounds.length < 2) fail(`need at least 2 rounds to compare, got ${rounds.length}`);

    const report = buildReport(rounds);
    if (report.fatalMismatch.length > 0) {
        console.error('refusing to compare:');
        for (const m of report.fatalMismatch) console.error(`  ${m}`);
        process.exit(1);
    }

    const enforce = process.argv.includes('--enforce');

    console.table(
        report.rows.map((r) => ({
            scenario: r.scenario,
            metric: r.metric,
            before: r.baseMedian,
            after: r.headMedian,
            'delta (median)': pct(r.medianDeltaPct),
            'round range': r.exact ? '—' : `${pct(r.minDeltaPct)} … ${pct(r.maxDeltaPct)}`,
            spread: r.exact ? '—' : `${pct(r.baseSpreadPct)} / ${pct(r.headSpreadPct)}`,
            verdict: r.verdict
        }))
    );
    if (report.partial.length > 0) {
        console.log(`\nno verdict — matched in some rounds only (${report.partial.length}):`);
        for (const name of report.partial) console.log(`  - ${name}`);
    }

    const markdownPath = argValue('markdown');
    if (markdownPath) {
        fs.writeFileSync(markdownPath, markdownReport(report, enforce, argValue('title')));
        console.log(`\nreport → ${markdownPath}`);
    }

    // Written LAST, so the report exists on disk before this trips: the
    // caller has to be able to post the explanation of a failure it caused.
    const failed = failures(report, enforce);
    if (failed.length > 0) {
        console.error(`\n${failed.length} row(s) fail the gate:`);
        for (const row of failed) {
            console.error(
                `  - ${row.scenario} ${row.metric}: ${row.verdict} (${pct(row.medianDeltaPct)})`
            );
        }
        process.exit(1);
    }
}

main();
