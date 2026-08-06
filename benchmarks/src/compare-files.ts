/**
 * Diff two saved benchmark results — the comparer the Bench workflow's A/B job
 * runs, and the way to diff any two result files by hand.
 *
 *   node --conditions=production benchmarks/src/compare-files.ts \
 *     --before=base.json --after=head.json \
 *     [--threshold=10] [--markdown=out.md] [--title="…"]
 *
 * `pnpm bench:compare` answers "did this change move anything against the
 * reference I recorded on THIS machine?". CI cannot ask that: the baseline is
 * per-machine and gitignored for good reason, and `compare()` would rightly
 * refuse a stranger's — the calibration probe would differ and every verdict
 * would come back "no verdict". So CI asks a different question, and this is
 * the tool for it: two results measured minutes apart ON THE SAME RUNNER, one
 * per ref. Absolute numbers off a shared vCPU mean nothing; the delta between
 * two such runs does.
 *
 * The verdicts come from the same `compare()` as the local flow — the noise
 * gate, the `inconclusive` band and the machine-drift downgrade all apply
 * unchanged, which is the point of reusing it rather than writing a second,
 * laxer set of rules for CI.
 *
 * Exit code, which is the whole gate:
 *
 * - **0** for any timing move, however large. A shared runner cannot carry the
 *   burden of blocking a merge on one — measured on two IDENTICAL commits it
 *   produced regressions of 16%, 19% and 53%, all false.
 * - **1** when an `exact` metric regressed. Those are invariants, not
 *   measurements — directory calls per activation, microtask turns per
 *   dispatch — so the same code gives the same value on any machine under any
 *   load. In the same two-identical-commit run, every one of them came back
 *   bit-identical. A move there is a real behavioural change and nothing else.
 * - **1** when it cannot give an honest answer at all: a missing or unreadable
 *   file, or a comparison it must refuse (`fatalMismatch`).
 */
import { writeFileSync } from 'node:fs';
import { markdownComparison } from './markdown.ts';
import { DEFAULT_THRESHOLD, compare, loadResult, printComparison } from './report.ts';

interface Options {
    before: string;
    after: string;
    threshold: number;
    markdown?: string;
    title?: string;
}

function parseArgs(argv: readonly string[]): Options {
    let before: string | undefined;
    let after: string | undefined;
    let threshold = DEFAULT_THRESHOLD;
    let markdown: string | undefined;
    let title: string | undefined;

    for (const arg of argv) {
        if (arg.startsWith('--before=')) before = arg.slice('--before='.length);
        else if (arg.startsWith('--after=')) after = arg.slice('--after='.length);
        else if (arg.startsWith('--markdown=')) markdown = arg.slice('--markdown='.length);
        else if (arg.startsWith('--title=')) title = arg.slice('--title='.length);
        else if (arg.startsWith('--threshold=')) {
            threshold = Number(arg.slice('--threshold='.length)) / 100;
        } else throw new Error(`unknown flag: ${arg}`);
    }
    if (!before || !after) {
        throw new Error('both --before=<file> and --after=<file> are required');
    }
    if (!Number.isFinite(threshold) || threshold <= 0) {
        throw new Error('--threshold must be a positive percentage');
    }
    return { before, after, threshold, markdown, title };
}

function main(): void {
    const options = parseArgs(process.argv.slice(2));
    const before = loadResult(options.before);
    const after = loadResult(options.after);
    const outcome = compare(after, before, options.threshold);

    if (outcome.fatalMismatch.length > 0) {
        // Refusing is not pedantry: the same three replicas differ by more
        // than 2x packed vs spread and every report looks identical, so a
        // comparison across shapes is a confident wrong answer.
        console.error('refusing to compare — the deployments differ:');
        for (const m of outcome.fatalMismatch) console.error(`  ${m}`);
        process.exitCode = 1;
        return;
    }

    printComparison(outcome, options.threshold);

    if (options.markdown !== undefined) {
        writeFileSync(
            options.markdown,
            markdownComparison(outcome, before, after, {
                threshold: options.threshold,
                title: options.title
            })
        );
        console.log('');
        console.log(`report → ${options.markdown}`);
    }

    // Written LAST, so the report exists on disk before this trips: the
    // caller has to be able to post the explanation of a failure it caused.
    if (outcome.exactRegressions.length > 0) {
        console.error('');
        console.error(
            `${outcome.exactRegressions.length} exact metric(s) regressed — ` +
                `these are invariants, so this is a behavioural change, not noise.`
        );
        process.exitCode = 1;
    }
}

main();
