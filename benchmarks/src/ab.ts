/**
 * The interleaved A/B driver: measure two checkouts alternately, several
 * rounds, and write every round's raw payloads for `ab-report.ts` to
 * interpret. Ported from signalxjs/core#639 (#98).
 *
 * Why interleave at all. The sequential A/B measured the base ref's whole
 * half, then the head ref's — so machine drift, thermal state, page-cache
 * and JIT warmth all accrue to the head's account, because the head is
 * always second. This workflow's own comment header used to quantify it:
 * "the head half is always measured second; across all throughput metrics
 * that order is worth about +2%." That is a systematic bias, not a caveat,
 * and raising `--runs` cannot fix it: more samples on one side measure the
 * same drift more precisely.
 *
 * So: alternate, and COUNTERBALANCE the alternation. Round r measures
 * [base, head] when r is even and [head, base] when odd. Under a linear
 * drift the two orders carry equal and opposite bias, so it cancels to
 * first order in the paired deltas rather than accruing to one side. An
 * A/B/A/B schedule that never swaps would leave base systematically earlier
 * in every round and reproduce the original bias in miniature.
 *
 * Each side runs ITS OWN `benchmarks/src/main.ts`, from its own checkout,
 * exactly as the sequential job did — the base ref is an arbitrary older
 * commit whose suite need not contain scenarios the head introduces, so
 * only flags the base already understands are passed (`--runs`, `--json`,
 * bare scenario filters), and rows that exist on one side only fall out as
 * unmatched rather than being force-matched.
 *
 * Deliberately dependency-free and free of local imports: payloads are
 * opaque JSON here and only `ab-report.ts` interprets them, so this file
 * runs identically from the head checkout whatever the base ref contains.
 *
 * Usage:
 *   node --conditions=production --expose-gc benchmarks/src/ab.ts \
 *     --base-dir=<path> --head-dir=<path> --out=<path> \
 *     [--rounds=5] [--runs=1] [scenario filters...]
 */
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const DEFAULT_ROUNDS = 5;

/**
 * One in-process repetition per side per round, by default: the statistics
 * come from BETWEEN-round repetition (which the verdict computes spread and
 * a sign test from), so in-process rounds would multiply wall-clock for
 * variance the report does not read.
 */
const DEFAULT_RUNS = 1;

type Side = 'base' | 'head';

interface RoundResult {
    index: number;
    /** The order this round actually ran in — counterbalancing is data, not a convention. */
    order: Side[];
    base: unknown;
    head: unknown;
}

function argValue(name: string): string | undefined {
    const prefix = `--${name}=`;
    return process.argv.find((a) => a.startsWith(prefix))?.slice(prefix.length);
}

function fail(message: string): never {
    console.error(`[ab] ${message}`);
    process.exit(1);
}

function requireDir(name: string): string {
    const dir = argValue(name);
    if (!dir) fail(`missing --${name}=<path>`);
    if (!fs.existsSync(path.join(dir, 'benchmarks', 'src', 'main.ts'))) {
        fail(`--${name}=${dir} does not look like a checkout (no benchmarks/src/main.ts)`);
    }
    return dir;
}

function integer(name: string, fallback: number, minimum: number): number {
    const raw = argValue(name);
    if (raw === undefined) return fallback;
    const value = Number(raw);
    if (!Number.isInteger(value) || value < minimum) {
        fail(`--${name} must be an integer >= ${minimum}, got: ${raw}`);
    }
    return value;
}

/** Bare scenario filters, passed through to both sides verbatim. */
function filters(): string[] {
    return process.argv.slice(2).filter((a) => !a.startsWith('--'));
}

/**
 * Run one side's suite in its own checkout and return the payload it wrote.
 * `process.execArgv` is forwarded so `--conditions=production --expose-gc`
 * survive into the child — without the former the child resolves the DEV
 * dist, whose warn branches would be measured instead of the shipping code.
 */
function measure(
    side: Side,
    dir: string,
    round: number,
    runs: number,
    scenarioFilters: string[],
    out: string
): unknown {
    const script = path.join(dir, 'benchmarks', 'src', 'main.ts');
    const resultFile = `${out}.round${round}-${side}.json`;
    const started = Date.now();
    console.log(`\n[ab] round ${round + 1} — ${side}`);
    const run = spawnSync(
        process.execPath,
        [...process.execArgv, script, ...scenarioFilters, `--runs=${runs}`, `--json=${resultFile}`],
        { cwd: dir, stdio: 'inherit' }
    );
    if (run.error) {
        fail(`round ${round + 1} ${side}: could not start main.ts — ${run.error.message}`);
    }
    if (run.status !== 0) {
        // A partial A/B is worse than none: a missing round silently changes
        // what "all rounds agree" means in the verdict.
        fail(`round ${round + 1} ${side}: main.ts exited ${run.status}`);
    }
    if (!fs.existsSync(resultFile)) {
        fail(`round ${round + 1} ${side}: main.ts wrote no ${resultFile}`);
    }
    console.log(`[ab] round ${round + 1} ${side} took ${((Date.now() - started) / 1000).toFixed(1)}s`);
    const payload: unknown = JSON.parse(fs.readFileSync(resultFile, 'utf8'));
    fs.unlinkSync(resultFile);
    return payload;
}

function main(): void {
    const baseDir = requireDir('base-dir');
    const headDir = requireDir('head-dir');
    const out = argValue('out');
    if (!out) fail('missing --out=<path>');
    // Two rounds is the minimum that can counterbalance; one round is just
    // the old sequential A/B wearing a new name, and the verdict rules would
    // read a single paired delta as unanimous.
    const totalRounds = integer('rounds', DEFAULT_ROUNDS, 2);
    const runs = integer('runs', DEFAULT_RUNS, 1);
    const scenarioFilters = filters();

    console.log(
        `[ab] ${totalRounds} interleaved rounds, counterbalanced ` +
            `(even rounds base-first, odd rounds head-first), --runs=${runs} per side per round`
    );
    console.log(`[ab] base: ${baseDir}`);
    console.log(`[ab] head: ${headDir}`);
    if (scenarioFilters.length > 0) console.log(`[ab] filters: ${scenarioFilters.join(' ')}`);

    const results: RoundResult[] = [];
    for (let round = 0; round < totalRounds; round++) {
        const order: Side[] = round % 2 === 0 ? ['base', 'head'] : ['head', 'base'];
        const payloads: Partial<Record<Side, unknown>> = {};
        for (const side of order) {
            payloads[side] = measure(
                side,
                side === 'base' ? baseDir : headDir,
                round,
                runs,
                scenarioFilters,
                out
            );
        }
        results.push({ index: round, order, base: payloads.base, head: payloads.head });
    }

    fs.mkdirSync(path.dirname(path.resolve(out)), { recursive: true });
    fs.writeFileSync(out, JSON.stringify({ rounds: results }, null, 2));
    console.log(`\n[ab] wrote ${out} (${results.length} rounds)`);
}

main();
