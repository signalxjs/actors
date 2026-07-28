/**
 * Benchmark CLI.
 *
 * Runs against the BUILT prod dist (`--conditions=production` selects
 * `dist/*.prod.js` from the export map), so what gets measured is the code
 * users actually run, with `__DEV__` stripped. `pnpm bench` builds first;
 * `pnpm bench:run` assumes you already did.
 *
 *   pnpm bench                       everything
 *   pnpm bench:run dispatch          only scenarios matching "dispatch"
 *   pnpm bench:run --quick           smaller working sets, 1 run — a smoke test
 *   pnpm bench:baseline              record the local reference
 *   pnpm bench:compare               run and diff against that reference
 *   pnpm bench:profile dispatch      same, under --cpu-prof
 */
import { existsSync } from 'node:fs';
import { calibrationScore } from './calibrate.ts';
import { captureEnv } from './env.ts';
import {
    BASELINE_PATH,
    DEFAULT_THRESHOLD,
    compare,
    loadResult,
    printComparison,
    printResult,
    saveResult
} from './report.ts';
import { runSuite } from './runner.ts';
import { ALL_SCENARIOS, selectScenarios, tier2Hint } from './scenarios/index.ts';
import type { BenchResult } from './types.ts';

interface Options {
    filters: string[];
    runs: number;
    durationMs: number;
    quick: boolean;
    saveBaseline: boolean;
    doCompare: boolean;
    threshold: number;
    list: boolean;
    gcProfile: boolean;
    json?: string;
}

function parseArgs(argv: readonly string[]): Options {
    const options: Options = {
        filters: [],
        runs: 3,
        durationMs: 400,
        quick: false,
        saveBaseline: false,
        doCompare: false,
        threshold: DEFAULT_THRESHOLD,
        list: false,
        gcProfile: true
    };
    for (const arg of argv) {
        if (arg === '--quick') options.quick = true;
        else if (arg === '--no-gc-profile') options.gcProfile = false;
        else if (arg === '--save-baseline') options.saveBaseline = true;
        else if (arg === '--compare') options.doCompare = true;
        else if (arg === '--list') options.list = true;
        else if (arg.startsWith('--runs=')) options.runs = Number(arg.slice(7));
        else if (arg.startsWith('--duration=')) options.durationMs = Number(arg.slice(11));
        else if (arg.startsWith('--threshold=')) options.threshold = Number(arg.slice(12)) / 100;
        else if (arg.startsWith('--json=')) options.json = arg.slice(7);
        else if (arg.startsWith('--')) throw new Error(`unknown flag: ${arg}`);
        else options.filters.push(arg);
    }
    if (options.quick) {
        options.runs = 1;
        options.durationMs = Math.min(options.durationMs, 150);
    }
    // A baseline is a reference, so it gets the careful settings unless the
    // caller deliberately overrode them.
    if (options.saveBaseline && !argv.some((a) => a.startsWith('--runs='))) options.runs = 5;
    return options;
}

async function main(): Promise<void> {
    const options = parseArgs(process.argv.slice(2));
    const scenarios = selectScenarios(options.filters);

    if (options.list) {
        for (const scenario of ALL_SCENARIOS) {
            console.log(`${scenario.name}\n    ${scenario.description}`);
        }
        return;
    }
    if (scenarios.length === 0) {
        const hint = tier2Hint(options.filters);
        if (hint) {
            // Guidance, not a crash: a stack trace here buries the one line
            // that tells you what to type.
            console.log(hint);
            process.exitCode = 1;
            return;
        }
        throw new Error(`no scenarios match ${JSON.stringify(options.filters)}. Try --list.`);
    }

    const env = captureEnv();
    if (env.conditions !== 'production') {
        console.log(
            `⚠ running without --conditions=production — this measures the DEV dist ` +
                `(__DEV__ guards live). Use \`pnpm bench:run\` for real numbers.`
        );
    }
    if (env.dirty) console.log('⚠ working tree is dirty — the commit stamp is approximate');

    console.log(`${scenarios.length} scenario(s), ${options.runs} interleaved round(s)`);
    const { results, calibrationSamples } = await runSuite(scenarios, {
        runs: options.runs,
        durationMs: options.durationMs,
        quick: options.quick,
        gcProfile: options.gcProfile,
        probe: calibrationScore,
        onProgress: (message) => console.log(`  ${message}`)
    });

    const result: BenchResult = {
        formatVersion: 1,
        createdAt: new Date().toISOString(),
        env,
        runs: options.runs,
        durationMs: options.durationMs,
        quick: options.quick,
        calibration: { samples: calibrationSamples },
        scenarios: results
    };

    printResult(result);

    const savedTo = saveResult(result, options.json);
    console.log('');
    console.log(`results → ${savedTo}`);

    if (options.saveBaseline) {
        saveResult(result, BASELINE_PATH);
        console.log(`baseline → ${BASELINE_PATH}`);
    } else if (options.doCompare) {
        if (!existsSync(BASELINE_PATH)) {
            throw new Error(
                `no baseline at ${BASELINE_PATH} — record one first with \`pnpm bench:baseline\`.`
            );
        }
        const outcome = compare(result, loadResult(BASELINE_PATH), options.threshold);
        printComparison(outcome, options.threshold);
        if (outcome.regressions.length > 0) process.exitCode = 1;
    }

    const failed = results.filter((r) => r.error);
    if (failed.length > 0) {
        console.log('');
        for (const f of failed) console.log(`FAILED ${f.name}: ${f.error}`);
        process.exitCode = 1;
    }
}

await main();
