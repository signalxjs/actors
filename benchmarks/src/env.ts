/**
 * The machine a result came from. Stamped into every result file because a
 * benchmark number without its environment is not a number — comparing a
 * run from a plugged-in desktop against one from a throttled laptop
 * produces confident nonsense, and `compare()` refuses to do it silently.
 */
import { execFileSync } from 'node:child_process';
import { arch, cpus, platform, totalmem } from 'node:os';
import { shapeMismatch } from './shape.mjs';

export interface BenchEnv {
    node: string;
    nodeMajor: number;
    platform: string;
    arch: string;
    cpuModel: string;
    cpuCount: number;
    totalMemBytes: number;
    /** Which export condition resolved — 'production' means dist/*.prod.js. */
    conditions: string;
    commit: string;
    dirty: boolean;
    /**
     * The DEPLOYMENT's shape, supplied via `INFRA_SHAPE` by whoever knows
     * it, when measuring against a real deployment. Empty otherwise. Compared verbatim and never guessed: three hosts
     * packed on one node and three spread across three look identical in
     * every report and differ by more than 2x in throughput, so a silent
     * comparison across shapes is a confident wrong answer.
     */
    infraShape?: string;
}

function gitCommit(): { commit: string; dirty: boolean } {
    try {
        const commit = execFileSync('git', ['rev-parse', '--short', 'HEAD'], {
            encoding: 'utf8',
            stdio: ['ignore', 'pipe', 'ignore']
        }).trim();
        const status = execFileSync('git', ['status', '--porcelain'], {
            encoding: 'utf8',
            stdio: ['ignore', 'pipe', 'ignore']
        }).trim();
        return { commit, dirty: status.length > 0 };
    } catch {
        return { commit: 'unknown', dirty: false };
    }
}

/**
 * Node does not expose the resolved condition set, so read it back off the
 * flags we were launched with. `pnpm bench:run` passes
 * `--conditions=production`; without it the export map falls through to
 * `import`, i.e. the dev dist.
 */
function resolvedConditions(): string {
    const flag = process.execArgv.find((a) => a.startsWith('--conditions='));
    return flag ? flag.slice('--conditions='.length) : 'default (dev dist)';
}

export function captureEnv(): BenchEnv {
    const { commit, dirty } = gitCommit();
    const cpu = cpus();
    return {
        node: process.version,
        nodeMajor: Number(process.versions.node.split('.')[0]),
        platform: platform(),
        arch: arch(),
        cpuModel: cpu[0]?.model ?? 'unknown',
        cpuCount: cpu.length,
        totalMemBytes: totalmem(),
        conditions: resolvedConditions(),
        infraShape: process.env.INFRA_SHAPE ?? '',
        commit,
        dirty
    };
}

/** Fields that must match for two results to be worth comparing. */
// The shape half of the contract lives in `shape.mjs` so `testenv.mjs` can
// apply the SAME refusal to a hand-run `ws-load` (#224); re-exported here
// because this module is where every in-tree comparer already looks.
export { INFRA_SHAPE_MISMATCH } from './shape.mjs';

export function envMismatch(a: BenchEnv, b: BenchEnv): string[] {
    const differing: string[] = [];
    if (a.nodeMajor !== b.nodeMajor) differing.push(`node ${a.node} vs ${b.node}`);
    if (a.platform !== b.platform) differing.push(`platform ${a.platform} vs ${b.platform}`);
    if (a.arch !== b.arch) differing.push(`arch ${a.arch} vs ${b.arch}`);
    if (a.cpuModel !== b.cpuModel) differing.push(`cpu ${a.cpuModel} vs ${b.cpuModel}`);
    if (a.conditions !== b.conditions) {
        differing.push(`conditions ${a.conditions} vs ${b.conditions}`);
    }
    // Prefixed (see shape.mjs) so the comparer can tell this apart from the
    // advisory mismatches above and refuse outright.
    const shape = shapeMismatch(a.infraShape, b.infraShape);
    if (shape) differing.push(shape);
    return differing;
}
