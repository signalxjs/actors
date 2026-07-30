/**
 * Tier 3 — a REAL deployment over its public endpoint.
 *
 * Tier 1 measures algorithmic shape in one process; Tier 2 measures counts
 * over real sockets across forked silos. Tier 3 measures neither: it
 * measures a deployment — TLS, an ingress, a placement policy, an edge
 * hash, a directory in Redis, whatever the chart happens to say today. Its
 * numbers are therefore NOT comparable to Tier 1 or Tier 2 ones, and
 * `BASELINES.md` says so in the tier legend.
 *
 * Opt-in twice over, because it costs money and needs an environment:
 *
 *   BENCH_INFRA=1 INFRA_URL=https://chat.example.net \
 *   INFRA_AUTH_SECRET=… pnpm bench:infra
 *
 * What it is FOR: change something (a placement policy, an annotation, a
 * replica count) and see whether it helped, with the same baseline/compare
 * machinery as every other tier. What it is NOT for: absolute capacity.
 * The driver is one Node process, which caps out well below what a real
 * fleet of clients extracts — `examples/aks-cluster/deploy/edge-ladder.mjs`
 * on a same-region VM is the tool for that, and the two must not be quoted
 * interchangeably.
 *
 * Give it TIME. Measured on a 3-silo deployment: `--runs=2 --duration=15000`
 * lands the read ladder at +/-13-32% but leaves the write and locality
 * scenarios at +/-68-91%, where nothing short of a 2x change is visible.
 * `--runs=3 --duration=30000` or more is the honest minimum for those, and
 * the harness prints the spread precisely so a too-short run cannot be
 * mistaken for a verdict ("noise >> signal" means exactly that).
 *
 * A comparison is refused outright when the DEPLOYMENT SHAPE differs (see
 * `INFRA_SHAPE`): three silos packed on one node and three spread across
 * three look identical in every report and differ by more than 2x in
 * throughput, so silently comparing them produces a confident wrong
 * answer.
 */
import { execFileSync } from 'node:child_process';
import { createHmac } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { LATENCY_NOISE_FLOOR_MS } from '../loop.ts';
import type { Metric, Scenario } from '../types.ts';

const URL_BASE = process.env.INFRA_URL?.replace(/\/$/, '') ?? '';
const AUTH_SECRET = process.env.INFRA_AUTH_SECRET ?? '';
const ROOMS = Number(process.env.INFRA_ROOMS ?? 64);
/** Where the load is DRIVEN FROM. Measured: a laptop across an ocean
 *  reports +/-50-80% run to run, which cannot detect a 30% regression; the
 *  same ladder from a VM beside the cluster holds a few percent. So the
 *  driver is the VM, and this tier refuses to run without one. */
const VM_RG = process.env.INFRA_VM_RG ?? '';
const VM_NAME = process.env.INFRA_VM_NAME ?? '';

export const TIER3_ENABLED =
    process.env.BENCH_INFRA === '1' &&
    URL_BASE !== '' &&
    VM_RG !== '' &&
    VM_NAME !== '' &&
    // Without it every call is a 401 and the ladder still reports rows —
    // near-zero throughput that reads exactly like a real regression.
    AUTH_SECRET !== '';

/**
 * The deployment's identity for baseline purposes, supplied by whoever
 * knows it (`testenv.mjs` computes it from the live release). Compared
 * verbatim; an empty value on either side of a comparison is itself a
 * mismatch, because "unknown shape" cannot be asserted equal to anything.
 */
export const INFRA_SHAPE = process.env.INFRA_SHAPE ?? '';

// --- driving the load from the region, not from here -----------------------

const LADDER_PATH = fileURLToPath(
    new URL('../../../examples/aks-cluster/deploy/edge-ladder.mjs', import.meta.url)
);

/** The generator, base64'd so it needs nothing pre-installed on the VM. */
const ladderB64 = (): string => readFileSync(LADDER_PATH).toString('base64');

interface LadderRow {
    c: number;
    opsPerSec: number;
    p50: number;
    p90: number;
    p99: number;
    errs: number;
}

/**
 * Run the ladder on the VM and parse its JSON lines. One `az` round trip
 * per call, so scenarios keep their ladders in a single invocation rather
 * than one per rung.
 */
function driveFromVm(env: Record<string, string>): LadderRow[] {
    const script = [
        'set -e',
        `echo '${ladderB64()}' | base64 -d > /tmp/ladder.mjs`,
        'ulimit -n 65535',
        `export COOKIE='${cookie()}'`,
        `export TARGET_URL='${URL_BASE}'`,
        ...Object.entries(env).map(([k, v]) => `export ${k}='${v}'`),
        '/opt/n/bin/node /tmp/ladder.mjs'
    ].join('\n');
    const out = execFileSync(
        'az',
        ['vm', 'run-command', 'invoke', '-g', VM_RG, '-n', VM_NAME, '--command-id',
            'RunShellScript', '--scripts', script, '--query', 'value[0].message', '-o', 'tsv'],
        { encoding: 'utf8', maxBuffer: 8 * 1024 * 1024 }
    );
    const rows = out
        .split('\n')
        .filter((l) => l.trim().startsWith('{'))
        .map((l) => JSON.parse(l) as LadderRow);
    if (rows.length === 0) {
        throw new Error(`the ladder produced no result on ${VM_NAME}:\n${out.slice(-500)}`);
    }
    return rows;
}

const cookie = (): string => {
    const sig = createHmac('sha256', AUTH_SECRET).update('bench').digest('hex');
    return `user=${encodeURIComponent(`bench.${sig}`)}`;
};

/** One ladder row as metrics. p99 stays informational: a tail over a real
 *  network moves for reasons no code change caused. */
function rowMetrics(row: LadderRow, prefix = ''): Metric[] {
    const at = prefix || `c=${row.c}/`;
    return [
        { name: `${at}ops_per_sec`, value: row.opsPerSec, unit: 'ops/s', direction: 'higher' },
        {
            name: `${at}p50_ms`,
            value: row.p50,
            unit: 'ms',
            direction: 'lower',
            noiseFloor: LATENCY_NOISE_FLOOR_MS
        },
        { name: `${at}p99_ms`, value: row.p99, unit: 'ms', direction: 'lower', informational: true },
        {
            name: `${at}errors`,
            value: row.errs,
            unit: 'count',
            direction: 'lower',
            // A few connection resets at the edge are expected (#142) and
            // must not fail a perf comparison by themselves.
            informational: true,
            noiseFloor: 1
        }
    ];
}

// --- scenarios --------------------------------------------------------------

const readLadder: Scenario = {
    name: 'infra/read-ladder',
    description: 'guarded actor reads over the public endpoint, driven from a same-region VM',
    async run(ctx) {
        const rows = driveFromVm({
            MIX: '0',
            ROOMS: String(ROOMS),
            WORKERS: '4',
            DURATION_MS: String(ctx.durationMs),
            LADDER: ctx.quick ? '32,64' : '32,64,128,256'
        });
        return rows.flatMap((r) => rowMetrics(r));
    }
};

const writeMix: Scenario = {
    name: 'infra/write-mix',
    description: 'a fifth of calls are writes (a storage CAS each), same-region VM',
    async run(ctx) {
        const rows = driveFromVm({
            MIX: '0.2',
            ROOMS: String(ROOMS),
            WORKERS: '4',
            DURATION_MS: String(ctx.durationMs),
            LADDER: ctx.quick ? '32' : '64'
        });
        return rows.flatMap((r) => rowMetrics(r));
    }
};

/**
 * The routing token sent versus withheld. This is what turns "locality
 * routing helps" into a number — and what would have caught my first
 * locality run reading as a 7% no-op because the driver never sent it.
 */
const localityAb: Scenario = {
    name: 'infra/locality-ab',
    description: 'the edge-hash delta: same load with the routing token sent and withheld',
    async run(ctx) {
        const common = {
            ROOMS: String(ROOMS),
            WORKERS: '4',
            DURATION_MS: String(ctx.durationMs),
            LADDER: ctx.quick ? '32' : '64'
        };
        const [withToken] = driveFromVm({ ...common, ROUTE: '1' });
        const [without] = driveFromVm({ ...common, ROUTE: '0' });
        return [
            ...rowMetrics(withToken!, 'token/'),
            ...rowMetrics(without!, 'no_token/'),
            {
                // >1 means the edge hash is doing something; ~1 means either
                // it is not configured or the token never reaches it.
                name: 'token_speedup',
                value: without!.opsPerSec === 0 ? 0 : withToken!.opsPerSec / without!.opsPerSec,
                unit: 'x',
                direction: 'higher',
                noiseFloor: 0.05
            }
        ];
    }
};

export const tier3Scenarios: Scenario[] = [readLadder, writeMix, localityAb];
