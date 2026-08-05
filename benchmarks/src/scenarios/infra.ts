/**
 * Tier 3 — a REAL deployment over its public endpoint.
 *
 * Tier 1 measures algorithmic shape in one process; Tier 2 measures counts
 * over real sockets across forked hosts. Tier 3 measures neither: it
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
 * fleet of clients extracts — `perf/aks/deploy/edge-ladder.mjs`
 * on a same-region VM is the tool for that, and the two must not be quoted
 * interchangeably.
 *
 * Give it TIME. Measured on a 3-host deployment: `--runs=2 --duration=15000`
 * lands the read ladder at +/-13-32% but leaves the write and locality
 * scenarios at +/-68-91%, where nothing short of a 2x change is visible.
 * `--runs=3 --duration=30000` or more is the honest minimum for those, and
 * the harness prints the spread precisely so a too-short run cannot be
 * mistaken for a verdict ("noise >> signal" means exactly that).
 *
 * A comparison is refused outright when the DEPLOYMENT SHAPE differs (see
 * `INFRA_SHAPE`): three hosts packed on one node and three spread across
 * three look identical in every report and differ by more than 2x in
 * throughput, so silently comparing them produces a confident wrong
 * answer.
 */
import { execFileSync } from 'node:child_process';
import { createHmac } from 'node:crypto';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnable } from '../spawn.mjs';
// Derived from the build, never pasted: see post-fn.mjs for the failure
// mode that motivated it.
import { postMessageFnId } from '../../../examples/chat/deploy/post-fn.mjs';
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
/** The internal listener, via a port-forward. Only the scenarios that read
 *  a placement SPREAD need it, and they degrade rather than fail without. */
const OPS_URL = process.env.INFRA_OPS_URL ?? 'http://127.0.0.1:7399';
const OPS_SECRET = process.env.INFRA_OPS_SECRET ?? '';

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
    new URL('../../../perf/aks/deploy/edge-ladder.mjs', import.meta.url)
);

/** The generator, base64'd so it needs nothing pre-installed on the VM. */
const ladderB64 = (): string => readFileSync(LADDER_PATH).toString('base64');

interface LadderRow {
    c: number;
    /** Successful calls. Optional: a ladder from before this field existed
     *  reports none, and an error RATE without a denominator is no rate. */
    ops?: number;
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
    // `@file` rather than inline, and `spawnable` rather than a bare `az`:
    // a Windows command line can carry neither a raw newline nor a `.cmd`
    // shim spawned without a shell, and this call is both. See
    // `../spawn.mjs`.
    const dir = mkdtempSync(join(tmpdir(), 'sigx-bench-'));
    const file = join(dir, 'ladder.sh');
    let out: string;
    try {
        writeFileSync(file, script);
        const run = spawnable('az', ['vm', 'run-command', 'invoke', '-g', VM_RG, '-n', VM_NAME,
            '--command-id', 'RunShellScript', '--scripts', `@${file}`,
            '--query', 'value[0].message', '-o', 'tsv']);
        out = execFileSync(run.command, run.args, {
            encoding: 'utf8',
            maxBuffer: 8 * 1024 * 1024,
            ...run.options
        });
    } finally {
        // Retried: Windows holds a directory briefly after the process that
        // used it exits, and cleanup must not fail a run that succeeded.
        rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 200 });
    }
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

/**
 * Live activations per host, from the ops fan-out.
 *
 * Optional by design: the ladder scenarios need nothing but the public
 * endpoint, and a scenario must not fail because a port-forward is not up.
 * Null means "no spread number this run", never "spread of zero".
 */
async function ownershipSpread(): Promise<number | null> {
    if (!OPS_SECRET) return null;
    try {
        const res = await fetch(`${OPS_URL}/_sigx/ops/cluster`, {
            headers: { authorization: `Bearer ${OPS_SECRET}` }
        });
        if (!res.ok) return null;
        const body = (await res.json()) as {
            partial: boolean;
            hosts: { stats: { activations: number } }[];
        };
        // A partial fan-out makes every count a lower bound, and a lower
        // bound divided by a lower bound is not a ratio anyone can read.
        if (body.partial || body.hosts.length === 0) return null;
        const counts = body.hosts.map((h) => h.stats.activations);
        const mean = counts.reduce((a, b) => a + b, 0) / counts.length;
        return mean === 0 ? 1 : Math.max(...counts) / mean;
    } catch {
        return null;
    }
}

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
        },
        {
            // The RATE gates, which the raw count cannot. A stale serverFn
            // id makes every write 404, and a 404 is CHEAPER than a real
            // write — so a wholly broken run reads as a throughput WIN on
            // `ops_per_sec` alone, which is exactly how the pasted-in id in
            // `edge-ladder.mjs` went stale unnoticed. 1% absorbs the
            // expected resets; above that is a broken run, not a noisy one.
            name: `${at}error_rate`,
            value: row.errs === 0 ? 0 : row.errs / ((row.ops ?? 0) + row.errs),
            unit: 'ratio',
            direction: 'lower',
            noiseFloor: 0.01
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
            // Resolved from the build every run. The literal this replaced
            // had gone stale, and a stale id turns every write into a 404 —
            // which is FASTER than a write, so the scenario reported the
            // breakage as a throughput gain. `error_rate` now gates it too.
            POST_FN: postMessageFnId(),
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
 *
 * A `token_speedup` near 1.0 has two causes and this scenario cannot tell
 * them apart: locality genuinely not paying off, or the edge never hashing
 * at all. The second is not hypothetical — ingress-nginx >= 1.12 silently
 * drops `upstream-hash-by` under its default annotations-risk-level, which
 * made this report 1.01 at three hosts and 0.99 at seven while the chart
 * looked correct. Read it next to the `edge hash actually pins a token`
 * assertion in `infra.test.ts`, which fails outright in that case; a
 * speedup of ~1.0 is only a statement about locality once that is green.
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

/**
 * `defineWorker` versus the same work on a single activation.
 *
 * Both arms in ONE run, so the verdict never depends on a baseline: the
 * ratio is read off two ladders taken minutes apart against one deployment,
 * which is the only comparison a tier this noisy supports.
 *
 * What makes this Tier 3 rather than Tier 1: the pool sizes itself from the
 * REAL `navigator.hardwareConcurrency` (clamped to 16) on the real node SKU
 * and under the container's real CPU limit. Every unit test and both exact
 * bench gates pin `maxLocal` explicitly, so the default is only ever
 * exercised here.
 *
 * What the ratio means — measured, not assumed: a host is ONE JS thread,
 * so a CPU-bound body gains ~nothing from extra pool members on that host
 * (8 members on an 8-core node: 1.07x). The pool's fleet-wide multiple
 * comes from workers activating on EVERY host that receives a call while
 * the single-activation control is pinned to one — the ratio tracks host
 * count (measured 3.03 on 3 hosts, 6.51 on 7, per-host throughput flat),
 * and its name says so.
 *
 * One key per arm on purpose: same-key concurrency IS the declared
 * contract, and spreading the load over many keys would measure nothing
 * a plain actor does not already do.
 */
const workerPool: Scenario = {
    name: 'infra/worker-pool',
    description: 'a defineWorker pool vs the identical body on one activation, same key',
    async run(ctx) {
        const common = {
            ROOMS: '1',
            WORKERS: '4',
            DURATION_MS: String(ctx.durationMs),
            LADDER: ctx.quick ? '16' : '32',
            READ_METHOD: 'summarize',
            READ_ARGS: JSON.stringify(['payload', Number(process.env.INFRA_DIGEST_ITERS ?? 2_000)])
        };
        const [pool] = driveFromVm({ ...common, ACTOR_TYPE: 'Digest', KEY_PREFIX: 'wp-pool' });
        const [single] = driveFromVm({
            ...common,
            ACTOR_TYPE: 'DigestActor',
            KEY_PREFIX: 'wp-single'
        });
        return [
            ...rowMetrics(pool!, 'worker/'),
            ...rowMetrics(single!, 'actor/'),
            {
                // NOT worker parallelism: this tracks how many hosts the
                // pool spread over (see the scenario comment). ~1 means the
                // pool never left the first host — one host in the fleet,
                // or a load pattern that never fanned out. Informational
                // because the value is deployment shape, not code speed.
                name: 'pool_spread',
                value: single!.opsPerSec === 0 ? 0 : pool!.opsPerSec / single!.opsPerSec,
                unit: 'x',
                direction: 'higher',
                informational: true,
                noiseFloor: 0.05
            }
        ];
    }
};

/**
 * Cold placement: every key fresh, so every call pays an activation.
 *
 * `KEY_FRESH=1` salts the key space per run, which is what makes this
 * measure PLACEMENT rather than the steady state a warm ladder measures.
 * The number to read next to throughput is `ownership_spread` — max/mean
 * activations across hosts — because a policy can buy latency by wrecking
 * balance, or balance by wrecking locality, and one figure alone hides
 * whichever it traded.
 *
 * Not an in-run A/B: the arms are separate DEPLOYMENTS (`PLACEMENT=…`), so
 * comparing them is `bench:diff --before --after` on two saved files. Never
 * `--compare` — the policy is part of `INFRA_SHAPE`, so that is refused, on
 * purpose.
 */
const coldPlacement: Scenario = {
    name: 'infra/cold-placement',
    description: 'a fresh-key ladder — every call activates — plus the resulting host spread',
    async run(ctx) {
        const rows = driveFromVm({
            MIX: '0',
            ROOMS: String(ROOMS * 8),
            WORKERS: '4',
            DURATION_MS: String(ctx.durationMs),
            LADDER: ctx.quick ? '32' : '32,64,128',
            KEY_FRESH: '1',
            KEY_PREFIX: 'cold'
        });
        const spread = await ownershipSpread();
        return [
            ...rows.flatMap((r) => rowMetrics(r)),
            ...(spread === null
                ? []
                : [
                      {
                          // 1.0 is perfectly even. Informational: it is a
                          // sample of a live gauge taken after the fact, not
                          // a measurement of the run.
                          name: 'ownership_spread',
                          value: spread,
                          unit: 'x',
                          direction: 'lower' as const,
                          informational: true
                      }
                  ])
        ];
    }
};

/**
 * The publish path against the plain write path.
 *
 * Every `Room#post` fans out to `ActivityFeed('all')` — ONE actor, on ONE
 * host, for the whole cluster — so this asks the question no in-process
 * bench can: does a singleton subscriber cap cluster-wide write throughput,
 * and at what concurrency does it start to?
 *
 * `Room#setTopic` publishes too, so the control arm is a READ. The ratio is
 * therefore not "publish overhead" in isolation; it is what a caller pays
 * for a write-with-fan-out over a read, which is the number an app author
 * actually chooses against.
 */
const topicsFanout: Scenario = {
    name: 'infra/topics-fanout',
    description: 'writes that fan out to the singleton subscriber, against plain reads',
    async run(ctx) {
        const common = {
            ROOMS: String(ROOMS),
            WORKERS: '4',
            DURATION_MS: String(ctx.durationMs),
            LADDER: ctx.quick ? '32' : '32,64,128',
            KEY_PREFIX: 'fanout'
        };
        const publishing = driveFromVm({
            ...common,
            MIX: '1',
            WRITE_MODE: 'actor',
            WRITE_METHOD: 'post',
            // `post(from, text)` — the key is the wire call's first arg and
            // the runtime consumes it, so only these two travel.
            WRITE_ARGS: JSON.stringify(['bench', 'x'])
        });
        const reading = driveFromVm({ ...common, MIX: '0' });
        return [
            ...publishing.flatMap((r) => rowMetrics(r, `publish/c=${r.c}/`)),
            ...reading.flatMap((r) => rowMetrics(r, `read/c=${r.c}/`))
        ];
    }
};

export const tier3Scenarios: Scenario[] = [
    readLadder,
    writeMix,
    localityAb,
    workerPool,
    coldPlacement,
    topicsFanout
];

/**
 * Why `pnpm bench:infra` matched nothing — the Tier-2 hint's twin.
 *
 * The filter is a substring match, so an un-gated tier produces
 * `no scenarios match ["infra/"]`, which says nothing about the five
 * environment variables that actually decide it.
 */
export function tier3Hint(): string | null {
    if (TIER3_ENABLED) return null;
    const missing = [
        process.env.BENCH_INFRA === '1' ? null : 'BENCH_INFRA=1',
        URL_BASE ? null : 'INFRA_URL',
        AUTH_SECRET ? null : 'INFRA_AUTH_SECRET',
        VM_RG ? null : 'INFRA_VM_RG',
        VM_NAME ? null : 'INFRA_VM_NAME'
    ].filter((v): v is string => v !== null);
    return (
        `Tier 3 (infra/*) is off: missing ${missing.join(', ')}. It measures a real ` +
        `DEPLOYMENT with the load driven from a same-region VM — ` +
        `\`node perf/aks/deploy/testenv.mjs test\` sets all of it up.`
    );
}
