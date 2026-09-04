/**
 * Tier 2 — the workflow engine as N real host processes on ONE box (#381).
 *
 * The first multi-core measurement. Every recorded `workflow/*` figure is
 * three 1 vCPU pods on three nodes, and no run had ever put N hosts on one
 * machine — which is the shape "one host per core" needs a number for
 * before anything is built around it. `perf/aks/wf-fleet.mjs` forks N
 * copies of the real host entry over one local Redis and drives them with
 * the real load generator through a round-robin proxy (the Service's
 * stand-in); this file is the metric mapping over it, reusing
 * `workflow.ts`'s so a laptop row and a Tier-3 row name the same things.
 *
 * Opt-in, like Tier 2, plus a Redis to share:
 *
 *   BENCH_WF_LOCAL=1 REDIS_URL=redis://127.0.0.1:6379 pnpm bench:wf-local
 *
 * ## What is evidence here
 *
 * The Tier-2 rule applies (see the tier legend in `BASELINES.md`): N
 * processes share the cores, so every timing is `informational` — the
 * comparer structurally cannot gate on one. Counts gate: `stuck_ratio`,
 * `run_error_rate`, `start_deferred_ratio`, `completed_unreported`,
 * `wakes_lost`, `reminder_set_failure_ratio`. And the decision number of
 * the multi-core arm is a RATIO measured back to back on one box —
 * completed runs/s at the widest fleet over the narrowest — which is
 * robust to the machine in a way the absolute knee is not.
 *
 * ## The shape
 *
 * `INFRA_SHAPE` is stamped from this module when the run is enabled and
 * nothing set it: `wf-local hosts=<ladder> cores=… cpu=… node=… image=…
 * knobs=…`. The prefix differs from Tier 3's `wf`, so `--compare` refuses
 * a laptop run against a cluster baseline; `cores` and `cpu` are in it
 * because here the box IS the estate.
 */
import { execFileSync } from 'node:child_process';
import { availableParallelism, cpus } from 'node:os';
import { runWfFleet } from '../../../perf/aks/wf-fleet.mjs';
import type { WfFleetRung } from '../../../perf/aks/wf-fleet.d.mts';
import type { Metric, RunContext, Scenario } from '../types.ts';
import { mechanismMetrics, rowMetrics } from './workflow.ts';

const REDIS_URL = process.env.REDIS_URL ?? '';

export const WF_LOCAL_ENABLED = process.env.BENCH_WF_LOCAL === '1' && REDIS_URL !== '';

export function wfLocalHint(): string {
    if (process.env.BENCH_WF_LOCAL !== '1') {
        return (
            'wf-local/* scenarios are opt-in — they fork a host process per fleet member and ' +
            'need a Redis. Run:\n  BENCH_WF_LOCAL=1 REDIS_URL=redis://127.0.0.1:6379 pnpm bench:wf-local'
        );
    }
    return 'wf-local/* need REDIS_URL (a local `redis-server` is enough)';
}

/**
 * A comma ladder, in the caller's order. Pure over the raw value so it is
 * tested; an env var that is set but parses to nothing is refused rather
 * than read as "no rungs" — a scenario that silently no-ops on a typo is
 * worse than one that fails to start.
 */
export function parseLadder(name: string, raw: string | undefined, fallback: string): number[] {
    const rungs = (raw ?? fallback)
        .split(',')
        .map((s) => Number(s.trim()))
        .filter((n) => Number.isFinite(n) && n > 0);
    if (rungs.length === 0) {
        throw new Error(`[wf-local] ${name} must list positive numbers, got '${raw ?? fallback}'`);
    }
    return rungs;
}

const ladder = (name: string, fallback: string): number[] => parseLadder(name, process.env[name], fallback);

/** A positive number from the env, or the fallback when unset — never 0 or
 *  NaN passed on to the generator to fail later under another name. */
export function positiveNumber(name: string, raw: string | undefined, fallback: number): number {
    if (raw === undefined) return fallback;
    const value = Number(raw);
    if (raw.trim() === '' || !Number.isFinite(value) || value <= 0) {
        throw new Error(`[wf-local] ${name} must be a positive number, got '${raw}'`);
    }
    return value;
}

/** Every host knob that changes the curve without changing the image —
 *  the same list `testenv.mjs` puts in the Tier-3 `wf` shape. */
const HOST_KNOBS = ['FETCH_CONNECTIONS', 'TRANSPORT', 'REMINDERS'];

/**
 * The `wf-local` shape string. Pure over its inputs so it is tested; the
 * module stamps it into `INFRA_SHAPE` below.
 */
export function wfLocalShape(input: {
    hosts: readonly number[];
    cores: number;
    cpu: string;
    nodeMajor: number;
    image: string;
    env: Record<string, string | undefined>;
}): string {
    const knobs = Object.keys(input.env)
        .filter((k) => (k.startsWith('WF_') || HOST_KNOBS.includes(k)) && input.env[k] !== undefined && input.env[k] !== '')
        .sort()
        .map((k) => `${k}=${input.env[k]}`)
        .join(',');
    // The CPU model with its spaces collapsed: the shape is split on
    // whitespace by eye, never parsed, but a model name with spaces in it
    // reads as several fields.
    const cpu = input.cpu.replace(/\s+/g, '_');
    return `wf-local hosts=${input.hosts.join(',')} cores=${input.cores} cpu=${cpu} node=${input.nodeMajor} image=${input.image} knobs=${knobs || '(default)'}`;
}

function gitSha(): string {
    try {
        return execFileSync('git', ['rev-parse', '--short', 'HEAD'], {
            encoding: 'utf8',
            stdio: ['ignore', 'pipe', 'ignore']
        }).trim();
    } catch {
        return 'unknown';
    }
}

/** The fleet-size ladder of the multi-core arm; the widest is the drown-vs-shed fleet. */
const HOSTS = ladder('WF_LOCAL_HOSTS', '1,2,4,8');
const WIDEST = Math.max(...HOSTS);

if (WF_LOCAL_ENABLED && !process.env.INFRA_SHAPE) {
    process.env.INFRA_SHAPE = wfLocalShape({
        hosts: HOSTS,
        cores: availableParallelism(),
        cpu: cpus()[0]?.model ?? 'unknown',
        nodeMajor: Number(process.versions.node.split('.')[0]),
        image: gitSha(),
        env: process.env
    });
}

/**
 * Timings are context on a shared box: demote every non-ratio, non-count
 * metric to informational. Ratios (`ratio`, and the `x` multipliers between
 * arms) and counts carry their own noise floors and keep gating.
 */
export function asTier2(metrics: Metric[]): Metric[] {
    return metrics.map((m) =>
        m.unit === 'ratio' || m.unit === 'x' || m.unit === 'count' ? m : { ...m, informational: true }
    );
}

/** The per-rung CPU and backlog columns, all informational. */
function rungExtras(rung: WfFleetRung, prefix: string): Metric[] {
    const queued = Object.values(rung.queuedAfter).reduce((a, b) => a + b, 0);
    const extras: Metric[] = [
        {
            // The BUSIEST host per sample: ~100 means one host sat at a core,
            // which on a fleet of N is the one-thread-per-host ceiling made
            // visible; the generator's own share says whether the rung
            // measured the fleet or the generator.
            name: `${prefix}host_cpu_peak_pct`,
            value: rung.hostCpu.peak ?? 0,
            unit: '%',
            direction: 'lower',
            informational: true
        },
        {
            name: `${prefix}generator_cpu_peak_pct`,
            value: rung.generatorCpu.peak ?? 0,
            unit: '%',
            direction: 'lower',
            informational: true
        },
        {
            // Turns still queued when the generator exited — a backlog the
            // drain did not clear, the drown signature.
            name: `${prefix}queued_after`,
            value: queued,
            unit: 'count',
            direction: 'lower',
            noiseFloor: 1,
            informational: true
        },
        {
            name: `${prefix}drain_s`,
            value: Math.round(rung.row.drainMs / 100) / 10,
            unit: 's',
            direction: 'lower',
            informational: true
        }
    ];
    // The outcome split. `finished` alone hides the drown signature: an
    // overloaded fleet that fails runs through the 30 s call deadline
    // finishes them FASTER than one that completes them, and its
    // `run_error_rate` stays 0 because the engine absorbed every deadline
    // as a run failure rather than a client error.
    for (const key of ['completed', 'failed', 'compensated', 'cancelled'] as const) {
        extras.push({
            name: `${prefix}${key}`,
            value: rung.row[key],
            unit: 'count',
            direction: key === 'completed' ? 'higher' : 'lower',
            noiseFloor: 1,
            informational: true
        });
    }
    return extras;
}

const DURATION_S = positiveNumber('WF_LOCAL_DURATION_S', process.env.WF_LOCAL_DURATION_S, 20);

/**
 * The knobs every arm shares: the Tier-3 throughput ladder's short delays
 * (volatile timers); a short signal timeout so the drain's floor is the
 * unsignalled approvals' 5 s, not their 30 s; the generator's admission
 * cap lifted out of the way (a deferral must mean the engine, never the
 * harness); and a drain long enough that a saturated one-host arm can
 * finish every run it accepted — `stuck` must stay a finding, not a
 * timeout.
 */
const COMMON = {
    WF_DELAY_MS: '2000',
    WF_SIGNAL_TIMEOUT_MS: '5000',
    WF_MAX_INFLIGHT: '50000',
    WF_DRAIN_S: '900'
};

const multiCore: Scenario = {
    name: 'wf-local/multi-core',
    description:
        'The same offered rate against 1, 2, 4, 8 hosts on one box — does one host per core scale the engine?',
    async run(ctx: RunContext) {
        // Above EVERY arm's knee on purpose — saturating, so completed/s is
        // each fleet's capacity and the ratio is a capacity ratio. Below the
        // knee every arm completes everything and the ratio reads 1.0
        // whatever the fleet does; between the knees the drain floor (the
        // 5 s signal timeout) compresses it. One host of the default mix
        // saturates at ~15 runs/s on this class of core (the mix is ~60 ms
        // of sha256 per run), so 200 clears eight of them.
        const rate = positiveNumber('WF_LOCAL_RATE', process.env.WF_LOCAL_RATE, 200);
        const hosts = ctx.quick ? HOSTS.slice(0, 2) : HOSTS;
        const durationS = ctx.quick ? 10 : DURATION_S;
        const metrics: Metric[] = [];
        const completed: Record<number, number> = {};
        // A ten-minute call deadline, so overload on the narrow arms is
        // QUEUEING rather than deadline failures: the first run of this
        // scenario had the one-host arm fail two thirds of its runs at the
        // 30 s default (order p50 landed at 30.9 s, the deadline itself),
        // which made completed/s a ratio between an arm that failed fast
        // and one that finished — not a capacity ratio. Drown-vs-shed keeps
        // the default deadline on purpose; that is where it is the subject.
        const env = { ...COMMON, WF_CALL_TIMEOUT_MS: '600000' };
        for (const n of hosts) {
            const result = await runWfFleet({ hosts: n, rate, durationS, env, redisUrl: REDIS_URL });
            const rung = result.rungs[0];
            if (!rung) throw new Error(`${this.name} hosts=${n}: no rung came back`);
            const prefix = `h=${n}/`;
            completed[n] = rung.row.runsCompletedPerSec;
            metrics.push(
                ...asTier2(rowMetrics(rung.row, prefix)),
                ...asTier2(mechanismMetrics(rung).map((m) => ({ ...m, name: `${prefix}${m.name}` }))),
                ...rungExtras(rung, prefix)
            );
        }
        const narrowest = Math.min(...hosts);
        const widest = Math.max(...hosts);
        if (narrowest !== widest && completed[narrowest]) {
            metrics.push({
                // THE number: completed runs/s at the widest fleet over the
                // narrowest, both measured on this box in this run.
                // >= ~6x at 8 hosts says one host per core scales the
                // engine; <= ~3x says something shared caps the box. It
                // gates, with a floor of a whole multiple: two runs on one
                // busy box read 5.18x and 5.93x fifteen minutes apart, so a
                // finer floor would call the machine, not the code — but a
                // fleet that stopped scaling by 1x is exactly what a
                // comparison must say.
                name: `scale_${widest}_over_${narrowest}`,
                value: Math.round((completed[widest]! / completed[narrowest]!) * 100) / 100,
                unit: 'x',
                direction: 'higher',
                noiseFloor: 1
            });
        }
        return metrics;
    }
};

/** The admission cap the `cap=` arm runs under (#384).
 *
 *  Sized by the rule the runtime documents — `maxQueued ≈ callTimeoutMs /
 *  p50 turn ms` — against the numbers this very scenario recorded before
 *  the cap existed: a 30 s call deadline over a ~55 ms turn at the knee is
 *  ~545, and the arm that drowned did so with thousands queued behind the
 *  compute pool and the singleton aggregator. `WF_LOCAL_CAP` moves it,
 *  because the right value is a deployment's to choose and the point of
 *  the arm is the SHAPE of the failure, not this number. */
const CAP = process.env.WF_LOCAL_CAP ?? '512';

const drownVsShed: Scenario = {
    name: 'wf-local/drown-vs-shed',
    description:
        'The widest fleet past its knee, at 20 ms tasks (the recorded workload) and 2 ms (the runtime is what saturates), then 20 ms again under an admission cap',
    async run(ctx: RunContext) {
        const sweep = ladder('WF_LOCAL_SWEEP', ctx.quick ? '50,100' : '50,100,200,500');
        const durationS = ctx.quick ? 10 : DURATION_S;
        const metrics: Metric[] = [];
        // The third arm is the #384 after-picture: the same 20 ms workload
        // that drowned, with a per-actor queue cap. Read `refused` against
        // `timeouts` — shedding turns a deadline failure into a fast
        // branded refusal, and the drain empties.
        const arms = [
            { taskMs: '20', cap: undefined },
            { taskMs: '2', cap: undefined },
            { taskMs: '20', cap: CAP }
        ];
        for (const arm of arms) {
            const result = await runWfFleet({
                hosts: WIDEST,
                sweep,
                durationS,
                env: {
                    ...COMMON,
                    WF_TASK_MS: arm.taskMs,
                    ...(arm.cap === undefined ? {} : { WF_MAX_QUEUED: arm.cap })
                },
                redisUrl: REDIS_URL
            });
            for (const rung of result.rungs) {
                const prefix = `task=${arm.taskMs}${arm.cap === undefined ? '' : `,cap=${arm.cap}`}/r=${rung.rate}/`;
                metrics.push(
                    ...asTier2(rowMetrics(rung.row, prefix)),
                    ...asTier2(mechanismMetrics(rung).map((m) => ({ ...m, name: `${prefix}${m.name}` }))),
                    ...rungExtras(rung, prefix)
                );
            }
        }
        return metrics;
    }
};

export const wfLocalScenarios: Scenario[] = [multiCore, drownVsShed];
