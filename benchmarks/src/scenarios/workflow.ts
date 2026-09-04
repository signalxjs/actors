/**
 * Tier 3 — the workflow engine under scale-out (#297, the #85 workload).
 *
 * A third axis within Tier 3, and like `sockets/*` not comparable with
 * `infra/*`: driven from a Job INSIDE the cluster straight at the Service,
 * no ingress, no TLS, no load VM. What it measures is an ENGINE — runs
 * started and completed per second, end-to-end run latency per template,
 * the delay-node wake lag, what a fan-out join costs, what a lost wake
 * costs — on top of a real Redis directory, real reminders shards and real
 * cross-host calls. Every number here is an outcome of the runtime's
 * timers, reminders, worker pools, topics and CAS storage working together,
 * which is precisely why a workflow engine is the workload: nothing else
 * exercises all of them in one run.
 *
 * Opt-in twice over, because each run is a real Job on a real cluster:
 *
 *   BENCH_WF=1 INFRA_WF_CONTEXT=… INFRA_WF_IMAGE=… INFRA_WF_IMAGE_TAG=… pnpm bench:wf
 *
 * `testenv.mjs wf-bench` assembles that env from the live release, and the
 * shape it stamps carries the engine's HOST knobs (`WF_*`), so a run under
 * a different timer threshold or reminder tick is refused rather than
 * compared.
 *
 * **Give it `--runs=1`.** A warmup ladder plus the measured ones is several
 * minutes of a paid cluster per extra run.
 *
 * No metric is `exact`, and none can be: arrivals are Poisson, placement is
 * random, and the reminder tick quantizes every wake. `stuck_ratio` gates
 * hardest — a run the engine lost is the one failure that would otherwise
 * read as a faster completion rate.
 */
import { fileURLToPath } from 'node:url';
import { runWfLoad } from '../../../perf/aks/deploy/wf-load.mjs';
import type { WfLoadResult, WfLoadRow } from '../../../perf/aks/deploy/wf-load.d.mts';
import type { Metric, RunContext, Scenario } from '../types.ts';

const CONTEXT = process.env.INFRA_WF_CONTEXT ?? process.env.INFRA_CONTEXT ?? '';
const NAMESPACE = process.env.INFRA_WF_NS ?? 'sigx-actors-test';
const RELEASE = process.env.INFRA_WF_RELEASE ?? 'sigx';
const IMAGE = process.env.INFRA_WF_IMAGE ?? '';
const IMAGE_TAG = process.env.INFRA_WF_IMAGE_TAG ?? '';
const WORKLOAD = process.env.INFRA_WF_WORKLOAD ?? 'sigx-actors-test';

const CHART = fileURLToPath(new URL('../../../perf/aks/deploy/chart', import.meta.url));

export const WF_ENABLED =
    process.env.BENCH_WF === '1' && CONTEXT !== '' && IMAGE !== '' && IMAGE_TAG !== '';

export function wfHint(): string | null {
    if (process.env.BENCH_WF !== '1') return 'set BENCH_WF=1 (and see `testenv.mjs wf-bench`)';
    const missing = [
        CONTEXT === '' ? 'INFRA_WF_CONTEXT' : null,
        IMAGE === '' ? 'INFRA_WF_IMAGE' : null,
        IMAGE_TAG === '' ? 'INFRA_WF_IMAGE_TAG' : null
    ].filter(Boolean);
    return missing.length > 0 ? `missing ${missing.join(', ')}` : null;
}

/** A comma ladder, in the caller's order. */
const ladder = (name: string, fallback: string): number[] =>
    (process.env[name] ?? fallback)
        .split(',')
        .map((s) => Number(s.trim()))
        .filter((n) => Number.isFinite(n) && n > 0);

/** Seconds of arrivals per rung; the drain is bounded by the generator. */
const DURATION_S = process.env.INFRA_WF_DURATION_S ?? '60';

/** FNV-1a of the scenario's values — the seed version it runs under. */
function seedVersion(values: Record<string, unknown>): string {
    const text = JSON.stringify(values, Object.keys(values).sort());
    let h = 0x811c9dc5;
    for (let i = 0; i < text.length; i++) {
        h ^= text.charCodeAt(i);
        h = Math.imul(h, 0x01000193);
    }
    return String(2_000_000 + ((h >>> 0) % 1_000_000_000));
}

/**
 * The generator's in-flight cap, as a ladder value (#380). Its default
 * (5,000) is a fine bound for the mixed ladder and a false ceiling on the
 * sleep axis, where every run is in flight for its whole 90 s: past ~50/s
 * the generator deferred starts and the rung measured its own cap.
 */
const MAX_INFLIGHT = process.env.INFRA_WF_MAX_INFLIGHT ?? '';

async function drive(values: Record<string, unknown>): Promise<WfLoadResult> {
    // `WorkflowDefinition.put` is idempotent by version: without a version
    // of its own per scenario, every scenario after the first ran the
    // FIRST one's definitions — 2 s delays in the sleeping-runs rung, width
    // 8 in every fan-out rung. The generator derives one from its knobs
    // too; this is the harness-side belt to that suspender, and it lets a
    // deployed generator that predates the derivation still seed correctly.
    return await runWfLoad({
        context: CONTEXT,
        namespace: NAMESPACE,
        release: RELEASE,
        chartDir: CHART,
        imageRepository: IMAGE,
        imageTag: IMAGE_TAG,
        workload: WORKLOAD,
        values: {
            durationS: DURATION_S,
            WF_SEED_VERSION: seedVersion(values),
            ...(MAX_INFLIGHT ? { WF_MAX_INFLIGHT: MAX_INFLIGHT } : {}),
            ...values
        }
    });
}

/** The rung label: the rate the FLEET was offered, not one pod's share. */
const rung = (row: WfLoadRow): string => `r=${row.offeredRate ?? row.rate}/`;

/**
 * A `partial` result is a run that lost a generator pod or timed out —
 * its counts are a lower bound and its rates describe nothing. Throwing
 * fails the scenario, which is the honest outcome.
 */
function refusePartial(result: WfLoadResult, what: string): void {
    if (result.partial) {
        throw new Error(`${what}: the load Job did not complete cleanly — results are partial`);
    }
}

const ratio = (num: number, den: number): number => (den > 0 ? num / den : 0);

/** Per-rung metrics; `prefix` labels the rung. Exported for `wf-local/*`,
 *  so a laptop fleet and a Tier-3 fleet name their numbers identically. */
export function rowMetrics(row: WfLoadRow, prefix: string): Metric[] {
    const finished = row.completed + row.failed + row.compensated + row.cancelled;
    const metrics: Metric[] = [
        {
            name: `${prefix}runs_completed_per_sec`,
            value: row.runsCompletedPerSec,
            unit: 'runs/s',
            direction: 'higher'
        },
        {
            name: `${prefix}runs_started_per_sec`,
            value: row.runsStartedPerSec,
            unit: 'runs/s',
            direction: 'higher',
            informational: true
        },
        // The one that gates: a run the engine lost. Floor 0.1% — one
        // stuck run in a thousand is noise on a Poisson arrival stream,
        // one in a hundred is a finding.
        {
            name: `${prefix}stuck_ratio`,
            value: ratio(row.stuck.total, row.started),
            unit: 'ratio',
            direction: 'lower',
            noiseFloor: 0.001
        },
        // Admission the generator had to defer: an overloaded engine shows
        // up HERE rather than as a quietly slower arrival rate.
        {
            name: `${prefix}start_deferred_ratio`,
            value: ratio(row.startsDeferred, row.started + row.startsDeferred),
            unit: 'ratio',
            direction: 'lower',
            noiseFloor: 0.01
        },
        {
            name: `${prefix}run_error_rate`,
            value: ratio(row.errors.total + row.startFailures, row.started + row.startFailures),
            unit: 'ratio',
            direction: 'lower',
            noiseFloor: 0.01
        },
        // A completion the aggregator never saw — a topic delivery lost
        // between a run and the singleton subscriber.
        {
            name: `${prefix}completed_unreported`,
            value: row.completedUnreported,
            unit: 'count',
            direction: 'lower',
            noiseFloor: 1
        },
        {
            name: `${prefix}finished`,
            value: finished,
            unit: 'count',
            direction: 'higher',
            informational: true
        }
    ];
    if (typeof row.transitionsPerSec === 'number') {
        metrics.push({
            name: `${prefix}transitions_per_sec`,
            value: row.transitionsPerSec,
            unit: 'ops/s',
            direction: 'higher'
        });
    }
    if (row.startMs) {
        metrics.push({
            name: `${prefix}start_p50_ms`,
            value: row.startMs.p50,
            unit: 'ms',
            direction: 'lower'
        });
    }
    for (const [template, p] of Object.entries(row.latencyMs ?? {})) {
        if (!p) continue;
        metrics.push(
            {
                name: `${prefix}${template}_p50_ms`,
                value: p.p50,
                unit: 'ms',
                direction: 'lower'
            },
            {
                name: `${prefix}${template}_p99_ms`,
                value: p.p99,
                unit: 'ms',
                direction: 'lower',
                informational: true
            }
        );
    }
    const task = row.nodeMs?.task;
    if (task) {
        metrics.push({ name: `${prefix}task_p50_ms`, value: task.p50, unit: 'ms', direction: 'lower' });
    }
    if (row.wakeLagMs && row.wakeLagMs.count > 0) {
        // Actual − nominal on every delay node. Its floor is the reminder
        // tick (a durable wake fires on the next tick after `due`), which
        // is why the tick is part of the shape.
        metrics.push({
            name: `${prefix}wake_lag_p50_ms`,
            value: row.wakeLagMs.p50,
            unit: 'ms',
            direction: 'lower'
        });
    }
    if (typeof row.generatorCpuMs === 'number' && row.started > 0) {
        // What the GENERATOR spent per run — the number that sizes the
        // generator fleet for a rate, never a statement about the engine.
        metrics.push({
            name: `${prefix}generator_cpu_ms_per_run`,
            value: row.generatorCpuMs / row.started,
            unit: 'ms',
            direction: 'lower',
            informational: true
        });
    }
    return metrics;
}

/**
 * The fleet's resource curve over the run (#380): host CPU against its
 * limit beside Redis CPU as a fraction of one core — RUNBOOK (c)'s
 * "which saturates first". Informational, and absent when the sampler
 * could not see (no metrics-server, a Redis that did not answer).
 */
function timelineMetrics(result: WfLoadResult): Metric[] {
    const p = result.peaks ?? {};
    const info = (name: string, value: number | undefined, unit: string): Metric[] =>
        typeof value === 'number'
            ? [{ name, value, unit, direction: 'lower', informational: true }]
            : [];
    return [
        ...info('host_cpu_peak_ratio', p.hostCpuPeakRatio, 'ratio'),
        ...info('host_mem_peak_bytes', p.hostMemPeakBytes, 'bytes'),
        ...info('redis_cpu_peak_ratio', p.redisCpuPeakRatio, 'ratio'),
        ...info('redis_ops_per_sec_peak', p.redisOpsPerSecPeak, 'ops/s'),
        ...info('redis_mem_end_bytes', p.redisMemEndBytes, 'bytes'),
        // A fence the kubelet restarted, or a pod the run replaced: zero
        // on a clean run, the finding on a packed or a chaos one.
        ...info('restarts_during_run', result.restartsDuringRun ?? undefined, 'count'),
        ...info('pods_replaced', result.podsReplaced ?? undefined, 'count')
    ];
}

/**
 * The engine's mechanism counters over the run, from the hosts' own
 * `ops.workflow` sections — absent when either snapshot missed a host,
 * because a delta with an unknown error direction is worse than none.
 */
export function mechanismMetrics(
    result: Pick<WfLoadResult, 'delta' | 'countersTrustworthy' | 'peakActivations'>
): Metric[] {
    if (!result.countersTrustworthy) return [];
    const d = result.delta;
    const count = (name: string, key: string, direction: 'lower' | 'higher'): Metric => ({
        name,
        value: d[key] ?? 0,
        unit: 'count',
        direction,
        informational: true
    });
    const set = d.remindersSet ?? 0;
    // The per-request pair (#52), not `remoteDispatches`/`routedLocal`:
    // `routedLocal` counts placement decisions and never sees the warm
    // local fast path, so the old ratio read ~100% remote on a fleet that
    // was serving nearly everything locally. Both `?? 0` — a fleet on a
    // build that predates the pair reports neither, and `ratio` then says
    // 0 rather than NaN.
    const remote = d['cluster/dispatchesRemote'] ?? 0;
    const local = d['cluster/dispatchesLocal'] ?? 0;
    return [
        count('reminders_set', 'remindersSet', 'higher'),
        count('reminders_fired', 'remindersFired', 'higher'),
        {
            // The runtime throws after three CAS conflicts on a shard —
            // this is how often arming a durable wake lost that race.
            name: 'reminder_set_failure_ratio',
            value: ratio(d.reminderSetFailures ?? 0, set + (d.reminderSetFailures ?? 0)),
            unit: 'ratio',
            direction: 'lower',
            noiseFloor: 0.001
        },
        {
            // At-most-once firing: wakes that never arrived and were
            // recovered by a touch.
            name: 'wakes_lost',
            value: d.wakesLost ?? 0,
            unit: 'count',
            direction: 'lower',
            noiseFloor: 1
        },
        count('timers_fired', 'timersFired', 'higher'),
        count('child_starts', 'childStarts', 'higher'),
        count('join_repairs', 'joinRepairs', 'lower'),
        count('publish_failures', 'publishFailures', 'lower'),
        count('def_reads', 'defReads', 'higher'),
        count('def_cache_hits', 'defCacheHits', 'higher'),
        {
            // `dispatchesRemote / (dispatchesLocal + dispatchesRemote)`:
            // the share of this run's calls that crossed a host boundary —
            // the definition hot key and every child hop.
            name: 'remote_dispatch_ratio',
            value: ratio(remote, remote + local),
            unit: 'ratio',
            direction: 'lower',
            informational: true
        },
        ...(typeof result.peakActivations === 'number'
            ? [
                  {
                      name: 'peak_activations',
                      value: result.peakActivations,
                      unit: 'count',
                      direction: 'lower' as const,
                      informational: true
                  }
              ]
            : [])
    ];
}

const quickOr = (ctx: RunContext, quick: string, full: string) => (ctx.quick ? quick : full);

const throughputLadder: Scenario = {
    name: 'workflow/throughput-ladder',
    description:
        'Open-loop run arrivals at rising rates over the default template mix, short delays (volatile timers)',
    async run(ctx) {
        // The first measured run put the knee of this mix at ~25 runs/s on 3 x 1
        // vCPU and collapse at 50 (30 s deadlines, lost wakes). 100 is not in
        // the default: it wedged the fleet (#302) and every later scenario
        // in the run started on that backlog. INFRA_WF_RATE_LADDER reaches it.
        const rates = ladder('INFRA_WF_RATE_LADDER', quickOr(ctx, '10,25', '10,25,50'));
        const result = await drive({
            sweep: rates.join(','),
            WF_DELAY_MS: '2000'
        });
        refusePartial(result, this.name);
        return [
            ...result.merged.flatMap((row) => rowMetrics(row, rung(row))),
            ...mechanismMetrics(result),
            ...timelineMetrics(result)
        ];
    }
};

const sleepingRuns: Scenario = {
    name: 'workflow/sleeping-runs',
    description:
        'Orders whose shipping delay is a DURABLE reminder: every run leaves memory and comes back on a tick — the reminder-shard axis',
    async run(ctx) {
        // Orders only, and asleep for most of their life: cheap per run, so
        // this ladder reaches past the mixed knee on purpose — the question is
        // the reminder shards, not the CPU.
        const rates = ladder('INFRA_WF_SLEEP_RATE_LADDER', quickOr(ctx, '25', '25,50'));
        const result = await drive({
            sweep: rates.join(','),
            WF_MIX: 'order:100',
            WF_DELAY_MS: process.env.INFRA_WF_SLEEP_MS ?? '90000',
            WF_DRAIN_S: '240'
        });
        refusePartial(result, this.name);
        return [
            ...result.merged.flatMap((row) => rowMetrics(row, rung(row))),
            ...mechanismMetrics(result),
            ...timelineMetrics(result)
        ];
    }
};

const fanoutWidth: Scenario = {
    name: 'workflow/fanout-width',
    description:
        'ETL fan-out to child RUNS at rising widths: cross-host sub-workflows and the durable join',
    async run(ctx) {
        const widths = ladder('INFRA_WF_WIDTH_LADDER', quickOr(ctx, '4', '4,16,64'));
        const metrics: Metric[] = [];
        for (const width of widths) {
            // Rate scales with width so child runs/s stays ~32 across the
            // ladder: the variable is the join's width, not the load.
            const result = await drive({
                WF_MIX: 'etl:100',
                WF_START_RATE: String(Math.max(1, Math.round(32 / width))),
                WF_FANOUT_WIDTH: String(width),
                WF_FANOUT_MODE: 'children'
            });
            refusePartial(result, `${this.name} w=${width}`);
            for (const row of result.merged) {
                metrics.push(
                    ...rowMetrics(row, `w=${width}/`),
                    {
                        name: `w=${width}/child_runs`,
                        value: row.childRuns,
                        unit: 'count',
                        direction: 'higher',
                        informational: true
                    }
                );
                const join = row.nodeMs?.join;
                if (join) {
                    metrics.push({
                        name: `w=${width}/join_p50_ms`,
                        value: join.p50,
                        unit: 'ms',
                        direction: 'lower'
                    });
                }
            }
            if (width === widths[widths.length - 1]) metrics.push(...mechanismMetrics(result));
        }
        return metrics;
    }
};

const fanoutPool: Scenario = {
    name: 'workflow/fanout-pool',
    description: 'The same ETL fan-out as pool TASKS inside one turn — worker-pool saturation',
    async run(ctx) {
        const widths = ladder('INFRA_WF_WIDTH_LADDER', quickOr(ctx, '4', '4,16,64'));
        const metrics: Metric[] = [];
        for (const width of widths) {
            const result = await drive({
                WF_MIX: 'etl:100',
                WF_START_RATE: String(Math.max(1, Math.round(32 / width))),
                WF_FANOUT_WIDTH: String(width),
                WF_FANOUT_MODE: 'tasks'
            });
            refusePartial(result, `${this.name} w=${width}`);
            for (const row of result.merged) metrics.push(...rowMetrics(row, `w=${width}/`));
        }
        return metrics;
    }
};

const signals: Scenario = {
    name: 'workflow/signals',
    description:
        'Approvals: a signal delivered before, during or never — buffered signals, the wait node, the timeout edge',
    async run(ctx) {
        const delays = ladder('INFRA_WF_SIGNAL_LADDER', quickOr(ctx, '2000', '500,2000,10000'));
        const metrics: Metric[] = [];
        for (const delay of delays) {
            const result = await drive({
                WF_MIX: 'approval:100',
                WF_START_RATE: '25',
                WF_SIGNAL_DELAY_MS: String(delay),
                WF_SIGNAL_TIMEOUT_MS: '15000',
                WF_SIGNAL_SKIP_RATIO: '0.2'
            });
            refusePartial(result, `${this.name} d=${delay}`);
            for (const row of result.merged) {
                const sent = row.signalsSent;
                metrics.push(
                    ...rowMetrics(row, `d=${delay}/`),
                    {
                        name: `d=${delay}/signals_delivered`,
                        value: row.signalsDelivered ?? 0,
                        unit: 'count',
                        direction: 'higher'
                    },
                    {
                        name: `d=${delay}/signals_buffered_ratio`,
                        value: ratio(row.signalsBuffered ?? 0, sent),
                        unit: 'ratio',
                        direction: 'lower',
                        informational: true
                    },
                    {
                        // A signal that arrived after the timeout edge was
                        // taken: the engine's latency made a delivered
                        // signal late.
                        name: `d=${delay}/signal_late_ratio`,
                        value: ratio(row.signalsLate ?? 0, sent),
                        unit: 'ratio',
                        direction: 'lower',
                        noiseFloor: 0.01
                    },
                    {
                        name: `d=${delay}/signal_timeouts`,
                        value: row.signalTimeouts ?? 0,
                        unit: 'count',
                        direction: 'lower',
                        informational: true
                    }
                );
            }
        }
        return metrics;
    }
};

const sagaFailure: Scenario = {
    name: 'workflow/saga-failure',
    description: 'Sagas at rising failure rates: retries with backoff, then compensation walked backwards',
    async run(ctx) {
        const rates = ladder('INFRA_WF_FAILURE_LADDER', quickOr(ctx, '0.2', '0.05,0.2,0.5'));
        const metrics: Metric[] = [];
        for (const rate of rates) {
            const result = await drive({
                WF_MIX: 'saga:100',
                WF_START_RATE: '25',
                WF_FAILURE_RATE: String(rate)
            });
            refusePartial(result, `${this.name} f=${rate}`);
            for (const row of result.merged) {
                const finished = row.completed + row.failed + row.compensated;
                metrics.push(
                    ...rowMetrics(row, `f=${rate}/`),
                    {
                        name: `f=${rate}/compensated_ratio`,
                        value: ratio(row.compensated, finished),
                        unit: 'ratio',
                        direction: 'lower',
                        informational: true
                    },
                    {
                        name: `f=${rate}/failed_ratio`,
                        value: ratio(row.failed, finished),
                        unit: 'ratio',
                        direction: 'lower',
                        informational: true
                    },
                    {
                        name: `f=${rate}/task_attempts_per_run`,
                        value: ratio(row.taskAttempts ?? 0, finished),
                        unit: 'count',
                        direction: 'lower',
                        informational: true
                    }
                );
            }
        }
        return metrics;
    }
};

const definitionHotKey: Scenario = {
    name: 'workflow/definition-hotkey',
    description:
        'A high start rate over the default mix: every start reads one of five shared definition keys — the locality axis',
    async run() {
        const result = await drive({
            WF_START_RATE: process.env.INFRA_WF_HOT_RATE ?? '25',
            WF_DELAY_MS: '1000'
        });
        refusePartial(result, this.name);
        return [
            ...result.merged.flatMap((row) => rowMetrics(row, rung(row))),
            ...mechanismMetrics(result),
            ...timelineMetrics(result)
        ];
    }
};

export const workflowScenarios: Scenario[] = [
    throughputLadder,
    sleepingRuns,
    fanoutWidth,
    fanoutPool,
    signals,
    sagaFailure,
    definitionHotKey
];
