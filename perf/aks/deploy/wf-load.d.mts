/**
 * Types for `wf-load.mjs` — plain JS because `testenv.mjs` imports it with
 * no build step; declared here so `benchmarks/src/scenarios/workflow.ts`
 * (which IS typechecked) sees a shape and not `any`. The same arrangement
 * as `ws-load.d.mts`, for the same reason: the metric mapping is the part
 * most likely to be silently wrong.
 */

export interface Percentile {
    count: number;
    p50: number;
    p90: number;
    p99: number;
    max: number;
}

export interface WfStuck {
    sleeping: number;
    waiting: number;
    blocked: number;
    running: number;
    other: number;
    total: number;
}

/** One rate rung, summed across the Job's pods. */
export interface WfLoadRow {
    rate: number;
    pods: number;
    arrival: string;
    mix: string;
    knobs: Record<string, unknown>;
    durationMs: number;
    drainMs: number;
    started: number;
    startFailures: number;
    startsDeferred: number;
    completed: number;
    failed: number;
    compensated: number;
    cancelled: number;
    childRuns: number;
    completedUnreported: number;
    droppedEvents: number;
    unknownEvents: number;
    /** start() errored at the client but the run's completion arrived. */
    startFailedButRan: number;
    sweepUnpollable: number;
    signalsSent: number;
    signalsSkipped: number;
    signalFailures: number;
    stuck: WfStuck;
    byTemplate: Record<string, Record<string, number>>;
    failedByError: Record<string, number>;
    /** From ONE pod — percentiles do not merge. */
    latencyMs: Record<string, Percentile | null> | null;
    observedMs: Record<string, Percentile | null> | null;
    startMs: Percentile | null;
    deferredMs: Percentile | null;
    latencyFromPods: number;
    /** Engine sums, read from the aggregator by one pod. */
    transitions?: number | null;
    timersFired?: number | null;
    remindersFired?: number | null;
    wakesFallback?: number | null;
    wakesLost?: number | null;
    wakesStale?: number | null;
    signalsDelivered?: number | null;
    signalsBuffered?: number | null;
    signalsLate?: number | null;
    signalTimeouts?: number | null;
    taskAttempts?: number | null;
    taskFailures?: number | null;
    compensations?: number | null;
    nodeMs?: Record<string, Percentile | null>;
    wakeLagMs?: Percentile | null;
    errors: { total: number; byKind: Record<string, number> };
    runsStartedPerSec: number;
    runsCompletedPerSec: number;
    transitionsPerSec: number | null;
    /** `rate × pods` — what the fleet was offered (#380). */
    offeredRate: number;
    /** The generator pods' own CPU over the rung, summed; null until every pod reports it. */
    generatorCpuMs: number | null;
    partial?: boolean;
}

export interface TimelineHost {
    pod: string;
    cpuM: number;
    memBytes: number;
}

export interface TimelineRedis {
    /** Cumulative user+sys CPU seconds — a rate needs two samples. */
    cpuS: number;
    /** Each null when INFO did not carry a finite value for it. */
    opsPerSec: number | null;
    memBytes: number | null;
    clients: number | null;
}

/** One poll of the fleet while the Job ran (#380). */
export interface TimelineSample {
    /** ms since the poll loop started — after the Job was applied and the quiet-fleet wait ended */
    t: number;
    hosts: TimelineHost[];
    redis: TimelineRedis | null;
    activations: number | null;
    queued: number;
}

/** What a timeline reduces to; every field absent when it cannot be known. */
export interface TimelinePeaks {
    hostCpuPeakM?: number;
    hostCpuPeakRatio?: number;
    hostMemPeakBytes?: number;
    redisCpuPeakRatio?: number;
    redisOpsPerSecPeak?: number;
    redisMemEndBytes?: number;
}

export interface WfLoadResult {
    job: string;
    pods: string[];
    rows: unknown[];
    merged: WfLoadRow[];
    hosts: number;
    /** Sampled during the run; `null` when no host reported a gauge. */
    peakActivations: number | null;
    /** Queued turns per host pod after the Job finished. */
    queuedAfter: Record<string, number>;
    samples: number;
    /** `ops.workflow` + `cluster/*` deltas; empty unless `countersTrustworthy`. */
    delta: Record<string, number>;
    countersTrustworthy: boolean;
    partial: boolean;
    /** Hosts whose transport chain includes tcp at the end of the run. */
    tcpHosts: number;
    timeline: TimelineSample[];
    peaks: TimelinePeaks;
    hostCpuLimitM: number | null;
    /** Container restarts on host pods present at both ends of the run; null when unobservable. */
    restartsDuringRun: number | null;
    /** Host pods that appeared during the run (a replaced victim); null when unobservable. */
    podsReplaced: number | null;
    chaos: { kind: 'owner-kill'; pod: string; atMs: number } | null;
}

export interface WfLoadOptions {
    context: string;
    namespace: string;
    chartDir: string;
    release?: string;
    imageRepository: string;
    imageTag: string;
    workload: string;
    values?: Record<string, unknown>;
    onLog?: (line: string) => void;
    sampleIntervalMs?: number;
    timeoutMs?: number;
    /** A run starts only once the fleet's queued turns are at or under this. */
    quietQueued?: number;
    /** How long to wait for that before refusing. */
    quietTimeoutMs?: number;
}

export function runWfLoad(options: WfLoadOptions): Promise<WfLoadResult>;
export function mergeWfRows(rows: unknown[], options?: { partial?: boolean }): WfLoadRow[];
export function workflowTotals(
    kube: (args: string[], opts?: { allowFail?: boolean }) => string | null,
    namespace: string
): {
    hosts: number;
    pods: number;
    totals: Record<string, number>;
    hostsComplete: boolean;
    activations: number | null;
    queued: Record<string, number>;
    tcpHosts: number;
};
export function parseCpuMillis(text: string | null | undefined): number | null;
export function parseTopPods(text: string | null | undefined): TimelineHost[];
export function parseRedisInfo(text: string | null | undefined): TimelineRedis | null;
export function timelinePeaks(
    timeline: TimelineSample[],
    options: { hostCpuLimitM: number | null }
): TimelinePeaks;
