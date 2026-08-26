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
    partial?: boolean;
}

export interface WfLoadResult {
    job: string;
    pods: string[];
    rows: unknown[];
    merged: WfLoadRow[];
    hosts: number;
    /** Sampled during the run; `null` when no host reported a gauge. */
    peakActivations: number | null;
    samples: number;
    /** `ops.workflow` + `cluster/*` deltas; empty unless `countersTrustworthy`. */
    delta: Record<string, number>;
    countersTrustworthy: boolean;
    partial: boolean;
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
};
