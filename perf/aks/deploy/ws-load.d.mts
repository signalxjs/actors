/**
 * Types for `ws-load.mjs`, which is plain JS because it is deployment
 * tooling that `testenv.mjs` imports — the same arrangement, and the same
 * reason, as `perf/app/deploy/post-fn.d.mts`.
 *
 * `benchmarks/src` IS typechecked, so the scenario importing this needs
 * declarations or the whole module arrives as `any` and the metric mapping
 * — the part most likely to be silently wrong — goes unchecked.
 */

/** One rung, summed across the Job's pods. */
export interface WsLoadRow {
    n: number;
    pods: number;
    mode: string;
    read: string;
    principal: string;
    connected: number;
    socketsExpected: number;
    connectFailures: number;
    deliveries: number;
    durationMs: number;
    publishes: number;
    publishFailures: number;
    subscriptionErrors: number;
    drops: number;
    seqMismatches: number;
    maxBufferedBytes: number;
    clientRssMb: number;
    /** From the PUBLISHING pod only — percentiles do not merge. */
    latencyMs: { count: number; p50: number; p90: number; p99: number; max: number } | null;
    /** How many pods contributed latency; 1 when a publisher ran. */
    latencyFromPods: number;
    /** `n` is per pod; this is the run's total. */
    totalConnections: number;
    deliveriesPerSec: number;
    /** A coalescing ratio, not a constant — see the 50 ms watch throttle. */
    deliveriesPerPublish: number;
    /** Present only when the run did not complete cleanly. */
    partial?: boolean;
}

export interface WsLoadResult {
    job: string;
    pods: string[];
    rows: unknown[];
    merged: WsLoadRow[];
    hosts: number;
    /** The `open` GAUGE sampled off the hosts while the job ran. */
    peakOpen: number;
    peakSubscriptions: number;
    samples: number;
    /**
     * Monotonic totals, after minus before: `ops.sockets` always, plus
     * `cluster/remoteWatches` / `cluster/coalescedWatches` /
     * `cluster/inboundWatches` when `watchesTrustworthy`.
     */
    delta: Record<string, number>;
    /**
     * Both snapshots saw EVERY host's cluster section. False means no
     * `cluster/*` key is present in `delta` at all — a delta is only as
     * trustworthy as both ends of it, and a short baseline overstates it
     * while a short end snapshot understates it.
     */
    watchesTrustworthy: boolean;
    partial: boolean;
}

export interface WsLoadOptions {
    context: string;
    namespace: string;
    chartDir: string;
    release?: string;
    imageRepository: string;
    imageTag: string;
    workload: string;
    /** `wsLoadgen.*` chart values without the prefix. An `image.*` key is
     *  REFUSED — the image is named by imageRepository/imageTag alone. */
    values?: Record<string, unknown>;
    onLog?: (message: string) => void;
    sampleIntervalMs?: number;
    timeoutMs?: number;
}

export function runWsLoad(options: WsLoadOptions): Promise<WsLoadResult>;

export function mergeRows(
    rows: readonly unknown[],
    options?: { partial?: boolean }
): WsLoadRow[];

export function socketTotals(
    kube: (args: string[], opts?: { allowFail?: boolean }) => string | null,
    namespace: string
): {
    hosts: number;
    /** `ops.sockets` per host, plus `cluster/*` watch counters, summed. */
    totals: Record<string, number>;
    /** Every pod answered with a cluster section; false ⇒ the sum is short. */
    watchesComplete: boolean;
};
