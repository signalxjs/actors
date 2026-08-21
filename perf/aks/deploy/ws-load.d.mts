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
    /** Subscribers deliberately made to stop reading (#182); 0 unless asked. */
    slowConnections: number;
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
    /**
     * Present when a pod re-ran this rung after its first attempt's
     * connect-failure rate crossed the generator's threshold (#222). The
     * counts above are the RETRY's; the discarded attempt's live only in
     * the raw per-pod rows, under `firstAttempt`.
     */
    retried?: boolean;
    /**
     * Present on the rung that ended the ladder: the generator's reason for
     * stopping. Absent everywhere ⇒ the ladder completed.
     */
    ladderStopped?: string;
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
    /**
     * Deepest HOST-side send buffer sampled during the run, summed across
     * hosts — `null` when no host could report one (#252). Never diffed
     * across snapshots: it is a gauge, and both ends of a healthy run read
     * 0 even if the middle was drowning.
     */
    peakBufferedBytes: number | null;
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
    /**
     * Hosts reporting `tcp` in their transport chain at the end of the run.
     * A `TRANSPORT=tcp` run where this is short of `hosts` measured a fleet
     * still partly on HTTP — the chain falls through per link, so that
     * produces a clean HTTP number wearing the tcp label (#203).
     */
    tcpHosts: number;
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
    /** Pods reporting `tcp` in their transport chain (#203). */
    tcpHosts: number;
};
