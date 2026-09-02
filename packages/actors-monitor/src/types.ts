/**
 * The normalized view a monitor renders, and the seam that produces it.
 *
 * NOTHING in this package may import a renderer. A dashboard is a rendering
 * of this shape; `@sigx/actors-cli` is one renderer and
 * `@sigx/actors-dashboard` is another, and both import this package
 * unchanged rather than reimplement the awkward parts (rate derivation,
 * `partial` handling, reconciling the embedded and HTTP shapes).
 *
 * The awkward parts are the point. Core reports cumulative counters with no
 * windowing, splits a cross-host call across two hosts on purpose, and marks
 * an incomplete fan-out rather than failing it. Every consumer needs the
 * same handling of all three, so it lives here once.
 */
import type {
    ActivationInfo,
    ActorMetricsSnapshot,
    HealthStatus,
    MetricsDigest,
    HostStats
} from '@sigx/actors/host';
import type {
    ClusterCounterTotals,
    ClusterMetricsTotals,
    HostReport
} from '@sigx/actors/cluster';
import type { HealthReport } from '@sigx/actors/host';
import type { SocketStatsSnapshot } from '@sigx/actors/server';

/** One host, however we reached it. */
export interface HostView {
    hostId: string;
    address: string;
    /** `'active' | 'joining' | 'leaving' | 'fenced'`, or `'unknown'` from a
     *  single-node host that has no membership to report one. */
    status: string;
    /** Since the host started, ms. */
    uptimeMs: number;
    stats: HostStats;
    /** Absent for a host reached without cluster reporting. */
    counters: ClusterCounterTotals | null;
    /** Reminder shards this host claims under ITS OWN view. */
    reminderShards: readonly string[];
    /** This host's membership view version — a spread across hosts means
     *  the view has not converged, which explains any other disagreement. */
    membershipVersion: number | null;
    /** Transport names in chain order; a disagreement here is a half-rolled
     *  transport deploy, which is usually the whole story. */
    transports: readonly string[] | null;
    /**
     * The free-form placement hints this host was started with
     * (`PlacementOptions.meta`), or null when it published none.
     *
     * `node` is the key with an agreed meaning: the machine the host runs on,
     * which a Kubernetes chart fills from `spec.nodeName`. It exists because
     * `3/3 replicas` reads the same whether the fleet is spread across three
     * nodes or packed onto one 2-vCPU box — and finding out otherwise meant
     * joining `kubectl top pods` against `kubectl get pods -o wide` by hand
     * (#51).
     */
    meta: Readonly<Record<string, string>> | null;
    /**
     * THIS host's own metrics, when it reported them.
     *
     * Null is "this host said nothing", not "this host did nothing" — an
     * uninstrumented host and an idle one are different findings, and a
     * panel that renders the first as zeroes claims the second.
     */
    metrics: MetricsDigest | null;
    /** THIS host's readiness. Null when it reported none. */
    health: HealthReport | null;
    /** THIS host's live actors — only present under a detail poll. */
    activations: readonly ActivationInfo[] | null;
    /**
     * THIS host's socket sessions (#166), when it published them — the
     * `sockets` ops section an app registers from `socketStats()`.
     *
     * Null is "this host said nothing", the same rule as `metrics`: a host
     * with no socket mount and one with no open sessions are different
     * findings. Today only the POLLED host can say anything — the cluster
     * fan-out carries no socket digest yet — so a peer's is always null,
     * and a renderer must not read that as "no sockets there".
     */
    sockets: SocketStatsSnapshot | null;
}

export interface ClusterView {
    /** The host that assembled the fan-out. */
    from: string;
    view: { version: number; size: number; active: number };
    totals: {
        hosts: number;
        activations: number;
        queued: number;
        perType: Record<string, number>;
        counters: ClusterCounterTotals;
        /**
         * Cluster-wide calls, errors, storage and latency — with `hosts` as
         * the denominator. Null when nothing anywhere is instrumented.
         */
        metrics: ClusterMetricsTotals | null;
        /** Readiness across the fleet. */
        health: { ready: number; notReady: number; fatal: number; unknown: number };
    };
    /** `p0`…`p15` → the hosts CLAIMING each shard. Two claimants means views
     *  diverged; an empty list means nothing is ticking that shard. */
    reminderShards: Record<string, readonly string[]>;
    unreachable: readonly {
        hostId: string;
        address: string;
        reason: string;
        message: string;
    }[];
}

export interface MonitorSnapshot {
    /** Epoch-ms this snapshot was taken. */
    at: number;
    hosts: readonly HostView[];
    /** Null on a single-node host — not an error, just no cluster. */
    cluster: ClusterView | null;
    /**
     * The POLLED host's own metrics — not the cluster's.
     *
     * Kept distinct from `cluster.totals.metrics` because the two are
     * different facts and the whole complaint behind #121 was a dashboard
     * printing them adjacently with nothing saying which was which. Null
     * when `metrics()` is not attached, which is real and common.
     */
    metrics: ActorMetricsSnapshot | null;
    /** Null when the source cannot see them (`ops({ activations: 0 })`). */
    activations: readonly ActivationInfo[] | null;
    health: HealthStatus | null;
    /**
     * A member did not answer, so every total is a LOWER BOUND.
     *
     * Surfaced all the way to the UI rather than smoothed over: a dashboard
     * that silently under-reports during a partial outage is worse than one
     * that says it is under-reporting, because the numbers still look
     * plausible.
     */
    partial: boolean;
}

/** How much a single poll should ask for. */
export interface SnapshotOptions {
    /**
     * Ask for per-host actor lists and recent errors as well.
     *
     * Off for the list view and on only while a drill-down is open: the
     * walk is O(activations) on every host at once, so a dashboard that
     * always asked would make the cluster pay for a panel nobody is
     * looking at.
     */
    detail?: boolean;
    /** Limit the expensive parts to one host — what a drill-down wants. */
    hostId?: string;
}

export interface MonitorSource {
    readonly kind: 'embedded' | 'http';
    /** Shown in the UI so it is never ambiguous WHAT is being watched. */
    readonly label: string;
    snapshot(signal?: AbortSignal, options?: SnapshotOptions): Promise<MonitorSnapshot>;
    close(): Promise<void>;
}

/** Fold a `HostReport` (cluster wire shape) into the normalized view. */
export function hostViewFromReport(report: HostReport): HostView {
    const { membershipVersion, status, ...totals } = report.counters;
    return {
        hostId: report.hostId,
        address: report.address,
        status,
        uptimeMs: report.uptimeMs,
        stats: report.stats,
        counters: totals,
        reminderShards: report.reminderShards,
        membershipVersion,
        transports: report.transports ?? null,
        meta: report.meta ?? null,
        metrics: report.metrics ?? null,
        health: report.health ?? null,
        activations: report.activations ?? null,
        // The wire shape has no socket digest yet (#166 PR 2 — the
        // aggregation reads only `'metrics'`); the polled host's own is
        // attached afterwards by `withSockets`.
        sockets: null
    };
}

/**
 * Attach the polled host's socket section to ITS row in the host list.
 *
 * The ops body carries `ops.sockets` for the host that answered, and the
 * cluster report says which host that was (`from`). Every other row keeps
 * null. Shared by both sources so the rule — the section belongs to exactly
 * one host, never summed, never guessed onto another — is made once.
 */
export function withSockets(
    hosts: readonly HostView[],
    hostId: string,
    sockets: SocketStatsSnapshot | null
): HostView[] {
    return hosts.map((host) => (host.hostId === hostId && sockets ? { ...host, sockets } : host));
}
