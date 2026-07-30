/**
 * The normalized view a monitor renders, and the seam that produces it.
 *
 * NOTHING in `src/source/` or `src/model/` may import `@sigx/terminal`. A
 * dashboard is a rendering of this shape, and the terminal is one renderer;
 * a web one is a likely follow-up, and it should import these two
 * directories unchanged rather than reimplement the awkward parts (rate
 * derivation, `partial` handling, reconciling the embedded and HTTP shapes).
 *
 * The awkward parts are the point. Core reports cumulative counters with no
 * windowing, splits a cross-silo call across two silos on purpose, and marks
 * an incomplete fan-out rather than failing it. Every consumer needs the
 * same handling of all three, so it lives here once.
 */
import type {
    ActivationInfo,
    ActorMetricsSnapshot,
    HealthStatus,
    MetricsDigest,
    SiloStats
} from '@sigx/actors/silo';
import type {
    ClusterCounterTotals,
    ClusterMetricsTotals,
    SiloReport
} from '@sigx/actors/cluster';
import type { HealthReport } from '@sigx/actors/silo';

/** One silo, however we reached it. */
export interface SiloView {
    siloId: string;
    address: string;
    /** `'active' | 'joining' | 'leaving' | 'fenced'`, or `'unknown'` from a
     *  single-node silo that has no membership to report one. */
    status: string;
    /** Since the silo started, ms. */
    uptimeMs: number;
    stats: SiloStats;
    /** Absent for a silo reached without cluster reporting. */
    counters: ClusterCounterTotals | null;
    /** Reminder shards this silo claims under ITS OWN view. */
    reminderShards: readonly string[];
    /** This silo's membership view version — a spread across silos means
     *  the view has not converged, which explains any other disagreement. */
    membershipVersion: number | null;
    /** Transport names in chain order; a disagreement here is a half-rolled
     *  transport deploy, which is usually the whole story. */
    transports: readonly string[] | null;
    /**
     * THIS silo's own metrics, when it reported them.
     *
     * Null is "this silo said nothing", not "this silo did nothing" — an
     * uninstrumented silo and an idle one are different findings, and a
     * panel that renders the first as zeroes claims the second.
     */
    metrics: MetricsDigest | null;
    /** THIS silo's readiness. Null when it reported none. */
    health: HealthReport | null;
    /** THIS silo's live grains — only present under a detail poll. */
    activations: readonly ActivationInfo[] | null;
}

export interface ClusterView {
    /** The silo that assembled the fan-out. */
    from: string;
    view: { version: number; size: number; active: number };
    totals: {
        silos: number;
        activations: number;
        queued: number;
        perType: Record<string, number>;
        counters: ClusterCounterTotals;
        /**
         * Cluster-wide calls, errors, storage and latency — with `silos` as
         * the denominator. Null when nothing anywhere is instrumented.
         */
        metrics: ClusterMetricsTotals | null;
        /** Readiness across the fleet. */
        health: { ready: number; notReady: number; fatal: number; unknown: number };
    };
    /** `p0`…`p15` → the silos CLAIMING each shard. Two claimants means views
     *  diverged; an empty list means nothing is ticking that shard. */
    reminderShards: Record<string, readonly string[]>;
    unreachable: readonly {
        siloId: string;
        address: string;
        reason: string;
        message: string;
    }[];
}

export interface MonitorSnapshot {
    /** Epoch-ms this snapshot was taken. */
    at: number;
    silos: readonly SiloView[];
    /** Null on a single-node silo — not an error, just no cluster. */
    cluster: ClusterView | null;
    /**
     * The POLLED silo's own metrics — not the cluster's.
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
     * Ask for per-silo grain lists and recent errors as well.
     *
     * Off for the list view and on only while a drill-down is open: the
     * walk is O(activations) on every silo at once, so a dashboard that
     * always asked would make the cluster pay for a panel nobody is
     * looking at.
     */
    detail?: boolean;
    /** Limit the expensive parts to one silo — what a drill-down wants. */
    siloId?: string;
}

export interface MonitorSource {
    readonly kind: 'embedded' | 'http';
    /** Shown in the UI so it is never ambiguous WHAT is being watched. */
    readonly label: string;
    snapshot(signal?: AbortSignal, options?: SnapshotOptions): Promise<MonitorSnapshot>;
    close(): Promise<void>;
}

/** Fold a `SiloReport` (cluster wire shape) into the normalized view. */
export function siloViewFromReport(report: SiloReport): SiloView {
    const { membershipVersion, status, ...totals } = report.counters;
    return {
        siloId: report.siloId,
        address: report.address,
        status,
        uptimeMs: report.uptimeMs,
        stats: report.stats,
        counters: totals,
        reminderShards: report.reminderShards,
        membershipVersion,
        transports: report.transports ?? null,
        metrics: report.metrics ?? null,
        health: report.health ?? null,
        activations: report.activations ?? null
    };
}
