/**
 * `clusterStats()` — one operator-facing read of the whole cluster.
 *
 * The fan-out rides the EXISTING internal mount as a reserved symbol,
 * `$sigx:host#stats`, rather than a new route. That is not a shortcut: it
 * inherits the per-request HMAC guard, the envelope, the wire codec, the
 * body cap and the error mapping, so there is no second authentication path
 * to get wrong — and an unauthenticated second mount that reports your
 * topology is the single most likely way to get this wrong.
 *
 * It never throws because a peer is sick. A cluster-wide read you cannot get
 * during an incident is worthless, so an unreachable host is LISTED, and
 * `partial` says the totals are a lower bound.
 */
import { isActorError } from '../errors';
import { reminderShardKeys } from '../host/reminder-shards';
import { createMetricsAccumulator, type MergedMetrics, type MetricsDigest } from '../host/digest';
import type { HealthReport } from '../host/app';
import type { ActivationInfo, HostStats } from '../types';
import {
    addCounters,
    createCounters,
    type ClusterCounters,
    type ClusterCounterTotals
} from './counters';
import type { ClusterPlacement } from './placement';
import type { HostDescriptor, HostStatus } from './types';

/** The actor type reserved for host-level internal calls. */
export const HOST_STATS_TYPE = '$sigx:host';
export const HOST_STATS_METHOD = 'stats';
/** The wire symbol the internal mount answers. */
export const HOST_STATS_SYMBOL = `${HOST_STATS_TYPE}#${HOST_STATS_METHOD}`;

/** One host's operational snapshot — what a peer answers with. */
export interface HostReport {
    /** Payload version. A rolling deploy IS a mixed-version cluster, and the
     *  ops tool has to keep working during the deploy that broke things. */
    v: 1;
    hostId: string;
    epoch: number;
    address: string;
    /** Wire status, plus `'fenced'`. */
    status: HostStatus | 'fenced';
    /** Live gauges: activations, queued turns, per-type counts. */
    stats: HostStats;
    counters: ClusterCounters;
    /** Reminder shards this host owns under ITS OWN view (`p0`…`p15`). */
    reminderShards: string[];
    /** Since `placement.start()`, ms. 0 before start. */
    uptimeMs: number;
    /**
     * Transport names this host is configured with, in chain order. Makes a
     * half-rolled-out transport visible at a glance: during the deploy that
     * introduces one, hosts disagree here, and that disagreement is the
     * whole story.
     */
    transports?: string[];
    /**
     * The actor types this host registers — `HostDescriptor.types` on the
     * ops channel (#212), so a heterogeneous cluster's registration split
     * is visible in `clusterStats()`. Absent from a peer that predates the
     * field.
     */
    types?: string[];
    meta?: Record<string, string>;
    /**
     * This host's mergeable metrics, when it has `metrics()` attached and
     * the collector asked for them.
     *
     * Absent means "no numbers from here", never "zero": a host with no
     * metrics plugin, or a peer on a build that predates this field, both
     * answer without it. `totals.metrics.hosts` is the denominator that
     * makes the difference visible.
     */
    metrics?: MetricsDigest;
    /**
     * This host's readiness, so a dashboard shows EVERY host's health
     * rather than only the one it happens to be polling. Absent from a peer
     * that predates this field.
     */
    health?: HealthReport;
    /**
     * Live activations, most queued turns first — only under `detail`.
     *
     * Off by default because the walk is O(activations) on the answering
     * host, and because actor KEYS are the one field on this wire that can
     * be personal data.
     */
    activations?: ActivationInfo[];
}

/**
 * What a peer is ASKED to include in its report.
 *
 * Rides as the single argument to `$sigx:host#stats`. ABSENT — which is
 * what an older collector sends — means exactly the payload this endpoint
 * answered before any of these fields existed, so upgrading the collector
 * is what grows the traffic, not upgrading a peer.
 *
 * Every field is a request, not an instruction: the RESPONDER clamps them.
 * The wire is HMAC-guarded, but `activations: 1e9` on a host with millions
 * of actors is a CPU and memory amplifier, and a guard that trusts the
 * caller is not a guard.
 */
export interface HostReportOptions {
    /** Include the metrics digest. Default false. */
    metrics?: boolean;
    /** Actor-type rows in the digest before `'(other)'`. Default 32. */
    types?: number;
    /** `Type#method` rows, same fold. Default 32. */
    methods?: number;
    /** Recent failures to include. Default 0. */
    errors?: number;
    /** Live activations to list, most queued turns first. Default 0. */
    activations?: number;
}

/** How much detail `clusterStats()` should ask each host for. */
export interface ClusterStatsDetail {
    /**
     * Live activations per host. Default 20 with `detail: true`; 0 omits.
     * Hard-capped by the responder.
     */
    activations?: number;
    /** Recent failures per host. Default 8; 0 omits. */
    errors?: number;
    /** Widen the digest's per-type / per-method rows. */
    types?: number;
    methods?: number;
    /**
     * Ask only these hosts for the expensive parts; everyone else answers
     * the cheap report.
     *
     * Drill-down is almost always ONE host, and asking N of them for actor
     * lists at 1 Hz is the cost that actually matters.
     */
    hosts?: readonly string[];
}

/** Cluster-wide metrics, plus how much of the cluster they cover. */
export interface ClusterMetricsTotals extends MergedMetrics {
    /**
     * Hosts that CONTRIBUTED a digest — the denominator.
     *
     * Less than `totals.hosts` means every number here is a lower bound: a
     * host with no `metrics()`, or one on an older build. Published rather
     * than inferred, because totals that quietly cover half the fleet look
     * exactly like totals that cover all of it.
     */
    hosts: number;
    /** Hosts whose bucket layout differed: counts included, distributions not. */
    layoutMismatch: string[];
}

export type ClusterStatsFailureReason =
    /** No listener, connection refused, DNS. */
    | 'unreachable'
    /** Did not answer inside `timeoutMs`. */
    | 'timeout'
    /** 403 — secret mismatch, e.g. mid-rotation. */
    | 'unauthorized'
    /** 404 — the peer predates this build and has no stats symbol. */
    | 'unsupported'
    | 'error';

export interface ClusterStatsFailure {
    hostId: string;
    address: string;
    reason: ClusterStatsFailureReason;
    message: string;
}

export interface ClusterStatsReport {
    /** Epoch-ms the report was assembled. */
    at: number;
    /** The collecting host. */
    from: string;
    /** The collector's view this fan-out was taken against. */
    view: { version: number; size: number; active: number };
    /** Hosts that answered, including the collector (answered in-process). */
    hosts: HostReport[];
    unreachable: ClusterStatsFailure[];
    /** True when a member did not answer — every total is then a LOWER BOUND. */
    partial: boolean;
    totals: {
        hosts: number;
        activations: number;
        queued: number;
        perType: Record<string, number>;
        counters: ClusterCounterTotals;
        /**
         * Cluster-wide call, error, storage and latency numbers.
         *
         * Null when NO host carried a digest — "there is no `metrics()`
         * anywhere" rather than a wall of zeroes, which would read as a
         * cluster serving no traffic.
         */
        metrics: ClusterMetricsTotals | null;
        /** Readiness across the fleet. `unknown` reported no health. */
        health: { ready: number; notReady: number; fatal: number; unknown: number };
    };
    /**
     * `p0`…`p15` → the hosts CLAIMING each shard, built from the reports
     * themselves rather than recomputed by the collector.
     *
     * Exactly one claimant is the healthy case. Two means views disagree
     * (safe — the per-shard etag CAS keeps reminders at-most-once — but
     * worth knowing). An EMPTY array means no reachable host is ticking that
     * shard, which is how "only 16 hosts ever do reminder work" stops being
     * invisible.
     */
    reminderShards: Record<string, string[]>;
}

export interface ClusterStatsOptions {
    /**
     * Ask each host for its actor list and recent errors as well.
     *
     * The metrics digest is NOT behind this — cluster-wide totals are the
     * point of the fan-out, so every poll carries it. `detail` is for the
     * per-host drill-down, which is expensive in a way the totals are not.
     */
    detail?: boolean | ClusterStatsDetail;
    /** Per-peer budget, ms. Default 2000 — a hung peer must not hang ops. */
    timeoutMs?: number;
    /** Cap on peers queried at once. Default 16, so N=100 is 7 waves. */
    concurrency?: number;
    /** Abort the whole fan-out. */
    signal?: AbortSignal;
}

const DEFAULT_TIMEOUT_MS = 2000;
const DEFAULT_CONCURRENCY = 16;

/**
 * Collect every host's report over the internal transport.
 *
 * `placement` is the one from `cluster(...).placement`. The collector
 * answers for itself in process — a host behind a load balancer often
 * cannot reach its own advertised address, and a loopback hop could fail
 * for reasons that say nothing about the host.
 */
export async function clusterStats(
    placement: ClusterPlacement,
    options: ClusterStatsOptions = {}
): Promise<ClusterStatsReport> {
    const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    const concurrency = Math.max(1, options.concurrency ?? DEFAULT_CONCURRENCY);
    const view = placement.view();
    const self = placement.identity.hostId;
    const request = resolveRequest(options.detail);
    const wanted = detailHosts(options.detail);
    /** What one host is asked for: everyone gets the digest, some get more. */
    const requestFor = (hostId: string): HostReportOptions =>
        !wanted || wanted.has(hostId) ? request : { metrics: true };

    const hosts: HostReport[] = [placement.report(requestFor(self))];
    const unreachable: ClusterStatsFailure[] = [];

    // Every status is queried, `leaving` included: a draining host's
    // remaining activations are precisely what an operator wants to see.
    const peers = view.hosts.filter((s) => s.hostId !== self);
    let next = 0;
    const worker = async (): Promise<void> => {
        for (;;) {
            const index = next++;
            const peer = peers[index];
            if (!peer) return;
            try {
                hosts.push(
                    await placement.peerReport(
                        peer,
                        timeoutMs,
                        options.signal,
                        requestFor(peer.hostId)
                    )
                );
            } catch (error) {
                unreachable.push(classify(peer, error));
            }
        }
    };
    await Promise.all(
        Array.from({ length: Math.min(concurrency, peers.length) }, () => worker())
    );

    // Stable output regardless of which peer answered first.
    hosts.sort((a, b) => (a.hostId < b.hostId ? -1 : a.hostId > b.hostId ? 1 : 0));
    unreachable.sort((a, b) => (a.hostId < b.hostId ? -1 : a.hostId > b.hostId ? 1 : 0));

    const perType: Record<string, number> = {};
    let activations = 0;
    let queued = 0;
    let counters = createCounters();
    const reminderShards: Record<string, string[]> = {};
    const metrics = createMetricsAccumulator();
    let metricsHosts = 0;
    const layoutMismatch: string[] = [];
    const health = { ready: 0, notReady: 0, fatal: 0, unknown: 0 };
    for (const report of hosts) {
        activations += report.stats.activations;
        queued += report.stats.queued;
        for (const [type, count] of Object.entries(report.stats.perType)) {
            perType[type] = (perType[type] ?? 0) + count;
        }
        counters = addCounters(counters, report.counters);
        for (const shard of report.reminderShards) {
            (reminderShards[shard] ??= []).push(report.hostId);
        }
        // A host with no `metrics()`, or one on a build that predates the
        // digest, simply does not contribute — which is why the count of
        // contributors is published rather than assumed to be all of them.
        const folded = metrics.add(report.metrics);
        if (folded !== 'rejected') metricsHosts++;
        // A different bucket layout still has usable COUNTERS; only its
        // distribution is incomparable. Dropping the host entirely would be
        // a worse lie than dropping its percentiles.
        if (folded === 'counts-only') layoutMismatch.push(report.hostId);
        if (!report.health) health.unknown++;
        else if (report.health.fatal) health.fatal++;
        else if (report.health.ready) health.ready++;
        else health.notReady++;
    }
    // The FULL key space, not just the shards someone reported: a shard
    // whose only owner is unreachable has to appear as empty. A missing key
    // reads as "not measured", an empty one as "nothing is ticking this" —
    // and the second is an incident.
    for (const shard of reminderShardKeys()) reminderShards[shard] ??= [];

    return {
        at: Date.now(),
        from: self,
        view: {
            version: view.version,
            size: view.hosts.length,
            active: view.hosts.filter((s) => s.status === 'active').length
        },
        hosts,
        unreachable,
        partial: unreachable.length > 0,
        totals: {
            hosts: hosts.length,
            activations,
            queued,
            perType,
            counters,
            metrics:
                metricsHosts === 0
                    ? null
                    : { ...metrics.totals(), hosts: metricsHosts, layoutMismatch },
            health
        },
        reminderShards
    };
}

/** Defaults for `detail: true` — enough to drill into, cheap enough to poll. */
const DETAIL_ACTIVATIONS = 20;
const DETAIL_ERRORS = 8;

/** Turn the caller's `detail` into one wire request. */
function resolveRequest(detail: ClusterStatsOptions['detail']): HostReportOptions {
    // The digest always travels: it is what makes `totals` mean anything,
    // and it is the cheap part.
    const base: HostReportOptions = { metrics: true };
    if (!detail) return base;
    if (detail === true) return { ...base, activations: DETAIL_ACTIVATIONS, errors: DETAIL_ERRORS };
    return {
        ...base,
        activations: detail.activations ?? DETAIL_ACTIVATIONS,
        errors: detail.errors ?? DETAIL_ERRORS,
        ...(detail.types === undefined ? {} : { types: detail.types }),
        ...(detail.methods === undefined ? {} : { methods: detail.methods })
    };
}

/** Which hosts get the expensive parts, or null for "all of them". */
function detailHosts(detail: ClusterStatsOptions['detail']): Set<string> | null {
    if (!detail || detail === true || !detail.hosts) return null;
    return new Set(detail.hosts);
}

function classify(peer: HostDescriptor, error: unknown): ClusterStatsFailure {
    const message = (error as Error)?.message ?? String(error);
    const status = (error as { status?: number }).status;
    let reason: ClusterStatsFailureReason = 'error';
    if (isActorError(error) && error.kind === 'unreachable') {
        reason = 'unreachable';
    } else if (
        (error as Error)?.name === 'TimeoutError' ||
        (error as Error)?.name === 'AbortError' ||
        (isActorError(error) && error.kind === 'call-timeout')
    ) {
        reason = 'timeout';
    } else if (status === 403) {
        reason = 'unauthorized';
    } else if (status === 404) {
        // A peer that predates the stats symbol resolves it to null.
        reason = 'unsupported';
    }
    return { hostId: peer.hostId, address: peer.address, reason, message };
}
