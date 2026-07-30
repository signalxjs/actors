/**
 * `clusterStats()` — one operator-facing read of the whole cluster.
 *
 * The fan-out rides the EXISTING internal mount as a reserved symbol,
 * `$sigx:silo#stats`, rather than a new route. That is not a shortcut: it
 * inherits the per-request HMAC guard, the envelope, the wire codec, the
 * body cap and the error mapping, so there is no second authentication path
 * to get wrong — and an unauthenticated second mount that reports your
 * topology is the single most likely way to get this wrong.
 *
 * It never throws because a peer is sick. A cluster-wide read you cannot get
 * during an incident is worthless, so an unreachable silo is LISTED, and
 * `partial` says the totals are a lower bound.
 */
import { isActorError } from '../errors';
import { reminderShardKeys } from '../silo/reminder-shards';
import { createMetricsAccumulator, type MergedMetrics, type MetricsDigest } from '../silo/digest';
import type { HealthReport } from '../silo/app';
import type { ActivationInfo, SiloStats } from '../types';
import {
    addCounters,
    createCounters,
    type ClusterCounters,
    type ClusterCounterTotals
} from './counters';
import type { ClusterPlacement } from './placement';
import type { SiloDescriptor, SiloStatus } from './types';

/** The actor type reserved for silo-level internal calls. */
export const SILO_STATS_TYPE = '$sigx:silo';
export const SILO_STATS_METHOD = 'stats';
/** The wire symbol the internal mount answers. */
export const SILO_STATS_SYMBOL = `${SILO_STATS_TYPE}#${SILO_STATS_METHOD}`;

/** One silo's operational snapshot — what a peer answers with. */
export interface SiloReport {
    /** Payload version. A rolling deploy IS a mixed-version cluster, and the
     *  ops tool has to keep working during the deploy that broke things. */
    v: 1;
    siloId: string;
    epoch: number;
    address: string;
    /** Wire status, plus `'fenced'`. */
    status: SiloStatus | 'fenced';
    /** Live gauges: activations, mailbox depth, per-type counts. */
    stats: SiloStats;
    counters: ClusterCounters;
    /** Reminder shards this silo owns under ITS OWN view (`p0`…`p15`). */
    reminderShards: string[];
    /** Since `placement.start()`, ms. 0 before start. */
    uptimeMs: number;
    /**
     * Transport names this silo is configured with, in chain order. Makes a
     * half-rolled-out transport visible at a glance: during the deploy that
     * introduces one, silos disagree here, and that disagreement is the
     * whole story.
     */
    transports?: string[];
    meta?: Record<string, string>;
    /**
     * This silo's mergeable metrics, when it has `metrics()` attached and
     * the collector asked for them.
     *
     * Absent means "no numbers from here", never "zero": a silo with no
     * metrics plugin, or a peer on a build that predates this field, both
     * answer without it. `totals.metrics.silos` is the denominator that
     * makes the difference visible.
     */
    metrics?: MetricsDigest;
    /**
     * This silo's readiness, so a dashboard shows EVERY silo's health
     * rather than only the one it happens to be polling. Absent from a peer
     * that predates this field.
     */
    health?: HealthReport;
    /**
     * Live activations, deepest mailbox first — only under `detail`.
     *
     * Off by default because the walk is O(activations) on the answering
     * silo, and because grain KEYS are the one field on this wire that can
     * be personal data.
     */
    activations?: ActivationInfo[];
}

/**
 * What a peer is ASKED to include in its report.
 *
 * Rides as the single argument to `$sigx:silo#stats`. ABSENT — which is
 * what an older collector sends — means exactly the payload this endpoint
 * answered before any of these fields existed, so upgrading the collector
 * is what grows the traffic, not upgrading a peer.
 *
 * Every field is a request, not an instruction: the RESPONDER clamps them.
 * The wire is HMAC-guarded, but `activations: 1e9` on a silo with millions
 * of grains is a CPU and memory amplifier, and a guard that trusts the
 * caller is not a guard.
 */
export interface SiloReportOptions {
    /** Include the metrics digest. Default false. */
    metrics?: boolean;
    /** Actor-type rows in the digest before `'(other)'`. Default 32. */
    types?: number;
    /** `Type#method` rows, same fold. Default 32. */
    methods?: number;
    /** Recent failures to include. Default 0. */
    errors?: number;
    /** Live activations to list, deepest mailbox first. Default 0. */
    activations?: number;
}

/** How much detail `clusterStats()` should ask each silo for. */
export interface ClusterStatsDetail {
    /**
     * Live activations per silo. Default 20 with `detail: true`; 0 omits.
     * Hard-capped by the responder.
     */
    activations?: number;
    /** Recent failures per silo. Default 8; 0 omits. */
    errors?: number;
    /** Widen the digest's per-type / per-method rows. */
    types?: number;
    methods?: number;
    /**
     * Ask only these silos for the expensive parts; everyone else answers
     * the cheap report.
     *
     * Drill-down is almost always ONE silo, and asking N of them for grain
     * lists at 1 Hz is the cost that actually matters.
     */
    silos?: readonly string[];
}

/** Cluster-wide metrics, plus how much of the cluster they cover. */
export interface ClusterMetricsTotals extends MergedMetrics {
    /**
     * Silos that CONTRIBUTED a digest — the denominator.
     *
     * Less than `totals.silos` means every number here is a lower bound: a
     * silo with no `metrics()`, or one on an older build. Published rather
     * than inferred, because totals that quietly cover half the fleet look
     * exactly like totals that cover all of it.
     */
    silos: number;
    /** Silos whose bucket layout differed: counts included, distributions not. */
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
    siloId: string;
    address: string;
    reason: ClusterStatsFailureReason;
    message: string;
}

export interface ClusterStatsReport {
    /** Epoch-ms the report was assembled. */
    at: number;
    /** The collecting silo. */
    from: string;
    /** The collector's view this fan-out was taken against. */
    view: { version: number; size: number; active: number };
    /** Silos that answered, including the collector (answered in-process). */
    silos: SiloReport[];
    unreachable: ClusterStatsFailure[];
    /** True when a member did not answer — every total is then a LOWER BOUND. */
    partial: boolean;
    totals: {
        silos: number;
        activations: number;
        queued: number;
        perType: Record<string, number>;
        counters: ClusterCounterTotals;
        /**
         * Cluster-wide call, error, storage and latency numbers.
         *
         * Null when NO silo carried a digest — "there is no `metrics()`
         * anywhere" rather than a wall of zeroes, which would read as a
         * cluster serving no traffic.
         */
        metrics: ClusterMetricsTotals | null;
        /** Readiness across the fleet. `unknown` reported no health. */
        health: { ready: number; notReady: number; fatal: number; unknown: number };
    };
    /**
     * `p0`…`p15` → the silos CLAIMING each shard, built from the reports
     * themselves rather than recomputed by the collector.
     *
     * Exactly one claimant is the healthy case. Two means views disagree
     * (safe — the per-shard etag CAS keeps reminders at-most-once — but
     * worth knowing). An EMPTY array means no reachable silo is ticking that
     * shard, which is how "only 16 silos ever do reminder work" stops being
     * invisible.
     */
    reminderShards: Record<string, string[]>;
}

export interface ClusterStatsOptions {
    /**
     * Ask each silo for its grain list and recent errors as well.
     *
     * The metrics digest is NOT behind this — cluster-wide totals are the
     * point of the fan-out, so every poll carries it. `detail` is for the
     * per-silo drill-down, which is expensive in a way the totals are not.
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
 * Collect every silo's report over the internal transport.
 *
 * `placement` is the one from `cluster(...).placement`. The collector
 * answers for itself in process — a silo behind a load balancer often
 * cannot reach its own advertised address, and a loopback hop could fail
 * for reasons that say nothing about the silo.
 */
export async function clusterStats(
    placement: ClusterPlacement,
    options: ClusterStatsOptions = {}
): Promise<ClusterStatsReport> {
    const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    const concurrency = Math.max(1, options.concurrency ?? DEFAULT_CONCURRENCY);
    const view = placement.view();
    const self = placement.identity.siloId;
    const request = resolveRequest(options.detail);
    const wanted = detailSilos(options.detail);
    /** What one silo is asked for: everyone gets the digest, some get more. */
    const requestFor = (siloId: string): SiloReportOptions =>
        !wanted || wanted.has(siloId) ? request : { metrics: true };

    const silos: SiloReport[] = [placement.report(requestFor(self))];
    const unreachable: ClusterStatsFailure[] = [];

    // Every status is queried, `leaving` included: a draining silo's
    // remaining activations are precisely what an operator wants to see.
    const peers = view.silos.filter((s) => s.siloId !== self);
    let next = 0;
    const worker = async (): Promise<void> => {
        for (;;) {
            const index = next++;
            const peer = peers[index];
            if (!peer) return;
            try {
                silos.push(
                    await placement.peerReport(
                        peer,
                        timeoutMs,
                        options.signal,
                        requestFor(peer.siloId)
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
    silos.sort((a, b) => (a.siloId < b.siloId ? -1 : a.siloId > b.siloId ? 1 : 0));
    unreachable.sort((a, b) => (a.siloId < b.siloId ? -1 : a.siloId > b.siloId ? 1 : 0));

    const perType: Record<string, number> = {};
    let activations = 0;
    let queued = 0;
    let counters = createCounters();
    const reminderShards: Record<string, string[]> = {};
    const metrics = createMetricsAccumulator();
    let metricsSilos = 0;
    const layoutMismatch: string[] = [];
    const health = { ready: 0, notReady: 0, fatal: 0, unknown: 0 };
    for (const report of silos) {
        activations += report.stats.activations;
        queued += report.stats.queued;
        for (const [type, count] of Object.entries(report.stats.perType)) {
            perType[type] = (perType[type] ?? 0) + count;
        }
        counters = addCounters(counters, report.counters);
        for (const shard of report.reminderShards) {
            (reminderShards[shard] ??= []).push(report.siloId);
        }
        // A silo with no `metrics()`, or one on a build that predates the
        // digest, simply does not contribute — which is why the count of
        // contributors is published rather than assumed to be all of them.
        const folded = metrics.add(report.metrics);
        if (folded !== 'rejected') metricsSilos++;
        // A different bucket layout still has usable COUNTERS; only its
        // distribution is incomparable. Dropping the silo entirely would be
        // a worse lie than dropping its percentiles.
        if (folded === 'counts-only') layoutMismatch.push(report.siloId);
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
            size: view.silos.length,
            active: view.silos.filter((s) => s.status === 'active').length
        },
        silos,
        unreachable,
        partial: unreachable.length > 0,
        totals: {
            silos: silos.length,
            activations,
            queued,
            perType,
            counters,
            metrics:
                metricsSilos === 0
                    ? null
                    : { ...metrics.totals(), silos: metricsSilos, layoutMismatch },
            health
        },
        reminderShards
    };
}

/** Defaults for `detail: true` — enough to drill into, cheap enough to poll. */
const DETAIL_ACTIVATIONS = 20;
const DETAIL_ERRORS = 8;

/** Turn the caller's `detail` into one wire request. */
function resolveRequest(detail: ClusterStatsOptions['detail']): SiloReportOptions {
    // The digest always travels: it is what makes `totals` mean anything,
    // and it is the cheap part.
    const base: SiloReportOptions = { metrics: true };
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

/** Which silos get the expensive parts, or null for "all of them". */
function detailSilos(detail: ClusterStatsOptions['detail']): Set<string> | null {
    if (!detail || detail === true || !detail.silos) return null;
    return new Set(detail.silos);
}

function classify(peer: SiloDescriptor, error: unknown): ClusterStatsFailure {
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
    return { siloId: peer.siloId, address: peer.address, reason, message };
}
