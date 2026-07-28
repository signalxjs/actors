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
import type { SiloStats } from '../types';
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
    meta?: Record<string, string>;
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

    const silos: SiloReport[] = [placement.report()];
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
                silos.push(await placement.peerReport(peer, timeoutMs, options.signal));
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
        totals: { silos: silos.length, activations, queued, perType, counters },
        reminderShards
    };
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
