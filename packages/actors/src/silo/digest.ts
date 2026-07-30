/**
 * The mergeable half of `metrics()` — what a silo can hand a peer so that
 * the CLUSTER's numbers can be computed rather than guessed.
 *
 * `ActorMetricsSnapshot` is for a human: it carries derived percentiles,
 * message strings, and a window that only makes sense for the silo that
 * produced it. None of that adds up across silos — averaging p99s produces
 * a figure describing no call that ever happened, and summing two silos'
 * `windowMs` means nothing at all.
 *
 * A digest is for `clusterStats()`: additive counters, plus the raw
 * distributions the percentiles get re-derived from after they are summed.
 *
 * Deliberately dependency-free — types and pure folding, no plugin, no
 * silo, no cluster. That is what lets `cluster/stats.ts` import it without
 * dragging the metrics plugin into a bundle that never uses one, and what
 * lets a client (a dashboard merging a user-selected subset of silos) reuse
 * the exact same arithmetic instead of reimplementing the bucket layout.
 */
import type { ActorErrorKind } from '../errors';
import {
    HISTOGRAM_LAYOUT,
    digestSnapshot,
    mergeHistogramDigests,
    type HistogramDigest,
    type HistogramSnapshot
} from './histogram';

/**
 * How a failure is classified: a branded `ActorError`'s own kind, or
 * `'(unknown)'` for anything an actor method threw itself.
 *
 * Named and exported rather than widened to `string` so a consumer can
 * switch over it exhaustively — the whole point of counting kinds is that
 * they mean different things and lead to different actions.
 *
 * Lives here rather than in `metrics.ts` so that the aggregation side can
 * name it without importing the plugin.
 */
export type MetricsErrorKind = ActorErrorKind | '(unknown)';

/** Calls and failures for one actor type, or one `Type#method` pair. */
export interface CallDigest {
    calls: number;
    failed: number;
}

/** One failure, as `errors.recent` remembers it. */
export interface RecentActorError {
    /** Epoch-ms. Wall clock on purpose: this one is read as a timestamp. */
    at: number;
    type: string;
    method: string;
    kind: MetricsErrorKind;
    /** The message only — never args, never state. */
    message: string;
}

/**
 * One silo's mergeable metrics.
 *
 * Everything here is additive EXCEPT `windowMs` and `layout`, which are
 * facts about the reporting silo.
 */
export interface MetricsDigest {
    /** Digest wire version, separate from `SiloReport.v` so this half can
     *  evolve without touching the report envelope. */
    v: 1;
    /**
     * The bucket layout every histogram digest here is expressed in
     * (`HISTOGRAM_LAYOUT`). A peer on a different layout has its COUNTS
     * merged and its distributions dropped: bucket indices are not
     * comparable across layouts, and a wrong p99 is worse than an absent
     * one.
     */
    layout: string;
    /**
     * This silo's collection window, ms. NOT additive — each silo has its
     * own, and one silo having been `reset()` is the fact it carries. A
     * rate is derived by differencing polls, never by dividing by this.
     */
    windowMs: number;
    calls: { total: number; failed: number; streams: number };
    /** Null under `metrics({ histograms: false })`. */
    latency: HistogramDigest | null;
    queue: HistogramDigest | null;
    turn: HistogramDigest | null;
    storageLatency: HistogramDigest | null;
    errors: {
        /** Partial: a kind that has not happened is absent, not zero. */
        byKind: Partial<Record<MetricsErrorKind, number>>;
        /** Only when asked for. Message only — never args, never state. */
        recent?: RecentActorError[];
    };
    activations: { created: number; destroyed: number; byReason: Record<string, number> };
    storage: { loads: number; saves: number; clears: number; conflicts: number };
    /** Top rows by calls; the remainder folds into `'(other)'`, so the
     *  breakdown still sums to `calls.total`. */
    byType: Record<string, CallDigest>;
    byMethod: Record<string, CallDigest>;
}

/** The cluster-wide totals a set of digests folds into. */
export interface MergedMetrics {
    calls: { total: number; failed: number; streams: number };
    /** Re-derived from SUMMED buckets. Null when no contributor had any. */
    latencyMs: HistogramSnapshot | null;
    queueMs: HistogramSnapshot | null;
    turnMs: HistogramSnapshot | null;
    storageLatencyMs: HistogramSnapshot | null;
    errors: { byKind: Partial<Record<MetricsErrorKind, number>> };
    activations: { created: number; destroyed: number; byReason: Record<string, number> };
    storage: { loads: number; saves: number; clears: number; conflicts: number };
    byType: Record<string, CallDigest>;
    byMethod: Record<string, CallDigest>;
}

/** What folding one digest in achieved. */
export type MetricsFoldResult =
    /** Counters and distributions both. */
    | 'merged'
    /** A different bucket layout: counters in, distributions dropped. */
    | 'counts-only'
    /** Not a digest this build understands; nothing taken. */
    | 'rejected';

export interface MetricsAccumulator {
    add(digest: MetricsDigest | null | undefined): MetricsFoldResult;
    totals(): MergedMetrics;
}

const OTHER = '(other)';

/**
 * Fold silos' digests into one set of cluster-wide numbers.
 *
 * Distributions are collected and summed at the END rather than merged
 * pairwise, so the result does not depend on the order peers answered in.
 */
export function createMetricsAccumulator(): MetricsAccumulator {
    let total = 0;
    let failed = 0;
    let streams = 0;
    let created = 0;
    let destroyed = 0;
    let loads = 0;
    let saves = 0;
    let clears = 0;
    let conflicts = 0;
    const byReason: Record<string, number> = {};
    const byKind: Partial<Record<MetricsErrorKind, number>> = {};
    const byType: Record<string, CallDigest> = {};
    const byMethod: Record<string, CallDigest> = {};
    const latency: HistogramDigest[] = [];
    const queue: HistogramDigest[] = [];
    const turn: HistogramDigest[] = [];
    const storageLatency: HistogramDigest[] = [];

    const addCalls = (into: Record<string, CallDigest>, from: Record<string, CallDigest> | undefined): void => {
        for (const [key, value] of Object.entries(from ?? {})) {
            if (!value) continue;
            const bucket = (into[key] ??= { calls: 0, failed: 0 });
            bucket.calls += num(value.calls);
            bucket.failed += num(value.failed);
        }
    };

    return {
        add(digest) {
            if (!digest || digest.v !== 1) return 'rejected';
            total += num(digest.calls?.total);
            failed += num(digest.calls?.failed);
            streams += num(digest.calls?.streams);
            created += num(digest.activations?.created);
            destroyed += num(digest.activations?.destroyed);
            for (const [reason, n] of Object.entries(digest.activations?.byReason ?? {})) {
                byReason[reason] = (byReason[reason] ?? 0) + num(n);
            }
            loads += num(digest.storage?.loads);
            saves += num(digest.storage?.saves);
            clears += num(digest.storage?.clears);
            conflicts += num(digest.storage?.conflicts);
            for (const [kind, n] of Object.entries(digest.errors?.byKind ?? {})) {
                const key = kind as MetricsErrorKind;
                byKind[key] = (byKind[key] ?? 0) + num(n);
            }
            addCalls(byType, digest.byType);
            addCalls(byMethod, digest.byMethod);

            // Counters are comparable across layouts; buckets are not.
            if (digest.layout !== HISTOGRAM_LAYOUT) return 'counts-only';
            if (digest.latency) latency.push(digest.latency);
            if (digest.queue) queue.push(digest.queue);
            if (digest.turn) turn.push(digest.turn);
            if (digest.storageLatency) storageLatency.push(digest.storageLatency);
            return 'merged';
        },

        totals() {
            const merge = (parts: HistogramDigest[]): HistogramSnapshot | null =>
                parts.length === 0 ? null : digestSnapshot(mergeHistogramDigests(parts));
            return {
                calls: { total, failed, streams },
                latencyMs: merge(latency),
                queueMs: merge(queue),
                turnMs: merge(turn),
                storageLatencyMs: merge(storageLatency),
                errors: { byKind },
                activations: { created, destroyed, byReason },
                storage: { loads, saves, clears, conflicts },
                byType,
                byMethod
            };
        }
    };
}

/**
 * Keep the top `limit` entries by calls and fold the rest into
 * `'(other)'`, so a breakdown that is capped for the wire still SUMS to the
 * total it belongs to. A breakdown whose rows silently do not add up is
 * worse than one that says "and this much else".
 */
export function foldCallDigests(
    rows: Record<string, CallDigest>,
    limit: number
): Record<string, CallDigest> {
    const entries = Object.entries(rows);
    if (limit <= 0) return {};
    if (entries.length <= limit) return { ...rows };
    entries.sort((a, b) => b[1].calls - a[1].calls || (a[0] < b[0] ? -1 : 1));
    const kept: Record<string, CallDigest> = {};
    const other: CallDigest = { calls: 0, failed: 0 };
    for (const [key, value] of entries.slice(0, limit)) kept[key] = { ...value };
    for (const [, value] of entries.slice(limit)) {
        other.calls += value.calls;
        other.failed += value.failed;
    }
    const existing = kept[OTHER];
    if (existing) {
        existing.calls += other.calls;
        existing.failed += other.failed;
    } else {
        kept[OTHER] = other;
    }
    return kept;
}

/**
 * A counter from a digest, or 0.
 *
 * Clamped at zero as well as checked for finiteness: every field folded
 * here is a monotonic count, and these arrive from a peer over a wire. One
 * negative — from a buggy build, a truncated payload, or a hostile one —
 * would otherwise subtract from a cluster total and produce a figure that
 * cannot describe anything that happened. Same defence as the bucket
 * indices in `mergeHistogramDigests`, for the same reason.
 */
function num(value: unknown): number {
    return typeof value === 'number' && Number.isFinite(value) && value > 0 ? value : 0;
}
