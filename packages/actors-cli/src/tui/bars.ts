/**
 * The two shapes core produces that upstream has no opinion about: a
 * `HistogramSnapshot`, and the reminder-shard claim map.
 *
 * Everything generic that used to live in this directory — the sparkline,
 * the table layout, the cell measurement, the bar maths — is now
 * `@sigx/terminal` 0.11's, upstreamed via signalxjs/terminal#103. What is
 * left here is only what is about ACTORS: the percentile triple, and the
 * fact that a reminder shard with no claimant is an incident while a shard
 * with two is merely a divergence.
 */
import { commonScale, type BarChartItem, type StatusCell } from '@sigx/terminal';
import type { HistogramSnapshot } from '@sigx/actors/host';

/**
 * A histogram as three comparable bars.
 *
 * `null` for an absent or empty histogram rather than three zeroed bars,
 * because `BarChart` draws a `null` as "no reading" while a row of zeroes
 * asserts "we measured, and it was fast". `metrics({ histograms: false })`
 * and a host that has served nothing are both the former.
 */
export function percentileItems(snapshot: HistogramSnapshot | null): BarChartItem[] {
    const empty = !snapshot || snapshot.count === 0;
    return [
        { label: 'p50', value: empty ? null : snapshot.p50Ms },
        { label: 'p90', value: empty ? null : snapshot.p90Ms },
        { label: 'p99', value: empty ? null : snapshot.p99Ms }
    ];
}

/**
 * One scale for a set of histograms, so latency, queue and turn are read
 * against a single axis — the comparison IS the diagnosis, and auto-scaling
 * each row would make a 12µs queue wait and a 47ms turn draw identically.
 *
 * Every percentile is offered to `commonScale`, not just p99: the scale has
 * to cover the tallest bar actually drawn.
 */
export function histogramScale(snapshots: readonly (HistogramSnapshot | null)[]): number {
    const values: (number | null)[] = [];
    for (const snapshot of snapshots) {
        if (!snapshot || snapshot.count === 0) continue;
        values.push(snapshot.p50Ms, snapshot.p90Ms, snapshot.p99Ms);
    }
    return commonScale(values);
}

/**
 * The reminder shard map as status cells.
 *
 * The three states mean genuinely different things and none of them is a
 * count you would read off a number:
 *
 *   one claimant   healthy
 *   none           NOTHING is ticking that shard — those reminders are not
 *                  firing, and nothing else in the system surfaces it
 *   two or more    views have diverged; safe (the per-shard etag CAS keeps
 *                  delivery at-most-once) but worth knowing
 *
 * Sorted numerically by index rather than lexically, so `p10` does not sit
 * between `p1` and `p2`.
 */
export function shardCells(shards: Record<string, readonly string[]>): StatusCell[] {
    return Object.keys(shards)
        .sort((a, b) => shardIndex(a) - shardIndex(b))
        .map((shard) => {
            const claimants = shards[shard] ?? [];
            return {
                label: shard,
                tone: claimants.length === 0 ? 'danger' : claimants.length === 1 ? 'ok' : 'warn',
                detail: claimants.join(' ')
            } satisfies StatusCell;
        });
}

/** `p12` → 12; anything unparseable sorts last but stays stable. */
function shardIndex(shard: string): number {
    const digits = /^p(\d+)$/.exec(shard);
    return digits ? Number(digits[1]) : Number.MAX_SAFE_INTEGER;
}

/** Shards nothing is ticking — the finding worth alerting on. */
export function unclaimedShards(shards: Record<string, readonly string[]>): string[] {
    return Object.entries(shards)
        .filter(([, claimants]) => claimants.length === 0)
        .map(([shard]) => shard)
        .sort((a, b) => shardIndex(a) - shardIndex(b));
}

/** Shards claimed by more than one host — views have diverged. */
export function splitShards(shards: Record<string, readonly string[]>): string[] {
    return Object.entries(shards)
        .filter(([, claimants]) => claimants.length > 1)
        .map(([shard]) => shard)
        .sort((a, b) => shardIndex(a) - shardIndex(b));
}
