/**
 * The terminal's rendering of the two shapes core produces that upstream has
 * no opinion about: a `HistogramSnapshot`, and the reminder-shard claim map.
 *
 * Everything generic that used to live in this directory — the sparkline, the
 * table layout, the cell measurement, the bar maths — is now
 * `@sigx/terminal` 0.11's, upstreamed via signalxjs/terminal#103. Everything
 * that was a JUDGEMENT about actors — a shard with no claimant is an incident
 * while a shard with two is merely a divergence; an empty histogram is "no
 * reading" and not three zeroes — is now `@sigx/actors-monitor`'s (#239), so
 * the web dashboard reaches the same verdicts rather than re-deriving them.
 *
 * What is left is the mapping between the two: monitor vocabulary in,
 * `@sigx/terminal` vocabulary out.
 */
import { commonScale, type BarChartItem, type StatusCell, type StatusTone } from '@sigx/terminal';
import {
    percentilePoints,
    shardStates,
    type AlertTone,
    type ShardState
} from '@sigx/actors-monitor';
import type { HistogramSnapshot } from '@sigx/actors/host';

/**
 * A histogram as three comparable bars.
 *
 * `BarChartItem` is structurally a `PercentilePoint`, so this is a widening
 * rather than a conversion — stated as a function anyway, because the day the
 * two shapes diverge should be a type error here and not at every call site.
 */
export function percentileItems(snapshot: HistogramSnapshot | null): BarChartItem[] {
    return percentilePoints(snapshot);
}

/**
 * One scale for a set of histograms, so latency, queue and turn are read
 * against a single axis — the comparison IS the diagnosis, and auto-scaling
 * each row would make a 12µs queue wait and a 47ms turn draw identically.
 *
 * Every percentile is offered to `commonScale`, not just p99: the scale has
 * to cover the tallest bar actually drawn. (`percentileCeiling` in the
 * monitor answers the same question in plain numbers; this one goes through
 * `commonScale` because a terminal axis is quantised to cells and the
 * rounding is the terminal's to own.)
 */
export function histogramScale(snapshots: readonly (HistogramSnapshot | null)[]): number {
    const values: (number | null)[] = [];
    for (const snapshot of snapshots) {
        if (!snapshot || snapshot.count === 0) continue;
        values.push(snapshot.p50Ms, snapshot.p90Ms, snapshot.p99Ms);
    }
    return commonScale(values);
}

/** The three shard states as `StatusGrid` tones. */
const SHARD_TONE: Record<ShardState, StatusTone> = {
    claimed: 'ok',
    unclaimed: 'danger',
    split: 'warn'
};

/** The reminder shard map as status cells, in shard order. */
export function shardCells(shards: Record<string, readonly string[]>): StatusCell[] {
    return shardStates(shards).map(
        (shard) =>
            ({
                label: shard.label,
                tone: SHARD_TONE[shard.state],
                detail: shard.claimants.join(' ')
            }) satisfies StatusCell
    );
}

/** An `AlertTone` as a `@sigx/terminal` colour. They happen to agree today;
 *  the mapping exists so a new severity is a compile error, not a blank. */
export const ALERT_COLOR: Record<AlertTone, string> = {
    danger: 'danger',
    warn: 'warn'
};
