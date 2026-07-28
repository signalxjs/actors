/**
 * Rendering a `HistogramSnapshot`, and the reminder-shard grid.
 *
 * Both are shapes core produces that no upstream component knows how to
 * draw. Pure functions here; JSX wrappers over them.
 */
import type { HistogramSnapshot } from '@sigx/actors/silo';

export interface PercentileCell {
    label: 'p50' | 'p90' | 'p99';
    ms: number;
    /** 0…1 against the row's scale — the bar length. */
    ratio: number;
}

export interface HistogramRow {
    label: string;
    count: number;
    cells: PercentileCell[];
    /**
     * True when there is nothing to draw — the histogram was absent
     * (`metrics({ histograms: false })`) or recorded no samples. `cells` is
     * then empty rather than three zeroed bars, because a row of zeroes
     * asserts "we measured, and it was fast".
     */
    empty: boolean;
}

/**
 * Turn a histogram into three comparable percentile cells.
 *
 * `scaleMs` is passed in rather than derived per row, because the entire
 * value of stacking `latency`, `queue` and `turn` is reading them against
 * ONE scale — auto-scaling each row would make a 12µs queue wait and a 47ms
 * turn draw identical bars.
 */
export function histogramRow(
    label: string,
    snapshot: HistogramSnapshot | null,
    scaleMs: number
): HistogramRow {
    if (!snapshot || snapshot.count === 0) {
        return { label, count: 0, cells: [], empty: true };
    }
    const scale = Number.isFinite(scaleMs) && scaleMs > 0 ? scaleMs : snapshot.p99Ms || 1;
    const cell = (name: PercentileCell['label'], ms: number): PercentileCell => ({
        label: name,
        ms,
        ratio: Math.max(0, Math.min(1, ms / scale))
    });
    return {
        label,
        count: snapshot.count,
        cells: [cell('p50', snapshot.p50Ms), cell('p90', snapshot.p90Ms), cell('p99', snapshot.p99Ms)],
        empty: false
    };
}

/**
 * A common scale for a set of histograms: the highest p99 present.
 *
 * p99 rather than max, because one pathological outlier would otherwise
 * flatten every bar in the panel to nothing.
 */
export function commonScale(snapshots: readonly (HistogramSnapshot | null)[]): number {
    let scale = 0;
    for (const snapshot of snapshots) {
        if (snapshot && snapshot.count > 0 && snapshot.p99Ms > scale) scale = snapshot.p99Ms;
    }
    return scale;
}

/** How a shard's claimant count should read. */
export type ShardTone = 'ok' | 'unclaimed' | 'split';

export interface ShardCell {
    shard: string;
    claimants: readonly string[];
    tone: ShardTone;
    /** `●` claimed once, `○` unclaimed, `◆` claimed more than once. */
    glyph: string;
}

const GLYPHS: Record<ShardTone, string> = { ok: '●', unclaimed: '○', split: '◆' };

/**
 * Lay the reminder shard map out as a grid.
 *
 * The three states mean genuinely different things and none of them is a
 * count you would read off a number:
 *
 *   one claimant   healthy
 *   none           NOTHING is ticking that shard — those reminders are not
 *                  firing, and nothing else in the system surfaces it
 *   two or more    views have diverged; safe (the per-shard etag CAS keeps
 *                  delivery at-most-once) but worth knowing
 */
export function shardGrid(
    shards: Record<string, readonly string[]>,
    perRow = 8
): ShardCell[][] {
    const columns = Math.max(1, Math.floor(perRow));
    // Sorted numerically by index, not lexically: p10 must not sit between
    // p1 and p2.
    const names = Object.keys(shards).sort((a, b) => shardIndex(a) - shardIndex(b));
    const cells = names.map((shard) => {
        const claimants = shards[shard] ?? [];
        const tone: ShardTone =
            claimants.length === 0 ? 'unclaimed' : claimants.length === 1 ? 'ok' : 'split';
        return { shard, claimants, tone, glyph: GLYPHS[tone] };
    });
    const rows: ShardCell[][] = [];
    for (let index = 0; index < cells.length; index += columns) {
        rows.push(cells.slice(index, index + columns));
    }
    return rows;
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

/** Shards claimed by more than one silo — views have diverged. */
export function splitShards(shards: Record<string, readonly string[]>): string[] {
    return Object.entries(shards)
        .filter(([, claimants]) => claimants.length > 1)
        .map(([shard]) => shard)
        .sort((a, b) => shardIndex(a) - shardIndex(b));
}
