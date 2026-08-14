/**
 * The terminal layer, in two halves.
 *
 * **The contract with upstream.** The layout maths this dashboard used to
 * own now lives in `@sigx/terminal` (signalxjs/terminal#103). It kept our
 * decisions as its defaults, and those decisions are the reason the numbers
 * on screen are not misleading — a gap is not a zero, a tiny non-zero value
 * does not round away to nothing, a truncated key is marked, an unchanged
 * list does not appear to move. Deleting these assertions along with the
 * implementation would mean a future `@sigx/terminal` bump could quietly
 * take any of them back. So they stay, pointed at upstream: this is what we
 * depend on, stated as a test rather than as a hope.
 *
 * **What is still ours.** The MAPPING onto `@sigx/terminal`'s vocabulary. The
 * judgements behind it — which shard state is an incident, what an empty
 * histogram means — moved to `@sigx/actors-monitor` (#239) and are tested
 * there, because the web dashboard reaches the same verdicts through them.
 * What is asserted here is that the terminal renders those verdicts as the
 * right tones, in the right order.
 */
import { describe, expect, it } from 'vitest';
import {
    displayWidth,
    fitCell,
    layoutTable,
    meter,
    moveCursor,
    scrollWindow,
    sortRows,
    sparkline,
    trend,
    type TableColumn
} from '@sigx/terminal';
import { histogramScale, percentileItems, shardCells } from '../src/tui';

describe('upstream contract: sparkline', () => {
    it('scales from ZERO, not from the minimum', () => {
        // A min-anchored sparkline turns a flat line at 1000 into a
        // mountain range of noise — the commonest way these mislead.
        expect(sparkline([1000, 1001, 1000, 999])).toBe('████');
    });

    it('draws a gap as neither a zero nor a blank', () => {
        const line = sparkline([10, null, 10]);
        expect(line).toHaveLength(3);
        expect(line[1]).toBe('·');
        // Crucially not the baseline block: a reset would otherwise read as
        // "traffic stopped".
        expect(line[1]).not.toBe('▁');
    });

    it('never renders a non-zero value as the same as zero', () => {
        const line = sparkline([0, 1], { max: 1000 });
        // 1 of 1000 is tiny but real; rounding it to nothing hides that
        // anything happened at all.
        expect(line[1]).not.toBe(line[0]);
    });

    it('shows the NEWEST values when history exceeds the width', () => {
        // Right-aligned: the newest sample is the one being watched.
        expect(sparkline([0, 0, 0, 0, 100], { width: 2, max: 100 })).toBe('▁█');
    });

    it('honours an explicit max, so two sparklines are comparable', () => {
        // Auto-scaled, both would fill the height and look identical.
        expect(sparkline([100], { max: 100 })).toBe('█');
        expect(sparkline([50], { max: 100 })).not.toBe(sparkline([100], { max: 100 }));
    });
});

describe('upstream contract: meter and trend', () => {
    it('fills proportionally', () => {
        expect(meter(50, 100, 10)).toBe('█████░░░░░');
        expect(meter(0, 100, 4)).toBe('░░░░');
        expect(meter(100, 100, 4)).toBe('████');
    });

    it('shows at least one cell for a small non-zero ratio', () => {
        // Rounding a real 2% down to empty reads as "nothing".
        expect(meter(2, 100, 10)).toBe('█░░░░░░░░░');
    });

    it('reports no direction across a gap', () => {
        // Comparing across a counter reset would report a crash that never
        // happened.
        expect(trend(5, 3)).toBe(1);
        expect(trend(3, 5)).toBe(-1);
        expect(trend(5, null)).toBe(0);
        expect(trend(null, 5)).toBe(0);
    });
});

interface Row {
    id: string;
    n: number;
}

const columns: TableColumn<Row>[] = [
    { key: 'id', header: 'ID', value: (r) => r.id },
    { key: 'n', header: 'N', value: (r) => String(r.n), align: 'right' }
];

describe('upstream contract: table layout', () => {
    it('takes space from the RIGHT when it has to shrink', () => {
        const table = layoutTable(columns, [{ id: 'a-very-long-host-id', n: 1 }], { width: 14 });
        // The left column is the identity; truncating it to make room for a
        // number would make the table unusable.
        expect(table.widths[0]).toBeGreaterThan(table.widths[1]!);
    });

    it('marks a truncated cell rather than cutting it silently', () => {
        expect(fitCell('abcdefgh', 4)).toBe('abc…');
        expect(fitCell('ab', 4)).toBe('ab  ');
        expect(fitCell('ab', 4, 'right')).toBe('  ab');
    });

    it('measures display cells, not code units', () => {
        // An actor key is user data, so this is reachable with an ordinary
        // key: two wide glyphs (4 cells) plus `-42` (3) is 7, where
        // `.length` says 5 — a column sized by length under-pads by 2 and
        // everything to its right shifts left.
        expect('用户-42'.length).toBe(5);
        expect(displayWidth('用户-42')).toBe(7);
        expect(displayWidth(fitCell('用户-42', 12))).toBe(12);
        expect(displayWidth(fitCell('plain', 12))).toBe(12);
    });

    it('never splits a wide glyph when truncating', () => {
        const out = fitCell('用户名称超长', 7);
        expect(displayWidth(out)).toBeLessThanOrEqual(7);
        expect(out).toContain('…');
        // Half a character would be a mojibake cell.
        expect(out).not.toMatch(/�/);
    });
});

describe('upstream contract: cursor and viewport', () => {
    it('clamps the cursor instead of wrapping', () => {
        // Holding ↓ should stop at the bottom, not silently return to the
        // top and look like nothing happened.
        expect(moveCursor(9, 1, 10)).toBe(9);
        expect(moveCursor(0, -1, 10)).toBe(0);
        expect(moveCursor(5, 3, 10)).toBe(8);
        expect(moveCursor(0, 5, 0)).toBe(0);
    });

    it('scrolls by ONE when the cursor steps past the edge', () => {
        // A screenful jump per keypress makes a list impossible to follow.
        expect(scrollWindow(3, 2, 10, 0)).toBe(0);
        expect(scrollWindow(100, 10, 10, 0)).toBe(1);
        expect(scrollWindow(100, 4, 10, 20)).toBe(4);
    });

    it('breaks sort ties on identity, so equal rows do not shuffle between polls', () => {
        const rows: Row[] = [
            { id: 'c', n: 1 },
            { id: 'a', n: 1 },
            { id: 'b', n: 1 }
        ];
        const compare = (x: Row, y: Row): number => y.n - x.n;
        const identity = (r: Row): string => r.id;
        const once = sortRows(rows, compare, identity).map(identity);
        expect(once).toEqual(['a', 'b', 'c']);
        expect(sortRows(rows, compare, identity).map(identity)).toEqual(once);
        // Not mutated: the caller still holds its own order.
        expect(rows[0]!.id).toBe('c');
    });
});

const hist = (p50: number, p90: number, p99: number, count = 10) => ({
    count,
    minMs: 0,
    maxMs: p99,
    meanMs: p50,
    p50Ms: p50,
    p90Ms: p90,
    p99Ms: p99
});

describe('percentileItems', () => {
    it('measures every group against ONE scale', () => {
        const scale = histogramScale([hist(1, 2, 100), hist(0.01, 0.02, 0.05)]);
        expect(scale).toBe(100);
        // The whole value of stacking latency, queue and turn is the
        // comparison; a per-group scale would draw a 12µs queue wait and a
        // 47ms turn identically.
        expect(histogramScale([hist(0.01, 0.02, 0.05)])).toBe(0.05);
    });

    it('reports an absent or empty histogram as no reading, not as zero', () => {
        // A row of zeroes asserts "we measured, and it was fast".
        expect(percentileItems(null).map((i) => i.value)).toEqual([null, null, null]);
        expect(percentileItems(hist(0, 0, 0, 0)).map((i) => i.value)).toEqual([null, null, null]);
        expect(percentileItems(hist(1, 2, 3)).map((i) => i.value)).toEqual([1, 2, 3]);
        expect(percentileItems(hist(1, 2, 3)).map((i) => i.label)).toEqual(['p50', 'p90', 'p99']);
    });

    it('ignores a histogram that recorded nothing when picking the scale', () => {
        expect(histogramScale([null, hist(0, 0, 0, 0)])).toBe(0);
    });
});

describe('shardCells', () => {
    const shards = {
        p0: ['host-a'],
        p1: [],
        p2: ['host-a', 'host-b'],
        p10: ['host-c']
    };

    it('distinguishes the three states that mean different things', () => {
        const cells = shardCells(shards);
        // Unclaimed is the incident (nothing is ticking it); claimed twice
        // is a divergence, which is safe but worth knowing.
        expect(cells.map((c) => c.tone)).toEqual(['ok', 'danger', 'warn', 'ok']);
    });

    it('orders shards numerically, so p10 does not sit between p1 and p2', () => {
        expect(shardCells(shards).map((c) => c.label)).toEqual(['p0', 'p1', 'p2', 'p10']);
    });

    it('names the claimants, so a divergence says WHICH hosts', () => {
        expect(shardCells(shards)[2]!.detail).toBe('host-a host-b');
    });

    it('handles an empty map', () => {
        expect(shardCells({})).toEqual([]);
    });
});
