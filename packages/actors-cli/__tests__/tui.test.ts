/**
 * The terminal building blocks.
 *
 * These are pure on purpose — the layout maths is the part that is easy to
 * get subtly wrong and hard to notice by looking at a running dashboard, so
 * it is the part that carries tests. Several assertions here are about
 * *not misleading*: a gap is not a zero, a truncated key is marked, and an
 * unchanged list does not appear to move.
 */
import { describe, expect, it } from 'vitest';
import {
    cellWidth,
    commonScale,
    fit,
    histogramRow,
    layoutTable,
    meter,
    moveCursor,
    peakOf,
    scrollOffset,
    shardGrid,
    sortRows,
    sparkline,
    splitShards,
    trend,
    unclaimedShards,
    type Column
} from '../src/tui';

describe('sparkline', () => {
    it('scales from ZERO, not from the minimum', () => {
        // A min-anchored sparkline turns a flat line at 1000 into a
        // mountain range of noise — the commonest way these mislead.
        expect(sparkline([1000, 1001, 1000, 999])).toBe('████');
        // Zero sits on the reserved lowest block; the rest spread above it.
        expect(sparkline([0, 50, 100])).toBe('▁▅█');
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
        expect(line[0]).toBe('▁');
        // 1 of 1000 is tiny but real; rounding it to nothing hides that
        // anything happened at all.
        expect(line[1]).not.toBe(line[0]);
    });

    it('shows the NEWEST values when history exceeds the width', () => {
        // Right-aligned: the newest sample is the one being watched.
        expect(sparkline([0, 0, 0, 0, 100], { width: 2, max: 100 })).toBe('▁█');
    });

    it('renders an all-zero series flat rather than full', () => {
        expect(sparkline([0, 0, 0])).toBe('▁▁▁');
    });

    it('honours an explicit max, so two sparklines are comparable', () => {
        // Auto-scaled, both would fill the height and look identical.
        expect(sparkline([50], { max: 100 })).toBe('▅');
        expect(sparkline([100], { max: 100 })).toBe('█');
        expect(sparkline([50])).toBe('█');
    });

    it('pads to width only when asked', () => {
        expect(sparkline([1], { width: 4, pad: true })).toBe('   █');
        expect(sparkline([1], { width: 4 })).toBe('█');
    });

    it('survives nonsense input', () => {
        expect(sparkline([])).toBe('');
        expect(sparkline([1, 2], { width: 0 })).toBe('');
        expect(sparkline([Number.NaN, 1], { max: 1 })).toBe('·█');
        expect(peakOf([null, Number.NaN, 3])).toBe(3);
    });
});

describe('meter', () => {
    it('fills proportionally', () => {
        expect(meter(50, 100, 10)).toBe('█████░░░░░');
        expect(meter(0, 100, 4)).toBe('░░░░');
        expect(meter(100, 100, 4)).toBe('████');
    });

    it('shows at least one cell for a small non-zero ratio', () => {
        // Rounding a real 2% down to empty reads as "nothing".
        expect(meter(2, 100, 10)).toBe('█░░░░░░░░░');
    });

    it('clamps rather than overflowing', () => {
        expect(meter(500, 100, 4)).toBe('████');
        expect(meter(-5, 100, 4)).toBe('░░░░');
        expect(meter(1, 0, 4)).toBe('░░░░');
    });
});

describe('trend', () => {
    it('marks direction', () => {
        expect(trend(5, 3)).toBe('▲');
        expect(trend(3, 5)).toBe('▼');
        expect(trend(3, 3)).toBe('·');
    });

    it('reports no direction across a gap', () => {
        // Comparing across a reset would show a crash that never happened.
        expect(trend(5, null)).toBe('·');
        expect(trend(null, 5)).toBe('·');
    });
});

interface Row {
    id: string;
    n: number;
}

const columns: Column<Row>[] = [
    { key: 'id', header: 'ID', value: (r) => r.id },
    { key: 'n', header: 'N', value: (r) => String(r.n), align: 'right' }
];

describe('layoutTable', () => {
    it('sizes columns to the widest cell and aligns', () => {
        const table = layoutTable(columns, [
            { id: 'silo-a', n: 5 },
            { id: 'b', n: 1200 }
        ]);
        // 'silo-a' is 6 wide; '1200' is 4.
        expect(table.widths).toEqual([6, 4]);
        // Right-aligned numbers line up on their last digit, which is the
        // only way a column of magnitudes is comparable at a glance.
        expect(table.rows[0]!.endsWith('   5')).toBe(true);
        expect(table.rows[1]!.endsWith('1200')).toBe(true);
        expect(table.rows[0]).toHaveLength(table.rows[1]!.length);
        expect(table.rows[0]!.startsWith('silo-a')).toBe(true);
        expect(table.rows[1]!.startsWith('b ')).toBe(true);
        // Trailing whitespace is trimmed, so a short header does not carry
        // invisible padding into a selected-row highlight.
        expect(table.header).toBe(table.header.trimEnd());
        expect(table.header.startsWith('ID')).toBe(true);
        expect(table.header.endsWith('N')).toBe(true);
    });

    it('takes space from the RIGHT when it has to shrink', () => {
        const table = layoutTable(columns, [{ id: 'a-very-long-silo-id', n: 1 }], { width: 14 });
        // The left column is the identity; truncating it to make room for a
        // number would make the table unusable.
        expect(table.widths[0]).toBeGreaterThan(table.widths[1]!);
    });

    it('marks a truncated cell rather than cutting it silently', () => {
        expect(fit('abcdefgh', 4)).toBe('abc…');
        expect(fit('ab', 4)).toBe('ab  ');
        expect(fit('ab', 4, 'right')).toBe('  ab');
        expect(fit('abc', 1)).toBe('…');
        expect(fit('abc', 0)).toBe('');
    });

    it('handles no rows', () => {
        const table = layoutTable(columns, []);
        expect(table.rows).toEqual([]);
        expect(table.header).toBe('ID  N');
    });
});

describe('scrollOffset', () => {
    it('does not scroll when everything fits', () => {
        expect(scrollOffset(3, 2, 10, 0)).toBe(0);
    });

    it('scrolls by ONE when the cursor steps past the edge', () => {
        // A screenful jump per keypress makes a list impossible to follow.
        expect(scrollOffset(100, 10, 10, 0)).toBe(1);
        expect(scrollOffset(100, 11, 10, 1)).toBe(2);
    });

    it('follows the cursor upwards', () => {
        expect(scrollOffset(100, 4, 10, 20)).toBe(4);
    });

    it('never scrolls past the end', () => {
        expect(scrollOffset(15, 14, 10, 99)).toBe(5);
    });
});

describe('moveCursor', () => {
    it('clamps instead of wrapping', () => {
        // Holding ↓ should stop at the bottom, not silently return to the
        // top and look like nothing happened.
        expect(moveCursor(9, 1, 10)).toBe(9);
        expect(moveCursor(0, -1, 10)).toBe(0);
        expect(moveCursor(5, 3, 10)).toBe(8);
        expect(moveCursor(0, 5, 0)).toBe(0);
    });
});

describe('sortRows', () => {
    it('breaks ties on identity, so equal rows do not shuffle between polls', () => {
        const rows: Row[] = [
            { id: 'c', n: 1 },
            { id: 'a', n: 1 },
            { id: 'b', n: 1 }
        ];
        const once = sortRows(rows, (x, y) => y.n - x.n, (r) => r.id).map((r) => r.id);
        const twice = sortRows(rows, (x, y) => y.n - x.n, (r) => r.id).map((r) => r.id);
        expect(once).toEqual(['a', 'b', 'c']);
        expect(twice).toEqual(once);
    });

    it('does not mutate the input', () => {
        const rows: Row[] = [{ id: 'b', n: 2 }, { id: 'a', n: 1 }];
        sortRows(rows, (x, y) => x.n - y.n, (r) => r.id);
        expect(rows[0]!.id).toBe('b');
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

describe('histogramRow', () => {
    it('measures every row against ONE scale', () => {
        const scale = commonScale([hist(1, 2, 100), hist(0.01, 0.02, 0.05)]);
        expect(scale).toBe(100);
        const slow = histogramRow('turn', hist(1, 2, 100), scale);
        const fast = histogramRow('queue', hist(0.01, 0.02, 0.05), scale);
        // The whole value of stacking these is the comparison; auto-scaling
        // each row would draw a 12µs queue wait and a 47ms turn identically.
        expect(slow.cells[2]!.ratio).toBe(1);
        expect(fast.cells[2]!.ratio).toBeLessThan(0.01);
    });

    it('marks an absent or empty histogram as empty', () => {
        expect(histogramRow('turn', null, 10).empty).toBe(true);
        expect(histogramRow('turn', hist(0, 0, 0, 0), 10).empty).toBe(true);
    });

    it('falls back to its own p99 when given no usable scale', () => {
        const row = histogramRow('turn', hist(1, 2, 4), 0);
        expect(row.cells[2]!.ratio).toBe(1);
    });

    it('scales on p99, so one outlier cannot flatten the panel', () => {
        // maxMs would be the outlier; p99 keeps the bars readable.
        expect(commonScale([{ ...hist(1, 2, 3), maxMs: 100_000 }])).toBe(3);
        expect(commonScale([null, hist(0, 0, 0, 0)])).toBe(0);
    });
});

describe('shardGrid', () => {
    const shards = {
        p0: ['silo-a'],
        p1: [],
        p2: ['silo-a', 'silo-b'],
        p10: ['silo-c']
    };

    it('distinguishes the three states that mean different things', () => {
        const [row] = shardGrid(shards, 8);
        expect(row!.map((c) => c.tone)).toEqual(['ok', 'unclaimed', 'split', 'ok']);
        expect(row!.map((c) => c.glyph)).toEqual(['●', '○', '◆', '●']);
    });

    it('orders shards numerically, so p10 does not sit between p1 and p2', () => {
        const [row] = shardGrid(shards, 8);
        expect(row!.map((c) => c.shard)).toEqual(['p0', 'p1', 'p2', 'p10']);
    });

    it('wraps into rows', () => {
        const rows = shardGrid(shards, 2);
        expect(rows).toHaveLength(2);
        expect(rows[0]!.map((c) => c.shard)).toEqual(['p0', 'p1']);
    });

    it('picks out the two findings worth acting on', () => {
        // Unclaimed means those reminders are simply not firing, and
        // nothing else in the system surfaces it.
        expect(unclaimedShards(shards)).toEqual(['p1']);
        // Two claimants is safe but means views diverged.
        expect(splitShards(shards)).toEqual(['p2']);
    });

    it('handles an empty map', () => {
        expect(shardGrid({}, 8)).toEqual([]);
        expect(unclaimedShards({})).toEqual([]);
    });
});

describe('display width, not code units', () => {
    it('counts a wide glyph as two cells', () => {
        // A grain key is user data, so this is reachable with an ordinary
        // key: two wide glyphs (4 cells) plus `-42` (3) is 7, where
        // `.length` says 5 — so a column sized by length under-pads by 2
        // and everything to its right shifts left.
        expect('用户-42'.length).toBe(5);
        expect(cellWidth('用户-42')).toBe(7);
        expect(cellWidth('plain')).toBe(5);
        expect(cellWidth('')).toBe(0);
    });

    it('pads to the DISPLAYED width, so later columns do not shift left', () => {
        // Sized by `.length` this returns 5 visible cells too many.
        expect(cellWidth(fit('用户-42', 12))).toBe(12);
        expect(cellWidth(fit('plain', 12))).toBe(12);
        expect(cellWidth(fit('plain', 12, 'right'))).toBe(12);
    });

    it('never splits a wide glyph or overflows when truncating', () => {
        const out = fit('用户名称超长', 7);
        expect(cellWidth(out)).toBeLessThanOrEqual(7);
        expect(out).toContain('…');
        // Half a character would be a mojibake cell.
        expect(out).not.toMatch(/�/);
    });

    it('aligns a mixed-width column', () => {
        const rows = ['用户-42', 'user-42', '猫'];
        const widths = rows.map((r) => cellWidth(fit(r, 10)));
        expect(new Set(widths).size).toBe(1);
    });
});
