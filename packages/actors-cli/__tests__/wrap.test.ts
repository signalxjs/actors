/**
 * Wrapping prose to a pane.
 *
 * Tables truncate, banners wrap — an alert reads "3 reminder shard(s)
 * UNCLAIMED (p3 p7 p9) — nothing is ticking them", and cutting it at the
 * pane edge throws away the half that says what to do.
 */
import { describe, expect, it } from 'vitest';
import { displayWidth } from '@sigx/terminal';
import { wrapText } from '../src/tui/wrap';

describe('wrapText', () => {
    it('breaks at spaces and keeps every line inside the width', () => {
        const lines = wrapText('the quick brown fox jumps over the lazy dog', 12);
        for (const line of lines) expect(displayWidth(line)).toBeLessThanOrEqual(12);
        expect(lines.join(' ').replace(/\s+/g, ' ')).toBe('the quick brown fox jumps over the lazy dog');
    });

    it('indents continuation lines, so a wrapped alert reads as one item', () => {
        const lines = wrapText('! 3 reminder shards unclaimed', 16, '  ');
        expect(lines.length).toBeGreaterThan(1);
        expect(lines[0]!.startsWith('!')).toBe(true);
        for (const line of lines.slice(1)) expect(line.startsWith('  ')).toBe(true);
        for (const line of lines) expect(displayWidth(line)).toBeLessThanOrEqual(16);
    });

    it('cuts a single over-long word rather than overflowing', () => {
        // A grain key or a URL has no space to break at, and running off
        // the right edge is the failure this whole module exists to stop.
        const lines = wrapText('short aaaaaaaaaaaaaaaaaaaaaaaaaaaa', 10);
        for (const line of lines) expect(displayWidth(line)).toBeLessThanOrEqual(10);
        expect(lines.join('')).toContain('aaaaaaaa');
    });

    it('measures display cells, not code units', () => {
        // Each glyph is two cells: eight of them do not fit a width of 10,
        // however short `.length` says the string is.
        const lines = wrapText('用户名称超长的键', 10);
        for (const line of lines) expect(displayWidth(line)).toBeLessThanOrEqual(10);
        expect(lines.length).toBeGreaterThan(1);
    });

    it('survives nonsense', () => {
        expect(wrapText('anything', 0)).toEqual([]);
        expect(wrapText('', 10)).toEqual([]);
        expect(wrapText('   ', 10)).toEqual([]);
    });
});
