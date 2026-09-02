/**
 * `format.bytes` — the one formatter with a `null` that MEANS something.
 *
 * A host's buffered-bytes total is `null` when no open session could report
 * one (#208): rendering that as `0 B` claims the host is not buffering when
 * the truth is that nobody could say.
 */
import { describe, expect, it } from 'vitest';
import { bytes } from '@sigx/actors-monitor/format';

describe('format.bytes', () => {
    it('scales in decimal units', () => {
        expect(bytes(0)).toBe('0 B');
        expect(bytes(950)).toBe('950 B');
        expect(bytes(12_400)).toBe('12.4 kB');
        expect(bytes(3_100_000)).toBe('3.1 MB');
        expect(bytes(2_000_000_000)).toBe('2 GB');
    });

    it('draws a gap for "nobody could tell us", not a zero', () => {
        expect(bytes(null)).toBe('—');
        expect(bytes(Number.NaN)).toBe('—');
    });
});
