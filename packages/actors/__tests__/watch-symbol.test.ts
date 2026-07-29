/**
 * The watch wire symbol and its options, unit-level.
 *
 * `parseWatchOptions` is the ONE validator both wire paths call, so what it
 * accepts is the contract — a transport that disagreed would make the same
 * call succeed on TCP and fail on HTTP.
 */
import { describe, expect, it } from 'vitest';
import { parseWatchOptions, watchSymbol, WATCH_SYMBOL_PREFIX } from '@sigx/actors/cluster';

describe('watchSymbol', () => {
    it('prefixes the ordinary symbol, and stays parseable by the last hash', () => {
        expect(watchSymbol('Counter', 'read')).toBe('$watch:Counter#read');
        expect(watchSymbol('Counter', 'read').startsWith(WATCH_SYMBOL_PREFIX)).toBe(true);
    });
});

describe('parseWatchOptions', () => {
    it('accepts absent options and an explicit throttle', () => {
        expect(parseWatchOptions(null, 's')).toBeUndefined();
        expect(parseWatchOptions(undefined, 's')).toBeUndefined();
        expect(parseWatchOptions({}, 's')).toEqual({});
        expect(parseWatchOptions({ throttleMs: 0 }, 's')).toEqual({ throttleMs: 0 });
        expect(parseWatchOptions({ throttleMs: 250 }, 's')).toEqual({ throttleMs: 250 });
    });

    it('refuses a non-finite or negative throttle rather than defaulting', () => {
        // Fail-open is the hazard: a non-finite value compares false against
        // every bound, so it does not fall back to the default — it disables
        // throttling outright, and looks healthy while doing it.
        for (const bad of [Number.NaN, Number.POSITIVE_INFINITY, -1, '50', null]) {
            expect(() => parseWatchOptions({ throttleMs: bad }, 's'), String(bad)).toThrow(
                /throttleMs/
            );
        }
    });

    it('refuses an ARRAY, which typeof calls an object', () => {
        // `typeof [] === 'object'`, so a bare shape check waves arrays
        // through and silently reads them as `{}` — no throttle, no
        // complaint, and nothing to point at when the rate is wrong.
        expect(() => parseWatchOptions([], 's')).toThrow(/object or null/);
        expect(() => parseWatchOptions([{ throttleMs: 5 }], 's')).toThrow(/object or null/);
    });

    it('refuses a primitive, and says which symbol was at fault', () => {
        expect(() => parseWatchOptions(5, '$watch:Counter#read')).toThrow(
            /\$watch:Counter#read/
        );
    });

    it('carries a 400 on the error, so both wire paths answer the same status', () => {
        const error = (() => {
            try {
                parseWatchOptions([], 's');
            } catch (caught) {
                return caught as { status?: number };
            }
            throw new Error('expected a throw');
        })();
        expect(error.status).toBe(400);
    });
});
