/**
 * `watchFingerprint` (#449): the distinct-delivery key a live read's result
 * is compared under. Two rules, both correctness:
 *
 *  - INJECTIVE across primitive types and edge values — `1` and `'1'`,
 *    `0` and `-0`, `null` and `undefined`, `NaN` and the string `'NaN'`
 *    must never collapse, or a real change is swallowed;
 *  - the CODEC IS NOT CONSULTED for a primitive — that walk was ~12% of a
 *    trivial live read (`streams/live-watch rows=0`, the #447 A/B), and a
 *    number needs no walk to be compared.
 */
import { describe, expect, it } from 'vitest';
import { watchFingerprint } from '../src/watch-core';

describe('watchFingerprint', () => {
    it('never consults the codec for a primitive, and always does for an object', () => {
        let encodes = 0;
        const encode = (value: unknown): unknown => {
            encodes++;
            return value;
        };
        for (const primitive of [0, -0, 1, NaN, Infinity, '', 'x', true, false, null, undefined, 10n]) {
            watchFingerprint(primitive, encode);
        }
        expect(encodes).toBe(0);
        watchFingerprint({ a: 1 }, encode);
        watchFingerprint([1], encode);
        watchFingerprint(new Date(0), encode);
        expect(encodes).toBe(3);
    });

    it('is injective across the values a read is likely to return', () => {
        const encode = (value: unknown): unknown => value;
        const values: unknown[] = [
            0, -0, 1, -1, 1.5, NaN, Infinity, -Infinity,
            '', '0', '1', 'NaN', 'null', 'undefined', 'true', 'n:1', '[1]', '{}',
            true, false, null, undefined, 0n, 1n,
            [], [1], [1, 2], ['1'], {}, { a: 1 }, { a: '1' }, { b: 1 }
        ];
        const prints = values.map((v) => watchFingerprint(v, encode));
        expect(new Set(prints).size).toBe(values.length);
    });

    it('is stable: equal values fingerprint equally', () => {
        const encode = (value: unknown): unknown => value;
        expect(watchFingerprint(3, encode)).toBe(watchFingerprint(3, encode));
        expect(watchFingerprint('ok', encode)).toBe(watchFingerprint('ok', encode));
        expect(watchFingerprint({ a: [1, 'b'] }, encode)).toBe(watchFingerprint({ a: [1, 'b'] }, encode));
        expect(watchFingerprint(NaN, encode)).toBe(watchFingerprint(NaN, encode));
    });
});
