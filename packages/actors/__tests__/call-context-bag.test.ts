/**
 * The context-bag vocabulary (#246): stamp, lift, merge, validate, sanitize.
 *
 * The posture under test: DEVELOPER input (stampCallBag, .with({ bag }))
 * throws on violation — their code, their stack — while PEER input
 * (sanitizeWireBag, which decodes the envelope) drops the WHOLE bag
 * silently: a 400 is a DoS lever and a partial identity is worse than none.
 */
import { describe, expect, it } from 'vitest';
import {
    CALL_BAG_MAX_ENTRIES,
    CALL_BAG_MAX_KEY_BYTES,
    CALL_BAG_MAX_TOTAL_BYTES,
    CALL_BAG_MAX_VALUE_BYTES,
    EMPTY_CALL_BAG,
    stampCallBag
} from '@sigx/actors';
import {
    isValidCallBag,
    mergeCallBag,
    sanitizeWireBag,
    takeCallBag
} from '../src/call-context-bag';

const rq = () => ({ locals: {} as Record<string, unknown> });

describe('stampCallBag', () => {
    it('stamps entries a later takeCallBag lifts, frozen', () => {
        const scope = rq();
        stampCallBag(scope, { user: 'ada' });
        stampCallBag(scope, { 'corr-id': 'r-1' });
        const bag = takeCallBag(scope.locals)!;
        expect(bag).toEqual({ user: 'ada', 'corr-id': 'r-1' });
        expect(Object.isFrozen(bag)).toBe(true);
    });

    it('merges last-wins across stamps', () => {
        const scope = rq();
        stampCallBag(scope, { user: 'ada' });
        stampCallBag(scope, { user: 'grace' });
        expect(takeCallBag(scope.locals)).toEqual({ user: 'grace' });
    });

    it.each([
        ['an empty key', { '': 'x' }],
        ['a non-string value', { k: 1 as unknown as string }],
        ['a __proto__ key', JSON.parse('{"__proto__": "x"}') as Record<string, string>],
        ['a constructor key', { constructor: 'x' } as unknown as Record<string, string>],
        ['a prototype key', { prototype: 'x' } as unknown as Record<string, string>],
        ['an oversize key', { ['k'.repeat(CALL_BAG_MAX_KEY_BYTES + 1)]: 'x' }],
        ['an oversize value', { k: 'v'.repeat(CALL_BAG_MAX_VALUE_BYTES + 1) }]
    ])('throws on %s and stamps nothing usable', (_label, entries) => {
        const scope = rq();
        expect(() => stampCallBag(scope, entries)).toThrow(/\[sigx actors\]/);
    });

    it('byte-counts caps in UTF-8, not code units', () => {
        // '€' is 1 code unit but 3 UTF-8 bytes; 22 of them = 66 > 64.
        const scope = rq();
        expect(() => stampCallBag(scope, { ['€'.repeat(22)]: 'x' })).toThrow(/refused an entry/);
        // 21 of them = 63 bytes fits.
        stampCallBag(scope, { ['€'.repeat(21)]: 'x' });
        expect(Object.keys(takeCallBag(scope.locals)!)).toHaveLength(1);
    });

    it('throws when the merged bag would blow the entry or total caps', () => {
        const scope = rq();
        for (let i = 0; i < CALL_BAG_MAX_ENTRIES; i++) stampCallBag(scope, { [`k${i}`]: 'v' });
        expect(() => stampCallBag(scope, { one: 'more' })).toThrow(/entry or byte caps/);

        const big = rq();
        const value = 'v'.repeat(CALL_BAG_MAX_VALUE_BYTES);
        stampCallBag(big, { a: value, b: value, c: value });
        expect(() => stampCallBag(big, { d: value, e: value })).toThrow(/entry or byte caps/);
    });

    it('survives a tampered locals store instead of crashing into it', () => {
        const scope = rq();
        scope.locals['__sigxCallBag'] = 'junk';
        stampCallBag(scope, { user: 'ada' });
        expect(takeCallBag(scope.locals)).toEqual({ user: 'ada' });
    });
});

describe('takeCallBag', () => {
    it('returns undefined when nothing was stamped, or the stamp is empty', () => {
        expect(takeCallBag({})).toBeUndefined();
        const scope = rq();
        scope.locals['__sigxCallBag'] = {};
        expect(takeCallBag(scope.locals)).toBeUndefined();
    });

    it('returns a copy — mutating the store later does not reach a lifted bag', () => {
        const scope = rq();
        stampCallBag(scope, { user: 'ada' });
        const bag = takeCallBag(scope.locals)!;
        (scope.locals['__sigxCallBag'] as Record<string, string>).user = 'mallory';
        expect(bag.user).toBe('ada');
    });
});

describe('mergeCallBag', () => {
    it('explicit entries win over the base, and the result is frozen', () => {
        const merged = mergeCallBag(Object.freeze({ user: 'ada', keep: 'y' }), {
            user: 'grace'
        })!;
        expect(merged).toEqual({ user: 'grace', keep: 'y' });
        expect(Object.isFrozen(merged)).toBe(true);
    });

    it('sanitizes and passes the base when there is nothing to merge', () => {
        const base = Object.freeze({ user: 'ada' });
        expect(mergeCallBag(base, undefined)).toEqual(base);
        expect(mergeCallBag(base, {})).toEqual(base);
        expect(Object.isFrozen(mergeCallBag(base, undefined))).toBe(true);
        expect(mergeCallBag(undefined, undefined)).toBeUndefined();
    });

    it('drops a middleware-tainted base WHOLE instead of propagating it', () => {
        // The base is whatever the call context carries — a useDispatch
        // middleware can have put anything there. Propagating it would only
        // postpone the drop to envelope encode, where it takes the valid
        // explicit entries down with it.
        const tainted = { user: 'ada', evil: 42 } as unknown as Readonly<Record<string, string>>;
        expect(mergeCallBag(tainted, { fresh: 'x' })).toEqual({ fresh: 'x' });
        expect(mergeCallBag(tainted, undefined)).toBeUndefined();
    });

    it('validates the extras and the merged totals — developer input throws', () => {
        expect(() => mergeCallBag(undefined, { '': 'x' })).toThrow(/\[sigx actors\]/);
        const value = 'v'.repeat(CALL_BAG_MAX_VALUE_BYTES);
        expect(() =>
            mergeCallBag(Object.freeze({ a: value, b: value, c: value }), { d: value, e: value })
        ).toThrow(/entry or byte caps/);
    });
});

describe('isValidCallBag / sanitizeWireBag', () => {
    it.each([
        ['an array', ['x']],
        ['a string', 'x'],
        ['null', null],
        ['a number', 7],
        ['a nested object value', { k: { nested: true } }],
        ['a non-string value', { k: 1 }],
        ['too many entries', Object.fromEntries(Array.from({ length: 9 }, (_, i) => [`k${i}`, 'v']))],
        ['an oversize value', { k: 'v'.repeat(CALL_BAG_MAX_VALUE_BYTES + 1) }],
        ['a __proto__ key', JSON.parse('{"__proto__": "x", "user": "ada"}')]
    ])('rejects %s whole — never a partial bag', (_label, value) => {
        expect(sanitizeWireBag(value)).toBeUndefined();
        if (value !== null) expect(isValidCallBag(value)).toBe(false);
    });

    it('accepts a valid bag as a frozen REBUILT copy, never an alias', () => {
        const input = { user: 'ada' };
        const out = sanitizeWireBag(input)!;
        expect(out).toEqual({ user: 'ada' });
        expect(out).not.toBe(input);
        expect(Object.isFrozen(out)).toBe(true);
        input.user = 'mallory';
        expect(out.user).toBe('ada');
    });

    it('decoding a __proto__ bag leaves Object.prototype unpolluted', () => {
        sanitizeWireBag(JSON.parse('{"__proto__": {"polluted": true}}'));
        expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
    });

    it('EMPTY_CALL_BAG is frozen and empty', () => {
        expect(Object.isFrozen(EMPTY_CALL_BAG)).toBe(true);
        expect(Object.keys(EMPTY_CALL_BAG)).toEqual([]);
    });
});
