/**
 * The invalidation matching relation, pinned against the same table core
 * uses. This function exists three times now — here, in `@sigx/server`, and
 * in `@sigx/cache` — so its semantics are the thing that must not drift.
 */
import { describe, expect, it } from 'vitest';
import { keyMatches } from '@sigx/actors/app';
import { actorKey, defineActor } from '@sigx/actors';

const CartActor = defineActor({
    type: 'Cart',
    unguarded: true,
    state: () => ({ items: [] as string[] }),
    methods: (ctx) => ({
        async total() {
            return ctx.state.items.length;
        }
    })
});

const canonical = (tuple: readonly unknown[]): string => JSON.stringify(tuple);

describe('keyMatches', () => {
    it('matches a string pattern by exact equality only', () => {
        expect(keyMatches('cart', 'cart')).toBe(true);
        expect(keyMatches('cart', 'car')).toBe(false);
        expect(keyMatches('cart:1', 'cart')).toBe(false);
    });

    it('matches a tuple pattern as a prefix', () => {
        const entry = canonical(['posts', 'u1', 2]);
        expect(keyMatches(entry, ['posts'])).toBe(true);
        expect(keyMatches(entry, ['posts', 'u1'])).toBe(true);
        expect(keyMatches(entry, ['posts', 'u1', 2])).toBe(true); // exact
        expect(keyMatches(entry, ['posts', 'u2'])).toBe(false);
        expect(keyMatches(entry, ['comments'])).toBe(false);
    });

    it('a pattern longer than the entry does not match', () => {
        expect(keyMatches(canonical(['posts', 'u1']), ['posts', 'u1', 2])).toBe(false);
    });

    it('does not match on a shared textual prefix — c1 is not c10', () => {
        const c10 = canonical(['@actor', 'Cart', 'c10', 'total']);
        expect(keyMatches(c10, ['@actor', 'Cart', 'c1'])).toBe(false);
        expect(keyMatches(c10, ['@actor', 'Cart', 'c10'])).toBe(true);
    });

    it('a tuple pattern never matches a plain string key', () => {
        expect(keyMatches('posts', ['posts'])).toBe(false);
    });

    it('an empty pattern matches nothing — it is not a wildcard', () => {
        expect(keyMatches(canonical(['posts']), [])).toBe(false);
    });

    it('distinguishes element types, as canonical JSON does', () => {
        expect(keyMatches(canonical(['n', 2]), ['n', 2])).toBe(true);
        expect(keyMatches(canonical(['n', 2]), ['n', '2'])).toBe(false);
        expect(keyMatches(canonical(['n', null]), ['n', null])).toBe(true);
    });

    it('addresses actorKey tuples the way invalidation needs', () => {
        const one = canonical([...actorKey(CartActor, 'c1', 'total')]);
        expect(keyMatches(one, [...actorKey(CartActor, 'c1')])).toBe(true); // this actor
        expect(keyMatches(one, ['@actor', 'Cart'])).toBe(true); // every cart
        expect(keyMatches(one, [...actorKey(CartActor, 'c2')])).toBe(false); // another cart
        expect(keyMatches(one, ['@actor', 'Order'])).toBe(false); // another type
    });
});
