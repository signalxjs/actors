/**
 * Invalidation patterns are key material, so they carry the same
 * compile-time constraint `actorKey()` puts on its arguments: JSON
 * primitives only.
 *
 * Not cosmetic. `JSON.stringify` throws on a `bigint`, and collapses
 * `undefined`, symbols and functions all to `null` — so an unconstrained
 * pattern could silently match every key whose element there is `null` and
 * refresh reads it has nothing to do with. A wrong match is worse than a
 * crash; both are ruled out here.
 */
import { describe, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import { keyMatches, useActorAction } from '@sigx/actors/app';

const CartActor = defineActor({
    type: 'Cart',
    unguarded: true,
    state: () => ({ items: [] as string[] }),
    methods: (ctx) => ({
        async add(item: string) {
            ctx.state.items.push(item);
            return ctx.state.items.length;
        }
    })
});

describe('keyMatches patterns', () => {
    it('accepts JSON primitives', () => {
        keyMatches('k', 'k');
        keyMatches('k', ['a', 1, true, null]);
    });

    it('rejects a bigint — JSON.stringify throws on it', () => {
        // @ts-expect-error — not a JSON primitive
        keyMatches('k', ['a', 1n]);
    });

    it('rejects undefined — it would collapse to null and mis-match', () => {
        // @ts-expect-error — not a JSON primitive
        keyMatches('k', ['a', undefined]);
    });

    it('rejects an object', () => {
        // @ts-expect-error — not a JSON primitive
        keyMatches('k', ['a', { id: 1 }]);
    });
});

describe('useActorAction invalidates patterns', () => {
    it('accepts primitive tuples and strings', () => {
        useActorAction(CartActor, 'c1', 'add', { invalidates: [['@actor', 'Cart', 'c1'], 'other'] });
        useActorAction(CartActor, 'c1', 'add', { invalidates: false });
        useActorAction(CartActor, 'c1', 'add', {
            invalidates: (_result, key) => [['@actor', 'Cart', key]]
        });
    });

    it('rejects a non-primitive in the pattern list', () => {
        useActorAction(CartActor, 'c1', 'add', {
            // @ts-expect-error — not a JSON primitive
            invalidates: [['@actor', 'Cart', { id: 1 }]]
        });
    });

    it('rejects a non-primitive returned by the function form', () => {
        useActorAction(CartActor, 'c1', 'add', {
            // @ts-expect-error — not a JSON primitive
            invalidates: () => [['@actor', 'Cart', 1n]]
        });
    });
});
