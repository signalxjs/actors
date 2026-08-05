/**
 * The canonical-key invariant. SSR seeding, hydration-without-refetch,
 * invalidation and live-subscription identity all rest on `actorKey`
 * producing a BYTE-IDENTICAL tuple in the browser and on the server —
 * across a build that swaps the module. Pinned here explicitly rather than
 * left implied by an integration test.
 */
import { describe, expect, it } from 'vitest';
import { actorKey, defineActor } from '@sigx/actors';
import { __actorRef } from '@sigx/actors/client';

const CartActor = defineActor({
    type: 'Cart',
    allowAnonymous: true,
    state: () => ({ items: [] as string[] }),
    methods: (ctx) => ({
        async total() {
            return ctx.state.items.length;
        },
        async page(n: number, open: boolean) {
            return [n, open] as const;
        }
    })
});

/**
 * What the build transform emits in the client bundle for the same actor.
 * Typed as the definition on purpose: the swap replaces VALUES, never
 * types, so this is exactly the shape a browser call site sees.
 */
const CartRef = __actorRef('Cart', '/_sigx/actor') as unknown as typeof CartActor;

describe('actorKey', () => {
    it('produces the same tuple from a definition and from a client ref', () => {
        expect(actorKey(CartActor, 'c1')).toEqual(actorKey(CartRef, 'c1'));
        expect(actorKey(CartActor, 'c1', 'total')).toEqual(actorKey(CartRef, 'c1', 'total'));
        expect(actorKey(CartActor, 'c1', 'page', 2, true)).toEqual(
            actorKey(CartRef, 'c1', 'page', 2, true)
        );
    });

    it('canonicalizes identically on both sides — the SSR/hydration contract', () => {
        // Core's canonical identity is the JSON of the tuple, so comparing
        // the JSON is comparing exactly what the page payload is keyed by.
        expect(JSON.stringify(actorKey(CartActor, 'c1', 'total'))).toBe(
            JSON.stringify(actorKey(CartRef, 'c1', 'total'))
        );
    });

    it('namespaces the tuple so a prefix addresses a whole actor, then the type', () => {
        expect(actorKey(CartActor, 'c1', 'total')).toEqual(['@actor', 'Cart', 'c1', 'total']);
        expect(actorKey(CartActor, 'c1')).toEqual(['@actor', 'Cart', 'c1']);
        // The prefix relation invalidation depends on.
        const whole = actorKey(CartActor, 'c1');
        const one = actorKey(CartActor, 'c1', 'total');
        expect(one.slice(0, whole.length)).toEqual([...whole]);
    });

    it('appends arguments so two argument sets are different keys', () => {
        expect(actorKey(CartActor, 'c1', 'page', 1, false)).not.toEqual(
            actorKey(CartActor, 'c1', 'page', 2, false)
        );
    });

    it('refuses a non-primitive argument in dev — it would not canonicalize stably', () => {
        expect(() =>
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            actorKey(CartActor, 'c1', 'page', { id: 1 } as any, false)
        ).toThrow(/JSON primitives/);
    });

    it('refuses an empty actor key', () => {
        expect(() => actorKey(CartActor, '')).toThrow(/non-empty string actor key/);
    });

    it('refuses something that is not an actor at all', () => {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        expect(() => actorKey({} as any, 'c1')).toThrow(/actor definition/);
    });
});
