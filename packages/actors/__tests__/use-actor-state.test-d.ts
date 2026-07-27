/**
 * Typing contract for the read/mutate hooks: method names, argument types
 * and results all come from the actor definition, so a rename or signature
 * change surfaces at every call site rather than at runtime.
 */
import { describe, expectTypeOf, it } from 'vitest';
import { actorKey, defineActor } from '@sigx/actors';
import { useActorAction, useActorState } from '@sigx/actors/app';
import type { AsyncState } from 'sigx';

const CartActor = defineActor({
    type: 'Cart',
    unguarded: true,
    state: () => ({ items: [] as string[] }),
    methods: (ctx) => ({
        async total(): Promise<number> {
            return ctx.state.items.length;
        },
        async page(n: number, open: boolean): Promise<string[]> {
            return open ? ctx.state.items.slice(0, n) : [];
        },
        async add(item: string): Promise<number> {
            ctx.state.items.push(item);
            return ctx.state.items.length;
        },
        /**
         * An ARROW PROPERTY rather than method shorthand. The two are typed
         * differently under `strictFunctionTypes` (shorthand stays
         * bivariant), so inference is pinned for both shapes — a user
         * should not get different result types for writing one or the
         * other.
         */
        first: async (n: number): Promise<string | undefined> => ctx.state.items[n],
        /** An OBJECT parameter — cannot canonicalize into a data key. */
        async search(filter: { term: string }): Promise<string[]> {
            return ctx.state.items.filter((i) => i.includes(filter.term));
        },
        /** An OPTIONAL primitive — must stay usable, omitted or supplied. */
        async recent(limit?: number): Promise<string[]> {
            return ctx.state.items.slice(0, limit ?? 5);
        }
    })
});

describe('useActorState typing', () => {
    it('infers the result from the method', () => {
        expectTypeOf(useActorState(CartActor, 'c1', 'total')).toEqualTypeOf<AsyncState<number>>();
        expectTypeOf(useActorState(CartActor, 'c1', 'page', 2, true)).toEqualTypeOf<
            AsyncState<string[]>
        >();
    });

    it('infers the same way for an arrow property as for method shorthand', () => {
        expectTypeOf(useActorState(CartActor, 'c1', 'first', 0)).toEqualTypeOf<
            AsyncState<string | undefined>
        >();
        // @ts-expect-error — first takes a number
        useActorState(CartActor, 'c1', 'first', 'zero');
    });

    it('rejects an unknown method name', () => {
        // @ts-expect-error — 'nope' is not a method on Cart
        useActorState(CartActor, 'c1', 'nope');
    });

    it('rejects wrong argument types', () => {
        // @ts-expect-error — page takes (number, boolean)
        useActorState(CartActor, 'c1', 'page', 'two', true);
    });

    it('rejects missing arguments', () => {
        // @ts-expect-error — page needs both arguments
        useActorState(CartActor, 'c1', 'page', 2);
    });
});

describe('useActorAction typing', () => {
    it('infers argument and result types', () => {
        const action = useActorAction(CartActor, 'c1', 'add');
        expectTypeOf(action.run).parameter(0).toEqualTypeOf<[item: string]>();
        expectTypeOf(action.value).toEqualTypeOf<number | null>();
    });

    it('rejects an unknown method name', () => {
        // @ts-expect-error — 'nope' is not a method on Cart
        useActorAction(CartActor, 'c1', 'nope');
    });
});

describe('key arguments must be JSON primitives at COMPILE time', () => {
    it('rejects an object argument on the read hook', () => {
        // @ts-expect-error — a key argument must be a JSON primitive
        useActorState(CartActor, 'c1', 'search', { term: 'a' });
    });

    it('rejects an object argument on actorKey', () => {
        // @ts-expect-error — a key argument must be a JSON primitive
        actorKey(CartActor, 'c1', 'search', { term: 'a' });
    });

    it('rejects an object argument in the reactive form', () => {
        // @ts-expect-error — a key argument must be a JSON primitive
        useActorState(CartActor, () => ['c1', 'search', { term: 'a' }] as const);
    });

    it('still accepts an optional primitive, omitted or supplied', () => {
        expectTypeOf(useActorState(CartActor, 'c1', 'recent')).toEqualTypeOf<
            AsyncState<string[]>
        >();
        expectTypeOf(useActorState(CartActor, 'c1', 'recent', 3)).toEqualTypeOf<
            AsyncState<string[]>
        >();
    });

    it('does NOT constrain useActorAction — action arguments are not key material', () => {
        const action = useActorAction(CartActor, 'c1', 'search');
        expectTypeOf(action.run).parameter(0).toEqualTypeOf<[filter: { term: string }]>();
    });
});

describe('actorKey typing', () => {
    it('rejects an unknown method name', () => {
        // @ts-expect-error — 'nope' is not a method on Cart
        actorKey(CartActor, 'c1', 'nope');
    });

    it('rejects wrong argument types', () => {
        // @ts-expect-error — page takes (number, boolean)
        actorKey(CartActor, 'c1', 'page', true, 2);
    });
});
