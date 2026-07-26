/**
 * Typing contracts: the client proxy's inference from the definition —
 * methods promise-wrapped, streams as AsyncIterable, `.with` preserved.
 */
import { describe, expectTypeOf, it } from 'vitest';
import { actor, defineActor } from '@sigx/actors';
import type { ActorContext } from '@sigx/actors';

const Cart = defineActor({
    type: 'Cart',
    unguarded: true,
    state: () => ({ items: [] as string[], updated: null as Date | null }),
    methods: (ctx) => ({
        async add(item: string, at: Date) {
            ctx.state.items.push(item);
            ctx.state.updated = at;
            return ctx.state.items.length;
        },
        // Non-async return types are Promise-wrapped for callers.
        size() {
            return ctx.state.items.length;
        }
    }),
    streams: (ctx) => ({
        async *watch(everyMs: number) {
            void everyMs;
            for await (const s of ctx.changes()) yield s.items.length;
        }
    })
});

describe('ActorClient inference', () => {
    it('wraps methods in promises with exact argument types', () => {
        const client = actor(Cart, 'k');
        expectTypeOf(client.add).parameters.toEqualTypeOf<[string, Date]>();
        expectTypeOf(client.add).returns.toEqualTypeOf<Promise<number>>();
        // sync method still promise-wrapped at the client:
        expectTypeOf(client.size).returns.toEqualTypeOf<Promise<number>>();
        // @ts-expect-error unknown method
        void client.nope;
        // @ts-expect-error wrong argument type
        void client.add(1, new Date());
    });

    it('types stream methods as AsyncIterable', () => {
        const client = actor(Cart, 'k');
        expectTypeOf(client.watch).parameters.toEqualTypeOf<[number]>();
        expectTypeOf(client.watch).returns.toEqualTypeOf<AsyncIterable<number>>();
    });

    it('.with returns the same client shape', () => {
        const bound = actor(Cart, 'k').with({ signal: new AbortController().signal });
        expectTypeOf(bound.add).returns.toEqualTypeOf<Promise<number>>();
    });

    it('the methods factory sees a typed state', () => {
        void defineActor({
            type: 'Typed',
            unguarded: true,
            state: () => ({ n: 0 }),
            methods: (ctx: ActorContext<{ n: number }>) => ({
                async bump() {
                    // @ts-expect-error unknown state field
                    void ctx.state.missing;
                    return ++ctx.state.n;
                }
            })
        });
    });
});
