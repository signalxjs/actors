/**
 * Typing contracts: the client proxy's inference from the definition —
 * methods promise-wrapped, streams as AsyncIterable, `.with` preserved.
 */
import { describe, expectTypeOf, it } from 'vitest';
import { actor, defineActor } from '@sigx/actors';
import type {
    ActorContext,
    ActorContextBase,
    ActorPlacementStrategy,
    AnyActorDefinition
} from '@sigx/actors';
import { defineActorApp, type ActorPlugin } from '@sigx/actors/host';

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

// ---------------------------------------------------------------------------
// Plugin context extension: `.use()` accumulates each plugin's `Ext`, and the
// app-bound `defineActor` types it inside every actor — no global
// declaration merging, so the additions stay per-app.

interface Logger {
    info(message: string): void;
}

const loggerPlugin: ActorPlugin<{ log: Logger }> = {
    name: 'logger',
    setup(registry) {
        registry.extendContext(() => ({ log: { info: () => {} } }));
    }
};

const tracingPlugin: ActorPlugin<{ traceId: string }> = {
    name: 'tracing',
    setup(registry) {
        registry.extendContext(() => ({ traceId: 'trace' }));
    }
};

describe('public type surface', () => {
    it('exports the types the docs name from the root entry', () => {
        // Regression guard: these are reachable from `@sigx/actors` itself,
        // not just from `../types`. Ordinary .test.ts files are transpiled
        // without type checking and `pnpm typecheck` covers src only, so a
        // missing barrel export is invisible anywhere but here.
        const strategy: ActorPlacementStrategy = { name: 'prefer-local' };
        expectTypeOf(strategy.name).toEqualTypeOf<string | undefined>();
        // `ActorContext` is the base plus the app's plugin additions.
        expectTypeOf<ActorContext<{ n: number }>>().toMatchTypeOf<
            ActorContextBase<{ n: number }>
        >();
    });

    it('accepts a declared placement on an actor', () => {
        void defineActor({
            type: 'Placed',
            unguarded: true,
            placement: { name: 'prefer-local' },
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                bump() {
                    return ++ctx.state.n;
                }
            })
        });
    });
});

describe('plugin ctx extension inference', () => {
    it('types a single plugin addition inside the actor', () => {
        const app = defineActorApp({ actors: [] }).use(loggerPlugin);
        void app.defineActor({
            type: 'Logged',
            unguarded: true,
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                bump() {
                    expectTypeOf(ctx.log).toEqualTypeOf<Logger>();
                    ctx.log.info('bump');
                    // built-ins are still there alongside the addition
                    expectTypeOf(ctx.key).toEqualTypeOf<string>();
                    return ++ctx.state.n;
                }
            })
        });
    });

    it('accumulates across several .use() calls', () => {
        const app = defineActorApp({ actors: [] }).use(loggerPlugin).use(tracingPlugin);
        void app.defineActor({
            type: 'Both',
            unguarded: true,
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                bump() {
                    expectTypeOf(ctx.log).toEqualTypeOf<Logger>();
                    expectTypeOf(ctx.traceId).toEqualTypeOf<string>();
                    return ++ctx.state.n;
                }
            })
        });
    });

    it('survives destructuring — the documented `export const { defineActor }` shape', () => {
        const { defineActor: bound } = defineActorApp({ actors: [] }).use(loggerPlugin);
        void bound({
            type: 'Destructured',
            unguarded: true,
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                bump() {
                    expectTypeOf(ctx.log).toEqualTypeOf<Logger>();
                    return ++ctx.state.n;
                }
            })
        });
    });

    it('does NOT leak a plugin member into an app that never used it', () => {
        const app = defineActorApp({ actors: [] });
        void app.defineActor({
            type: 'Bare',
            unguarded: true,
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                bump() {
                    // @ts-expect-error no plugin contributed `log`
                    void ctx.log;
                    return ++ctx.state.n;
                }
            })
        });
    });

    it('still produces a plain ActorDefinition the host accepts', () => {
        const app = defineActorApp({ actors: [] }).use(loggerPlugin);
        const Logged = app.defineActor({
            type: 'Plain',
            unguarded: true,
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                bump() {
                    return ++ctx.state.n;
                }
            })
        });
        expectTypeOf(Logged).toMatchTypeOf<AnyActorDefinition>();
        // and the client inference is unchanged by the extension
        expectTypeOf(actor(Logged, 'k').bump).returns.toEqualTypeOf<Promise<number>>();
    });
});
