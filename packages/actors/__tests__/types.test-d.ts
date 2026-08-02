/**
 * Typing contracts: the client proxy's inference from the definition —
 * methods promise-wrapped, streams as AsyncIterable, `.with` preserved.
 */
import { describe, expectTypeOf, it } from 'vitest';
import { actor, defineActor, defineWorker, publishTopic, topic } from '@sigx/actors';
import type {
    ActorContext,
    ActorContextBase,
    ActorPlacementStrategy,
    AnyActorDefinition,
    Topic,
    TopicEvent,
    TopicPublishReport,
    WorkerContext
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

    it('migrateState does not widen the inferred state type', () => {
        void defineActor({
            type: 'Migrating',
            unguarded: true,
            state: () => ({ v: 2, items: [] as string[] }),
            // Returns `any` — what casting a `stored: unknown` naturally
            // produces, and what the issue's own example does. Were this an
            // inference site, `S` would become `any` and every assertion
            // below would silently evaporate.
            migrateState: (stored) => stored as any,
            methods: (ctx) => ({
                size() {
                    expectTypeOf(ctx.state).toEqualTypeOf<{ v: number; items: string[] }>();
                    // @ts-expect-error unknown state field — proof S was not widened
                    void ctx.state.missing;
                    return ctx.state.items.length;
                }
            })
        });
    });

    it('migrateState is a check site: a wrong return shape errors on the hook', () => {
        void defineActor({
            type: 'BadMigrateShape',
            unguarded: true,
            state: () => ({ v: 2, items: [] as string[] }),
            // @ts-expect-error the wrong shape is refused HERE rather than
            // being taken as a second candidate for `S`. Drop the `NoInfer`
            // and this line compiles — which fails this expect-error, so the
            // pin cuts both ways.
            migrateState: () => ({ nope: true }),
            methods: () => ({})
        });
    });

    it('migrateState may not be async — the sync rule is the type', () => {
        void defineActor({
            type: 'AsyncMigrate',
            unguarded: true,
            state: () => ({ n: 0 }),
            // @ts-expect-error `Promise<S>` is not `S`
            migrateState: async (stored: unknown) => stored as { n: number },
            methods: () => ({})
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

    it('the app-bound defineWorker threads plugin Ext into the worker ctx', () => {
        const app = defineActorApp({ actors: [] }).use(loggerPlugin);
        const Worked = app.defineWorker({
            type: 'Worked',
            unguarded: true,
            methods: (ctx) => ({
                async run() {
                    expectTypeOf(ctx.log).toEqualTypeOf<Logger>();
                    expectTypeOf(ctx.key).toEqualTypeOf<string>();
                    // still a WORKER ctx: the persistence surface stays away
                    // @ts-expect-error workers have no state, plugins or not
                    void ctx.state;
                    return 1;
                }
            })
        });
        expectTypeOf(Worked).toMatchTypeOf<AnyActorDefinition>();
        expectTypeOf(actor(Worked, 'k').run).returns.toEqualTypeOf<Promise<number>>();
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

// ---------------------------------------------------------------------------
// Stateless workers: the identity-bound surface is typed AWAY — absent from
// WorkerOptions and from the ctx the factories receive — while method/stream
// inference and the client mapping work exactly as for defineActor.

describe('defineWorker typing', () => {
    const Resize = defineWorker({
        type: 'Resize',
        unguarded: true,
        maxLocal: 4,
        methods: (ctx) => ({
            async run(input: string) {
                expectTypeOf(ctx.key).toEqualTypeOf<string>();
                return input.length;
            }
        }),
        streams: () => ({
            async *chunks(n: number) {
                for (let i = 0; i < n; i++) yield i;
            }
        })
    });

    it('flows through actor() with the ordinary client mapping', () => {
        const client = actor(Resize, 'any');
        expectTypeOf(client.run).parameters.toEqualTypeOf<[string]>();
        expectTypeOf(client.run).returns.toEqualTypeOf<Promise<number>>();
        expectTypeOf(client.chunks).returns.toEqualTypeOf<AsyncIterable<number>>();
        expectTypeOf(Resize).toMatchTypeOf<AnyActorDefinition>();
    });

    it('types away the identity-bound ctx members', () => {
        void defineWorker({
            type: 'CtxShape',
            unguarded: true,
            methods: (ctx) => ({
                async probe() {
                    // still there: addressing, composition, lifecycle
                    expectTypeOf(ctx.key).toEqualTypeOf<string>();
                    void ctx.actor;
                    void ctx.publish;
                    void ctx.timer;
                    void ctx.deactivate;
                    expectTypeOf(ctx.abortSignal).toEqualTypeOf<AbortSignal>();
                    // typed away: the persistence surface
                    // @ts-expect-error workers have no state
                    void ctx.state;
                    // @ts-expect-error workers have no save
                    void ctx.save;
                    // @ts-expect-error workers have no clearState
                    void ctx.clearState;
                    // @ts-expect-error workers have no reminders
                    void ctx.reminders;
                    // @ts-expect-error workers have no tasks
                    void ctx.tasks;
                    // @ts-expect-error workers have no snapshot
                    void ctx.snapshot;
                    // @ts-expect-error workers have no change feed
                    void ctx.changes;
                }
            })
        });
        expectTypeOf<WorkerContext>().not.toHaveProperty('state');
    });

    it('cannot express the identity-bound options', () => {
        void defineWorker({
            type: 'NoState',
            unguarded: true,
            // @ts-expect-error workers have no state factory
            state: () => ({ n: 0 }),
            methods: () => ({})
        });
        void defineWorker({
            type: 'NoSubs',
            unguarded: true,
            // @ts-expect-error workers cannot subscribe to topics
            subscriptions: { chat: () => {} },
            methods: () => ({})
        });
        void defineWorker({
            type: 'NoTasks',
            unguarded: true,
            // @ts-expect-error workers have no durable tasks
            tasks: () => ({}),
            methods: () => ({})
        });
        void defineWorker({
            type: 'NoPlacement',
            unguarded: true,
            // @ts-expect-error workers always place locally
            placement: { name: 'prefer-local' },
            methods: () => ({})
        });
        void defineWorker({
            type: 'NoReentrancy',
            unguarded: true,
            // @ts-expect-error workers are never reentrant
            reentrant: true,
            methods: () => ({})
        });
        void defineWorker({
            type: 'NoPersistence',
            unguarded: true,
            // @ts-expect-error workers persist nothing
            persistence: 'explicit',
            methods: () => ({})
        });
    });
});

describe('topics typing', () => {
    const chat = topic<{ from: string; text: string }>('chat');

    it('phantom-types the publish payload', () => {
        expectTypeOf(topic<number>('n')).toEqualTypeOf<Topic<number>>();
        const client = defineActor({
            type: 'Publisher',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                async post() {
                    // payload must match the topic's phantom type
                    // @ts-expect-error wrong payload type
                    void ctx.publish(chat, 42);
                    return ctx.publish(chat, { from: 'a', text: 'hi' });
                }
            })
        });
        void client;
        // host-side publish carries the same constraint
        expectTypeOf(publishTopic<number>).parameter(1).toEqualTypeOf<number>();
        expectTypeOf(publishTopic(chat, { from: 'a', text: 'hi' })).toEqualTypeOf<
            Promise<TopicPublishReport>
        >();
    });

    it('types subscription handlers with the actor state and keeps them off the client', () => {
        const Sub = defineActor({
            type: 'Sub',
            unguarded: true,
            state: () => ({ seen: 0 }),
            methods: (ctx) => ({
                async seen() {
                    return ctx.state.seen;
                }
            }),
            subscriptions: {
                chat: (ctx, event) => {
                    expectTypeOf(ctx.state.seen).toEqualTypeOf<number>();
                    expectTypeOf(event).toEqualTypeOf<TopicEvent>();
                },
                mapped: {
                    key: (topicKey) => {
                        expectTypeOf(topicKey).toEqualTypeOf<string>();
                        return 'aggregate';
                    },
                    handle: (ctx) => void ctx.state.seen++
                }
            }
        });
        const client = actor(Sub, 'k');
        expectTypeOf(client.seen).returns.toEqualTypeOf<Promise<number>>();
        // @ts-expect-error subscription handlers are not client-callable
        void client.chat;
    });
});
