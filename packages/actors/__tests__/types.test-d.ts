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
import { defineActorApp, type ActorApp, type ActorPlugin } from '@sigx/actors/host';
import type { ClusterPlugin, PlacementPolicy } from '@sigx/actors/cluster';

const Cart = defineActor({
    type: 'Cart',
    allowAnonymous: true,
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

    it('.with({ oneWay: true }) narrows methods to Promise<void> and streams to never', () => {
        const oneWay = actor(Cart, 'k').with({ oneWay: true });
        expectTypeOf(oneWay.add).parameters.toEqualTypeOf<[string, Date]>();
        expectTypeOf(oneWay.add).returns.toEqualTypeOf<Promise<void>>();
        expectTypeOf(oneWay.size).returns.toEqualTypeOf<Promise<void>>();
        expectTypeOf(oneWay.watch).toEqualTypeOf<never>();
        // Composes with the other options without losing the narrowing.
        const composed = actor(Cart, 'k').with({
            oneWay: true,
            signal: new AbortController().signal
        });
        expectTypeOf(composed.add).returns.toEqualTypeOf<Promise<void>>();
    });

    it('the methods factory sees a typed state', () => {
        void defineActor({
            type: 'Typed',
            allowAnonymous: true,
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
            allowAnonymous: true,
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
            allowAnonymous: true,
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
            allowAnonymous: true,
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
            allowAnonymous: true,
            placement: { name: 'prefer-local' },
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                bump() {
                    return ++ctx.state.n;
                }
            })
        });
    });

    it('types the reentrancy union and the per-method map', () => {
        const base = {
            allowAnonymous: true as const,
            state: () => ({ n: 0 }),
            methods: () => ({
                get() {
                    return 0;
                }
            })
        };
        void defineActor({ ...base, type: 'R1', reentrant: true });
        void defineActor({ ...base, type: 'R2', reentrant: 'call-chain' });
        void defineActor({ ...base, type: 'R3', reentrant: 'always' });
        void defineActor({
            ...base,
            type: 'R4',
            // @ts-expect-error not a reentrancy mode
            reentrant: 'sometimes'
        });
        void defineActor({ ...base, type: 'R5', methodReentrancy: { get: 'always' } });
        void defineActor({
            ...base,
            type: 'R6',
            // @ts-expect-error the map only takes 'always'
            methodReentrancy: { get: 'sometimes' }
        });
    });
});

describe('plugin ctx extension inference', () => {
    it('types a single plugin addition inside the actor', () => {
        const app = defineActorApp({ actors: [] }).use(loggerPlugin);
        void app.defineActor({
            type: 'Logged',
            allowAnonymous: true,
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
            allowAnonymous: true,
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
            allowAnonymous: true,
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
            allowAnonymous: true,
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
            allowAnonymous: true,
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
            allowAnonymous: true,
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
// Placement narrowing (#58): a plugin that installs a placement backend
// carries the strategy type it understands, `.use()` threads it alongside
// `Ext`, and the app-bound `defineActor` accepts only that. The runtime
// `backend`-tag refusal stays the floor — this is the compile-time layer
// over it for the common authoring path.

declare const clusterPlugin: ClusterPlugin;

const clusterPolicy: PlacementPolicy = {
    name: 'prefer-local',
    backend: 'cluster',
    choose(_ref, _view, self) {
        return self;
    }
};

// A strategy tagged for another backend — the runtime ignores it silently
// on a cluster host, so the actor lands somewhere its author did not ask.
const foreignStrategy = { name: 'durable-object', backend: 'durable-objects' } as const;

const placedBase = {
    allowAnonymous: true as const,
    state: () => ({ n: 0 }),
    methods: () => ({
        get() {
            return 0;
        }
    })
};

describe('placement narrowing through the app-bound defineActor', () => {
    it('accepts a cluster PlacementPolicy on a cluster() app', () => {
        const app = defineActorApp({ actors: [] }).use(clusterPlugin);
        expectTypeOf(app).toMatchTypeOf<ActorApp<Record<never, never>, PlacementPolicy>>();
        void app.defineActor({ ...placedBase, type: 'P1', placement: clusterPolicy });
        // an untagged strategy with the right shape is a PlacementPolicy too
        void app.defineActor({
            ...placedBase,
            type: 'P2',
            placement: { name: 'mine', choose: (_r, _v, self) => self }
        });
        // and no placement at all is still fine
        void app.defineActor({ ...placedBase, type: 'P3' });
    });

    it('rejects a strategy for another backend, or an unusable one, on a cluster() app', () => {
        const app = defineActorApp({ actors: [] }).use(clusterPlugin);
        void app.defineActor({
            ...placedBase,
            type: 'P4',
            // @ts-expect-error tagged for a different backend
            placement: foreignStrategy
        });
        void app.defineActor({
            ...placedBase,
            type: 'P5',
            // @ts-expect-error untagged and no choose(): the runtime would throw
            placement: { name: 'my-strategy' }
        });
    });

    it('narrowing survives destructuring and composes with Ext', () => {
        const { defineActor: bound } = defineActorApp({ actors: [] })
            .use(loggerPlugin)
            .use(clusterPlugin);
        void bound({
            ...placedBase,
            type: 'P6',
            placement: clusterPolicy,
            methods: (ctx) => ({
                get() {
                    expectTypeOf(ctx.log).toEqualTypeOf<Logger>();
                    return 0;
                }
            })
        });
        void bound({
            ...placedBase,
            type: 'P7',
            // @ts-expect-error tagged for a different backend
            placement: foreignStrategy
        });
    });

    it('an app without a placement plugin, and the unbound defineActor, stay wide', () => {
        const app = defineActorApp({ actors: [] }).use(loggerPlugin);
        void app.defineActor({ ...placedBase, type: 'P8', placement: foreignStrategy });
        void app.defineActor({ ...placedBase, type: 'P9', placement: { name: 'my-strategy' } });
        void defineActor({ ...placedBase, type: 'P10', placement: foreignStrategy });
        void defineActor({ ...placedBase, type: 'P11', placement: clusterPolicy });
    });
});

// ---------------------------------------------------------------------------
// Stateless workers: the identity-bound surface is typed AWAY — absent from
// WorkerOptions and from the ctx the factories receive — while method/stream
// inference and the client mapping work exactly as for defineActor.

describe('defineWorker typing', () => {
    const Resize = defineWorker({
        type: 'Resize',
        allowAnonymous: true,
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
            allowAnonymous: true,
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
            allowAnonymous: true,
            // @ts-expect-error workers have no state factory
            state: () => ({ n: 0 }),
            methods: () => ({})
        });
        void defineWorker({
            type: 'NoSubs',
            allowAnonymous: true,
            // @ts-expect-error workers cannot subscribe to topics
            subscriptions: { chat: () => {} },
            methods: () => ({})
        });
        void defineWorker({
            type: 'NoTasks',
            allowAnonymous: true,
            // @ts-expect-error workers have no durable tasks
            tasks: () => ({}),
            methods: () => ({})
        });
        void defineWorker({
            type: 'NoPlacement',
            allowAnonymous: true,
            // @ts-expect-error workers always place locally
            placement: { name: 'prefer-local' },
            methods: () => ({})
        });
        void defineWorker({
            type: 'NoReentrancy',
            allowAnonymous: true,
            // @ts-expect-error workers are never reentrant
            reentrant: true,
            methods: () => ({})
        });
        void defineWorker({
            type: 'NoAlways',
            allowAnonymous: true,
            // @ts-expect-error workers are never reentrant, 'always' included
            reentrant: 'always',
            methods: () => ({})
        });
        void defineWorker({
            type: 'NoMethodReentrancy',
            allowAnonymous: true,
            // @ts-expect-error workers never interleave per method — they pool
            methodReentrancy: { m: 'always' },
            methods: () => ({})
        });
        void defineWorker({
            type: 'NoPersistence',
            allowAnonymous: true,
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
            allowAnonymous: true,
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
            allowAnonymous: true,
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
