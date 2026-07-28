/**
 * `defineActorApp` — plugin composition. The point of the app layer is that
 * the seams `createSilo` keeps exclusive (one placement, one storage, one
 * `PlacementBindings`) become composable, so most of what is asserted here
 * is ORDERING and the failure posture of each hook.
 */
import { describe, expect, it, vi } from 'vitest';
import {
    defineActor,
    peekSilo,
    type ActorDispatcher,
    type ActorPlacement,
    type ActorPlacementStrategy,
    type ActorStorage
} from '@sigx/actors';
import {
    defineActorApp,
    memoryStorage,
    type ActorPlugin,
    type DispatchMiddleware,
    type PluginRegistry
} from '@sigx/actors/silo';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        }
    })
});

/** Terse plugin literal — every test builds its behaviour from a setup fn. */
function plugin(name: string, setup: (registry: PluginRegistry) => void): ActorPlugin {
    return { name, setup };
}

/** A correct middleware: records the call AND forwards streaming. */
function tracing(label: string, seen: string[]): DispatchMiddleware {
    return (next) => ({
        dispatch: async (ref, method, args, call) => {
            seen.push(`${label}:in`);
            const result = await next.dispatch(ref, method, args, call);
            seen.push(`${label}:out`);
            return result;
        },
        ...(next.dispatchStream && {
            dispatchStream: (ref, method, args, call) =>
                next.dispatchStream!(ref, method, args, call)
        })
    });
}

describe('defineActorApp — lifecycle hook composition', () => {
    it('runs every plugin beforeActivate, in registration order', async () => {
        const seen: string[] = [];
        const app = defineActorApp({ actors: [Counter], defaults: quiet })
            .use(plugin('a', (r) => r.onBeforeActivate((ref) => void seen.push(`a:${ref.key}`))))
            .use(plugin('b', (r) => r.onBeforeActivate((ref) => void seen.push(`b:${ref.key}`))));

        const silo = await app.start();
        try {
            await silo.actor(Counter, 'k1').increment(1);
            expect(seen).toEqual(['a:k1', 'b:k1']);
        } finally {
            await app.stop();
        }
    });

    it('brackets plugin hooks with the placement own hooks', async () => {
        // This is the case that is IMPOSSIBLE today: a placement that claims
        // a directory entry AND a plugin that wants the same hook.
        const seen: string[] = [];
        let local: ActorDispatcher | null = null;
        const placement: ActorPlacement = {
            dispatcherFor: () => local!,
            bind(localDispatcher) {
                local = localDispatcher;
                return {
                    beforeActivate: () => void seen.push('placement:claim'),
                    afterDeactivate: () => void seen.push('placement:release')
                };
            }
        };

        const app = defineActorApp({ actors: [Counter], defaults: quiet })
            .use(plugin('cluster', (r) => r.setPlacement(() => placement)))
            .use(
                plugin('audit', (r) => {
                    r.onBeforeActivate(() => void seen.push('audit:before'));
                    r.onAfterDeactivate(() => void seen.push('audit:after'));
                })
            );

        const silo = await app.start();
        try {
            await silo.actor(Counter, 'k').increment(1);
            await silo.deactivate({ type: 'Counter', key: 'k' });
            // Claim first, release last — plugin hooks always see an
            // activation the placement already owns.
            expect(seen).toEqual([
                'placement:claim',
                'audit:before',
                'audit:after',
                'placement:release'
            ]);
        } finally {
            await app.stop();
        }
    });

    it('lets a beforeActivate hook refuse the activation', async () => {
        const app = defineActorApp({ actors: [Counter], defaults: quiet }).use(
            plugin('gate', (r) =>
                r.onBeforeActivate((ref) => {
                    if (ref.key === 'denied') throw new Error('not admitted');
                })
            )
        );
        const silo = await app.start();
        try {
            await expect(silo.actor(Counter, 'denied').increment(1)).rejects.toThrow();
            // A refusal must not poison the silo for other keys.
            await expect(silo.actor(Counter, 'ok').increment(1)).resolves.toBe(1);
        } finally {
            await app.stop();
        }
    });

    it('swallows an afterDeactivate throw so later hooks still run', async () => {
        const seen: string[] = [];
        const app = defineActorApp({ actors: [Counter], defaults: quiet })
            .use(
                plugin('boom', (r) =>
                    r.onAfterDeactivate(() => {
                        throw new Error('release failed');
                    })
                )
            )
            .use(plugin('after', (r) => r.onAfterDeactivate(() => void seen.push('after'))));

        const silo = await app.start();
        try {
            await silo.actor(Counter, 'k').increment(1);
            await silo.deactivate({ type: 'Counter', key: 'k' });
            // 'after' is registered second, so it runs FIRST (mirror order)
            // and the throwing hook must not have prevented it.
            expect(seen).toEqual(['after']);
        } finally {
            await app.stop();
        }
    });
});

describe('defineActorApp — custom placements', () => {
    it('lets a custom placement read the strategy an actor declared', async () => {
        // Orleans's attribute form: the strategy lives ON the actor, not in
        // a central map the placement had to be constructed with.
        const pinned: ActorPlacementStrategy = { name: 'prefer-local' };
        const Session = defineActor({
            type: 'Session',
            unguarded: true,
            placement: pinned,
            state: () => ({ hits: 0 }),
            methods: (ctx) => ({
                async touch() {
                    return ++ctx.state.hits;
                }
            })
        });

        const resolved: string[] = [];
        let local: ActorDispatcher | null = null;
        const app = defineActorApp({ actors: [Session], defaults: quiet }).use(
            plugin('custom-placement', (r) =>
                // The factory runs once the silo exists, so `definition()`
                // is available to resolve per-type strategies.
                r.setPlacement((ctx) => ({
                    dispatcherFor: (ref) => {
                        const def = ctx.definition(ref.type);
                        const strategy =
                            def && !(def instanceof Promise)
                                ? def.__sigxActor.placement
                                : undefined;
                        resolved.push(`${ref.type}:${strategy?.name ?? 'default'}`);
                        return local!;
                    },
                    bind(localDispatcher) {
                        local = localDispatcher;
                    }
                }))
            )
        );

        const silo = await app.start();
        try {
            await expect(silo.actor(Session, 's1').touch()).resolves.toBe(1);
            expect(resolved).toContain('Session:prefer-local');
        } finally {
            await app.stop();
        }
    });
});

describe('defineActorApp — placement is exclusive', () => {
    it('throws naming both plugins when two claim the placement', () => {
        const placement: ActorPlacement = { dispatcherFor: () => ({ dispatch: async () => null }) };
        const app = defineActorApp({ actors: [], defaults: quiet })
            .use(plugin('cluster', (r) => r.setPlacement(() => placement)))
            .use(plugin('durable-objects', (r) => r.setPlacement(() => placement)));

        expect(() => app.routes).toThrow(/"cluster".*"durable-objects"/s);
    });
});

describe('defineActorApp — dispatch middleware', () => {
    it('composes outside-in, first registered outermost', async () => {
        const seen: string[] = [];
        const app = defineActorApp({ actors: [Counter], defaults: quiet })
            .use(
                plugin('outer', (r) => r.useDispatch(tracing('outer', seen)))
            )
            .use(plugin('inner', (r) => r.useDispatch(tracing('inner', seen))));

        const silo = await app.start();
        try {
            await expect(silo.actor(Counter, 'k').increment(2)).resolves.toBe(2);
            expect(seen).toEqual(['outer:in', 'inner:in', 'inner:out', 'outer:out']);
        } finally {
            await app.stop();
        }
    });

    it('keeps streams working through middleware that forwards', async () => {
        const Ticker = defineActor({
            type: 'Ticker',
            unguarded: true,
            state: () => ({ n: 0 }),
            methods: () => ({}),
            streams: () => ({
                async *counts() {
                    yield 1;
                    yield 2;
                }
            })
        });
        const app = defineActorApp({ actors: [Ticker], defaults: quiet }).use(
            plugin('trace', (r) => r.useDispatch(tracing('t', [])))
        );
        const silo = await app.start();
        try {
            const got: number[] = [];
            for await (const n of silo.actor(Ticker, 'k').counts()) got.push(n);
            expect(got).toEqual([1, 2]);
        } finally {
            await app.stop();
        }
    });

    it('warns when a middleware drops dispatchStream', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        // The footgun: `dispatchStream` is optional, so this type-checks.
        const app = defineActorApp({ actors: [Counter], defaults: quiet }).use(
            plugin('lossy', (r) =>
                r.useDispatch((next) => ({
                    dispatch: (ref, method, args, call) =>
                        next.dispatch(ref, method, args, call)
                }))
            )
        );
        const silo = await app.start();
        try {
            await silo.actor(Counter, 'k').increment(1);
            expect(warn).toHaveBeenCalledWith(
                expect.stringContaining('without `dispatchStream`')
            );
        } finally {
            await app.stop();
            warn.mockRestore();
        }
    });
});

describe('defineActorApp — storage decoration', () => {
    it('applies decorators so the last registered is outermost', async () => {
        const seen: string[] = [];
        const tag =
            (label: string) =>
            (inner: ActorStorage): ActorStorage => ({
                load: (type, key) => {
                    seen.push(`${label}:load`);
                    return inner.load(type, key);
                },
                save: (type, key, state, etag) => inner.save(type, key, state, etag),
                clear: (type, key, etag) => inner.clear(type, key, etag)
            });

        const app = defineActorApp({
            actors: [Counter],
            storage: memoryStorage(),
            defaults: quiet
        })
            .use(plugin('first', (r) => r.decorateStorage(tag('first'))))
            .use(plugin('second', (r) => r.decorateStorage(tag('second'))));

        const silo = await app.start();
        try {
            await silo.actor(Counter, 'k').increment(1);
            expect(seen).toEqual(['second:load', 'first:load']);
        } finally {
            await app.stop();
        }
    });
});

describe('defineActorApp — context extension', () => {
    it('merges every plugin extension onto ctx', async () => {
        const probe = defineActor({
            type: 'Probe',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                async read() {
                    const ext = ctx as unknown as { log: string; who: string };
                    return `${ext.log}/${ext.who}`;
                }
            })
        });

        const app = defineActorApp({ actors: [probe], defaults: quiet })
            .use(plugin('log', (r) => r.extendContext(() => ({ log: 'logger' }))))
            .use(plugin('who', (r) => r.extendContext((ref) => ({ who: ref.key }))));

        const silo = await app.start();
        try {
            await expect(silo.actor(probe, 'abc').read()).resolves.toBe('logger/abc');
        } finally {
            await app.stop();
        }
    });

    it('never lets a plugin shadow a built-in ctx member', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const probe = defineActor({
            type: 'Probe2',
            unguarded: true,
            state: () => ({ count: 7 }),
            methods: (ctx) => ({
                async read() {
                    return ctx.state.count;
                }
            })
        });
        const app = defineActorApp({ actors: [probe], defaults: quiet }).use(
            plugin('rogue', (r) => r.extendContext(() => ({ state: 'hijacked', save: 'nope' })))
        );

        const silo = await app.start();
        try {
            await expect(silo.actor(probe, 'k').read()).resolves.toBe(7);
            expect(warn).toHaveBeenCalledWith(
                expect.stringContaining('tried to overwrite the built-in ctx.state')
            );
        } finally {
            await app.stop();
            warn.mockRestore();
        }
    });
});

describe('defineActorApp — routes and app lifecycle', () => {
    it('collects plugin routes', () => {
        const app = defineActorApp({ actors: [], defaults: quiet }).use(
            plugin('mount', (r) =>
                r.route({
                    name: 'cluster:internal',
                    match: (request) => new URL(request.url).pathname.startsWith('/_sigx/silo/'),
                    handle: async () => new Response('ok')
                })
            )
        );
        expect(app.routes.map((route) => route.name)).toEqual(['cluster:internal']);
    });

    it('runs onStart in order and onStop in reverse, after the drain', async () => {
        const seen: string[] = [];
        const app = defineActorApp({ actors: [Counter], defaults: quiet })
            .use(
                plugin('a', (r) => {
                    r.onStart(() => void seen.push('start:a'));
                    r.onStop(() => void seen.push('stop:a'));
                })
            )
            .use(
                plugin('b', (r) => {
                    r.onStart(() => void seen.push('start:b'));
                    r.onStop(() => void seen.push('stop:b'));
                })
            );

        const silo = await app.start();
        // The silo is live by the time onStart runs.
        expect(seen).toEqual(['start:a', 'start:b']);
        await silo.actor(Counter, 'k').increment(1);
        await app.stop();
        expect(seen).toEqual(['start:a', 'start:b', 'stop:b', 'stop:a']);
    });

    it('is idempotent and refuses plugins once sealed', async () => {
        const app = defineActorApp({ actors: [], defaults: quiet });
        const first = await app.start();
        expect(await app.start()).toBe(first);
        expect(app.silo).toBe(first);
        expect(() => app.use(plugin('late', () => {}))).toThrow(/after the app has been started/);
        await app.stop();
        await app.stop();
    });

    it('accepts a registry supplied by a host', async () => {
        // The point: an app module can omit its registry, so it imports
        // nothing Vite-specific and loads under any runtime. The Vite
        // plugin hands over the one it already builds; a plain-Node entry
        // names its actors.
        const app = defineActorApp({ defaults: quiet });
        expect(app.hasActors).toBe(false);
        app.withActors([Counter]);
        expect(app.hasActors).toBe(true);

        const silo = await app.start();
        try {
            await expect(silo.actor(Counter, 'k').increment(2)).resolves.toBe(2);
        } finally {
            await app.stop();
        }
    });

    it('refuses to replace a registry the author configured', () => {
        const app = defineActorApp({ actors: [Counter], defaults: quiet });
        expect(app.hasActors).toBe(true);
        // A host must never silently override what the app declared.
        expect(() => app.withActors([])).toThrow(/already has actors/);
    });

    it('says so clearly when nobody supplied a registry', async () => {
        const app = defineActorApp({ defaults: quiet });
        await expect(app.start()).rejects.toThrow(/no actors/);
    });

    it('refuses withActors once the app is sealed', async () => {
        const app = defineActorApp({ defaults: quiet });
        app.withActors([Counter]);
        await app.start();
        try {
            // By now the registry is baked into a running silo.
            expect(() => app.withActors([Counter])).toThrow(/already has actors/);
            const fresh = defineActorApp({ defaults: quiet });
            void fresh.routes; // seals it
            expect(() => fresh.withActors([Counter])).toThrow(/after the app has been started/);
        } finally {
            await app.stop();
        }
    });

    it('clears the silo on stop and refuses a restart', async () => {
        const app = defineActorApp({ actors: [], defaults: quiet });
        await app.start();
        await app.stop();
        // A stopped app must not keep handing out a dead silo...
        expect(app.silo).toBeNull();
        // ...nor silently hand back the old one from a fresh start(): a
        // plugin placement's identity is minted per run.
        await expect(app.start()).rejects.toThrow(/cannot be restarted/);
    });

    it('stays retryable when a start fails, and leaks nothing', async () => {
        let attempts = 0;
        const rolledBack: string[] = [];
        const app = defineActorApp({ actors: [], defaults: quiet }).use(
            plugin('flaky', (r) => {
                r.onStart(() => {
                    if (++attempts === 1) throw new Error('first start fails');
                });
                r.onStop(() => void rolledBack.push('onStop'));
            })
        );
        await expect(app.start()).rejects.toThrow('first start fails');
        // The silo was ALREADY started when the hook threw. It must have
        // been torn back down — the ambient seam is the witness, since
        // silo.stop() is what clears it. A leaked silo would still be
        // stamped here (and still running its sweeper and reminder tick).
        expect(peekSilo()).toBeUndefined();
        expect(app.silo).toBeNull();
        expect(rolledBack).toEqual(['onStop']);

        // The rejection must not be cached — a second start really retries.
        const silo = await app.start();
        expect(attempts).toBe(2);
        expect(app.silo).toBe(silo);
        await app.stop();
    });

    it('keeps the silo reachable and retryable when teardown fails', async () => {
        let failStop = true;
        const placement: ActorPlacement = {
            dispatcherFor: () => local!,
            bind(localDispatcher) {
                local = localDispatcher;
            },
            stop() {
                // A cluster membership leave against a dead store.
                if (failStop) throw new Error('leave failed');
            }
        };
        let local: ActorDispatcher | null = null;

        const stopped: string[] = [];
        const app = defineActorApp({ actors: [Counter], defaults: quiet })
            .use(plugin('cluster', (r) => r.setPlacement(() => placement)))
            .use(plugin('hooks', (r) => r.onStop(() => void stopped.push('onStop'))));

        const silo = await app.start();
        await expect(app.stop()).rejects.toThrow('leave failed');
        // Must not claim to be down when it is not.
        expect(app.silo).toBe(silo);
        // ...and the onStop hooks must not have run on a failed teardown.
        expect(stopped).toEqual([]);

        failStop = false;
        await app.stop();
        expect(app.silo).toBeNull();
        expect(stopped).toEqual(['onStop']);
    });

    it('is a no-op to stop an app that never started', async () => {
        const app = defineActorApp({ actors: [], defaults: quiet });
        await app.stop();
        // Stopping something that was never running must not seal it.
        const silo = await app.start();
        expect(silo).toBeTruthy();
        await app.stop();
    });
});

describe('defineActorApp — context extension safety', () => {
    it('refuses prototype-mutating keys from a plugin', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const probe = defineActor({
            type: 'Probe3',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                async read() {
                    // Neither the ctx itself nor any plain object may have
                    // picked the payload up through a prototype.
                    const viaCtx = (ctx as unknown as Record<string, unknown>).pwned;
                    const viaObject = ({} as Record<string, unknown>).pwned;
                    return viaCtx ?? viaObject ?? 'clean';
                }
            })
        });
        // An own `__proto__` key, exactly as JSON.parse would produce it.
        const hostile = JSON.parse('{"__proto__": {"pwned": "yes"}}') as object;
        const app = defineActorApp({ actors: [probe], defaults: quiet }).use(
            plugin('rogue', (r) => r.extendContext(() => hostile))
        );

        const silo = await app.start();
        try {
            await expect(silo.actor(probe, 'k').read()).resolves.toBe('clean');
            expect(({} as Record<string, unknown>).pwned).toBeUndefined();
            expect(warn).toHaveBeenCalledWith(expect.stringContaining('unsafe key "__proto__"'));
        } finally {
            await app.stop();
            warn.mockRestore();
        }
    });
});
