/**
 * `createWorkerHandler` — the Worker half: a `fetch` that resolves each ref
 * to its Durable Object and forwards.
 *
 * It exists because `env` only exists inside `fetch`. The app therefore has
 * to be built lazily and memoized, and `createFetchHandler` answers 503 while
 * `app.host` is null — so `app.start()` must be awaited before delegating.
 * That is enough boilerplate, with enough ways to get it subtly wrong, to be
 * worth owning.
 */
import type { ActorStorage } from '@sigx/actors';
import {
    defineActorApp,
    type ActorApp,
    type ActorAppOptions
} from '@sigx/actors/host';
import { createFetchHandler, type FetchHandlerOptions } from '@sigx/actors/server';
import {
    durableObjects,
    durableObjectStubResolver,
    type DurableObjectPlacementOptions
} from './placement';
import { objectSocketRoute, workerSocket, type WorkerSocketOptions } from './socket';
import type { DurableObjectNamespaceLike } from './types';

/**
 * Storage for a host that never activates anything.
 *
 * The Worker's host routes every ref to a Durable Object, so it holds no
 * state — and in-memory storage there would be a silent lie, with
 * `createHost`'s "state dies with the process" dev warning pointing at a
 * problem that does not exist while hiding one that would.
 */
export function unhostedStorage(): ActorStorage {
    // Async, not a synchronous throw: `ActorStorage` promises a promise, and
    // a caller doing `storage.load(...).catch(...)` must not be blown up
    // before it has one to attach to.
    const refuse = async (): Promise<never> => {
        throw new Error(
            '[sigx actors-cloudflare] the Worker host tried to read or write actor state, ' +
                'but it never hosts an activation — every actor lives in a Durable Object. ' +
                'Reaching this means a call was dispatched locally instead of to an object: ' +
                'check that the Durable Object namespace binding in wrangler.jsonc matches ' +
                'the one passed to createWorkerHandler({ namespace }).'
        );
    };
    return { load: refuse, save: refuse, clear: refuse };
}

export interface WorkerHandlerOptions<Env = unknown> {
    /** Registry, unless the `app` factory supplies one via `withActors`. */
    actors?: ActorAppOptions['actors'];
    /** The Durable Object binding every ref routes through. */
    namespace(env: Env): DurableObjectNamespaceLike;
    /** Add plugins. Receives the base options it must pass on. */
    app?(base: ActorAppOptions): ActorApp;
    /** Must match every Durable Object's, byte for byte. */
    placement?: Pick<
        DurableObjectPlacementOptions,
        'objectName' | 'locationHint' | 'jurisdiction' | 'base'
    >;
    /** Forwarded to the public mount — `base`, `fallback`, guards, caps. */
    fetch?: FetchHandlerOptions;
    /**
     * Client-facing WebSocket surface, in one of two termination modes:
     *
     * - `terminate: 'worker'` (default) — sugar over
     *   `app.use(workerSocket(...))`: the WORKER upgrades at `{path}`, one
     *   multiplexed socket per client, every call re-dispatched through
     *   placement. It does NOT fix #47 — see `workerSocket`'s doc.
     * - `terminate: 'object'` — the upgrade at `{path}/{type}/{key}` is
     *   FORWARDED to that actor's Durable Object, which must be built with
     *   `createHostDurableObject({ socket })`. One socket per actor, teardown
     *   local to the object (the #47 answer), hibernation-ready. The session
     *   options ride on the OBJECT side in that mode, not here.
     *
     * The two paths differ by arity, so mounting both — this option for one
     * mode plus `app.use(workerSocket(...))` for the other — composes.
     *
     * A discriminated union on purpose: in `'object'` mode only `path` is
     * meaningful on the Worker, and the type refusing the session options
     * here is what stops them being configured on the side that never runs
     * the session.
     */
    socket?:
        | (WorkerSocketOptions & { terminate?: 'worker' })
        | { terminate: 'object'; path?: string };
}

export interface WorkerHandler<Env> {
    fetch(request: Request, env: Env, ctx?: unknown): Promise<Response>;
}

export function createWorkerHandler<Env = unknown>(
    options: WorkerHandlerOptions<Env>
): WorkerHandler<Env> {
    let started: Promise<(request: Request) => Promise<Response>> | null = null;

    const boot = async (env: Env): Promise<(request: Request) => Promise<Response>> => {
        const base: ActorAppOptions = { storage: unhostedStorage() };
        const app = options.app?.(base) ?? defineActorApp(base);
        if (!app.hasActors) {
            if (!options.actors) {
                throw new Error(
                    '[sigx actors-cloudflare] no actors registered — pass `actors` to ' +
                        'createWorkerHandler(), or call withActors() in the `app` factory.'
                );
            }
            app.withActors(options.actors);
        }
        app.use(
            durableObjects({
                // The BINDING is captured once and reused. That is safe:
                // what workerd refuses to carry across requests is a
                // Durable Object STUB, which the placement therefore
                // derives fresh on every dispatch and never caches. An
                // earlier version of this read the binding from a
                // per-isolate mutable slot instead, which was both
                // unnecessary and racy — a Worker isolate serves several
                // requests concurrently, so a second could overwrite the
                // slot while the first was still dispatching.
                namespace: options.namespace(env),
                // No `isSelf`: the Worker hosts nothing, so every ref is
                // somebody else's object.
                hostId: 'cf-worker',
                ...options.placement
            })
        );
        if (options.socket) {
            const config = options.socket;
            if (config.terminate === 'object') {
                if (__DEV__) {
                    // The union already refuses these in TS; the warning is
                    // for JS callers, whose session options would otherwise
                    // vanish silently — they belong on
                    // createHostDurableObject({ socket }), where the session
                    // actually runs.
                    const ignored = Object.keys(config).filter(
                        (key) => key !== 'path' && key !== 'terminate'
                    );
                    if (ignored.length > 0) {
                        console.warn(
                            `[sigx actors-cloudflare] socket: { terminate: 'object' } ignores ` +
                                `${ignored.join(', ')} on the Worker — session options belong ` +
                                `on createHostDurableObject({ socket }) in that mode.`
                        );
                    }
                }
                // Built HERE and not by the user, because only this handler
                // holds both `env` (the namespace binding) and the placement
                // options — the resolver must derive object names exactly as
                // the placement does, or the upgrade lands in an object that
                // refuses it.
                const route = objectSocketRoute({
                    resolver: durableObjectStubResolver({
                        namespace: options.namespace(env),
                        ...options.placement
                    }),
                    ...(config.path !== undefined ? { path: config.path } : {})
                });
                app.use({
                    name: 'cloudflare:object-socket',
                    setup(registry) {
                        registry.route(route);
                    }
                });
            } else {
                const { terminate: _terminate, ...socket } = config;
                app.use(workerSocket(socket));
            }
        }
        const handler = createFetchHandler(app, options.fetch);
        await app.start();
        return handler;
    };

    return {
        async fetch(request: Request, env: Env): Promise<Response> {
            // Memoized per isolate, and a rejection is never cached — a
            // failed start stays retryable on the next request rather than
            // poisoning the isolate for its lifetime.
            const handler = await (started ??= boot(env).catch((error: unknown) => {
                started = null;
                throw error;
            }));
            return handler(request);
        }
    };
}
