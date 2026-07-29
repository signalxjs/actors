/**
 * `createWorkerHandler` — the Worker half: a `fetch` that resolves each ref
 * to its Durable Object and forwards.
 *
 * It exists because `env` only exists inside `fetch`. The app therefore has
 * to be built lazily and memoized, and `createFetchHandler` answers 503 while
 * `app.silo` is null — so `app.start()` must be awaited before delegating.
 * That is enough boilerplate, with enough ways to get it subtly wrong, to be
 * worth owning.
 */
import type { ActorStorage } from '@sigx/actors';
import {
    defineActorApp,
    type ActorApp,
    type ActorAppOptions
} from '@sigx/actors/silo';
import { createFetchHandler, type FetchHandlerOptions } from '@sigx/actors/server';
import { durableObjects, type DurableObjectPlacementOptions } from './placement';
import type { DurableObjectNamespaceLike } from './types';

/**
 * Storage for a silo that never activates anything.
 *
 * The Worker's silo routes every ref to a Durable Object, so it holds no
 * state — and in-memory storage there would be a silent lie, with
 * `createSilo`'s "state dies with the process" dev warning pointing at a
 * problem that does not exist while hiding one that would.
 */
export function unhostedStorage(): ActorStorage {
    // Async, not a synchronous throw: `ActorStorage` promises a promise, and
    // a caller doing `storage.load(...).catch(...)` must not be blown up
    // before it has one to attach to.
    const refuse = async (): Promise<never> => {
        throw new Error(
            '[sigx actors-cloudflare] the Worker silo tried to read or write actor state, ' +
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
                namespace: options.namespace(env),
                // No `isSelf`: the Worker hosts nothing, so every ref is
                // somebody else's object.
                siloId: 'cf-worker',
                ...options.placement
            })
        );
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
