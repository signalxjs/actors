/**
 * `@sigx/actors` — Orleans-style virtual actors for SignalX.
 *
 * The root entry is ISOMORPHIC and light: `defineActor` (a pure
 * declaration), `actor()` (typed client — wire proxy in the browser,
 * in-process dispatch on the server), and the shared types/errors. The
 * runtime lives in `@sigx/actors/silo`; this entry never imports it — the
 * server branch reaches the running silo through the `__SIGX_ACTOR_SILO__`
 * seam only.
 */
import type { ServerFnContext } from '@sigx/server';
import { isActorDefinition } from './define';
import { runGuards } from './guards';
import { resolveServerContext } from './context';
import { currentSilo } from './seam';
import type { ActorCallOptions, ActorClientWith, AnyActorDefinition } from './types';

export { defineActor, isActorDefinition } from './define';
export { currentSilo, peekSilo } from './seam';
export {
    ActorError,
    ActorDeadlockError,
    ActorActivationError,
    ActorStateConflictError,
    ActorMethodNotFoundError,
    SiloShutdownError,
    ActorCallTimeoutError,
    ActorWrongHostError,
    ActorUnreachableError,
    ActorStorageConflict,
    isActorError,
    isStorageConflict,
    type ActorErrorKind,
    type ActorErrorShape,
    type ActorOwnerHint
} from './errors';
export type {
    ActorCallContext,
    ActorCallOptions,
    ActorClient,
    ActorClientWith,
    ActorContext,
    ActorDefinition,
    ActorDispatcher,
    ActorOptions,
    ActorPlacement,
    ActorRef,
    ActorStorage,
    ActorStorageRecord,
    AnyActorDefinition,
    DeactivationReason,
    PlacementBindings,
    ReminderApi,
    Silo,
    SiloStats,
    TimerHandle,
    TimerOptions
} from './types';

/** The client-ref brand the build transform's `__actorRef` stubs carry. */
interface ClientRefLike {
    __sigxActor: string;
    __sigxActorProxy: (key: string, options?: ActorCallOptions) => object;
}

function isClientRef(value: unknown): value is ClientRefLike {
    return (
        typeof value === 'object' &&
        value !== null &&
        typeof (value as ClientRefLike).__sigxActorProxy === 'function'
    );
}

/**
 * The typed actor client — the virtual-actor entry point. Works everywhere:
 *
 *  - **browser**: `def` is the build-swapped client ref; calls go over the
 *    wire (`/_sigx/actor`) through the generic proxy.
 *  - **server** (serverFns, SSR render, scripts): `def` is the real
 *    definition; the actor's `use`/`methodUse` guard chains run against the
 *    ambient request context, then the call dispatches in-process through
 *    the running silo (no HTTP hop).
 *
 * The actor need not exist — first dispatch activates it (and its state)
 * lazily; it never has to be created or destroyed explicitly.
 */
export function actor<D extends AnyActorDefinition>(def: D, key: string): ActorClientWith<D> {
    if (isClientRef(def)) {
        return def.__sigxActorProxy(key) as ActorClientWith<D>;
    }
    if (!isActorDefinition(def)) {
        throw new Error(
            '[sigx actors] actor() needs an actor definition (or a client ref from the build).'
        );
    }
    return serverClient(def, key, undefined);
}

/**
 * Component-flavored alias of {@link actor} — same client, named for call
 * sites inside components. (A reactive `useActorState` integration is a
 * later, separate design.)
 */
export function useActor<D extends AnyActorDefinition>(def: D, key: string): ActorClientWith<D> {
    return actor(def, key);
}

/**
 * Server-side client: guards first (outside the mailbox), then in-process
 * dispatch through the silo seam. Streams run guards before the first pull.
 */
function serverClient<D extends AnyActorDefinition>(
    def: D,
    key: string,
    options: ActorCallOptions | undefined
): ActorClientWith<D> {
    const streamNames = new Set<string>(def.streamNames);
    const cache = new Map<string | symbol, unknown>();
    const proxy = new Proxy(Object.create(null) as object, {
        get: (_target, prop) => {
            if (typeof prop === 'symbol') return undefined;
            if (prop === 'then') return undefined;
            const hit = cache.get(prop);
            if (hit) return hit;
            let member: unknown;
            if (prop === 'with') {
                member = (next?: ActorCallOptions) => serverClient(def, key, next);
            } else if (streamNames.has(prop)) {
                member = (...args: unknown[]) => guardedStream(def, key, prop, args, options);
            } else {
                member = async (...args: unknown[]) => {
                    const silo = currentSilo();
                    await runGuards(def, prop, contextFor(options));
                    const raw = options?.signal
                        ? silo.actor(def, key).with({ signal: options.signal })
                        : silo.actor(def, key);
                    return (raw as Record<string, (...a: unknown[]) => Promise<unknown>>)[prop](
                        ...args
                    );
                };
            }
            cache.set(prop, member);
            return member;
        }
    });
    return proxy as ActorClientWith<D>;
}

function contextFor(options: ActorCallOptions | undefined): ServerFnContext {
    return resolveServerContext(
        options?.context as Request | Partial<ServerFnContext> | undefined,
        options?.signal
    );
}

function guardedStream(
    def: AnyActorDefinition,
    key: string,
    method: string,
    args: readonly unknown[],
    options: ActorCallOptions | undefined
): AsyncIterable<unknown> {
    const open = async (): Promise<AsyncIterator<unknown>> => {
        const silo = currentSilo();
        await runGuards(def, method, contextFor(options));
        const raw = options?.signal
            ? silo.actor(def, key).with({ signal: options.signal })
            : silo.actor(def, key);
        const stream = (
            raw as Record<string, (...a: unknown[]) => AsyncIterable<unknown>>
        )[method](...args);
        return stream[Symbol.asyncIterator]();
    };
    let inner: Promise<AsyncIterator<unknown>> | null = null;
    return {
        [Symbol.asyncIterator]: () => ({
            next: async () => {
                inner ??= open();
                return (await inner).next();
            },
            return: async () => {
                if (inner) {
                    const it = await inner;
                    if (it.return) await it.return(undefined);
                }
                return { value: undefined, done: true as const };
            }
        })
    };
}
