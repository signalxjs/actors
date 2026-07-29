/**
 * `@sigx/actors/server` — the actor wire endpoint, as a thin delegation to
 * core's `handleServerFnRequest`. The endpoint duck-types whatever
 * `resolve(symbol)` returns (anything carrying `__sigxFn`), so a
 * runtime-synthesized wrapper per actor method inherits the entire serverFn
 * stack: origin policy, content-type gate, body caps, the wire codec,
 * `ServerFnError` masking, `onError`, timeouts, NDJSON streaming, and the
 * request scope. WinterCG-clean — no `node:` imports.
 *
 * Wire shape:  POST {base}/{Type}%23{method}   {"args": [key, ...args]}
 * The actor KEY is the first wire argument — spliced in by the client
 * proxy, peeled off here before `silo.dispatch`.
 */
import { ServerFnError, type ServerFnContext, type ServerFnInfo } from '@sigx/server';
import {
    handleServerFnRequest,
    type ServerFnRequestOptions
} from '@sigx/server/server';
import { mintCallId } from '../call-id';
import { runGuards } from '../guards';
import { toClientError } from './client-error';
import { LIVE_SYMBOL, subscribeAll } from './live-endpoint';
import type { ActorCallContext, AnyActorDefinition, Silo } from '../types';

export interface ActorRequestOptions
    extends Omit<ServerFnRequestOptions, 'resolve' | 'renderBoundaries'> {
    /** The running silo — explicit, never ambient. */
    silo: Silo;
}

/** Does this request target the actor endpoint? A predicate, not a
 *  combinator — composition stays in the entry (the `matchesServerFn`
 *  idiom). */
export function matchesActorRequest(request: Request, base = '/_sigx/actor'): boolean {
    const prefix = base.endsWith('/') ? base : `${base}/`;
    return new URL(request.url).pathname.startsWith(prefix);
}

/** Handle one actor RPC request. Mount beside `handleServerFnRequest`. */
export function handleActorRequest(
    request: Request,
    options: ActorRequestOptions
): Promise<Response> {
    const { silo, ...rest } = options;
    return handleServerFnRequest(request, { ...rest, resolve: resolverFor(silo) });
}

const resolvers = new WeakMap<Silo, (symbol: string) => unknown | Promise<unknown>>();

function resolverFor(silo: Silo): (symbol: string) => unknown | Promise<unknown> {
    let resolver = resolvers.get(silo);
    if (!resolver) {
        resolver = createActorResolver(silo);
        resolvers.set(silo, resolver);
    }
    return resolver;
}

/**
 * symbol (`Cart#addItem`) → synthesized wrapped function, memoized per
 * method. Exposed for hand-rolled mounts (custom bases, other adapters).
 *
 * An unknown type or malformed symbol resolves to a wrapper that THROWS a
 * 404 `ServerFnError` (see `notFound`) rather than to `null`. Returning
 * `null` would hand the miss to core's generic resolver failure, which
 * answers "Unknown server function" — the wrong noun on the actor mount.
 * Callers get the same status and envelope either way, but a hand-rolled
 * mount that tests the result for `null` to fall through to another handler
 * must instead compare against its own registry first.
 */
export function createActorResolver(silo: Silo): (symbol: string) => unknown | Promise<unknown> {
    const cache = new Map<string, unknown>();
    return (symbol: string) => {
        const hit = cache.get(symbol);
        if (hit !== undefined) return hit;
        // The runtime's own mount, resolved BEFORE any definition lookup —
        // `$live` is not an actor type, and `defineActor` refuses to let one
        // be named that.
        if (symbol === LIVE_SYMBOL) {
            const live = synthesizeLive(silo);
            cache.set(symbol, live);
            return live;
        }
        const hash = symbol.lastIndexOf('#');
        if (hash <= 0 || hash === symbol.length - 1) return notFound(symbol);
        const type = symbol.slice(0, hash);
        const method = symbol.slice(hash + 1);
        const def = silo.definition(type);
        if (!def) return notFound(symbol, type);
        if (isPromise(def)) {
            return def.then((resolved) => {
                if (!resolved) return notFound(symbol, type);
                const wrapped = synthesize(silo, resolved, symbol, method);
                cache.set(symbol, wrapped);
                return wrapped;
            });
        }
        const wrapped = synthesize(silo, def, symbol, method);
        cache.set(symbol, wrapped);
        return wrapped;
    };
}

/**
 * The `$live` mount as a stream-flavoured serverFn. Its single wire
 * argument is the subscription array; per-subscription guards run inside,
 * so one rejection cannot fail the whole connection.
 */
function synthesizeLive(silo: Silo): unknown {
    return {
        __sigxName: 'subscribe',
        __sigxStream: true,
        __sigxFn: (rq: ServerFnContext, _info: ServerFnInfo, args: unknown[]) =>
            Promise.resolve(subscribeAll(silo, rq, args[0]))
    };
}

function isPromise<T>(value: T | Promise<T>): value is Promise<T> {
    return typeof (value as { then?: unknown })?.then === 'function';
}

/**
 * A resolved-but-always-throwing wrapper, rather than `null`. Returning
 * `null` hands the request to core's generic miss, which answers "Unknown
 * server function" — the wrong noun on the actor mount, and the first thing
 * a developer sees when a client and server build fall out of sync. The
 * status and envelope shape are identical either way.
 *
 * Deliberately NOT memoized: `silo.definition()` is lazy behind the
 * `virtual:sigx-actors` registry, so a type that misses now can resolve
 * after an HMR edit adds it.
 */
function notFound(symbol: string, type?: string): unknown {
    const detail = type
        ? `no actor type "${type}" is registered with this silo — is it registered, ` +
          `and are the client and server builds from the same deploy?`
        : `expected a "Type#method" symbol.`;
    return {
        __sigxName: symbol,
        __sigxFn: () => {
            throw new ServerFnError(404, `Unknown actor "${symbol}" — ${detail}`, {
                kind: 'method-not-found'
            });
        }
    };
}

function synthesize(
    silo: Silo,
    def: AnyActorDefinition,
    symbol: string,
    method: string
): unknown {
    const isStream = def.streamNames.includes(method);

    const prepare = async (
        rq: ServerFnContext,
        args: unknown[]
    ): Promise<{ key: string; rest: unknown[]; call: ActorCallContext }> => {
        const [key, ...rest] = args;
        if (typeof key !== 'string' || key.length === 0) {
            throw new ServerFnError(
                400,
                `actor call "${symbol}" needs a non-empty string key as its first argument`
            );
        }
        // The transport-independent chains — same pipeline as in-process
        // calls, run OUTSIDE the mailbox.
        await runGuards(def, method, rq);
        return {
            key,
            rest,
            call: {
                callChain: [],
                callId: mintCallId(),
                abortSignal: rq.abortSignal
            }
        };
    };

    if (isStream) {
        return {
            __sigxName: method,
            __sigxStream: true,
            // Resolves to an async generator: the endpoint's streamResponse
            // drives it (and generator.return() propagates to the actor's
            // stream on client disconnect).
            __sigxFn: async (rq: ServerFnContext, _info: ServerFnInfo, args: unknown[]) => {
                const { key, rest, call } = await prepare(rq, args);
                const iterable = silo.dispatchStream!(
                    { type: def.type, key },
                    method,
                    rest,
                    call
                );
                return (async function* pump(): AsyncGenerator<unknown> {
                    try {
                        yield* iterable;
                    } catch (error) {
                        throw toClientError(error);
                    }
                })();
            }
        };
    }

    return {
        __sigxName: method,
        __sigxFn: async (rq: ServerFnContext, _info: ServerFnInfo, args: unknown[]) => {
            const { key, rest, call } = await prepare(rq, args);
            try {
                return await silo.dispatch({ type: def.type, key }, method, rest, call);
            } catch (error) {
                throw toClientError(error);
            }
        }
    };
}

