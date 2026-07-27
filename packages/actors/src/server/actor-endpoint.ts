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
import { isActorError } from '../errors';
import { runGuards } from '../guards';
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
                        throw toWireError(error);
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
                throw toWireError(error);
            }
        }
    };
}

/**
 * Map branded actor errors onto client-visible `ServerFnError` statuses.
 * Anything else stays as-is and gets the endpoint's prod masking.
 */
function toWireError(error: unknown): unknown {
    if (!isActorError(error)) return error;
    switch (error.kind) {
        case 'method-not-found':
            return new ServerFnError(404, error.message, { kind: error.kind });
        case 'state-conflict':
            return new ServerFnError(409, error.message, { kind: error.kind });
        case 'silo-shutdown':
            return new ServerFnError(503, error.message, { kind: error.kind });
        case 'call-timeout':
            return new ServerFnError(504, error.message, { kind: error.kind });
        case 'wrong-host':
        case 'unreachable':
            // Cluster routing errors are resolved silo-to-silo and should
            // never surface here; if one does, it's retryable — 503.
            return new ServerFnError(503, error.message, { kind: error.kind });
        case 'deadlock':
        case 'activation':
        default:
            // Server-side bugs/config problems: mask in prod (plain 500).
            return error;
    }
}
