/**
 * The INTERNAL silo-to-silo endpoint — a sibling of `@sigx/actors/server`
 * riding the same `handleServerFnRequest` machinery (codec, body caps,
 * NDJSON, cancellation) with three deliberate differences:
 *
 *  1. no guards — the public edge already ran them; trust here comes from
 *     the route plus the shared secret, never a spoofable payload marker;
 *  2. the call context comes from the envelope header (chain, callId,
 *     remaining-ms deadline) instead of being minted fresh;
 *  3. dispatch is LOCAL-ONLY — a misdirected call answers 421 wrong-host
 *     with the owner hint (redirect-not-proxy), never forwarded onward.
 *
 * This is HTTP's receiving half, contributed by `httpTransport()` as an
 * ordinary route. It is expressed against `SiloTransportRuntime` rather
 * than the placement directly, so the symbol-resolution rules — the 404
 * case, the `$sigx:silo` shadowing guard — are the ones every transport
 * shares rather than HTTP's private habits.
 *
 * Mount beside the public endpoint on the same listener:
 * `matchesSiloRequest(req) ? handleSiloRequest(req, {...}) : ...`
 */
import { ServerFnError, type ServerFnContext, type ServerFnInfo } from '@sigx/server';
import {
    handleServerFnRequest,
    type ServerFnRequestOptions
} from '@sigx/server/server';
import type { ActorCallContext, ActorRef, AnyActorDefinition, Silo } from '../types';
import { decodeEnvelope, verifyAuth, SILO_AUTH_HEADER, SILO_CALL_HEADER } from './envelope';
import type { ClusterPlacement } from './placement';
import type { SiloCallTarget, SiloTransportRuntime } from './seam';
import { SILO_STATS_METHOD, SILO_STATS_SYMBOL, SILO_STATS_TYPE } from './stats';
import { parseWatchOptions, WATCH_SYMBOL_PREFIX } from './watch-symbol';
import { toSiloWireError } from './wire-errors';

/** Endpoint knobs a transport may forward: body caps, `onError`, timeouts. */
export type SiloEndpointOptions = Omit<
    ServerFnRequestOptions,
    'resolve' | 'renderBoundaries' | 'origin' | 'guard'
>;

export interface SiloRequestOptions extends SiloEndpointOptions {
    silo: Silo;
    placement: ClusterPlacement;
    /** Shared cluster secret; when set, requests without it are 403'd. */
    secret?: string;
}

/** The runtime-shaped form `httpTransport()` uses. */
export interface SiloRuntimeRequestOptions extends SiloEndpointOptions {
    runtime: SiloTransportRuntime;
    secret?: string;
}

export function matchesSiloRequest(request: Request, base = '/_sigx/silo'): boolean {
    const prefix = base.endsWith('/') ? base : `${base}/`;
    return new URL(request.url).pathname.startsWith(prefix);
}

/**
 * Resolve an inbound wire symbol against a silo — THE rule, shared by every
 * transport rather than reimplemented per wire.
 *
 * The reserved ops symbol is answered BEFORE any definition lookup, so an
 * actor type literally named `$sigx:silo` cannot shadow it. That is the
 * safe direction: such an actor becomes uncallable cross-silo, which is
 * loud and testable, rather than silently taking over the channel.
 */
export function resolveSiloSymbol(
    silo: Silo,
    symbol: string
): SiloCallTarget | null | Promise<SiloCallTarget | null> {
    if (symbol === SILO_STATS_SYMBOL) {
        return { type: SILO_STATS_TYPE, method: SILO_STATS_METHOD, mode: 'unary' };
    }
    // A watch is an ordinary read asked for in watch mode, so the intent
    // rides on the symbol — the one caller-supplied field the per-call HMAC
    // covers. Stripped before the lookup, exactly like the stats symbol is
    // answered before it: `defineActor` refuses a `$`-prefixed type, so no
    // actor can be reached by, or shadow, the prefixed form.
    const watch = symbol.startsWith(WATCH_SYMBOL_PREFIX);
    const bare = watch ? symbol.slice(WATCH_SYMBOL_PREFIX.length) : symbol;
    const hash = bare.lastIndexOf('#');
    if (hash <= 0 || hash === bare.length - 1) return null;
    const type = bare.slice(0, hash);
    const method = bare.slice(hash + 1);
    const def = silo.definition(type);
    if (!def) return null;
    const resolve = (d: AnyActorDefinition): SiloCallTarget | null => {
        const stream = d.streamNames.includes(method);
        // Watching a `streams:` method is not a thing: a watch re-invokes a
        // read and yields its RESULT, and a generator has none. 404 rather
        // than a confusing runtime failure halfway into the subscription.
        if (watch && stream) return null;
        return { type, method, mode: watch ? 'watch' : stream ? 'stream' : 'unary' };
    };
    if (isPromise(def)) return def.then((resolved) => (resolved ? resolve(resolved) : null));
    return resolve(def);
}

/**
 * Adapt a silo + placement to the transport runtime. It lives here rather
 * than on the placement so `handleSiloRequest`'s published
 * `(silo, placement)` shape keeps working for hand-rolled mounts.
 */
export function siloRuntime(silo: Silo, placement: ClusterPlacement): SiloTransportRuntime {
    return {
        descriptor: () => placement.descriptor(),
        view: () => placement.view(),
        resolve: (symbol) => resolveSiloSymbol(silo, symbol),
        dispatch: (ref, method, args, call) => {
            // The ops channel answers before any activation lookup, and
            // deliberately does NOT count as an inbound dispatch — reading
            // the counters must not move them.
            if (ref.type === SILO_STATS_TYPE) return Promise.resolve(placement.report());
            return placement.dispatchInbound(ref, method, args, call);
        },
        dispatchStream: (ref, method, args, call) =>
            placement.dispatchInboundStream(ref, method, args, call),
        dispatchWatch: (ref, method, args, call, options) =>
            placement.dispatchInboundWatch(ref, method, args, call, options),
        noteAuthFailure: () => placement.noteAuthFailure?.()
    };
}

export function handleSiloRequest(
    request: Request,
    options: SiloRequestOptions
): Promise<Response> {
    const { silo, placement, ...rest } = options;
    return handleSiloRequestForRuntime(request, {
        ...rest,
        runtime: siloRuntime(silo, placement)
    });
}

export function handleSiloRequestForRuntime(
    request: Request,
    options: SiloRuntimeRequestOptions
): Promise<Response> {
    const { runtime, secret, ...rest } = options;
    // Malformed percent-encoding would throw inside the core endpoint's
    // own decode (before any guard) — reject it here so an invalid path
    // can never crash the mount.
    try {
        decodeURIComponent(new URL(request.url).pathname);
    } catch {
        runtime.noteAuthFailure();
        return Promise.resolve(authFailed());
    }
    return handleServerFnRequest(request, {
        ...rest,
        // Server-to-server traffic sends no Origin header; the per-request
        // HMAC (bound to symbol + callId, freshness-windowed) is the
        // authentication, checked before anything dispatches.
        origin: false,
        guard: async (rq) => {
            if (secret === undefined) return;
            let symbol: string;
            try {
                symbol = decodeURIComponent(
                    rq.url.pathname.slice(rq.url.pathname.lastIndexOf('/') + 1)
                );
            } catch {
                // Malformed percent-encoding can't crash the endpoint —
                // an undecodable path is an unauthenticated request.
                runtime.noteAuthFailure();
                throw new ServerFnError(403, '[sigx actors] cluster authentication failed');
            }
            const callHeader = rq.request.headers.get(SILO_CALL_HEADER);
            let callId = '';
            try {
                callId = callHeader ? decodeEnvelope(callHeader).call.callId : '';
            } catch {
                callId = '';
            }
            const ok =
                callId !== '' &&
                (await verifyAuth(
                    secret,
                    rq.request.headers.get(SILO_AUTH_HEADER),
                    symbol,
                    callId
                ));
            if (!ok) {
                // Counted, because a 403 here is otherwise completely
                // silent — and during a secret rotation it is the only
                // signal that half the cluster has not rotated yet.
                runtime.noteAuthFailure();
                throw new ServerFnError(403, '[sigx actors] cluster authentication failed');
            }
        },
        resolve: resolverFor(runtime)
    });
}

function authFailed(): Response {
    return new Response(
        JSON.stringify({
            error: { message: '[sigx actors] cluster authentication failed', status: 403 }
        }),
        { status: 403, headers: { 'content-type': 'application/json' } }
    );
}

const resolvers = new WeakMap<
    SiloTransportRuntime,
    (symbol: string) => unknown | Promise<unknown>
>();

function resolverFor(
    runtime: SiloTransportRuntime
): (symbol: string) => unknown | Promise<unknown> {
    let resolver = resolvers.get(runtime);
    if (!resolver) {
        resolver = createRuntimeResolver(runtime);
        resolvers.set(runtime, resolver);
    }
    return resolver;
}

/** symbol (`Cart#addItem`) → synthesized wrapper, memoized per method. */
export function createSiloResolver(
    silo: Silo,
    placement: ClusterPlacement
): (symbol: string) => unknown | Promise<unknown> {
    return createRuntimeResolver(siloRuntime(silo, placement));
}

function createRuntimeResolver(
    runtime: SiloTransportRuntime
): (symbol: string) => unknown | Promise<unknown> {
    const cache = new Map<string, unknown>();
    // The ops channel. Synthesized once, and answered WITHOUT a `prepare()`:
    // the guard already authenticated symbol + callId, and there is no ref
    // to route, no deadline to re-anchor and no activation to dispatch to.
    const statsFn = {
        __sigxName: SILO_STATS_METHOD,
        __sigxFn: () =>
            runtime.dispatch(
                { type: SILO_STATS_TYPE, key: SILO_STATS_METHOD },
                SILO_STATS_METHOD,
                [],
                { callChain: [], callId: SILO_STATS_METHOD }
            )
    };
    return (symbol: string) => {
        if (symbol === SILO_STATS_SYMBOL) return statsFn;
        const hit = cache.get(symbol);
        if (hit !== undefined) return hit;
        const target = runtime.resolve(symbol);
        if (isPromise(target)) {
            return target.then((resolved) => {
                if (!resolved) return null;
                const wrapped = synthesize(runtime, resolved, symbol);
                cache.set(symbol, wrapped);
                return wrapped;
            });
        }
        if (!target) return null;
        const wrapped = synthesize(runtime, target, symbol);
        cache.set(symbol, wrapped);
        return wrapped;
    };
}

function isPromise<T>(value: T | Promise<T>): value is Promise<T> {
    return typeof (value as { then?: unknown })?.then === 'function';
}

function synthesize(
    runtime: SiloTransportRuntime,
    target: SiloCallTarget,
    symbol: string
): unknown {
    const { type, method, mode } = target;

    const prepare = (
        rq: ServerFnContext,
        args: unknown[]
    ): { ref: ActorRef; rest: unknown[]; call: ActorCallContext } => {
        const [key, ...rest] = args;
        if (typeof key !== 'string' || key.length === 0) {
            throw new ServerFnError(
                400,
                `silo call "${symbol}" needs a non-empty string key as its first argument`
            );
        }
        const header = rq.request.headers.get(SILO_CALL_HEADER);
        if (!header) {
            throw new ServerFnError(
                400,
                `silo call "${symbol}" is missing the ${SILO_CALL_HEADER} envelope header`
            );
        }
        let decoded;
        try {
            decoded = decodeEnvelope(header);
        } catch (error) {
            throw new ServerFnError(400, (error as Error).message);
        }
        return {
            ref: { type, key },
            rest,
            call: { ...decoded.call, abortSignal: rq.abortSignal }
        };
    };

    // Both streamed modes answer identically on the wire — the same NDJSON
    // pump, the same cancellation path, the same error mapping. Only the
    // runtime call and one extra leading argument differ.
    if (mode !== 'unary') {
        return {
            __sigxName: method,
            __sigxStream: true,
            __sigxFn: async (rq: ServerFnContext, _info: ServerFnInfo, args: unknown[]) => {
                const { ref, rest, call } = prepare(rq, args);
                const iterable =
                    mode === 'watch'
                        ? runtime.dispatchWatch(
                              ref,
                              method,
                              rest.slice(1),
                              call,
                              watchOptions(rest[0], symbol)
                          )
                        : runtime.dispatchStream(ref, method, rest, call);
                return (async function* pump(): AsyncGenerator<unknown> {
                    try {
                        yield* iterable;
                    } catch (error) {
                        throw toServerFnError(error);
                    }
                })();
            }
        };
    }

    return {
        __sigxName: method,
        __sigxFn: async (rq: ServerFnContext, _info: ServerFnInfo, args: unknown[]) => {
            const { ref, rest, call } = prepare(rq, args);
            try {
                return await runtime.dispatch(ref, method, rest, call);
            } catch (error) {
                throw toServerFnError(error);
            }
        }
    };
}

/** The shared validator, in this mount's error currency. */
function watchOptions(raw: unknown, symbol: string): { throttleMs?: number } | undefined {
    try {
        return parseWatchOptions(raw, symbol);
    } catch (error) {
        throw new ServerFnError(400, (error as Error).message);
    }
}

/**
 * Actor error → the HTTP shape, through the SHARED wire mapping so this
 * route and a socket transport cannot disagree about what a `wrong-host`
 * is. Every kind travels with its `kind` (and `owner` for wrong-host) —
 * no prod masking between trusted peers; the public endpoint still masks
 * whatever ultimately reaches a browser. Non-actor errors pass through
 * untouched for the core endpoint to handle.
 */
function toServerFnError(error: unknown): unknown {
    const wire = toSiloWireError(error);
    if (wire.data === undefined) return error;
    return new ServerFnError(wire.status ?? 500, wire.message ?? 'error', wire.data);
}
