/**
 * The INTERNAL host-to-host endpoint — a sibling of `@sigx/actors/server`
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
 * ordinary route. It is expressed against `HostEndpointRuntime` rather than
 * the placement directly, so the symbol-resolution rules — the 404 case, the
 * `$sigx:host` shadowing guard — are the ones every transport shares rather
 * than HTTP's private habits. That runtime is the INBOUND half only: a
 * single-host runtime with no membership (a Durable Object) serves the mount
 * through `hostEndpointRuntime(host)`, while a cluster passes the wider
 * `HostTransportRuntime` its sending side also needs.
 *
 * Mount beside the public endpoint on the same listener:
 * `matchesHostRequest(req) ? handleHostRequest(req, {...}) : ...`
 */
import { ServerFnError, type ServerFnContext, type ServerFnInfo } from '@sigx/server';
import {
    handleServerFnRequest,
    type ServerFnRequestOptions
} from '@sigx/server/server';
import type { ActorCallContext, ActorRef, AnyActorDefinition, Host } from '../types';
import { decodeEnvelope, verifyAuth, HOST_AUTH_HEADER, HOST_CALL_HEADER } from './envelope';
import type { ClusterPlacement } from './placement';
import type { HostCallTarget, HostEndpointRuntime, HostTransportRuntime } from './seam';
import {
    HOST_STATS_METHOD,
    HOST_STATS_SYMBOL,
    HOST_STATS_TYPE,
    type HostReportOptions
} from './stats';
import { parseWatchOptions, WATCH_SYMBOL_PREFIX } from './watch-symbol';
import { relayStream } from '../stream-relay';
import { toHostWireError } from './wire-errors';

/** Endpoint knobs a transport may forward: body caps, `onError`, timeouts. */
export type HostEndpointOptions = Omit<
    ServerFnRequestOptions,
    'resolve' | 'renderBoundaries' | 'origin' | 'guard'
>;

export interface HostRequestOptions extends HostEndpointOptions {
    host: Host;
    placement: ClusterPlacement;
    /** Shared cluster secret; when set, requests without it are 403'd. */
    secret?: string;
}

/** The runtime-shaped form `httpTransport()` uses. */
export interface HostRuntimeRequestOptions extends HostEndpointOptions {
    /**
     * Only the INBOUND half is required. A clustered transport passes its
     * `HostTransportRuntime` (which extends this); a single-host runtime
     * with no membership — a Durable Object — passes
     * `hostEndpointRuntime(host)`.
     */
    runtime: HostEndpointRuntime;
    secret?: string;
}

export function matchesHostRequest(request: Request, base = '/_sigx/host'): boolean {
    const prefix = base.endsWith('/') ? base : `${base}/`;
    return new URL(request.url).pathname.startsWith(prefix);
}

/**
 * Resolve an inbound wire symbol against a host — THE rule, shared by every
 * transport rather than reimplemented per wire.
 *
 * The reserved ops symbol is answered BEFORE any definition lookup, so an
 * actor type literally named `$sigx:host` cannot shadow it. That is the
 * safe direction: such an actor becomes uncallable cross-host, which is
 * loud and testable, rather than silently taking over the channel.
 */
export function resolveHostSymbol(
    host: Host,
    symbol: string
): HostCallTarget | null | Promise<HostCallTarget | null> {
    if (symbol === HOST_STATS_SYMBOL) {
        return { type: HOST_STATS_TYPE, method: HOST_STATS_METHOD, mode: 'unary' };
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
    const def = host.definition(type);
    if (!def) return null;
    const resolve = (d: AnyActorDefinition): HostCallTarget | null => {
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
 * Adapt a host + placement to the transport runtime. It lives here rather
 * than on the placement so `handleHostRequest`'s published
 * `(host, placement)` shape keeps working for hand-rolled mounts.
 */
export function hostRuntime(host: Host, placement: ClusterPlacement): HostTransportRuntime {
    return {
        descriptor: () => placement.descriptor(),
        view: () => placement.view(),
        resolve: (symbol) => resolveHostSymbol(host, symbol),
        dispatch: (ref, method, args, call) => {
            // The ops channel answers before any activation lookup, and
            // deliberately does NOT count as an inbound dispatch — reading
            // the counters must not move them.
            if (ref.type === HOST_STATS_TYPE) {
                return Promise.resolve(placement.report(args[0] as HostReportOptions | undefined));
            }
            return placement.dispatchInbound(ref, method, args, call);
        },
        dispatchStream: (ref, method, args, call) =>
            placement.dispatchInboundStream(ref, method, args, call),
        dispatchWatch: (ref, method, args, call, options) =>
            placement.dispatchInboundWatch(ref, method, args, call, options),
        noteAuthFailure: () => placement.noteAuthFailure?.()
    };
}

/**
 * Adapt a PLAIN host to the inbound endpoint — no cluster, no membership.
 *
 * This is what lets a single-host runtime serve the internal mount. A
 * Cloudflare Durable Object is the motivating case: it holds one actor, the
 * platform guarantees the single instance, and there is no directory to
 * claim or peer to redirect to — so the endpoint's machinery is wanted but
 * `descriptor()`/`view()` have no honest answer.
 *
 * Dispatch goes through the host itself rather than a placement's
 * `dispatchInbound`, so the app's `useDispatch` middleware still runs. Only
 * a cluster needs the raw path, and only because an inbound hop there has
 * already been counted by the originating host.
 *
 * `$sigx:host#stats` deliberately resolves to `null` (→ method-not-found):
 * `HostReport` is a cluster identity — hostId, epoch, address, owned
 * reminder shards — and fabricating one for a host that has no cluster
 * would put fiction on the ops channel.
 */
export function hostEndpointRuntime(host: Host): HostEndpointRuntime {
    // `dispatchStream`/`dispatchWatch` are optional on `ActorDispatcher` so a
    // transport that cannot stream may omit them; the host itself always has
    // both. Fail with the reason rather than `undefined is not a function`.
    const missing = (name: string): never => {
        throw new Error(
            `[sigx actors] this host has no ${name}() — the internal endpoint cannot ` +
                `serve that call.`
        );
    };

    return {
        resolve: (symbol) =>
            symbol === HOST_STATS_SYMBOL ? null : resolveHostSymbol(host, symbol),
        dispatch: (ref, method, args, call) => host.dispatch(ref, method, args, call),
        dispatchStream: (ref, method, args, call) =>
            host.dispatchStream?.(ref, method, args, call) ?? missing('dispatchStream'),
        dispatchWatch: (ref, method, args, call, options) =>
            host.dispatchWatch?.(ref, method, args, call, options) ?? missing('dispatchWatch')
        // No `noteAuthFailure`: with no shared secret nothing is ever
        // refused, so there is nothing to count.
    };
}

export function handleHostRequest(
    request: Request,
    options: HostRequestOptions
): Promise<Response> {
    const { host, placement, ...rest } = options;
    return handleHostRequestForRuntime(request, {
        ...rest,
        runtime: hostRuntime(host, placement)
    });
}

export function handleHostRequestForRuntime(
    request: Request,
    options: HostRuntimeRequestOptions
): Promise<Response> {
    const { runtime, secret, ...rest } = options;
    // Malformed percent-encoding would throw inside the core endpoint's
    // own decode (before any guard) — reject it here so an invalid path
    // can never crash the mount.
    try {
        decodeURIComponent(new URL(request.url).pathname);
    } catch {
        runtime.noteAuthFailure?.();
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
                runtime.noteAuthFailure?.();
                throw new ServerFnError(403, '[sigx actors] cluster authentication failed');
            }
            const callHeader = rq.request.headers.get(HOST_CALL_HEADER);
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
                    rq.request.headers.get(HOST_AUTH_HEADER),
                    symbol,
                    callId
                ));
            if (!ok) {
                // Counted, because a 403 here is otherwise completely
                // silent — and during a secret rotation it is the only
                // signal that half the cluster has not rotated yet.
                runtime.noteAuthFailure?.();
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
    HostEndpointRuntime,
    (symbol: string) => unknown | Promise<unknown>
>();

function resolverFor(
    runtime: HostEndpointRuntime
): (symbol: string) => unknown | Promise<unknown> {
    let resolver = resolvers.get(runtime);
    if (!resolver) {
        resolver = createRuntimeResolver(runtime);
        resolvers.set(runtime, resolver);
    }
    return resolver;
}

/** symbol (`Cart#addItem`) → synthesized wrapper, memoized per method. */
export function createHostResolver(
    host: Host,
    placement: ClusterPlacement
): (symbol: string) => unknown | Promise<unknown> {
    return createRuntimeResolver(hostRuntime(host, placement));
}

function createRuntimeResolver(
    runtime: HostEndpointRuntime
): (symbol: string) => unknown | Promise<unknown> {
    const cache = new Map<string, unknown>();
    // The ops channel. Synthesized once, and answered WITHOUT a `prepare()`:
    // the guard already authenticated symbol + callId, and there is no ref
    // to route, no deadline to re-anchor and no activation to dispatch to.
    const statsFn = {
        __sigxName: HOST_STATS_METHOD,
        // The wire sends `[ref.key, ...args]`, so what the collector asked
        // for is `args[1]`. Forwarded rather than dropped, which is how the
        // caller gets to say "and the actor list, please" — the responder
        // clamps whatever it is told.
        __sigxFn: (_rq: unknown, _info: unknown, args: unknown[] = []) =>
            runtime.dispatch(
                { type: HOST_STATS_TYPE, key: HOST_STATS_METHOD },
                HOST_STATS_METHOD,
                args.slice(1),
                { callChain: [], callId: HOST_STATS_METHOD }
            )
    };
    return (symbol: string) => {
        if (symbol === HOST_STATS_SYMBOL) {
            // Still the RUNTIME's call whether it offers the ops channel.
            // A cluster's `resolveHostSymbol` always answers it, so this is
            // unchanged there; a single-host runtime with no cluster
            // identity to report answers `null` and the symbol 404s like
            // any other unknown one.
            const offered = runtime.resolve(symbol);
            if (isPromise(offered)) return offered.then((t) => (t ? statsFn : null));
            return offered ? statsFn : null;
        }
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
    runtime: HostEndpointRuntime,
    target: HostCallTarget,
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
                `host call "${symbol}" needs a non-empty string key as its first argument`
            );
        }
        const header = rq.request.headers.get(HOST_CALL_HEADER);
        if (!header) {
            throw new ServerFnError(
                400,
                `host call "${symbol}" is missing the ${HOST_CALL_HEADER} envelope header`
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
                // An iterator, not a generator — see `relayStream`: a
                // generator parked at `yield*` queues `return()` and never
                // forwards it, so a peer hanging up would leak the owner's
                // keep-alive ref.
                return relayStream(iterable, {
                    mapError: toServerFnError,
                    signal: rq.abortSignal
                });
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
    const wire = toHostWireError(error);
    if (wire.data === undefined) return error;
    return new ServerFnError(wire.status ?? 500, wire.message ?? 'error', wire.data);
}
