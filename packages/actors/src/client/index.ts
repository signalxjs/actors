/**
 * `@sigx/actors/client` — the browser-side actor transport. Dependency-light
 * by contract (`@sigx/serialize` only, size-limited): the build transform
 * swaps a `*.actor.ts` module for `__actorRef(...)` calls importing this
 * entry, so its bytes ride every client bundle that touches an actor.
 *
 * One GENERIC Proxy serves every actor — no per-method stub compilation.
 * Method types come from the real module's declarations (the swap changes
 * values, never types).
 *
 * The proxy never speaks HTTP itself: it delegates to an `ActorTransport`,
 * so a batching or WebSocket transport drops in without any call site
 * changing. `fetchTransport()` is the default and IS the wire contract
 * (mirrors `@sigx/server/client`, pinned by integration tests against the
 * real endpoint): POST `{endpoint}/{Type}%23{method}` with
 * `{"args": [key, ...args]}` → `{"data"}` / `{"error"}` envelope, or NDJSON
 * `{"chunk"}* ({"done"}|{"error"})` for stream methods. Errors are
 * re-created with the `__sigxServerFnError` brand so `isServerFnError`
 * matches them. The parsing/branding plumbing lives in `../wire-shared` —
 * shared with the silo-to-silo transport.
 *
 * Policy — retries, caching, invalidation, live subscriptions — deliberately
 * lives in `@sigx/actors/app`, not here: this entry's byte budget rides
 * every client bundle whether or not the app uses any of it.
 */
import {
    ACTOR_ROUTE_HEADER,
    encodeRouteToken,
    routePath,
    routeTokenFor,
    type ActorRouteToken
} from '../route';
import type { ActorRef } from '../types';
import {
    encodeWire,
    readNdjson,
    reviveWire,
    reviver,
    wireFail,
    type WireError
} from '../wire-shared';

export {
    ACTOR_ROUTE_HEADER,
    ACTOR_ROUTE_SEGMENT,
    hashRouteToken,
    type ActorRouteMode,
    type ActorRouteToken
} from '../route';
export {
    actorRedirect,
    chainRouters,
    learningRouter,
    routedFetchTransport,
    routedTransport,
    staticRouter,
    type ActorRouteContext,
    type ActorRouteRedirect,
    type ActorRouter,
    type ActorRouterStats,
    type ActorRoutingStats,
    type LearningRouterOptions,
    type RoutedFetchConfig,
    type RoutedTransport
} from './routing';

// ---------------------------------------------------------------------------
// The seam

/** Per-call options a transport receives. */
export interface ActorCallInit {
    signal?: AbortSignal;
    headers?: Record<string, string>;
    /**
     * The endpoint the build baked into the client ref. A transport that
     * carries its own endpoint ignores it; `fetchTransport` treats it as the
     * fallback, so `configureActors({ headers })` can override headers alone
     * without restating where the server is.
     */
    endpoint?: string;
    /**
     * The grain this call is for, when there is exactly ONE.
     *
     * Absent for `$live#subscribe`, which multiplexes many grains onto one
     * request, and for any hand-built transport call. A transport or router
     * MUST read absence as "no routing opinion is possible", never as an
     * error.
     *
     * Threaded from the proxy rather than re-derived from the symbol and
     * `args[0]`: the "key is wire argument 0" convention belongs to
     * `__actorRef`, and a transport that re-implemented it would break
     * silently the day that changes.
     */
    ref?: ActorRef;
    /**
     * An endpoint a ROUTER chose for this call.
     *
     * Wins over both `endpoint` and the transport's own configured endpoint —
     * a router is strictly more specific than static config, and returning
     * `null` is how it asks for the static answer instead. A transport that
     * ignores this silently defeats every router.
     */
    route?: string;
}

/** One actor subscription on a transport's push channel. */
export interface ActorSubscription {
    type: string;
    key: string;
    method: string;
    args?: readonly unknown[];
}

/** A transport's optional server→client push channel. */
export interface ActorLiveChannel {
    /** Returns an unsubscribe function. */
    subscribe(
        sub: ActorSubscription,
        onValue: (value: unknown) => void,
        onError?: (error: Error) => void
    ): () => void;
}

/**
 * How actor calls reach the server. Swap it to batch, to authenticate
 * differently, or to speak a protocol other than HTTP — `actor()` and
 * everything built on it are unaffected.
 */
export interface ActorTransport {
    /** Diagnostic label, e.g. `'fetch'`. */
    readonly name: string;
    call(symbol: string, args: unknown[], init?: ActorCallInit): Promise<unknown>;
    stream(symbol: string, args: unknown[], init?: ActorCallInit): AsyncIterable<unknown>;
    /**
     * Optional push channel. A transport without one simply has no live
     * layer — consumers fall back to re-reading.
     */
    live?(): ActorLiveChannel;
    close?(): void | Promise<void>;
}

/**
 * Config for the default fetch transport. Deliberately SEPARATE from core's
 * `configureServerFn`: a remote-backend app may point fn stubs and actor
 * calls at different bases.
 */
export interface ActorTransportConfig {
    /** Absolute URL or path prefix; wins over the build-time endpoint. */
    endpoint?: string;
    /** Extra request headers — static map or (possibly async) factory. */
    headers?:
        | Record<string, string>
        | (() => Record<string, string> | Promise<Record<string, string>>);
    /** Fetch implementation; default is the global fetch. */
    fetch?: typeof globalThis.fetch;
    /**
     * How the per-grain routing token is derived — the thing an edge load
     * balancer hashes to send every call for one grain to one silo. Default
     * `'hash'`: an opaque hash of the actor id, so keys stay out of access
     * logs. See `../route`.
     *
     * On by default because a wire that only SOMETIMES carries the token
     * means the LB config only sometimes works. It is inert until something
     * upstream routes on it, and the server ignores it either way.
     */
    route?: ActorRouteToken;
}

// ---------------------------------------------------------------------------
// The default transport

function endpointOf(config: ActorTransportConfig, init: ActorCallInit | undefined): string {
    // `init.route` FIRST: it is the only source that was told, rather than
    // assumed, where this grain lives. Without this an app that called
    // `configureActors({ endpoint })` would silently defeat its own router.
    const target = init?.route ?? config.endpoint ?? init?.endpoint ?? '';
    return target.endsWith('/') ? target.slice(0, -1) : target;
}

async function send(
    config: ActorTransportConfig,
    symbol: string,
    args: unknown[],
    init?: ActorCallInit,
): Promise<Response> {
    const extra = typeof config.headers === 'function' ? await config.headers() : config.headers;
    // content-type is NOT overridable — the endpoint 415s anything else.
    const headers: Record<string, string> = {};
    for (const source of [extra, init?.headers]) {
        for (const key in source) {
            if (key.toLowerCase() !== 'content-type') headers[key] = source[key]!;
        }
    }
    headers['content-type'] = 'application/json';
    // The routing token: the grain's type comes from the symbol, its key is
    // wire arg 0 (the proxy splices it there). Both carriers get the SAME
    // token from one call, so a path segment and a header can never disagree.
    const hash = symbol.lastIndexOf('#');
    const token =
        hash > 0 && typeof args[0] === 'string'
            ? routeTokenFor(config.route ?? 'hash', symbol.slice(0, hash), args[0])
            : null;
    // The SAME bytes as the path segment — an LB hashes what it sees, so a
    // raw header beside an encoded path would route the two carriers to
    // different silos. Encoding also keeps the value valid ASCII.
    if (token !== null) headers[ACTOR_ROUTE_HEADER] = encodeRouteToken(token);
    const request: RequestInit = {
        method: 'POST',
        headers,
        body: JSON.stringify({ args: encodeWire(args) }),
        ...(init?.signal ? { signal: init.signal } : {})
    };
    const url = routePath(endpointOf(config, init), token, symbol);
    return config.fetch ? config.fetch(url, request) : fetch(url, request);
}

function skewHint(symbol: string, status: number): string {
    return status === 404
        ? `[sigx actors] the server does not know "${symbol}" — is the actor registered ` +
              `with the silo, and are client and server builds from the same deploy?`
        : `[sigx actors] call to "${symbol}" failed with HTTP ${status}`;
}

/**
 * The default transport: one POST per call, NDJSON for streams. Its
 * behaviour is exactly what shipped before the seam existed.
 */
export function fetchTransport(config: ActorTransportConfig = {}): ActorTransport {
    const dispatch = (symbol: string, args: unknown[], init?: ActorCallInit) =>
        send(config, symbol, args, init);

    return {
        name: 'fetch',
        async call(symbol: string, args: unknown[], init?: ActorCallInit): Promise<unknown> {
            const res = await dispatch(symbol, args, init);
            let parsed: { data?: unknown; error?: WireError } | undefined;
            try {
                parsed = JSON.parse(await res.text(), reviver) as {
                    data?: unknown;
                    error?: WireError;
                };
            } catch {
                parsed = undefined;
            }
            if (!res.ok || !parsed || parsed.error) {
                throw wireFail(res.status, parsed?.error, skewHint(symbol, res.status));
            }
            return reviveWire(parsed.data);
        },
        stream(symbol: string, args: unknown[], init?: ActorCallInit): AsyncIterable<unknown> {
            const controller = new AbortController();
            const signal = init?.signal
                ? AbortSignal.any([init.signal, controller.signal])
                : controller.signal;
            async function* stream(): AsyncGenerator<unknown> {
                try {
                    const res = await dispatch(symbol, args, { ...init, signal });
                    if (!res.ok || !res.body) {
                        let wire: WireError | undefined;
                        try {
                            wire = (
                                JSON.parse(await res.text(), reviver) as { error?: WireError }
                            )?.error;
                        } catch {
                            wire = undefined;
                        }
                        throw wireFail(res.status, wire, skewHint(symbol, res.status));
                    }
                    yield* readNdjson(res, symbol);
                } finally {
                    controller.abort(); // consumer break/return, error, or normal end
                }
            }
            return stream();
        }
    };
}

// ---------------------------------------------------------------------------
// Transport selection

const DEFAULT_TRANSPORT = fetchTransport();
let configured: ActorTransport | null = null;

function isTransport(value: ActorTransportConfig | ActorTransport): value is ActorTransport {
    return typeof (value as ActorTransport).call === 'function';
}

/**
 * A config-or-transport, resolved to a transport.
 *
 * Exported because `@sigx/actors/app` accepts the same union and MUST NOT
 * re-derive it: a second copy of this duck-check silently diverged once
 * already.
 *
 * Deliberately knows nothing about routing. Everything in `./routing` is
 * reachable only by importing it, so an app that never mentions a router
 * ships none of it — this entry's bytes ride every client bundle whether or
 * not the app uses any of it.
 */
export function resolveTransport(
    config: ActorTransportConfig | ActorTransport
): ActorTransport {
    return isTransport(config) ? config : fetchTransport(config);
}

/**
 * Set (or with `null` clear) the transport every actor ref resolves at call
 * time. Accepts a full {@link ActorTransport}, or — the common case — an
 * {@link ActorTransportConfig} as sugar for `fetchTransport(config)`.
 */
export function configureActors(config: ActorTransportConfig | ActorTransport | null): void {
    configured = config === null ? null : resolveTransport(config);
}

/** The transport in force. Exported for `@sigx/actors/app`. */
export function currentTransport(): ActorTransport {
    return configured ?? DEFAULT_TRANSPORT;
}

// ---------------------------------------------------------------------------

/**
 * The client-side actor ref the build transform emits for each exported
 * `defineActor`. Carries the `__sigxActorProxy` brand that `actor()` from
 * the root entry branches on.
 */
export function __actorRef(
    type: string,
    endpoint: string,
    streams: readonly string[] = []
): object {
    const streamNames = new Set(streams);
    const makeProxy = (key: string, options?: ActorCallInit): object => {
        const cache = new Map<string | symbol, unknown>();
        // `ref` LAST so it is authoritative: a caller's `.with({ ref })`
        // cannot make a proxy route as some other grain.
        const init: ActorCallInit = {
            ...options,
            endpoint: options?.endpoint ?? endpoint,
            ref: { type, key }
        };
        return new Proxy(Object.create(null) as object, {
            get(_target, prop) {
                if (typeof prop === 'symbol') return undefined;
                if (prop === 'then') return undefined;
                const hit = cache.get(prop);
                if (hit) return hit;
                const symbol = `${type}#${prop}`;
                let member: unknown;
                if (prop === 'with') {
                    member = (next?: ActorCallInit) => makeProxy(key, next);
                } else if (streamNames.has(prop)) {
                    member = (...args: unknown[]) =>
                        currentTransport().stream(symbol, [key, ...args], init);
                } else {
                    member = (...args: unknown[]) =>
                        currentTransport().call(symbol, [key, ...args], init);
                }
                cache.set(prop, member);
                return member;
            }
        });
    };
    return {
        __sigxActor: type,
        __sigxActorProxy: makeProxy
    };
}
