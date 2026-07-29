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
    ACTOR_FOLLOW_HEADER,
    ACTOR_HOPS_HEADER,
    ACTOR_OWNER_HEADER,
    ACTOR_ROUTE_HEADER,
    encodeRouteToken,
    routePath,
    routeTokenFor,
    type ActorRouteToken
} from '../route';
import { routeMemo, type RouteMemo } from './route-memo';
import { actorId } from '../types';
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
    /**
     * Follow `421` wrong-host redirects, remembering where each grain lives
     * so only the FIRST call to it pays the extra round trip.
     *
     * Default `false`, deliberately. The default consumer is a browser
     * behind one origin, where a redirect is not merely useless but broken:
     * the retry would be cross-origin, so it preflights and is refused by
     * the server's same-origin policy. Turn it on for server-to-server and
     * native clients that can reach silos directly — and note the server
     * must be mounted with `onMiss: 'redirect'` or `'auto'` for anything to
     * redirect in the first place.
     *
     * `maxHops` (default 2) bounds one call's redirect chain; `cache`
     * (default 10 000) bounds how many grain→endpoint pairs are remembered.
     */
    follow?: boolean | { maxHops?: number; cache?: number };
}

// ---------------------------------------------------------------------------
// The default transport

function endpointOf(config: ActorTransportConfig, init: ActorCallInit | undefined): string {
    const target = config.endpoint ?? init?.endpoint ?? '';
    return target.endsWith('/') ? target.slice(0, -1) : target;
}

async function send(
    config: ActorTransportConfig,
    symbol: string,
    args: unknown[],
    init?: ActorCallInit,
    following?: { endpoint?: string; hops: number }
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
    if (following !== undefined) {
        // Advertise that we can follow, so an `onMiss:'auto'` mount knows to
        // redirect us rather than proxy. The hop count lets the SERVER bound
        // the chain too — a loop must be impossible, not just unlikely.
        headers[ACTOR_FOLLOW_HEADER] = '1';
        if (following.hops > 0) headers[ACTOR_HOPS_HEADER] = String(following.hops);
    }
    const request: RequestInit = {
        method: 'POST',
        headers,
        body: JSON.stringify({ args: encodeWire(args) }),
        ...(init?.signal ? { signal: init.signal } : {})
    };
    // A learned endpoint wins over every configured one: it is the only
    // source that was told, rather than assumed, where this grain lives.
    const url = routePath(following?.endpoint ?? endpointOf(config, init), token, symbol);
    return config.fetch ? config.fetch(url, request) : fetch(url, request);
}

function skewHint(symbol: string, status: number): string {
    return status === 404
        ? `[sigx actors] the server does not know "${symbol}" — is the actor registered ` +
              `with the silo, and are client and server builds from the same deploy?`
        : `[sigx actors] call to "${symbol}" failed with HTTP ${status}`;
}

/** The grain a call is for, or null when there isn't exactly one — `$live`
 *  multiplexes many grains onto one request and cannot be routed. */
function grainOf(symbol: string, args: unknown[]): string | null {
    const hash = symbol.lastIndexOf('#');
    if (hash <= 0 || typeof args[0] !== 'string') return null;
    const type = symbol.slice(0, hash);
    if (type.startsWith('$')) return null;
    return actorId({ type, key: args[0] });
}

/** The owner endpoint a 421 named, or null if this is any other failure. */
async function redirectTarget(res: Response): Promise<string | null> {
    if (res.status !== 421) return null;
    // The header mirrors the body, and reading it costs nothing — but the
    // body is authoritative, so fall back to it when a proxy stripped the
    // header on the way through.
    const header = res.headers.get(ACTOR_OWNER_HEADER);
    if (header !== null && header !== '') return header;
    try {
        // CLONE: this Response is handed back to the caller when the hop cap
        // is hit, and a drained body would leave it unable to parse the error.
        const body = JSON.parse(await res.clone().text()) as {
            error?: { data?: { kind?: string; owner?: { endpoint?: string } } };
        };
        const data = body?.error?.data;
        if (data?.kind !== 'wrong-host') return null;
        return typeof data.owner?.endpoint === 'string' ? data.owner.endpoint : null;
    } catch {
        return null;
    }
}

/**
 * One request, following wrong-host redirects and remembering the answer.
 *
 * The memo is what makes redirect worth doing: without it, a miss costs two
 * client round trips forever, which is strictly worse than letting the silo
 * proxy. With it, only the first call to a grain pays.
 */
async function sendFollowing(
    config: ActorTransportConfig,
    memo: RouteMemo,
    maxHops: number,
    symbol: string,
    args: unknown[],
    init?: ActorCallInit
): Promise<Response> {
    const grain = grainOf(symbol, args);
    if (grain === null) return send(config, symbol, args, init);

    let learned = memo.get(grain);
    let hops = 0;
    for (;;) {
        let res: Response;
        try {
            res = await send(config, symbol, args, init, { ...(learned !== undefined ? { endpoint: learned } : {}), hops });
        } catch (error) {
            // A learned endpoint that no longer answers must not strand this
            // grain forever — forget it and fall back to the configured one.
            // Only ever once: after forgetting, `learned` is undefined and a
            // second failure propagates.
            if (learned === undefined) throw error;
            memo.forgetEndpoint(learned);
            learned = undefined;
            continue;
        }

        const owner = await redirectTarget(res);
        if (owner !== null) {
            if (hops >= maxHops) return res; // give up; surface the 421
            // Abandoning a body keeps its connection out of the pool in
            // undici, which under load is a leak rather than a tidiness nit.
            void res.body?.cancel();
            memo.learn(grain, owner);
            learned = owner;
            hops++;
            continue;
        }

        // A learned endpoint answering 5xx is the same staleness signal as a
        // dead socket: the silo may be gone. Retry against the default once.
        if (res.status >= 500 && learned !== undefined) {
            void res.body?.cancel();
            memo.forgetEndpoint(learned);
            learned = undefined;
            continue;
        }
        return res;
    }
}

/**
 * The default transport: one POST per call, NDJSON for streams. Its
 * behaviour is exactly what shipped before the seam existed.
 */
export function fetchTransport(config: ActorTransportConfig = {}): ActorTransport {
    const follow = config.follow ?? false;
    const opts = typeof follow === 'object' ? follow : {};
    const maxHops = opts.maxHops ?? 2;
    // Built only when following, so the default transport allocates no Map.
    // Per transport instance, so `configureActors(...)` naturally clears it.
    const memo = follow === false ? undefined : routeMemo(opts.cache);
    const dispatch = (symbol: string, args: unknown[], init?: ActorCallInit) =>
        memo === undefined
            ? send(config, symbol, args, init)
            : sendFollowing(config, memo, maxHops, symbol, args, init);

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
 * Set (or with `null` clear) the transport every actor ref resolves at call
 * time. Accepts a full {@link ActorTransport}, or — the common case — an
 * {@link ActorTransportConfig} as sugar for `fetchTransport(config)`.
 */
export function configureActors(config: ActorTransportConfig | ActorTransport | null): void {
    configured = config === null ? null : isTransport(config) ? config : fetchTransport(config);
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
        const init: ActorCallInit = { ...options, endpoint: options?.endpoint ?? endpoint };
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
