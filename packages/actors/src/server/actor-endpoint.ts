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
 * proxy, peeled off here before `host.dispatch`.
 */
import { ServerFnError, type ServerFnContext, type ServerFnInfo } from '@sigx/server';
import {
    handleServerFnRequest,
    type ServerFnRequestOptions
} from '@sigx/server/server';
import { mintCallId } from '../call-id';
import { runGuards } from '../guards';
import { ACTOR_FOLLOW_HEADER, ACTOR_HOPS_HEADER, ACTOR_OWNER_HEADER } from '../route';
import { relayStream } from '../stream-relay';
import { toClientError } from './client-error';
import { LIVE_SYMBOL, subscribeAll } from './live-endpoint';
import {
    actorLabel,
    type ActorCallContext,
    type ActorReadCache,
    type ActorRef,
    type AnyActorDefinition,
    type Host
} from '../types';

/**
 * What this mount does with a call for an actor another host owns.
 *
 * - `'proxy'` — forward it over the host-to-host transport and answer with
 *   the result. One client round trip, one internal hop, every time. The
 *   default, and the only correct answer for browsers: they usually cannot
 *   reach hosts individually, and a cross-origin retry would preflight and
 *   be refused by the same-origin policy.
 * - `'redirect'` — answer `421` naming the owner and let the client retry
 *   there. One extra round trip on a miss, then direct forever if the
 *   client remembers (`fetchTransport({ follow: true })` does).
 * - `'auto'` — redirect callers that advertise they can follow, proxy the
 *   rest. Lets one mount serve browsers and services at once.
 *
 * A redirect requires the owner to publish `cluster({ publicAddress })`.
 * Without it this mount proxies regardless, rather than sending a client
 * somewhere it cannot reach.
 */
export type ActorMissPolicy = 'proxy' | 'redirect' | 'auto';

/** Knobs that change what the synthesized wrappers do, so the resolver
 *  memo has to be keyed by them. */
export interface ActorResolverOptions {
    /** Default `'proxy'` — today's behaviour, byte for byte. */
    onMiss?: ActorMissPolicy;
    /** This mount's path prefix, used to compose the redirect endpoint.
     *  Default `/_sigx/actor`. */
    base?: string;
    /** Redirects one call may be told to follow before this mount proxies
     *  instead. Default 2. */
    maxHops?: number;
}

export interface ActorRequestOptions
    extends Omit<ServerFnRequestOptions, 'resolve' | 'renderBoundaries'>,
        ActorResolverOptions {
    /** The running host — explicit, never ambient. */
    host: Host;
    /**
     * How long an NDJSON stream may go silent before the endpoint emits a
     * keepalive `{"ping":1}` line. `0` disables it. Default 30 s, matching
     * `$live`.
     *
     * A live stream is mostly quiet — that is the normal case, not the edge —
     * and every intermediary with an idle timeout (ingress at 60 s, cloud load
     * balancers at ~4 min, mobile NATs) closes a bytes-silent response. The
     * client then sees a stream that "ended without a done/error terminator"
     * and has to reconnect, paying a full catch-up read each time.
     *
     * Applied to every streaming actor method, including `$live` — whose own
     * chunk-level ping predates this and stays, since it must reach the
     * subscription multiplexer rather than only the byte layer.
     */
    streamPingMs?: number;
}

/** Does this request target the actor endpoint? A predicate, not a
 *  combinator — composition stays in the entry (the `matchesServerFn`
 *  idiom). */
export function matchesActorRequest(request: Request, base = '/_sigx/actor'): boolean {
    const prefix = base.endsWith('/') ? base : `${base}/`;
    return new URL(request.url).pathname.startsWith(prefix);
}

/** Handle one actor RPC request. Mount beside `handleServerFnRequest`. */
export async function handleActorRequest(
    request: Request,
    options: ActorRequestOptions
): Promise<Response> {
    const { host, onMiss, base, maxHops, streamPingMs, ...rest } = options;
    const resolverOptions: ActorResolverOptions = {
        ...(onMiss !== undefined ? { onMiss } : {}),
        ...(base !== undefined ? { base } : {}),
        ...(maxHops !== undefined ? { maxHops } : {})
    };
    const response = await handleServerFnRequest(request, {
        ...rest,
        resolve: resolverFor(host, resolverOptions)
    });
    return withKeepalive(response, streamPingMs ?? DEFAULT_STREAM_PING_MS);
}

/** See `ActorRequestOptions.streamPingMs`. Matches `DEFAULT_LIVE_PING_MS`. */
export const DEFAULT_STREAM_PING_MS = 30_000;

const PING_LINE = new TextEncoder().encode('{"ping":1}\n');

/**
 * Re-wrap a streaming response so silence produces bytes (#178).
 *
 * At the BYTE layer, deliberately, rather than inside each stream generator:
 * core owns the NDJSON envelope, the actor's generator owns the values, and
 * neither can emit a line the other does not know about. Wrapping the body is
 * the one place that sees "nothing has been written for a while" for every
 * streaming method at once — including a `streams:` body parked on
 * `ctx.changes()`, which is exactly the quiet case that breaks.
 *
 * A non-streaming response is returned UNTOUCHED, object identity included:
 * unary calls are the overwhelming majority and must pay nothing.
 */
function withKeepalive(response: Response, pingMs: number): Response {
    if (pingMs <= 0 || !response.body) return response;
    if (!response.headers.get('content-type')?.includes('x-ndjson')) return response;

    const reader = response.body.getReader();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let sink: ReadableStreamDefaultController<Uint8Array> | null = null;

    const disarm = (): void => {
        if (timer !== undefined) clearTimeout(timer);
        timer = undefined;
    };

    /**
     * Start (or restart) the silence clock.
     *
     * Armed when a read begins and again after every write, so a ping means
     * `pingMs` of actual SILENCE rather than every `pingMs` of wall clock. A
     * stream already producing bytes needs no keepalive — that is the whole
     * premise — so pinging it anyway would be payload and parse work for
     * nothing. Re-arming also bounds the worst case, which a fixed interval
     * plus a silence test does not: that combination lets up to `2 × pingMs`
     * pass between writes, and for the 30 s default that lands exactly on the
     * 60 s idle timeout this exists to survive.
     *
     * A tick that cannot write does NOT re-arm, and that is the whole
     * lifetime story: the timer exists only while somebody is waiting for
     * bytes. Re-arming regardless would leave an abandoned response — nobody
     * reading, nobody cancelling — ticking for the life of the process. The
     * next `pull` starts the clock again, and a `pull` is exactly what a
     * consumer that resumed reading produces.
     */
    const arm = (): void => {
        disarm();
        timer = setTimeout(() => {
            timer = undefined;
            // `null` once the stream is closed or errored; `<= 0` while the
            // consumer is not draining, where the socket already has bytes
            // pending and there is no idle for a keepalive to interrupt.
            const room = sink?.desiredSize;
            if (room === null || room === undefined || room <= 0) return;
            try {
                sink!.enqueue(PING_LINE);
                arm();
            } catch {
                // Lost a race with teardown. Nothing to clean up: this tick
                // has already fired and is deliberately not re-arming.
            }
        }, pingMs);
    };

    const stream = new ReadableStream<Uint8Array>({
        start(controller) {
            sink = controller;
            arm();
        },
        async pull(controller) {
            // The silence being measured starts HERE, not at the last write:
            // this is the point where the response has nothing to send and is
            // waiting for the actor to produce something.
            arm();
            try {
                const { value, done } = await reader.read();
                if (done) {
                    disarm();
                    controller.close();
                    return;
                }
                controller.enqueue(value);
                arm();
            } catch (error) {
                // An upstream failure (a broken source, an abrupt cancel).
                // Without this the timer outlives the stream and goes on
                // pinging a response nobody will ever read again.
                disarm();
                await reader.cancel(error).catch(() => {});
                controller.error(error);
            }
        },
        cancel(reason) {
            disarm();
            // Forwarded, so the actor's generator still sees the disconnect
            // and releases its keep-alive — the #184 chain runs through here.
            return reader.cancel(reason);
        }
    });

    return new Response(stream, {
        status: response.status,
        statusText: response.statusText,
        headers: response.headers
    });
}

/**
 * Per-host, then per-CONFIGURATION: the synthesized wrappers close over
 * `onMiss`, so two mounts of one host with different miss policies — the
 * browser origin proxying while the service origin redirects — must not
 * share a cache. Keyed by a canonical string rather than the options
 * object, which is freshly built per call.
 */
const resolvers = new WeakMap<
    Host,
    Map<string, (symbol: string) => unknown | Promise<unknown>>
>();

function resolverFor(
    host: Host,
    options: ActorResolverOptions
): (symbol: string) => unknown | Promise<unknown> {
    let perHost = resolvers.get(host);
    if (!perHost) {
        perHost = new Map();
        resolvers.set(host, perHost);
    }
    const key = [
        options.onMiss ?? 'proxy',
        options.base ?? DEFAULT_BASE,
        options.maxHops ?? DEFAULT_MAX_HOPS
    ].join('|');
    let resolver = perHost.get(key);
    if (!resolver) {
        resolver = createActorResolver(host, options);
        perHost.set(key, resolver);
    }
    return resolver;
}

const DEFAULT_BASE = '/_sigx/actor';
const DEFAULT_MAX_HOPS = 2;

/** How many redirects this call has already been told to follow. Absent,
 *  negative and unparsable all read as 0 — a hint, never trusted input. */
function hopsOf(request: Request): number {
    const raw = request.headers.get(ACTOR_HOPS_HEADER);
    if (raw === null) return 0;
    const n = Number.parseInt(raw, 10);
    return Number.isFinite(n) && n > 0 ? n : 0;
}

const warnedHosts = new Set<string>();

/** Once per owner host: a redirect mount that silently proxies forever is
 *  a misconfiguration worth naming, but not one worth logging per request. */
function warnOnce(hostId: string): void {
    if (!__DEV__ || warnedHosts.has(hostId)) return;
    warnedHosts.add(hostId);
    console.warn(
        `[sigx actors] onMiss:'redirect' cannot redirect to host ${hostId}: it ` +
            `publishes no publicAddress, so this mount is proxying for it instead. ` +
            `Set cluster({ publicAddress }) on that host if clients can reach it directly.`
    );
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
export function createActorResolver(
    host: Host,
    options: ActorResolverOptions = {}
): (symbol: string) => unknown | Promise<unknown> {
    const cache = new Map<string, unknown>();
    return (symbol: string) => {
        const hit = cache.get(symbol);
        if (hit !== undefined) return hit;
        // The runtime's own mount, resolved BEFORE any definition lookup —
        // `$live` is not an actor type, and `defineActor` refuses to let one
        // be named that.
        if (symbol === LIVE_SYMBOL) {
            const live = synthesizeLive(host);
            cache.set(symbol, live);
            return live;
        }
        const hash = symbol.lastIndexOf('#');
        if (hash <= 0 || hash === symbol.length - 1) return notFound(symbol);
        const type = symbol.slice(0, hash);
        const method = symbol.slice(hash + 1);
        const def = host.definition(type);
        if (!def) return notFound(symbol, type);
        if (isPromise(def)) {
            return def.then((resolved) => {
                if (!resolved) return notFound(symbol, type);
                const wrapped = synthesize(host, resolved, symbol, method, options);
                cache.set(symbol, wrapped);
                return wrapped;
            });
        }
        const wrapped = synthesize(host, def, symbol, method, options);
        cache.set(symbol, wrapped);
        return wrapped;
    };
}

/**
 * The `$live` mount as a stream-flavoured serverFn. Its single wire
 * argument is the subscription array; per-subscription guards run inside,
 * so one rejection cannot fail the whole connection.
 */
function synthesizeLive(host: Host): unknown {
    return {
        __sigxName: 'subscribe',
        __sigxStream: true,
        __sigxFn: (rq: ServerFnContext, _info: ServerFnInfo, args: unknown[]) =>
            Promise.resolve(subscribeAll(host, rq, args[0]))
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
 * Deliberately NOT memoized: `host.definition()` is lazy behind the
 * `virtual:sigx-actors` registry, so a type that misses now can resolve
 * after an HMR edit adds it.
 */
function notFound(symbol: string, type?: string): unknown {
    const detail = type
        ? `no actor type "${type}" is registered with this host — is it registered, ` +
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
    host: Host,
    def: AnyActorDefinition,
    symbol: string,
    method: string,
    options: ActorResolverOptions
): unknown {
    const isStream = def.streamNames.includes(method);
    const onMiss = options.onMiss ?? 'proxy';
    const base = options.base ?? DEFAULT_BASE;
    const maxHops = options.maxHops ?? DEFAULT_MAX_HOPS;

    /**
     * Answer 421 with the owner instead of proxying — or return quietly and
     * let the caller dispatch, which forwards as it always has.
     *
     * Every early return is a fall-back to proxying, never an error: this
     * whole path is an optimization, and the one thing it must not do is
     * turn a working call into a failing one.
     */
    const redirectIfRemote = async (rq: ServerFnContext, ref: ActorRef): Promise<void> => {
        // Checked FIRST so the default mount pays nothing new at all — not
        // a header read, not a locate, not an await.
        if (onMiss === 'proxy' || host.locate === undefined) return;
        // 'auto': only callers that said they can follow get redirected.
        // Everyone else — browsers, curl, an old client — gets the proxy.
        if (onMiss === 'auto' && rq.request.headers.get(ACTOR_FOLLOW_HEADER) === null) return;
        // Bounded even against a client that ignores its own cap, for the
        // price of one header read: a redirect loop must be impossible, not
        // merely unlikely.
        if (hopsOf(rq.request) >= maxHops) return;

        const location = await host.locate(ref);
        if (location === undefined || location.local) return;

        const origin = location.owner.publicAddress;
        if (origin === undefined) {
            // The owner never published a client-reachable origin. Its
            // internal `address` is a pod IP — unreachable from outside and
            // not ours to disclose — so proxy instead.
            warnOnce(location.owner.hostId);
            return;
        }

        const endpoint = `${origin.endsWith('/') ? origin.slice(0, -1) : origin}${base}`;
        rq.responseHeaders.set(ACTOR_OWNER_HEADER, endpoint);
        throw new ServerFnError(
            421,
            `[sigx actors] ${actorLabel(ref)} is owned by ${location.owner.hostId} — ` +
                `retry at ${endpoint}`,
            {
                kind: 'wrong-host',
                // `endpoint`, never `address`: the internal origin must not
                // cross to a client.
                owner: { hostId: location.owner.hostId, endpoint },
                hops: hopsOf(rq.request) + 1
            }
        );
    };

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
        // Redirect BEFORE dispatching, never by letting a wrong-host escape
        // the routing loop. 421 is a status a user agent may retry on its
        // own (RFC 7540 §9.1.2) and actor calls are not idempotent, so the
        // answer has to be provably free of side effects — which "we have
        // not dispatched anything yet" is, and "the forward failed partway"
        // is not.
        await redirectIfRemote(rq, { type: def.type, key });
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
            // Resolves to an async ITERATOR, not a generator: `streamResponse`
            // calls `return()` on client disconnect, and a generator parked at
            // `yield*` would queue that instead of forwarding it — stranding
            // the activation's teardown and leaking its keep-alive ref. See
            // `relayStream`.
            __sigxFn: async (rq: ServerFnContext, _info: ServerFnInfo, args: unknown[]) => {
                const { key, rest, call } = await prepare(rq, args);
                const iterable = host.dispatchStream!(
                    { type: def.type, key },
                    method,
                    rest,
                    call
                );
                return relayStream(iterable, {
                    mapError: toClientError,
                    signal: rq.abortSignal
                });
            }
        };
    }

    /**
     * A `reads:` declaration turns the wrapper into a GET target, and core's
     * endpoint does the rest: it accepts GET only for a wrapper carrying both
     * flags, decodes `?args=` through the same codec and pollution-safe
     * reviver as a body, caps the query length, emits this `Cache-Control` on
     * a 2xx and `no-store` otherwise, and appends `Vary: Cookie` unless the
     * declaration opted into shared caches.
     *
     * Nothing about the dispatch changes — same guards, same redirect check,
     * same mailbox turn. A GET is the same call arriving another way.
     */
    const declared = def.__sigxActor.reads;
    // `hasOwn`, not a plain index: `reads?.[method]` follows the prototype
    // chain, so a method named `toString` (or `constructor`, `valueOf`, …)
    // found a truthy "declaration" nobody wrote — and the wrapper was then
    // stamped GET-capable with `max-age=undefined`. `validateReads` and the
    // build both work from OWN keys; this has to agree with them.
    const read = declared && Object.hasOwn(declared, method) ? declared[method] : undefined;

    return {
        __sigxName: method,
        ...(read ? { __sigxGet: true as const, __sigxCacheControl: cacheControl(read) } : {}),
        __sigxFn: async (rq: ServerFnContext, _info: ServerFnInfo, args: unknown[]) => {
            const { key, rest, call } = await prepare(rq, args);
            try {
                return await host.dispatch({ type: def.type, key }, method, rest, call);
            } catch (error) {
                throw toClientError(error);
            }
        }
    };
}

/**
 * The `Cache-Control` a declaration means — core's composition, reproduced.
 *
 * It has to be reproduced rather than called: core builds the header inside
 * `serverFn`'s options form, which an actor method never goes through, and
 * exports the type but not the function. Kept byte-identical on purpose, so
 * a serverFn and an actor read declaring the same cache are cached the same
 * way by every intermediary. (Upstream ask: export the builder.)
 */
function cacheControl(read: ActorReadCache): string {
    const swr =
        read.staleWhileRevalidate === undefined
            ? ''
            : `, stale-while-revalidate=${read.staleWhileRevalidate}`;
    return read.public === true
        ? `public, max-age=${read.maxAge}, s-maxage=${read.sMaxAge ?? read.maxAge}${swr}`
        : `private, max-age=${read.maxAge}${swr}`;
}

