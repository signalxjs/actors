/**
 * The server half of the client-facing socket transport (#99): one session
 * object per connection, speaking the `@sigx/actors/socket-wire` vocabulary.
 *
 * A `Request` in, two callbacks out — that is the whole interop story. It is
 * what lets `ws`, socket.io, uWS, Bun, Deno and a Durable Object all drive
 * the same core: the adapter owns the socket, the session owns everything
 * that matters — auth, caps, dispatch, masking.
 *
 * It lives in `@sigx/actors/server`, not the transport package, because it
 * needs `authorizeActorCall`, `encodePrincipal`, `takeCallBag`, `mintCallId`,
 * `toClientError` and `relayStream` — all internal. Copying them is how two
 * transports' auth paths drift, which is the defect `guards.ts` exists to
 * prevent.
 *
 * ## The trust model, in three facts
 *
 * - **Identity is pinned at upgrade.** The prelude runs once against the
 *   upgrade request with `allowAnonymous: true` — the *connection* is not an
 *   operation — and `encodePrincipal` is held for the connection's life.
 *   v1 is cookies-only: the browser `WebSocket` constructor sends cookies
 *   for the target origin and cannot set headers.
 * - **Authorization runs per message**, with each definition's own
 *   `allowAnonymous`, exactly like a `$live` subscription. The prelude runs
 *   per message too — middleware may be a rate limiter, and skipping it per
 *   message is how you build an unmetered call path. (Authentication itself
 *   memoizes on the connection's locals, which per-call
 *   `Object.create(rq.locals)` views inherit — so identity resolves once,
 *   the gate and middleware run every time.)
 * - **Origin is checked at upgrade.** A browser opens a cross-origin
 *   WebSocket WITH COOKIES ATTACHED and no preflight; getting this wrong is
 *   textbook cross-site WebSocket hijacking. The posture is the app's own
 *   (`posture.origin ?? 'same-origin'`).
 *
 * ## What leaving the HTTP pipeline costs, repaid here
 *
 * Byte cap → `maxMessageBytes` (close 1009). Content-type gate → "parses as
 * our vocabulary or close 1003". Implicit per-origin connection cap →
 * `maxConcurrent` (a page over HTTP cannot exceed ~6 in-flight calls; over
 * one socket it can have thousands, each able to force an activation).
 * Timeout → per-call deadline from `posture.timeoutMs`, time-to-first-byte
 * for streams like core. `onError` → the same masked-failure seam. Honest
 * asymmetry, documented rather than hidden: there is ONE `ServerFnContext`
 * per connection, so middleware sees per-call `locals` views but
 * `responseHeaders`/`status()` are inert — `__DEV__` warns on use.
 *
 * The 421 wrong-host redirect is deliberately NOT repaid: a socket has one
 * peer and multiplexes many actors, so every call re-dispatches through
 * placement (`onMiss: 'proxy'` semantics — "the only correct answer for
 * browsers"). The session never calls `redirectIfRemote`.
 */
import { ServerFnError, type ServerFnContext, type ServerFnInfo } from '@sigx/server';
import { mintCallId } from '../call-id';
import { takeCallBag } from '../call-context-bag';
import { ActorMethodNotFoundError } from '../errors';
import {
    actorPosture,
    actorPrincipal,
    authorizeActorCall,
    encodePrincipal,
    enterActorRequest
} from '../guards';
import { relayStream } from '../stream-relay';
import { parseWire } from '../wire-parse';
import { encodeWire, reviveWire } from '../wire-shared';
import type { SocketReply, SocketRequest } from '../socket-wire';
import type { LiveSubscription } from '../wire-shared';
import { toClientError } from './client-error';
import {
    DEFAULT_LIVE_PING_MS,
    DEFAULT_MAX_LIVE_SUBSCRIPTIONS,
    DEFAULT_THROTTLE_POLICY,
    resolveClientThrottle,
    resolveMaxSubscriptions,
    resolveThrottlePolicy,
    toFrameError,
    type LiveThrottlePolicy
} from './live-endpoint';
import type { ActorCallContext, AnyActorDefinition, Host } from '../types';
import type { SocketSessionRecorder } from './socket-stats';

/** The upgrade-time operation the prelude runs against. */
const CONNECT_INFO: ServerFnInfo = {
    symbol: '$socket#connect',
    name: 'connect',
    transport: 'wire'
};

export interface ActorSocketSessionOptions {
    /** The running host — explicit, never ambient. */
    host: Host;
    /** The UPGRADE request; identity, origin and posture all pin from it. */
    request: Request;
    /** Write one text message to the socket. */
    send(message: string): void;
    /**
     * Close the socket with a code and reason. The session calls it for
     * protocol breaches (1003 unparseable, 1008 origin, 1009 oversized);
     * the adapter still owns the socket's own lifecycle events.
     */
    close(code: number, reason: string): void;
    /**
     * Origin policy for the upgrade, `posture.origin ?? 'same-origin'` by
     * default — the exact `ServerFnRequestOptions.origin` contract. The one
     * check that CANNOT move to per-message: cookies ride the upgrade.
     */
    origin?: 'same-origin' | 'verify-when-present' | string[] | false;
    /** Per-message byte cap. Default `posture.maxBodyBytes ?? 1 MiB`; breach
     *  closes 1009. `0` disables deliberately. */
    maxMessageBytes?: number;
    /** Most calls in flight at once. Default 256; excess calls fail with a
     *  429 error frame (the call, never the connection). `0` disables. */
    maxConcurrent?: number;
    /**
     * Most live subscriptions this CONNECTION may hold — the connection is
     * the thing that actually costs activations. Default 256, validated the
     * same way as the `$live` cap (a security bound must not be disabled by
     * a typo); a misconfiguration throws at construction, not at first use.
     */
    maxSubscriptions?: number;
    /** Outbound keepalive after this much send-silence. Default 30 s
     *  (`DEFAULT_LIVE_PING_MS`); `0` disables. */
    pingMs?: number;
    /**
     * Re-run authentication against the PINNED upgrade request every this
     * many milliseconds, and close 1008 when it no longer stands: the
     * authenticate hook throws, a previously-authenticated connection comes
     * back anonymous (sign-out, server-side revocation, an expiring signed
     * cookie), or the identity CHANGES — a swap mid-connection is a
     * reconnect, not a mutation. Default `0` = off.
     *
     * The honest contract: this answers "are the credentials presented at
     * upgrade still valid", never "what would the browser send now" — a
     * rotated cookie is invisible until the next connection. Pair with
     * {@link maxConnectionMs} when rotation matters: a reconnect is a fresh
     * upgrade, and the browser attaches its CURRENT cookies to it.
     */
    revalidateMs?: number;
    /**
     * Hard cap on one connection's lifetime; close 1008 at the cap.
     * Default `0` = off. This is also the credential-refresh mechanism —
     * see {@link revalidateMs}: subscriptions re-establish over the
     * reconnect under whatever the browser now holds, and in-flight calls
     * fail un-retried as on any drop.
     */
    maxConnectionMs?: number;
    /**
     * How much say a client gets over its own delivery rate (#247).
     *
     * A subscription may carry `w` — a REQUESTED window in ms — which this
     * policy floors and rounds up to one of a fixed ladder. Defaults to
     * `DEFAULT_THROTTLE_POLICY`, whose floor is the runtime's own 50 ms watch
     * throttle, so a client can only ever ask to be served MORE slowly.
     * `{ min: 50, buckets: [50] }` refuses the whole feature; a lower `min`
     * opts a deployment into sub-50 ms delivery deliberately.
     *
     * Validated at construction: a bad policy closes the socket with 1011
     * rather than surfacing per subscription.
     */
    throttlePolicy?: LiveThrottlePolicy;
    /**
     * The HOST's own send-buffer depth for this connection, in bytes (#252).
     *
     * The session cannot see it — `send(message: string)` is the whole
     * contract — so the adapter supplies it: `@sigx/actors-ws/node` passes
     * `() => client.bufferedAmount`. Without it `stats().bufferedBytes` is
     * `null`, which MEANS "the adapter cannot tell us" and never zero: the
     * load generators sample the CLIENT's buffer, and reading their zeros as
     * "the hosts never outran the clients" is exactly the misreading #208
     * was filed about.
     */
    bufferedBytes?(): number;
    /** Masked-failure observability — defaults to `posture.onError`. */
    onError?(error: unknown, info: ServerFnInfo, ctx: ServerFnContext): void | Promise<void>;
    /**
     * Counter observability (#166): a `socketStats()` recorder shared by
     * every session on this host/listener. The session records at the event
     * sites it owns; the APP publishes the stats object as an ops section
     * (`registry.reportOps('sockets', () => stats.snapshot())`).
     */
    stats?: SocketSessionRecorder;
}

export interface ActorSocketSession {
    /** Feed one inbound text message. Never throws; protocol breaches close
     *  the socket through the `close` callback instead. */
    handle(message: string): void;
    /** Tear the session down: abort every in-flight call. Call it from the
     *  adapter's socket-close event. Idempotent. */
    close(): void;
    /**
     * Observability counters. `bufferedBytes` is whatever the adapter's
     * `bufferedBytes` option reports, or `null` when none was supplied —
     * null MEANS "the adapter cannot tell us", never zero (#252).
     */
    stats(): { inFlight: number; subscriptions: number; bufferedBytes: number | null };
}

/**
 * The upgrade-time origin check. Same semantics as core's endpoint, with one
 * socket-specific normalization: adapters may build the upgrade `Request`
 * with a `ws:`/`wss:` URL, while the browser's `Origin` header is always
 * `http(s)://` — so schemes are folded before comparing.
 */
function originAllowed(request: Request, policy: ActorSocketSessionOptions['origin']): boolean {
    if (policy === false) return true;
    const origin = request.headers.get('origin');
    if (Array.isArray(policy)) return origin !== null && policy.includes(origin);
    if (origin === null) return policy === 'verify-when-present';
    const self = new URL(request.url);
    self.protocol = self.protocol === 'ws:' ? 'http:' : self.protocol === 'wss:' ? 'https:' : self.protocol;
    return origin === self.origin;
}

/** A lifetime bound must not be disabled by a typo — the same contract as
 *  `resolveMaxSubscriptions`. `0` is the documented off switch. */
function nonNegativeMs(name: string, value: number): number {
    if (!Number.isInteger(value) || value < 0) {
        throw new Error(
            `[sigx actors] ${name} must be a non-negative integer of milliseconds — got ` +
                `${String(value)}. Use 0 to disable it deliberately.`
        );
    }
    return value;
}

/** The socket carries text JSON; a message must byte-fit the cap. Counting
 *  code units first keeps the common ASCII case free of an encode pass. */
function withinBytes(message: string, max: number): boolean {
    if (max <= 0) return true;
    if (message.length > max) return false;
    if (message.length * 4 <= max) return true;
    return new TextEncoder().encode(message).length <= max;
}

/**
 * Build the socket session for one accepted connection.
 *
 * Async on purpose: the upgrade-time prelude (origin → middleware →
 * authenticate) completes before the first message is accepted, so a session
 * that fails to construct never sees a byte. On refusal it closes the socket
 * (1008) AND rejects, so both callback-shaped and response-shaped adapters
 * can do the right thing.
 */
export async function createActorSocketSession(
    options: ActorSocketSessionOptions
): Promise<ActorSocketSession> {
    const { host, request, send } = options;
    const posture = actorPosture();
    const originPolicy = options.origin ?? posture.origin ?? 'same-origin';
    const maxMessageBytes = options.maxMessageBytes ?? posture.maxBodyBytes ?? 1024 * 1024;
    const maxConcurrent = options.maxConcurrent ?? 256;
    // Validated at construction: a typo'd bound must throw rather than
    // silently disable itself. A misconfiguration still closes the socket
    // (1011 — this is the SERVER'S error, not the client's), because a
    // rejected construction must never leave an accepted socket dangling
    // with no session behind it.
    let maxSubscriptions: number;
    let revalidateMs: number;
    let maxConnectionMs: number;
    let throttlePolicy: LiveThrottlePolicy;
    try {
        maxSubscriptions = resolveMaxSubscriptions(
            options.maxSubscriptions ?? DEFAULT_MAX_LIVE_SUBSCRIPTIONS
        );
        throttlePolicy = resolveThrottlePolicy(options.throttlePolicy ?? DEFAULT_THROTTLE_POLICY);
        revalidateMs = nonNegativeMs('revalidateMs', options.revalidateMs ?? 0);
        maxConnectionMs = nonNegativeMs('maxConnectionMs', options.maxConnectionMs ?? 0);
    } catch (error) {
        try {
            options.close(1011, 'session misconfigured');
        } catch {
            // The socket may already be dying; the throw below is the signal.
        }
        throw error;
    }
    const pingMs = options.pingMs ?? DEFAULT_LIVE_PING_MS;
    const onError = options.onError ?? posture.onError;
    const stats = options.stats;

    if (!originAllowed(request, originPolicy)) {
        stats?.refused();
        options.close(1008, 'origin not allowed');
        throw new ServerFnError(403, '[sigx actors] socket upgrade refused: origin not allowed');
    }

    // The connection prelude. `allowAnonymous: true` is deliberate: the
    // connection is not an operation, and per-message `authorizeActorCall`
    // re-runs the gate with the right value for each definition. Inverting
    // this either 401s every anonymous-allowed app at upgrade, or waives the
    // gate for actors that need it.
    let rq: ServerFnContext;
    try {
        rq = await enterActorRequest(request, CONNECT_INFO, { allowAnonymous: true });
    } catch (error) {
        stats?.refused();
        options.close(1008, 'unauthorized');
        throw error;
    }
    // Identity pinned at upgrade (Decision 3 of #99) — and re-pinned by a
    // successful revalidation, which swaps `rq` so later per-call views
    // chain off the freshly-authenticated context.
    let principal = await encodePrincipal(rq);
    const hadPrincipal = (await actorPrincipal(rq)) != null;

    let closed = false;
    const openedAt = performance.now();
    /** Assigned before any message can arrive; `fail`/`close` only run
     *  through `handle()`/the adapter, both of which need the object. */
    let self: ActorSocketSession;
    /** In-flight calls by client id. Presence = frames may still be sent. */
    const inFlight = new Map<number, { ctrl: AbortController }>();
    /** Open subscriptions by client id — the SAME id namespace as calls,
     *  because replies carry only `i`. */
    const watches = new Map<
        number,
        { ctrl: AbortController; iterator: AsyncIterator<unknown> | null }
    >();
    let pingTimer: ReturnType<typeof setTimeout> | undefined;
    let lifetimeTimer: ReturnType<typeof setTimeout> | undefined;
    let revalidateTimer: ReturnType<typeof setInterval> | undefined;

    /**
     * The keepalive is a DEADLINE, not a heartbeat: a ping goes out `pingMs`
     * after the last outbound frame, and only if nothing was sent since.
     *
     * It used to be spelled as a `clearTimeout` + `setTimeout` on every
     * outbound frame, which reads naturally and costs a timer-heap removal
     * and insertion PER DELIVERY — profiled at 2.6% of a fan-out host's busy
     * time doing nothing but re-arming a timer it was about to cancel again
     * (#250; the measurement is in #245's BASELINES section). At the
     * Tier-3-measured ~16k deliveries/s per host that is ~32 000 heap
     * operations a second against ~10 000 live `Timeout` objects.
     *
     * The same deadline, priced properly: a frame stamps a timestamp, and one
     * self-rescheduling timer checks it. A timer per PING instead of two heap
     * operations per FRAME, with identical observable timing — a busy
     * connection is never pinged, a connection quiet for `pingMs` always is.
     */
    let lastSendAt = performance.now();

    /**
     * Polled from an ops request, so it must never turn a stats read into a
     * throw: an adapter whose socket died under it reports "no data" rather
     * than taking the whole `snapshot()` down with it.
     */
    const bufferedBytesOf = (): number | null => {
        if (options.bufferedBytes === undefined) return null;
        try {
            const bytes = options.bufferedBytes();
            return Number.isFinite(bytes) ? bytes : null;
        } catch {
            return null;
        }
    };

    const armPing = (delay = pingMs): void => {
        if (pingTimer !== undefined) clearTimeout(pingTimer);
        pingTimer = undefined;
        if (pingMs <= 0 || closed) return;
        pingTimer = setTimeout(() => {
            pingTimer = undefined;
            if (closed) return;
            // Re-check rather than assume: frames sent while this timer was
            // parked moved the deadline, and `reply` deliberately does not
            // touch the timer.
            const idle = performance.now() - lastSendAt;
            if (idle >= pingMs) {
                // `reply` stamps `lastSendAt`, so the next window starts from
                // the ping itself — as it did when the ping re-armed here.
                reply({ p: 1 });
                armPing();
                return;
            }
            armPing(pingMs - idle);
        }, delay);
    };

    /**
     * Returns the code units written, so a caller that counts deliveries
     * gets the size for free rather than re-stringifying the frame to
     * measure it (#252). `0` when nothing went out.
     */
    const reply = (frame: SocketReply): number => {
        if (closed) return 0;
        let written = 0;
        try {
            const text = JSON.stringify(frame);
            send(text);
            written = text.length;
        } catch {
            // The adapter's socket died under us; its close event tears the
            // session down, and a throwing send must not take a turn with it.
        }
        // Stamped whether or not the send landed, exactly as the keepalive
        // was re-armed before: on a socket dying under us the close event is
        // what resolves this, not a shifted ping deadline.
        lastSendAt = performance.now();
        return written;
    };

    const fail = (code: number, reason: string): void => {
        if (closed) return;
        closed = true;
        // Every 1003/1009 through here is a vocabulary breach; every 1008 is
        // a lifetime/revalidation close — the pre-session refusals (origin,
        // auth) never reach `fail`, they count as `refused` above.
        if (code === 1003 || code === 1009) stats?.protocolBreach();
        else if (code === 1008) stats?.lifetimeClose();
        try {
            options.close(code, reason);
        } catch {
            // The adapter's socket may already be dying; `handle()` is
            // documented never to throw, so a throwing close stays here.
        }
        teardown();
        stats?.closed(self, openedAt);
    };

    const teardown = (): void => {
        if (pingTimer !== undefined) clearTimeout(pingTimer);
        pingTimer = undefined;
        if (lifetimeTimer !== undefined) clearTimeout(lifetimeTimer);
        lifetimeTimer = undefined;
        if (revalidateTimer !== undefined) clearInterval(revalidateTimer);
        revalidateTimer = undefined;
        for (const [, entry] of inFlight) entry.ctrl.abort();
        inFlight.clear();
        for (const [, watch] of watches) {
            stopWatch(watch);
            stats?.subscriptionClosed();
        }
        watches.clear();
    };

    /**
     * Do the credentials presented at upgrade still stand? A FRESH context
     * per check, deliberately: core memoizes authenticate on `rq.locals`,
     * and a fresh `enter` is the one way to make it actually re-decide.
     *
     * Close 1008 on any of: authenticate throws; an authenticated
     * connection comes back anonymous (presence compared even with no
     * principal codec configured); the ENCODED identity changes — either
     * direction, because a connection that changes who it is mid-flight
     * should be a reconnect, not a mutation. A success re-pins `rq` and
     * `principal`, so later calls carry the freshly-authenticated context.
     */
    let revalidating = false;
    const revalidate = async (): Promise<void> => {
        // At most one at a time: a slow auth store must not let interval
        // ticks pile overlapping runs onto the pipeline.
        if (closed || revalidating) return;
        revalidating = true;
        try {
            const fresh = await enterActorRequest(request, CONNECT_INFO, {
                allowAnonymous: true
            });
            const encoded = await encodePrincipal(fresh);
            const hasPrincipal = (await actorPrincipal(fresh)) != null;
            if (closed) return;
            if (hasPrincipal !== hadPrincipal || encoded !== principal) {
                fail(
                    1008,
                    hadPrincipal && !hasPrincipal ? 'session expired' : 'identity changed'
                );
                return;
            }
            rq = fresh;
            principal = encoded;
        } catch {
            if (!closed) fail(1008, 'session expired');
        } finally {
            revalidating = false;
        }
    };

    const stopWatch = (watch: { ctrl: AbortController; iterator: AsyncIterator<unknown> | null }): void => {
        watch.ctrl.abort();
        // `return()` forwarded inward too — the abort wakes a parked
        // `next()`, the return releases the activation's keep-alive; the
        // same belt-and-braces pairing the `$live` endpoint uses.
        void watch.iterator?.return?.(undefined);
    };

    /**
     * One call's request-context view: per-call `locals` (prototype-chained,
     * so the connection's authenticate memo is visible but writes stay
     * per-call), per-call abort, inert response surface.
     */
    const callContext = (signal: AbortSignal): ServerFnContext => {
        const view: ServerFnContext = {
            ...rq,
            locals: Object.create(rq.locals) as Record<string, unknown>,
            abortSignal: signal,
            responseHeaders: new Headers(),
            status: (code: number): void => {
                if (__DEV__) {
                    console.warn(
                        `[sigx actors] socket session: middleware set status(${code}), which is ` +
                            `inert on a socket — there is no per-message response to carry it.`
                    );
                }
            }
        };
        return view;
    };

    const dispatchCall = async (
        id: number,
        symbol: string,
        args: unknown[],
        oneWay: boolean
    ): Promise<void> => {
        const entry = { ctrl: new AbortController() };
        inFlight.set(id, entry);
        const tracked = (): boolean => !closed && inFlight.get(id) === entry;
        let deadline: ReturnType<typeof setTimeout> | undefined;
        const disarmDeadline = (): void => {
            if (deadline !== undefined) clearTimeout(deadline);
            deadline = undefined;
        };
        stats?.callStarted();
        // Declared here so the failure path can hand `onError` the CALL'S
        // context view (per-call abort + locals), not the upgrade context.
        let rqCall: ServerFnContext | null = null;
        try {
            const hash = symbol.lastIndexOf('#');
            const type = hash > 0 ? symbol.slice(0, hash) : '';
            const method = hash > 0 ? symbol.slice(hash + 1) : '';
            if (!type || !method) {
                throw new ServerFnError(
                    404,
                    `Unknown actor "${symbol}" — expected a "Type#method" symbol.`
                );
            }
            const def = (await host.definition(type)) as AnyActorDefinition | undefined;
            if (!def) {
                throw new ServerFnError(
                    404,
                    `Unknown actor "${symbol}" — no actor type "${type}" is registered with ` +
                        `this host — is it registered, and are the client and server builds ` +
                        `from the same deploy?`
                );
            }
            // Runtime-reserved methods answer exactly like a method that does
            // not exist — same rule and same reason as the public HTTP mount.
            if (method.startsWith('$sigx:')) {
                throw new ActorMethodNotFoundError(def.type, method);
            }
            // The client sends `a: encodeWire(args)` exactly like the HTTP
            // body's `{args}`; core's endpoint revives there, so this entry
            // revives here — the codec's custom types must round-trip the
            // same on every wire.
            const [key, ...rest] = reviveWire(args) as unknown[];
            if (typeof key !== 'string' || key.length === 0) {
                throw new ServerFnError(
                    400,
                    `actor call "${symbol}" needs a non-empty string key as its first argument`
                );
            }
            rqCall = callContext(entry.ctrl.signal);
            // The FULL prelude, per message, never memoized per connection —
            // middleware may be a rate limiter. Then the authorization
            // decision with the instance as the resource.
            await authorizeActorCall(def, method, key, rqCall, 'wire');

            // The bag comes only from what THIS message's middleware stamped;
            // identity is the connection's pinned principal.
            const bag = takeCallBag(rqCall.locals);
            const call: ActorCallContext = {
                callChain: [],
                callId: mintCallId(),
                ...(bag !== undefined ? { bag } : {}),
                ...(principal !== undefined ? { principal } : {}),
                ...(oneWay ? { oneWay: true as const } : {}),
                abortSignal: entry.ctrl.signal
            };

            // Same bound as core's endpoint: pipeline + handler + a stream's
            // first chunk. A STARTED stream is not deadlined.
            if (posture.timeoutMs !== undefined && posture.timeoutMs > 0) {
                deadline = setTimeout(() => {
                    entry.ctrl.abort(
                        new ServerFnError(504, `[sigx actors] call "${symbol}" timed out`)
                    );
                }, posture.timeoutMs);
            }

            const ref = { type: def.type, key };
            if (def.streamNames.includes(method)) {
                // Optional on Host — a clear 501 beats a masked TypeError.
                if (!host.dispatchStream) {
                    throw new ServerFnError(
                        501,
                        '[sigx actors] this host cannot stream (no dispatchStream on its placement).'
                    );
                }
                const relay = relayStream(host.dispatchStream(ref, method, rest, call), {
                    mapError: toClientError,
                    signal: entry.ctrl.signal
                });
                for await (const value of relay) {
                    disarmDeadline();
                    if (!tracked()) break;
                    reply({ i: id, v: encodeWire(value) });
                }
                if (tracked() && !oneWay) reply({ i: id, d: 1 });
            } else {
                const result = await host.dispatch(ref, method, rest, call);
                disarmDeadline();
                if (tracked() && !oneWay) {
                    reply({ i: id, v: encodeWire(result) });
                    reply({ i: id, d: 1 });
                }
            }
        } catch (error) {
            disarmDeadline();
            stats?.callFailed();
            // Same seam as core: masked failures — anything that is not a
            // client-visible branded error after classification — reach
            // `onError` in dev AND prod, before the frame goes out.
            const classified = toClientError(error) as { __sigxServerFnError?: boolean };
            if (classified?.__sigxServerFnError !== true && onError) {
                try {
                    // The CALL's context view when the failure got that far —
                    // so the hook sees the per-call abort and locals, and
                    // cannot mutate the shared upgrade locals by accident.
                    await onError(
                        error,
                        { symbol, name: symbol, transport: 'wire' },
                        rqCall ?? callContext(entry.ctrl.signal)
                    );
                } catch {
                    // Its own throws are swallowed and never affect the reply.
                }
            }
            if (tracked() && !oneWay) reply({ i: id, e: toFrameError(error) });
        } finally {
            disarmDeadline();
            if (inFlight.get(id) === entry) inFlight.delete(id);
        }
    };

    /**
     * One live subscription — the same sequence the `$live` endpoint runs
     * per entry, on a per-message context: definition → 404, the FULL
     * prelude + authorization, bag from this message's middleware, the
     * connection's pinned identity, then `dispatchWatch` until `{i,uns}`,
     * a terminal error, or session teardown. Failure is PER SUBSCRIPTION —
     * a guard rejecting one widget costs the page nothing else.
     */
    const dispatchWatchFor = async (id: number, sub: LiveSubscription): Promise<void> => {
        const watch = { ctrl: new AbortController(), iterator: null as AsyncIterator<unknown> | null };
        watches.set(id, watch);
        stats?.subscriptionOpened();
        const tracked = (): boolean => !closed && watches.get(id) === watch;
        // The same bound the call path arms, applied to ESTABLISHMENT:
        // pipeline + authorization + dispatch + the FIRST value. A watch
        // read is a normal turn on a single-threaded actor, so on a busy
        // one the seed can queue arbitrarily long — and the watch path
        // deliberately has no runtime deadline (the loop lives forever), so
        // before this timer a starved seed held the subscription open
        // SILENTLY, forever, loop and keep-alive included (#180). A seeded
        // subscription is never timed out — pushes are the loop's cadence.
        let deadline: ReturnType<typeof setTimeout> | undefined;
        let timedOut: ServerFnError | null = null;
        const disarmDeadline = (): void => {
            if (deadline !== undefined) clearTimeout(deadline);
            deadline = undefined;
        };
        if (posture.timeoutMs !== undefined && posture.timeoutMs > 0) {
            deadline = setTimeout(() => {
                timedOut = new ServerFnError(
                    504,
                    `[sigx actors] subscription "${sub.t}#${sub.m}" timed out before its first value`
                );
                // The abort releases everything the starved seed held —
                // fan-out subscriber, shared loop when last-out, keep-alive.
                watch.ctrl.abort(timedOut);
            }, posture.timeoutMs);
        }
        try {
            if (!host.dispatchWatch) {
                throw new ServerFnError(
                    501,
                    '[sigx actors] this host cannot watch (no dispatchWatch on its placement).'
                );
            }
            const def = (await host.definition(sub.t)) as AnyActorDefinition | undefined;
            if (!def) throw new ServerFnError(404, `unknown actor type "${sub.t}"`);
            const rqCall = callContext(watch.ctrl.signal);
            await authorizeActorCall(def, sub.m, sub.k, rqCall, 'wire');
            const bag = takeCallBag(rqCall.locals);
            const iterable = host.dispatchWatch(
                { type: sub.t, key: sub.k },
                sub.m,
                sub.a ?? [],
                {
                    callChain: [],
                    callId: mintCallId(),
                    ...(bag !== undefined ? { bag } : {}),
                    ...(principal !== undefined ? { principal } : {}),
                    abortSignal: watch.ctrl.signal
                },
                sub.w !== undefined ? { throttleMs: sub.w } : undefined
            );
            const iterator = iterable[Symbol.asyncIterator]();
            watch.iterator = iterator;
            for (;;) {
                const { value, done } = await iterator.next();
                if (done || !tracked()) break;
                // Established: the deadline covered exactly the first value.
                disarmDeadline();
                // A pushed value is byte-identical to a `$live` frame.
                //
                // Two statements, and it must stay that way: written as
                // `stats?.delivered(reply(…))` the optional chain
                // short-circuits its own ARGUMENT when no recorder is
                // configured, so the frame is never sent at all. Every
                // subscription test caught it, which is the only reason this
                // comment is here rather than a bug.
                const written = reply({ i: id, v: encodeWire(value) });
                stats?.delivered(written);
            }
            // A fired deadline ends the aborted subscriber CLEANLY (`done`),
            // so the starvation must be re-raised to become the frame the
            // client is owed — silence is the #180 failure mode.
            if (timedOut !== null) throw timedOut;
            // The watch ended server-side (the actor stopped): like `$live`,
            // no terminal frame — a reconnect re-seeds it.
        } catch (error) {
            if (tracked()) reply({ i: id, e: toFrameError(error) });
        } finally {
            disarmDeadline();
            if (watches.get(id) === watch) {
                watches.delete(id);
                stats?.subscriptionClosed();
                // Belt and braces on EVERY exit, the failure path included:
                // an iterator the loop abandoned mid-error must still release
                // the activation's keep-alive, and once the entry leaves the
                // table `teardown()` can no longer reach it.
                stopWatch(watch);
            }
        }
    };

    /** The single-subscription validator — the same rules `$live`'s
     *  `parseSubscriptions` applies per entry. */
    const validSub = (raw: unknown): LiveSubscription | null => {
        const sub = raw as Partial<LiveSubscription> | null;
        if (typeof sub?.t !== 'string' || !sub.t) return null;
        if (typeof sub.k !== 'string' || !sub.k) return null;
        if (typeof sub.m !== 'string' || !sub.m) return null;
        if (sub.a !== undefined && !Array.isArray(sub.a)) return null;
        let w: number | undefined;
        try {
            w = resolveClientThrottle(sub.w, throttlePolicy);
            // Counted only when the policy actually MOVED the request:
            // "asked and got it" is not a thing an operator needs to see,
            // and "asked for 300, serving 1000" is (#252).
            if (w !== undefined && w !== sub.w) stats?.throttleQuantized();
        } catch {
            // Refused, never defaulted — the same posture as every other
            // field here. It becomes the per-subscription 400 the caller
            // already sends for a malformed record; the socket stays open,
            // because one bad subscription is not a vocabulary breach.
            return null;
        }
        // Absent stays absent: the watch key must be byte-for-byte the one a
        // client that never sends `w` produces (#247).
        return { t: sub.t, k: sub.k, m: sub.m, a: sub.a ?? [], ...(w !== undefined ? { w } : {}) };
    };

    const handleParsed = (message: SocketRequest): void => {
        if ('p' in message && message.p === 1) {
            reply({ p: 1 });
            return;
        }
        if (!('i' in message) || typeof message.i !== 'number' || !Number.isInteger(message.i)) {
            fail(1003, 'malformed message');
            return;
        }
        if ('c' in message && message.c === 1) {
            // Cancelling one call must not close the socket. Unknown ids are
            // ignored — a cancel legitimately races completion.
            const entry = inFlight.get(message.i);
            if (entry) {
                inFlight.delete(message.i);
                entry.ctrl.abort();
            }
            return;
        }
        if ('uns' in message && message.uns === 1) {
            // Closing is silent (no reply); unknown ids are ignored — an
            // unsubscribe legitimately races the watch ending server-side.
            const watch = watches.get(message.i);
            if (watch) {
                watches.delete(message.i);
                stopWatch(watch);
                stats?.subscriptionClosed();
            }
            return;
        }
        if ('sub' in message) {
            if (inFlight.has(message.i) || watches.has(message.i)) {
                fail(1003, 'duplicate id');
                return;
            }
            const sub = validSub(message.sub);
            if (!sub) {
                reply({
                    i: message.i,
                    e: toFrameError(
                        new ServerFnError(400, '[sigx actors] malformed subscription record')
                    )
                });
                return;
            }
            // The cap is on the CONNECTION — the thing that actually costs
            // activations — with the same rationale as `$live`'s: each watch
            // can force a distinct activation pinned for `idleAfterMs`.
            if (maxSubscriptions > 0 && watches.size >= maxSubscriptions) {
                reply({
                    i: message.i,
                    e: toFrameError(
                        new ServerFnError(
                            400,
                            `[sigx actors] too many subscriptions on one socket — ` +
                                `limit is ${maxSubscriptions}`
                        )
                    )
                });
                return;
            }
            void dispatchWatchFor(message.i, sub);
            return;
        }
        if ('s' in message && typeof message.s === 'string' && Array.isArray(message.a)) {
            if (inFlight.has(message.i) || watches.has(message.i)) {
                // A reused id would interleave two calls' frames under one
                // tag — bookkeeping is corrupt, and that is a protocol
                // breach, not a failed call. Calls and subscriptions share
                // the namespace, because replies carry only `i`.
                fail(1003, 'duplicate id');
                return;
            }
            if (maxConcurrent > 0 && inFlight.size >= maxConcurrent) {
                reply({
                    i: message.i,
                    e: toFrameError(
                        new ServerFnError(
                            429,
                            `[sigx actors] too many concurrent calls on one socket ` +
                                `(limit ${maxConcurrent})`
                        )
                    )
                });
                return;
            }
            void dispatchCall(message.i, message.s, message.a, message.w === 1);
            return;
        }
        fail(1003, 'unrecognized message');
    };

    armPing();
    if (maxConnectionMs > 0) {
        lifetimeTimer = setTimeout(() => {
            lifetimeTimer = undefined;
            fail(1008, 'connection lifetime exceeded');
        }, maxConnectionMs);
    }
    if (revalidateMs > 0) {
        revalidateTimer = setInterval(() => void revalidate(), revalidateMs);
    }

    self = {
        handle(message: string): void {
            if (closed) return;
            if (!withinBytes(message, maxMessageBytes)) {
                fail(1009, 'message too large');
                return;
            }
            let parsed: unknown;
            try {
                parsed = parseWire(message);
            } catch {
                fail(1003, 'unparseable message');
                return;
            }
            if (typeof parsed !== 'object' || parsed === null) {
                fail(1003, 'unrecognized message');
                return;
            }
            handleParsed(parsed as SocketRequest);
        },
        close(): void {
            if (closed) return;
            closed = true;
            teardown();
            stats?.closed(self, openedAt);
        },
        stats(): { inFlight: number; subscriptions: number; bufferedBytes: number | null } {
            return {
                inFlight: inFlight.size,
                subscriptions: watches.size,
                // Guarded: this is polled from an ops request, and an
                // adapter whose socket died under it must not turn a stats
                // read into a throw.
                bufferedBytes: bufferedBytesOf()
            };
        }
    };
    stats?.opened(self);
    return self;
}
