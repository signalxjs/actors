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
    authorizeActorCall,
    encodePrincipal,
    enterActorRequest
} from '../guards';
import { relayStream } from '../stream-relay';
import { parseWire } from '../wire-parse';
import { encodeWire, reviveWire } from '../wire-shared';
import type { SocketReply, SocketRequest } from '../socket-wire';
import { toClientError } from './client-error';
import {
    DEFAULT_LIVE_PING_MS,
    DEFAULT_MAX_LIVE_SUBSCRIPTIONS,
    resolveMaxSubscriptions,
    toFrameError
} from './live-endpoint';
import type { ActorCallContext, AnyActorDefinition, Host } from '../types';

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
     * a typo). Subscriptions themselves land in a later PR; the option is
     * validated now so a misconfiguration throws at construction, not at
     * first use.
     */
    maxSubscriptions?: number;
    /** Outbound keepalive after this much send-silence. Default 30 s
     *  (`DEFAULT_LIVE_PING_MS`); `0` disables. */
    pingMs?: number;
    /** Masked-failure observability — defaults to `posture.onError`. */
    onError?(error: unknown, info: ServerFnInfo, ctx: ServerFnContext): void | Promise<void>;
}

export interface ActorSocketSession {
    /** Feed one inbound text message. Never throws; protocol breaches close
     *  the socket through the `close` callback instead. */
    handle(message: string): void;
    /** Tear the session down: abort every in-flight call. Call it from the
     *  adapter's socket-close event. Idempotent. */
    close(): void;
    /** Observability counters. */
    stats(): { inFlight: number; subscriptions: number };
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
    // Validated now, consumed by the live half when it lands: a typo'd bound
    // must throw at construction rather than silently disable itself later.
    resolveMaxSubscriptions(options.maxSubscriptions ?? DEFAULT_MAX_LIVE_SUBSCRIPTIONS);
    const pingMs = options.pingMs ?? DEFAULT_LIVE_PING_MS;
    const onError = options.onError ?? posture.onError;

    if (!originAllowed(request, originPolicy)) {
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
        options.close(1008, 'unauthorized');
        throw error;
    }
    // Identity pinned once for the connection's life (Decision 3 of #99).
    const principal = await encodePrincipal(rq);

    let closed = false;
    /** In-flight calls by client id. Presence = frames may still be sent. */
    const inFlight = new Map<number, { ctrl: AbortController }>();
    let pingTimer: ReturnType<typeof setTimeout> | undefined;

    const armPing = (): void => {
        if (pingTimer !== undefined) clearTimeout(pingTimer);
        pingTimer = undefined;
        if (pingMs <= 0 || closed) return;
        pingTimer = setTimeout(() => {
            pingTimer = undefined;
            reply({ p: 1 });
        }, pingMs);
    };

    const reply = (frame: SocketReply): void => {
        if (closed) return;
        try {
            send(JSON.stringify(frame));
        } catch {
            // The adapter's socket died under us; its close event tears the
            // session down, and a throwing send must not take a turn with it.
        }
        armPing();
    };

    const fail = (code: number, reason: string): void => {
        if (closed) return;
        closed = true;
        try {
            options.close(code, reason);
        } catch {
            // The adapter's socket may already be dying; `handle()` is
            // documented never to throw, so a throwing close stays here.
        }
        teardown();
    };

    const teardown = (): void => {
        if (pingTimer !== undefined) clearTimeout(pingTimer);
        pingTimer = undefined;
        for (const [, entry] of inFlight) entry.ctrl.abort();
        inFlight.clear();
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
        if ('sub' in message || 'uns' in message) {
            // The vocabulary reserves them; the live half is a later PR of
            // #99. A 501 error frame fails the subscription, never the
            // connection.
            reply({
                i: message.i,
                e: toFrameError(
                    new ServerFnError(
                        501,
                        '[sigx actors] live subscriptions over the socket are not supported yet'
                    )
                )
            });
            return;
        }
        if ('s' in message && typeof message.s === 'string' && Array.isArray(message.a)) {
            if (inFlight.has(message.i)) {
                // A reused id would interleave two calls' frames under one
                // tag — bookkeeping is corrupt, and that is a protocol
                // breach, not a failed call.
                fail(1003, 'duplicate call id');
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

    return {
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
        },
        stats(): { inFlight: number; subscriptions: number } {
            return { inFlight: inFlight.size, subscriptions: 0 };
        }
    };
}
