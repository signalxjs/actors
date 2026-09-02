/**
 * `createHostDurableObject` — a Durable Object that IS an actor host.
 *
 * A factory returning a class rather than a base class to extend, because
 * the object must own its seams: storage, reminders, placement and the
 * defaults all come from the Durable Object's own state, and a subclass that
 * wired them itself would be one `super()` away from silent corruption.
 * With `socket` configured the class implements `webSocketMessage` and
 * friends itself (#158), scoped to its own `sigx:socket` tag — a subclass
 * accepting sockets under a different tag composes, and one that overrides
 * the handlers must delegate tagged sockets back through `super`.
 *
 * `cloudflare:workers` is deliberately NOT imported. Extending Cloudflare's
 * `DurableObject` base is only needed for RPC entrypoints and `ctx.props`;
 * `fetch` and `alarm` work on a plain class. Importing it would make this
 * package unloadable outside workerd and take the fake-driven tests with it.
 */
import type { ActorRef, ActorStorage, Host } from '@sigx/actors';
import {
    defineActorApp,
    type ActorApp,
    type ActorAppOptions,
    type HostDefaults,
    reminderTaskLiveness
} from '@sigx/actors/host';
import {
    handleHostRequestForRuntime,
    matchesHostRequest,
    hostEndpointRuntime,
    type HostEndpointOptions,
    type HostEndpointRuntime
} from '@sigx/actors/cluster';
import {
    createActorSocketSession,
    type ActorSocketSession,
    type ActorSocketSessionOptions
} from '@sigx/actors/server';
import { durableObjectReminders, type DurableObjectReminders } from './reminders';
import { durableObjectStorage } from './storage';
import {
    durableObjectName,
    durableObjects,
    type DurableObjectPlacementOptions
} from './placement';
import { DEFAULT_SOCKET_PATH, parseSocketActorPath, refusalResponse } from './socket';
import type { DurableAlarms } from './reminders';
import type { DurableStorage } from './storage';
import type { DurableObjectIdLike, DurableObjectNamespaceLike } from './types';

/**
 * The slice of a hibernatable `WebSocket` the socket handlers drive.
 * Structural for the same reason everything in `types.ts` is: the package —
 * and the fake-driven tests — must load without a Workers runtime.
 */
export interface DurableWebSocketLike {
    send(message: string): void;
    close(code?: number, reason?: string): void;
    /** At most ~2 KiB survives an eviction; this package stores `{v, deadline?}`. */
    serializeAttachment(value: unknown): void;
    deserializeAttachment(): unknown;
}

/** The slice of `DurableObjectState` a host needs. The WebSocket-hibernation
 *  members are optional — present on real workerd, absent from older fakes. */
export interface DurableObjectStateLike {
    readonly id: DurableObjectIdLike;
    readonly storage: DurableStorage & DurableAlarms;
    blockConcurrencyWhile<T>(fn: () => Promise<T>): Promise<T>;
    acceptWebSocket?(ws: DurableWebSocketLike, tags?: string[]): void;
    getWebSockets?(tag?: string): DurableWebSocketLike[];
    setWebSocketAutoResponse?(pair: unknown): void;
}

/**
 * The seams the object derived from its own state. An `app` factory MUST
 * pass these to `defineActorApp` — they are what make the app this object's
 * rather than a generic one.
 */
export interface DurableAppOptions extends ActorAppOptions {
    storage: ActorStorage;
    defaults: HostDefaults;
}

export interface HostDurableObjectOptions<Env = unknown> {
    /** Registry, unless the `app` factory supplies one via `withActors`. */
    actors?: ActorAppOptions['actors'];
    /**
     * THE binding, for actor-to-actor calls. A call from this object to a
     * DIFFERENT actor must reach that actor's object rather than activating
     * a second copy here.
     */
    namespace(env: Env): DurableObjectNamespaceLike;
    /**
     * Add plugins. Receives the object-derived options it must pass on, and
     * deliberately NOT `env`: without a namespace binding it cannot build a
     * placement, which is what keeps `setPlacement` ours.
     */
    app?(base: DurableAppOptions): ActorApp;
    /** Must match the Worker's, byte for byte. */
    placement?: Pick<
        DurableObjectPlacementOptions,
        'objectName' | 'locationHint' | 'jurisdiction' | 'base'
    >;
    /** Forwarded to the internal mount (body caps, `onError`, `timeoutMs`). */
    endpoint?: HostEndpointOptions;
    /**
     * Terminate client sockets INSIDE this object (#158): the upgrade at
     * `{path}/{type}/{key}` — forwarded by
     * `createWorkerHandler({ socket: { terminate: 'object' } })` — is
     * accepted with `state.acceptWebSocket`, so the session lives where the
     * actor lives. That is what makes a disconnect actually release the
     * activation: teardown runs locally, `iterator.return()` reaches the
     * watch, `keptAlive` clears — no `stub.fetch` boundary for the
     * cancellation to die at (#47). Hibernation makes an idle page free.
     *
     * The survival contract is deliberately minimal: an EVICTED isolate
     * loses the session (pinned principal, in-flight calls, watches), so
     * the first message after a cold wake closes `1012 'session evicted —
     * reconnect'` and the client transport redials — a fresh upgrade with
     * the browser's CURRENT cookies, subscriptions re-seeded exactly as on
     * any drop.
     *
     * `pingMs` is not accepted: keepalive is `setWebSocketAutoResponse`,
     * answered by the runtime without waking the object — a session-owned
     * ping timer would hold the object resident, which is the cost
     * hibernation exists to remove. `maxConnectionMs` is enforced by the
     * session's own timer while resident and by a deadline in the socket
     * attachment across evictions — checked per message, so an IDLE
     * hibernated socket can nominally outlive it until its next frame (the
     * alternative is an alarm, and the object's one alarm belongs to
     * reminders).
     */
    socket?: Omit<
        ActorSocketSessionOptions,
        'host' | 'request' | 'send' | 'close' | 'pingMs'
    > & { path?: string };
}

export interface HostDurableObjectInstance {
    fetch(request: Request): Promise<Response>;
    alarm(): Promise<void>;
    /** The running host, for subclasses adding their own handlers. */
    host(): Promise<Host>;
    /**
     * The hibernation handlers, scoped to sockets tagged `sigx:socket` —
     * ones a subclass accepts itself (under its own tag) are ignored here,
     * but a subclass OVERRIDING these must delegate tagged sockets back via
     * `super.webSocketMessage(...)` or the session never hears them.
     */
    webSocketMessage(ws: DurableWebSocketLike, message: unknown): Promise<void>;
    webSocketClose(ws: DurableWebSocketLike): Promise<void>;
    webSocketError(ws: DurableWebSocketLike): Promise<void>;
}

interface Started {
    host: Host;
    app: ActorApp;
    reminders: DurableObjectReminders;
    runtime: HostEndpointRuntime;
}

/** What rides `serializeAttachment`: versioned, tiny, and nothing secret. */
interface SocketAttachment {
    v: 1;
    /** Absolute epoch-ms deadline mirroring `maxConnectionMs`, when set. */
    deadline?: number;
}

interface WebSocketPairLike {
    0: unknown;
    1: DurableWebSocketLike;
}

export function createHostDurableObject<Env = unknown>(
    options: HostDurableObjectOptions<Env>
): new (state: DurableObjectStateLike, env: Env) => HostDurableObjectInstance {
    const base = options.placement?.base ?? '/_sigx/do';
    const objectName = options.placement?.objectName ?? durableObjectName;
    const socketPath = options.socket?.path ?? DEFAULT_SOCKET_PATH;
    const SOCKET_TAG = 'sigx:socket';

    return class HostDurableObject implements HostDurableObjectInstance {
        readonly #state: DurableObjectStateLike;
        readonly #env: Env;
        #starting: Promise<Started> | null = null;
        /** Live sessions by socket. NOT persistent — an eviction empties it,
         *  which is exactly what the 1012 cold-wake close detects. */
        readonly #sessions = new Map<DurableWebSocketLike, ActorSocketSession>();
        #autoResponseArmed = false;

        constructor(state: DurableObjectStateLike, env: Env) {
            this.#state = state;
            this.#env = env;
            // Nothing async here on purpose. `blockConcurrencyWhile` in a
            // constructor looks like the idiomatic place to boot, but a throw
            // inside it RESETS the object — so a transient start failure
            // would tear the isolate down and retry invisibly instead of
            // surfacing a 503 that says what went wrong.
        }

        /** Boot once per instance; never cache a rejection. */
        #ready(): Promise<Started> {
            return (this.#starting ??= this.#boot().catch((error: unknown) => {
                this.#starting = null;
                throw error;
            }));
        }

        async #boot(): Promise<Started> {
            const state = this.#state;
            if (state.id.name === undefined) {
                // Without a name there is nothing to compare a ref against, so
                // `isSelf` would answer false for EVERY ref — including this
                // object's own actor. The object would then fetch itself,
                // forever. Refuse to start rather than run a host that is
                // guaranteed to recurse.
                //
                // `name` is populated for ids from `idFromName`, which is what
                // the placement uses; an id parsed with `idFromString` has
                // none, and cannot host an actor here.
                throw new Error(
                    '[sigx actors-cloudflare] this Durable Object id has no name, so it ' +
                        'cannot tell which actor it hosts — every call would be forwarded ' +
                        'back out and the object would fetch itself. Route through ' +
                        'idFromName() (which the placement does), not idFromString().'
                );
            }
            const gate = <T,>(fn: () => Promise<T>): Promise<T> =>
                state.blockConcurrencyWhile(fn);

            const storage = durableObjectStorage(state.storage, {
                blockConcurrencyWhile: gate
            });
            const reminders = durableObjectReminders({
                storage: state.storage,
                alarms: state.storage,
                blockConcurrencyWhile: gate
            });

            const appBase: DurableAppOptions = {
                storage,
                reminders,
                // One actor per object and an alarm that IS the wake-up: a
                // reminder per running task is the right liveness here, and
                // a per-host roster would be a roster of one.
                taskLiveness: reminderTaskLiveness(),
                // No idle sweeper: the platform evicts this whole object when
                // it goes idle, so a sweeper could never usefully fire — and
                // a pending interval would hold the object in memory, and
                // billable, for as long as the actor exists.
                defaults: { sweepIntervalMs: 0 }
            };
            const app = options.app?.(appBase) ?? defineActorApp(appBase);
            if (!app.hasActors) {
                if (!options.actors) {
                    throw new Error(
                        '[sigx actors-cloudflare] no actors registered — pass `actors` to ' +
                            'createHostDurableObject(), or call withActors() in the `app` factory.'
                    );
                }
                app.withActors(options.actors);
            }

            // The SAME placement the Worker uses, plus the self predicate.
            // `setPlacement` is exclusive, so an app factory that already
            // installed `durableObjects()` throws here naming both plugins —
            // the core's own guard IS the recursion guard.
            app.use(
                durableObjects({
                    namespace: options.namespace(this.#env),
                    isSelf: (ref) => objectName(ref) === state.id.name,
                    hostId: state.id.name ?? state.id.toString(),
                    ...options.placement
                })
            );

            const host = await app.start();
            return {
                host,
                app,
                reminders,
                runtime: this.#guardIdentity(hostEndpointRuntime(host))
            };
        }

        /**
         * Refuse a call for an actor this object does not host.
         *
         * Under Durable Objects `ref` → object id is a pure function, so this
         * cannot be a race the way a cluster's wrong-host can: it means the
         * Worker and this object disagree about `objectName`, `jurisdiction`
         * or which namespace is bound. Failing here names both sides, rather
         * than letting one actor quietly exist in two objects.
         *
         */
        #guardIdentity(runtime: HostEndpointRuntime): HostEndpointRuntime {
            // Always present: `#boot()` refuses to start without it.
            const own = this.#state.id.name as string;
            const check = (ref: ActorRef): void => {
                const expected = objectName(ref);
                if (expected === own) return;
                throw new Error(
                    `[sigx actors-cloudflare] this Durable Object is "${own}", but a call ` +
                        `for ${ref.type}/${ref.key} (which derives "${expected}") arrived. ` +
                        `The Worker and this object disagree about objectName/jurisdiction, ` +
                        `or the wrong namespace is bound.`
                );
            };
            return {
                resolve: (symbol) => runtime.resolve(symbol),
                dispatch: (ref, method, args, call) => {
                    check(ref);
                    return runtime.dispatch(ref, method, args, call);
                },
                dispatchStream: (ref, method, args, call) => {
                    check(ref);
                    return runtime.dispatchStream(ref, method, args, call);
                },
                dispatchWatch: (ref, method, args, call, watch) => {
                    check(ref);
                    return runtime.dispatchWatch(ref, method, args, call, watch);
                }
            };
        }

        async host(): Promise<Host> {
            return (await this.#ready()).host;
        }

        async fetch(request: Request): Promise<Response> {
            if (
                options.socket &&
                request.headers.get('upgrade')?.toLowerCase() === 'websocket'
            ) {
                const parsed = parseSocketActorPath(new URL(request.url).pathname, socketPath);
                if (parsed) return this.#acceptSocket(request, parsed);
            }
            const { runtime } = await this.#ready();
            if (!matchesHostRequest(request, base)) {
                return new Response(
                    JSON.stringify({
                        error: {
                            message:
                                `[sigx actors-cloudflare] this Durable Object serves the ` +
                                `actor mount at ${base}/ only.`,
                            status: 404
                        }
                    }),
                    { status: 404, headers: { 'content-type': 'application/json' } }
                );
            }
            // No `secret`: a stub is not network-reachable, and holding the
            // namespace binding is the capability grant.
            return handleHostRequestForRuntime(request, {
                ...options.endpoint,
                // This mount lives at `/_sigx/do`, not the internal default.
                // Core checks the path prefix itself (signalxjs/core#563) and everything
                // after it IS the symbol, so a mount that does not name its
                // own base 404s every call before resolving.
                base,
                runtime
            });
        }

        async alarm(): Promise<void> {
            // Booting first is required, not defensive: `onAlarm()` throws if
            // the host has not bound its reminders, and an alarm can be the
            // FIRST thing an evicted object sees.
            const { reminders } = await this.#ready();
            await reminders.onAlarm();
        }

        /**
         * Accept an object-terminated client socket (#158). The 101 Response
         * is only returned after the session constructed, and the client end
         * of a `WebSocketPair` only exists inside that Response — so no
         * frame can race construction, the same argument (and the same
         * absence of a pre-session buffer) as `workerSocket`.
         */
        async #acceptSocket(
            request: Request,
            ref: { type: string; key: string }
        ): Promise<Response> {
            const state = this.#state;
            // The same disagreement `#guardIdentity` catches on the internal
            // mount: an upgrade for an actor this object does not host means
            // the forwarding route and this object derive names differently.
            const expected = objectName(ref);
            if (expected !== state.id.name) {
                return socketError(
                    403,
                    `this Durable Object is "${String(state.id.name)}", but a socket for ` +
                        `${ref.type}/${ref.key} (which derives "${expected}") arrived. The ` +
                        `Worker's forwarding route and this object disagree about ` +
                        `objectName/jurisdiction, or the wrong namespace is bound.`
                );
            }
            if (typeof state.acceptWebSocket !== 'function') {
                return socketError(
                    500,
                    'this runtime has no WebSocket hibernation API (state.acceptWebSocket) — ' +
                        'the object-terminated socket needs real workerd or a fake providing it.'
                );
            }
            const Pair = (globalThis as { WebSocketPair?: new () => WebSocketPairLike })
                .WebSocketPair;
            if (typeof Pair !== 'function') {
                return socketError(500, 'WebSocketPair is not available in this runtime.');
            }

            const { host } = await this.#ready();
            const pair = new Pair();
            const client = pair[0];
            const server = pair[1];
            // The hibernation API, not accept(): the runtime owns delivery
            // through webSocketMessage/Close/Error, and an idle socket costs
            // nothing while the object is evicted.
            state.acceptWebSocket(server, [SOCKET_TAG]);

            const { path: _path, ...session } = options.socket!;
            let live: ActorSocketSession;
            try {
                live = await createActorSocketSession({
                    ...session,
                    // Keepalive belongs to setWebSocketAutoResponse below —
                    // a session-owned timer would pin the object resident.
                    pingMs: 0,
                    host,
                    request,
                    send: (message) => server.send(message),
                    close: (code, reason) => server.close(code, reason)
                });
            } catch (error) {
                // The session closed the accepted end with its own code; the
                // client end was never delivered, so answer with a status.
                return refusalResponse(error);
            }

            const attachment: SocketAttachment = { v: 1 };
            if (session.maxConnectionMs) {
                attachment.deadline = Date.now() + session.maxConnectionMs;
            }
            server.serializeAttachment(attachment);

            if (!this.#autoResponseArmed && typeof state.setWebSocketAutoResponse === 'function') {
                const AutoPair = (
                    globalThis as {
                        WebSocketRequestResponsePair?: new (
                            request: string,
                            response: string
                        ) => unknown;
                    }
                ).WebSocketRequestResponsePair;
                if (AutoPair) {
                    // A client `{"p":1}` ping is answered by the RUNTIME,
                    // without waking a hibernated object. Marked armed only
                    // on success, so a failure here retries on the next
                    // accept instead of silently disabling keepalive for
                    // the isolate's life.
                    state.setWebSocketAutoResponse(new AutoPair('{"p":1}', '{"p":1}'));
                    this.#autoResponseArmed = true;
                }
            }

            this.#sessions.set(server, live);
            return new Response(
                null,
                { status: 101, webSocket: client } as ResponseInit & { webSocket: unknown }
            );
        }

        /**
         * Only sockets this class accepted (tag `sigx:socket`) are ours; a
         * subclass's own sockets pass through untouched. The sessions map is
         * checked FIRST and the tag second, because the runtime removes a
         * closing socket from `getWebSockets` before delivering
         * `webSocketClose` — gate on the tag alone and the close handler
         * ignores exactly the event it exists for, leaving the watch held
         * and the object billable (the #47 shape, reintroduced one layer
         * up). The tag check still matters for the post-hibernation socket,
         * which has no session yet.
         */
        #ownsSocket(ws: DurableWebSocketLike): boolean {
            if (this.#sessions.has(ws)) return true;
            return this.#state.getWebSockets?.(SOCKET_TAG)?.includes(ws) ?? false;
        }

        #deadlinePassed(ws: DurableWebSocketLike): boolean {
            let attachment: unknown;
            try {
                attachment = ws.deserializeAttachment();
            } catch {
                return false;
            }
            const deadline = (attachment as SocketAttachment | null)?.deadline;
            return typeof deadline === 'number' && Date.now() > deadline;
        }

        #dropSession(ws: DurableWebSocketLike): void {
            this.#sessions.get(ws)?.close();
            this.#sessions.delete(ws);
        }

        async webSocketMessage(ws: DurableWebSocketLike, message: unknown): Promise<void> {
            if (!this.#ownsSocket(ws)) return;
            if (typeof message !== 'string') {
                // Text JSON is the protocol; same 1003 posture as the other
                // adapters.
                this.#dropSession(ws);
                ws.close(1003, 'binary message');
                return;
            }
            if (this.#deadlinePassed(ws)) {
                // The eviction-surviving half of maxConnectionMs: the
                // session's own timer dies with the isolate, so the deadline
                // rides the attachment and is enforced lazily, per message.
                this.#dropSession(ws);
                ws.close(1008, 'connection lifetime exceeded');
                return;
            }
            const session = this.#sessions.get(ws);
            if (!session) {
                // A cold wake: the socket survived hibernation, the session
                // (pinned principal, watches, in-flight calls) did not. A
                // reconnect is the CORRECT recovery, not merely the cheap
                // one — a fresh upgrade carries the browser's current
                // cookies, and the client re-seeds its subscriptions the
                // same way it does after any drop.
                ws.close(1012, 'session evicted — reconnect');
                return;
            }
            session.handle(message);
        }

        async webSocketClose(ws: DurableWebSocketLike): Promise<void> {
            // THE #47 fix, in one line: the session tears down inside the
            // object that owns the actor, so `iterator.return()` reaches the
            // watch locally and `keptAlive` clears — there is no stub
            // boundary for the cancellation to die at.
            if (!this.#ownsSocket(ws)) return;
            this.#dropSession(ws);
        }

        async webSocketError(ws: DurableWebSocketLike): Promise<void> {
            if (!this.#ownsSocket(ws)) return;
            this.#dropSession(ws);
        }
    };
}

function socketError(status: number, message: string): Response {
    return new Response(
        JSON.stringify({ error: { message: `[sigx actors-cloudflare] ${message}`, status } }),
        { status, headers: { 'content-type': 'application/json' } }
    );
}
