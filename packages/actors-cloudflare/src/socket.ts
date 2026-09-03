/**
 * `workerSocket` — the Worker-terminated route for the client socket
 * transport (#99, #157): browsers speaking `@sigx/actors/socket-wire` to
 * `createActorSocketSession`, upgraded by the Worker itself.
 *
 * On Workers a 101 upgrade IS a `Response`, so the constraint that forced a
 * Node upgrade adapter (`@sigx/actors-ws/node`) does not exist here: an
 * ordinary contributed route builds a `WebSocketPair`, accepts the server
 * end, constructs the session, and returns the client end inside the
 * Response. Fan-out is free — the session re-dispatches every call and
 * every subscription through placement, which on the Worker resolves each
 * ref to its Durable Object with a stub derived fresh per dispatch.
 *
 * Two things this route is NOT:
 *
 * - **A fix for #47.** Every call still hops Worker→object via
 *   `stub.fetch`, whose abort signal is swallowed at the boundary — so a
 *   departed live consumer does NOT release `keptAlive` in the objects it
 *   was watching. When empty-room economics matter (the last player leaving
 *   must release the room), terminate the socket in the object instead
 *   (#158).
 * - **Durable.** The Worker isolate holds the session; isolate eviction
 *   drops the socket like any network fault, and the client transport
 *   reconnects and re-seeds its subscriptions.
 *
 * Unlike the Node adapter there is no pre-session message buffer: the
 * client end of a `WebSocketPair` is only delivered inside the returned
 * Response, so no frame can arrive before construction resolves — awaiting
 * the session before returning 101 closes the race by shape. It also means
 * a refused construction can answer with an honest HTTP status instead of
 * Node's accept-then-1008.
 */
import type { AnyActorDefinition } from '@sigx/actors';
import type { ActorPlugin, ActorRoute } from '@sigx/actors/host';
import {
    createActorSocketSession,
    type ActorSocketSessionOptions
} from '@sigx/actors/server';
import type { DurableObjectStubResolver } from './placement';

/**
 * The slice of a Cloudflare `WebSocket` this route drives — structural on
 * purpose: no `cloudflare:workers` import and no runtime dependency on
 * `@cloudflare/workers-types`, so the module stays loadable (and testable)
 * outside workerd.
 */
export interface CloudflareWebSocketLike {
    accept(): void;
    send(message: string): void;
    close(code?: number, reason?: string): void;
    addEventListener(
        type: 'message' | 'close' | 'error',
        handler: (event: { data?: unknown }) => void
    ): void;
}

interface WebSocketPairLike {
    0: CloudflareWebSocketLike;
    1: CloudflareWebSocketLike;
}

/** Same path as `@sigx/actors-ws/node`'s, defined locally on purpose — the
 *  packages must not depend on each other for a string. */
export const DEFAULT_SOCKET_PATH = '/_sigx/socket';

/**
 * Parse the OBJECT-terminated upgrade path: `{path}/{type}/{key}`, both
 * segments URI-encoded and non-empty. Anything else — the bare path (that is
 * the Worker-terminated route's), a missing segment, an extra one — is `null`.
 * The two shapes share a prefix and are disambiguated by arity, so both
 * termination modes can coexist on one deployment.
 */
export function parseSocketActorPath(
    pathname: string,
    path = DEFAULT_SOCKET_PATH
): { type: string; key: string } | null {
    // A configured prefix with a trailing slash must mean the same mount —
    // `/ws/` silently matching nothing is a footgun, not a feature.
    while (path.length > 1 && path.endsWith('/')) path = path.slice(0, -1);
    if (!pathname.startsWith(`${path}/`)) return null;
    const segments = pathname.slice(path.length + 1).split('/');
    if (segments.length !== 2 || segments[0] === '' || segments[1] === '') return null;
    try {
        return { type: decodeURIComponent(segments[0]), key: decodeURIComponent(segments[1]) };
    } catch {
        return null;
    }
}

export interface WorkerSocketOptions
    extends Omit<ActorSocketSessionOptions, 'host' | 'request' | 'send' | 'close'> {
    /**
     * Upgrade path, matched EXACTLY — a prefix match would adopt
     * `/socketanything` and silently steal a neighbouring endpoint's
     * upgrades (the same rule, for the same reason, as the Node adapter).
     * Default `/_sigx/socket`.
     */
    path?: string;
}

/**
 * The route as a plugin — a route reaches an app's mounts through
 * `PluginRegistry.route()`, and `createFetchHandler` consults routes before
 * the actor endpoint. `createWorkerHandler({ socket })` is sugar over
 * `app.use(workerSocket(socket))`.
 */
export function workerSocket(options: WorkerSocketOptions = {}): ActorPlugin {
    const { path = DEFAULT_SOCKET_PATH, ...session } = options;

    const route: ActorRoute = {
        name: 'cloudflare:worker-socket',
        match(request: Request): boolean {
            if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') return false;
            let pathname: string;
            try {
                pathname = new URL(request.url).pathname;
            } catch {
                return false;
            }
            return pathname === path;
        },
        async handle(request, host): Promise<Response> {
            const Pair = (globalThis as { WebSocketPair?: new () => WebSocketPairLike })
                .WebSocketPair;
            if (typeof Pair !== 'function') {
                return errorResponse(
                    500,
                    '[sigx actors-cloudflare] workerSocket needs the Cloudflare Workers ' +
                        'runtime — WebSocketPair is not available here. On Node, use ' +
                        'attachActorSocket from @sigx/actors-ws/node instead.'
                );
            }
            const pair = new Pair();
            const client = pair[0];
            const server = pair[1];
            server.accept();

            let live;
            try {
                live = await createActorSocketSession({
                    ...session,
                    host,
                    request,
                    send: (message) => server.send(message),
                    close: (code, reason) => server.close(code, reason)
                });
            } catch (error) {
                // The session already closed the server end with its own
                // code; the client end was never delivered, so the handshake
                // can fail with a status instead of a dead socket. A
                // `ServerFnError` carries a client-safe status and message;
                // anything else is the server's own fault and stays masked.
                return refusalResponse(error);
            }

            server.addEventListener('message', (event) => {
                if (typeof event.data !== 'string') {
                    // Text JSON is the protocol; same 1003 posture as an
                    // unparseable message.
                    server.close(1003, 'binary message');
                    return;
                }
                live.handle(event.data);
            });
            server.addEventListener('close', () => live.close());
            server.addEventListener('error', () => live.close());

            return new Response(
                null,
                { status: 101, webSocket: client } as ResponseInit & {
                    webSocket: CloudflareWebSocketLike;
                }
            );
        }
    };

    return {
        name: 'cloudflare:worker-socket',
        setup(registry) {
            registry.route(route);
        }
    };
}

export interface ObjectSocketRouteOptions {
    /** The exact ref → stub derivation the placement uses — never a copy. */
    resolver: DurableObjectStubResolver;
    /** Upgrade path PREFIX; the actor rides as `{path}/{type}/{key}`.
     *  Default `/_sigx/socket`. */
    path?: string;
}

/**
 * The Worker half of the OBJECT-terminated socket (#158): parse the actor
 * out of the upgrade path and forward the request — verbatim, cookies and
 * `Origin` and all — to that actor's Durable Object, whose
 * `createHostDurableObject({ socket })` accepts it with the hibernation API.
 * The 101 Response carrying the client end passes straight back through.
 *
 * Registered by `createWorkerHandler({ socket: { terminate: 'object' } })`
 * rather than by users directly, because the resolver must be built from the
 * SAME namespace binding and placement options the handler already holds —
 * a hand-assembled route is one config drift away from the Worker and the
 * object disagreeing about where an actor lives.
 */
export function objectSocketRoute(options: ObjectSocketRouteOptions): ActorRoute {
    const { resolver, path = DEFAULT_SOCKET_PATH } = options;
    return {
        name: 'cloudflare:object-socket',
        match(request: Request): boolean {
            if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') return false;
            let pathname: string;
            try {
                pathname = new URL(request.url).pathname;
            } catch {
                return false;
            }
            return parseSocketActorPath(pathname, path) !== null;
        },
        async handle(request, host): Promise<Response> {
            const parsed = parseSocketActorPath(new URL(request.url).pathname, path);
            if (!parsed) return errorResponse(404, '[sigx actors-cloudflare] not found');
            // The Worker is the 404 authority, exactly as on the HTTP mount:
            // an unknown type must not mint a Durable Object. An `internal:
            // true` type (#74) takes the SAME branch — the lookup succeeds,
            // but this is a public entry point, and a 101 here where an
            // unknown type 404s would confirm the type's existence (and wake
            // its object) for whoever is probing. Same flag `isInternalActor`
            // in core's `guards.ts` reads; it is read inline here, next to
            // `stateless`, rather than widening the `/server` surface for it.
            const def = (await host.definition(parsed.type)) as AnyActorDefinition | undefined;
            if (!def || def.__sigxActor.internal === true) {
                return errorResponse(
                    404,
                    `[sigx actors-cloudflare] unknown actor type "${parsed.type}"`
                );
            }
            if (def.__sigxActor.stateless !== undefined) {
                // A stateless worker runs in whichever isolate dispatches it —
                // there is no object to terminate a socket in.
                return errorResponse(
                    400,
                    `[sigx actors-cloudflare] "${parsed.type}" is a stateless worker pool — ` +
                        `it has no Durable Object to terminate a socket in. Use the ` +
                        `worker-terminated socket for stateless calls.`
                );
            }
            // Fresh stub per upgrade, never cached — an I/O object owned by
            // this request.
            return resolver.stub({ type: parsed.type, key: parsed.key }).fetch(request);
        }
    };
}

/** The HTTP answer for a session construction that rejected. Shared by the
 *  Worker-terminated route and the object-terminated handler in `host.ts`. */
export function refusalResponse(error: unknown): Response {
    const status = statusOf(error);
    return status
        ? errorResponse(status, messageOf(error))
        : errorResponse(500, '[sigx actors-cloudflare] socket session failed to construct');
}

/**
 * A client-safe status, or 0 for "mask it". Keyed on `ServerFnError`'s own
 * brand (`__sigxServerFnError`, the same field its `isServerFnError` checks)
 * rather than the mere presence of a numeric `status` — an internal error
 * that happens to carry one must stay masked. The brand is structural so
 * this package needs no dependency on `@sigx/server` for an `instanceof`,
 * and it survives minification and realm crossings the way a class name
 * does not. Clamped to the HTTP error range as a belt on top.
 */
function statusOf(error: unknown): number {
    const shaped = error as { __sigxServerFnError?: unknown; status?: unknown } | null;
    if (shaped?.__sigxServerFnError !== true) return 0;
    const status = shaped.status;
    return typeof status === 'number' && Number.isInteger(status) && status >= 400 && status <= 599
        ? status
        : 0;
}

/** The message of a status-carrying error is client-safe by the same
 *  contract that makes its status one, and already carries its own prefix. */
function messageOf(error: unknown): string {
    const message = (error as { message?: unknown } | null)?.message;
    return typeof message === 'string' ? message : 'socket upgrade refused';
}

function errorResponse(status: number, message: string): Response {
    return new Response(JSON.stringify({ error: { message, status } }), {
        status,
        headers: { 'content-type': 'application/json' }
    });
}
