/**
 * `@sigx/actors-ws/node` — the Node server adapter: hook the socket session
 * into an HTTP server's `'upgrade'` event.
 *
 * `attachActorSocket()` is SUGAR for people who do not already run a
 * WebSocket server. The manual form is the honest answer to "hook into a
 * server I already own": keep your own `'upgrade'` listener (or socket.io
 * namespace, or uWS handler) and construct `createActorSocketSession`
 * yourself — a `Request` in, two callbacks out is the whole contract, and
 * `toRequest()` builds that `Request` from a Node upgrade. Runtimes that
 * hand you a real `Request` and a real `WebSocket` (Bun, Deno, Cloudflare)
 * need no adapter at all.
 *
 * Path matching is EXACT on the parsed pathname — a prefix match would
 * adopt `/socketanything` and silently steal a neighbouring endpoint's
 * upgrades. Unmatched upgrades are destroyed only when this listener is the
 * server's only one; when others exist, the upgrade is theirs to answer.
 */
import type { IncomingMessage, Server } from 'node:http';
import type { Duplex } from 'node:stream';
import type {
    ActorSocketSession,
    ActorSocketSessionOptions
} from '@sigx/actors/server';
import { createActorSocketSession } from '@sigx/actors/server';

/** The one method this adapter needs from `ws`'s `WebSocketServer`. */
export interface WebSocketServerLike {
    handleUpgrade(
        request: IncomingMessage,
        socket: Duplex,
        head: Buffer,
        done: (client: MinimalWebSocket) => void
    ): void;
}

/** The slice of a `ws` WebSocket the adapter drives. */
export interface MinimalWebSocket {
    send(message: string): void;
    close(code?: number, reason?: string): void;
    on(event: 'message', listener: (data: unknown, isBinary: boolean) => void): void;
    on(event: 'close', listener: () => void): void;
    /**
     * Bytes queued but not yet flushed to the socket — the HOST-side depth
     * the session reports as `stats().bufferedBytes` (#252). Optional so a
     * hand-rolled `wss` stand-in stays a valid `WebSocketServerLike`; absent
     * simply means the host cannot report its buffer.
     */
    readonly bufferedAmount?: number;
}

export const DEFAULT_SOCKET_PATH = '/_sigx/socket';

export interface AttachActorSocketOptions
    extends Omit<ActorSocketSessionOptions, 'request' | 'send' | 'close'> {
    /** Upgrade path, matched EXACTLY. Default `/_sigx/socket`. */
    path?: string;
    /** Bring your own server (`new WebSocketServer({ noServer: true })`);
     *  without one, `ws` is imported dynamically — it is an optional peer,
     *  needed only by this adapter. */
    wss?: WebSocketServerLike;
}

/**
 * Build a WinterCG `Request` from a Node upgrade. The URL scheme is `http`
 * (the session folds `ws:`/`http:` when checking origin); headers — cookies
 * and `Origin` included — carry over verbatim, which is exactly what the
 * session's upgrade-time auth and origin check need.
 */
export function toRequest(request: IncomingMessage): Request {
    // TLS upgrades read as `https:` so a same-origin check against a
    // browser's `Origin: https://…` compares like with like.
    const encrypted =
        (request.socket as { encrypted?: boolean } | undefined)?.encrypted === true;
    const scheme = encrypted ? 'https' : 'http';
    const headers = new Headers();
    for (const [name, value] of Object.entries(request.headers)) {
        if (typeof value === 'string') headers.set(name, value);
        else if (Array.isArray(value)) for (const one of value) headers.append(name, one);
    }
    // Host header and url are UNTRUSTED input, and this runs inside an
    // upgrade callback where a throw would be an uncaught exception. A
    // malformed pair falls back to a URL whose origin matches nothing, so a
    // same-origin posture fails CLOSED rather than crashing the process.
    let url: URL;
    try {
        url = new URL(request.url ?? '/', `${scheme}://${request.headers.host ?? 'localhost'}`);
    } catch {
        url = new URL(`${scheme}://localhost/`);
    }
    return new Request(url, { headers });
}

/**
 * Register the upgrade listener. Returns a detach function; the sockets a
 * detached listener already accepted keep running until their own close.
 */
export function attachActorSocket(
    server: Server,
    options: AttachActorSocketOptions
): () => void {
    const { path = DEFAULT_SOCKET_PATH, wss, ...session } = options;
    // Lazy so a server that never sees an upgrade never imports `ws`.
    let resolved: Promise<WebSocketServerLike> | null = null;
    const serverFor = (): Promise<WebSocketServerLike> =>
        (resolved ??= wss
            ? Promise.resolve(wss)
            : import('ws').then(
                  (m) =>
                      new m.WebSocketServer({ noServer: true }) as unknown as WebSocketServerLike
              ));

    const onUpgrade = (request: IncomingMessage, socket: Duplex, head: Buffer): void => {
        // The EXACT pathname, query excluded — never a prefix.
        let pathname: string;
        try {
            pathname = new URL(request.url ?? '/', 'http://localhost').pathname;
        } catch {
            pathname = '';
        }
        if (pathname !== path) {
            // Someone else's upgrade — unless nobody else is listening, in
            // which case the connection would hang forever un-answered.
            if (server.listenerCount('upgrade') === 1) socket.destroy();
            return;
        }
        void serverFor()
            .then((ws) => {
                ws.handleUpgrade(request, socket, head, (client) => {
                    // Listeners FIRST: `ws` starts reading as soon as the
                    // upgrade completes, and a message emitted before the
                    // session resolves must buffer, not vanish.
                    let live: ActorSocketSession | null = null;
                    let dead = false;
                    const buffered: string[] = [];
                    // The session's own byte cap applies once it exists; the
                    // pre-session buffer enforces the SAME bound so an
                    // oversized first frame cannot buy a large allocation in
                    // the window before construction resolves.
                    const capBytes = session.maxMessageBytes ?? 1024 * 1024;
                    let bufferedBytes = 0;
                    client.on('message', (data, isBinary) => {
                        if (isBinary) {
                            // Text JSON is the protocol; same 1003 posture as
                            // an unparseable message.
                            client.close(1003, 'binary message');
                            return;
                        }
                        const text = String(data);
                        if (live) {
                            live.handle(text);
                            return;
                        }
                        bufferedBytes += text.length;
                        if (capBytes > 0 && bufferedBytes > capBytes) {
                            dead = true;
                            buffered.length = 0;
                            client.close(1009, 'message too large');
                            return;
                        }
                        buffered.push(text);
                    });
                    client.on('close', () => {
                        dead = true;
                        live?.close();
                        live = null;
                    });
                    createActorSocketSession({
                        ...session,
                        request: toRequest(request),
                        send: (message) => client.send(message),
                        close: (code, reason) => client.close(code, reason),
                        // The host-side send-buffer depth (#252/#208). `ws`
                        // exposes it directly; the session cannot see it,
                        // because `send(message: string)` is the whole
                        // contract between them. An adapter that omits this
                        // reports `null` — "cannot tell you" — rather than a
                        // zero anyone could read as "not buffering".
                        bufferedBytes: () => client.bufferedAmount ?? 0
                    }).then(
                        (created) => {
                            // The socket may have closed while the session was
                            // constructing; a late session must not keep its
                            // pinned identity and prelude work alive.
                            if (dead) {
                                created.close();
                                return;
                            }
                            live = created;
                            for (const message of buffered.splice(0)) created.handle(message);
                        },
                        () => {
                            // The session already closed the socket with its
                            // own code (1008); this is only the backstop for
                            // a failure before it got that far.
                            client.close(1008, 'refused');
                        }
                    );
                });
            })
            .catch(() => {
                socket.destroy();
            });
    };

    server.on('upgrade', onUpgrade);
    return () => {
        server.off('upgrade', onUpgrade);
    };
}
