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
    const host = request.headers.host ?? 'localhost';
    const headers = new Headers();
    for (const [name, value] of Object.entries(request.headers)) {
        if (typeof value === 'string') headers.set(name, value);
        else if (Array.isArray(value)) for (const one of value) headers.append(name, one);
    }
    return new Request(`http://${host}${request.url ?? '/'}`, { headers });
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
                    const buffered: string[] = [];
                    client.on('message', (data, isBinary) => {
                        if (isBinary) {
                            // Text JSON is the protocol; same 1003 posture as
                            // an unparseable message.
                            client.close(1003, 'binary message');
                            return;
                        }
                        const text = String(data);
                        if (live) live.handle(text);
                        else buffered.push(text);
                    });
                    client.on('close', () => {
                        live?.close();
                        live = null;
                    });
                    createActorSocketSession({
                        ...session,
                        request: toRequest(request),
                        send: (message) => client.send(message),
                        close: (code, reason) => client.close(code, reason)
                    }).then(
                        (created) => {
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
