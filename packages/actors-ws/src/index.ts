/**
 * `@sigx/actors-ws` — silo-to-silo traffic over WebSocket.
 *
 * It speaks the same frames as `@sigx/actors-tcp` (`@sigx/actors/cluster/frames`)
 * with the same multiplexing, cancellation and credit semantics. The only
 * difference is the socket underneath — and the three things that buys:
 *
 *  - **One port.** Frames ride the HTTP listener the silo already has, so
 *    there is no second port to open in a security group or a Service.
 *  - **Through proxies and load balancers**, which forward WebSocket and
 *    generally will not forward an arbitrary TCP protocol.
 *  - **Dialable from WinterCG runtimes**, since the client half is the
 *    standard `WebSocket` — something `node:net` can never be.
 *
 * Raw TCP is the better wire where you control the network; this is for the
 * cases TCP cannot reach.
 *
 * ```ts
 * import { createServer } from 'node:http';
 * import { cluster, httpTransport } from '@sigx/actors/cluster';
 * import { wsTransport } from '@sigx/actors-ws';
 *
 * const ws = wsTransport({ advertiseUrl: () => `ws://10.0.4.7:7311/_sigx/silo-ws` });
 * const app = defineActorApp({ actors, storage }).use(
 *     cluster({ providers, advertise, secret, transport: [ws, httpTransport()] })
 * );
 *
 * const server = createServer(createAppHandler(app));
 * // The upgrade a contributed route cannot express. `attach` resolves the
 * // instance `cluster()` built, so there is no handle to thread by hand.
 * await ws.attach(server);
 * await new Promise<void>((r) => server.listen(7311, r));
 * await app.start();
 * ```
 */
import type { Server as HttpServer, IncomingMessage } from 'node:http';
import type { Duplex } from 'node:stream';
import {
    FrameType,
    SiloConnection,
    DEFAULT_CREDIT,
    DEFAULT_MAX_FRAME_BYTES
} from '@sigx/actors/cluster/frames';
import {
    encodeEnvelope,
    signAuth,
    watchSymbol,
    type MembershipView,
    type SiloDescriptor,
    type SiloTransport,
    type SiloTransportConfig,
    type SiloTransportFactory,
    type SiloTransportRuntime
} from '@sigx/actors/cluster';
import { ActorUnreachableError } from '@sigx/actors';
import type { ActorCallContext, ActorDispatcher, ActorRef } from '@sigx/actors';
import { webSocketLink, type MinimalWebSocket } from './link';

export { webSocketLink, type MinimalWebSocket } from './link';
export { DEFAULT_CREDIT, DEFAULT_MAX_FRAME_BYTES } from '@sigx/actors/cluster/frames';

/** This transport's key in `SiloDescriptor.addresses`. */
export const WS_TRANSPORT_NAME = 'ws';
/** Default path the upgrade handler matches. */
export const DEFAULT_WS_PATH = '/_sigx/silo-ws';

export interface WsTransportOptions {
    /** Path the upgrade handler matches. Default `/_sigx/silo-ws`. */
    path?: string;
    /**
     * The URL peers should dial, e.g. `ws://10.0.4.7:7311/_sigx/silo-ws`.
     *
     * A function, because the silo's own port is often not known until its
     * HTTP listener has bound — and the seam requires the address be produced
     * at `start()`, *before* the membership join, so nothing is advertised
     * that is not already answering.
     */
    advertiseUrl(): string | Promise<string>;
    maxFrameBytes?: number;
    credit?: number;
    /** Idle ping interval, ms. 0 disables. Default 15 000. */
    keepAliveMs?: number;
    /**
     * How to open a client connection. Defaults to the global `WebSocket`,
     * which exists on Node 22+ and on every WinterCG runtime. Supply `ws`'s
     * implementation to run on older Node.
     */
    connect?(url: string): MinimalWebSocket;
}

/** The bits of a `SiloTransport` this package adds for the upgrade handler. */
export interface WsSiloTransport extends SiloTransport {
    /** @internal — used by `attachSiloUpgrade`. */
    readonly __accept: (socket: MinimalWebSocket) => void;
    /** @internal — the path the upgrade handler should match. */
    readonly __path: string;
    /** Live peer connections; the conformance suite's link probe. */
    openLinks(): number;
}

/**
 * A `wsTransport()` handle: usable directly as `cluster({ transport })`, and
 * carrying `attach()` so the built instance is reachable.
 *
 * That second part exists because `cluster()` builds the transport internally
 * from the factory, so a caller otherwise has no handle to give the upgrade
 * handler — and the upgrade cannot go through the route seam.
 */
export interface WsTransportHandle {
    (config: SiloTransportConfig): SiloTransport;
    /**
     * Attach the silo upgrade handler to a server. Safe to call before or
     * after `cluster()` builds the transport; it waits for the instance.
     * Returns a detach function.
     */
    attach(server: HttpServer, options?: { wss?: WebSocketServerLike }): Promise<() => void>;
}

export function wsTransport(options: WsTransportOptions): WsTransportHandle {
    let built: SiloTransport | undefined;
    let announce: ((t: SiloTransport) => void) | undefined;
    const ready = new Promise<SiloTransport>((resolve) => {
        announce = resolve;
    });

    const factory = (config: SiloTransportConfig): SiloTransport => {
        const path = options.path ?? DEFAULT_WS_PATH;
        const maxFrameBytes = options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;
        const credit = options.credit ?? DEFAULT_CREDIT;
        const keepAliveMs = options.keepAliveMs ?? 15_000;

        let runtime: SiloTransportRuntime | null = null;
        let keepAlive: ReturnType<typeof setInterval> | null = null;
        const links = new Map<string, SiloConnection>();
        const dialing = new Map<string, Promise<SiloConnection>>();
        const pending = new Set<SiloConnection>();
        const outbound = new WeakSet<SiloConnection>();

        const register = (siloId: string, connection: SiloConnection): void => {
            const existing = links.get(siloId);
            if (existing && existing !== connection && !existing.closed) {
                // Same rule as the TCP transport: the lexicographically
                // smaller siloId is the designated dialer and its outbound
                // connection wins. Both ends compute it from ids they have
                // already exchanged, so exactly one survives with no extra
                // round trip.
                const weDial = config.siloId < siloId;
                const keepExisting = weDial ? outbound.has(existing) : !outbound.has(existing);
                if (keepExisting) {
                    connection.close('duplicate');
                    return;
                }
                existing.close('superseded by the designated dialer');
            }
            links.set(siloId, connection);
        };

        const adopt = (
            socket: MinimalWebSocket,
            dialer: boolean,
            expectedSiloId?: string
        ): SiloConnection => {
            const connection: SiloConnection = new SiloConnection({
                config,
                link: webSocketLink(socket),
                dialer,
                maxFrameBytes,
                credit,
                runtime: () => runtime,
                onPeer: (siloId) => {
                    pending.delete(connection);
                    if (expectedSiloId && siloId !== expectedSiloId) {
                        connection.close(
                            `expected ${expectedSiloId} at this address, got ${siloId}`
                        );
                        return;
                    }
                    register(siloId, connection);
                },
                onClose: () => {
                    pending.delete(connection);
                    const id = connection.peerSiloId;
                    if (id && links.get(id) === connection) links.delete(id);
                }
            });
            if (dialer) outbound.add(connection);
            return connection;
        };

        const hello = (connection: SiloConnection, type: number): void => {
            connection.send({
                type,
                flags: 0,
                status: 0,
                corrId: 0,
                payload: { siloId: config.siloId, epoch: config.epoch, proto: 1 }
            });
        };

        const openSocket = (url: string): MinimalWebSocket => {
            if (options.connect) return options.connect(url);
            const Ctor = (globalThis as { WebSocket?: new (url: string) => MinimalWebSocket })
                .WebSocket;
            if (!Ctor) {
                throw new Error(
                    `[sigx actors] no global WebSocket — pass wsTransport({ connect }) with ` +
                        `an implementation (e.g. from "ws") on this runtime.`
                );
            }
            return new Ctor(url);
        };

        const dial = async (target: SiloDescriptor): Promise<SiloConnection> => {
            const url = target.addresses?.[WS_TRANSPORT_NAME];
            if (!url) throw new Error(`[sigx actors] ${target.siloId} advertises no ws address`);
            let socket: MinimalWebSocket;
            try {
                socket = openSocket(url);
            } catch (cause) {
                throw new ActorUnreachableError(`${target.siloId} (${url})`, { cause });
            }
            const connection = adopt(socket, true, target.siloId);
            try {
                await new Promise<void>((resolve, reject) => {
                    socket.addEventListener('open', () => resolve(), { once: true });
                    socket.addEventListener('error', () => reject(new Error('ws connect failed')), {
                        once: true
                    });
                    socket.addEventListener('close', () => reject(new Error('ws closed')), {
                        once: true
                    });
                });
            } catch (cause) {
                // Must be UNREACHABLE, not a raw socket error: the placement
                // classifies on the actor error kind, and anything it cannot
                // classify is a hard failure where a retry would converge.
                connection.close('connect failed');
                throw new ActorUnreachableError(`${target.siloId} (${url})`, { cause });
            }
            hello(connection, FrameType.HELLO);
            links.set(target.siloId, connection);
            return connection;
        };

        const linkTo = (target: SiloDescriptor): Promise<SiloConnection> => {
            const existing = links.get(target.siloId);
            if (existing && !existing.closed) return Promise.resolve(existing);
            const inFlight = dialing.get(target.siloId);
            if (inFlight) return inFlight;
            const promise = dial(target).finally(() => dialing.delete(target.siloId));
            dialing.set(target.siloId, promise);
            return promise;
        };

        const dispatcherFor = (target: SiloDescriptor): ActorDispatcher | null => {
            // A routing answer, not a failure — the next transport in the
            // chain gets its turn. That is what makes a rolling deploy work.
            if (!target.addresses?.[WS_TRANSPORT_NAME]) return null;

            const prepare = async (
                symbol: string,
                payloadArgs: readonly unknown[],
                call: ActorCallContext
            ): Promise<{
                connection: SiloConnection;
                symbol: string;
                payload: unknown;
                envelope: string;
                auth: string | undefined;
            }> => {
                const connection = await linkTo(target);
                return {
                    connection,
                    symbol,
                    payload: config.codec.encode(payloadArgs),
                    envelope: encodeEnvelope(call, config.siloId),
                    auth:
                        config.secret === undefined
                            ? undefined
                            : await signAuth(config.secret, symbol, call.callId)
                };
            };

            /** Shared by both streamed modes — they differ only in the symbol. */
            const streamOver = (
                symbol: string,
                payloadArgs: readonly unknown[],
                call: ActorCallContext
            ): AsyncIterable<unknown> => {
                const codec = config.codec;
                async function* run(): AsyncGenerator<unknown> {
                    const p = await prepare(symbol, payloadArgs, call);
                    for await (const chunk of p.connection.dispatchStream(
                        p.symbol,
                        p.payload,
                        p.envelope,
                        p.auth,
                        call
                    )) {
                        yield codec.decode(chunk);
                    }
                }
                return run();
            };

            return {
                async dispatch(
                    ref: ActorRef,
                    method: string,
                    args: readonly unknown[],
                    call: ActorCallContext
                ) {
                    const p = await prepare(`${ref.type}#${method}`, [ref.key, ...args], call);
                    const value = await p.connection.dispatch(
                        p.symbol,
                        p.payload,
                        p.envelope,
                        p.auth,
                        call
                    );
                    return config.codec.decode(value);
                },
                dispatchStream(
                    ref: ActorRef,
                    method: string,
                    args: readonly unknown[],
                    call: ActorCallContext
                ) {
                    return streamOver(`${ref.type}#${method}`, [ref.key, ...args], call);
                },
                dispatchWatch(
                    ref: ActorRef,
                    method: string,
                    args: readonly unknown[],
                    call: ActorCallContext,
                    options?: { throttleMs?: number }
                ) {
                    // FLAG_STREAM still goes on the CALL — the reply IS a
                    // chunk stream — so credit, CANCEL and the rest apply
                    // unchanged. The symbol is what says this is a watch.
                    return streamOver(
                        watchSymbol(ref.type, method),
                        [ref.key, options ?? null, ...args],
                        call
                    );
                }
            };
        };

        const transport: WsSiloTransport = {
            name: WS_TRANSPORT_NAME,
            __path: path,
            __accept(socket: MinimalWebSocket): void {
                const connection = adopt(socket, false);
                pending.add(connection);
                hello(connection, FrameType.WELCOME);
            },

            async start(rt: SiloTransportRuntime): Promise<string> {
                runtime = rt;
                if (keepAliveMs > 0) {
                    keepAlive = setInterval(() => {
                        for (const connection of links.values()) {
                            connection.send({ type: FrameType.PING, flags: 0, status: 0, corrId: 0 });
                        }
                    }, keepAliveMs);
                    keepAlive.unref?.();
                }
                // Resolved here, before the membership join, so nothing is
                // advertised until the listener behind it is already up.
                return await options.advertiseUrl();
            },

            dispatcherFor,

            onMembership(view: MembershipView): void {
                // Silo ids are minted per START and never reused, so a link to
                // a departed peer can never become useful again.
                const live = new Set(view.silos.map((s) => s.siloId));
                for (const [siloId, connection] of [...links]) {
                    if (!live.has(siloId)) {
                        connection.close('peer left the cluster');
                        links.delete(siloId);
                    }
                }
            },

            async stop(): Promise<void> {
                if (keepAlive) clearInterval(keepAlive);
                keepAlive = null;
                for (const connection of [...links.values()]) connection.close('silo stopping');
                links.clear();
                for (const connection of [...pending]) connection.close('silo stopping');
                pending.clear();
                runtime = null;
            },

            openLinks(): number {
                let count = 0;
                for (const connection of links.values()) if (!connection.closed) count++;
                return count;
            }
        };
        built = transport;
        announce?.(transport);
        return transport;
    };

    factory.attach = async (
        server: HttpServer,
        attachOptions?: { wss?: WebSocketServerLike }
    ): Promise<() => void> =>
        attachSiloUpgrade(server, {
            transport: built ?? (await ready),
            ...(attachOptions?.wss ? { wss: attachOptions.wss } : {})
        });

    return factory as WsTransportHandle;
}

export interface UpgradeOptions {
    /** The transport instance built by `wsTransport()(config)`. */
    transport: SiloTransport;
    /**
     * `ws`'s `WebSocketServer`, in `noServer` mode. Optional: if omitted, one
     * is created via a dynamic `import('ws')`.
     */
    wss?: WebSocketServerLike;
}

/** The slice of `ws`'s `WebSocketServer` this needs. */
export interface WebSocketServerLike {
    handleUpgrade(
        request: IncomingMessage,
        socket: Duplex,
        head: Buffer,
        callback: (ws: MinimalWebSocket) => void
    ): void;
}

/**
 * Attach the silo upgrade handler to an existing HTTP server.
 *
 * The package does not own the server on purpose: `ActorRoute.handle` returns
 * a `Response` and cannot express a Node upgrade — that needs the raw socket —
 * so this is the one piece the plugin's route seam cannot carry. Everything
 * else about this transport goes through the ordinary seam.
 *
 * Returns a detach function.
 */
export async function attachSiloUpgrade(
    server: HttpServer,
    options: UpgradeOptions
): Promise<() => void> {
    const transport = options.transport as WsSiloTransport;
    const path = transport.__path ?? DEFAULT_WS_PATH;
    let wss = options.wss;
    if (!wss) {
        const ws = (await import('ws')) as unknown as {
            WebSocketServer: new (o: { noServer: true }) => WebSocketServerLike;
        };
        wss = new ws.WebSocketServer({ noServer: true });
    }
    const onUpgrade = (request: IncomingMessage, socket: Duplex, head: Buffer): void => {
        const url = request.url ?? '';
        if (!url.startsWith(path)) return; // someone else's upgrade
        wss.handleUpgrade(request, socket, head, (client) => {
            transport.__accept(client);
        });
    };
    server.on('upgrade', onUpgrade);
    return () => server.off('upgrade', onUpgrade);
}
