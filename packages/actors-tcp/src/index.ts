/**
 * `@sigx/actors-tcp` — an Orleans-style TCP transport for `@sigx/actors`.
 *
 * Orleans silos talk over persistent TCP with binary framing rather than
 * HTTP, and this is that shape for this runtime: one multiplexed connection
 * per peer instead of one per in-flight request.
 *
 * **Why this exists, stated honestly.** It is not about latency. The
 * measurements in `benchmarks/BASELINES.md` put per-call HMAC at 1.19× over a
 * real socket (not the 3.35× the in-process figures suggested), and show
 * throughput plateauing at Node's HTTP stack rather than at the runtime. What
 * this buys is **socket count**: HTTP's pool sizes to `concurrency × peers`,
 * which is file descriptors, kernel buffers, conntrack entries and a
 * connection burst on every peer restart. This holds one connection per peer.
 *
 * It is Node-only (`node:net`) by construction, which is exactly why it is a
 * separate package: `@sigx/actors/cluster` stays zero-dep and WinterCG-clean
 * so Cloudflare Workers keep working, and HTTP stays the default.
 *
 * ```ts
 * import { cluster, httpTransport } from '@sigx/actors/cluster';
 * import { tcpTransport } from '@sigx/actors-tcp';
 *
 * cluster({
 *     providers, advertise, secret,
 *     // A chain: TCP where the peer advertises it, HTTP everywhere else.
 *     // That is what makes the rolling deploy possible.
 *     transport: [tcpTransport({ port: 11111 }), httpTransport()]
 * });
 * ```
 */
import { createServer, connect, type Server, type Socket } from 'node:net';
import {
    encodeEnvelope,
    signAuth,
    type MembershipView,
    type SiloDescriptor,
    type SiloTransport,
    type SiloTransportConfig,
    type SiloTransportFactory,
    type SiloTransportRuntime
} from '@sigx/actors/cluster';
import { ActorUnreachableError } from '@sigx/actors';
import type { ActorCallContext, ActorDispatcher, ActorRef } from '@sigx/actors';
import { FrameType, encodeFrame } from '@sigx/actors/cluster/frames';
import {
    DEFAULT_CREDIT,
    DEFAULT_MAX_FRAME_BYTES,
    SiloConnection
} from './connection';

export { DEFAULT_CREDIT, DEFAULT_MAX_FRAME_BYTES } from './connection';

/** This transport's key in `SiloDescriptor.addresses`. */
export const TCP_TRANSPORT_NAME = 'tcp';

export interface TcpTransportOptions {
    /** Port to listen on. `0` binds an ephemeral port, which is then what
     *  gets advertised — the address is read back after binding. */
    port?: number;
    /** Interface to bind. Default: all. */
    host?: string;
    /**
     * Host peers should dial. Defaults to `host` when it names a specific
     * interface, otherwise `127.0.0.1` — which is right for a single box and
     * WRONG for any real deployment, so set it explicitly when binding all
     * interfaces. IPv6 literals are bracketed automatically.
     */
    advertiseHost?: string;
    /** Reject any frame larger than this BEFORE buffering it. */
    maxFrameBytes?: number;
    /** Stream chunks a consumer accepts before it must extend credit. */
    credit?: number;
    /** Idle ping interval, ms. 0 disables. Default 15 000. */
    keepAliveMs?: number;
}

/** Bracket an IPv6 literal, so `host:port` stays unambiguous. */
function formatHost(host: string): string {
    if (!host.includes(':') || host.startsWith('[')) return host;
    return `[${host}]`;
}

function parseTcpAddress(address: string): { host: string; port: number } | null {
    const match = /^tcp:\/\/(\[[^\]]+\]|[^:]+):(\d+)$/.exec(address);
    if (!match) return null;
    const rawHost = match[1] as string;
    return {
        host: rawHost.startsWith('[') ? rawHost.slice(1, -1) : rawHost,
        port: Number(match[2])
    };
}

export function tcpTransport(options: TcpTransportOptions = {}): SiloTransportFactory {
    return (config: SiloTransportConfig): SiloTransport => {
        const maxFrameBytes = options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;
        const credit = options.credit ?? DEFAULT_CREDIT;
        const keepAliveMs = options.keepAliveMs ?? 15_000;

        let runtime: SiloTransportRuntime | null = null;
        let server: Server | null = null;
        let keepAlive: ReturnType<typeof setInterval> | null = null;
        /** siloId → the connection we use for it. */
        const links = new Map<string, SiloConnection>();
        /** siloId → in-flight dial, so N concurrent calls share one connect. */
        const dialing = new Map<string, Promise<SiloConnection>>();
        /** Connections accepted before their HELLO named them. */
        const pendingInbound = new Set<SiloConnection>();

        const register = (siloId: string, connection: SiloConnection): void => {
            const existing = links.get(siloId);
            if (existing && existing !== connection && !existing.closed) {
                // Simultaneous dial: both sides opened at once. Orleans
                // settles this without an extra round trip, and so do we —
                // the LEXICOGRAPHICALLY SMALLER siloId is the designated
                // dialer and its outbound connection wins. Both ends compute
                // the same answer from ids they already exchanged, so
                // exactly one connection survives.
                const weDial = config.siloId < siloId;
                const keepExisting = weDial
                    ? existing.peerSiloId === siloId && isOutbound(existing)
                    : !isOutbound(existing);
                if (keepExisting) {
                    connection.send({
                        type: FrameType.GOAWAY,
                        flags: 0,
                        status: 409,
                        corrId: 0,
                        payload: { m: 'duplicate connection' }
                    });
                    connection.close('duplicate');
                    return;
                }
                existing.close('superseded by the designated dialer');
            }
            links.set(siloId, connection);
        };

        const outbound = new WeakSet<SiloConnection>();
        const isOutbound = (connection: SiloConnection): boolean => outbound.has(connection);

        const hello = (connection: SiloConnection, type: number): void => {
            connection.send({
                type,
                flags: 0,
                status: 0,
                corrId: 0,
                payload: { siloId: config.siloId, epoch: config.epoch, proto: 1 }
            });
        };

        const adopt = (socket: Socket, dialer: boolean, expectedSiloId?: string): SiloConnection => {
            const connection: SiloConnection = new SiloConnection({
                config,
                socket,
                dialer,
                maxFrameBytes,
                credit,
                runtime: () => runtime,
                onPeer: (siloId) => {
                    pendingInbound.delete(connection);
                    if (expectedSiloId && siloId !== expectedSiloId) {
                        // We dialled the address a peer advertised and a
                        // DIFFERENT silo answered — a stale directory entry
                        // or a recycled port. Refuse rather than route to it.
                        connection.close(
                            `expected ${expectedSiloId} at this address, got ${siloId}`
                        );
                        return;
                    }
                    register(siloId, connection);
                },
                onClose: () => {
                    pendingInbound.delete(connection);
                    const id = connection.peerSiloId;
                    if (id && links.get(id) === connection) links.delete(id);
                }
            });
            if (dialer) outbound.add(connection);
            return connection;
        };

        const dial = async (target: SiloDescriptor): Promise<SiloConnection> => {
            const address = target.addresses?.[TCP_TRANSPORT_NAME];
            const parsed = address ? parseTcpAddress(address) : null;
            if (!parsed) throw new Error(`[sigx actors] ${target.siloId} advertises no tcp address`);
            const socket = connect({ host: parsed.host, port: parsed.port });
            const connection = adopt(socket, true, target.siloId);
            try {
                await new Promise<void>((resolve, reject) => {
                    socket.once('connect', resolve);
                    socket.once('error', reject);
                });
            } catch (cause) {
                // A refused or timed-out connect must arrive as UNREACHABLE,
                // not as a raw ECONNREFUSED. The placement classifies on the
                // actor error kind: anything it cannot classify is a hard
                // failure where evict-refresh-retry would have converged.
                connection.close('connect failed');
                throw new ActorUnreachableError(`${target.siloId} (${address})`, { cause });
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
            // One dial per peer however many callers arrive at once — the
            // whole point is a single connection, so racing dials would
            // defeat it before the simultaneous-dial rule ever ran.
            const promise = dial(target).finally(() => dialing.delete(target.siloId));
            dialing.set(target.siloId, promise);
            return promise;
        };

        const dispatcherFor = (target: SiloDescriptor): ActorDispatcher | null => {
            // A ROUTING answer, not a failure: a peer that advertises no tcp
            // address is reached by the next transport in the chain. This is
            // what makes a rolling deploy possible.
            if (!target.addresses?.[TCP_TRANSPORT_NAME]) return null;

            const prepare = async (
                ref: ActorRef,
                method: string,
                args: readonly unknown[],
                call: ActorCallContext
            ): Promise<{
                connection: SiloConnection;
                symbol: string;
                payload: unknown;
                envelope: string;
                auth: string | undefined;
            }> => {
                const symbol = `${ref.type}#${method}`;
                const connection = await linkTo(target);
                return {
                    connection,
                    symbol,
                    payload: config.codec.encode([ref.key, ...args]),
                    envelope: encodeEnvelope(call, config.siloId),
                    auth:
                        config.secret === undefined
                            ? undefined
                            : await signAuth(config.secret, symbol, call.callId)
                };
            };

            return {
                async dispatch(ref: ActorRef, method: string, args: readonly unknown[], call: ActorCallContext) {
                    const p = await prepare(ref, method, args, call);
                    const value = await p.connection.dispatch(
                        p.symbol,
                        p.payload,
                        p.envelope,
                        p.auth,
                        call
                    );
                    return config.codec.decode(value);
                },
                dispatchStream(ref: ActorRef, method: string, args: readonly unknown[], call: ActorCallContext) {
                    const codec = config.codec;
                    async function* run(): AsyncGenerator<unknown> {
                        const p = await prepare(ref, method, args, call);
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
                }
            };
        };

        return {
            name: TCP_TRANSPORT_NAME,

            async start(rt: SiloTransportRuntime): Promise<string> {
                runtime = rt;
                const listener = createServer();
                listener.on('connection', (socket) => {
                    const connection = adopt(socket, false);
                    pendingInbound.add(connection);
                    hello(connection, FrameType.WELCOME);
                });
                server = listener;
                await new Promise<void>((resolve, reject) => {
                    listener.once('error', reject);
                    listener.listen(options.port ?? 0, options.host, () => {
                        listener.off('error', reject);
                        resolve();
                    });
                });
                const bound = listener.address() as { port: number };
                if (keepAliveMs > 0) {
                    keepAlive = setInterval(() => {
                        for (const connection of links.values()) {
                            connection.send({
                                type: FrameType.PING,
                                flags: 0,
                                status: 0,
                                corrId: 0
                            });
                        }
                    }, keepAliveMs);
                    keepAlive.unref?.();
                }
                // Returned BEFORE the membership join, so no peer can learn
                // this address until the listener above is already answering.
                const host =
                    options.advertiseHost ??
                    (options.host && options.host !== '0.0.0.0' && options.host !== '::'
                        ? options.host
                        : '127.0.0.1');
                return `tcp://${formatHost(host)}:${bound.port}`;
            },

            dispatcherFor,

            onMembership(view: MembershipView): void {
                // Silo ids are minted per START and never reused, so a peer
                // that has left can never come back under the same id and
                // its connection can never be useful again.
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
                for (const connection of [...pendingInbound]) connection.close('silo stopping');
                pendingInbound.clear();
                const listener = server;
                server = null;
                runtime = null;
                if (listener) {
                    await new Promise<void>((resolve) => listener.close(() => resolve()));
                }
            },

            /** @internal — the conformance suite's link-hygiene probe. */
            openLinks(): number {
                let count = 0;
                for (const connection of links.values()) if (!connection.closed) count++;
                return count;
            }
        } as SiloTransport & { openLinks(): number };
    };
}

/** Re-exported so a host can frame by hand if it needs to. */
export { encodeFrame, FrameType };
