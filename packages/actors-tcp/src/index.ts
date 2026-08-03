/**
 * `@sigx/actors-tcp` — a framed TCP transport for `@sigx/actors`.
 *
 * Peers talk over persistent TCP with binary framing rather than HTTP:
 * one multiplexed connection per peer instead of one per in-flight
 * request.
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
    watchSymbol,
    type MembershipView,
    type HostDescriptor,
    type HostTransport,
    type HostTransportConfig,
    type HostTransportFactory,
    type HostTransportRuntime
} from '@sigx/actors/cluster';
import { ActorUnreachableError } from '@sigx/actors';
import type { ActorCallContext, ActorDispatcher, ActorRef } from '@sigx/actors';
import {
    encodeFrame,
    FrameType,
    HostConnection,
    DEFAULT_CREDIT,
    DEFAULT_MAX_FRAME_BYTES
} from '@sigx/actors/cluster/frames';
import { socketLink } from './link';

export { DEFAULT_CREDIT, DEFAULT_MAX_FRAME_BYTES } from '@sigx/actors/cluster/frames';

/** This transport's key in `HostDescriptor.addresses`. */
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
    /**
     * How long an accepted connection may go without naming itself before it
     * is closed. `0` disables the deadline. Default 10 000.
     *
     * The listener adopts every socket and answers WELCOME, and the pending
     * set is drained only on HELLO, on close, or at `stop()`. Without a
     * deadline a peer that connects and says nothing holds a file descriptor
     * and a `maxFrameBytes`-capable read buffer for the life of the process.
     * A real handshake is one round trip on an already-open socket, so this is
     * generous by three orders of magnitude.
     */
    handshakeTimeoutMs?: number;
    /**
     * Most un-handshaken connections held at once; further ones are closed
     * immediately. `0` disables the cap. Default 256.
     *
     * The deadline above bounds how LONG each pending connection lives; this
     * bounds how MANY can accumulate inside one deadline window. A cluster
     * handshakes one connection per peer, so anything near this number is an
     * attack rather than a busy cluster.
     */
    maxPendingInbound?: number;
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

function nonNegative(value: number, name: string): number {
    if (!Number.isFinite(value) || value < 0) {
        throw new Error(
            `[sigx actors] tcpTransport({ ${name} }) must be a non-negative finite number — ` +
                `got ${String(value)}. Use 0 to disable it deliberately.`
        );
    }
    return value;
}

function nonNegativeInteger(value: number, name: string): number {
    if (!Number.isInteger(value) || value < 0) {
        throw new Error(
            `[sigx actors] tcpTransport({ ${name} }) must be a non-negative integer — ` +
                `got ${String(value)}. Use 0 to disable it deliberately.`
        );
    }
    return value;
}

export function tcpTransport(options: TcpTransportOptions = {}): HostTransportFactory {
    return (config: HostTransportConfig): HostTransport => {
        const maxFrameBytes = options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;
        const credit = options.credit ?? DEFAULT_CREDIT;
        const keepAliveMs = options.keepAliveMs ?? 15_000;
        // Validated, not merely defaulted. Both are security bounds guarded by
        // a `> 0` test, so a negative or non-finite value silently turned the
        // protection OFF — misconfiguration reaching exactly the state the
        // option exists to prevent. `0` stays the documented escape hatch.
        const handshakeTimeoutMs = nonNegative(
            options.handshakeTimeoutMs ?? 10_000,
            'handshakeTimeoutMs'
        );
        const maxPendingInbound = nonNegativeInteger(
            options.maxPendingInbound ?? 256,
            'maxPendingInbound'
        );

        let runtime: HostTransportRuntime | null = null;
        let server: Server | null = null;
        let keepAlive: ReturnType<typeof setInterval> | null = null;
        /** hostId → the connection we use for it. */
        const links = new Map<string, HostConnection>();
        /** hostId → in-flight dial, so N concurrent calls share one connect. */
        const dialing = new Map<string, Promise<HostConnection>>();
        /** Connections accepted before their HELLO named them. */
        const pendingInbound = new Set<HostConnection>();
        /**
         * Handshake deadlines, keyed by the connection they will close.
         *
         * A separate map rather than a field on the connection: the deadline
         * is this listener's policy, not part of the connection contract, and
         * `HostConnection` is shared with the dialling side, which has no
         * pending phase at all.
         */
        const handshakeTimers = new Map<HostConnection, ReturnType<typeof setTimeout>>();

        /** Called on HELLO and on close — whichever happens first. */
        const clearHandshakeTimer = (connection: HostConnection): void => {
            const timer = handshakeTimers.get(connection);
            if (timer === undefined) return;
            clearTimeout(timer);
            handshakeTimers.delete(connection);
        };

        const register = (hostId: string, connection: HostConnection): void => {
            const existing = links.get(hostId);
            if (existing && existing !== connection && !existing.closed) {
                // Simultaneous dial: both sides opened at once. Settled
                // without an extra round trip — the
                // LEXICOGRAPHICALLY SMALLER hostId is the designated
                // dialer and its outbound connection wins. Both ends compute
                // the same answer from ids they already exchanged, so
                // exactly one connection survives.
                const weDial = config.hostId < hostId;
                const keepExisting = weDial
                    ? existing.peerHostId === hostId && isOutbound(existing)
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
            links.set(hostId, connection);
        };

        const outbound = new WeakSet<HostConnection>();
        const isOutbound = (connection: HostConnection): boolean => outbound.has(connection);

        const hello = (connection: HostConnection, type: number): void => {
            connection.send({
                type,
                flags: 0,
                status: 0,
                corrId: 0,
                payload: { hostId: config.hostId, epoch: config.epoch, proto: 1 }
            });
        };

        const adopt = (socket: Socket, dialer: boolean, expectedHostId?: string): HostConnection => {
            const connection: HostConnection = new HostConnection({
                config,
                link: socketLink(socket),
                dialer,
                maxFrameBytes,
                credit,
                runtime: () => runtime,
                onPeer: (hostId) => {
                    pendingInbound.delete(connection);
                    clearHandshakeTimer(connection);
                    if (expectedHostId && hostId !== expectedHostId) {
                        // We dialled the address a peer advertised and a
                        // DIFFERENT host answered — a stale directory entry
                        // or a recycled port. Refuse rather than route to it.
                        connection.close(
                            `expected ${expectedHostId} at this address, got ${hostId}`
                        );
                        return;
                    }
                    register(hostId, connection);
                },
                onClose: () => {
                    pendingInbound.delete(connection);
                    clearHandshakeTimer(connection);
                    const id = connection.peerHostId;
                    if (id && links.get(id) === connection) links.delete(id);
                }
            });
            if (dialer) outbound.add(connection);
            return connection;
        };

        const dial = async (target: HostDescriptor): Promise<HostConnection> => {
            const address = target.addresses?.[TCP_TRANSPORT_NAME];
            const parsed = address ? parseTcpAddress(address) : null;
            if (!parsed) throw new Error(`[sigx actors] ${target.hostId} advertises no tcp address`);
            const socket = connect({ host: parsed.host, port: parsed.port });
            const connection = adopt(socket, true, target.hostId);
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
                throw new ActorUnreachableError(`${target.hostId} (${address})`, { cause });
            }
            hello(connection, FrameType.HELLO);
            links.set(target.hostId, connection);
            return connection;
        };

        const linkTo = (target: HostDescriptor): Promise<HostConnection> => {
            const existing = links.get(target.hostId);
            if (existing && !existing.closed) return Promise.resolve(existing);
            const inFlight = dialing.get(target.hostId);
            if (inFlight) return inFlight;
            // One dial per peer however many callers arrive at once — the
            // whole point is a single connection, so racing dials would
            // defeat it before the simultaneous-dial rule ever ran.
            const promise = dial(target).finally(() => dialing.delete(target.hostId));
            dialing.set(target.hostId, promise);
            return promise;
        };

        const dispatcherFor = (target: HostDescriptor): ActorDispatcher | null => {
            // A ROUTING answer, not a failure: a peer that advertises no tcp
            // address is reached by the next transport in the chain. This is
            // what makes a rolling deploy possible.
            if (!target.addresses?.[TCP_TRANSPORT_NAME]) return null;

            const prepare = async (
                symbol: string,
                payloadArgs: readonly unknown[],
                call: ActorCallContext
            ): Promise<{
                connection: HostConnection;
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
                    envelope: encodeEnvelope(call, config.hostId),
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
                async dispatch(ref: ActorRef, method: string, args: readonly unknown[], call: ActorCallContext) {
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
                dispatchStream(ref: ActorRef, method: string, args: readonly unknown[], call: ActorCallContext) {
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
                    // chunk stream — so the credit window, the CANCEL frame
                    // and every other stream mechanic apply unchanged. The
                    // symbol is the only thing that says this is a watch.
                    return streamOver(
                        watchSymbol(ref.type, method),
                        [ref.key, options ?? null, ...args],
                        call
                    );
                }
            };
        };

        return {
            name: TCP_TRANSPORT_NAME,

            async start(rt: HostTransportRuntime): Promise<string> {
                runtime = rt;
                const listener = createServer();
                listener.on('connection', (socket) => {
                    // Cap BEFORE adopting: a refused connection must not cost
                    // a HostConnection, a read buffer or a WELCOME write.
                    if (maxPendingInbound > 0 && pendingInbound.size >= maxPendingInbound) {
                        socket.destroy();
                        return;
                    }
                    const connection = adopt(socket, false);
                    pendingInbound.add(connection);
                    if (handshakeTimeoutMs > 0) {
                        const timer = setTimeout(() => {
                            handshakeTimers.delete(connection);
                            // `close()` runs onClose, which drains the pending
                            // set — so the check is "did HELLO arrive", not
                            // "is this still tracked".
                            if (!connection.peerHostId) {
                                connection.close('handshake timeout');
                            }
                        }, handshakeTimeoutMs);
                        // A pending handshake must never be the reason a
                        // process stays alive.
                        timer.unref?.();
                        handshakeTimers.set(connection, timer);
                    }
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
                // Host ids are minted per START and never reused, so a peer
                // that has left can never come back under the same id and
                // its connection can never be useful again.
                const live = new Set(view.hosts.map((s) => s.hostId));
                for (const [hostId, connection] of [...links]) {
                    if (!live.has(hostId)) {
                        connection.close('peer left the cluster');
                        links.delete(hostId);
                    }
                }
            },

            async stop(): Promise<void> {
                if (keepAlive) clearInterval(keepAlive);
                keepAlive = null;
                for (const connection of [...links.values()]) connection.close('host stopping');
                links.clear();
                for (const connection of [...pendingInbound]) connection.close('host stopping');
                pendingInbound.clear();
                // `close()` above fires onClose, which clears each timer — but
                // a connection that was already closed by other means would
                // not, so sweep the map rather than trusting the callback to
                // have run for every entry.
                for (const timer of handshakeTimers.values()) clearTimeout(timer);
                handshakeTimers.clear();
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
        } as HostTransport & { openLinks(): number };
    };
}

/** Re-exported so a host can frame by hand if it needs to. */
export { encodeFrame, FrameType };
