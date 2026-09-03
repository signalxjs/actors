/**
 * The transport conformance suite, run over real TCP.
 *
 * This is the whole point of the suite existing before this package did: the
 * cases were written against `httpTransport()` in #94, so they describe the
 * contract rather than this transport's habits. Everything here either passes
 * unchanged or names a real difference.
 *
 * Unlike HTTP, this transport HOLDS CONNECTIONS — so `openLinks` is
 * implemented and the two link-hygiene cases that skip for HTTP actually run
 * here. Those are the cases only a connection-oriented transport can fail,
 * which is exactly why they live in the shared suite.
 */
import { describe, expect, it } from 'vitest';
import { createServer, type Server } from 'node:http';
import {
    transportConformance,
    type ConformanceClusterOptions,
    type TransportConformanceFactory,
    type TransportConformanceHarness
} from '@sigx/actors/cluster/testing';
import { defineActorApp, memoryStorage, type ActorApp } from '@sigx/actors/host';
import { createAppHandler } from '@sigx/actors/node';
import {
    cluster,
    memoryClusterHub,
    type ClusterPlacement,
    type HostDescriptor
} from '@sigx/actors/cluster';
import type { Host } from '@sigx/actors';
import { tcpTransport, TCP_TRANSPORT_NAME } from '../src/index.ts';

/** Deterministic: every host activates its own new actors. */
const selfPolicy = { name: 'tcp-conformance-self', choose: (_r: unknown, _v: unknown, self: HostDescriptor) => self };

interface Member {
    app: ActorApp;
    host: Host;
    placement: ClusterPlacement;
    server: Server;
    transport: { openLinks?(): number; stop?(): void | Promise<void> };
}

/**
 * A real N-host cluster: each host gets its own HTTP listener (for the public
 * wire and so `unbind`/`crash` have something to sever) plus a TCP listener
 * that carries every host-to-host call.
 */
const createTcpCluster: TransportConformanceFactory = async (
    options: ConformanceClusterOptions
): Promise<TransportConformanceHarness> => {
    const hub = memoryClusterHub();
    const storage = memoryStorage();
    const members: Member[] = [];
    const secret = options.secret === null ? undefined : (options.secret ?? 'tcp-conformance');

    for (let i = 0; i < options.hosts; i++) {
        // Bind the HTTP listener first so `advertise` is a real address, the
        // same ordering the seam requires of the TCP listener itself.
        const server = createServer();
        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        const httpPort = (server.address() as { port: number }).port;

        const captured: { openLinks?(): number; stop?(): void | Promise<void> } = {};
        const plugin = cluster({
            providers: hub.providers(),
            advertise: `http://127.0.0.1:${httpPort}`,
            policy: (options.policy ?? selfPolicy) as never,
            // TCP ONLY — no HTTP fallback in the chain, so every cross-host
            // call in the suite is genuinely carried by this package.
            transport: (config) => {
                const t = tcpTransport({ host: '127.0.0.1', keepAliveMs: 0 })(config);
                captured.openLinks = (t as { openLinks?(): number }).openLinks;
                // Captured so `unbind` can sever the WIRE without touching
                // membership — stopping the whole app would also leave the
                // cluster, and the caller would then legitimately re-place
                // the actor instead of seeing it as unreachable.
                captured.stop = () => t.stop?.();
                return t;
            },
            ...(secret === undefined ? {} : { secret }),
            ...(options.retryBackoffMs !== undefined
                ? { retryBackoffMs: options.retryBackoffMs }
                : {})
        });
        const app = defineActorApp({
            actors: options.actorsFor?.(i) ?? options.actors,
            storage,
            defaults: { sweepIntervalMs: 3_600_000, reminderTickMs: 3_600_000, callTimeoutMs: 0 }
        }).use(plugin);
        server.on('request', createAppHandler(app));
        const host = await app.start();
        members.push({ app, host, placement: plugin.placement, server, transport: captured });
    }

    const severed = new Set<number>();

    return {
        placements: members.map((m) => m.placement),
        hosts: members.map((m) => m.host),
        /** Wire-level unreachable while membership still lists the host. */
        unbind: (i) => {
            // Wire-level unreachable while membership STILL lists the host.
            // Only the transport is torn down; the placement keeps its
            // heartbeat, so peers must classify this as `unreachable` rather
            // than quietly re-placing the actor somewhere else.
            void members[i]!.transport.stop?.();
            members[i]!.server.closeAllConnections?.();
            members[i]!.server.close();
        },
        crash: (i) => {
            // Membership drops it AND the listeners die. No cleanup runs.
            severed.add(i);
            hub.kill(members[i]!.placement.identity.hostId);
            void members[i]!.transport.stop?.();
            members[i]!.server.closeAllConnections?.();
            members[i]!.server.close();
        },
        /** Membership only — every socket stays up, so the reaping case can
         *  isolate the membership path from a socket simply closing. */
        dropMembership: (i) => {
            hub.kill(members[i]!.placement.identity.hostId);
        },
        impostor: async (target: HostDescriptor) => {
            // Speak the real frame protocol with the WRONG secret.
            const { connect } = await import('node:net');
            const { encodeFrame, FrameType, FrameReader } = await import(
                '@sigx/actors/cluster/frames'
            );
            const { encodeEnvelope, signAuth } = await import('@sigx/actors/cluster');
            const address = target.addresses?.[TCP_TRANSPORT_NAME];
            if (!address) return { ok: false, status: 0 };
            // Bracketed IPv6 too — the transport emits it, so the harness
            // must accept it or this case silently stops covering v6.
            const [, host, port] = /^tcp:\/\/(\[[^\]]+\]|[^:]+):(\d+)$/.exec(
                address
            ) as RegExpExecArray;
            const dialHost = (host as string).startsWith('[')
                ? (host as string).slice(1, -1)
                : (host as string);
            const socket = connect({ host: dialHost, port: Number(port) });
            await new Promise<void>((resolve, reject) => {
                socket.once('connect', resolve);
                socket.once('error', reject);
            });
            const symbol = 'ConformanceEcho#increment';
            const callId = 'c1.forged';
            socket.write(
                encodeFrame({
                    type: FrameType.CALL,
                    flags: 0,
                    status: 0,
                    corrId: 2,
                    payload: {
                        s: symbol,
                        e: encodeEnvelope({ callChain: [], callId }, 's.attacker'),
                        a: ['forged', 1],
                        h: await signAuth('wrong-secret', symbol, callId)
                    }
                })
            );
            const reader = new FrameReader(1 << 20, (_k, v) => v);
            const status = await new Promise<number>((resolve) => {
                const timer = setTimeout(() => resolve(0), 2000);
                socket.on('data', (chunk: Buffer) => {
                    reader.push(new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength));
                    for (const frame of reader.drain()) {
                        if (frame.type === FrameType.ERROR || frame.type === FrameType.GOAWAY) {
                            clearTimeout(timer);
                            resolve(frame.status);
                            return;
                        }
                    }
                });
            });
            socket.destroy();
            return { ok: false, status };
        },
        openLinks: (i) => members[i]!.transport.openLinks?.() ?? 0,
        stop: async () => {
            await Promise.allSettled(
                members.map(async (m, i) => {
                    if (!severed.has(i)) await m.app.stop({ timeoutMs: 1000 });
                    m.server.closeAllConnections?.();
                    await new Promise<void>((resolve) => m.server.close(() => resolve()));
                })
            );
        }
    };
};

describe('transport conformance: tcpTransport()', () => {
    for (const testCase of transportConformance) {
        it(testCase.name, async (ctx) => {
            const outcome = await testCase.run(createTcpCluster);
            if (outcome && 'skipped' in outcome) {
                ctx.skip(outcome.skipped);
            }
            expect(outcome).toBeUndefined();
        }, testCase.timeoutMs);
    }
});
