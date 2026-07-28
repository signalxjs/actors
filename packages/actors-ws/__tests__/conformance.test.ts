/**
 * The transport conformance suite, run over real WebSockets.
 *
 * Identical cases to the TCP run — which is the point. Both transports speak
 * the same frames over the same connection state machine, so the suite should
 * not be able to tell them apart, and any case that behaves differently is a
 * real difference rather than a stylistic one.
 *
 * Like TCP and unlike HTTP, this transport HOLDS CONNECTIONS, so `openLinks`
 * is implemented and the two link-hygiene cases actually run.
 */
import { describe, expect, it } from 'vitest';
import { createServer, type Server } from 'node:http';
import {
    transportConformance,
    type ConformanceClusterOptions,
    type TransportConformanceFactory,
    type TransportConformanceHarness
} from '@sigx/actors/cluster/testing';
import { defineActorApp, memoryStorage, type ActorApp } from '@sigx/actors/silo';
import { createAppHandler } from '@sigx/actors/node';
import {
    cluster,
    memoryClusterHub,
    type ClusterPlacement,
    type SiloDescriptor
} from '@sigx/actors/cluster';
import type { Silo } from '@sigx/actors';
import { WebSocketServer } from 'ws';
import { wsTransport, WS_TRANSPORT_NAME } from '../src/index.ts';

/** Deterministic: every silo activates its own new actors. */
const selfPolicy = { name: 'ws-conformance-self', choose: (_r: unknown, _v: unknown, self: SiloDescriptor) => self };

interface Member {
    app: ActorApp;
    silo: Silo;
    placement: ClusterPlacement;
    server: Server;
    detach?: () => void;
    transport: { openLinks?(): number; stop?(): void | Promise<void> };
}

/**
 * A real N-silo cluster: one HTTP listener per silo, carrying both the public
 * wire and — through an `upgrade` handler — every silo-to-silo call. One port,
 * which is the whole reason to pick this transport over raw TCP.
 */
const createWsCluster: TransportConformanceFactory = async (
    options: ConformanceClusterOptions
): Promise<TransportConformanceHarness> => {
    const hub = memoryClusterHub();
    const storage = memoryStorage();
    const members: Member[] = [];
    const secret = options.secret === null ? undefined : (options.secret ?? 'ws-conformance');

    for (let i = 0; i < options.silos; i++) {
        // Bind the HTTP listener first so `advertise` is a real address, the
        // same ordering the seam requires of the TCP listener itself.
        const server = createServer();
        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        const httpPort = (server.address() as { port: number }).port;

        const captured: { openLinks?(): number; stop?(): void | Promise<void> } = {};
        const handle = wsTransport({
            keepAliveMs: 0,
            advertiseUrl: () => `ws://127.0.0.1:${httpPort}/_sigx/silo-ws`,
            // `ws`'s client, not the global: it is the peer dep anyway, and
            // this keeps the test off whatever the runtime happens to ship.
            connect: (url) => new WebSocket(url) as never
        });
        const plugin = cluster({
            providers: hub.providers(),
            advertise: `http://127.0.0.1:${httpPort}`,
            policy: (options.policy ?? selfPolicy) as never,
            // WEBSOCKET ONLY — no HTTP fallback in the chain, so every
            // cross-silo call in the suite is genuinely carried by this
            // package rather than quietly falling back.
            transport: (config) => {
                const t = handle(config);
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
            actors: options.actors,
            storage,
            defaults: { sweepIntervalMs: 3_600_000, reminderTickMs: 3_600_000, callTimeoutMs: 0 }
        }).use(plugin);
        server.on('request', createAppHandler(app));
        // The upgrade handler is the one piece a contributed route cannot
        // express, since `ActorRoute.handle` returns a `Response`.
        // The ergonomic path users actually take: `attach` resolves the
        // instance `cluster()` built, so nothing has to be threaded by hand.
        const detach = await handle.attach(server, {
            wss: new WebSocketServer({ noServer: true }) as never
        });
        const silo = await app.start();
        members.push({ app, silo, placement: plugin.placement, server, detach, transport: captured });
    }

    const severed = new Set<number>();

    return {
        placements: members.map((m) => m.placement),
        silos: members.map((m) => m.silo),
        /** Wire-level unreachable while membership still lists the silo. */
        unbind: (i) => {
            members[i]!.detach?.();
            // Wire-level unreachable while membership STILL lists the silo.
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
            members[i]!.detach?.();
            hub.kill(members[i]!.placement.identity.siloId);
            void members[i]!.transport.stop?.();
            members[i]!.server.closeAllConnections?.();
            members[i]!.server.close();
        },
        /** Membership only — every socket stays up, so the reaping case can
         *  isolate the membership path from a socket simply closing. */
        dropMembership: (i) => {
            hub.kill(members[i]!.placement.identity.siloId);
        },
        impostor: async (target: SiloDescriptor) => {
            // Speak the real frame protocol with the WRONG secret.
            const { encodeFrameBody, decodeFrameBody, FrameType } = await import(
                '@sigx/actors/cluster/frames'
            );
            const { encodeEnvelope, signAuth } = await import('@sigx/actors/cluster');
            const url = target.addresses?.[WS_TRANSPORT_NAME];
            if (!url) return { ok: false, status: 0 };
            const socket = new WebSocket(url);
            socket.binaryType = 'arraybuffer';
            await new Promise<void>((resolve, reject) => {
                socket.addEventListener('open', () => resolve(), { once: true });
                socket.addEventListener('error', () => reject(new Error('connect failed')), {
                    once: true
                });
            });
            const symbol = 'ConformanceEcho#increment';
            const callId = 'c1.forged';
            const forged = encodeFrameBody({
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
            });
            // A detached ArrayBuffer copy: the WHATWG send() signature does not
            // accept a view over a SharedArrayBuffer-capable buffer.
            socket.send(forged.slice().buffer as ArrayBuffer);
            const status = await new Promise<number>((resolve) => {
                const timer = setTimeout(() => resolve(0), 2000);
                socket.addEventListener('message', (event) => {
                    const bytes = new Uint8Array(event.data as ArrayBuffer);
                    const frame = decodeFrameBody(bytes, (_k, v) => v);
                    if (frame.type === FrameType.ERROR || frame.type === FrameType.GOAWAY) {
                        clearTimeout(timer);
                        resolve(frame.status);
                    }
                });
            });
            socket.close();
            return { ok: false, status };
        },
        openLinks: (i) => members[i]!.transport.openLinks?.() ?? 0,
        stop: async () => {
            await Promise.allSettled(
                members.map(async (m, i) => {
                    m.detach?.();
                    if (!severed.has(i)) await m.app.stop({ timeoutMs: 1000 });
                    m.server.closeAllConnections?.();
                    await new Promise<void>((resolve) => m.server.close(() => resolve()));
                })
            );
        }
    };
};

describe('transport conformance: wsTransport()', () => {
    for (const testCase of transportConformance) {
        it(testCase.name, async (ctx) => {
            const outcome = await testCase.run(createWsCluster);
            if (outcome && 'skipped' in outcome) {
                ctx.skip(outcome.skipped);
            }
            expect(outcome).toBeUndefined();
        });
    }
});
