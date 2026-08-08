/**
 * `socketTransport()` under the shared client-transport conformance suite —
 * the same cases `fetchTransport` (the incumbent) passes, driven over an
 * in-memory link into the real `createActorSocketSession`. Until the live
 * layer ships, the live case reports a SKIP — visibly, never silently.
 */
import { describe, expect, it } from 'vitest';
import { createHost } from '@sigx/actors/host';
import { createActorSocketSession, type ActorSocketSession } from '@sigx/actors/server';
import {
    socketTransportConformance,
    type SocketTransportFactory
} from '@sigx/actors/testing';
import { socketTransport } from '@sigx/actors-ws/client';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const create: SocketTransportFactory = async (actors) => {
    const host = createHost({
        actors: actors as Parameters<typeof createHost>[0]['actors'],
        defaults: quiet
    });
    await host.start();
    const sessions: ActorSocketSession[] = [];
    const transport = socketTransport({
        retryMs: 1,
        maxRetryMs: 5,
        connect: (handlers) => {
            let session: ActorSocketSession | null = null;
            const outbound: string[] = [];
            void createActorSocketSession({
                host,
                request: new Request('http://actors.test/socket'),
                origin: false,
                pingMs: 0,
                send: (message) => handlers.onMessage(message),
                close: () => handlers.onClose()
            }).then((created) => {
                session = created;
                sessions.push(created);
                handlers.onOpen();
                for (const message of outbound.splice(0)) created.handle(message);
            });
            return {
                send: (message) => {
                    if (session) session.handle(message);
                    else outbound.push(message);
                },
                close: () => session?.close()
            };
        }
    });
    return {
        transport,
        host,
        stop: async () => {
            void transport.close?.();
            for (const session of sessions) session.close();
            await host.stop();
        }
    };
};

describe('socketTransportConformance × socketTransport', () => {
    for (const conformanceCase of socketTransportConformance) {
        it(`${conformanceCase.name} — ${conformanceCase.why}`, async () => {
            const outcome = await conformanceCase.run(create);
            if (outcome && 'skipped' in outcome) {
                // Live lands in a later #99 PR; until then only that case skips.
                expect(conformanceCase.name).toMatch(/^live:/);
            }
        });
    }
});
