/**
 * The client-transport conformance suite against `fetchTransport()` — the
 * INCUMBENT. It shipped first and its behaviour is the contract, so this
 * runner existing (and passing) is what proves the suite describes the real
 * contract rather than a newcomer's habits. `fetchTransport` has no live
 * channel, so the live case reports a SKIP — visibly, never silently.
 */
import { describe, expect, it } from 'vitest';
import { createHost } from '@sigx/actors/host';
import { handleActorRequest } from '@sigx/actors/server';
import { fetchTransport } from '@sigx/actors/client';
import {
    socketTransportConformance,
    type SocketTransportFactory
} from '@sigx/actors/testing';

const ENDPOINT = 'http://actors.test/_sigx/actor';
const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const create: SocketTransportFactory = async (actors) => {
    const host = createHost({
        actors: actors as Parameters<typeof createHost>[0]['actors'],
        defaults: quiet
    });
    await host.start();
    const transport = fetchTransport({
        endpoint: ENDPOINT,
        // In-process fetch shim. Real `fetch` REJECTS when its signal aborts;
        // `handleActorRequest` just returns a Response whenever the dispatch
        // ends, so the abort contract has to be reproduced here or the abort
        // case would measure the shim, not the transport.
        fetch: (url, init) => {
            const signal = (init as RequestInit | undefined)?.signal;
            const responding = handleActorRequest(new Request(url, init), {
                host,
                origin: false
            });
            if (!signal) return responding;
            return Promise.race([
                responding,
                new Promise<never>((_, reject) => {
                    const fail = (): void =>
                        reject(
                            Object.assign(new Error('The operation was aborted.'), {
                                name: 'AbortError'
                            })
                        );
                    if (signal.aborted) fail();
                    else signal.addEventListener('abort', fail, { once: true });
                })
            ]);
        }
    });
    return {
        transport,
        host,
        stop: async () => {
            await host.stop();
        }
    };
};

describe('socketTransportConformance × fetchTransport (the incumbent)', () => {
    for (const conformanceCase of socketTransportConformance) {
        it(`${conformanceCase.name} — ${conformanceCase.why}`, async () => {
            const outcome = await conformanceCase.run(create);
            if (outcome && 'skipped' in outcome) {
                // fetchTransport legitimately skips exactly the live case.
                expect(conformanceCase.name).toMatch(/^live:/);
                expect(outcome.skipped).toBeTruthy();
            }
        });
    }
});
