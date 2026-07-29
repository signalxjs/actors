/**
 * `siloEndpointRuntime` — the internal endpoint WITHOUT a cluster.
 *
 * The mount was only ever reachable through `siloRuntime(silo, placement)`,
 * which needs a `ClusterPlacement` for `descriptor()`/`view()`. The endpoint
 * itself never calls either — they belong to the sending side, which has to
 * pick a peer. A host that holds one actor and has no peers (a Cloudflare
 * Durable Object) needs the inbound half and has no honest answer for the
 * outbound one, so the two are now separate contracts.
 */
import { describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import { createSilo, manualScheduler, memoryStorage } from '@sigx/actors/silo';
import {
    encodeEnvelope,
    handleSiloRequestForRuntime,
    matchesSiloRequest,
    SILO_CALL_HEADER,
    siloEndpointRuntime
} from '@sigx/actors/cluster';

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        }
    }),
    streams: (ctx) => ({
        async *watch() {
            yield* ctx.changes({ initial: true });
        }
    })
});

const BASE = '/_sigx/silo';

function call(symbol: string, args: readonly unknown[]): Request {
    return new Request(`https://host.invalid${BASE}/${encodeURIComponent(symbol)}`, {
        method: 'POST',
        headers: {
            'content-type': 'application/json',
            [SILO_CALL_HEADER]: encodeEnvelope({ callChain: [], callId: 'c1.test.0' }, 'origin')
        },
        body: JSON.stringify({ args })
    });
}

async function withSilo<T>(fn: (silo: ReturnType<typeof createSilo>) => Promise<T>): Promise<T> {
    const silo = createSilo({
        actors: [Counter],
        storage: memoryStorage(),
        scheduler: manualScheduler(),
        // The Durable Object shape: no idle collection, no cluster.
        defaults: { sweepIntervalMs: 0, callTimeoutMs: 0 }
    });
    await silo.start();
    try {
        return await fn(silo);
    } finally {
        await silo.stop({ timeoutMs: 1_000 });
    }
}

describe('siloEndpointRuntime', () => {
    it('serves a unary call over the internal mount with no placement at all', async () => {
        await withSilo(async (silo) => {
            const runtime = siloEndpointRuntime(silo);
            expect(matchesSiloRequest(call('Counter#increment', ['a', 2]), BASE)).toBe(true);

            const res = await handleSiloRequestForRuntime(call('Counter#increment', ['a', 2]), {
                runtime
            });
            expect(res.status).toBe(200);
            expect((await res.json()).data).toBe(2);

            // Same actor, same silo: the second call sees the first's state.
            const again = await handleSiloRequestForRuntime(
                call('Counter#increment', ['a', 3]),
                { runtime }
            );
            expect((await again.json()).data).toBe(5);
        });
    });

    it('streams NDJSON, so a stream method is reachable too', async () => {
        await withSilo(async (silo) => {
            const res = await handleSiloRequestForRuntime(call('Counter#watch', ['a']), {
                runtime: siloEndpointRuntime(silo)
            });
            expect(res.status).toBe(200);
            expect(res.body).toBeTruthy();

            // Read until a newline rather than assuming the first chunk
            // carries a whole line — stream chunk boundaries are not
            // guaranteed to align with them.
            const reader = res.body!.getReader();
            const decoder = new TextDecoder();
            let buffer = '';
            while (!buffer.includes('\n')) {
                const { value, done } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });
            }
            const line = buffer.slice(0, buffer.indexOf('\n'));
            // The initial snapshot of a fresh actor.
            expect(JSON.parse(line).chunk).toEqual({ count: 0 });
            await reader.cancel();
        });
    });

    it('404s an unknown type, keeping the endpoint the 404 authority', async () => {
        await withSilo(async (silo) => {
            const res = await handleSiloRequestForRuntime(call('Nope#whatever', ['a']), {
                runtime: siloEndpointRuntime(silo)
            });
            expect(res.status).toBe(404);
        });
    });

    it('refuses the cluster stats symbol rather than fabricating a report', async () => {
        // `SiloReport` is a cluster identity — siloId, epoch, address, owned
        // reminder shards. A host with no cluster has no honest answer, and
        // inventing one would put fiction on the ops channel.
        await withSilo(async (silo) => {
            const runtime = siloEndpointRuntime(silo);
            expect(await runtime.resolve('$sigx:silo#stats')).toBeNull();

            const res = await handleSiloRequestForRuntime(call('$sigx:silo#stats', []), {
                runtime
            });
            expect(res.status).toBe(404);
        });
    });

    it('declares no noteAuthFailure — with no secret, nothing is ever refused', () => {
        // It is optional on the inbound contract precisely so this case does
        // not have to supply a no-op that looks like a counter.
        const runtime = siloEndpointRuntime({} as never);
        expect(runtime.noteAuthFailure).toBeUndefined();
    });
});
