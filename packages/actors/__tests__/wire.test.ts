/**
 * The load-bearing integration test: the actor client proxy talks to the
 * REAL `handleServerFnRequest` (through `handleActorRequest`) — envelope,
 * rich types, error brands, 404 skew, guards, and NDJSON streaming are all
 * pinned against core's actual endpoint, not a mock.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { isServerFnError, ServerFnError } from '@sigx/server';
import { actor, defineActor } from '@sigx/actors';
import { createSilo, type Silo } from '@sigx/actors/silo';
import { handleActorRequest, matchesActorRequest } from '@sigx/actors/server';
import { __actorRef, configureActors } from '@sigx/actors/client';

const ENDPOINT = 'http://actors.test/_sigx/actor';
const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const guardLog: string[] = [];

const cart = defineActor({
    type: 'Cart',
    use: [
        (_rq, info) => {
            guardLog.push(`use:${info.symbol}`);
        }
    ],
    methodUse: {
        forbidden: [
            () => {
                throw new ServerFnError(403, 'no entry');
            }
        ]
    },
    state: () => ({ items: [] as { name: string; addedAt: Date }[] }),
    methods: (ctx) => ({
        async add(name: string, addedAt: Date) {
            ctx.state.items.push({ name, addedAt });
            await ctx.save();
            return { count: ctx.state.items.length, tags: new Map([[name, addedAt]]) };
        },
        async boom() {
            throw new Error('internal detail that must be masked');
        },
        async forbidden() {
            return 'leaked';
        }
    }),
    streams: (ctx) => ({
        async *countTo(to: number) {
            for (let i = 1; i <= to; i++) yield { i, at: new Date(1000 * i) };
            void ctx;
        },
        async *explode() {
            yield 'one';
            throw new Error('mid-stream failure');
        },
        async *forever() {
            try {
                for (let i = 0; ; i++) {
                    yield i;
                    await new Promise((r) => setTimeout(r, 5));
                }
            } finally {
                cleanupLog.push('forever:finally');
            }
        }
    })
});

const cleanupLog: string[] = [];

/**
 * Real fetch cancels the response body when its signal aborts; an in-memory
 * handler call has no such link. Recreate it so consumer break/return()
 * reaches the server generator exactly like production.
 */
function abortLinked(response: Response, signal: AbortSignal | null | undefined): Response {
    if (!response.body || !signal) return response;
    const reader = response.body.getReader();
    const stream = new ReadableStream<Uint8Array>({
        async pull(controller) {
            const { value, done } = await reader.read();
            if (done) controller.close();
            else controller.enqueue(value);
        },
        cancel(reason) {
            return reader.cancel(reason);
        }
    });
    signal.addEventListener('abort', () => void reader.cancel().catch(() => {}));
    return new Response(stream, { status: response.status, headers: response.headers });
}

function wireSilo(): Silo {
    const silo = createSilo({ actors: [cart], defaults: quiet });
    // The client's fetch goes straight into the real endpoint handler.
    configureActors({
        endpoint: ENDPOINT,
        fetch: async (input, init) => {
            const request = new Request(input, init);
            expect(matchesActorRequest(request)).toBe(true);
            const response = await handleActorRequest(request, { silo, origin: false });
            return abortLinked(response, init?.signal);
        }
    });
    return silo;
}

afterEach(() => {
    configureActors(null);
    guardLog.length = 0;
    cleanupLog.length = 0;
});

/** The build-swapped client module, hand-built: same ref the transform emits. */
const CartRef = __actorRef('Cart', ENDPOINT, ['countTo', 'explode', 'forever']) as typeof cart;

describe('actor wire (client proxy ↔ real endpoint)', () => {
    it('round-trips a call with rich types both ways', async () => {
        wireSilo();
        const when = new Date(1234567890000);
        const result = (await actor(CartRef, 'w1').add('socks', when)) as {
            count: number;
            tags: Map<string, Date>;
        };
        expect(result.count).toBe(1);
        expect(result.tags).toBeInstanceOf(Map);
        expect(result.tags.get('socks')).toBeInstanceOf(Date);
        expect(result.tags.get('socks')!.getTime()).toBe(1234567890000);
    });

    it('runs definition guards on the wire and reports Type#method info', async () => {
        wireSilo();
        await actor(CartRef, 'w2').add('x', new Date());
        expect(guardLog).toContain('use:Cart#add');
    });

    it('a guard veto surfaces as a branded ServerFnError with its status', async () => {
        wireSilo();
        const failure = actor(CartRef, 'w3').forbidden();
        await expect(failure).rejects.toSatisfy(
            (e: unknown) => isServerFnError(e) && e.status === 403 && e.message === 'no entry'
        );
    });

    it('a non-ServerFnError throw still reaches the client as a branded 500', async () => {
        // (Prod masking of the message is core's wireErrorShape behavior —
        // __DEV__ test builds intentionally keep the detail visible.)
        wireSilo();
        await expect(actor(CartRef, 'w4').boom()).rejects.toSatisfy(
            (e: unknown) => isServerFnError(e) && e.status === 500
        );
    });

    it('an unknown actor type 404s naming the ACTOR, not a "server function"', async () => {
        const silo = wireSilo();
        // Straight at the endpoint: the client proxy replaces the server's
        // message with its own skew hint, so assert the wire body itself.
        const response = await handleActorRequest(
            new Request(`${ENDPOINT}/${encodeURIComponent('Ghost#spook')}`, {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ args: ['g'] })
            }),
            { silo, origin: false }
        );
        expect(response.status).toBe(404);
        const body = (await response.json()) as { error: { message: string } };
        expect(body.error.message).toMatch(/actor/i);
        expect(body.error.message).toContain('Ghost');
        expect(body.error.message).not.toMatch(/server function/i);
    });

    it('the 404 the client sees names the actor and points at build skew', async () => {
        wireSilo();
        const ghost = __actorRef('Ghost', ENDPOINT) as unknown as {
            __sigxActorProxy: (key: string) => { spook(): Promise<unknown> };
        };
        await expect(ghost.__sigxActorProxy('g').spook()).rejects.toSatisfy(
            (e: unknown) =>
                isServerFnError(e) &&
                e.status === 404 &&
                /Unknown actor "Ghost#spook"/.test((e as Error).message) &&
                /same deploy/i.test((e as Error).message)
        );
    });

    it('an unknown METHOD on a known actor maps to 404 method-not-found', async () => {
        wireSilo();
        const bad = actor(CartRef, 'w5') as unknown as { nope(): Promise<unknown> };
        await expect(bad.nope()).rejects.toSatisfy(
            (e: unknown) => isServerFnError(e) && e.status === 404
        );
    });

    it('rejects a non-string key with a 400', async () => {
        const silo = wireSilo();
        const response = await handleActorRequest(
            new Request(`${ENDPOINT}/${encodeURIComponent('Cart#add')}`, {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ args: [123, 'x'] })
            }),
            { silo, origin: false }
        );
        expect(response.status).toBe(400);
    });

    it('streams NDJSON end-to-end with rich chunk types', async () => {
        wireSilo();
        const out: { i: number; at: Date }[] = [];
        for await (const chunk of actor(CartRef, 's1').countTo(3)) {
            out.push(chunk as { i: number; at: Date });
        }
        expect(out.map((c) => c.i)).toEqual([1, 2, 3]);
        expect(out[0].at).toBeInstanceOf(Date);
    });

    it('a mid-stream throw arrives as an in-band branded error after prior chunks', async () => {
        wireSilo();
        const seen: unknown[] = [];
        const iterate = async () => {
            for await (const chunk of actor(CartRef, 's2').explode()) seen.push(chunk);
        };
        await expect(iterate()).rejects.toSatisfy((e: unknown) => isServerFnError(e));
        expect(seen).toEqual(['one']);
    });

    it('consumer break cancels the server generator (its finally runs) and frees the actor', async () => {
        const silo = wireSilo();
        for await (const chunk of actor(CartRef, 's3').forever()) {
            if ((chunk as number) >= 2) break;
        }
        await new Promise((r) => setTimeout(r, 20));
        expect(cleanupLog).toContain('forever:finally');
        // Keep-alive released: the activation can deactivate.
        await silo.deactivateType('Cart');
        expect(silo.stats().activations).toBe(0);
    });

    it('external wire calls inherit the silo callTimeoutMs deadline (504 on overrun)', async () => {
        const slowpoke = defineActor({
            type: 'Slowpoke',
            unguarded: true,
            state: () => ({}),
            methods: () => ({
                async nap() {
                    await new Promise((r) => setTimeout(r, 500));
                    return 'rested';
                }
            })
        });
        const silo = createSilo({
            actors: [slowpoke],
            defaults: { ...quiet, callTimeoutMs: 40 }
        });
        configureActors({
            endpoint: ENDPOINT,
            fetch: async (input, init) =>
                handleActorRequest(new Request(input, init), { silo, origin: false })
        });
        const ref = __actorRef('Slowpoke', ENDPOINT) as unknown as typeof slowpoke;
        await expect(actor(ref, 't1').nap()).rejects.toSatisfy(
            (e: unknown) => isServerFnError(e) && e.status === 504
        );
    });

    it('the endpoint-level guard is a wire-only backstop over the same mount', async () => {
        const silo = createSilo({ actors: [cart], defaults: quiet });
        const response = await handleActorRequest(
            new Request(`${ENDPOINT}/${encodeURIComponent('Cart#add')}`, {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ args: ['k', 'x', null] })
            }),
            {
                silo,
                origin: false,
                guard: () => {
                    throw new ServerFnError(401, 'endpoint backstop');
                }
            }
        );
        expect(response.status).toBe(401);
    });
});
