/**
 * `socketTransport()` paired against the REAL `createActorSocketSession`
 * over an in-memory link — no socket anywhere, which is the interop seam
 * (`connect` takes handlers and returns a link) doing its job.
 *
 * What must hold: the whole call surface (unary, stream, one-way, cancel,
 * errors) round-trips the same codec and the same error classification as
 * the HTTP transport, and a dropped connection fails in-flight calls
 * WITHOUT retrying them.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { ServerFnError } from '@sigx/server';
import { defineActor } from '@sigx/actors';
import { createHost, type Host } from '@sigx/actors/host';
import { createActorSocketSession, type ActorSocketSession } from '@sigx/actors/server';
import { socketTransport, type SocketLink } from '@sigx/actors-ws/client';
import type { ActorTransport } from '@sigx/actors/client';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

/** `slow()` parks here; released per test / in afterEach. */
const gates: Array<() => void> = [];
let adds = 0;

const Cart = defineActor({
    type: 'Cart',
    allowAnonymous: true,
    state: () => ({ items: [] as string[] }),
    methods: (ctx) => ({
        async add(item: string) {
            adds += 1;
            ctx.state.items.push(item);
            await ctx.save();
            return ctx.state.items.length;
        },
        async total() {
            return ctx.state.items.length;
        },
        async echo(value: unknown) {
            return value;
        },
        async slow() {
            await new Promise<void>((resolve) => gates.push(resolve));
            return 'released';
        }
    }),
    streams: () => ({
        async *countTo(to: number) {
            for (let i = 1; i <= to; i++) yield i;
        },
        async *forever() {
            let i = 0;
            for (;;) {
                yield i++;
                await new Promise((resolve) => setTimeout(resolve, 5));
            }
        }
    })
});

const Secret = defineActor({
    type: 'Secret',
    authorize: [
        () => {
            throw new ServerFnError(401, 'nope');
        }
    ],
    state: () => ({ n: 1 }),
    methods: (ctx) => ({
        async peek() {
            return ctx.state.n;
        }
    })
});

let host: Host | null = null;
const transports: ActorTransport[] = [];
const sessions: ActorSocketSession[] = [];

afterEach(async () => {
    for (const t of transports) void t.close?.();
    transports.length = 0;
    for (const s of sessions) s.close();
    sessions.length = 0;
    const drain = setInterval(() => {
        for (const release of gates.splice(0)) release();
    }, 5);
    try {
        await host?.stop();
    } finally {
        clearInterval(drain);
    }
    host = null;
    adds = 0;
});

async function start(): Promise<Host> {
    host = createHost({ actors: [Cart, Secret], defaults: quiet });
    await host.start();
    return host;
}

interface Pair {
    transport: ActorTransport;
    /** Sever the current connection from "the network", as a drop would. */
    drop(): void;
    /** How many connections were ever dialled. */
    dials(): number;
}

/**
 * Wire the transport straight into a real session. `connect` is sync while
 * session construction is not, so messages buffer until the session is up —
 * the transport gates its own sends on `onOpen` anyway.
 */
function pair(s: Host): Pair {
    let dialled = 0;
    let sever: (() => void) | null = null;
    const transport = socketTransport({
        retryMs: 1,
        maxRetryMs: 5,
        connect: (handlers) => {
            dialled += 1;
            let session: ActorSocketSession | null = null;
            let dead = false;
            const outbound: string[] = [];
            sever = () => {
                dead = true;
                session?.close();
                handlers.onClose();
            };
            void createActorSocketSession({
                host: s,
                request: new Request('http://actors.test/socket'),
                origin: false,
                pingMs: 0,
                send: (message) => {
                    if (!dead) handlers.onMessage(message);
                },
                close: () => {
                    if (!dead) {
                        dead = true;
                        handlers.onClose();
                    }
                }
            }).then((created) => {
                session = created;
                sessions.push(created);
                handlers.onOpen();
                for (const message of outbound.splice(0)) created.handle(message);
            });
            const link: SocketLink = {
                send: (message) => {
                    if (dead) return;
                    if (session) session.handle(message);
                    else outbound.push(message);
                },
                close: () => {
                    session?.close();
                }
            };
            return link;
        }
    });
    transports.push(transport);
    return { transport, drop: () => sever?.(), dials: () => dialled };
}

describe('unary calls over the socket', () => {
    it('resolves with the revived value; key rides as the first argument', async () => {
        const s = await start();
        const { transport } = pair(s);
        expect(await transport.call('Cart#add', ['k1', 'apple'])).toBe(1);
        expect(await transport.call('Cart#total', ['k1'])).toBe(1);
    });

    it('round-trips codec types exactly like the HTTP wire', async () => {
        const s = await start();
        const { transport } = pair(s);
        const sent = new Date(86_400_000);
        const back = await transport.call('Cart#echo', ['k1', sent]);
        expect(back).toBeInstanceOf(Date);
        expect((back as Date).getTime()).toBe(sent.getTime());
    });

    it('rejects branded, with the endpoint classification', async () => {
        const s = await start();
        const { transport } = pair(s);
        await expect(transport.call('Secret#peek', ['k'])).rejects.toMatchObject({
            __sigxServerFnError: true,
            status: 401
        });
        await expect(transport.call('Nope#x', ['k'])).rejects.toMatchObject({
            status: 404
        });
        // …and the connection survived both failures.
        expect(await transport.call('Cart#total', ['k1'])).toBe(0);
    });

    it('a one-way call resolves immediately and still lands', async () => {
        const s = await start();
        const { transport } = pair(s);
        await transport.call('Cart#add', ['k1', 'x'], { oneWay: true });
        await expect
            .poll(async () => transport.call('Cart#total', ['k1']))
            .toBe(1);
    });

    it('abort cancels the call without closing the connection', async () => {
        const s = await start();
        const { transport } = pair(s);
        const controller = new AbortController();
        const pending = transport.call('Cart#slow', ['k1'], { signal: controller.signal });
        // Let the turn actually start (and park) before cancelling it.
        await expect.poll(() => gates.length).toBe(1);
        controller.abort();
        await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
        // Release the parked turn; the actor is single-threaded, so the next
        // call proves both that the turn finished and the socket survived.
        for (const release of gates.splice(0)) release();
        expect(await transport.call('Cart#total', ['k1'])).toBe(0);
    });
});

describe('streams over the socket', () => {
    it('yields revived chunks and completes', async () => {
        const s = await start();
        const { transport } = pair(s);
        const seen: unknown[] = [];
        for await (const value of transport.stream('Cart#countTo', ['k', 3])) {
            seen.push(value);
        }
        expect(seen).toEqual([1, 2, 3]);
    });

    it('a consumer break cancels server-side and keeps the connection', async () => {
        const s = await start();
        const { transport } = pair(s);
        const seen: unknown[] = [];
        for await (const value of transport.stream('Cart#forever', ['k'])) {
            seen.push(value);
            if (seen.length === 2) break;
        }
        // The server saw the cancel: its in-flight table drains.
        await expect.poll(() => sessions.at(-1)!.stats().inFlight).toBe(0);
        expect(await transport.call('Cart#total', ['k'])).toBe(0);
    });
});

describe('the drop policy', () => {
    it('fails in-flight calls on a drop and NEVER retries them', async () => {
        const s = await start();
        const { transport, drop } = pair(s);
        // Prove the connection works, then wedge a call mid-flight: `slow`
        // parks its turn on a gate, so the reply cannot race the drop.
        expect(await transport.call('Cart#total', ['k1'])).toBe(0);
        const pending = transport.call('Cart#slow', ['k1']);
        await expect.poll(() => gates.length).toBe(1);
        drop();
        await expect(pending).rejects.toMatchObject({
            __sigxServerFnError: true,
            status: 0
        });
        for (const release of gates.splice(0)) release();
        // NEVER retried: the failed call is not re-sent on the next dial, so
        // no second turn ever parks on the gate.
        expect(await transport.call('Cart#total', ['k1'])).toBe(0);
        await new Promise((resolve) => setTimeout(resolve, 20));
        expect(gates).toHaveLength(0);
    });

    it('the next call after a drop dials a fresh connection', async () => {
        const s = await start();
        const { transport, drop, dials } = pair(s);
        expect(await transport.call('Cart#total', ['k1'])).toBe(0);
        drop();
        expect(await transport.call('Cart#total', ['k1'])).toBe(0);
        expect(dials()).toBe(2);
    });
});

describe('lifecycle', () => {
    it('close() is idempotent and fails everything in flight', async () => {
        const s = await start();
        const { transport } = pair(s);
        const pending = transport.call('Cart#slow', ['k1']);
        await expect.poll(() => gates.length).toBe(1);
        void transport.close?.();
        void transport.close?.();
        await expect(pending).rejects.toMatchObject({ status: 0 });
        await expect(transport.call('Cart#total', ['k1'])).rejects.toMatchObject({
            status: 0
        });
    });

    it('is lazy: nothing dials until the first call', async () => {
        const s = await start();
        const { dials } = pair(s);
        await new Promise((resolve) => setTimeout(resolve, 20));
        expect(dials()).toBe(0);
    });
});
