/**
 * `createActorSocketSession` (#99) over a fake link — no socket anywhere.
 *
 * That the whole session is testable with two arrays and a string is the
 * design working: a `Request` in, two callbacks out, so `ws`, socket.io,
 * uWS, Bun, Deno and a Durable Object can all drive the same core.
 *
 * The load-bearing assertions: the per-message pipeline is the SAME one the
 * HTTP mount runs (guard rejections and 404s answer per CALL, never per
 * connection), a protocol breach closes the connection while a failed call
 * does not, and identity resolves once per connection while middleware runs
 * per message.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ServerFnError } from '@sigx/server';
import { stubServerApp } from '@sigx/server/testing';
import { defineActor } from '@sigx/actors';
import { createHost, type Host } from '@sigx/actors/host';
import { createActorSocketSession, type ActorSocketSession } from '@sigx/actors/server';
import { encodeWire, reviveWire, type SocketReply } from '@sigx/actors/socket-wire';
import { createFanOut } from '../src/watch-core';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

/** `Cart#slow` parks on one of these; afterEach releases them so a turn in
 *  flight never outlives its test (a sleep would hang `host.stop()`). */
const gates: Array<() => void> = [];

const Cart = defineActor({
    type: 'Cart',
    allowAnonymous: true,
    state: () => ({ items: [] as string[] }),
    methods: (ctx) => ({
        async add(item: string) {
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

const Whoami = defineActor({
    type: 'Whoami',
    allowAnonymous: true,
    state: () => ({}),
    methods: (ctx) => ({
        async principal() {
            return ctx.principal ?? null;
        }
    })
});

let host: Host | null = null;
let restore: (() => void) | undefined;
const sessions: ActorSocketSession[] = [];

afterEach(async () => {
    for (const s of sessions) s.close();
    sessions.length = 0;
    // Drain WHILE stopping: a `slow` turn may not have parked on its gate
    // yet when this hook runs, so a single splice would strand it.
    const drain = setInterval(() => {
        for (const release of gates.splice(0)) release();
    }, 5);
    try {
        await host?.stop();
    } finally {
        clearInterval(drain);
    }
    host = null;
    restore?.();
    restore = undefined;
});

async function start(defs = [Cart, Secret, Whoami] as const): Promise<Host> {
    host = createHost({ actors: [...defs], defaults: quiet });
    await host.start();
    return host;
}

/**
 * Build an upgrade request that really carries an `Origin` header.
 *
 * Duck-typed rather than `new Request(url, { headers })`, because the test
 * environment is happy-dom, whose `Request` enforces the BROWSER'S
 * forbidden-header rules and silently drops `Origin` — a constraint real
 * adapters do not have (undici keeps it, and a server port hands it over
 * verbatim). A standalone `Headers` carries no guard.
 */
function upgradeRequest(url: string, origin?: string): Request {
    const headers = new Headers();
    if (origin !== undefined) headers.set('origin', origin);
    return {
        url,
        method: 'GET',
        headers,
        signal: new AbortController().signal
    } as unknown as Request;
}

interface FakeLink {
    sent: SocketReply[];
    closes: Array<{ code: number; reason: string }>;
    send(message: string): void;
    close(code: number, reason: string): void;
}

function fakeLink(): FakeLink {
    const link: FakeLink = {
        sent: [],
        closes: [],
        send(message) {
            link.sent.push(JSON.parse(message) as SocketReply);
        },
        close(code, reason) {
            link.closes.push({ code, reason });
        }
    };
    return link;
}

async function connect(
    s: Host,
    overrides: Record<string, unknown> = {},
    request = new Request('http://actors.test/socket')
): Promise<{ session: ActorSocketSession; link: FakeLink }> {
    const link = fakeLink();
    const session = await createActorSocketSession({
        host: s,
        request,
        send: link.send,
        close: link.close,
        origin: false,
        pingMs: 0,
        ...overrides
    });
    sessions.push(session);
    return { session, link };
}

async function until(predicate: () => boolean, ms = 2000): Promise<void> {
    const started = Date.now();
    while (!predicate()) {
        if (Date.now() - started > ms) throw new Error('timed out waiting');
        await new Promise((resolve) => setTimeout(resolve, 5));
    }
}

const framesFor = (link: FakeLink, id: number): SocketReply[] =>
    link.sent.filter((f) => 'i' in f && f.i === id);

describe('unary calls', () => {
    it('answers {i,v} then {i,d}, key as first argument', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 1, s: 'Cart#add', a: ['k1', 'apple'] }));
        await until(() => framesFor(link, 1).length === 2);
        expect(framesFor(link, 1)).toEqual([{ i: 1, v: 1 }, { i: 1, d: 1 }]);
    });

    it('multiplexes concurrent calls under their own ids', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 7, s: 'Cart#add', a: ['k1', 'a'] }));
        session.handle(JSON.stringify({ i: 9, s: 'Cart#add', a: ['k2', 'b'] }));
        await until(() => framesFor(link, 7).length === 2 && framesFor(link, 9).length === 2);
        expect(framesFor(link, 7).at(-1)).toEqual({ i: 7, d: 1 });
        expect(framesFor(link, 9).at(-1)).toEqual({ i: 9, d: 1 });
    });

    it('a guard rejection fails THAT call, not the connection', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 1, s: 'Secret#peek', a: ['k'] }));
        await until(() => framesFor(link, 1).length === 1);
        expect(framesFor(link, 1)[0]).toEqual({ i: 1, e: { message: 'nope', status: 401 } });
        expect(link.closes).toEqual([]);
        // The connection still serves the next call.
        session.handle(JSON.stringify({ i: 2, s: 'Cart#total', a: ['k'] }));
        await until(() => framesFor(link, 2).length === 2);
        expect(framesFor(link, 2)[0]).toEqual({ i: 2, v: 0 });
    });

    it('unknown types and malformed symbols answer 404 error frames', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 1, s: 'Nope#x', a: ['k'] }));
        session.handle(JSON.stringify({ i: 2, s: 'no-hash', a: ['k'] }));
        await until(() => framesFor(link, 1).length === 1 && framesFor(link, 2).length === 1);
        for (const id of [1, 2]) {
            const frame = framesFor(link, id)[0] as { e: { status: number } };
            expect(frame.e.status).toBe(404);
        }
    });

    it('refuses $sigx: reserved methods exactly like an unknown method', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 1, s: 'Cart#$sigx:reminder', a: ['k'] }));
        await until(() => framesFor(link, 1).length === 1);
        expect((framesFor(link, 1)[0] as { e: { status: number } }).e.status).toBe(404);
        expect(link.closes).toEqual([]);
    });

    it('a missing key answers 400', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 1, s: 'Cart#total', a: [] }));
        await until(() => framesFor(link, 1).length === 1);
        expect((framesFor(link, 1)[0] as { e: { status: number } }).e.status).toBe(400);
    });

    it('round-trips codec types the same as the HTTP wire', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        const sent = new Date(86_400_000);
        // The client encodes `a` exactly like the HTTP body's `{args}`…
        session.handle(JSON.stringify({ i: 1, s: 'Cart#echo', a: encodeWire(['k', sent]) }));
        await until(() => framesFor(link, 1).length === 2);
        // …and values come back through the same codec.
        const frame = framesFor(link, 1)[0] as { v: unknown };
        const revived = reviveWire(frame.v);
        expect(revived).toBeInstanceOf(Date);
        expect((revived as Date).getTime()).toBe(sent.getTime());
    });

    it('a one-way call answers nothing and still lands', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 1, s: 'Cart#add', a: ['k1', 'x'], w: 1 }));
        // The observable effect: the next read sees the item.
        session.handle(JSON.stringify({ i: 2, s: 'Cart#total', a: ['k1'] }));
        await until(() => framesFor(link, 2).length === 2);
        expect(framesFor(link, 2)[0]).toEqual({ i: 2, v: 1 });
        expect(framesFor(link, 1)).toEqual([]);
    });
});

describe('streams', () => {
    it('relays chunks as {i,v} frames and terminates with {i,d}', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 3, s: 'Cart#countTo', a: ['k', 3] }));
        await until(() => framesFor(link, 3).length === 4);
        expect(framesFor(link, 3)).toEqual([
            { i: 3, v: 1 },
            { i: 3, v: 2 },
            { i: 3, v: 3 },
            { i: 3, d: 1 }
        ]);
    });

    it('cancel stops a stream without closing the socket', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 5, s: 'Cart#forever', a: ['k'] }));
        await until(() => framesFor(link, 5).length >= 2);
        session.handle(JSON.stringify({ i: 5, c: 1 }));
        await new Promise((resolve) => setTimeout(resolve, 40));
        const after = framesFor(link, 5).length;
        await new Promise((resolve) => setTimeout(resolve, 40));
        // No more frames for the cancelled id — no values, no d, no e.
        expect(framesFor(link, 5).length).toBe(after);
        expect(framesFor(link, 5).every((f) => 'v' in f)).toBe(true);
        expect(link.closes).toEqual([]);
        // And the connection still works.
        session.handle(JSON.stringify({ i: 6, s: 'Cart#total', a: ['k'] }));
        await until(() => framesFor(link, 6).length === 2);
    });
});

describe('protocol breaches close; failed calls do not', () => {
    it('unparseable input closes 1003', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle('not json');
        expect(link.closes).toEqual([{ code: 1003, reason: 'unparseable message' }]);
    });

    it('an unrecognized shape closes 1003', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 1, what: true }));
        expect(link.closes).toEqual([{ code: 1003, reason: 'unrecognized message' }]);
    });

    it('a duplicate id closes 1003 — across calls AND subscriptions', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 1, s: 'Cart#slow', a: ['k'] }));
        session.handle(JSON.stringify({ i: 1, s: 'Cart#total', a: ['k'] }));
        await until(() => link.closes.length === 1);
        expect(link.closes[0]).toEqual({ code: 1003, reason: 'duplicate id' });

        // Replies carry only `i`, so the namespace is shared: a subscription
        // reusing a call's id is the same breach.
        const second = await connect(s);
        second.session.handle(JSON.stringify({ i: 5, s: 'Cart#slow', a: ['k2'] }));
        second.session.handle(
            JSON.stringify({ i: 5, sub: { t: 'Cart', k: 'k2', m: 'total' } })
        );
        await until(() => second.link.closes.length === 1);
        expect(second.link.closes[0]).toEqual({ code: 1003, reason: 'duplicate id' });
    });

    it('an oversized message closes 1009 before parsing', async () => {
        const s = await start();
        const { session, link } = await connect(s, { maxMessageBytes: 64 });
        session.handle(JSON.stringify({ i: 1, s: 'Cart#add', a: ['k', 'x'.repeat(200)] }));
        expect(link.closes).toEqual([{ code: 1009, reason: 'message too large' }]);
        expect(link.sent).toEqual([]);
    });

    it('excess concurrency fails the CALL with 429, never the connection', async () => {
        const s = await start();
        const { session, link } = await connect(s, { maxConcurrent: 1 });
        session.handle(JSON.stringify({ i: 1, s: 'Cart#slow', a: ['k'] }));
        session.handle(JSON.stringify({ i: 2, s: 'Cart#total', a: ['k'] }));
        await until(() => framesFor(link, 2).length === 1);
        expect((framesFor(link, 2)[0] as { e: { status: number } }).e.status).toBe(429);
        expect(link.closes).toEqual([]);
    });

    it('a misconfigured maxSubscriptions throws at construction', async () => {
        const s = await start();
        await expect(connect(s, { maxSubscriptions: 1.5 })).rejects.toThrow(
            /non-negative integer/
        );
    });

    it('a malformed subscription record fails THAT subscription with 400', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 1, sub: { t: 'Cart', k: '', m: 'total' } }));
        await until(() => framesFor(link, 1).length === 1);
        expect((framesFor(link, 1)[0] as { e: { status: number } }).e.status).toBe(400);
        expect(link.closes).toEqual([]);
    });

    it('the per-connection subscription cap fails the subscription, not the socket', async () => {
        const s = await start();
        const { session, link } = await connect(s, { maxSubscriptions: 1 });
        session.handle(JSON.stringify({ i: 1, sub: { t: 'Cart', k: 'a', m: 'total' } }));
        await until(() => framesFor(link, 1).length >= 1);
        session.handle(JSON.stringify({ i: 2, sub: { t: 'Cart', k: 'b', m: 'total' } }));
        await until(() => framesFor(link, 2).length === 1);
        const frame = framesFor(link, 2)[0] as { e: { status: number; message: string } };
        expect(frame.e.status).toBe(400);
        expect(link.closes).toEqual([]);
    });
});

describe('live subscriptions', () => {
    it('a subscription seeds with the current value and pushes every change', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 1, sub: { t: 'Cart', k: 'w', m: 'total' } }));
        await until(() => framesFor(link, 1).length >= 1);
        expect(framesFor(link, 1)[0]).toEqual({ i: 1, v: 0 });
        session.handle(JSON.stringify({ i: 2, s: 'Cart#add', a: ['w', 'x'] }));
        await until(() => framesFor(link, 1).some((f) => 'v' in f && f.v === 1));
        expect(session.stats().subscriptions).toBe(1);
    });

    it('{i,uns} closes the subscription silently and releases the watch', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 1, sub: { t: 'Cart', k: 'w', m: 'total' } }));
        await until(() => framesFor(link, 1).length >= 1);
        session.handle(JSON.stringify({ i: 1, uns: 1 }));
        await until(() => session.stats().subscriptions === 0);
        const seen = framesFor(link, 1).length;
        session.handle(JSON.stringify({ i: 2, s: 'Cart#add', a: ['w', 'x'] }));
        await until(() => framesFor(link, 2).length === 2);
        await new Promise((resolve) => setTimeout(resolve, 30));
        // No frame after the unsubscribe — and no terminal reply either.
        expect(framesFor(link, 1).length).toBe(seen);
        // The watch released its keep-alive, so the actor can idle out.
        await until(() => s.activations().every((a) => !a.keptAlive));
    });

    it('a guard rejecting one subscription costs the connection nothing', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 1, sub: { t: 'Secret', k: 'k', m: 'peek' } }));
        await until(() => framesFor(link, 1).length === 1);
        expect(framesFor(link, 1)[0]).toEqual({ i: 1, e: { message: 'nope', status: 401 } });
        session.handle(JSON.stringify({ i: 2, sub: { t: 'Cart', k: 'w', m: 'total' } }));
        await until(() => framesFor(link, 2).length >= 1);
        expect(framesFor(link, 2)[0]).toEqual({ i: 2, v: 0 });
    });

    it('a subscription whose seed starves answers a 504 frame instead of hanging (#180)', async () => {
        // The same posture bound the CALL path arms: pipeline + dispatch +
        // the FIRST value. Before #180 the watch path armed nothing, and a
        // seed queued behind a wedged turn held the subscription — loop,
        // change sub and keep-alive included — open forever, silently.
        restore = stubServerApp({ posture: { timeoutMs: 50 } });
        const s = await start();
        const { session, link } = await connect(s);
        // Wedge the actor: `slow` parks on a gate the afterEach releases.
        session.handle(JSON.stringify({ i: 1, s: 'Cart#slow', a: ['w'] }));
        // The seed read queues behind the parked turn and cannot run.
        session.handle(JSON.stringify({ i: 2, sub: { t: 'Cart', k: 'w', m: 'total' } }));
        await until(() => framesFor(link, 2).length === 1);
        expect((framesFor(link, 2)[0] as { e: { status: number } }).e.status).toBe(504);
        // The failure is the subscription's, not the connection's — and the
        // entry is gone, so the id is reusable and stats read zero.
        expect(link.closes).toEqual([]);
        await until(() => session.stats().subscriptions === 0);
    });

    it('the deadline covers only establishment — a seeded subscription is never timed out', async () => {
        restore = stubServerApp({ posture: { timeoutMs: 50 } });
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 1, sub: { t: 'Cart', k: 'w', m: 'total' } }));
        await until(() => framesFor(link, 1).length >= 1);
        expect(framesFor(link, 1)[0]).toEqual({ i: 1, v: 0 });
        // Outlive the timeout, then prove the watch still pushes.
        await new Promise((resolve) => setTimeout(resolve, 120));
        session.handle(JSON.stringify({ i: 2, s: 'Cart#add', a: ['w', 'x'] }));
        await until(() => framesFor(link, 1).some((f) => 'v' in f && f.v === 1));
        expect(framesFor(link, 1).every((f) => !('e' in f))).toBe(true);
    });

    it('a value the abort races out of the fan-out still counts as seeding — no 504 after it', async () => {
        // The deterministic replay path (the same latent flaw as the `$live`
        // mount, found fixing #192): the fan-out hands a subscriber whose
        // signal is ALREADY aborted its cached `last` before `done`
        // (`watch-core.ts` checks the queue first). When the prelude is slow
        // enough that the deadline fires before `dispatchWatch` attaches,
        // the sequence is: deadline → abort → subscribe → replayed value →
        // done — and re-raising the stale `timedOut` after that value told a
        // healthy widget its subscription failed.
        restore = stubServerApp({ posture: { timeoutMs: 5 } });
        const fanOut = createFanOut(() => {});
        fanOut.push(7); // a sibling seeded this shared entry earlier
        const fake = {
            definition: async () => {
                // Parked past the deadline: this 30 ms timer is armed after
                // the 5 ms deadline and is longer, so the deadline callback
                // runs first — deterministic by timer ordering, not a race.
                await new Promise((resolve) => setTimeout(resolve, 30));
                return Cart;
            },
            dispatchWatch: (
                _target: unknown,
                _method: unknown,
                _args: unknown,
                call: { abortSignal?: AbortSignal }
            ) => fanOut.subscribe(call.abortSignal)
        };
        const { session, link } = await connect(fake as unknown as Host);
        session.handle(JSON.stringify({ i: 1, sub: { t: 'Cart', k: 'w', m: 'total' } }));
        await until(() => framesFor(link, 1).length >= 1);
        // Give a (buggy) trailing 504 every chance to arrive before the
        // assertion — the unwind after the value is pure microtasks.
        await new Promise((resolve) => setTimeout(resolve, 30));
        expect(framesFor(link, 1)).toEqual([{ i: 1, v: 7 }]);
        expect(session.stats().subscriptions).toBe(0);
    });
});

describe('keepalive', () => {
    it('answers an inbound {p:1} with {p:1}', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ p: 1 }));
        expect(link.sent).toEqual([{ p: 1 }]);
    });

    it('pings after outbound silence, and only then', async () => {
        const s = await start();
        const { link } = await connect(s, { pingMs: 30 });
        await until(() => link.sent.some((f) => 'p' in f));
        expect(link.sent[0]).toEqual({ p: 1 });
    });

    /**
     * The two burst cases run on FAKE timers (#108): under real time, "one
     * frame every 15 ms against a 40 ms window" is only a small margin over
     * the scheduler, and one routine stall on a loaded runner opens a
     * genuine 40 ms gap — the ping then fires exactly as specified and the
     * assertion, not the behaviour, breaks. `performance` is faked alongside
     * `setTimeout` because the deadline is a `performance.now()` stamp the
     * timer consults (#250) — mixing a fake timer with the real clock would
     * make `idle` nonsense. Both must be faked BEFORE `connect`, which arms
     * the ping timer and takes the first stamp.
     */
    it('does not ping a connection that keeps sending', async () => {
        // The deadline is measured from the last outbound frame, so traffic
        // must keep pushing it out. Since #250 `reply()` only stamps a
        // timestamp — it no longer re-arms the timer — so this is the test
        // that the timer still consults that stamp instead of firing blind.
        vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
        try {
            const s = await start();
            const { session, link } = await connect(s, { pingMs: 40 });
            for (let i = 1; i <= 12; i++) {
                session.handle(JSON.stringify({ i, s: 'Cart#total', a: ['busy'] }));
                await vi.advanceTimersByTimeAsync(15);
            }
            // 180 fake-ms of traffic, never 40 quiet — a timer firing blind
            // (or a `reply()` that stopped stamping) pings at 40.
            expect(link.sent.some((f) => 'p' in f)).toBe(false);
        } finally {
            vi.useRealTimers();
        }
    });

    it('pings once the burst stops', async () => {
        // …and the other half: the same connection, gone quiet, is pinged
        // within about one window rather than never — the failure mode a
        // stamp-only `reply()` would have if nothing rescheduled.
        vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
        try {
            const s = await start();
            const { session, link } = await connect(s, { pingMs: 40 });
            for (let i = 1; i <= 5; i++) {
                session.handle(JSON.stringify({ i, s: 'Cart#total', a: ['burst'] }));
                await vi.advanceTimersByTimeAsync(10);
            }
            expect(link.sent.some((f) => 'p' in f)).toBe(false);
            // Quiet from here: one full window later the ping must be out.
            await vi.advanceTimersByTimeAsync(80);
            expect(link.sent.some((f) => 'p' in f)).toBe(true);
        } finally {
            vi.useRealTimers();
        }
    });
});

describe('origin, pinned at upgrade', () => {
    it("default 'same-origin' refuses a cross-origin upgrade with 1008", async () => {
        const s = await start();
        const link = fakeLink();
        await expect(
            createActorSocketSession({
                host: s,
                request: upgradeRequest('http://actors.test/socket', 'http://evil.test'),
                send: link.send,
                close: link.close
            })
        ).rejects.toThrow(/origin/);
        expect(link.closes).toEqual([{ code: 1008, reason: 'origin not allowed' }]);
    });

    it("default 'same-origin' also refuses an origin-LESS upgrade", async () => {
        const s = await start();
        const link = fakeLink();
        await expect(
            createActorSocketSession({
                host: s,
                request: upgradeRequest('http://actors.test/socket'),
                send: link.send,
                close: link.close
            })
        ).rejects.toThrow(/origin/);
    });

    it('accepts the same origin, normalizing a ws:// request URL', async () => {
        const s = await start();
        for (const url of ['http://actors.test/socket', 'ws://actors.test/socket']) {
            const { link } = await connect(
                s,
                { origin: undefined },
                upgradeRequest(url, 'http://actors.test')
            );
            expect(link.closes).toEqual([]);
        }
    });

    it("'verify-when-present' admits an origin-less programmatic client", async () => {
        const s = await start();
        const { link } = await connect(
            s,
            { origin: 'verify-when-present' },
            upgradeRequest('http://actors.test/socket')
        );
        expect(link.closes).toEqual([]);
    });

    it('an allowlist admits exactly its entries', async () => {
        const s = await start();
        const allowed = await connect(
            s,
            { origin: ['http://app.test'] },
            upgradeRequest('http://actors.test/socket', 'http://app.test')
        );
        expect(allowed.link.closes).toEqual([]);
        const link = fakeLink();
        await expect(
            createActorSocketSession({
                host: s,
                request: upgradeRequest('http://actors.test/socket', 'http://other.test'),
                send: link.send,
                close: link.close,
                origin: ['http://app.test']
            })
        ).rejects.toThrow(/origin/);
    });
});

describe('identity and middleware across one connection', () => {
    it('authenticates once per connection, runs middleware per message', async () => {
        let authenticated = 0;
        let middlewareRuns = 0;
        restore = stubServerApp({
            middleware: [
                async () => {
                    middlewareRuns += 1;
                }
            ],
            authenticate: () => {
                authenticated += 1;
                return { id: 'u1' };
            },
            codec: {
                encode: (p) => (p as { id: string }).id,
                decode: (encoded) => ({ id: encoded })
            }
        });
        const s = await start();
        const { session, link } = await connect(s);
        const afterUpgrade = middlewareRuns;
        session.handle(JSON.stringify({ i: 1, s: 'Whoami#principal', a: ['k'] }));
        await until(() => framesFor(link, 1).length === 2);
        session.handle(JSON.stringify({ i: 2, s: 'Whoami#principal', a: ['k'] }));
        await until(() => framesFor(link, 2).length === 2);
        // The pinned principal crossed the dispatch on both calls…
        expect(framesFor(link, 1)[0]).toEqual({ i: 1, v: { id: 'u1' } });
        expect(framesFor(link, 2)[0]).toEqual({ i: 2, v: { id: 'u1' } });
        // …authentication resolved once, at upgrade…
        expect(authenticated).toBe(1);
        // …and middleware ran again for EVERY message (a rate limiter must
        // meter calls, not connections).
        expect(middlewareRuns).toBe(afterUpgrade + 2);
    });

    it('an actor without allowAnonymous still denies an ANONYMOUS socket', async () => {
        // The suite-wide stamp signs everyone in; anonymity must be explicit.
        restore = stubServerApp({ authenticate: () => null });
        const Locked = defineActor({
            type: 'Locked',
            state: () => ({}),
            methods: () => ({
                async read() {
                    return 'secret';
                }
            })
        });
        const s = await start([Locked] as never);
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 1, s: 'Locked#read', a: ['k'] }));
        await until(() => framesFor(link, 1).length === 1);
        expect((framesFor(link, 1)[0] as { e: { status: number } }).e.status).toBe(401);
        // And the signed-in background admits the same call — the gate is
        // deciding on identity, not refusing the transport.
        restore();
        restore = undefined;
        const again = await connect(s);
        again.session.handle(JSON.stringify({ i: 1, s: 'Locked#read', a: ['k'] }));
        await until(() => framesFor(again.link, 1).length === 2);
        expect(framesFor(again.link, 1)[0]).toEqual({ i: 1, v: 'secret' });
    });
});

describe('teardown', () => {
    it('close() aborts in-flight streams and goes quiet', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 1, s: 'Cart#forever', a: ['k'] }));
        await until(() => framesFor(link, 1).length >= 1);
        session.close();
        const sentAfterClose = link.sent.length;
        await new Promise((resolve) => setTimeout(resolve, 40));
        expect(link.sent.length).toBe(sentAfterClose);
        expect(session.stats().inFlight).toBe(0);
    });
});

describe('sessions must not outlive credentials (#159)', () => {
    it('rejects a typo-disabled bound at construction — and still closes the socket', async () => {
        const s = await start();
        const l = fakeLink();
        await expect(
            createActorSocketSession({
                host: s,
                request: new Request('http://actors.test/socket'),
                send: l.send,
                close: l.close,
                origin: false,
                revalidateMs: -1
            })
        ).rejects.toThrow(/non-negative integer/);
        // The server's own error, and an accepted socket must not dangle.
        expect(l.closes).toEqual([{ code: 1011, reason: 'session misconfigured' }]);
        await expect(connect(s, { maxConnectionMs: 1.5 })).rejects.toThrow(
            /non-negative integer/
        );
    });

    it('revalidations never overlap, however slow authenticate is', async () => {
        let inFlight = 0;
        let peak = 0;
        restore = stubServerApp({
            authenticate: async () => {
                inFlight += 1;
                peak = Math.max(peak, inFlight);
                await new Promise((resolve) => setTimeout(resolve, 45));
                inFlight -= 1;
                return { id: 'u1' };
            }
        });
        const s = await start();
        const { link } = await connect(s, { revalidateMs: 10 });
        // Several intervals fire while one revalidation is still running.
        await new Promise((resolve) => setTimeout(resolve, 120));
        expect(peak).toBe(1);
        expect(link.closes).toEqual([]);
    });

    it('maxConnectionMs closes 1008 at the cap and tears the session down', async () => {
        const s = await start();
        const { session, link } = await connect(s, { maxConnectionMs: 40 });
        session.handle(JSON.stringify({ i: 1, sub: { t: 'Cart', k: 'w', m: 'total' } }));
        await until(() => framesFor(link, 1).length >= 1);
        await until(() => link.closes.length === 1);
        expect(link.closes[0]).toEqual({ code: 1008, reason: 'connection lifetime exceeded' });
        // `bufferedBytes` is null because `connect()` supplies no adapter
        // callback — "cannot tell you", not zero (#252).
        expect(session.stats()).toEqual({ inFlight: 0, subscriptions: 0, bufferedBytes: null });
        // The watch released its keep-alive with the session.
        await until(() => s.activations().every((a) => !a.keptAlive));
    });

    it('revalidation closes 1008 when the session is revoked server-side', async () => {
        let revoked = false;
        restore = stubServerApp({
            authenticate: () => (revoked ? null : { id: 'u1' }),
            codec: {
                encode: (p) => (p as { id: string }).id,
                decode: (encoded) => ({ id: encoded })
            }
        });
        const s = await start();
        const { session, link } = await connect(s, { revalidateMs: 15 });
        // Live across several revalidations while the credentials stand.
        session.handle(JSON.stringify({ i: 1, s: 'Cart#total', a: ['k'] }));
        await until(() => framesFor(link, 1).length === 2);
        await new Promise((resolve) => setTimeout(resolve, 40));
        expect(link.closes).toEqual([]);
        revoked = true;
        await until(() => link.closes.length === 1);
        expect(link.closes[0]).toEqual({ code: 1008, reason: 'session expired' });
    });

    it('revalidation closes 1008 when the identity CHANGES', async () => {
        let current = 'u1';
        restore = stubServerApp({
            authenticate: () => ({ id: current }),
            codec: {
                encode: (p) => (p as { id: string }).id,
                decode: (encoded) => ({ id: encoded })
            }
        });
        const s = await start();
        const { link } = await connect(s, { revalidateMs: 15 });
        current = 'u2';
        await until(() => link.closes.length === 1);
        expect(link.closes[0]).toEqual({ code: 1008, reason: 'identity changed' });
    });

    it('an anonymous connection staying anonymous revalidates forever', async () => {
        restore = stubServerApp({ authenticate: () => null });
        const s = await start();
        const { session, link } = await connect(s, { revalidateMs: 10 });
        await new Promise((resolve) => setTimeout(resolve, 50));
        expect(link.closes).toEqual([]);
        session.handle(JSON.stringify({ i: 1, s: 'Cart#total', a: ['k'] }));
        await until(() => framesFor(link, 1).length === 2);
    });

    it('catches a sign-out even with NO principal codec configured', async () => {
        let revoked = false;
        // No codec: the encoded principal is undefined either way, so only
        // the PRESENCE comparison can see the sign-out.
        restore = stubServerApp({ authenticate: () => (revoked ? null : { id: 'u1' }) });
        const s = await start();
        const { link } = await connect(s, { revalidateMs: 15 });
        await new Promise((resolve) => setTimeout(resolve, 35));
        expect(link.closes).toEqual([]);
        revoked = true;
        await until(() => link.closes.length === 1);
        expect(link.closes[0]).toEqual({ code: 1008, reason: 'session expired' });
    });

    it('close() disarms both timers', async () => {
        let authenticated = 0;
        restore = stubServerApp({
            authenticate: () => {
                authenticated += 1;
                return { id: 'u1' };
            }
        });
        const s = await start();
        const { session, link } = await connect(s, { revalidateMs: 10, maxConnectionMs: 30 });
        const after = authenticated;
        session.close();
        await new Promise((resolve) => setTimeout(resolve, 60));
        // No revalidation ran after close, and the lifetime cap never fired.
        expect(authenticated).toBe(after);
        expect(link.closes).toEqual([]);
    });
});
