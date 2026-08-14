/**
 * Client-selectable delivery windows (#247) — the resolver, and the sharing
 * invariant that licenses offering the option at all.
 *
 * The reason this is quantized rather than honoured verbatim is not taste.
 * `throttleMs` is part of the watch identity (`Activation.openWatch`) AND of
 * the cross-host coalescing key (`ClusterPlacement.#coalescedWatch`), so one
 * loop per distinct requested number would undo #121/#138/#139 — and a socket
 * may hold 256 subscriptions, which makes it a cheap way for one client to
 * multiply an actor's work. Every assertion below that counts `watchLoops` is
 * guarding that, not the arithmetic.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import { createHost, type Host } from '@sigx/actors/host';
import {
    DEFAULT_THROTTLE_POLICY,
    createActorSocketSession,
    type ActorSocketSession,
    type LiveThrottlePolicy
} from '@sigx/actors/server';
// Off the barrel on purpose (the mounts' shared internals, like
// `resolveMaxSubscriptions`), so the test reaches them where they live.
import {
    resolveClientThrottle,
    resolveThrottlePolicy,
    subscribeAll
} from '../src/server/live-endpoint';
import type { SocketReply } from '@sigx/actors/socket-wire';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const Board = defineActor({
    type: 'Board',
    allowAnonymous: true,
    state: () => ({ n: 0 }),
    methods: (ctx) => ({
        async bump() {
            ctx.state.n += 1;
            return ctx.state.n;
        },
        async current() {
            return ctx.state.n;
        }
    }),
    watches: { current: { principalIndependent: true } }
});

let host: Host | null = null;
const sessions: ActorSocketSession[] = [];

afterEach(async () => {
    for (const s of sessions) s.close();
    sessions.length = 0;
    await host?.stop();
    host = null;
});

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

async function start(): Promise<Host> {
    host = createHost({ actors: [Board], defaults: quiet });
    await host.start();
    return host;
}

async function connect(
    s: Host,
    overrides: Record<string, unknown> = {}
): Promise<{ session: ActorSocketSession; link: FakeLink }> {
    const link = fakeLink();
    const session = await createActorSocketSession({
        host: s,
        request: new Request('http://actors.test/socket'),
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

/** Open `n` subscriptions on ONE session, each asking for its own window. */
async function subscribeEach(
    s: Host,
    windows: readonly (number | undefined)[]
): Promise<{ session: ActorSocketSession; link: FakeLink }> {
    const conn = await connect(s);
    windows.forEach((w, index) => {
        conn.session.handle(
            JSON.stringify({
                i: index + 1,
                sub: { t: 'Board', k: 'b1', m: 'current', ...(w !== undefined ? { w } : {}) }
            })
        );
    });
    // Every subscription seeds with a value, so waiting for the frames is a
    // barrier on the loops actually existing.
    await until(() => conn.link.sent.filter((f) => 'v' in f).length === windows.length);
    return conn;
}

describe('resolveClientThrottle', () => {
    const policy = DEFAULT_THROTTLE_POLICY; // min 50, buckets [50, 250, 1000, 5000]

    it('leaves an absent request absent, so the watch key is unchanged', () => {
        expect(resolveClientThrottle(undefined, policy)).toBeUndefined();
    });

    it('rounds UP to the nearest bucket — never serves faster than asked', () => {
        expect(resolveClientThrottle(50, policy)).toBe(50);
        expect(resolveClientThrottle(51, policy)).toBe(250);
        expect(resolveClientThrottle(250, policy)).toBe(250);
        expect(resolveClientThrottle(251, policy)).toBe(1000);
        expect(resolveClientThrottle(1000, policy)).toBe(1000);
        expect(resolveClientThrottle(4999, policy)).toBe(5000);
    });

    it('floors at the policy min, so a client cannot ask to go faster', () => {
        expect(resolveClientThrottle(0, policy)).toBe(50);
        expect(resolveClientThrottle(1, policy)).toBe(50);
    });

    it('clamps a request slower than the whole ladder to its top', () => {
        expect(resolveClientThrottle(60_000, policy)).toBe(5000);
    });

    it('refuses a malformed window rather than defaulting it', () => {
        // Reading `'1000'` as "the default" would bill the client 20x what it
        // asked for — the exact cost the option exists to let it avoid.
        for (const bad of ['1000', -1, Number.NaN, Number.POSITIVE_INFINITY, null, {}]) {
            expect(() => resolveClientThrottle(bad, policy)).toThrow();
        }
    });

    it('honours a policy that opts into sub-default windows', () => {
        const fast: LiveThrottlePolicy = { min: 0, buckets: [0, 16, 50] };
        expect(resolveClientThrottle(0, fast)).toBe(0);
        expect(resolveClientThrottle(1, fast)).toBe(16);
    });

    it('honours a policy that refuses the feature outright', () => {
        const off: LiveThrottlePolicy = { min: 50, buckets: [50] };
        expect(resolveClientThrottle(5000, off)).toBe(50);
        expect(resolveClientThrottle(undefined, off)).toBeUndefined();
    });
});

describe('resolveThrottlePolicy', () => {
    it('accepts the default', () => {
        expect(resolveThrottlePolicy(DEFAULT_THROTTLE_POLICY)).toBe(DEFAULT_THROTTLE_POLICY);
    });

    it('refuses a ladder that is empty, unsorted, or not integer ms', () => {
        expect(() => resolveThrottlePolicy({ min: 50, buckets: [] })).toThrow(/non-empty/);
        expect(() => resolveThrottlePolicy({ min: 50, buckets: [250, 50] })).toThrow(/ascending/);
        expect(() => resolveThrottlePolicy({ min: 50, buckets: [50, 50] })).toThrow(/ascending/);
        expect(() => resolveThrottlePolicy({ min: 50, buckets: [50, 1.5] })).toThrow(/integers/);
        expect(() => resolveThrottlePolicy({ min: -1, buckets: [50] })).toThrow(/min/);
    });

    it('refuses a ladder that sits entirely below its own floor', () => {
        // Otherwise every request is served FASTER than it asked for, which
        // is the one direction the resolver promises never to go.
        expect(() => resolveThrottlePolicy({ min: 1000, buckets: [50, 250] })).toThrow(
            /faster than it asked/
        );
    });
});

describe('the sharing invariant', () => {
    it('shares one loop across requests that land in the same bucket', async () => {
        const s = await start();
        // 51..60 all round up to 250. Ten subscribers, one loop.
        await subscribeEach(s, [51, 52, 53, 54, 55, 56, 57, 58, 59, 60]);
        expect(s.stats().watchLoops).toBe(1);
    });

    it('costs one loop per distinct bucket, and never more', async () => {
        const s = await start();
        await subscribeEach(s, [50, 51, 251, 1001, 4000, 9999]);
        // 50 | 250 | 1000 | 5000 | 5000 | 5000 — four distinct buckets, and
        // the ladder is the CEILING on fragmentation however many distinct
        // numbers the clients send.
        expect(s.stats().watchLoops).toBe(4);
    });

    it('puts an absent window on the same loop as an explicit default', async () => {
        const s = await start();
        // The compatibility assertion: a client that never heard of `w` and
        // one that asks for exactly the default must not split the loop.
        // It works because `openWatch` resolves an absent option to
        // DEFAULT_WATCH_THROTTLE_MS and keys on the RESOLVED number.
        await subscribeEach(s, [undefined, 50, undefined, 50]);
        expect(s.stats().watchLoops).toBe(1);
    });

    it('is the POLICY that makes absent and explicit agree, not the field', async () => {
        // Raise the floor above the runtime default and the two part company:
        // every explicit request resolves to 250, while a client that asks
        // for nothing still gets the runtime's own 50. That is the correct
        // reading of `min` — a floor on what may be ASKED for, not a way to
        // slow down clients that ask for nothing — and the doc says so
        // rather than promising an equivalence that holds only by default.
        const s = await start();
        const conn = await connect(s, { throttlePolicy: { min: 250, buckets: [250, 1000] } });
        for (const [index, w] of [undefined, 50, 250].entries()) {
            conn.session.handle(
                JSON.stringify({
                    i: index + 1,
                    sub: { t: 'Board', k: 'b1', m: 'current', ...(w !== undefined ? { w } : {}) }
                })
            );
        }
        await until(() => conn.link.sent.filter((f) => 'v' in f).length === 3);
        // absent → 50; `50` and `250` both → 250.
        expect(s.stats().watchLoops).toBe(2);
    });

    it('answers a malformed window per subscription, leaving the socket open', async () => {
        const s = await start();
        const { session, link } = await connect(s);
        session.handle(JSON.stringify({ i: 1, sub: { t: 'Board', k: 'b1', m: 'current', w: -5 } }));
        await until(() => link.sent.length > 0);
        expect(link.sent[0]).toMatchObject({ i: 1, e: { status: 400 } });
        expect(link.closes).toEqual([]);
        // …and the connection still works afterwards.
        session.handle(JSON.stringify({ i: 2, sub: { t: 'Board', k: 'b1', m: 'current' } }));
        await until(() => link.sent.some((f) => 'i' in f && f.i === 2 && 'v' in f));
    });

    it('quantizes on the $live mount too — one field, both wires', async () => {
        const s = await start();
        const rq = {
            abortSignal: new AbortController().signal,
            request: new Request('http://test.local/_sigx/live'),
            locals: {}
        } as unknown as Parameters<typeof subscribeAll>[1];
        // Four windows, two buckets: 51|60 → 250, 900|1000 → 1000.
        const frames = subscribeAll(
            s,
            rq,
            [
                { t: 'Board', k: 'b1', m: 'current', w: 51 },
                { t: 'Board', k: 'b1', m: 'current', w: 60 },
                { t: 'Board', k: 'b1', m: 'current', w: 900 },
                { t: 'Board', k: 'b1', m: 'current', w: 1000 }
            ],
            { pingMs: 0 }
        );
        try {
            for (let i = 0; i < 4; i++) await frames.next();
            expect(s.stats().watchLoops).toBe(2);
        } finally {
            await frames.return(undefined);
        }
    });

    it('fails the whole $live request on a malformed window', async () => {
        // `$live` refuses per REQUEST where the socket refuses per
        // subscription — each mount's existing posture, unchanged.
        const s = await start();
        const rq = {
            abortSignal: new AbortController().signal,
            request: new Request('http://test.local/_sigx/live'),
            locals: {}
        } as unknown as Parameters<typeof subscribeAll>[1];
        expect(() =>
            subscribeAll(s, rq, [{ t: 'Board', k: 'b1', m: 'current', w: 'soon' }], { pingMs: 0 })
        ).toThrow(/subscription 0/);
    });

    it('closes with 1011 when the POLICY is misconfigured', async () => {
        const s = await start();
        const link = fakeLink();
        await expect(
            createActorSocketSession({
                host: s,
                request: new Request('http://actors.test/socket'),
                send: link.send,
                close: link.close,
                origin: false,
                pingMs: 0,
                throttlePolicy: { min: 50, buckets: [] }
            })
        ).rejects.toThrow(/non-empty/);
        expect(link.closes).toEqual([{ code: 1011, reason: 'session misconfigured' }]);
    });
});
