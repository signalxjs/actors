/**
 * The `$live` mount, over the real endpoint.
 *
 * What it has to prove is the multiplex and its failure isolation: many
 * subscriptions on ONE response, and a guard rejecting one of them costing
 * the others nothing. That second property is the reason a page with a
 * forbidden widget still updates everywhere else.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ServerFnError } from '@sigx/server';
import { stubServerApp } from '@sigx/server/testing';
import { defineActor, type Host } from '@sigx/actors';
import { createHost } from '@sigx/actors/host';
import { handleActorRequest } from '@sigx/actors/server';
import { DEFAULT_LIVE_PING_MS, subscribeAll } from '../src/server/live-endpoint';
import { createFanOut } from '../src/watch-core';

const ENDPOINT = 'http://actors.test/_sigx/actor';
const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

/** `Cart#slow` parks on one of these; afterEach releases them so a turn in
 *  flight never outlives its test (a sleep would hang `host.stop()`). */
const gates: Array<() => void> = [];

const Cart = defineActor({
    type: 'Cart',
    allowAnonymous: true,
    state: () => ({ items: [] as string[] }),
    methods: (ctx) => ({
        async total() {
            return ctx.state.items.length;
        },
        async add(item: string) {
            ctx.state.items.push(item);
            await ctx.save();
            return ctx.state.items.length;
        },
        async slow() {
            await new Promise<void>((resolve) => gates.push(resolve));
            return 'released';
        }
    })
});

const Secret = defineActor({
    type: 'Secret',
    // A thrown ServerFnError passes through a policy verbatim, so a
    // deliberate 401 still reads as one rather than the generic deny.
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

const running: Host[] = [];

function host(): Host {
    const s = createHost({ actors: [Cart, Secret], defaults: quiet });
    running.push(s);
    return s;
}

/** POST the subscribe symbol and return the raw NDJSON reader. */
async function subscribe(s: Host, subs: unknown[]): Promise<Response> {
    return handleActorRequest(
        new Request(`${ENDPOINT}/${encodeURIComponent('$live#subscribe')}`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args: [subs] })
        }),
        { host: s, origin: false }
    );
}

/** Read NDJSON lines until `want` chunk frames have arrived. */
async function readFrames(response: Response, want: number): Promise<unknown[]> {
    const reader = response.body!.getReader();
    const decoder = new TextDecoder();
    const frames: unknown[] = [];
    let buffer = '';
    try {
        while (frames.length < want) {
            const { value, done } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            let nl: number;
            while ((nl = buffer.indexOf('\n')) >= 0) {
                const line = buffer.slice(0, nl).trim();
                buffer = buffer.slice(nl + 1);
                if (!line) continue;
                const parsed = JSON.parse(line) as { chunk?: unknown; done?: number };
                if (parsed.chunk !== undefined) frames.push(parsed.chunk);
            }
        }
    } finally {
        await reader.cancel().catch(() => {});
    }
    return frames;
}

afterEach(async () => {
    // Drain WHILE stopping: a `slow` turn may not have parked on its gate
    // yet when this hook runs, so a single splice would strand it.
    const drain = setInterval(() => {
        for (const release of gates.splice(0)) release();
    }, 5);
    try {
        for (const s of running.splice(0)) await s.stop({ timeoutMs: 1000 });
    } finally {
        clearInterval(drain);
    }
});

describe('$live over the wire', () => {
    it('multiplexes two actors on one response, tagged by index', async () => {
        const s = host();
        await s.start();
        await s.actor(Cart, 'a').add('x');

        const response = await subscribe(s, [
            { t: 'Cart', k: 'a', m: 'total' },
            { t: 'Cart', k: 'b', m: 'total' }
        ]);
        expect(response.status).toBe(200);
        expect(response.headers.get('content-type')).toContain('x-ndjson');

        const frames = await readFrames(response, 2);
        // Both initial values, each carrying its subscription index.
        expect(frames).toContainEqual({ i: 0, v: 1 });
        expect(frames).toContainEqual({ i: 1, v: 0 });
    });

    it('pushes only the index whose actor changed', async () => {
        const s = host();
        await s.start();

        const response = await subscribe(s, [
            { t: 'Cart', k: 'c', m: 'total' },
            { t: 'Cart', k: 'd', m: 'total' }
        ]);
        const reading = readFrames(response, 3);
        // Give both subscriptions their initial frame, then move ONE actor.
        await new Promise((r) => setTimeout(r, 30));
        await s.actor(Cart, 'c').add('x');

        const frames = (await reading) as Array<{ i: number; v: unknown }>;
        expect(frames.filter((f) => f.i === 0).at(-1)).toEqual({ i: 0, v: 1 });
        // 'd' produced its initial 0 and nothing since.
        expect(frames.filter((f) => f.i === 1)).toEqual([{ i: 1, v: 0 }]);
    });

    it('a guard rejection fails ONLY its own subscription', async () => {
        const s = host();
        await s.start();

        const response = await subscribe(s, [
            { t: 'Secret', k: 's', m: 'peek' }, // 401s
            { t: 'Cart', k: 'e', m: 'total' } // must still stream
        ]);
        const frames = (await readFrames(response, 2)) as Array<Record<string, unknown>>;

        expect(frames).toContainEqual({ i: 0, e: { message: 'nope', status: 401 } });
        expect(frames).toContainEqual({ i: 1, v: 0 });
    });

    it('an unknown actor type fails only its own subscription', async () => {
        const s = host();
        await s.start();

        const response = await subscribe(s, [
            { t: 'Ghost', k: 'g', m: 'boo' },
            { t: 'Cart', k: 'f', m: 'total' }
        ]);
        const frames = (await readFrames(response, 2)) as Array<{
            i: number;
            e?: { status: number };
            v?: unknown;
        }>;

        expect(frames.find((f) => f.i === 0)?.e?.status).toBe(404);
        expect(frames).toContainEqual({ i: 1, v: 0 });
    });

    it('an unknown METHOD on a known type reports 404, the same as a unary call', async () => {
        // The status the client sees must not depend on whether it polled or
        // subscribed: a branded actor error carries no `status` of its own,
        // so without classifying it first this arrives as a masked 500 and
        // sends the caller hunting a server fault that is really its typo.
        const s = host();
        await s.start();

        const response = await subscribe(s, [
            { t: 'Cart', k: 'g', m: 'nosuchmethod' },
            { t: 'Cart', k: 'h', m: 'total' }
        ]);
        const frames = (await readFrames(response, 2)) as Array<{
            i: number;
            e?: { status: number };
            v?: unknown;
        }>;

        expect(frames.find((f) => f.i === 0)?.e?.status).toBe(404);
        expect(frames).toContainEqual({ i: 1, v: 0 });
    });

    it('a prototype member reports 404 too — a live watch is not a back door', async () => {
        // `openWatch` funnels through the same `#invoke`, so `Cart#toString`
        // used to push a real VALUE onto the live channel.
        const s = host();
        await s.start();

        const response = await subscribe(s, [
            { t: 'Cart', k: 'p', m: 'toString' },
            { t: 'Cart', k: 'q', m: 'total' }
        ]);
        const frames = (await readFrames(response, 2)) as Array<{
            i: number;
            e?: { status: number };
            v?: unknown;
        }>;

        expect(frames.find((f) => f.i === 0)?.e?.status).toBe(404);
        expect(frames).toContainEqual({ i: 1, v: 0 });
    });

    it('rejects every malformed subscription shape with a 400, never a 500', async () => {
        const s = host();
        await s.start();

        // A non-object entry must not reach the property reads and throw a
        // TypeError — the optional chaining on the first check is what makes
        // the rest safe, so this pins it.
        const shapes: unknown[][] = [
            [null],
            [5],
            ['x'],
            [{}],
            [{ t: 'Cart' }],
            [{ t: 'Cart', k: 'x' }],
            [{ t: 'Cart', k: 'x', m: 'total', a: 'not-an-array' }]
        ];
        for (const subs of shapes) {
            const response = await subscribe(s, subs);
            // The generator throws before the first frame, so the endpoint
            // still answers with a status rather than an in-band error.
            expect(response.status, JSON.stringify(subs)).toBe(400);
            const body = (await response.json()) as { error: { message: string } };
            expect(body.error.message, JSON.stringify(subs)).toContain('$live');
        }
    });

    it('a client disconnect releases the watch, even on a QUIET actor', async () => {
        // The frame loop parks on a promise, and an async generator suspended
        // at an `await` cannot observe `return()` — it is queued until the
        // generator next reaches a `yield`. For a busy actor the next value
        // resumes it within milliseconds; for a quiet one nothing ever does,
        // so the watch and its keep-alive were pinned for the life of the
        // process. This is the property a live page depends on most: tabs
        // close far more often than they mutate.
        const s = host();
        await s.start();

        const response = await subscribe(s, [{ t: 'Cart', k: 'quiet', m: 'total' }]);
        const reader = response.body!.getReader();
        await reader.read(); // the initial value — the watch is now open
        expect(s.activations().map((a) => a.keptAlive)).toEqual([true]);

        await reader.cancel();

        await vi.waitFor(
            () => {
                // Keep-alive released ⇒ idle collection can have it back.
                expect(s.activations().map((a) => a.keptAlive)).toEqual([false]);
            },
            { timeout: 1000 }
        );
    });

    it('pings a quiet connection, and the mount asks for one by default', async () => {
        // The frame exists so an idle connection survives the proxy and
        // mobile-NAT idle timeouts that sit between a browser and the host.
        // It was declared, documented and never emitted: the mount called
        // `subscribeAll` without `pingMs` and the default was 0.
        const s = host();
        await s.start();

        const rq = {
            abortSignal: new AbortController().signal,
            // The endpoint reads the traceparent header off the request…
            request: new Request('http://test.local/_sigx/live'),
            // …and lifts the context bag out of the locals store.
            locals: {}
        } as unknown as Parameters<typeof subscribeAll>[1];
        const frames = subscribeAll(s, rq, [{ t: 'Cart', k: 'ping', m: 'total' }], { pingMs: 10 });
        try {
            expect((await frames.next()).value).toEqual({ i: 0, v: 0 });
            // Nothing mutates, so the only thing that can arrive is a ping.
            expect((await frames.next()).value).toEqual({ p: 1 });
        } finally {
            await frames.return(undefined);
        }

    });

    it('the default the MOUNT inherits is not 0', () => {
        // The bug was here, not in the frame: `synthesizeLive` passes no
        // options, so every real connection — the only kind that goes
        // through the mount — took the default, and the default was 0.
        // Asserting the interval end to end would cost 30 s of wall clock
        // in CI; asserting it is switched ON costs nothing and is the
        // regression that actually happened.
        expect(DEFAULT_LIVE_PING_MS).toBeGreaterThan(0);
    });

    it('refuses a subscription array over the cap, before dispatching any of it', async () => {
        // The fan-out is one dispatchWatch per entry, started all at once,
        // and each one can force a distinct activation that then sits pinned
        // for idleAfterMs. A minimal entry is ~25 bytes, so the 1 MiB default
        // body cap buys tens of thousands of them — the largest unauthenticated
        // amplification the mount has. Guards run per subscription, but AFTER
        // the fan-out has already been kicked off, so they do not bound it.
        const s = host();
        const subs = Array.from({ length: 5 }, (_, i) => ({
            t: 'Cart',
            k: `k${i}`,
            m: 'total'
        }));
        const response = await handleActorRequest(
            new Request(`${ENDPOINT}/${encodeURIComponent('$live#subscribe')}`, {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ args: [subs] })
            }),
            { host: s, origin: false, maxLiveSubscriptions: 4 }
        );
        // 400, not a per-subscription `e` frame: the array is malformed as a
        // whole, and answering per index would mean doing the work first.
        expect(response.status).toBe(400);
        expect(await response.text()).toMatch(/too many subscriptions/i);
    });

    it('allows exactly the cap', async () => {
        const s = host();
        const subs = Array.from({ length: 4 }, (_, i) => ({
            t: 'Cart',
            k: `k${i}`,
            m: 'total'
        }));
        const response = await handleActorRequest(
            new Request(`${ENDPOINT}/${encodeURIComponent('$live#subscribe')}`, {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ args: [subs] })
            }),
            { host: s, origin: false, maxLiveSubscriptions: 4 }
        );
        expect(response.status).toBe(200);
        await response.body?.cancel();
    });

    it('two mounts with different caps do not share a cached resolver', async () => {
        // `resolverFor` memoizes per host on a composite key. A new option
        // that does not join that key would let whichever mount ran first
        // decide the cap for both — silently, and only under load.
        const s = host();
        const five = Array.from({ length: 5 }, (_, i) => ({
            t: 'Cart',
            k: `k${i}`,
            m: 'total'
        }));
        const send = (max: number): Promise<Response> =>
            handleActorRequest(
                new Request(`${ENDPOINT}/${encodeURIComponent('$live#subscribe')}`, {
                    method: 'POST',
                    headers: { 'content-type': 'application/json' },
                    body: JSON.stringify({ args: [five] })
                }),
                { host: s, origin: false, maxLiveSubscriptions: max }
            );

        const tight = await send(4);
        expect(tight.status).toBe(400);

        const loose = await send(8);
        expect(loose.status).toBe(200);
        await loose.body?.cancel();
    });

    it.each([-1, 1.5, Number.NaN])(
        'refuses maxLiveSubscriptions %s rather than silently disabling the cap',
        async (max) => {
            // The cap is applied with `max > 0`, so every one of these turned
            // it OFF before validation — a typo reaching exactly the state the
            // option exists to prevent. `0` remains the documented opt-out.
            const s = host();
            const response = await handleActorRequest(
                new Request(`${ENDPOINT}/${encodeURIComponent('$live#subscribe')}`, {
                    method: 'POST',
                    headers: { 'content-type': 'application/json' },
                    body: JSON.stringify({ args: [[{ t: 'Cart', k: 'a', m: 'total' }]] })
                }),
                { host: s, origin: false, maxLiveSubscriptions: max }
            );
            expect(response.status).toBe(500);
        }
    );

    it('$live is not addressable as an actor type', () => {
        expect(() =>
            defineActor({
                type: '$live',
                allowAnonymous: true,
                state: () => ({}),
                methods: () => ({})
            })
        ).toThrow(/must not start with "\$" or "@"/);
        expect(() =>
            defineActor({
                type: '@actor',
                allowAnonymous: true,
                state: () => ({}),
                methods: () => ({})
            })
        ).toThrow(/must not start with "\$" or "@"/);
    });
});

// ---------------------------------------------------------------------------
// The context bag on the $live edge: the per-subscription guard still runs
// against the request and may stamp it (#246), but a watch read is a SHARED
// turn and never sees a bag — not even its own edge stamp (#137). One
// subscriber or many, `$live` or in-process, `ctx.bag` inside the watched
// method is the empty bag; the stamp reaches unary calls only.

import { stampCallBag } from '@sigx/actors';

describe('$live: context bag edge capture', () => {
    it('a guard stamp never reaches the watched method — a watch turn has no bag (#137)', async () => {
        let stamped = 0;
        const Stamped = defineActor({
            type: 'LiveStamped',
            authorize: [
                (_p, rq) => {
                    stamped++;
                    stampCallBag(rq, { user: 'ada' });
                    return true;
                }
            ],
            state: () => ({}),
            methods: (ctx) => ({
                async bagOf() {
                    return { ...ctx.bag };
                }
            })
        });
        const s = createHost({ actors: [Stamped], defaults: quiet });
        running.push(s);
        await s.start();

        const response = await subscribe(s, [{ t: 'LiveStamped', k: 'a', m: 'bagOf' }]);
        expect(response.status).toBe(200);
        const frames = await readFrames(response, 1);
        // The guard ran for this subscription (authorization is per
        // subscriber); the read it admitted ran without its stamp.
        expect(stamped).toBe(1);
        expect(frames).toEqual([{ i: 0, v: {} }]);
    });
});

// ---------------------------------------------------------------------------
// The establishment deadline (#192): the same posture bound the socket
// session arms per subscription (#180), mirrored onto the `$live` mount. A
// watch read is a normal turn on a single-threaded actor, so on a busy one
// the seed can queue arbitrarily long — and the watch path deliberately has
// no runtime deadline, so before this a starved seed held its subscription
// (watch loop, change subscription and keep-alive included) open silently,
// forever.

/** Incremental NDJSON reading: `take(n)` more frames off the SAME response,
 *  so a test can assert what arrives AFTER an earlier batch. */
function frameStream(response: Response): {
    take: (want: number) => Promise<unknown[]>;
    close: () => Promise<void>;
} {
    const reader = response.body!.getReader();
    const decoder = new TextDecoder();
    const queue: unknown[] = [];
    let buffer = '';
    return {
        async take(want: number): Promise<unknown[]> {
            const out: unknown[] = [];
            while (out.length < want) {
                if (queue.length > 0) {
                    out.push(queue.shift());
                    continue;
                }
                const { value, done } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });
                let nl: number;
                while ((nl = buffer.indexOf('\n')) >= 0) {
                    const line = buffer.slice(0, nl).trim();
                    buffer = buffer.slice(nl + 1);
                    if (!line) continue;
                    const parsed = JSON.parse(line) as { chunk?: unknown };
                    if (parsed.chunk !== undefined) queue.push(parsed.chunk);
                }
            }
            return out;
        },
        close: () => reader.cancel().catch(() => {}) as Promise<void>
    };
}

describe('$live: watch establishment deadline (#192)', () => {
    let restore: (() => void) | undefined;

    afterEach(() => {
        restore?.();
        restore = undefined;
    });

    /** The direct-seam context the ping test uses — a fake `ServerFnContext`
     *  with exactly what `subscribeAll` reads off it. */
    const fakeRq = (): Parameters<typeof subscribeAll>[1] =>
        ({
            abortSignal: new AbortController().signal,
            request: new Request('http://test.local/_sigx/live'),
            locals: {}
        }) as unknown as Parameters<typeof subscribeAll>[1];

    it('a subscription whose seed starves answers a per-subscription 504 frame', async () => {
        // Identity so the mount's own gate passes; per-subscription
        // authorization still runs against each actor's own policy. 250 ms
        // rather than a tight 50: the deadline is the STARVED index's bound,
        // and the sibling's cold activation gets real-time headroom so a
        // loaded CI machine cannot 504 the control arm of the test.
        restore = stubServerApp({
            authenticate: () => ({ id: 'u1' }),
            posture: { timeoutMs: 250 }
        });
        const s = host();
        await s.start();

        // Wedge 'w': `slow` parks its turn on a gate the afterEach releases,
        // so the seed read for the same key queues behind it and cannot run.
        const wedged = s.actor(Cart, 'w').slow();
        await vi.waitFor(() => expect(gates.length).toBe(1));

        const response = await subscribe(s, [
            { t: 'Cart', k: 'w', m: 'total' }, // starves behind the parked turn
            { t: 'Cart', k: 'free', m: 'total' } // must still seed and stream
        ]);
        const stream = frameStream(response);
        const frames = (await stream.take(2)) as Array<{
            i: number;
            e?: { status: number; message: string };
            v?: unknown;
        }>;

        // The failure is the starved subscription's alone — its sibling on
        // the SAME connection delivered its seed untouched.
        expect(frames).toContainEqual({ i: 1, v: 0 });
        const starved = frames.find((f) => f.i === 0);
        expect(starved?.e?.status).toBe(504);
        expect(starved?.e?.message).toMatch(/timed out before its first value/);

        // The 504 released everything the starved seed held: its fan-out
        // subscriber is gone, so the wedged activation's keep-alive is free
        // again (the socket twin's `subscriptions === 0` in `$live` terms).
        await vi.waitFor(() => {
            const starvedActivation = s.activations().find((a) => a.key === 'w');
            expect(starvedActivation?.keptAlive).toBe(false);
            expect(starvedActivation?.watchSubscribers).toBe(0);
        });

        // And the connection is still LIVE, not merely un-aborted at the
        // instant the sibling seeded: a later mutation must still arrive on
        // this same response. A regression that aborted anything shared —
        // one controller for all subscriptions, or the stream they hang
        // off — passes the two-frame read above and fails only here.
        await s.actor(Cart, 'free').add('x');
        const later = await stream.take(1);
        expect(later).toEqual([{ i: 1, v: 1 }]);

        await stream.close();
        for (const release of gates.splice(0)) release();
        await wedged.catch(() => {});
    });

    it('a client that disconnects while its seed starves unwinds: nothing emitted, no armed timer', async () => {
        // The leak path #192 worries about, at the `subscribeAll` seam under
        // fake timers (a real host under fake timers is the deadlock trap
        // #290 documents, so the host here is a scripted stand-in): the
        // client goes away BEFORE the deadline, and the teardown must both
        // unwind the parked seed and disarm the timer it left behind.
        restore = stubServerApp({ posture: { timeoutMs: 50 } });
        vi.useFakeTimers();
        try {
            const fanOut = createFanOut(() => {});
            const fake = {
                definition: async () => Cart,
                dispatchWatch: (
                    _target: unknown,
                    _method: unknown,
                    _args: unknown,
                    call: { abortSignal?: AbortSignal }
                ) => fanOut.subscribe(call.abortSignal) // no `last` — the seed starves
            } as unknown as Host;

            const frames = subscribeAll(fake, fakeRq(), [{ t: 'Cart', k: 'w', m: 'total' }], {
                pingMs: 0
            });
            const first = frames.next();
            // Let establishment park on the starving seed, then verify the
            // deadline is armed — the ONLY timer in this scripted world.
            await vi.advanceTimersByTimeAsync(0);
            expect(fanOut.size).toBe(1);
            expect(vi.getTimerCount()).toBe(1);

            await vi.advanceTimersByTimeAsync(25); // t=25ms — before the deadline
            await frames.return(undefined); // the client is gone

            // Nothing was ever emitted, the subscriber unwound, and no timer
            // is left armed to fire into the torn-down connection.
            await expect(first).resolves.toEqual({ done: true, value: undefined });
            expect(fanOut.size).toBe(0);
            expect(vi.getTimerCount()).toBe(0);
        } finally {
            vi.useRealTimers();
        }
    });

    it('a value the abort races out of the fan-out still counts as seeding — no 504 after it', async () => {
        // The deterministic replay path: the fan-out hands a subscriber
        // whose signal is ALREADY aborted its cached `last` before `done`
        // (`watch-core.ts` checks the queue first). So when everything
        // between arming the timer and `fanOut.subscribe` is slow — a
        // directory round-trip, a busy prelude — the sequence is: deadline
        // fires → abort → subscribe → replayed value → done. The value made
        // it to the client; re-raising the stale `timedOut` after it would
        // tell a healthy widget its subscription failed.
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
        } as unknown as Host;

        const frames = subscribeAll(fake, fakeRq(), [{ t: 'Cart', k: 'w', m: 'total' }], {
            pingMs: 0
        });
        const seen: unknown[] = [];
        for await (const frame of frames) seen.push(frame);
        expect(seen).toEqual([{ i: 0, v: 7 }]);
    });

    it('the deadline signal reaches the prelude — a starving hook that honors rq.abortSignal answers 504', async () => {
        // The socket precedent runs authorization under the per-watch
        // signal (`callContext(watch.ctrl.signal)`); the `$live` mount must
        // match, or an app whose middleware/policy waits on an overloaded
        // store holds establishment open PAST the deadline — the #192
        // symptom one stage earlier. Core's pipeline never races the signal
        // itself, so the contract is exactly this: a hook that honors
        // `rq.abortSignal` observes the deadline.
        let sawAbort = false;
        const Gated = defineActor({
            type: 'GatedLive',
            authorize: [
                async (_principal, rq) => {
                    await new Promise<void>((resolve) => {
                        const unblock = (): void => {
                            sawAbort = true;
                            clearTimeout(fallback);
                            resolve();
                        };
                        if (rq.abortSignal.aborted) return unblock();
                        rq.abortSignal.addEventListener('abort', unblock, { once: true });
                        // Bounded fallback so an unfixed runtime fails the
                        // assertion below rather than hanging the suite.
                        const fallback = setTimeout(resolve, 1500);
                    });
                    return true;
                }
            ],
            state: () => ({}),
            methods: () => ({
                async peek() {
                    return 0;
                }
            })
        });
        restore = stubServerApp({
            authenticate: () => ({ id: 'u1' }),
            posture: { timeoutMs: 50 }
        });
        const s = createHost({ actors: [Gated, Cart], defaults: quiet });
        running.push(s);
        await s.start();

        const response = await subscribe(s, [
            { t: 'GatedLive', k: 'g', m: 'peek' }, // starves in its own prelude
            { t: 'Cart', k: 'free', m: 'total' } // must still seed and stream
        ]);
        const frames = (await readFrames(response, 2)) as Array<{
            i: number;
            e?: { status: number };
            v?: unknown;
        }>;

        expect(frames).toContainEqual({ i: 1, v: 0 });
        expect(frames.find((f) => f.i === 0)?.e?.status).toBe(504);
        // The hook was released BY the deadline's abort, not by its own
        // fallback — the prelude genuinely sees the composed signal.
        expect(sawAbort).toBe(true);
    });

    it('the deadline covers only establishment — a seeded subscription is never timed out', async () => {
        restore = stubServerApp({
            authenticate: () => ({ id: 'u1' }),
            posture: { timeoutMs: 50 }
        });
        const s = host();
        await s.start();

        const response = await subscribe(s, [{ t: 'Cart', k: 'calm', m: 'total' }]);
        const reading = readFrames(response, 2);
        // Outlive the timeout with the subscription seeded and idle, then
        // prove the watch still pushes rather than having been aborted.
        await new Promise((resolve) => setTimeout(resolve, 120));
        await s.actor(Cart, 'calm').add('x');

        const frames = (await reading) as Array<Record<string, unknown>>;
        expect(frames).toEqual([
            { i: 0, v: 0 },
            { i: 0, v: 1 }
        ]);
        expect(frames.every((f) => !('e' in f))).toBe(true);
    });
});
