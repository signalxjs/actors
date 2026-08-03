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
import { defineActor, type Host } from '@sigx/actors';
import { createHost } from '@sigx/actors/host';
import { handleActorRequest } from '@sigx/actors/server';
import { DEFAULT_LIVE_PING_MS, subscribeAll } from '../src/server/live-endpoint';

const ENDPOINT = 'http://actors.test/_sigx/actor';
const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const Cart = defineActor({
    type: 'Cart',
    unguarded: true,
    state: () => ({ items: [] as string[] }),
    methods: (ctx) => ({
        async total() {
            return ctx.state.items.length;
        },
        async add(item: string) {
            ctx.state.items.push(item);
            await ctx.save();
            return ctx.state.items.length;
        }
    })
});

const Secret = defineActor({
    type: 'Secret',
    use: [
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
    for (const s of running.splice(0)) await s.stop({ timeoutMs: 1000 });
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
                unguarded: true,
                state: () => ({}),
                methods: () => ({})
            })
        ).toThrow(/must not start with "\$" or "@"/);
        expect(() =>
            defineActor({
                type: '@actor',
                unguarded: true,
                state: () => ({}),
                methods: () => ({})
            })
        ).toThrow(/must not start with "\$" or "@"/);
    });
});

// ---------------------------------------------------------------------------
// The context bag on the $live edge (#246): the per-subscription guard runs
// against the request, and its stamp reaches the watched method's ctx.bag.

import { stampCallBag } from '@sigx/actors';

describe('$live: context bag edge capture', () => {
    it('a guard stamp is visible to the watched method', async () => {
        const Stamped = defineActor({
            type: 'LiveStamped',
            use: [
                (rq) => {
                    stampCallBag(rq, { user: 'ada' });
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
        expect(frames).toEqual([{ i: 0, v: { user: 'ada' } }]);
    });
});
