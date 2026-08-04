/**
 * `dispatchWatch` — a change-driven READ.
 *
 * The distinction that makes it necessary: `ctx.changes()` yields STATE, but
 * a subscriber asked for a METHOD RESULT. Only the actor can derive one from
 * the other, so the runtime re-invokes the read rather than pushing state.
 *
 * Two properties carry the design and are asserted on observed turn counts
 * rather than on internals: one loop per (method, args), and an open watch
 * keeping the activation alive.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor, type Host } from '@sigx/actors';
import { createHost, manualScheduler } from '@sigx/actors/host';

let invocations = 0;

const Cart = defineActor({
    type: 'Cart',
    allowAnonymous: true,
    state: () => ({ items: [] as string[] }),
    methods: (ctx) => ({
        async total() {
            invocations++;
            return ctx.state.items.length;
        },
        async firstN(n: number) {
            invocations++;
            return ctx.state.items.slice(0, n);
        },
        async add(item: string) {
            ctx.state.items.push(item);
            await ctx.save();
            return ctx.state.items.length;
        },
        /** Reports WHICH argument it was invoked with — the dedupe-key probe. */
        async echo(v: unknown) {
            invocations++;
            return typeof v === 'bigint' ? `${v}n` : String(v);
        },
        /** A turn that changes nothing — must not push. */
        async ping() {
            return 'pong';
        }
    })
});

const running: Host[] = [];

/** No throttle by default: these assert emissions, not timing. */
function host(defaults: Record<string, unknown> = {}): Host {
    const s = createHost({
        actors: [Cart],
        defaults: { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0, ...defaults }
    });
    running.push(s);
    return s;
}

/** Pull `n` values, then release the subscription. */
async function take(
    iterable: AsyncIterable<unknown>,
    n: number,
    between?: (i: number) => Promise<void>
): Promise<unknown[]> {
    const out: unknown[] = [];
    const iterator = iterable[Symbol.asyncIterator]();
    try {
        for (let i = 0; i < n; i++) {
            if (between) await between(i);
            const { value, done } = await iterator.next();
            if (done) break;
            out.push(value);
        }
    } finally {
        await iterator.return?.();
    }
    return out;
}

afterEach(async () => {
    for (const s of running.splice(0)) await s.stop({ timeoutMs: 1000 });
    invocations = 0;
});

describe('dispatchWatch', () => {
    it('emits the current result immediately, then one per mutating turn', async () => {
        const s = host();
        await s.start();

        const watching = s.dispatchWatch!({ type: 'Cart', key: 'w1' }, 'total', [], {
            callChain: [],
            callId: 'c'
        });
        const seen = await take(watching, 3, async (i) => {
            if (i > 0) await s.actor(Cart, 'w1').add(`item${i}`);
        });

        expect(seen).toEqual([0, 1, 2]);
    });

    it('re-runs the READ, not the state — the result is derived by the actor', async () => {
        const s = host();
        await s.start();
        await s.actor(Cart, 'w2').add('a');
        await s.actor(Cart, 'w2').add('b');

        const watching = s.dispatchWatch!({ type: 'Cart', key: 'w2' }, 'firstN', [1], {
            callChain: [],
            callId: 'c'
        });
        const seen = await take(watching, 2, async (i) => {
            if (i > 0) await s.actor(Cart, 'w2').add('c');
        });

        // `firstN(1)` — a projection no client could compute from state
        // alone without reimplementing the method.
        expect(seen).toEqual([['a'], ['a']]);
    });

    it('a non-mutating turn emits nothing', async () => {
        const s = host();
        await s.start();

        const iterator = s
            .dispatchWatch!({ type: 'Cart', key: 'w3' }, 'total', [], {
                callChain: [],
                callId: 'c'
            })
            [Symbol.asyncIterator]();
        expect((await iterator.next()).value).toBe(0);

        await s.actor(Cart, 'w3').ping();
        await s.actor(Cart, 'w3').ping();

        // Nothing queued: race the next pull against a timer.
        const next = iterator.next();
        const raced = await Promise.race([
            next.then(() => 'emitted'),
            new Promise((r) => setTimeout(() => r('quiet'), 60))
        ]);
        expect(raced).toBe('quiet');

        await s.actor(Cart, 'w3').add('x'); // …and it is still live
        expect((await next).value).toBe(1);
        await iterator.return?.();
    });

    it('DEDUPES: two identical watches run the method once per turn', async () => {
        const s = host();
        await s.start();
        await s.actor(Cart, 'w4').add('seed');
        invocations = 0;

        const ref = { type: 'Cart', key: 'w4' };
        const call = { callChain: [], callId: 'c' };
        const a = s.dispatchWatch!(ref, 'total', [], call)[Symbol.asyncIterator]();
        const b = s.dispatchWatch!(ref, 'total', [], call)[Symbol.asyncIterator]();

        expect((await a.next()).value).toBe(1);
        expect((await b.next()).value).toBe(1);
        expect(invocations).toBe(1); // ONE initial read, not two

        await s.actor(Cart, 'w4').add('again');
        expect((await a.next()).value).toBe(2);
        expect((await b.next()).value).toBe(2);
        expect(invocations).toBe(2); // ONE re-read for both subscribers

        await a.return?.();
        await b.return?.();
    });

    it('different args are different watches', async () => {
        const s = host();
        await s.start();
        await s.actor(Cart, 'w5').add('a');
        invocations = 0;

        const ref = { type: 'Cart', key: 'w5' };
        const call = { callChain: [], callId: 'c' };
        const one = s.dispatchWatch!(ref, 'firstN', [1], call)[Symbol.asyncIterator]();
        const two = s.dispatchWatch!(ref, 'firstN', [2], call)[Symbol.asyncIterator]();
        await one.next();
        await two.next();

        expect(invocations).toBe(2); // not shared — the args differ
        await one.return?.();
        await two.return?.();
    });

    it('args that JSON.stringify alike are still different watches', async () => {
        // The dedupe key decides who shares a loop, so a collision does not
        // merely waste a turn — it serves one subscriber the result of the
        // OTHER's arguments. `JSON.stringify` folds NaN onto null, so these
        // two shared a loop and `nan` was answered `'null'`.
        const s = host();
        await s.start();
        await s.actor(Cart, 'w6').add('a');
        invocations = 0;

        const ref = { type: 'Cart', key: 'w6' };
        const call = { callChain: [], callId: 'c' };
        const nan = s.dispatchWatch!(ref, 'echo', [Number.NaN], call)[Symbol.asyncIterator]();
        const nul = s.dispatchWatch!(ref, 'echo', [null], call)[Symbol.asyncIterator]();

        expect((await nan.next()).value).toBe('NaN');
        expect((await nul.next()).value).toBe('null');
        expect(invocations).toBe(2);

        await nan.return?.();
        await nul.return?.();
    });

    it('watches an argument JSON.stringify cannot serialise at all', async () => {
        // `JSON.stringify(10n)` THROWS, so a bigint argument — which the
        // codec carries perfectly well over the wire — used to make the
        // subscription itself fail.
        const s = host();
        await s.start();

        const watching = s.dispatchWatch!({ type: 'Cart', key: 'w7' }, 'echo', [10n], {
            callChain: [],
            callId: 'c'
        });
        const iterator = watching[Symbol.asyncIterator]();
        expect((await iterator.next()).value).toBe('10n');
        await iterator.return?.();
    });

    it('shares one loop for equal args written in a different key order', async () => {
        const s = host();
        await s.start();
        invocations = 0;

        const ref = { type: 'Cart', key: 'w8' };
        const call = { callChain: [], callId: 'c' };
        const a = s.dispatchWatch!(ref, 'echo', [{ x: 1, y: 2 }], call)[Symbol.asyncIterator]();
        const b = s.dispatchWatch!(ref, 'echo', [{ y: 2, x: 1 }], call)[Symbol.asyncIterator]();

        await a.next();
        await b.next();
        expect(invocations).toBe(1);

        await a.return?.();
        await b.return?.();
    });

    it('KEEPS THE ACTIVATION ALIVE against idle collection, and releases on return()', async () => {
        // The sweeper is what keep-alive guards — `deactivateType` is an
        // explicit dev/HMR hook and deactivates regardless, by design.
        const clock = manualScheduler();
        const s = createHost({
            actors: [Cart],
            scheduler: clock,
            defaults: { idleAfterMs: 1, sweepIntervalMs: 1000, reminderTickMs: 60_000 }
        });
        running.push(s);
        await s.start();

        const iterator = s
            .dispatchWatch!({ type: 'Cart', key: 'w6' }, 'total', [], {
                callChain: [],
                callId: 'c'
            })
            [Symbol.asyncIterator]();
        await iterator.next();
        expect(s.stats().activations).toBe(1);

        // Idle by wall-clock, and swept — but held open by the watch.
        await new Promise((r) => setTimeout(r, 20));
        clock.advance(1000);
        await new Promise((r) => setTimeout(r, 10));
        expect(s.stats().activations).toBe(1);

        await iterator.return?.();
        await new Promise((r) => setTimeout(r, 20));
        clock.advance(1000);
        await new Promise((r) => setTimeout(r, 10));
        expect(s.stats().activations).toBe(0);
    });

    it('the last subscriber leaving stops the loop', async () => {
        const s = host();
        await s.start();

        const ref = { type: 'Cart', key: 'w7' };
        const call = { callChain: [], callId: 'c' };
        const a = s.dispatchWatch!(ref, 'total', [], call)[Symbol.asyncIterator]();
        const b = s.dispatchWatch!(ref, 'total', [], call)[Symbol.asyncIterator]();
        await a.next();
        await b.next();

        await a.return?.(); // one leaves — the shared loop stays up for b
        await s.actor(Cart, 'w7').add('x');
        expect((await b.next()).value).toBe(1);

        await b.return?.();
        invocations = 0;
        await s.actor(Cart, 'w7').add('y');
        await new Promise((r) => setTimeout(r, 40));
        expect(invocations).toBe(0); // nothing re-reads once all are gone
    });

    it('trailing-throttles a burst into ONE read', async () => {
        const clock = manualScheduler();
        const s = createHost({
            actors: [Cart],
            scheduler: clock,
            defaults: { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 }
        });
        running.push(s);
        await s.start();

        const iterator = s
            .dispatchWatch!(
                { type: 'Cart', key: 'w8' },
                'total',
                [],
                { callChain: [], callId: 'c' },
                { throttleMs: 50 }
            )
            [Symbol.asyncIterator]();
        expect((await iterator.next()).value).toBe(0);
        invocations = 0;

        // Three mutations inside one window.
        await s.actor(Cart, 'w8').add('a');
        await s.actor(Cart, 'w8').add('b');
        await s.actor(Cart, 'w8').add('c');
        expect(invocations).toBe(0); // still inside the window

        clock.advance(50);
        expect((await iterator.next()).value).toBe(3); // one read, latest value
        expect(invocations).toBe(1);

        // The burst is SPENT. Advancing again must not produce a second
        // read — an earlier version queued one settle-and-read per change,
        // so three mutations cost three turns reporting values each other
        // superseded. Draining the clock is what exposes that.
        clock.advance(50);
        clock.advance(50);
        await new Promise((r) => setTimeout(r, 20));
        expect(invocations).toBe(1);

        // …and a NEW mutation still gets through.
        await s.actor(Cart, 'w8').add('d');
        clock.advance(50);
        expect((await iterator.next()).value).toBe(4);
        expect(invocations).toBe(2);

        await iterator.return?.();
    });

    it('return() wakes a parked next() rather than hanging it', async () => {
        const s = host();
        await s.start();

        const iterator = s
            .dispatchWatch!({ type: 'Cart', key: 'w9' }, 'total', [], {
                callChain: [],
                callId: 'c'
            })
            [Symbol.asyncIterator]();
        await iterator.next(); // initial value; the actor is now quiet

        // A parked pull, then an external teardown — which is exactly what
        // `$live` does when the connection drops.
        const parked = iterator.next();
        await iterator.return?.();

        const settled = await Promise.race([
            parked.then(() => 'settled'),
            new Promise((r) => setTimeout(() => r('HUNG'), 300))
        ]);
        expect(settled).toBe('settled');
    });

});
