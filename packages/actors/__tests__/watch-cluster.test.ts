/**
 * Watching across a cluster hop.
 *
 * A watch is an ordinary read dispatched in watch mode, so the receiving
 * silo cannot tell from `Counter#read` alone what is being asked of it —
 * `streamNames` separates streams from calls and a watch is neither. The
 * intent therefore rides on the SYMBOL (`$watch:Counter#read`), which is
 * the one caller-supplied field the per-call HMAC covers.
 *
 * Two properties carry this and both are asserted below: a mutation on the
 * owner reaches a watcher on another host, and a disconnect releases the
 * OWNER's keep-alive. The second is the one worth being careful about — a
 * dropped connection must not pin an activation on a host that has no idea
 * the subscriber has gone.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { defineActor } from '@sigx/actors';
import { createSilo } from '@sigx/actors/silo';
import { handleActorRequest } from '@sigx/actors/server';
import { preferLocalPolicy } from '@sigx/actors/cluster';
import { createCluster, type ClusterHarness } from './harness';

let invocations = 0;

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async read() {
            invocations++;
            return ctx.state.count;
        },
        async scaled(by: number) {
            invocations++;
            return ctx.state.count * by;
        },
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        }
    }),
    streams: (ctx) => ({
        async *feed() {
            yield ctx.snapshot();
        }
    })
});

const call = (): { callChain: never[]; callId: string } => ({ callChain: [], callId: 'c' });

let harness: ClusterHarness | null = null;

async function twoSilos(): Promise<ClusterHarness> {
    harness = await createCluster(2, { actors: [Counter], policy: preferLocalPolicy() });
    return harness;
}

afterEach(async () => {
    await harness?.stop();
    harness = null;
    invocations = 0;
});

describe('dispatchWatch in a cluster', () => {
    it('watches an actor placed on the LOCAL silo', async () => {
        const h = await twoSilos();
        // prefer-local places it here on first touch.
        await h.silos[0]!.actor(Counter, 'local').increment(1);

        const iterator = h.silos[0]!
            .dispatchWatch!({ type: 'Counter', key: 'local' }, 'read', [], call())
            [Symbol.asyncIterator]();
        expect((await iterator.next()).value).toBe(1);

        await h.silos[0]!.actor(Counter, 'local').increment(4);
        expect((await iterator.next()).value).toBe(5);
        await iterator.return?.();
    });

    it('watches an actor placed on ANOTHER silo, and a mutation there arrives', async () => {
        const h = await twoSilos();
        // Place it on silo 1, then watch from silo 0 — the whole point.
        await h.silos[1]!.actor(Counter, 'remote').increment(1);

        const iterator = h.silos[0]!
            .dispatchWatch!({ type: 'Counter', key: 'remote' }, 'read', [], call())
            [Symbol.asyncIterator]();
        expect((await iterator.next()).value).toBe(1);

        await h.silos[1]!.actor(Counter, 'remote').increment(4);
        expect((await iterator.next()).value).toBe(5);
        await iterator.return?.();
    });

    it('carries the watch ARGUMENTS across the hop', async () => {
        // The options object leads the payload, so an off-by-one in the
        // argument splice would silently pass `{throttleMs}` as the method's
        // first argument — and `scaled` would answer against the wrong one.
        const h = await twoSilos();
        await h.silos[1]!.actor(Counter, 'args').increment(3);

        const iterator = h.silos[0]!
            .dispatchWatch!({ type: 'Counter', key: 'args' }, 'scaled', [10], call())
            [Symbol.asyncIterator]();
        expect((await iterator.next()).value).toBe(30);
        await iterator.return?.();
    });

    it('forwards throttleMs — it is per SUBSCRIPTION, not per actor', async () => {
        const h = await twoSilos();
        await h.silos[1]!.actor(Counter, 'throttled').increment(1);

        // Distinct throttles are distinct watches on the owner, so two
        // subscriptions that agree on everything else still run separate
        // loops. If the option were dropped on the wire they would collapse
        // into one and this count would be 1.
        const ref = { type: 'Counter', key: 'throttled' } as const;
        const slow = h.silos[0]!
            .dispatchWatch!(ref, 'read', [], call(), { throttleMs: 200 })
            [Symbol.asyncIterator]();
        const fast = h.silos[0]!
            .dispatchWatch!(ref, 'read', [], call(), { throttleMs: 5 })
            [Symbol.asyncIterator]();
        await slow.next();
        await fast.next();
        expect(invocations).toBe(2);

        await slow.return?.();
        await fast.return?.();
    });

    it('DEDUPES on the owner: two watchers on one silo cost one turn', async () => {
        // The dedupe that makes a live feed affordable has to survive the
        // hop, or fifty viewers on one edge silo become fifty turns per
        // mutation on the owner — exactly what it exists to prevent.
        const h = await twoSilos();
        await h.silos[1]!.actor(Counter, 'shared').increment(1);
        invocations = 0;

        const ref = { type: 'Counter', key: 'shared' } as const;
        const a = h.silos[0]!.dispatchWatch!(ref, 'read', [], call())[Symbol.asyncIterator]();
        const b = h.silos[0]!.dispatchWatch!(ref, 'read', [], call())[Symbol.asyncIterator]();
        expect((await a.next()).value).toBe(1);
        expect((await b.next()).value).toBe(1);
        expect(invocations).toBe(1); // ONE read on the owner, not two

        await a.return?.();
        await b.return?.();
    });

    it("a disconnect releases the OWNER's keep-alive", async () => {
        // The reason to do this deliberately rather than quickly: the owner
        // has no other way to learn the subscriber has gone, and a watch it
        // never releases pins the activation for the life of the process.
        const h = await twoSilos();
        await h.silos[1]!.actor(Counter, 'released').increment(1);

        const iterator = h.silos[0]!
            .dispatchWatch!({ type: 'Counter', key: 'released' }, 'read', [], call())
            [Symbol.asyncIterator]();
        await iterator.next();

        const owned = (): boolean =>
            h.silos[1]!.activations().some((a) => a.key === 'released' && a.keptAlive);
        expect(owned()).toBe(true);

        await iterator.return?.();

        await vi.waitFor(() => expect(owned()).toBe(false), { timeout: 1000 });
    });

    it('$live subscribes to a REMOTELY placed actor — the reason this exists', async () => {
        // The end-to-end shape: a browser holds one connection to whichever
        // silo served its page, and the actors it watches are wherever
        // placement put them. Before this, a live subscription only worked
        // for the actors that happened to land on that one host.
        const h = await twoSilos();
        await h.silos[1]!.actor(Counter, 'live').increment(2);

        const response = await handleActorRequest(
            new Request(`http://silo0.test/_sigx/actor/${encodeURIComponent('$live#subscribe')}`, {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ args: [[{ t: 'Counter', k: 'live', m: 'read' }]] })
            }),
            { silo: h.silos[0]!, origin: false }
        );
        expect(response.status).toBe(200);

        const reader = response.body!.getReader();
        const decoder = new TextDecoder();
        try {
            const { value } = await reader.read();
            expect(decoder.decode(value)).toContain('"v":2');
        } finally {
            await reader.cancel();
        }
    });

    it('still refuses loudly when a placement genuinely cannot watch', async () => {
        // `dispatchWatch` stays OPTIONAL on `ActorDispatcher`, so a custom
        // placement that does not implement it must say so rather than hang
        // or watch a second activation somewhere else. The cluster's own
        // placement can watch now; this is what is left of the old limit.
        const s = createSilo({
            actors: [Counter],
            defaults: { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 },
            placement: { dispatcherFor: () => ({ dispatch: () => Promise.resolve(0) }) }
        });
        await s.start();
        try {
            const watching = s.dispatchWatch!({ type: 'Counter', key: 'x' }, 'read', [], call());
            await expect(watching[Symbol.asyncIterator]().next()).rejects.toThrow(/cannot watch/);
        } finally {
            await s.stop({ timeoutMs: 1000 });
        }
    });

    it('refuses to watch a streams: method, on either side of the hop', async () => {
        // A watch re-invokes a read and yields its RESULT; a generator has
        // none. Refused at resolution rather than failing halfway into the
        // subscription.
        const h = await twoSilos();
        await h.silos[1]!.actor(Counter, 'streamy').increment(1);

        const watching = h.silos[0]!.dispatchWatch!(
            { type: 'Counter', key: 'streamy' },
            'feed',
            [],
            call()
        );
        await expect(watching[Symbol.asyncIterator]().next()).rejects.toThrow();
    });
});
