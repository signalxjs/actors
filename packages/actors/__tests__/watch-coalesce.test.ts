/**
 * Cross-host watch coalescing (#111): live fan-out scales with HOSTS, not
 * subscribers.
 *
 * n local subscribers to a remote read share ONE cross-host stream per
 * (actor, method, throttleMs, args, principal) and fan out locally, so the
 * owner does one serialized write per emission instead of n. The counters
 * are the observable: `remoteWatches` counts streams this host holds on
 * peers, `coalescedWatches` counts attaches that joined an existing one,
 * and the owner's `inboundWatches` counts what actually crossed.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { defineActor, type ActorCallContext } from '@sigx/actors';
import { preferLocalPolicy } from '@sigx/actors/cluster';
import { canonicalKey } from '../src/watch-core';
import { createCluster, type ClusterHarness } from './harness';

let invocations = 0;

const Counter = defineActor({
    type: 'Counter',
    allowAnonymous: true,
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
    })
});

let seq = 0;
function call(principal?: string): { call: ActorCallContext; abort: AbortController } {
    const abort = new AbortController();
    return {
        abort,
        call: {
            callChain: [],
            callId: `c${seq++}`,
            ...(principal !== undefined ? { principal } : {}),
            abortSignal: abort.signal
        }
    };
}

let harness: ClusterHarness | null = null;

async function twoHosts(): Promise<ClusterHarness> {
    harness = await createCluster(2, { actors: [Counter], policy: preferLocalPolicy() });
    return harness;
}

afterEach(async () => {
    await harness?.stop();
    harness = null;
    invocations = 0;
});

/** Counter deltas around a block — the observable this whole change is. */
function deltas(h: ClusterHarness): () => {
    remote: number;
    joined: number;
    inbound: number;
} {
    const before = {
        remote: h.placements[0]!.counters().remoteWatches,
        joined: h.placements[0]!.counters().coalescedWatches,
        inbound: h.placements[1]!.counters().inboundWatches
    };
    return () => ({
        remote: h.placements[0]!.counters().remoteWatches - before.remote,
        joined: h.placements[0]!.counters().coalescedWatches - before.joined,
        inbound: h.placements[1]!.counters().inboundWatches - before.inbound
    });
}

describe('cross-host watch coalescing', () => {
    it('three subscribers cost ONE cross-host stream, and a mutation reaches all three', async () => {
        const h = await twoHosts();
        await h.hosts[1]!.actor(Counter, 'fan').increment(1);
        const delta = deltas(h);

        const ref = { type: 'Counter', key: 'fan' } as const;
        const a = h.hosts[0]!.dispatchWatch!(ref, 'read', [], call().call)[Symbol.asyncIterator]();
        const b = h.hosts[0]!.dispatchWatch!(ref, 'read', [], call().call)[Symbol.asyncIterator]();
        const c = h.hosts[0]!.dispatchWatch!(ref, 'read', [], call().call)[Symbol.asyncIterator]();
        expect((await a.next()).value).toBe(1);
        expect((await b.next()).value).toBe(1); // replayed, not re-opened
        expect((await c.next()).value).toBe(1);

        expect(delta()).toEqual({ remote: 1, joined: 2, inbound: 1 });

        await h.hosts[1]!.actor(Counter, 'fan').increment(4);
        expect((await a.next()).value).toBe(5);
        expect((await b.next()).value).toBe(5);
        expect((await c.next()).value).toBe(5);

        await a.return?.();
        await b.return?.();
        await c.return?.();
    });

    it('one subscriber aborting leaves the shared stream up for the rest', async () => {
        const h = await twoHosts();
        await h.hosts[1]!.actor(Counter, 'abort').increment(1);

        const ref = { type: 'Counter', key: 'abort' } as const;
        const first = call();
        const a = h.hosts[0]!
            .dispatchWatch!(ref, 'read', [], first.call)
            [Symbol.asyncIterator]();
        const b = h.hosts[0]!.dispatchWatch!(ref, 'read', [], call().call)[Symbol.asyncIterator]();
        await a.next();
        await b.next();

        first.abort.abort();

        // The survivor still receives — the shared stream did not die with
        // the aborted subscriber.
        await h.hosts[1]!.actor(Counter, 'abort').increment(4);
        expect((await b.next()).value).toBe(5);

        const owned = (): boolean =>
            h.hosts[1]!.activations().some((x) => x.key === 'abort' && x.keptAlive);
        expect(owned()).toBe(true);
        await b.return?.();
        await vi.waitFor(() => expect(owned()).toBe(false), { timeout: 1000 });
    });

    it("the LAST subscriber leaving closes the remote stream and releases the owner's keep-alive", async () => {
        const h = await twoHosts();
        await h.hosts[1]!.actor(Counter, 'last').increment(1);
        const delta = deltas(h);

        const ref = { type: 'Counter', key: 'last' } as const;
        const a = h.hosts[0]!.dispatchWatch!(ref, 'read', [], call().call)[Symbol.asyncIterator]();
        const b = h.hosts[0]!.dispatchWatch!(ref, 'read', [], call().call)[Symbol.asyncIterator]();
        await a.next();
        await b.next();
        expect(delta().remote).toBe(1);

        await a.return?.();
        await b.return?.();
        const owned = (): boolean =>
            h.hosts[1]!.activations().some((x) => x.key === 'last' && x.keptAlive);
        await vi.waitFor(() => expect(owned()).toBe(false), { timeout: 1000 });

        // The entry is GONE, not cached: a fresh subscriber opens stream #2.
        const c = h.hosts[0]!.dispatchWatch!(ref, 'read', [], call().call)[Symbol.asyncIterator]();
        expect((await c.next()).value).toBe(1);
        expect(delta().remote).toBe(2);
        await c.return?.();
    });

    it('a shared-stream failure fails EVERY subscriber (v1) and drops the entry', async () => {
        const h = await twoHosts();
        await h.hosts[1]!.actor(Counter, 'crash').increment(1);

        const ref = { type: 'Counter', key: 'crash' } as const;
        const a = h.hosts[0]!.dispatchWatch!(ref, 'read', [], call().call)[Symbol.asyncIterator]();
        const b = h.hosts[0]!.dispatchWatch!(ref, 'read', [], call().call)[Symbol.asyncIterator]();
        await a.next();
        await b.next();

        const aNext = a.next();
        const bNext = b.next();
        h.crash(1);

        // Both parked pulls reject — nobody is left on a dead stream.
        await expect(aNext).rejects.toThrow();
        await expect(bNext).rejects.toThrow();
    });

    it('distinct principals do NOT share a stream', async () => {
        // The owner splits identity-dependent reads per principal (#121)
        // and the relay cannot see that discovery, so the key carries the
        // encoded principal. Cross-principal sharing for reads that ignore
        // identity is #138.
        const h = await twoHosts();
        await h.hosts[1]!.actor(Counter, 'who').increment(1);
        const delta = deltas(h);

        const ref = { type: 'Counter', key: 'who' } as const;
        const alice = h.hosts[0]!
            .dispatchWatch!(ref, 'read', [], call('alice').call)
            [Symbol.asyncIterator]();
        const bob = h.hosts[0]!
            .dispatchWatch!(ref, 'read', [], call('bob').call)
            [Symbol.asyncIterator]();
        const alice2 = h.hosts[0]!
            .dispatchWatch!(ref, 'read', [], call('alice').call)
            [Symbol.asyncIterator]();
        expect((await alice.next()).value).toBe(1);
        expect((await bob.next()).value).toBe(1);
        expect((await alice2.next()).value).toBe(1);

        // Two streams (one per principal); the same-principal repeat joined.
        expect(delta()).toEqual({ remote: 2, joined: 1, inbound: 2 });

        await alice.return?.();
        await bob.return?.();
        await alice2.return?.();
    });

    it('distinct args and distinct throttles do NOT share a stream', async () => {
        const h = await twoHosts();
        await h.hosts[1]!.actor(Counter, 'keys').increment(3);
        const delta = deltas(h);

        const ref = { type: 'Counter', key: 'keys' } as const;
        const by10 = h.hosts[0]!
            .dispatchWatch!(ref, 'scaled', [10], call().call)
            [Symbol.asyncIterator]();
        const by100 = h.hosts[0]!
            .dispatchWatch!(ref, 'scaled', [100], call().call)
            [Symbol.asyncIterator]();
        const slow = h.hosts[0]!
            .dispatchWatch!(ref, 'read', [], call().call, { throttleMs: 200 })
            [Symbol.asyncIterator]();
        const dflt = h.hosts[0]!
            .dispatchWatch!(ref, 'read', [], call().call)
            [Symbol.asyncIterator]();
        const dflt2 = h.hosts[0]!
            .dispatchWatch!(ref, 'read', [], call().call, { throttleMs: 50 })
            [Symbol.asyncIterator]();
        expect((await by10.next()).value).toBe(30);
        expect((await by100.next()).value).toBe(300);
        expect((await slow.next()).value).toBe(3);
        expect((await dflt.next()).value).toBe(3);
        expect((await dflt2.next()).value).toBe(3);

        // Four streams: scaled(10), scaled(100), read@200ms, read@default —
        // and an EXPLICIT default-throttle subscriber coalesces with the
        // absent-throttle one, because the owner treats them identically.
        expect(delta()).toEqual({ remote: 4, joined: 1, inbound: 4 });

        for (const it of [by10, by100, slow, dflt, dflt2]) await it.return?.();
    });

    it('a watch opened against a just-dead owner still converges — its own retry survives teardown sweeps', async () => {
        const h = await twoHosts();
        await h.hosts[1]!.actor(Counter, 'orphan').increment(2);
        h.crash(1);

        // The route (and possibly the claim) still point at the dead host;
        // the first pull must re-route — through `#noteFailure`, which also
        // sweeps coalesced entries — and re-place rather than surface the
        // routing error to the subscriber.
        const it = h.hosts[0]!
            .dispatchWatch!({ type: 'Counter', key: 'orphan' }, 'read', [], call().call)
            [Symbol.asyncIterator]();
        const first = await it.next();
        expect(first.done).toBe(false);
        expect(typeof first.value).toBe('number');
        await it.return?.();
    });

    it('a stale duplicate drop can NOT evict a newer entry under the same key', async () => {
        const h = await twoHosts();
        await h.hosts[1]!.actor(Counter, 'latch').increment(1);
        const delta = deltas(h);
        const ref = { type: 'Counter', key: 'latch' } as const;

        // Entry #1: one subscriber, opened and fully closed.
        const s1 = call();
        const it1 = h.hosts[0]!.dispatchWatch!(ref, 'read', [], s1.call)[Symbol.asyncIterator]();
        await it1.next();
        await it1.return?.();
        expect(delta()).toEqual({ remote: 1, joined: 0, inbound: 1 });

        // Entry #2 takes the same key.
        const it2 = h.hosts[0]!.dispatchWatch!(ref, 'read', [], call().call)[Symbol.asyncIterator]();
        await it2.next();
        expect(delta().remote).toBe(2);

        // Replay the stale teardown of subscriber #1 in both spellings.
        await it1.return?.();
        s1.abort.abort();

        // Entry #2 is untouched: a third subscriber JOINS it (no new
        // stream) and mutations still arrive.
        const it3 = h.hosts[0]!.dispatchWatch!(ref, 'read', [], call().call)[Symbol.asyncIterator]();
        expect((await it3.next()).value).toBe(1);
        expect(delta()).toEqual({ remote: 2, joined: 1, inbound: 2 });

        await h.hosts[1]!.actor(Counter, 'latch').increment(4);
        expect((await it2.next()).value).toBe(5);
        expect((await it3.next()).value).toBe(5);

        await it2.return?.();
        await it3.return?.();
    });

    it('the key grammar throws on a value that escaped the codec, never collapses it', () => {
        // A `Date` has no enumerable own keys, so writing it as a record
        // would emit `o0:` — silently colliding with `{}` and any other
        // key-less instance, which is the exact wrong-answer failure the
        // injective grammar exists to prevent.
        expect(() => canonicalKey([new Date()])).toThrow(/survived the codec/);
        expect(() => canonicalKey([new Map()])).toThrow(/survived the codec/);
        class Opaque {}
        expect(() => canonicalKey([{ nested: new Opaque() }])).toThrow(/survived the codec/);
        // Plain records (and null-prototype ones, which some codecs emit)
        // still write structurally.
        expect(canonicalKey([{ a: 1 }])).toBe(canonicalKey([{ a: 1 }]));
        expect(canonicalKey([Object.assign(Object.create(null), { a: 1 })])).toBe(
            canonicalKey([{ a: 1 }])
        );
        expect(canonicalKey([{}])).not.toBe(canonicalKey([[]]));
    });

    it('locally-placed watches never touch the coalescing layer', async () => {
        const h = await twoHosts();
        await h.hosts[0]!.actor(Counter, 'home').increment(1);
        const delta = deltas(h);

        const it = h.hosts[0]!
            .dispatchWatch!({ type: 'Counter', key: 'home' }, 'read', [], call().call)
            [Symbol.asyncIterator]();
        expect((await it.next()).value).toBe(1);
        expect(delta()).toEqual({ remote: 0, joined: 0, inbound: 0 });
        await it.return?.();
    });

    it('the owner still dedupes: coalesced or not, one turn serves every subscriber', async () => {
        // Belt and braces on top of watch-cluster.test.ts's dedupe case:
        // with coalescing, ONE inbound stream reaches the owner's shared
        // loop, so the invocation count stays one however many local
        // subscribers fan out.
        const h = await twoHosts();
        await h.hosts[1]!.actor(Counter, 'turns').increment(1);
        invocations = 0;

        const ref = { type: 'Counter', key: 'turns' } as const;
        const a = h.hosts[0]!.dispatchWatch!(ref, 'read', [], call().call)[Symbol.asyncIterator]();
        const b = h.hosts[0]!.dispatchWatch!(ref, 'read', [], call().call)[Symbol.asyncIterator]();
        const c = h.hosts[0]!.dispatchWatch!(ref, 'read', [], call().call)[Symbol.asyncIterator]();
        await a.next();
        await b.next();
        await c.next();
        expect(invocations).toBe(1);

        await a.return?.();
        await b.return?.();
        await c.return?.();
    });
});
