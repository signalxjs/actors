/**
 * Per-request locality counters (#52).
 *
 * `routedLocal` / `remoteDispatches` count PLACEMENT decisions and hop
 * ATTEMPTS. Neither answers the question a running fleet has — of the calls
 * being made right now, what fraction stayed on the host that received
 * them? — because `dispatcherFor` hands back the local dispatcher BEFORE the
 * routing loop whenever this host already holds the claim, so in the warm
 * steady state a local hit used to count nothing at all. This suite pins the
 * pair that does answer it: `dispatchesLocal` / `dispatchesRemote`, counted
 * once per call on the warm fast path AND the routed path, stream and watch
 * included.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor, defineWorker } from '@sigx/actors';
import { preferLocalPolicy } from '@sigx/actors/cluster';
import { addCounters, createCounters, type ClusterCounterTotals } from '../src/cluster/counters';
import { createCluster, type ClusterHarness } from './harness';

const Counter = defineActor({
    type: 'Counter',
    allowAnonymous: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async get() {
            return ctx.state.count;
        },
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        }
    }),
    streams: (ctx) => ({
        async *feed() {
            yield ctx.state.count;
        }
    })
});

const Work = defineWorker({
    type: 'Work',
    allowAnonymous: true,
    methods: () => ({
        async double(n: number) {
            return n * 2;
        }
    })
});

const call = (): { callChain: never[]; callId: string } => ({ callChain: [], callId: 'c' });

let harness: ClusterHarness | null = null;

/** Two hosts, prefer-local: the actor lands on whichever host touches it first. */
async function twoHosts(): Promise<ClusterHarness> {
    harness = await createCluster(2, { actors: [Counter, Work], policy: preferLocalPolicy() });
    return harness;
}

afterEach(async () => {
    await harness?.stop();
    harness = null;
});

describe('per-request locality counters', () => {
    it('counts the warm local fast path, which routedLocal never sees', async () => {
        const h = await twoHosts();
        const [owner] = h.placements;
        // Cold: the first touch routes, places here, and counts once on both
        // the placement counter and the per-request one.
        await h.hosts[0]!.actor(Counter, 'a').increment(1);
        expect(owner!.counters().routedLocal).toBe(1);
        expect(owner!.counters().dispatchesLocal).toBe(1);

        // Warm — the `cluster/locality-warm` bench shape: the claim is held,
        // `dispatcherFor` short-circuits, and the routing loop never runs.
        for (let i = 0; i < 10; i++) await h.hosts[0]!.actor(Counter, 'a').get();

        const c = owner!.counters();
        // The trap this pair exists to close: placement decisions stay at 1…
        expect(c.routedLocal).toBe(1);
        // …while every call was a local dispatch.
        expect(c.dispatchesLocal).toBe(11);
        expect(c.dispatchesRemote).toBe(0);
    });

    it('counts a remote call ONCE per call on the caller, never on the owner', async () => {
        const h = await twoHosts();
        await h.hosts[0]!.actor(Counter, 'b').increment(1);
        const before = h.placements[0]!.counters().dispatchesLocal;

        for (let i = 0; i < 5; i++) await h.hosts[1]!.actor(Counter, 'b').get();

        const caller = h.placements[1]!.counters();
        expect(caller.dispatchesRemote).toBe(5);
        expect(caller.dispatchesLocal).toBe(0);
        // No retries happened, so per-attempt and per-call agree here.
        expect(caller.remoteDispatches).toBe(5);

        // The owner served five INBOUND calls; those are the same logical
        // dispatches the caller already counted, so they do not count again.
        const served = h.placements[0]!.counters();
        expect(served.inboundDispatches).toBe(5);
        expect(served.dispatchesLocal).toBe(before);
        expect(served.dispatchesRemote).toBe(0);
    });

    it('reads as the locality fraction the bench derives by hand', async () => {
        const h = await twoHosts();
        // Two actors, one per host, then a warm mixed workload from host 0:
        // 8 calls to its own actor, 2 to the peer's.
        await h.hosts[0]!.actor(Counter, 'mine').increment(1);
        await h.hosts[1]!.actor(Counter, 'theirs').increment(1);
        const start = h.placements[0]!.counters();
        for (let i = 0; i < 8; i++) await h.hosts[0]!.actor(Counter, 'mine').get();
        for (let i = 0; i < 2; i++) await h.hosts[0]!.actor(Counter, 'theirs').get();
        const end = h.placements[0]!.counters();
        const local = end.dispatchesLocal - start.dispatchesLocal;
        const remote = end.dispatchesRemote - start.dispatchesRemote;
        expect(local).toBe(8);
        expect(remote).toBe(2);
        expect(local / (local + remote)).toBe(0.8);
    });

    it('counts streams and watches once per subscriber, local and remote alike', async () => {
        const h = await twoHosts();
        await h.hosts[0]!.actor(Counter, 's').increment(1);
        const ownerBefore = h.placements[0]!.counters().dispatchesLocal;

        // A local stream on the warm path.
        for await (const _ of h.hosts[0]!.dispatchStream!(
            { type: 'Counter', key: 's' },
            'feed',
            [],
            call()
        )) {
            // drain
        }
        // A local watch on the warm path.
        const localWatch = h.hosts[0]!
            .dispatchWatch!({ type: 'Counter', key: 's' }, 'get', [], call())
            [Symbol.asyncIterator]();
        expect((await localWatch.next()).value).toBe(1);
        await localWatch.return?.();
        expect(h.placements[0]!.counters().dispatchesLocal).toBe(ownerBefore + 2);

        // Two remote subscribers to the same read COALESCE onto one stream
        // (#111) — one hop, but two dispatches from the caller's side.
        const a = h.hosts[1]!
            .dispatchWatch!({ type: 'Counter', key: 's' }, 'get', [], call())
            [Symbol.asyncIterator]();
        expect((await a.next()).value).toBe(1);
        const b = h.hosts[1]!
            .dispatchWatch!({ type: 'Counter', key: 's' }, 'get', [], call())
            [Symbol.asyncIterator]();
        expect((await b.next()).value).toBe(1);
        const caller = h.placements[1]!.counters();
        expect(caller.remoteWatches).toBe(1);
        expect(caller.coalescedWatches).toBe(1);
        expect(caller.dispatchesRemote).toBe(2);
        expect(caller.dispatchesLocal).toBe(0);
        await a.return?.();
        await b.return?.();

        // A remote stream, once.
        for await (const _ of h.hosts[1]!.dispatchStream!(
            { type: 'Counter', key: 's' },
            'feed',
            [],
            call()
        )) {
            // drain
        }
        expect(h.placements[1]!.counters().dispatchesRemote).toBe(3);
        expect(h.placements[1]!.counters().remoteStreams).toBe(1);
    });

    it('counts stateless workers in neither — a worker has no locality to measure', async () => {
        const h = await twoHosts();
        for (const host of h.hosts) {
            for (let i = 0; i < 3; i++) await host.actor(Work, 'any').double(i);
        }
        for (const p of h.placements) {
            expect(p.counters().dispatchesLocal).toBe(0);
            expect(p.counters().dispatchesRemote).toBe(0);
        }
    });
});

describe('addCounters with a peer that predates the locality pair', () => {
    it('sums to the number, never NaN', () => {
        const legacy = { ...createCounters() } as Partial<ClusterCounterTotals>;
        delete legacy.dispatchesLocal;
        delete legacy.dispatchesRemote;
        const mine = createCounters();
        mine.dispatchesLocal = 7;
        mine.dispatchesRemote = 3;
        const sum = addCounters(mine, legacy as ClusterCounterTotals);
        expect(sum.dispatchesLocal).toBe(7);
        expect(sum.dispatchesRemote).toBe(3);
        for (const [key, value] of Object.entries(sum)) {
            expect(Number.isNaN(value), `${key} is NaN`).toBe(false);
        }
    });
});
