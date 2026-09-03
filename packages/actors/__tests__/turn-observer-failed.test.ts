import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { defineActor } from '@sigx/actors';
import { createHost, memoryStorage, type ActorTurnObserver } from '@sigx/actors/host';
import type { TypeHandler } from '@sigx/serialize';

/**
 * What `ActorTurnObserver`'s `failed` does — and deliberately does NOT —
 * cover (#53).
 *
 * `failed` is "the method invocation threw". The runtime's own post-turn
 * bookkeeping (change-feed fan-out, write-behind scheduling) runs AFTER the
 * observer has fired, inside the same `finally`, and can fail on its own —
 * in which case the caller sees a rejection for a turn the observer already
 * reported as succeeded. That gap was documented as intended but never
 * pinned by a test, which is what left it open to re-litigation.
 *
 * The one piece of that bookkeeping a test can drive to throw
 * deterministically is the boundary snapshot: with a value subscriber on
 * the change feed, every mutating turn clones the state through the host
 * codec, and a type handler that refuses to serialize is a codec failure
 * that surfaces exactly there and nowhere earlier (no `ctx.save()`, so no
 * snapshot is built inside the method itself).
 */
class Marker {}

let poisoned = false;

const probe: TypeHandler<Marker, number> = {
    name: 'marker',
    tag: '$marker',
    test: (value) => value instanceof Marker,
    serialize: () => {
        if (poisoned) throw new Error('poisoned snapshot');
        return 1;
    },
    revive: () => new Marker()
};

const quiet = {
    sweepIntervalMs: 60_000,
    reminderTickMs: 60_000,
    idleAfterMs: 60_000,
    callTimeoutMs: 0
};

const Counter = defineActor({
    type: 'ObservedCounter',
    allowAnonymous: true,
    state: () => ({ n: 0, marker: new Marker() }),
    methods: (ctx) => ({
        bump() {
            ctx.state.n++;
            return ctx.state.n;
        },
        total() {
            return ctx.state.n;
        }
    }),
    streams: (ctx) => ({
        async *watch() {
            for await (const s of ctx.changes({ initial: true })) yield s.n;
        }
    })
});

/**
 * Same shape under write-behind: the boundary snapshot is still the first
 * codec touch of a mutating turn (the flush runs out of turn, after
 * `#afterTurn`), so the poison surfaces in the same place — and the flush
 * it must not cost is observable through storage (#338).
 */
const WbCounter = defineActor({
    type: 'ObservedWbCounter',
    allowAnonymous: true,
    state: () => ({ n: 0, marker: new Marker() }),
    persistence: { mode: 'write-behind', debounceMs: 5 },
    methods: (ctx) => ({
        bump() {
            ctx.state.n++;
            return ctx.state.n;
        }
    }),
    streams: (ctx) => ({
        async *watch() {
            for await (const s of ctx.changes({ initial: true })) yield s.n;
        }
    })
});

const deactivations: string[] = [];

const Quitter = defineActor({
    type: 'ObservedQuitter',
    allowAnonymous: true,
    state: () => ({ n: 0, marker: new Marker() }),
    onDeactivate(_ctx, reason) {
        deactivations.push(reason);
    },
    methods: (ctx) => ({
        bumpAndQuit() {
            ctx.state.n++;
            ctx.deactivate();
        }
    }),
    streams: (ctx) => ({
        async *watch() {
            for await (const s of ctx.changes({ initial: true })) yield s.n;
        }
    })
});

const call = { callChain: [], callId: 'test' };
const ref = { type: Counter.type, key: 'k' };

describe('turn observer: failed vs post-turn bookkeeping', () => {
    // The poison flag is module-level state shared by `probe`; reset it per
    // case so a red run cannot leak it into a later one. The host is stopped
    // in `afterEach` for the same reason — a failing `expect` mid-test must
    // not leave a stream subscription and the sweeper timers behind.
    let host: ReturnType<typeof createHost> | undefined;
    beforeEach(() => {
        poisoned = false;
        deactivations.length = 0;
    });
    afterEach(async () => {
        poisoned = false;
        await host?.stop({ timeoutMs: 2000 });
        host = undefined;
    });

    it('reports failed:false for a turn whose post-turn snapshot threw on the caller', async () => {
        const seen: Array<{ method: string; failed: boolean }> = [];
        const onTurn: ActorTurnObserver = (_ref, method, _queued, _elapsed, failed) => {
            seen.push({ method, failed });
        };
        host = createHost({
            actors: [Counter],
            storage: memoryStorage(),
            types: [probe],
            defaults: quiet
        });
        host.observeTurns(onTurn);

        // A live value subscriber: `initial: true` registers the subscription
        // in the same synchronous call that queues the seed, so once the seed
        // has arrived every mutating turn builds a boundary snapshot.
        const feed = host.dispatchStream!(ref, 'watch', [], call)[Symbol.asyncIterator]();
        expect(await feed.next()).toEqual({ value: 0, done: false });

        // Poison the codec AFTER the seed: the method body runs to completion
        // untouched, and the very next clone — the post-turn snapshot for the
        // watcher — is the first thing that throws.
        poisoned = true;
        await expect(host.dispatch(ref, 'bump', [], call)).rejects.toThrow('poisoned snapshot');

        // The caller saw an error; the observer was told the turn succeeded.
        // That is the documented contract — `failed` is "the method threw",
        // and `bump()` did not — and this pins it.
        expect(seen).toEqual([{ method: 'bump', failed: false }]);

        // The method's own work landed: the failure was bookkeeping, not
        // the turn. (`total()` mutates nothing, so it builds no snapshot.)
        poisoned = false;
        await expect(host.dispatch(ref, 'total', [], call)).resolves.toBe(1);
        expect(seen).toEqual([
            { method: 'bump', failed: false },
            { method: 'total', failed: false }
        ]);

        // The control: a method that throws is what `failed:true` means.
        await expect(host.dispatch(ref, 'missing', [], call)).rejects.toThrow();
        expect(seen.at(-1)).toEqual({ method: 'missing', failed: true });
    });

    // #338: the snapshot failure reaches the caller, but it must not take
    // the REST of the turn's bookkeeping down with it. The two pieces a test
    // can observe from outside are the write-behind flush and the
    // `ctx.deactivate()` hand-off; both used to be skipped.

    it('still schedules the write-behind flush when the boundary snapshot threw (#338)', async () => {
        const storage = memoryStorage();
        host = createHost({
            actors: [WbCounter],
            storage,
            types: [probe],
            defaults: quiet
        });
        const wbRef = { type: WbCounter.type, key: 'k' };
        const feed = host.dispatchStream!(wbRef, 'watch', [], call)[Symbol.asyncIterator]();
        expect(await feed.next()).toEqual({ value: 0, done: false });

        poisoned = true;
        await expect(host.dispatch(wbRef, 'bump', [], call)).rejects.toThrow('poisoned snapshot');
        poisoned = false;

        // The debounce was armed by the failed boundary itself: with the
        // codec healthy again the flush lands with no further turn. Before
        // the fix nothing was scheduled, and the dirty state sat until the
        // next mutating boundary (or deactivation).
        await vi.waitFor(async () => {
            const record = await storage.load(WbCounter.type, 'k');
            expect(record?.state).toMatchObject({ n: 1 });
        });

        // The failed boundary is consumed, not retried: the watcher missed
        // version 1, and the NEXT mutating boundary delivers as normal —
        // whole-state, so it carries everything the missed one did.
        await expect(host.dispatch(wbRef, 'bump', [], call)).resolves.toBe(2);
        expect(await feed.next()).toEqual({ value: 2, done: false });
    });

    it('still honours ctx.deactivate() when the boundary snapshot threw (#338)', async () => {
        host = createHost({
            actors: [Quitter],
            storage: memoryStorage(),
            types: [probe],
            defaults: quiet
        });
        const quitRef = { type: Quitter.type, key: 'k' };
        const feed = host.dispatchStream!(quitRef, 'watch', [], call)[Symbol.asyncIterator]();
        expect(await feed.next()).toEqual({ value: 0, done: false });

        poisoned = true;
        await expect(host.dispatch(quitRef, 'bumpAndQuit', [], call)).rejects.toThrow(
            'poisoned snapshot'
        );
        poisoned = false;

        // `ctx.deactivate()` is acted on in the same post-turn step that
        // threw; before the fix the request was left pending and the
        // activation stayed up until the host stopped.
        await vi.waitFor(() => {
            expect(deactivations).toEqual(['explicit']);
        });
    });

    it('still ticks the subscribers behind the failing value subscriber (#338)', async () => {
        host = createHost({
            actors: [Counter],
            storage: memoryStorage(),
            types: [probe],
            defaults: quiet
        });
        // The value subscriber FIRST, so it is the one whose snapshot throws
        // mid-fan-out ...
        const feed = host.dispatchStream!(ref, 'watch', [], call)[Symbol.asyncIterator]();
        expect(await feed.next()).toEqual({ value: 0, done: false });
        // ... and a shared watch BEHIND it in subscription order. A watch is
        // a ticks-only subscriber: its notification never touches the codec,
        // and it re-reads through a turn of its own (`total()` returns a
        // number, so the poison is invisible to that read). Before the fix
        // the fan-out aborted at the throw, and every subscriber after it
        // lost the boundary outright — the watch kept serving 0 until the
        // next mutation.
        const abort = new AbortController();
        const watch = host
            .dispatchWatch!(
                ref,
                'total',
                [],
                { ...call, abortSignal: abort.signal },
                { throttleMs: 0 }
            )
            [Symbol.asyncIterator]();
        expect(await watch.next()).toEqual({ value: 0, done: false });

        poisoned = true;
        await expect(host.dispatch(ref, 'bump', [], call)).rejects.toThrow('poisoned snapshot');
        poisoned = false;

        // Raced against a timer rather than awaited bare: a lost tick parks
        // `next()` forever, and the failure should say so, not time the test
        // out.
        const noTick = new Promise<string>((r) => setTimeout(() => r('no tick'), 1000));
        expect(await Promise.race([watch.next(), noTick])).toEqual({ value: 1, done: false });

        abort.abort();
        await watch.return?.();
    });
});
