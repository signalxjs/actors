import { afterEach, beforeEach, describe, expect, it } from 'vitest';
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
});
