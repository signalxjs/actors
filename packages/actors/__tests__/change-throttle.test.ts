import { beforeEach, describe, expect, it } from 'vitest';
import { defineActor, type Host } from '@sigx/actors';
import { createHost, manualScheduler, memoryStorage, type ManualScheduler } from '@sigx/actors/host';
import type { TypeHandler } from '@sigx/serialize';

/**
 * What the change feed COSTS per boundary (#129).
 *
 * A snapshot is `revive(encode(state))` over the whole state, so the thing
 * worth asserting is not a duration — it is how many times a snapshot is
 * built at all. `probe` is a type handler over a marker held in state: the
 * codec must encode it on every clone, so counting its `encode` calls counts
 * snapshots exactly, deterministically, with no clock involved.
 *
 * (It counts saves too, which is why every actor here is explicit-persistence
 * and never calls `ctx.save()`.)
 */
class Marker {}

let encodes = 0;

const probe: TypeHandler<Marker, number> = {
    name: 'marker',
    tag: '$marker',
    test: (value) => value instanceof Marker,
    serialize: () => {
        encodes++;
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
    type: 'ThrottleCounter',
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
    // Both seed with `initial: true`, which registers the subscription in the
    // same synchronous call that queues the seed. So a test that has received
    // the seed knows the feed is live, and can mutate without the settle
    // delay the older change-feed tests need.
    streams: (ctx) => ({
        /** Unthrottled: the existing contract, one snapshot per mutating turn. */
        async *watch() {
            for await (const s of ctx.changes({ initial: true })) yield s.n;
        },
        async *watchThrottled(throttleMs: number) {
            for await (const s of ctx.changes({ initial: true, throttleMs })) yield s.n;
        }
    })
});

const call = { callChain: [], callId: 'test' };
const ref = { type: Counter.type, key: 'k' };

function start(): { host: Host; clock: ManualScheduler } {
    const clock = manualScheduler();
    const host = createHost({
        actors: [Counter],
        storage: memoryStorage(),
        scheduler: clock,
        types: [probe],
        defaults: quiet
    });
    return { host, clock };
}

/**
 * One iterator, pulled on demand. Every test here interleaves "mutate" and
 * "expect the next value", so a batch helper would race the subscription's
 * own initial read.
 */
function reader(stream: AsyncIterable<unknown>): {
    next(what: string): Promise<unknown>;
    ended(what: string): Promise<boolean>;
} {
    const iterator = stream[Symbol.asyncIterator]();
    const pull = (what: string): Promise<IteratorResult<unknown>> =>
        Promise.race([
            iterator.next(),
            new Promise<never>((_, reject) =>
                setTimeout(() => reject(new Error(`${what}: nothing arrived within 2000ms`)), 2000).unref?.()
            )
        ]);
    return {
        async next(what) {
            const r = await pull(what);
            if (r.done) throw new Error(`${what}: the feed ended instead of yielding`);
            return r.value;
        },
        async ended(what) {
            return (await pull(what)).done === true;
        }
    };
}

const bump = (host: Host): Promise<unknown> => host.dispatch(ref, 'bump', [], call);

beforeEach(() => {
    encodes = 0;
});

describe('change-feed snapshot cost', () => {
    it('builds no snapshot at all for a value-free watcher', async () => {
        // `dispatchWatch` re-invokes the read method on every change and never
        // reads the feed's value (`watch.ts` destructures `{ done }` alone),
        // so it subscribes ticks-only. Before #129 this cloned the entire
        // state once per mutating turn and threw the result away.
        const { host } = start();
        const r = reader(host.dispatchWatch!(ref, 'total', [], call, { throttleMs: 0 }));

        expect(await r.next('initial read')).toBe(0);
        await bump(host);
        expect(await r.next('after 1st bump')).toBe(1);
        await bump(host);
        expect(await r.next('after 2nd bump')).toBe(2);

        // Three reads delivered, zero snapshots built.
        expect(encodes).toBe(0);
        await host.stop({ timeoutMs: 2000 });
    });

    it('builds one snapshot per mutating turn when unthrottled', async () => {
        // The control for the case above, and the pin on the default
        // contract: `ctx.changes()` with no options is unchanged.
        const { host } = start();
        const r = reader(host.dispatchStream!(ref, 'watch', [], call));

        expect(await r.next('seed')).toBe(0);
        for (const n of [1, 2, 3]) {
            await bump(host);
            expect(await r.next(`bump ${n}`)).toBe(n);
        }
        // One for the seed, one per mutating turn.
        expect(encodes).toBe(4);
        await host.stop({ timeoutMs: 2000 });
    });

    it('collapses a burst into the leading emit while the window is open', async () => {
        const { host, clock } = start();
        const r = reader(host.dispatchStream!(ref, 'watchThrottled', [1000], call));
        expect(await r.next('seed')).toBe(0);
        expect(encodes).toBe(1);

        // Five mutating turns, all inside one window.
        for (let i = 0; i < 5; i++) await bump(host);

        // The first opened the window and emitted; the other four were
        // absorbed and cost no snapshot between them.
        expect(await r.next('leading emit')).toBe(1);
        expect(encodes).toBe(2);

        // Closing the window emits the LATEST state, not the one that was
        // current when the window opened.
        clock.advance(1000);
        expect(await r.next('trailing emit')).toBe(5);
        expect(encodes).toBe(3);

        await host.stop({ timeoutMs: 2000 });
    });

    it('a quiet window closes without emitting, and does not re-arm forever', async () => {
        const { host, clock } = start();
        const r = reader(host.dispatchStream!(ref, 'watchThrottled', [1000], call));
        expect(await r.next('seed')).toBe(0);

        await bump(host);
        expect(await r.next('leading emit')).toBe(1);
        expect(encodes).toBe(2);

        // Nothing landed inside the window: no trailing emit, no snapshot,
        // and no timer left rescheduling itself.
        clock.advance(60_000);
        expect(encodes).toBe(2);

        await host.stop({ timeoutMs: 2000 });
    });

    it('flushes the final state when the actor deactivates mid-window', async () => {
        // What makes throttling safe for a job's progress feed: a run that
        // finishes inside its own window must not lose its last state.
        // `next()` drains the queue before it reports `done`.
        const { host } = start();
        const r = reader(host.dispatchStream!(ref, 'watchThrottled', [60_000], call));
        expect(await r.next('seed')).toBe(0);

        await bump(host); // leading emit, opens a 60s window
        expect(await r.next('leading emit')).toBe(1);
        await bump(host); // absorbed; the window now owes an emit

        // Deactivate without ever advancing the clock, so the window cannot
        // have closed on its own — only the deactivation flush can deliver 2.
        await host.deactivate(ref);
        expect(await r.next('deactivation flush')).toBe(2);
        expect(await r.ended('feed closes after the flush')).toBe(true);

        await host.stop({ timeoutMs: 2000 });
    });

    it('refuses a malformed throttleMs instead of silently not throttling', async () => {
        const { host } = start();
        const r = reader(host.dispatchStream!(ref, 'watchThrottled', ['50' as never], call));
        await expect(r.next('bad throttleMs')).rejects.toThrow(
            /throttleMs.*non-negative finite number/s
        );
        await host.stop({ timeoutMs: 2000 });
    });
});
