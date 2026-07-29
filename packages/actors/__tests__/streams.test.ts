import { describe, expect, it, vi } from 'vitest';
import { defineActor } from '@sigx/actors';
import { createSilo, manualScheduler } from '@sigx/actors/silo';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

/**
 * Fail rather than hang: a teardown regression parks forever, and a test
 * that parks forever reports as a timed-out RUN, not as a failed assertion.
 */
function within<T>(promise: Promise<T>, ms: number): Promise<T> {
    return Promise.race([
        promise,
        new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error(`did not settle within ${ms}ms`)), ms).unref?.()
        )
    ]);
}

/** Stream bodies push here as they unwind — see `watchCleanup`. */
const cleanups: string[] = [];

const feed = defineActor({
    type: 'Feed',
    unguarded: true,
    state: () => ({ n: 0, big: [0] }),
    methods: (ctx) => ({
        async bump() {
            ctx.state.n++;
            return ctx.state.n;
        },
        async snapshotThenMutate() {
            const snap = ctx.snapshot();
            ctx.state.n = 999;
            return { snapped: snap.n, live: ctx.state.n };
        }
    }),
    streams: (ctx) => ({
        async *countTo(to: number) {
            for (let i = 1; i <= to; i++) yield i;
        },
        async *watch() {
            for await (const s of ctx.changes()) yield s.n;
        },
        // The collapsed form: subscribe and seed in ONE synchronous call, so
        // no mutation can land in the gap a `yield ctx.snapshot()` prologue
        // would open.
        async *watchSeeded() {
            for await (const s of ctx.changes({ initial: true })) yield s.n;
        },
        // Reads LIVE state from a detached stream body — the mistake the dev
        // guard exists to catch.
        async *watchLive() {
            yield (ctx.state as { n: number }).n;
        },
        // Parks inside changes() like `watch`, but records its unwind — the
        // disconnect contract is that the body's `finally` really runs.
        async *watchCleanup() {
            try {
                for await (const s of ctx.changes()) yield s.n;
            } finally {
                cleanups.push('watchCleanup');
            }
        },
        // Same mistake, but in the cleanup path a consumer disconnect runs.
        async *watchLiveOnCleanup() {
            try {
                yield 0;
            } finally {
                void (ctx.state as { n: number }).n;
            }
        }
    })
});

describe('streams', () => {
    it('enumerates stream names at definition time', () => {
        expect([...feed.streamNames].sort()).toEqual([
            'countTo',
            'watch',
            'watchCleanup',
            'watchLive',
            'watchLiveOnCleanup',
            'watchSeeded'
        ]);
    });

    it('a streams factory touching ctx during construction throws at define time', () => {
        expect(() =>
            defineActor({
                type: 'Bad',
                unguarded: true,
                state: () => ({ x: 1 }),
                methods: () => ({}),
                streams: (ctx) => {
                    void ctx.state; // construction-time access — forbidden
                    return {};
                }
            })
        ).toThrow(/streams.*factory.*accessed/i);
    });

    it('iterates a finite stream method end-to-end', async () => {
        const silo = createSilo({ actors: [feed], defaults: quiet });
        const out: number[] = [];
        for await (const n of silo.actor(feed, 's1').countTo(3)) out.push(n);
        expect(out).toEqual([1, 2, 3]);
    });

    it('the change feed yields a snapshot per mutating turn and releases on return()', async () => {
        const silo = createSilo({ actors: [feed], defaults: quiet });
        const client = silo.actor(feed, 's2');
        await client.bump(); // activate first

        const iterator = client.watch()[Symbol.asyncIterator]();
        const collected: number[] = [];
        // Pull the first change concurrently with mutations.
        const pulls = (async () => {
            for (let i = 0; i < 3; i++) {
                const { value, done } = await iterator.next();
                if (done) break;
                collected.push(value);
            }
        })();
        // Let the stream body reach its changes() subscription before the
        // mutations — the feed only sees changes made after subscribing.
        await new Promise((r) => setTimeout(r, 20));
        await client.bump();
        await client.bump();
        await client.bump();
        await pulls;
        expect(collected).toEqual([2, 3, 4]);
        await iterator.return?.();
        // Keep-alive released → deactivation can proceed.
        await silo.deactivateType('Feed');
        expect(silo.stats().activations).toBe(0);
    });

    it('changes({ initial: true }) seeds the current snapshot and drops nothing after it', async () => {
        const silo = createSilo({ actors: [feed], defaults: quiet });
        const client = silo.actor(feed, 'seed');
        await client.bump(); // n = 1

        const iterator = client.watchSeeded()[Symbol.asyncIterator]();
        // The seed arrives with NO settle delay and NO snapshot() prologue:
        // changes() registered the subscription in the same synchronous call
        // that queued the seed.
        expect((await iterator.next()).value).toBe(1);
        await client.bump(); // n = 2
        await client.bump(); // n = 3
        expect((await iterator.next()).value).toBe(2);
        expect((await iterator.next()).value).toBe(3);

        await iterator.return?.();
        await silo.deactivateType('Feed');
        expect(silo.stats().activations).toBe(0);
    });

    it('changes() without the option still yields no seed frame', async () => {
        const silo = createSilo({ actors: [feed], defaults: quiet });
        const client = silo.actor(feed, 'noseed');
        await client.bump(); // n = 1

        const iterator = client.watch()[Symbol.asyncIterator]();
        const first = iterator.next();
        // Give the body a turn to reach changes(), then mutate: the FIRST
        // value must be the mutation, never the pre-existing snapshot.
        await new Promise((r) => setTimeout(r, 20));
        await client.bump(); // n = 2
        expect((await first).value).toBe(2);
        await iterator.return?.();
    });

    it('warns in dev when a stream body reads live ctx.state', async () => {
        const silo = createSilo({ actors: [feed], defaults: quiet });
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        try {
            const iterator = silo.actor(feed, 'live').watchLive()[Symbol.asyncIterator]();
            await iterator.next();
            await iterator.return?.();
            expect(warn.mock.calls.flat().join(' ')).toMatch(/ctx\.state/);
        } finally {
            warn.mockRestore();
        }
    });

    it('warns in dev when a stream body reads live ctx.state while cleaning up', async () => {
        const silo = createSilo({ actors: [feed], defaults: quiet });
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        try {
            const iterator = silo
                .actor(feed, 'livefin')
                .watchLiveOnCleanup()[Symbol.asyncIterator]();
            await iterator.next();
            expect(warn.mock.calls.flat().join(' ')).not.toMatch(/ctx\.state/);
            // The read happens in the generator's finally, which only runs
            // once the consumer disconnects.
            await iterator.return?.();
            expect(warn.mock.calls.flat().join(' ')).toMatch(/ctx\.state/);
        } finally {
            warn.mockRestore();
        }
    });

    it('snapshot() is detached from live state', async () => {
        const silo = createSilo({ actors: [feed], defaults: quiet });
        await expect(silo.actor(feed, 's3').snapshotThenMutate()).resolves.toEqual({
            snapped: 0,
            live: 999
        });
    });

    it('a consumer that disconnects mid-pull can still tear the body down', async () => {
        const silo = createSilo({ actors: [feed], defaults: quiet });
        const client = silo.actor(feed, 'park');
        await client.bump(); // activate

        const iterator = client.watch()[Symbol.asyncIterator]();
        // Park the body INSIDE changes(): a pull with no change to serve.
        // This is what a live NDJSON writer is doing when its consumer goes
        // away, and it is where `gen.return()` is queued rather than run.
        const parked = iterator.next();
        await new Promise((r) => setTimeout(r, 20));

        await expect(within(iterator.return!(), 1000)).resolves.toEqual({
            value: undefined,
            done: true
        });
        await expect(within(parked, 1000)).resolves.toMatchObject({ done: true });

        // Keep-alive released → the activation is collectable again.
        await silo.deactivateType('Feed');
        expect(silo.stats().activations).toBe(0);
    });

    it('tears down a consumer that disconnects before the body has even started', async () => {
        const silo = createSilo({ actors: [feed], defaults: quiet });
        const client = silo.actor(feed, 'immediate');
        await client.bump();

        const iterator = client.watch()[Symbol.asyncIterator]();
        // Both calls land BEFORE the setup turn resolves, so the body opens
        // its feed after `return()` has already run its cleanup: whatever it
        // subscribes to has to be born closed, or the teardown misses it.
        const parked = iterator.next();
        const returned = iterator.return!();

        await expect(within(returned, 1000)).resolves.toMatchObject({ done: true });
        await expect(within(parked, 1000)).resolves.toMatchObject({ done: true });
        await silo.deactivateType('Feed');
        expect(silo.stats().activations).toBe(0);
    });

    it('runs the body’s finally when a mid-pull consumer disconnects', async () => {
        const silo = createSilo({ actors: [feed], defaults: quiet });
        const client = silo.actor(feed, 'cleanup');
        await client.bump();

        const iterator = client.watchCleanup()[Symbol.asyncIterator]();
        void iterator.next();
        await new Promise((r) => setTimeout(r, 20));
        cleanups.length = 0;

        // Closing the feed from outside must TEAR THE BODY DOWN, not abandon
        // it: `finally` blocks are where stream bodies release their own
        // resources.
        await within(iterator.return!(), 1000);
        expect(cleanups).toEqual(['watchCleanup']);
    });

    it('a disconnect closes only that body’s feed, not a sibling’s', async () => {
        const silo = createSilo({ actors: [feed], defaults: quiet });
        const client = silo.actor(feed, 'siblings');
        await client.bump(); // n = 1

        const first = client.watch()[Symbol.asyncIterator]();
        const second = client.watch()[Symbol.asyncIterator]();
        const firstPull = first.next();
        const secondPull = second.next();
        await new Promise((r) => setTimeout(r, 20));

        await within(first.return!(), 1000);
        expect((await within(firstPull, 1000)).done).toBe(true);

        // The surviving subscription is untouched: same activation, its own
        // feed, still delivering.
        await client.bump(); // n = 2
        expect((await within(secondPull, 1000)).value).toBe(2);
        await within(second.return!(), 1000);
    });

    it('releases the keep-alive when a mid-pull consumer disconnects', async () => {
        const scheduler = manualScheduler();
        const silo = createSilo({
            actors: [feed],
            scheduler,
            defaults: { idleAfterMs: 0, sweepIntervalMs: 1_000, callTimeoutMs: 0 }
        });
        await silo.start();
        try {
            const client = silo.actor(feed, 'keepalive');
            await client.bump();

            const iterator = client.watch()[Symbol.asyncIterator]();
            void iterator.next();
            await new Promise((r) => setTimeout(r, 20));
            expect(silo.activations()[0]?.keptAlive).toBe(true);

            await within(iterator.return!(), 1000);
            // The production cost of a stuck teardown is here rather than in
            // an error: the stream's ref stays held, so idle sweeping skips
            // this activation for as long as the process runs.
            expect(silo.activations()[0]?.keptAlive).toBe(false);
            scheduler.advance(1_000);
            await new Promise((r) => setTimeout(r, 20));
            expect(silo.stats().activations).toBe(0);
        } finally {
            await silo.stop({ timeoutMs: 1_000 });
        }
    });
});
