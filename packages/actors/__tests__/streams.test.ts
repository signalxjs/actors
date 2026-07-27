import { describe, expect, it, vi } from 'vitest';
import { defineActor } from '@sigx/actors';
import { createSilo } from '@sigx/actors/silo';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

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
});
