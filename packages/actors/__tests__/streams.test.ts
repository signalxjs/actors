import { describe, expect, it } from 'vitest';
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
        }
    })
});

describe('streams', () => {
    it('enumerates stream names at definition time', () => {
        expect([...feed.streamNames].sort()).toEqual(['countTo', 'watch']);
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

    it('snapshot() is detached from live state', async () => {
        const silo = createSilo({ actors: [feed], defaults: quiet });
        await expect(silo.actor(feed, 's3').snapshotThenMutate()).resolves.toEqual({
            snapped: 0,
            live: 999
        });
    });
});
