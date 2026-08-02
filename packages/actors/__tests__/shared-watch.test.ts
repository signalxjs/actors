/**
 * `createSharedWatch` against fake deps.
 *
 * The teardown contract is what needs a unit test rather than an end-to-end
 * one: whether the loop unsubscribes from the change feed is invisible from
 * outside a host — an orphaned `ctx.changes()` subscriber costs a queued
 * snapshot per later turn and shows up nowhere until the heap does.
 */
import { describe, expect, it } from 'vitest';
import { createSharedWatch } from '../src/host/watch';
import type { ActorScheduler } from '@sigx/actors';

/** Resolves immediately — these tests assert teardown, not timing. */
const immediate: ActorScheduler = {
    after: (_ms: number, run: () => void) => {
        queueMicrotask(run);
        return () => {};
    },
    every: () => () => {}
};

/**
 * A change feed that NEVER yields — a quiet actor — and records whether it
 * was closed.
 */
function quietFeed(): { iterable: AsyncIterable<unknown>; closed: () => boolean } {
    let closed = false;
    let wake: (() => void) | null = null;
    return {
        closed: () => closed,
        iterable: {
            [Symbol.asyncIterator]: () => ({
                async next(): Promise<IteratorResult<unknown>> {
                    if (closed) return { value: undefined, done: true };
                    await new Promise<void>((resolve) => {
                        wake = resolve;
                    });
                    return { value: undefined, done: true };
                },
                async return(): Promise<IteratorResult<unknown>> {
                    closed = true;
                    wake?.();
                    return { value: undefined, done: true };
                }
            })
        }
    };
}

describe('createSharedWatch teardown', () => {
    it('unsubscribes from the change feed when the last subscriber leaves', async () => {
        // The pump parks on the feed's `next()`, and only a mutation would
        // resume it — on a quiet actor, never. So without an explicit
        // `return()` the activation keeps a `ctx.changes()` subscriber for a
        // watch that has already gone, snapshotting on every later turn.
        const feed = quietFeed();
        let empties = 0;
        const shared = createSharedWatch(
            {
                invoke: () => Promise.resolve(1),
                changes: () => feed.iterable,
                keepAlive: () => () => {},
                scheduler: immediate,
                throttleMs: 0
            },
            () => empties++
        );

        const iterator = shared.subscribe()[Symbol.asyncIterator]();
        expect((await iterator.next()).value).toBe(1);
        expect(feed.closed()).toBe(false);

        await iterator.return?.();

        expect(empties).toBe(1);
        expect(shared.size).toBe(0);
        expect(feed.closed()).toBe(true);
    });

    it('drops a subscriber ONCE, even when return() races a parked next()', async () => {
        // Exactly what `$live` teardown does: `return()` while a `next()` is
        // parked. The external call drops the subscriber, and the resumed
        // `next()` then sees `done` and drops it again — re-running the
        // last-one-out cleanup. `onEmpty()` removes this watch from the
        // activation's map BY KEY, so a second pass can evict a NEWER shared
        // watch that has since taken the same key, leaving every later
        // subscriber to build its own loop with nothing to say it happened.
        const feed = quietFeed();
        let empties = 0;
        const shared = createSharedWatch(
            {
                invoke: () => Promise.resolve(1),
                changes: () => feed.iterable,
                keepAlive: () => () => {},
                scheduler: immediate,
                throttleMs: 0
            },
            () => empties++
        );

        const iterator = shared.subscribe()[Symbol.asyncIterator]();
        await iterator.next(); // the initial value
        const parked = iterator.next(); // no further value will come
        await iterator.return?.();
        await parked;

        expect(empties).toBe(1);
    });

    it('releases the keep-alive exactly once, however many subscribers leave', async () => {
        const feed = quietFeed();
        let refs = 0;
        const shared = createSharedWatch(
            {
                invoke: () => Promise.resolve('v'),
                changes: () => feed.iterable,
                keepAlive: () => {
                    refs++;
                    return () => refs--;
                },
                scheduler: immediate,
                throttleMs: 0
            },
            () => {}
        );

        const a = shared.subscribe()[Symbol.asyncIterator]();
        const b = shared.subscribe()[Symbol.asyncIterator]();
        await a.next();
        await b.next();
        expect(refs).toBe(1); // ONE ref for the shared loop, not one each

        await a.return?.();
        expect(refs).toBe(1); // still watched by b
        await b.return?.();
        expect(refs).toBe(0);
    });
});
