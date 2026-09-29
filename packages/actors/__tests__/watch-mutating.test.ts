import { describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import { createHost, manualScheduler, memoryStorage } from '@sigx/actors/host';
import { toClientError } from '../src/server/client-error';
import { ActorWatchMutationError } from '../src/errors';

const Counter = defineActor({
    type: 'WatchMutatingCounter',
    allowAnonymous: true,
    state: () => ({ n: 0, label: null as string | null }),
    methods: (ctx) => ({
        bump() {
            ctx.state.n++;
            return ctx.state.n;
        },
        total() {
            return ctx.state.n;
        },
        /** Lazily initialises on first read, then only reads. */
        label() {
            ctx.state.label ??= `counter-${ctx.key}`;
            return `${ctx.state.label}:${ctx.state.n}`;
        }
    })
});

const call = { callChain: [], callId: 'test' };
const ref = { type: Counter.type, key: 'k' };

describe('watching a mutating method (#497)', () => {
    it('does not let a watch re-run a mutating method on its own change', async () => {
        const host = createHost({
            actors: [Counter],
            storage: memoryStorage(),
            scheduler: manualScheduler(),
            defaults: { sweepIntervalMs: 60_000, reminderTickMs: 60_000, idleAfterMs: 60_000, callTimeoutMs: 0 }
        });
        const watching = host.dispatchWatch!(ref, 'bump', [], call, { throttleMs: 0 })[Symbol.asyncIterator]();
        const pulls: unknown[] = [];
        let failure: unknown = null;
        try {
            for (let i = 0; i < 5; i++) {
                const r = await Promise.race([
                    watching.next(),
                    new Promise<IteratorResult<unknown>>((resolve) =>
                        setTimeout(() => resolve({ done: true, value: 'idle' }), 200)
                    )
                ]);
                if (r.done) break;
                pulls.push(r.value);
            }
        } catch (error) {
            failure = error;
        }
        await watching.return?.(undefined);

        // Nothing but the subscription touched the actor. Before #497 the
        // watch drove itself (7 writes in this window, unbounded in general).
        // Now the second consecutive writing read fails it: two writes, one
        // delivered value, then the error.
        expect(await host.dispatch(ref, 'total', [], call)).toBe(2);
        expect(pulls).toEqual([1]);
        expect(failure).toMatchObject({ kind: 'watch-mutation' });
        await host.stop({ timeoutMs: 2000 });
    });

    it('still watches a pure read', async () => {
        const host = createHost({
            actors: [Counter],
            storage: memoryStorage(),
            scheduler: manualScheduler(),
            defaults: { sweepIntervalMs: 60_000, reminderTickMs: 60_000, idleAfterMs: 60_000, callTimeoutMs: 0 }
        });
        const watching = host.dispatchWatch!(ref, 'total', [], call, { throttleMs: 0 })[Symbol.asyncIterator]();
        expect((await watching.next()).value).toBe(0);
        await host.dispatch(ref, 'bump', [], call);
        expect((await watching.next()).value).toBe(1);
        await watching.return?.(undefined);
        await host.stop({ timeoutMs: 2000 });
    });

    it('still watches a read that lazily initialises state once', async () => {
        const host = createHost({
            actors: [Counter],
            storage: memoryStorage(),
            scheduler: manualScheduler(),
            defaults: { sweepIntervalMs: 60_000, reminderTickMs: 60_000, idleAfterMs: 60_000, callTimeoutMs: 0 }
        });
        const watching = host.dispatchWatch!(ref, 'label', [], call, { throttleMs: 0 })[Symbol.asyncIterator]();
        expect((await watching.next()).value).toBe('counter-k:0');
        await host.dispatch(ref, 'bump', [], call);
        expect((await watching.next()).value).toBe('counter-k:1');
        await watching.return?.(undefined);
        await host.stop({ timeoutMs: 2000 });
    });

    it('reaches a client as a 400, not a masked 500', () => {
        const mapped = toClientError(new ActorWatchMutationError('T', 'bump')) as {
            status: number;
        };
        expect(mapped.status).toBe(400);
    });
});
