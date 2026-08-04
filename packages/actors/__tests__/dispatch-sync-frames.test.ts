import { describe, expect, it } from 'vitest';
import { defineActor, isActorError } from '@sigx/actors';
import { createHost } from '@sigx/actors/host';

/**
 * The contract the de-asynced dispatch frames lean on (#24): however the
 * prologue fails — shutdown, unknown type, deadlock — `dispatch()` NEVER
 * throws synchronously; every failure surfaces as a rejection. While the
 * frames were `async` functions the language guaranteed this for free; now
 * that the warm path is a plain function returning the enqueue promise, the
 * try/catch in each frame is what upholds it, and these tests are the pin.
 *
 * Deliberately NO microtask-count assertion here — `dispatch/warm-turns`
 * (bench, exact-gated) is the single source of that integer.
 */

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

function counterActor() {
    return defineActor({
        type: 'Counter',
        unguarded: true,
        state: () => ({ count: 0 }),
        methods: (ctx) => ({
            async increment(by: number) {
                ctx.state.count += by;
                return ctx.state.count;
            }
        })
    });
}

const testCall = () => ({ callChain: [], callId: 'test' });

describe('dispatch never throws synchronously', () => {
    it('host shutdown rejects, never throws', async () => {
        const host = createHost({ actors: [counterActor()], defaults: quiet });
        const ref = { type: 'Counter', key: 'a' };
        await host.dispatch(ref, 'increment', [1], testCall());
        await host.stop();
        let result!: Promise<unknown>;
        expect(() => {
            result = host.dispatch(ref, 'increment', [1], testCall());
        }).not.toThrow();
        await expect(result).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'host-shutdown'
        );
    });

    it('unknown actor type rejects, never throws', async () => {
        const host = createHost({ actors: [counterActor()], defaults: quiet });
        let result!: Promise<unknown>;
        expect(() => {
            result = host.dispatch({ type: 'Nope', key: 'x' }, 'anything', [], testCall());
        }).not.toThrow();
        await expect(result).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'activation'
        );
        await host.stop();
    });

    it('warm-path deadlock (self in call chain) rejects, never throws', async () => {
        const host = createHost({ actors: [counterActor()], defaults: quiet });
        const ref = { type: 'Counter', key: 's' };
        // Warm it so the directory slot is `active` and the fast path is live.
        await host.dispatch(ref, 'increment', [1], testCall());
        let result!: Promise<unknown>;
        expect(() => {
            result = host.dispatch(ref, 'increment', [1], {
                callChain: [`Counter${String.fromCharCode(0)}s`],
                callId: 'test'
            });
        }).not.toThrow();
        await expect(result).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'deadlock'
        );
        await host.stop();
    });
});

describe('warm dispatch ordering', () => {
    it('two unawaited dispatches to one actor run their turns in issue order', async () => {
        const order: number[] = [];
        const probe = defineActor({
            type: 'Probe',
            unguarded: true,
            state: () => ({}),
            methods: () => ({
                async mark(n: number) {
                    order.push(n);
                    return n;
                }
            })
        });
        const host = createHost({ actors: [probe], defaults: quiet });
        const ref = { type: 'Probe', key: 'p' };
        await host.dispatch(ref, 'mark', [0], testCall());
        const a = host.dispatch(ref, 'mark', [1], testCall());
        const b = host.dispatch(ref, 'mark', [2], testCall());
        await Promise.all([a, b]);
        expect(order).toEqual([0, 1, 2]);
        await host.stop();
    });
});
