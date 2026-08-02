/**
 * The call-deadline machinery: the caller's deadline rejects the caller —
 * never the turn — without paying a host timer per call on the far
 * (production-default) path.
 */
import { describe, expect, it, vi } from 'vitest';
import { defineActor, isActorError } from '@sigx/actors';
import { createSilo, type Silo } from '@sigx/actors/silo';
import { CallDeadlines } from '../src/silo/deadlines';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000 };

function probeActor() {
    return defineActor({
        type: 'DeadlineProbe',
        unguarded: true,
        state: () => ({ count: 0 }),
        methods: (ctx) => ({
            async noop() {
                return 0;
            },
            async stuck() {
                await new Promise(() => {});
            },
            async bumpThenFail() {
                ctx.state.count++;
                throw new Error('boom');
            },
            async get() {
                return ctx.state.count;
            }
        })
    });
}

async function stopped(silo: Silo): Promise<void> {
    // Stuck turns never drain; force-drop them quickly.
    await silo.stop({ timeoutMs: 50 });
}

describe('call deadlines', () => {
    it('a stuck actor still times out on the far (30s default) path', async () => {
        vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
        const def = probeActor();
        const silo = createSilo({
            actors: [def],
            defaults: { ...quiet, callTimeoutMs: 30_000 }
        });
        try {
            const p = silo.actor(def, 's').stuck();
            const assertion = expect(p).rejects.toSatisfy(
                (e: unknown) =>
                    isActorError(e) &&
                    e.kind === 'call-timeout' &&
                    (e as Error).name === 'ActorCallTimeoutError' &&
                    (e as Error).message.includes('30000ms')
            );
            await vi.advanceTimersByTimeAsync(32_000);
            await assertion;
        } finally {
            vi.useRealTimers();
            await stopped(silo);
        }
    });

    it('the far path creates no per-call host timer', async () => {
        const def = probeActor();
        const silo = createSilo({
            actors: [def],
            defaults: { ...quiet, callTimeoutMs: 30_000 }
        });
        try {
            const client = silo.actor(def, 't');
            await client.noop(); // activation + first-call machinery priced out
            const real = globalThis.setTimeout;
            let timers = 0;
            globalThis.setTimeout = ((...args: Parameters<typeof setTimeout>) => {
                timers++;
                return real(...args);
            }) as typeof setTimeout;
            try {
                for (let i = 0; i < 50; i++) await client.noop();
            } finally {
                globalThis.setTimeout = real;
            }
            // One shared arm (plus at most a re-arm), never one per call.
            expect(timers).toBeLessThanOrEqual(2);
        } finally {
            await stopped(silo);
        }
    });

    it('a settled call is cleaned up: the deadline passing rejects only the stuck one', async () => {
        vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
        const def = probeActor();
        const silo = createSilo({
            actors: [def],
            defaults: { ...quiet, callTimeoutMs: 30_000 }
        });
        try {
            const client = silo.actor(def, 'mixed');
            const fine = client.noop();
            const stuck = client.stuck();
            const settled = expect(fine).resolves.toBe(0);
            const rejected = expect(stuck).rejects.toSatisfy(
                (e: unknown) => isActorError(e) && e.kind === 'call-timeout'
            );
            await vi.advanceTimersByTimeAsync(32_000);
            await settled;
            await rejected;
        } finally {
            vi.useRealTimers();
            await stopped(silo);
        }
    });

    it('an already-expired deadline rejects with 0ms, runs the turn, and leaves no unhandled rejection', async () => {
        const def = probeActor();
        const silo = createSilo({ actors: [def], defaults: { ...quiet, callTimeoutMs: 0 } });
        const unhandled: unknown[] = [];
        const onUnhandled = (reason: unknown): void => {
            unhandled.push(reason);
        };
        process.on('unhandledRejection', onUnhandled);
        try {
            const ref = { type: def.type, key: 'e' };
            const expired = { callChain: [], callId: 'test', deadline: Date.now() - 1 };
            // The method both mutates state and REJECTS — the caller has
            // already been rejected with the timeout, so the turn's own
            // rejection must be swallowed, not surface as unhandled.
            await expect(silo.dispatch(ref, 'bumpThenFail', [], expired)).rejects.toSatisfy(
                (e: unknown) =>
                    isActorError(e) && e.kind === 'call-timeout' && (e as Error).message.includes('0ms')
            );
            // The turn is never killed: the state change landed.
            await expect(
                silo.dispatch(ref, 'get', [], { callChain: [], callId: 'test' })
            ).resolves.toBe(1);
            // Let any orphaned rejection surface.
            await new Promise((r) => setTimeout(r, 20));
            expect(unhandled).toEqual([]);
        } finally {
            process.off('unhandledRejection', onUnhandled);
            await stopped(silo);
        }
    });

    it('splits near from far at the boundary and unrefs every timer it creates', () => {
        const ref = { type: 'DeadlineProbe', key: 'b' };
        const created: Array<{ ms: number; unrefed: boolean }> = [];
        const realSetTimeout = globalThis.setTimeout;
        const realClearTimeout = globalThis.clearTimeout;
        globalThis.setTimeout = ((_fn: () => void, ms?: number) => {
            const record = { ms: ms ?? 0, unrefed: false };
            created.push(record);
            return { unref: () => (record.unrefed = true) };
        }) as unknown as typeof setTimeout;
        globalThis.clearTimeout = (() => {}) as typeof clearTimeout;
        try {
            const deadlines = new CallDeadlines();
            const never = new Promise<unknown>(() => {});
            // Just inside the near band: an exact per-call timer for (about)
            // the remaining budget.
            void deadlines.race(never, Date.now() + 9_000, ref, 'm').catch(() => {});
            expect(created).toHaveLength(1);
            expect(created[0]!.ms).toBeGreaterThan(8_000);
            expect(created[0]!.ms).toBeLessThanOrEqual(9_000);
            // Far: the first registration arms the SHARED coarse tick...
            void deadlines.race(never, Date.now() + 30_000, ref, 'm').catch(() => {});
            expect(created).toHaveLength(2);
            expect(created[1]!.ms).toBe(1_000);
            // ...and further far registrations arm nothing at all.
            void deadlines.race(never, Date.now() + 30_000, ref, 'm').catch(() => {});
            void deadlines.race(never, Date.now() + 45_000, ref, 'm').catch(() => {});
            expect(created).toHaveLength(2);
            expect(created.every((t) => t.unrefed)).toBe(true);
            deadlines.dispose();
        } finally {
            globalThis.setTimeout = realSetTimeout;
            globalThis.clearTimeout = realClearTimeout;
        }
    });

    it('a settled far call leaves nothing behind: the registry fires clean and disarms', async () => {
        vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
        try {
            const ref = { type: 'DeadlineProbe', key: 'c' };
            const deadlines = new CallDeadlines();
            let settle!: (value: unknown) => void;
            const first = new Promise<unknown>((r) => (settle = r));
            const second = new Promise<unknown>(() => {});
            const p1 = deadlines.race(first, Date.now() + 30_000, ref, 'a');
            const p2 = deadlines.race(second, Date.now() + 30_000, ref, 'b');
            settle(1);
            await expect(p1).resolves.toBe(1);
            const rejected = expect(p2).rejects.toSatisfy(
                (e: unknown) => isActorError(e) && e.kind === 'call-timeout'
            );
            await vi.advanceTimersByTimeAsync(32_000);
            await rejected;
            // Everything fired or settled: the tick must not re-arm.
            const spy = vi.spyOn(globalThis, 'setTimeout');
            await vi.advanceTimersByTimeAsync(60_000);
            expect(spy).not.toHaveBeenCalled();
            spy.mockRestore();
            deadlines.dispose();
        } finally {
            vi.useRealTimers();
        }
    });

    it('the shared timer disarms once idle instead of ticking forever', async () => {
        vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
        const def = probeActor();
        const silo = createSilo({
            actors: [def],
            defaults: { ...quiet, callTimeoutMs: 30_000 }
        });
        try {
            const client = silo.actor(def, 'idle');
            const p = client.noop();
            await vi.advanceTimersByTimeAsync(0);
            await p;
            // Every call has settled. Advancing far past the deadline must
            // fire at most the one already-armed tick and then go quiet —
            // no timer may reject or re-arm for an empty registry.
            const spy = vi.spyOn(globalThis, 'setTimeout');
            await vi.advanceTimersByTimeAsync(120_000);
            expect(spy.mock.calls.length).toBeLessThanOrEqual(1);
            spy.mockRestore();
        } finally {
            vi.useRealTimers();
            await stopped(silo);
        }
    });
});
