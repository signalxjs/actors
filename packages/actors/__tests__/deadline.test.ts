/**
 * The call-deadline machinery: the caller's deadline rejects the caller —
 * never the turn — without paying a host timer per call on the far
 * (production-default) path.
 */
import { describe, expect, it, vi } from 'vitest';
import { defineActor, isActorError } from '@sigx/actors';
import { createHost, type Host } from '@sigx/actors/host';
import { CallDeadlines } from '../src/host/deadlines';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000 };

function probeActor() {
    return defineActor({
        type: 'DeadlineProbe',
        allowAnonymous: true,
        state: () => ({ count: 0 }),
        methods: (ctx) => ({
            async noop() {
                return 0;
            },
            async stuck() {
                await new Promise(() => {});
            },
            async nap() {
                await new Promise((r) => setTimeout(r, 100));
                return 'rested';
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

async function stopped(host: Host): Promise<void> {
    // Stuck turns never drain; force-drop them quickly.
    await host.stop({ timeoutMs: 50 });
}

describe('call deadlines', () => {
    it('a stuck actor still times out on the far (30s default) path', async () => {
        vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
        const def = probeActor();
        const host = createHost({
            actors: [def],
            defaults: { ...quiet, callTimeoutMs: 30_000 }
        });
        try {
            const p = host.actor(def, 's').stuck();
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
            await stopped(host);
        }
    });

    it('the far path creates no per-call host timer', async () => {
        const def = probeActor();
        const host = createHost({
            actors: [def],
            defaults: { ...quiet, callTimeoutMs: 30_000 }
        });
        try {
            const client = host.actor(def, 't');
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
            await stopped(host);
        }
    });

    it('a settled call is cleaned up: the deadline passing rejects only the stuck one', async () => {
        vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
        const def = probeActor();
        const host = createHost({
            actors: [def],
            defaults: { ...quiet, callTimeoutMs: 30_000 }
        });
        try {
            const client = host.actor(def, 'mixed');
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
            await stopped(host);
        }
    });

    it('an already-expired deadline rejects with 0ms, SKIPS the turn (#384), and leaves no unhandled rejection', async () => {
        const def = probeActor();
        const host = createHost({ actors: [def], defaults: { ...quiet, callTimeoutMs: 0 } });
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
            await expect(host.dispatch(ref, 'bumpThenFail', [], expired)).rejects.toSatisfy(
                (e: unknown) =>
                    isActorError(e) && e.kind === 'call-timeout' && (e as Error).message.includes('0ms')
            );
            // Drop-on-dequeue (#384): the caller had already given up when
            // the turn reached the head of the queue, so the body never
            // runs — the state change does NOT land. (Before #384 it did:
            // "the turn is never killed" applied to queued turns too, and a
            // drowning host spent its deadline window doing work for
            // nobody.) A RUNNING turn is still never killed.
            await new Promise((r) => setTimeout(r, 20));
            await expect(
                host.dispatch(ref, 'get', [], { callChain: [], callId: 'test' })
            ).resolves.toBe(0);
            // Let any orphaned rejection surface: the skipped turn's own
            // rejection is swallowed by the race, exactly as a late
            // failure was.
            await new Promise((r) => setTimeout(r, 20));
            expect(unhandled).toEqual([]);
        } finally {
            process.off('unhandledRejection', onUnhandled);
            await stopped(host);
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
        const host = createHost({
            actors: [def],
            defaults: { ...quiet, callTimeoutMs: 30_000 }
        });
        try {
            const client = host.actor(def, 'idle');
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
            await stopped(host);
        }
    });
});

/**
 * A two-hop chain for the per-call budget rules: `outer.hop(ms)` calls
 * `inner.stuck()` through `ctx.actor(...).with({ deadlineMs: ms })` and
 * reports how the hop ended, so a test can see the INNER outcome even when
 * the outer caller is one-way or would itself time out later.
 */
function chainActors() {
    const inner = defineActor({
        type: 'DeadlineInner',
        allowAnonymous: true,
        state: () => ({}),
        methods: () => ({
            async stuck() {
                await new Promise(() => {});
            }
        })
    });
    const seen: Array<{ kind: string; after: number }> = [];
    const outer = defineActor({
        type: 'DeadlineOuter',
        allowAnonymous: true,
        state: () => ({}),
        methods: (ctx) => ({
            async hop(deadlineMs: number) {
                const started = Date.now();
                try {
                    await ctx.actor(inner, 'i').with({ deadlineMs }).stuck();
                    return { kind: 'resolved', after: Date.now() - started };
                } catch (e) {
                    const entry = {
                        kind: isActorError(e) ? e.kind : 'other',
                        after: Date.now() - started
                    };
                    seen.push(entry);
                    return entry;
                }
            }
        })
    });
    return { inner, outer, seen };
}

describe('per-call deadlineMs', () => {
    it('an explicit budget wins over the host default: 50ms against a 30s default', async () => {
        vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
        const def = probeActor();
        const host = createHost({
            actors: [def],
            defaults: { ...quiet, callTimeoutMs: 30_000 }
        });
        try {
            const p = host.actor(def, 'short').with({ deadlineMs: 50 }).stuck();
            const assertion = expect(p).rejects.toSatisfy(
                (e: unknown) =>
                    isActorError(e) &&
                    e.kind === 'call-timeout' &&
                    (e as Error).message.includes('50ms')
            );
            await vi.advanceTimersByTimeAsync(100);
            await assertion;
        } finally {
            vi.useRealTimers();
            await stopped(host);
        }
    });

    it('an explicit budget longer than the host default extends it — the issue\'s case', async () => {
        const def = probeActor();
        const host = createHost({
            actors: [def],
            defaults: { ...quiet, callTimeoutMs: 40 }
        });
        try {
            const client = host.actor(def, 'long');
            // The default still bounds an un-annotated call...
            await expect(client.nap()).rejects.toSatisfy(
                (e: unknown) => isActorError(e) && e.kind === 'call-timeout'
            );
            // ...and the per-call budget lifts exactly this one past it.
            await expect(client.with({ deadlineMs: 5_000 }).nap()).resolves.toBe('rested');
        } finally {
            await stopped(host);
        }
    });

    it('in-chain: an explicit budget shorter than the inherited deadline wins', async () => {
        vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
        const { inner, outer } = chainActors();
        const host = createHost({
            actors: [inner, outer],
            defaults: { ...quiet, callTimeoutMs: 30_000 }
        });
        try {
            const p = host.actor(outer, 'o').hop(50);
            await vi.advanceTimersByTimeAsync(100);
            // The inner hop timed out at ITS budget; the outer turn (still
            // inside the inherited 30s) observed that and returned normally.
            await expect(p).resolves.toEqual({ kind: 'call-timeout', after: 50 });
        } finally {
            vi.useRealTimers();
            await stopped(host);
        }
    });

    it('in-chain: an explicit budget never EXTENDS the inherited deadline', async () => {
        vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
        const { inner, outer, seen } = chainActors();
        const host = createHost({
            actors: [inner, outer],
            defaults: { ...quiet, callTimeoutMs: 0 }
        });
        try {
            // One-way, so the outer caller is not itself raced: what is
            // observed below is the INNER hop alone, bounded by the 100ms it
            // inherited despite asking for a minute.
            await host.actor(outer, 'o').with({ oneWay: true, deadlineMs: 100 }).hop(60_000);
            await vi.advanceTimersByTimeAsync(200);
            expect(seen).toEqual([{ kind: 'call-timeout', after: 100 }]);
        } finally {
            vi.useRealTimers();
            await stopped(host);
        }
    });

    it('in-chain: with no inherited deadline the explicit budget applies on its own', async () => {
        vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
        const { inner, outer, seen } = chainActors();
        // No host default and an un-annotated one-way entry: the chain
        // carries NO deadline, so `inherited === undefined` and the hop's
        // own budget is the only bound on the inner call.
        const host = createHost({
            actors: [inner, outer],
            defaults: { ...quiet, callTimeoutMs: 0 }
        });
        try {
            await host.actor(outer, 'o').with({ oneWay: true }).hop(50);
            await vi.advanceTimersByTimeAsync(100);
            expect(seen).toEqual([{ kind: 'call-timeout', after: 50 }]);
        } finally {
            vi.useRealTimers();
            await stopped(host);
        }
    });

    it('rejects a budget that is not a positive finite number', async () => {
        const def = probeActor();
        const host = createHost({ actors: [def], defaults: { ...quiet, callTimeoutMs: 0 } });
        try {
            // `'50'` is what a JS caller can hand over: `'50' > 0` coerces
            // true and `Date.now() + '50'` would CONCATENATE into a deadline
            // centuries away — the budget silently disabled.
            for (const deadlineMs of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, '50' as never]) {
                await expect(async () =>
                    host.actor(def, 'bad').with({ deadlineMs }).noop()
                ).rejects.toThrow(/deadlineMs/);
            }
        } finally {
            await stopped(host);
        }
    });
});
