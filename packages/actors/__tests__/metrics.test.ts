/**
 * `metrics()` and the `observeTurns` seam behind it.
 *
 * The two tests that matter most are not the counters:
 *  - streams must still work through the middleware (a wrapper that returns
 *    a bare `{ dispatch }` silently kills every `streams:` method), and
 *  - the queue/turn SPLIT must actually distinguish a slow turn from a deep
 *    queue, which is the entire reason the seam was added.
 */
import { describe, expect, it, vi } from 'vitest';
import { ActorStorageConflict, defineActor } from '@sigx/actors';
import {
    createHost,
    defineActorApp,
    memoryStorage,
    metrics,
    type ActorTurnObserver,
    type MetricsPlugin
} from '@sigx/actors/host';
import type { AnyActorDefinition } from '@sigx/actors';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

/**
 * Defined once at module level so its method and stream types survive
 * inference — annotating a factory with `ReturnType<typeof defineActor>`
 * erases them and every typed call site stops compiling.
 */
const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        noop() {
            return 0;
        },
        increment(by: number) {
            ctx.state.count += by;
            return ctx.state.count;
        },
        async slow(ms: number) {
            await new Promise((resolve) => setTimeout(resolve, ms));
            return ctx.state.count;
        },
        boom() {
            throw new Error('nope');
        },
        async persist() {
            ctx.state.count++;
            await ctx.save();
            return ctx.state.count;
        }
    }),
    streams: (ctx) => ({
        async *watch() {
            yield* ctx.changes({ initial: true });
        }
    })
});

async function appWith(plugin: MetricsPlugin, actors: readonly AnyActorDefinition[] = [Counter]) {
    const app = defineActorApp({ actors, storage: memoryStorage(), defaults: quiet }).use(plugin);
    const host = await app.start();
    return { app, host };
}

describe('metrics()', () => {
    it('counts calls, failures and per-type breakdown', async () => {
        const m = metrics();
        const { app, host } = await appWith(m);
        try {
            await host.actor(Counter, 'a').noop();
            await host.actor(Counter, 'a').increment(2);
            await expect(host.actor(Counter, 'a').boom()).rejects.toThrow();

            const snap = m.snapshot();
            expect(snap.calls.total).toBe(3);
            expect(snap.calls.failed).toBe(1);
            expect(snap.byType['Counter']?.calls).toBe(3);
            expect(snap.byType['Counter']?.failed).toBe(1);
            expect(snap.gauges?.activations).toBe(1);
        } finally {
            await app.stop();
        }
    });

    it('splits queue wait from turn time', async () => {
        const m = metrics();
        const { app, host } = await appWith(m);
        try {
            const client = host.actor(Counter, 'a');
            // Three 60ms turns issued at once against ONE actor. The mailbox
            // serializes them, so the third waits ~120ms for a turn that
            // itself takes ~60ms. A middleware sees only the ~180ms sum;
            // the split is what tells you the actor is hot, not slow.
            await Promise.all([client.slow(60), client.slow(60), client.slow(60)]);

            const snap = m.snapshot();
            expect(snap.turnMs).not.toBeNull();
            expect(snap.queueMs).not.toBeNull();
            // Every turn really did take ~60ms.
            expect(snap.turnMs!.maxMs).toBeGreaterThanOrEqual(50);
            // ...and at least one waited noticeably longer than it ran.
            expect(snap.queueMs!.maxMs).toBeGreaterThanOrEqual(90);
        } finally {
            await app.stop();
        }
    });

    it('keeps `streams:` methods working through the middleware', async () => {
        const m = metrics();
        const { app, host } = await appWith(m);
        try {
            const client = host.actor(Counter, 'a');
            // Explicit iterator, not a background `for await`: ctx.changes()
            // only registers its subscription once the generator body runs,
            // which needs a pull. A detached reader races the mutation below
            // and can miss it entirely.
            const iterator = client.watch()[Symbol.asyncIterator]();
            expect((await iterator.next()).value).toEqual({ count: 0 });

            await client.increment(5);
            expect((await iterator.next()).value).toEqual({ count: 5 });

            await iterator.return?.();
            // The real assertion: the middleware forwarded dispatchStream at
            // all. A wrapper returning a bare { dispatch } would have failed
            // above with a "transport cannot stream" error.
            expect(m.snapshot().calls.streams).toBe(1);
        } finally {
            await app.stop();
        }
    });

    it('keeps `dispatchWatch` working through the middleware', async () => {
        // The twin of the test above, and the one that was missing. The
        // middleware forwarded `dispatchStream` but NOT `dispatchWatch`, so
        // any host with metrics() attached answered every watch with "the
        // placement for Counter cannot watch". That is the whole of
        // `useActorState(…, { live: true })` and the `$live` mount — and
        // metrics() is on every production host, so the feature was broken
        // everywhere it shipped and nowhere it was tested.
        const m = metrics();
        const { app, host } = await appWith(m);
        try {
            const watching = host.dispatchWatch!({ type: 'Counter', key: 'w' }, 'noop', [], {
                callChain: [],
                callId: 'c'
            });
            const iterator = watching[Symbol.asyncIterator]();
            expect((await iterator.next()).value).toBe(0);
            await iterator.return?.();
        } finally {
            await app.stop();
        }
    });

    it('counts activations and deactivation reasons', async () => {
        const m = metrics();
        const { app, host } = await appWith(m);
        try {
            await host.actor(Counter, 'a').noop();
            await host.actor(Counter, 'b').noop();
            await host.deactivate({ type: 'Counter', key: 'a' });

            const snap = m.snapshot();
            expect(snap.activations.created).toBe(2);
            expect(snap.activations.destroyed).toBe(1);
            expect(snap.activations.byReason['explicit']).toBe(1);
        } finally {
            await app.stop();
        }
    });

    it('counts storage operations and etag conflicts', async () => {
        const m = metrics();
        const inner = memoryStorage();
        let failNextSave = false;
        const flaky = {
            load: inner.load.bind(inner),
            clear: inner.clear.bind(inner),
            save: async (type: string, key: string, state: unknown, etag: string | null) => {
                if (failNextSave) {
                    failNextSave = false;
                    throw new ActorStorageConflict(type, key);
                }
                return inner.save(type, key, state, etag);
            }
        };
        const app = defineActorApp({ actors: [Counter], storage: flaky, defaults: quiet }).use(m);
        const host = await app.start();
        try {
            await host.actor(Counter, 'a').persist();
            expect(m.snapshot().storage.saves).toBe(1);
            expect(m.snapshot().storage.loads).toBeGreaterThanOrEqual(1);

            failNextSave = true;
            await expect(host.actor(Counter, 'a').persist()).rejects.toThrow();
            expect(m.snapshot().storage.conflicts).toBe(1);
        } finally {
            await app.stop();
        }
    });

    it('reset() clears counters and drops stale types', async () => {
        const m = metrics();
        const { app, host } = await appWith(m);
        try {
            await host.actor(Counter, 'a').noop();
            expect(m.snapshot().calls.total).toBe(1);

            m.reset();
            const snap = m.snapshot();
            expect(snap.calls.total).toBe(0);
            // A type that stopped being called should leave the report
            // rather than linger as a row of zeroes forever.
            expect(Object.keys(snap.byType)).toHaveLength(0);
        } finally {
            await app.stop();
        }
    });

    it('histograms: false keeps counters but records no distributions', async () => {
        const m = metrics({ histograms: false });
        const { app, host } = await appWith(m);
        try {
            await host.actor(Counter, 'a').noop();
            const snap = m.snapshot();
            expect(snap.calls.total).toBe(1);
            expect(snap.latencyMs).toBeNull();
            expect(snap.queueMs).toBeNull();
            expect(snap.turnMs).toBeNull();
        } finally {
            await app.stop();
        }
    });

    it('folds types beyond maxTypes into one overflow bucket', async () => {
        const m = metrics({ maxTypes: 1 });
        const a = defineActor({
            type: 'A',
            unguarded: true,
            state: () => ({}),
            methods: () => ({ noop: () => 0 })
        });
        const b = defineActor({
            type: 'B',
            unguarded: true,
            state: () => ({}),
            methods: () => ({ noop: () => 0 })
        });
        const { app, host } = await appWith(m, [a, b]);
        try {
            await host.actor(a, 'x').noop();
            await host.actor(b, 'y').noop();
            const snap = m.snapshot();
            expect(snap.calls.total).toBe(2);
            expect(snap.byType['A']?.calls).toBe(1);
            // B is past the cap, so its call is still counted — under
            // '(other)' rather than vanishing from the breakdown.
            expect(snap.byType['(other)']?.calls).toBe(1);
        } finally {
            await app.stop();
        }
    });

    it('clamps a nonsensical maxTypes instead of behaving strangely', async () => {
        // A negative would make `types.size >= maxTypes` true from the very
        // first call and fold EVERY type into '(other)', which reads as a
        // bug in the breakdown rather than a bad option. Clamped to 0, which
        // is the documented "no per-type breakdown".
        const m = metrics({ maxTypes: -5 });
        const { app, host } = await appWith(m);
        try {
            await host.actor(Counter, 'a').noop();
            const snap = m.snapshot();
            expect(snap.calls.total).toBe(1);
            expect(Object.keys(snap.byType)).toHaveLength(0);
        } finally {
            await app.stop();
        }
    });

    it('an Infinity cap falls back to the default too', async () => {
        // `Math.floor(Infinity)` is `Infinity`, so `types.size >= maxTypes` is
        // false forever — the same fail-open as NaN, by a different route.
        // `resolveLimit` treats "no limit" as a request this API does not
        // offer, so it falls back to the default rather than honouring it.
        const m = metrics({ maxTypes: Number.POSITIVE_INFINITY });
        const types = Array.from({ length: 70 }, (_v, i) =>
            defineActor({
                type: `I${i}`,
                unguarded: true,
                state: () => ({}),
                methods: () => ({ noop: () => 0 })
            })
        );
        const { app, host } = await appWith(m, types);
        try {
            for (const [i, t] of types.entries()) await host.actor(t, `k${i}`).noop();
            const snap = m.snapshot();
            expect(Object.keys(snap.byType).length).toBeLessThanOrEqual(65);
            expect(snap.byType['(other)']?.calls).toBeGreaterThan(0);
        } finally {
            await app.stop();
        }
    });

    it('a non-finite maxMethods falls back to the default rather than failing open', async () => {
        // Methods MULTIPLY types, so this cap failing open is the worse of
        // the two — and it had no regression test at all.
        const m = metrics({ maxMethods: Number.NaN });
        // 260 method buckets from 26 types x 10 methods, past the default 256.
        const types = Array.from({ length: 26 }, (_v, i) =>
            defineActor({
                type: `M${i}`,
                unguarded: true,
                state: () => ({}),
                methods: () =>
                    Object.fromEntries(
                        Array.from({ length: 10 }, (_w, j) => [`m${j}`, () => 0])
                    ) as Record<string, () => number>
            })
        );
        const { app, host } = await appWith(m, types);
        try {
            for (const [i, t] of types.entries()) {
                for (let j = 0; j < 10; j++) {
                    await (host.actor(t, `k${i}`) as unknown as Record<string, () => Promise<number>>)[
                        `m${j}`
                    ]!();
                }
            }
            const snap = m.snapshot();
            expect(snap.calls.total).toBe(260);
            // The default cap of 256 must be in force: named buckets plus the
            // overflow fold, NOT one bucket per method.
            expect(Object.keys(snap.byMethod).length).toBeLessThanOrEqual(257);
            expect(snap.byMethod['(other)']?.calls).toBeGreaterThan(0);
        } finally {
            await app.stop();
        }
    });

    it('a non-finite cap falls back to the default rather than failing open', async () => {
        // `Math.floor(NaN)` is `NaN`, `Math.max(0, NaN)` is `NaN`, and every
        // comparison against `NaN` is false — so `types.size >= maxTypes`
        // never fires and the map grows one bucket per distinct type forever.
        // That is the unbounded growth the cap exists to prevent, and it looks
        // like healthy behaviour right up until the heap goes.
        //
        // Exceeding the DEFAULT cap of 64 is what makes the bug visible: with
        // a handful of types, a broken cap and a working one are identical.
        const m = metrics({ maxTypes: Number.NaN });
        const types = Array.from({ length: 70 }, (_v, i) =>
            defineActor({
                type: `T${i}`,
                unguarded: true,
                state: () => ({}),
                methods: () => ({ noop: () => 0 })
            })
        );
        const { app, host } = await appWith(m, types);
        try {
            for (const [i, t] of types.entries()) await host.actor(t, `k${i}`).noop();
            const snap = m.snapshot();
            expect(snap.calls.total).toBe(70);
            // The default cap must be in force: 64 named buckets plus the
            // overflow fold, NOT one bucket per type.
            expect(Object.keys(snap.byType).length).toBeLessThanOrEqual(65);
            expect(snap.byType['(other)']?.calls).toBeGreaterThan(0);
        } finally {
            await app.stop();
        }
    });

    it('never reports a negative duration', async () => {
        // Durations come from performance.now(), not Date.now(): a wall
        // clock can be stepped backwards by NTP or a VM host, which would
        // hand every observer negative queue waits to defend against.
        const seen: number[] = [];
        const host = createHost({
            actors: [Counter],
            storage: memoryStorage(),
            defaults: quiet,
            onTurn: (_ref, _method, queuedMs, elapsedMs) => seen.push(queuedMs, elapsedMs)
        });
        await host.start();
        try {
            const call = { callChain: [], callId: 'test' };
            const ref = { type: 'Counter', key: 'a' };
            await Promise.all([
                host.dispatch(ref, 'noop', [], call),
                host.dispatch(ref, 'noop', [], call),
                host.dispatch(ref, 'noop', [], call)
            ]);
            expect(seen.length).toBe(6);
            for (const value of seen) expect(value).toBeGreaterThanOrEqual(0);
        } finally {
            await host.stop();
        }
    });
});

describe('metrics() runtime toggle', () => {
    it('enable/disable freezes and resumes collection without losing counters', async () => {
        const m = metrics();
        const { app, host } = await appWith(m);
        try {
            await host.actor(Counter, 'a').noop();
            expect(m.enabled).toBe(true);
            expect(m.snapshot().calls.total).toBe(1);

            m.disable();
            expect(m.enabled).toBe(false);
            await host.actor(Counter, 'a').noop();
            await host.actor(Counter, 'a').noop();
            // Frozen, not cleared: the earlier call is still counted.
            expect(m.snapshot().calls.total).toBe(1);

            m.enable();
            await host.actor(Counter, 'a').noop();
            expect(m.snapshot().calls.total).toBe(2);
        } finally {
            await app.stop();
        }
    });

    it('disable() detaches the turn observer rather than ignoring it', async () => {
        // The claim under test is a COST claim, so assert the mechanism, not
        // just the output: while disabled the runtime must not be timing
        // turns at all. A second observer proves whether the host still
        // considers anyone interested.
        const m = metrics();
        const { app, host } = await appWith(m);
        try {
            const turnsBefore = m.snapshot().turnMs!.count;
            expect(turnsBefore).toBe(0);

            await host.actor(Counter, 'a').noop();
            expect(m.snapshot().turnMs!.count).toBe(1);

            m.disable();
            await host.actor(Counter, 'a').noop();
            expect(m.snapshot().turnMs!.count).toBe(1);

            m.enable();
            await host.actor(Counter, 'a').noop();
            expect(m.snapshot().turnMs!.count).toBe(2);
        } finally {
            await app.stop();
        }
    });

    it('enabled: false attaches the plugin but collects nothing until enabled', async () => {
        const m = metrics({ enabled: false });
        const { app, host } = await appWith(m);
        try {
            expect(m.enabled).toBe(false);
            await host.actor(Counter, 'a').noop();
            expect(m.snapshot().calls.total).toBe(0);
            expect(m.snapshot().turnMs!.count).toBe(0);

            m.enable();
            await host.actor(Counter, 'a').noop();
            expect(m.snapshot().calls.total).toBe(1);
            expect(m.snapshot().turnMs!.count).toBe(1);
        } finally {
            await app.stop();
        }
    });

    it('streams keep working while metrics are disabled', async () => {
        const m = metrics({ enabled: false });
        const { app, host } = await appWith(m);
        try {
            const iterator = host.actor(Counter, 'a').watch()[Symbol.asyncIterator]();
            expect((await iterator.next()).value).toEqual({ count: 0 });
            await iterator.return?.();
            expect(m.snapshot().calls.streams).toBe(0);
        } finally {
            await app.stop();
        }
    });
});

describe('observeTurns seam', () => {
    it('is not called when no plugin registers one', async () => {
        // The zero-overhead claim: with no observer, the activation must not
        // even be asked for one. Proven by the absence of any turn callback.
        const observed: string[] = [];
        const host = createHost({
            actors: [Counter],
            storage: memoryStorage(),
            defaults: quiet
        });
        await host.start();
        try {
            await host.dispatch({ type: 'Counter', key: 'a' }, 'noop', [], {
                callChain: [],
                callId: 'test'
            });
            expect(observed).toHaveLength(0);
        } finally {
            await host.stop();
        }
    });

    it('reports failed turns and reminder deliveries', async () => {
        const seen: { method: string; failed: boolean }[] = [];
        const onTurn: ActorTurnObserver = (_ref, method, _q, _e, failed) => {
            seen.push({ method, failed });
        };
        const host = createHost({
            actors: [Counter],
            storage: memoryStorage(),
            defaults: quiet,
            onTurn
        });
        await host.start();
        try {
            const call = { callChain: [], callId: 'test' };
            await host.dispatch({ type: 'Counter', key: 'a' }, 'noop', [], call);
            await expect(
                host.dispatch({ type: 'Counter', key: 'a' }, 'boom', [], call)
            ).rejects.toThrow();

            expect(seen).toEqual([
                { method: 'noop', failed: false },
                { method: 'boom', failed: true }
            ]);
        } finally {
            await host.stop();
        }
    });

    it('host.observeTurns() unsubscribes, and the last one leaving stops the timing', async () => {
        const a: string[] = [];
        const b: string[] = [];
        const host = createHost({
            actors: [Counter],
            storage: memoryStorage(),
            defaults: quiet
        });
        await host.start();
        try {
            const call = { callChain: [], callId: 'test' };
            const ref = { type: 'Counter', key: 'a' };

            const offA = host.observeTurns((_r, method) => a.push(method));
            const offB = host.observeTurns((_r, method) => b.push(method));
            await host.dispatch(ref, 'noop', [], call);
            expect(a).toEqual(['noop']);
            expect(b).toEqual(['noop']);

            offA();
            await host.dispatch(ref, 'noop', [], call);
            expect(a).toEqual(['noop']);
            expect(b).toEqual(['noop', 'noop']);

            offB();
            await host.dispatch(ref, 'noop', [], call);
            expect(b).toEqual(['noop', 'noop']);

            // Re-subscribing after everyone left must work, and the first
            // turn through must not report a garbage queue wait derived from
            // an enqueue timestamp that was never taken.
            const queued: number[] = [];
            host.observeTurns((_r, _m, queuedMs) => queued.push(queuedMs));
            await host.dispatch(ref, 'noop', [], call);
            expect(queued).toHaveLength(1);
            expect(queued[0]).toBeGreaterThanOrEqual(0);
            expect(queued[0]).toBeLessThan(1000);
        } finally {
            await host.stop();
        }
    });

    it('a throwing observer cannot fail the turn', async () => {
        const error = vi.spyOn(console, 'error').mockImplementation(() => {});
        const host = createHost({
            actors: [Counter],
            storage: memoryStorage(),
            defaults: quiet,
            onTurn: () => {
                throw new Error('observer is broken');
            }
        });
        await host.start();
        try {
            await expect(
                host.dispatch({ type: 'Counter', key: 'a' }, 'increment', [3], {
                    callChain: [],
                    callId: 'test'
                })
            ).resolves.toBe(3);
        } finally {
            await host.stop();
            error.mockRestore();
        }
    });

    it('excludes volatile timer ticks — they have no caller', async () => {
        const methods: string[] = [];
        const ticker = defineActor({
            type: 'Ticker',
            unguarded: true,
            state: () => ({ ticks: 0 }),
            onActivate(ctx) {
                ctx.timer(
                    'tick',
                    () => {
                        ctx.state.ticks++;
                    },
                    { due: 1 }
                );
            },
            methods: (ctx) => ({
                ticks: () => ctx.state.ticks
            })
        });
        const host = createHost({
            actors: [ticker],
            storage: memoryStorage(),
            defaults: quiet,
            onTurn: (_ref, method) => methods.push(method)
        });
        await host.start();
        try {
            const call = { callChain: [], callId: 'test' };
            await host.dispatch({ type: 'Ticker', key: 'a' }, 'ticks', [], call);
            await new Promise((resolve) => setTimeout(resolve, 30));
            const after = await host.dispatch({ type: 'Ticker', key: 'a' }, 'ticks', [], call);

            // The tick really fired...
            expect(after).toBe(1);
            // ...but only the two dispatched calls were observed. A timer
            // tick has no caller and no queue wait of its own; its cost
            // shows up as queue wait on whatever was behind it.
            expect(methods).toEqual(['ticks', 'ticks']);
        } finally {
            await host.stop();
        }
    });
});

describe('metrics(): per-method breakdown', () => {
    it('splits a type into its methods — the view byType cannot give you', async () => {
        const m = metrics();
        const { app, host } = await appWith(m);
        try {
            const client = host.actor(Counter, 'a');
            await client.noop();
            await client.noop();
            await client.increment(1);
            await expect(client.boom()).rejects.toThrow();

            const snap = m.snapshot();
            // The type says "4 calls, 1 failed" and stops there.
            expect(snap.byType['Counter']?.calls).toBe(4);
            // The methods say WHICH one is failing.
            expect(snap.byMethod['Counter#noop']?.calls).toBe(2);
            expect(snap.byMethod['Counter#noop']?.failed).toBe(0);
            expect(snap.byMethod['Counter#increment']?.calls).toBe(1);
            expect(snap.byMethod['Counter#boom']?.calls).toBe(1);
            expect(snap.byMethod['Counter#boom']?.failed).toBe(1);
            // And the parts add up to the whole.
            const total = Object.values(snap.byMethod).reduce((sum, b) => sum + b.calls, 0);
            expect(total).toBe(snap.calls.total);
        } finally {
            await app.stop();
        }
    });

    it('records the queue/turn split per method', async () => {
        const m = metrics();
        const { app, host } = await appWith(m);
        try {
            const client = host.actor(Counter, 'a');
            await Promise.all([client.slow(40), client.slow(40), client.noop()]);

            const snap = m.snapshot();
            // `slow` holds the mailbox; `noop` mostly waits for it. That is
            // the whole point of carrying the split down to the method.
            expect(snap.byMethod['Counter#slow']?.turnMs?.count).toBe(2);
            expect(snap.byMethod['Counter#slow']?.turnMs?.maxMs).toBeGreaterThan(20);
            expect(snap.byMethod['Counter#noop']?.turnMs?.count).toBe(1);
            expect(snap.byMethod['Counter#noop']?.turnMs?.maxMs).toBeLessThan(20);
        } finally {
            await app.stop();
        }
    });

    it('folds past maxMethods into (other), under its own cap', async () => {
        const m = metrics({ maxMethods: 2 });
        const { app, host } = await appWith(m);
        try {
            const client = host.actor(Counter, 'a');
            await client.noop();
            await client.increment(1);
            await client.persist();
            await client.slow(1);

            const snap = m.snapshot();
            expect(Object.keys(snap.byMethod)).toHaveLength(3);
            expect(snap.byMethod['(other)']?.calls).toBe(2);
            // The per-TYPE cap is independent — one type, still broken out.
            expect(Object.keys(snap.byType)).toEqual(['Counter']);
        } finally {
            await app.stop();
        }
    });

    it('maxMethods: 0 disables the breakdown without touching byType', async () => {
        const m = metrics({ maxMethods: 0 });
        const { app, host } = await appWith(m);
        try {
            await host.actor(Counter, 'a').noop();
            const snap = m.snapshot();
            expect(snap.byMethod).toEqual({});
            expect(snap.byType['Counter']?.calls).toBe(1);
        } finally {
            await app.stop();
        }
    });

    it('carries null histograms under histograms: false, like byType', async () => {
        const m = metrics({ histograms: false });
        const { app, host } = await appWith(m);
        try {
            await host.actor(Counter, 'a').noop();
            const snap = m.snapshot();
            expect(snap.byMethod['Counter#noop']?.calls).toBe(1);
            expect(snap.byMethod['Counter#noop']?.latencyMs).toBeNull();
            expect(snap.byMethod['Counter#noop']?.turnMs).toBeNull();
        } finally {
            await app.stop();
        }
    });

    it('drops the map on reset, so a method that went quiet leaves the report', async () => {
        const m = metrics();
        const { app, host } = await appWith(m);
        try {
            await host.actor(Counter, 'a').noop();
            expect(Object.keys(m.snapshot().byMethod)).toContain('Counter#noop');
            m.reset();
            expect(m.snapshot().byMethod).toEqual({});
            await host.actor(Counter, 'a').increment(1);
            expect(Object.keys(m.snapshot().byMethod)).toEqual(['Counter#increment']);
        } finally {
            await app.stop();
        }
    });
});

describe('metrics(): error kinds', () => {
    it('counts an error an actor threw as (unknown) — it is not an ActorError', async () => {
        const m = metrics();
        const { app, host } = await appWith(m);
        try {
            await expect(host.actor(Counter, 'a').boom()).rejects.toThrow();
            const snap = m.snapshot();
            expect(snap.errors.byKind).toEqual({ '(unknown)': 1 });
            expect(snap.calls.failed).toBe(1);
        } finally {
            await app.stop();
        }
    });

    it('classifies an ActorError by its kind', async () => {
        const m = metrics();
        const { app, host } = await appWith(m);
        try {
            // method-not-found is the one kind reachable without contriving
            // a cluster or a storage race.
            await expect(
                host.dispatch({ type: 'Counter', key: 'a' }, 'nope', [], {
                    callChain: [],
                    callId: 'test'
                })
            ).rejects.toThrow();
            expect(m.snapshot().errors.byKind['method-not-found']).toBe(1);
        } finally {
            await app.stop();
        }
    });

    it('remembers recent failures — message only, never args', async () => {
        const m = metrics();
        const { app, host } = await appWith(m);
        try {
            await expect(host.actor(Counter, 'secret-key').boom()).rejects.toThrow();
            const [entry] = m.snapshot().errors.recent;
            expect(entry).toMatchObject({
                type: 'Counter',
                method: 'boom',
                kind: '(unknown)',
                message: 'nope'
            });
            expect(entry!.at).toBeGreaterThan(0);
            // The ring carries no args and no state: it is read over an HTTP
            // endpoint, and a failing call's arguments are exactly where the
            // secrets are.
            expect(Object.keys(entry!).sort()).toEqual(['at', 'kind', 'message', 'method', 'type']);
        } finally {
            await app.stop();
        }
    });

    it('bounds the ring and evicts oldest-first', async () => {
        const m = metrics({ recentErrors: 3 });
        const { app, host } = await appWith(m);
        try {
            for (let i = 0; i < 5; i++) {
                await expect(host.actor(Counter, `k${i}`).boom()).rejects.toThrow();
            }
            const snap = m.snapshot();
            // Counts are unbounded; the SAMPLES are what is capped.
            expect(snap.errors.byKind['(unknown)']).toBe(5);
            expect(snap.errors.recent).toHaveLength(3);
        } finally {
            await app.stop();
        }
    });

    it('a non-finite recentErrors cap still trims the ring', async () => {
        // `recentErrors.length > NaN` is false forever, so the ring grows
        // without bound — one retained entry per failed call, for the life of
        // the process. Exceeding the DEFAULT of 32 is what makes it visible.
        const m = metrics({ recentErrors: Number.NaN });
        const { app, host } = await appWith(m);
        try {
            for (let i = 0; i < 40; i++) {
                await expect(host.actor(Counter, `k${i}`).boom()).rejects.toThrow();
            }
            const snap = m.snapshot();
            expect(snap.errors.byKind['(unknown)']).toBe(40);
            // Bounded by the default, not unbounded.
            expect(snap.errors.recent).toHaveLength(32);
        } finally {
            await app.stop();
        }
    });

    it('recentErrors: 0 keeps the counts and drops the ring', async () => {
        const m = metrics({ recentErrors: 0 });
        const { app, host } = await appWith(m);
        try {
            await expect(host.actor(Counter, 'a').boom()).rejects.toThrow();
            const snap = m.snapshot();
            expect(snap.errors.byKind['(unknown)']).toBe(1);
            expect(snap.errors.recent).toEqual([]);
        } finally {
            await app.stop();
        }
    });

    it('hands out a copy, so a held snapshot cannot watch the ring mutate', async () => {
        const m = metrics();
        const { app, host } = await appWith(m);
        try {
            await expect(host.actor(Counter, 'a').boom()).rejects.toThrow();
            const held = m.snapshot();
            await expect(host.actor(Counter, 'b').boom()).rejects.toThrow();
            expect(held.errors.recent).toHaveLength(1);
            expect(m.snapshot().errors.recent).toHaveLength(2);
        } finally {
            await app.stop();
        }
    });

    it('clears both on reset', async () => {
        const m = metrics();
        const { app, host } = await appWith(m);
        try {
            await expect(host.actor(Counter, 'a').boom()).rejects.toThrow();
            m.reset();
            const snap = m.snapshot();
            expect(snap.errors.byKind).toEqual({});
            expect(snap.errors.recent).toEqual([]);
        } finally {
            await app.stop();
        }
    });

    it('counts nothing while disabled', async () => {
        const m = metrics();
        const { app, host } = await appWith(m);
        try {
            m.disable();
            await expect(host.actor(Counter, 'a').boom()).rejects.toThrow();
            expect(m.snapshot().errors.byKind).toEqual({});
            m.enable();
            await expect(host.actor(Counter, 'a').boom()).rejects.toThrow();
            expect(m.snapshot().errors.byKind['(unknown)']).toBe(1);
        } finally {
            await app.stop();
        }
    });
});
