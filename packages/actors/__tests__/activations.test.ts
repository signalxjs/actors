/**
 * `host.activations()` — the live actor list, and the transitional counts
 * `stats()` used to drop.
 *
 * The gap this closes: `stats()` could say a host held 12,000 activations
 * with 400 queued turns and could not say WHERE, which is the only question
 * worth asking at that point. Everything below is about that list being
 * usable from a dashboard — bounded, stably ordered, and honest about the
 * slots it cannot see.
 */
import { describe, expect, it, vi } from 'vitest';
import { defineActor } from '@sigx/actors';
import { defineActorApp, memoryStorage, type Host } from '@sigx/actors/host';

const quiet = { sweepIntervalMs: 600_000, reminderTickMs: 600_000, callTimeoutMs: 0 };

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        noop() {
            return 0;
        },
        async slow(ms: number) {
            await new Promise((resolve) => setTimeout(resolve, ms));
            return ctx.state.count;
        }
    })
});

const Other = defineActor({
    type: 'Other',
    unguarded: true,
    state: () => ({}),
    methods: () => ({
        noop() {
            return 0;
        }
    })
});

async function host(): Promise<{ live: Host; stop: () => Promise<void> }> {
    const app = defineActorApp({
        actors: [Counter, Other],
        storage: memoryStorage(),
        defaults: quiet
    });
    const live = await app.start();
    return { live, stop: () => app.stop() };
}

describe('host.activations()', () => {
    it('lists live actors with their type, key and queued-turn depth', async () => {
        const { live, stop } = await host();
        try {
            await live.actor(Counter, 'a').noop();
            await live.actor(Counter, 'b').noop();

            const rows = live.activations();
            expect(rows).toHaveLength(2);
            expect(rows.map((r) => `${r.type}/${r.key}`).sort()).toEqual([
                'Counter/a',
                'Counter/b'
            ]);
            for (const row of rows) {
                expect(row.queued).toBe(0);
                expect(row.ageMs).toBeGreaterThanOrEqual(0);
                expect(row.idleMs).toBeGreaterThanOrEqual(0);
                expect(row.keptAlive).toBe(false);
            }
        } finally {
            await stop();
        }
    });

    it('sorts by queue depth — the hot actors first', async () => {
        const { live, stop } = await host();
        try {
            const hot = live.actor(Counter, 'hot');
            const cold = live.actor(Counter, 'cold');
            await cold.noop();
            // Three turns against one actor; turns serialize, so
            // depth is non-zero while they run.
            const inflight = Promise.all([hot.slow(60), hot.slow(60), hot.slow(60)]);
            await new Promise((resolve) => setTimeout(resolve, 20));

            const rows = live.activations({ sortBy: 'queued' });
            expect(rows[0]!.key).toBe('hot');
            expect(rows[0]!.queued).toBeGreaterThan(1);
            expect(rows[1]!.key).toBe('cold');

            await inflight;
        } finally {
            await stop();
        }
    });

    it('sorts by age and by idle', async () => {
        const { live, stop } = await host();
        try {
            await live.actor(Counter, 'first').noop();
            await new Promise((resolve) => setTimeout(resolve, 25));
            await live.actor(Counter, 'second').noop();

            expect(live.activations({ sortBy: 'age' })[0]!.key).toBe('first');
            // `first` has not been touched since; `second` just was.
            expect(live.activations({ sortBy: 'idle' })[0]!.key).toBe('first');
        } finally {
            await stop();
        }
    });

    it('bounds the result, defaulting to 100', async () => {
        const { live, stop } = await host();
        try {
            for (let i = 0; i < 120; i++) await live.actor(Counter, `k${i}`).noop();
            expect(live.activations()).toHaveLength(100);
            expect(live.activations({ limit: 5 })).toHaveLength(5);
            expect(live.activations({ limit: 1000 })).toHaveLength(120);
            // A host can hold millions; asking for none must not walk them.
            expect(live.activations({ limit: 0 })).toEqual([]);
        } finally {
            await stop();
        }
    });

    it('falls back to the default on a non-finite limit, rather than unbounding', async () => {
        const { live, stop } = await host();
        try {
            for (let i = 0; i < 120; i++) await live.actor(Counter, `k${i}`).noop();
            // `Math.max(0, Math.floor(NaN))` is NaN, and every comparison
            // against NaN is false — so the `=== 0` check and the final
            // slice would BOTH miss and the bound would silently vanish. A
            // cap that fails open is worse than no cap.
            expect(live.activations({ limit: Number.NaN })).toHaveLength(100);
            expect(live.activations({ limit: Number.POSITIVE_INFINITY })).toHaveLength(100);
            expect(live.activations({ limit: undefined })).toHaveLength(100);
            // A negative is a real number and clamps to 0, as before.
            expect(live.activations({ limit: -5 })).toEqual([]);
            // A fraction floors rather than making the cap arbitrary.
            expect(live.activations({ limit: 3.9 })).toHaveLength(3);
        } finally {
            await stop();
        }
    });

    it('filters by type', async () => {
        const { live, stop } = await host();
        try {
            await live.actor(Counter, 'a').noop();
            await live.actor(Other, 'b').noop();

            const rows = live.activations({ type: 'Other' });
            expect(rows).toHaveLength(1);
            expect(rows[0]!.type).toBe('Other');
            expect(live.activations({ type: 'Nope' })).toEqual([]);
        } finally {
            await stop();
        }
    });

    it('orders ties stably, so a polled table does not reshuffle', async () => {
        const { live, stop } = await host();
        try {
            for (const key of ['c', 'a', 'b']) await live.actor(Counter, key).noop();
            // Every row has queued 0 — without the tie-break the order would
            // be insertion order, and a dashboard polling at 1 Hz would show
            // rows jumping for no reason.
            const first = live.activations().map((r) => r.key);
            const second = live.activations().map((r) => r.key);
            expect(first).toEqual(['a', 'b', 'c']);
            expect(second).toEqual(first);
        } finally {
            await stop();
        }
    });

    it('never reports a negative age or idle, even if the wall clock steps back', async () => {
        const { live, stop } = await host();
        try {
            await live.actor(Counter, 'a').noop();
            // NTP or a VM host stepping the clock backwards would otherwise
            // make `idleMs` negative — `ageMs` is monotonic and immune, which
            // is why the two use different clocks.
            const realNow = Date.now();
            vi.spyOn(Date, 'now').mockReturnValue(realNow - 60_000);
            const [row] = live.activations();
            expect(row!.idleMs).toBe(0);
            expect(row!.ageMs).toBeGreaterThanOrEqual(0);
        } finally {
            vi.restoreAllMocks();
            await stop();
        }
    });

    it('is empty on a host that has activated nothing', async () => {
        const { live, stop } = await host();
        try {
            expect(live.activations()).toEqual([]);
        } finally {
            await stop();
        }
    });
});

describe('host.stats(): transitional slots', () => {
    it('reports zero when nothing is in flight', async () => {
        const { live, stop } = await host();
        try {
            await live.actor(Counter, 'a').noop();
            const stats = live.stats();
            expect(stats.activations).toBe(1);
            expect(stats.transitional).toEqual({ activating: 0, deactivating: 0 });
        } finally {
            await stop();
        }
    });

    it('counts a slot mid-activation — a storm must not read as an idle host', async () => {
        let release: (() => void) | undefined;
        const gate = new Promise<void>((resolve) => {
            release = resolve;
        });
        const Slow = defineActor({
            type: 'SlowStart',
            unguarded: true,
            state: () => ({}),
            async onActivate() {
                await gate;
            },
            methods: () => ({
                noop() {
                    return 0;
                }
            })
        });
        const app = defineActorApp({
            actors: [Slow],
            storage: memoryStorage(),
            defaults: quiet
        });
        const live = await app.start();
        try {
            const pending = live.actor(Slow, 'a').noop();
            await new Promise((resolve) => setTimeout(resolve, 20));

            const stats = live.stats();
            // Nothing settled yet, so the old `stats()` said "0 activations"
            // for a host that was entirely busy activating.
            expect(stats.activations).toBe(0);
            expect(stats.transitional.activating).toBe(1);
            // And the list only ever shows settled slots — there is no
            // Activation to read from a reserving one.
            expect(live.activations()).toEqual([]);

            release!();
            await pending;
            expect(live.stats().activations).toBe(1);
            expect(live.stats().transitional.activating).toBe(0);
        } finally {
            await app.stop();
        }
    });
});
