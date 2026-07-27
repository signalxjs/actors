/** Let queued mailbox turns settle — timer callbacks run as turns. */
async function drain(): Promise<void> {
    for (let i = 0; i < 10; i++) await new Promise((r) => setTimeout(r, 0));
}

/**
 * The clock seam. Background work used to be pushed past the end of each
 * test with `{ sweepIntervalMs: 60_000, reminderTickMs: 60_000 }`; with a
 * manual scheduler the sweeper and the reminder tick can be driven exactly
 * and asserted on, which is also what makes an alarm-driven runtime
 * (Cloudflare Durable Objects) possible at all.
 */
import { describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import { createSilo, manualScheduler, memoryStorage } from '@sigx/actors/silo';

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        }
    })
});

describe('manualScheduler', () => {
    it('fires repeats in due order and re-arms them', () => {
        const scheduler = manualScheduler();
        const seen: string[] = [];
        scheduler.every(100, () => seen.push('every100'));
        scheduler.after(150, () => seen.push('after150'));

        scheduler.advance(320);
        // 100, 150, 200, 300 — strictly by due time, repeats interleaved
        // with the one-shot rather than batched.
        expect(seen).toEqual(['every100', 'after150', 'every100', 'every100']);
        expect(scheduler.now).toBe(320);
        // The one-shot is gone; the repeat is still armed.
        expect(scheduler.pending).toBe(1);
    });

    it('stops firing once cancelled', () => {
        const scheduler = manualScheduler();
        let ticks = 0;
        const cancel = scheduler.every(10, () => void ticks++);
        scheduler.advance(25);
        expect(ticks).toBe(2);
        cancel();
        scheduler.advance(100);
        expect(ticks).toBe(2);
        expect(scheduler.pending).toBe(0);
    });

    it('runs work a job registers while running', () => {
        const scheduler = manualScheduler();
        const seen: string[] = [];
        scheduler.after(10, () => {
            seen.push('first');
            scheduler.after(10, () => seen.push('second'));
        });
        // One advance covers both, because due jobs are re-scanned.
        scheduler.advance(50);
        expect(seen).toEqual(['first', 'second']);
    });
});

describe('the silo drives its background work through the seam', () => {
    it('sweeps idle activations when time actually advances', async () => {
        const scheduler = manualScheduler();
        const silo = createSilo({
            actors: [Counter],
            storage: memoryStorage(),
            scheduler,
            defaults: { idleAfterMs: 0, sweepIntervalMs: 1_000, callTimeoutMs: 0 }
        });
        await silo.start();
        try {
            await silo.actor(Counter, 'k').increment(1);
            expect(silo.stats().activations).toBe(1);

            // No wall-clock waiting, and no 60_000 sleight of hand: the
            // sweeper simply has not been ticked yet.
            expect(silo.stats().activations).toBe(1);
            scheduler.advance(1_000);
            await drain();
            expect(silo.stats().activations).toBe(0);
        } finally {
            await silo.stop({ timeoutMs: 1_000 });
        }
    });

    it('leaves nothing armed after stop()', async () => {
        const scheduler = manualScheduler();
        const silo = createSilo({
            actors: [Counter],
            storage: memoryStorage(),
            scheduler,
            defaults: { sweepIntervalMs: 1_000, reminderTickMs: 1_000, callTimeoutMs: 0 }
        });
        await silo.start();
        // Sweeper + reminder tick.
        expect(scheduler.pending).toBe(2);
        await silo.stop({ timeoutMs: 1_000 });
        expect(scheduler.pending).toBe(0);
    });

    it('coalesces timer fires that stack up inside one advance', async () => {
        // Documenting real behaviour, not a quirk of the fake clock:
        // `ctx.timer` drops a tick while one is already queued ("a tick
        // behind a slow turn must not pile up"). Advancing past three
        // periods at once is exactly that situation.
        const scheduler = manualScheduler();
        const ticks: number[] = [];
        const Ticker = defineActor({
            type: 'Coalesce',
            unguarded: true,
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                async arm() {
                    ctx.timer('tick', () => void ticks.push(++ctx.state.n), {
                        due: 50,
                        period: 100
                    });
                    return true;
                }
            })
        });
        const silo = createSilo({
            actors: [Ticker],
            storage: memoryStorage(),
            scheduler,
            defaults: { sweepIntervalMs: 10_000, reminderTickMs: 10_000, callTimeoutMs: 0 }
        });
        await silo.start();
        try {
            await silo.actor(Ticker, 'k').arm();
            scheduler.advance(250);
            await drain();
            expect(ticks).toEqual([1]);
        } finally {
            await silo.stop({ timeoutMs: 1_000 });
        }
    });

    it('drives ctx.timer through the seam too', async () => {
        const scheduler = manualScheduler();
        const ticks: number[] = [];
        const Ticker = defineActor({
            type: 'Ticker',
            unguarded: true,
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                async arm() {
                    ctx.timer('tick', () => void ticks.push(++ctx.state.n), {
                        due: 50,
                        period: 100
                    });
                    return true;
                }
            })
        });
        const silo = createSilo({
            actors: [Ticker],
            storage: memoryStorage(),
            scheduler,
            defaults: { sweepIntervalMs: 10_000, reminderTickMs: 10_000, callTimeoutMs: 0 }
        });
        await silo.start();
        try {
            await silo.actor(Ticker, 'k').arm();
            // due 50, then every 100 → 50, 150, 250. Drain between steps so
            // each fire gets its mailbox turn.
            for (const step of [50, 100, 100]) {
                scheduler.advance(step);
                await drain();
            }
            expect(ticks).toEqual([1, 2, 3]);
        } finally {
            await silo.stop({ timeoutMs: 1_000 });
        }
    });
});

describe('manualScheduler guards', () => {
    it('refuses to rewind', () => {
        const scheduler = manualScheduler();
        scheduler.advance(100);
        expect(() => scheduler.advance(-50)).toThrow(/cannot go backwards/);
        // ...and the clock is untouched by the refusal.
        expect(scheduler.now).toBe(100);
    });

    it('treats a zero advance as a no-op', () => {
        const scheduler = manualScheduler();
        let ticks = 0;
        scheduler.after(0, () => void ticks++);
        scheduler.advance(0);
        expect(ticks).toBe(1);
        expect(scheduler.now).toBe(0);
    });
});
