import { afterEach, describe, expect, it, vi } from 'vitest';
import { defineActor } from '@sigx/actors';
import { createSilo, memoryStorage, type Silo } from '@sigx/actors/silo';

let running: Silo | null = null;
afterEach(async () => {
    await running?.stop({ timeoutMs: 1000 });
    running = null;
});

function wakingActor(events: string[]) {
    return defineActor({
        type: 'Waking',
        unguarded: true,
        state: () => ({}),
        onActivate(ctx) {
            events.push(`activate:${ctx.key}`);
        },
        onReminder(_ctx, name) {
            events.push(`reminder:${name}`);
        },
        methods: (ctx) => ({
            async wakeMeIn(ms: number) {
                await ctx.reminders.set('wake', { due: ms });
            },
            async listReminders() {
                return ctx.reminders.list();
            }
        })
    });
}

describe('reminders', () => {
    it('rejects periods under the 60s floor', async () => {
        const events: string[] = [];
        const silo = createSilo({
            actors: [wakingActor(events)],
            defaults: { reminderTickMs: 60_000, sweepIntervalMs: 60_000 }
        });
        const bad = defineActor({
            type: 'BadPeriod',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                async go() {
                    await ctx.reminders.set('x', { due: 0, period: 1000 });
                }
            })
        });
        const silo2 = createSilo({ actors: [bad], defaults: { reminderTickMs: 60_000 } });
        await expect(silo2.actor(bad, 'k').go()).rejects.toThrow(/period must be >= 60000ms/);
        void silo;
    });

    it('a due reminder RE-ACTIVATES an idle actor and one-shots clear themselves', async () => {
        const events: string[] = [];
        const storage = memoryStorage();
        const def = wakingActor(events);
        const silo = createSilo({
            actors: [def],
            storage,
            defaults: { reminderTickMs: 25, sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        running = silo;
        await silo.start();

        const client = silo.actor(def, 'r1');
        await client.wakeMeIn(0);
        // Deactivate — the reminder must bring it back.
        await silo.deactivateType('Waking');
        expect(silo.stats().activations).toBe(0);

        await vi.waitFor(() => expect(events).toContain('reminder:wake'), { timeout: 3000 });
        // Re-activated by the reminder (a second activate event).
        expect(events.filter((e) => e === 'activate:r1').length).toBe(2);
        // One-shot: cleared after firing.
        await vi.waitFor(async () => {
            await expect(client.listReminders()).resolves.toEqual([]);
        });
    });

    it('reminders survive a silo restart on shared storage', async () => {
        const events: string[] = [];
        const storage = memoryStorage();
        const def = wakingActor(events);
        const siloA = createSilo({
            actors: [def],
            storage,
            defaults: { reminderTickMs: 25, sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        await siloA.start();
        await siloA.actor(def, 'r2').wakeMeIn(50);
        await siloA.stop();
        expect(events).not.toContain('reminder:wake');

        const siloB = createSilo({
            actors: [def],
            storage,
            defaults: { reminderTickMs: 25, sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        running = siloB;
        await siloB.start();
        await vi.waitFor(() => expect(events).toContain('reminder:wake'), { timeout: 3000 });
    });
});
