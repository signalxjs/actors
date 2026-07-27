import { afterEach, describe, expect, it, vi } from 'vitest';
import {
    ActorStorageConflict,
    defineActor,
    type ActorDispatcher,
    type ActorPlacement,
    type ActorStorage
} from '@sigx/actors';
import { createSilo, memoryStorage, REMINDER_TYPE, type Silo } from '@sigx/actors/silo';
// The service class itself is internal — imported directly for the
// concurrent-ticker CAS test.
import { ReminderService } from '../src/silo/reminders';

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

    it('ownsReminderShard gates the tick loop per shard (the cluster seam)', async () => {
        const events: string[] = [];
        const def = wakingActor(events);
        let leader = false;
        const out: { local?: ActorDispatcher } = {};
        const placement: ActorPlacement = {
            dispatcherFor: () => out.local!,
            bind(local) {
                out.local = local;
                return { ownsReminderShard: () => leader };
            }
        };
        const silo = createSilo({
            actors: [def],
            storage: memoryStorage(),
            placement,
            defaults: { reminderTickMs: 25, sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        running = silo;
        await silo.start();
        await silo.actor(def, 'g1').wakeMeIn(0);

        // Several tick intervals pass while gated off: nothing fires.
        await new Promise((r) => setTimeout(r, 120));
        expect(events).not.toContain('reminder:wake');

        leader = true;
        await vi.waitFor(() => expect(events).toContain('reminder:wake'), { timeout: 3000 });
    });

    it('reminders live in fixed hash shards (p0..p15), never in $all', async () => {
        const events: string[] = [];
        const def = wakingActor(events);
        const storage = memoryStorage();
        const silo = createSilo({
            actors: [def],
            storage,
            defaults: { reminderTickMs: 60_000, sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        for (const key of ['a', 'b', 'c', 'd', 'e']) {
            await silo.actor(def, key).wakeMeIn(120_000);
        }
        // Every reminder landed in a p<n> shard record.
        const found: string[] = [];
        for (let i = 0; i < 16; i++) {
            const record = await storage.load(REMINDER_TYPE, `p${i}`);
            if (record && Object.keys(record.state as object).length > 0) found.push(`p${i}`);
        }
        expect(found.length).toBeGreaterThan(0);
        const total = (
            await Promise.all(
                found.map(async (shard) => {
                    const record = await storage.load(REMINDER_TYPE, shard);
                    return Object.keys(record!.state as object).length;
                })
            )
        ).reduce((a, b) => a + b, 0);
        expect(total).toBe(5);
        // The same actor always resolves to the same shard: list() finds it.
        await expect(silo.actor(def, 'a').listReminders()).resolves.toEqual(['wake']);
    });

    it('two tickers on the same shard fire a reminder exactly once (CAS, no lease)', async () => {
        const storage = memoryStorage();
        const fired: string[] = [];
        // Two services both claim ownership of every shard — the divergent
        // membership-view case. The per-shard etag CAS must arbitrate.
        const a = new ReminderService(storage, async (_ref, name) => void fired.push(`A:${name}`));
        const b = new ReminderService(storage, async (_ref, name) => void fired.push(`B:${name}`));
        await a.apiFor({ type: 'Waking', key: 'race' }).set('wake', { due: 0 });
        a.start(20);
        b.start(20);
        try {
            await vi.waitFor(() => expect(fired.length).toBeGreaterThan(0), { timeout: 3000 });
            await new Promise((r) => setTimeout(r, 150)); // several tick windows
            expect(fired).toHaveLength(1);
        } finally {
            a.stop();
            b.stop();
        }
    });

    it('reminder mutations retry on a storage etag conflict (reload + reapply)', async () => {
        const events: string[] = [];
        const def = wakingActor(events);
        const base = memoryStorage();
        let conflicts = 0;
        const storage: ActorStorage = {
            load: (type, key) => base.load(type, key),
            save: async (type, key, state, expectedEtag) => {
                // The reminder table has concurrent writers in a cluster —
                // simulate another silo winning the first CAS.
                if (type === REMINDER_TYPE && conflicts === 0) {
                    conflicts++;
                    throw new ActorStorageConflict(type, key);
                }
                return base.save(type, key, state, expectedEtag);
            },
            clear: (type, key, expectedEtag) => base.clear(type, key, expectedEtag)
        };
        const silo = createSilo({
            actors: [def],
            storage,
            defaults: { reminderTickMs: 60_000, sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        const client = silo.actor(def, 'cas');
        await expect(client.wakeMeIn(120_000)).resolves.toBeUndefined();
        expect(conflicts).toBe(1);
        await expect(client.listReminders()).resolves.toEqual(['wake']);
    });
});
