/**
 * The two Durable Object seams, driven by fakes — no Workers runtime, and
 * no waiting: the alarm is fired by hand, which is the whole point of
 * reminders being a seam rather than a polling loop.
 */
import { describe, expect, it } from 'vitest';
import { defineActor, isStorageConflict } from '@sigx/actors';
import { createSilo, manualScheduler } from '@sigx/actors/silo';
import {
    durableObjectReminders,
    durableObjectStorage,
    type DurableAlarms,
    type DurableStorage
} from '@sigx/actors-cloudflare';

/** A DO's storage: a Map, which is all the real one is from here. */
function fakeStorage(): DurableStorage & { readonly map: Map<string, unknown> } {
    const map = new Map<string, unknown>();
    return {
        map,
        // structuredClone so a stored value never aliases live state, as
        // the real (serializing) DO storage would not.
        get: async <T,>(key: string) =>
            map.has(key) ? (structuredClone(map.get(key)) as T) : undefined,
        put: async <T,>(key: string, value: T) => void map.set(key, structuredClone(value)),
        delete: async (key: string) => map.delete(key)
    };
}

function fakeAlarms(): DurableAlarms & { at: number | null } {
    return {
        at: null,
        async getAlarm() {
            return this.at;
        },
        async setAlarm(scheduledTime: number) {
            this.at = scheduledTime;
        },
        async deleteAlarm() {
            this.at = null;
        }
    };
}

describe('durableObjectStorage', () => {
    it('round-trips state and bumps the etag', async () => {
        const storage = durableObjectStorage(fakeStorage());
        await expect(storage.load('Counter', 'a')).resolves.toBeNull();

        const first = await storage.save('Counter', 'a', { count: 1 }, null);
        expect(first).toBe('1');
        await expect(storage.load('Counter', 'a')).resolves.toEqual({
            state: { count: 1 },
            etag: '1'
        });

        const second = await storage.save('Counter', 'a', { count: 2 }, first);
        expect(second).toBe('2');
    });

    it('rejects a stale write with the conflict brand', async () => {
        const storage = durableObjectStorage(fakeStorage());
        await storage.save('Counter', 'a', { count: 1 }, null);
        // The runtime translates THIS brand into ActorStateConflictError and
        // discards the stale activation, so it has to be the branded one.
        await expect(storage.save('Counter', 'a', { count: 9 }, null)).rejects.toSatisfy(
            isStorageConflict
        );
    });

    it('keeps actors apart, and clears idempotently', async () => {
        const storage = durableObjectStorage(fakeStorage());
        await storage.save('Counter', 'a', { count: 1 }, null);
        await storage.save('Counter', 'b', { count: 2 }, null);
        await expect(storage.load('Counter', 'b')).resolves.toMatchObject({
            state: { count: 2 }
        });

        await storage.clear('Counter', 'a', '1');
        await expect(storage.load('Counter', 'a')).resolves.toBeNull();
        // Re-clearing with the SAME etag is a conflict, not a no-op: that
        // etag no longer exists. (See the CAS-edges block above.)
        await expect(storage.clear('Counter', 'a', '1')).rejects.toSatisfy(isStorageConflict);
        // ...and 'b' is untouched.
        await expect(storage.load('Counter', 'b')).resolves.toMatchObject({ etag: '1' });
    });
});

describe('durableObjectStorage — CAS edges', () => {
    it('conflicts when clearing a missing record with a non-null etag', async () => {
        const storage = durableObjectStorage(fakeStorage());
        // Only a clear that EXPECTED nothing may no-op. Otherwise the caller
        // holds a version that no longer exists, and letting it through lets
        // a stale activation wipe state undetected.
        await expect(storage.clear('Counter', 'gone', '3')).rejects.toSatisfy(isStorageConflict);
        await expect(storage.clear('Counter', 'gone', null)).resolves.toBeUndefined();
    });

    it('recovers from a non-numeric stored etag instead of producing NaN', async () => {
        const raw = fakeStorage();
        const storage = durableObjectStorage(raw);
        await storage.save('Counter', 'a', { count: 1 }, null);
        // Simulate a corrupt / foreign record.
        const key = [...raw.map.keys()][0]!;
        raw.map.set(key, { state: { count: 1 }, etag: 'bogus' });

        const next = await storage.save('Counter', 'a', { count: 2 }, 'bogus');
        expect(next).toBe('1');
        expect(Number.isNaN(Number(next))).toBe(false);
    });
});

describe('durableObjectReminders', () => {
    const Waking = defineActor({
        type: 'Waking',
        unguarded: true,
        state: () => ({ woke: 0 }),
        onReminder(ctx) {
            ctx.state.woke++;
        },
        methods: (ctx) => ({
            async armIn(ms: number, period?: number) {
                await ctx.reminders.set('wake', period === undefined ? { due: ms } : { due: ms, period });
            },
            async names() {
                return ctx.reminders.list();
            },
            async woke() {
                return ctx.state.woke;
            }
        })
    });

    /** A silo whose reminders are the DO implementation. */
    async function siloWithAlarms(now: () => number) {
        const storage = fakeStorage();
        const alarms = fakeAlarms();
        const reminders = durableObjectReminders({ storage, alarms, now });
        const silo = createSilo({
            actors: [Waking],
            storage: durableObjectStorage(storage),
            reminders,
            scheduler: manualScheduler(),
            defaults: { sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        await silo.start();
        return { silo, alarms, reminders };
    }

    it('arms the platform alarm instead of polling', async () => {
        let clock = 1_000;
        const { silo, alarms, reminders } = await siloWithAlarms(() => clock);
        try {
            await silo.actor(Waking, 'a').armIn(500);
            // No tick loop anywhere — the alarm carries the schedule.
            expect(alarms.at).toBe(1_500);
            await expect(silo.actor(Waking, 'a').names()).resolves.toEqual(['wake']);

            clock = 1_500;
            // The platform clears the alarm before invoking alarm().
            alarms.at = null;
            await reminders.onAlarm();
            await expect(silo.actor(Waking, 'a').woke()).resolves.toBe(1);
            // One-shot: gone, and the alarm with it.
            await expect(silo.actor(Waking, 'a').names()).resolves.toEqual([]);
            expect(alarms.at).toBeNull();
        } finally {
            await silo.stop({ timeoutMs: 1000 });
        }
    });

    it('re-arms a periodic reminder for its next due time', async () => {
        let clock = 0;
        const { silo, alarms, reminders } = await siloWithAlarms(() => clock);
        try {
            await silo.actor(Waking, 'p').armIn(60_000, 60_000);
            expect(alarms.at).toBe(60_000);

            clock = 60_000;
            alarms.at = null;
            await reminders.onAlarm();
            await expect(silo.actor(Waking, 'p').woke()).resolves.toBe(1);
            // Still registered, and the alarm moved on.
            await expect(silo.actor(Waking, 'p').names()).resolves.toEqual(['wake']);
            expect(alarms.at).toBe(120_000);
        } finally {
            await silo.stop({ timeoutMs: 1000 });
        }
    });

    it('keeps cadence when an alarm fires late, without a catch-up burst', async () => {
        let clock = 0;
        const { silo, alarms, reminders } = await siloWithAlarms(() => clock);
        try {
            await silo.actor(Waking, 'c').armIn(60_000, 60_000);
            expect(alarms.at).toBe(60_000);

            // Fires 5s late: the next one stays on the original cadence
            // (120_000), it does not drift to 125_000.
            clock = 65_000;
            alarms.at = null;
            await reminders.onAlarm();
            expect(alarms.at).toBe(120_000);

            // After real downtime the schedule is clamped forward instead of
            // queueing every missed firing.
            clock = 600_000;
            alarms.at = null;
            await reminders.onAlarm();
            expect(alarms.at).toBe(660_000);
            await expect(silo.actor(Waking, 'c').woke()).resolves.toBe(2);
        } finally {
            await silo.stop({ timeoutMs: 1000 });
        }
    });

    it('never pushes an armed alarm later', async () => {
        let clock = 0;
        const { silo, alarms } = await siloWithAlarms(() => clock);
        try {
            await silo.actor(Waking, 'x').armIn(1_000);
            expect(alarms.at).toBe(1_000);
            // A later reminder must not delay the one already due sooner.
            await silo.actor(Waking, 'x').armIn(5_000);
            expect(alarms.at).toBe(1_000);
        } finally {
            await silo.stop({ timeoutMs: 1000 });
        }
    });

    it('fires after a cold start, with nothing left in memory', async () => {
        // The failure this guards: a DO is evicted when idle and the alarm
        // that wakes it runs BEFORE any actor is activated. An owner held
        // only in memory would be null exactly when a reminder is due.
        let clock = 0;
        const storage = fakeStorage();
        const alarms = fakeAlarms();

        const first = durableObjectReminders({ storage, alarms, now: () => clock });
        const siloA = createSilo({
            actors: [Waking],
            storage: durableObjectStorage(storage),
            reminders: first,
            scheduler: manualScheduler(),
            defaults: { sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        await siloA.start();
        await siloA.actor(Waking, 'cold').armIn(1_000);
        await siloA.stop({ timeoutMs: 1000 });

        // Everything in memory is gone; only the DO's storage survives.
        clock = 1_000;
        alarms.at = null;
        const revived = durableObjectReminders({ storage, alarms, now: () => clock });
        const siloB = createSilo({
            actors: [Waking],
            storage: durableObjectStorage(storage),
            reminders: revived,
            scheduler: manualScheduler(),
            defaults: { sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        await siloB.start();
        try {
            // No apiFor() has been called on `revived` — the owner has to
            // come back from storage.
            await revived.onAlarm();
            await expect(siloB.actor(Waking, 'cold').woke()).resolves.toBe(1);
        } finally {
            await siloB.stop({ timeoutMs: 1000 });
        }
    });

    it('refuses a second actor in the same Durable Object', async () => {
        let clock = 0;
        const { silo } = await siloWithAlarms(() => clock);
        try {
            await silo.actor(Waking, 'first').armIn(1_000);
            // One DO hosts one actor; a second identity means the request
            // was routed to the wrong object.
            await expect(silo.actor(Waking, 'second').armIn(1_000)).rejects.toThrow(
                /hosts Waking\/first/
            );
        } finally {
            await silo.stop({ timeoutMs: 1000 });
        }
    });

    it('honours a reminder rescheduled from inside onReminder', async () => {
        // Rescheduling from the handler is a normal pattern. The write
        // happens DURING delivery, so arming from the table loaded before
        // delivery would overwrite the alarm it just set.
        let clock = 0;
        const storage = fakeStorage();
        const alarms = fakeAlarms();
        const Rescheduling = defineActor({
            type: 'Rescheduling',
            unguarded: true,
            state: () => ({ fired: 0 }),
            async onReminder(ctx) {
                ctx.state.fired++;
                if (ctx.state.fired === 1) {
                    // Ask to be woken again, sooner than anything else.
                    await ctx.reminders.set('again', { due: 10 });
                }
            },
            methods: (ctx) => ({
                async arm() {
                    await ctx.reminders.set('first', { due: 100 });
                    // A LATER reminder has to be present, otherwise the stale
                    // snapshot is empty and rearm has nothing to get wrong.
                    await ctx.reminders.set('slow', { due: 60_000, period: 60_000 });
                },
                async fired() {
                    return ctx.state.fired;
                }
            })
        });
        const reminders = durableObjectReminders({ storage, alarms, now: () => clock });
        const silo = createSilo({
            actors: [Rescheduling],
            storage: durableObjectStorage(storage),
            reminders,
            scheduler: manualScheduler(),
            defaults: { sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        await silo.start();
        try {
            await silo.actor(Rescheduling, 'r').arm();
            // Earliest of the two wins.
            expect(alarms.at).toBe(100);

            clock = 100;
            alarms.at = null;
            await reminders.onAlarm();
            // The handler set 'again' for t=110. Arming from the snapshot
            // loaded BEFORE delivery would see only 'slow' and push the
            // alarm out to 60_000, losing the sooner one.
            expect(alarms.at).toBe(110);

            clock = 110;
            alarms.at = null;
            await reminders.onAlarm();
            await expect(silo.actor(Rescheduling, 'r').fired()).resolves.toBe(2);
        } finally {
            await silo.stop({ timeoutMs: 1000 });
        }
    });

    it('runs every table read-modify-write inside the concurrency gate', async () => {
        // onAlarm() is called straight from the object's alarm() handler,
        // OUTSIDE any mailbox turn — so unlike actor state, its
        // read-modify-write really can interleave with a concurrent
        // ctx.reminders.set. The gate is what serializes them.
        let clock = 0;
        let depth = 0;
        let maxDepth = 0;
        const gated: string[] = [];
        const blockConcurrencyWhile = async <T,>(fn: () => Promise<T>): Promise<T> => {
            depth++;
            maxDepth = Math.max(maxDepth, depth);
            try {
                return await fn();
            } finally {
                depth--;
            }
        };
        const storage = fakeStorage();
        const alarms = fakeAlarms();
        const reminders = durableObjectReminders({
            storage,
            alarms,
            now: () => clock,
            blockConcurrencyWhile: async (fn) => {
                gated.push('call');
                return blockConcurrencyWhile(fn);
            }
        });
        const silo = createSilo({
            actors: [Waking],
            storage: durableObjectStorage(storage),
            reminders,
            scheduler: manualScheduler(),
            defaults: { sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        await silo.start();
        try {
            await silo.actor(Waking, 'g').armIn(100);
            clock = 100;
            alarms.at = null;
            await reminders.onAlarm();
            // set() and onAlarm() both went through it...
            expect(gated.length).toBeGreaterThanOrEqual(2);
            // ...and never nested, which would deadlock a real
            // blockConcurrencyWhile.
            expect(maxDepth).toBe(1);
            await expect(silo.actor(Waking, 'g').woke()).resolves.toBe(1);
        } finally {
            await silo.stop({ timeoutMs: 1000 });
        }
    });

    it('refuses an alarm before the silo bound its reminders', async () => {
        const reminders = durableObjectReminders({
            storage: fakeStorage(),
            alarms: fakeAlarms()
        });
        // Silent would be worse than loud: the platform has already cleared
        // the alarm, so a dropped periodic never fires again.
        await expect(reminders.onAlarm()).rejects.toThrow(/before the silo bound/);
    });

    it('keeps the runtime 60s period floor', async () => {
        let clock = 0;
        const { silo } = await siloWithAlarms(() => clock);
        try {
            await expect(silo.actor(Waking, 'f').armIn(0, 1_000)).rejects.toThrow(/floor/);
        } finally {
            await silo.stop({ timeoutMs: 1000 });
        }
    });
});
