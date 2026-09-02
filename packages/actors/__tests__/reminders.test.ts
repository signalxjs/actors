import { afterEach, describe, expect, it, vi } from 'vitest';
import {
    ActorStorageConflict,
    defineActor,
    type ActorDispatcher,
    type ActorPlacement,
    type ActorStorage
} from '@sigx/actors';
import {
    createHost,
    manualScheduler,
    memoryStorage,
    shardedReminders,
    timerScheduler,
    REMINDER_TYPE,
    type Host
} from '@sigx/actors/host';
// The service class itself is internal — imported directly for the
// concurrent-ticker CAS test.
import { ReminderService } from '../src/host/reminders';
import { reminderShardOf } from '../src/host/reminder-shards';
import { emptyHostStats } from '../src/types';
import { peekHost, type ActorReminders, type ActorRemindersContext } from '@sigx/actors';

let running: Host | null = null;
afterEach(async () => {
    await running?.stop({ timeoutMs: 1000 });
    running = null;
});

function wakingActor(events: string[]) {
    return defineActor({
        type: 'Waking',
        allowAnonymous: true,
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
        const host = createHost({
            actors: [wakingActor(events)],
            defaults: { reminderTickMs: 60_000, sweepIntervalMs: 60_000 }
        });
        const bad = defineActor({
            type: 'BadPeriod',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx) => ({
                async go() {
                    await ctx.reminders.set('x', { due: 0, period: 1000 });
                }
            })
        });
        const host2 = createHost({ actors: [bad], defaults: { reminderTickMs: 60_000 } });
        await expect(host2.actor(bad, 'k').go()).rejects.toThrow(/period must be >= 60000ms/);
        void host;
    });

    it('a due reminder RE-ACTIVATES an idle actor and one-shots clear themselves', async () => {
        const events: string[] = [];
        const storage = memoryStorage();
        const def = wakingActor(events);
        const host = createHost({
            actors: [def],
            storage,
            defaults: { reminderTickMs: 25, sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        running = host;
        await host.start();

        const client = host.actor(def, 'r1');
        await client.wakeMeIn(0);
        // Deactivate — the reminder must bring it back.
        await host.deactivateType('Waking');
        expect(host.stats().activations).toBe(0);

        await vi.waitFor(() => expect(events).toContain('reminder:wake'), { timeout: 3000 });
        // Re-activated by the reminder (a second activate event).
        expect(events.filter((e) => e === 'activate:r1').length).toBe(2);
        // One-shot: cleared after firing.
        await vi.waitFor(async () => {
            await expect(client.listReminders()).resolves.toEqual([]);
        });
    });

    it('reminders survive a host restart on shared storage', async () => {
        const events: string[] = [];
        const storage = memoryStorage();
        const def = wakingActor(events);
        const hostA = createHost({
            actors: [def],
            storage,
            defaults: { reminderTickMs: 25, sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        await hostA.start();
        await hostA.actor(def, 'r2').wakeMeIn(50);
        await hostA.stop();
        expect(events).not.toContain('reminder:wake');

        const hostB = createHost({
            actors: [def],
            storage,
            defaults: { reminderTickMs: 25, sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        running = hostB;
        await hostB.start();
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
        const host = createHost({
            actors: [def],
            storage: memoryStorage(),
            placement,
            defaults: { reminderTickMs: 25, sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        running = host;
        await host.start();
        await host.actor(def, 'g1').wakeMeIn(0);

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
        const host = createHost({
            actors: [def],
            storage,
            defaults: { reminderTickMs: 60_000, sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        for (const key of ['a', 'b', 'c', 'd', 'e']) {
            await host.actor(def, key).wakeMeIn(120_000);
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
        await expect(host.actor(def, 'a').listReminders()).resolves.toEqual(['wake']);
    });

    it('two tickers on the same shard fire a reminder exactly once (CAS, no lease)', async () => {
        const storage = memoryStorage();
        const fired: string[] = [];
        // Two services both claim ownership of every shard — the divergent
        // membership-view case. The per-shard etag CAS must arbitrate.
        const ticker = (label: string): ReminderService => {
            const service = new ReminderService();
            service.bind({
                storage,
                scheduler: timerScheduler(),
                tickMs: 20,
                ownsShard: () => true,
                deliver: async (_ref, name) => void fired.push(`${label}:${name}`)
            });
            return service;
        };
        const a = ticker('A');
        const b = ticker('B');
        await a.apiFor({ type: 'Waking', key: 'race' }).set('wake', { due: 0 });
        a.start();
        b.start();
        try {
            await vi.waitFor(() => expect(fired.length).toBeGreaterThan(0), { timeout: 3000 });
            await new Promise((r) => setTimeout(r, 150)); // several tick windows
            expect(fired).toHaveLength(1);
        } finally {
            a.stop();
            b.stop();
        }
    });

    it('a tick that changes nothing writes nothing', async () => {
        // The tick loop visits every owned shard on every tick, whether or
        // not that shard holds anything. Persisting a table it did not
        // change is pure write amplification: an idle host would rewrite all
        // 16 shard records every `reminderTickMs`, forever. With
        // `fileStorage` that also means 16 temp-file+rename pairs inside the
        // project tree every tick, which is what trips Vite's HMR reader.
        const base = memoryStorage();
        let saves = 0;
        const storage: ActorStorage = {
            load: (type, key) => base.load(type, key),
            save: (type, key, state, expectedEtag) => {
                if (type === REMINDER_TYPE) saves++;
                return base.save(type, key, state, expectedEtag);
            },
            clear: (type, key, expectedEtag) => base.clear(type, key, expectedEtag)
        };
        const fired: string[] = [];
        const service = new ReminderService();
        service.bind({
            storage,
            scheduler: timerScheduler(),
            tickMs: 10,
            ownsShard: () => true,
            deliver: async (_ref, name) => void fired.push(name)
        });

        // One reminder, far from due — so every shard is either empty or
        // holds nothing the tick may advance.
        await service.apiFor({ type: 'Waking', key: 'idle' }).set('wake', { due: 120_000 });
        expect(saves).toBe(1); // the set() itself

        service.start();
        try {
            await new Promise((r) => setTimeout(r, 120)); // ~12 ticks × 16 shards
            expect(saves).toBe(1);
            expect(fired).toEqual([]);
        } finally {
            service.stop();
        }
        // ...and the untouched shards were never brought into existence.
        const written: string[] = [];
        for (const shard of ['p0', 'p1', 'p2', 'p3']) {
            if (await storage.load(REMINDER_TYPE, shard)) written.push(shard);
        }
        expect(written.length).toBeLessThanOrEqual(1);
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
                // simulate another host winning the first CAS.
                if (type === REMINDER_TYPE && conflicts === 0) {
                    conflicts++;
                    throw new ActorStorageConflict(type, key);
                }
                return base.save(type, key, state, expectedEtag);
            },
            clear: (type, key, expectedEtag) => base.clear(type, key, expectedEtag)
        };
        const host = createHost({
            actors: [def],
            storage,
            defaults: { reminderTickMs: 60_000, sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        const client = host.actor(def, 'cas');
        await expect(client.wakeMeIn(120_000)).resolves.toBeUndefined();
        expect(conflicts).toBe(1);
        await expect(client.listReminders()).resolves.toEqual(['wake']);
    });
});

describe('a failed dispatch is retried, not lost (#306)', () => {
    // The shard entry is advanced / deleted BEFORE `deliver()` runs (the
    // at-most-once CAS design). Under overload the dispatch is exactly what
    // fails — a deadline, a host mid-restart — and the wake used to be gone
    // with it. A failed dispatch now re-arms the entry one tick out, so it
    // costs a tick rather than the wake, and is counted.
    const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
    const TICK = 5;
    const ref = { type: 'Waking', key: 'retry' };
    const id = `${ref.type}\u0000${ref.key}`;
    const shard = reminderShardOf(id);

    async function entryOf(storage: ActorStorage, name: string) {
        const record = await storage.load(REMINDER_TYPE, shard);
        const table = (record?.state ?? {}) as Record<
            string,
            Record<string, { nextDue: number; period?: number }>
        >;
        return table[id]?.[name];
    }

    function failing(storage: ActorStorage, failures: () => boolean) {
        const scheduler = manualScheduler();
        const attempts: string[] = [];
        const undelivered: { name: string; error: unknown }[] = [];
        const service = new ReminderService();
        service.bind({
            storage,
            scheduler,
            tickMs: TICK,
            ownsShard: () => true,
            deliver: async (_ref, name) => {
                attempts.push(name);
                if (failures()) throw new Error('dispatch deadline');
            },
            undelivered: (_ref, name, error) => void undelivered.push({ name, error })
        });
        return { service, scheduler, attempts, undelivered };
    }

    it('re-arms a one-shot whose dispatch failed for the next tick', async () => {
        const storage = memoryStorage();
        let fail = true;
        const { service, scheduler, attempts, undelivered } = failing(storage, () => fail);
        const api = service.apiFor(ref);
        await api.set('wake', { due: 0 });
        const armed = (await entryOf(storage, 'wake'))!.nextDue;
        service.start();
        try {
            const before = Date.now();
            scheduler.advance(TICK); // tick 1: deliver rejects
            await vi.waitFor(() => expect(attempts).toEqual(['wake']));
            // Still registered — nudged one tick out, not dropped.
            await vi.waitFor(async () => {
                const entry = await entryOf(storage, 'wake');
                expect(entry).toBeDefined();
                expect(entry!.nextDue).toBeGreaterThanOrEqual(before + TICK);
                expect(entry!.nextDue).toBeLessThanOrEqual(Date.now() + TICK);
                expect(entry!.nextDue).toBeGreaterThan(armed);
            });
            expect(undelivered).toHaveLength(1);
            expect(undelivered[0]!.name).toBe('wake');
            expect((undelivered[0]!.error as Error).message).toBe('dispatch deadline');

            fail = false;
            await sleep(TICK * 2);
            scheduler.advance(TICK); // tick 2: delivered
            await vi.waitFor(() => expect(attempts).toEqual(['wake', 'wake']));
            // A one-shot that finally fired clears itself.
            await vi.waitFor(async () => expect(await api.list()).toEqual([]));
            expect(undelivered).toHaveLength(1);
        } finally {
            service.stop();
        }
    });

    it('retries a periodic reminder next tick rather than a period later', async () => {
        const storage = memoryStorage();
        let fail = true;
        const { service, scheduler, attempts } = failing(storage, () => fail);
        const api = service.apiFor(ref);
        await api.set('beat', { due: 0, period: 60_000 });
        service.start();
        try {
            const before = Date.now();
            scheduler.advance(TICK);
            await vi.waitFor(() => expect(attempts).toEqual(['beat']));
            let rearmed = 0;
            await vi.waitFor(async () => {
                const entry = await entryOf(storage, 'beat');
                expect(entry!.period).toBe(60_000);
                expect(entry!.nextDue).toBeLessThanOrEqual(Date.now() + TICK);
                expect(entry!.nextDue).toBeGreaterThanOrEqual(before + TICK);
                rearmed = entry!.nextDue;
            });

            fail = false;
            await sleep(TICK * 2);
            scheduler.advance(TICK);
            await vi.waitFor(() => expect(attempts).toEqual(['beat', 'beat']));
            // A SUCCESSFUL firing advances by the period, as before.
            await vi.waitFor(async () => {
                const entry = await entryOf(storage, 'beat');
                expect(entry!.nextDue).toBeGreaterThanOrEqual(rearmed + 60_000);
            });
        } finally {
            service.stop();
        }
    });

    it('a permanently failing target costs one attempt per tick, no hotter', async () => {
        const storage = memoryStorage();
        const { service, scheduler, attempts, undelivered } = failing(storage, () => true);
        const api = service.apiFor(ref);
        await api.set('wake', { due: 0 });
        service.start();
        try {
            // One attempt per `advance()` — the guard is the count inside
            // the loop: a re-arm inside the tick would show as a second
            // attempt before the next advance. (A `manualScheduler()` never
            // ticks on its own, so waiting between ticks proves nothing.)
            for (let tick = 1; tick <= 4; tick++) {
                scheduler.advance(TICK);
                await vi.waitFor(() => expect(attempts).toHaveLength(tick));
                await sleep(TICK * 2);
                expect(attempts).toHaveLength(tick);
            }
            expect(undelivered).toHaveLength(4);
            await expect(api.list()).resolves.toEqual(['wake']);
        } finally {
            service.stop();
        }
    });

    /** A service whose every `deliver()` hangs until `reject()` fails them all. */
    function hanging(storage: ActorStorage) {
        let reject!: (error: Error) => void;
        const pending = new Promise<void>((_, r) => {
            reject = r;
        });
        const scheduler = manualScheduler();
        const attempts: string[] = [];
        const service = new ReminderService();
        service.bind({
            storage,
            scheduler,
            tickMs: TICK,
            ownsShard: () => true,
            deliver: (_ref, name) => {
                attempts.push(name);
                return pending;
            }
        });
        return { service, scheduler, attempts, reject };
    }

    it('does not overwrite a reminder the actor set again while its dispatch was failing', async () => {
        const storage = memoryStorage();
        const { service, scheduler, reject } = hanging(storage);
        const api = service.apiFor(ref);
        await api.set('wake', { due: 0 });
        service.start();
        try {
            scheduler.advance(TICK);
            // The entry is gone (deleted before dispatch) while delivery is
            // in flight...
            await vi.waitFor(async () => expect(await api.list()).toEqual([]));
            // ...the actor sets it again for later, from some other turn...
            await api.set('wake', { due: 120_000 });
            const rearmed = (await entryOf(storage, 'wake'))!.nextDue;
            // ...and THEN the original dispatch fails. The retry must not
            // overwrite the actor's own, later, decision.
            reject(new Error('dispatch deadline'));
            await sleep(TICK * 4);
            expect((await entryOf(storage, 'wake'))!.nextDue).toBe(rearmed);
        } finally {
            service.stop();
        }
    });

    it('does not resurrect a periodic reminder cleared while its dispatch was failing', async () => {
        const storage = memoryStorage();
        const { service, scheduler, attempts, reject } = hanging(storage);
        const api = service.apiFor(ref);
        await api.set('beat', { due: 0, period: 60_000 });
        service.start();
        try {
            scheduler.advance(TICK);
            await vi.waitFor(() => expect(attempts).toEqual(['beat']));
            // The advanced entry is still on the table; the clear removes it,
            // so the failed dispatch finds nothing to pull forward.
            await api.clear('beat');
            reject(new Error('dispatch deadline'));
            await sleep(TICK * 4);
            expect(await entryOf(storage, 'beat')).toBeUndefined();
            scheduler.advance(TICK);
            await sleep(TICK * 4);
            expect(attempts).toEqual(['beat']);
        } finally {
            service.stop();
        }
    });

    it('a one-shot cleared while its dispatch was failing is retried once (documented)', async () => {
        // The tick deleted the one-shot before dispatching, so the clear was a
        // no-op on the table and the re-arm cannot tell "cleared" from
        // "untouched" — it re-inserts the entry. That is the documented
        // exception to "a later decision wins" (module header, CHANGELOG):
        // `onReminder` must be idempotent anyway, and a tombstone would cost a
        // write on every clear of an already-fired reminder. This pins the
        // behaviour so a change to it is a deliberate one.
        const storage = memoryStorage();
        const { service, scheduler, attempts, reject } = hanging(storage);
        const api = service.apiFor(ref);
        await api.set('wake', { due: 0 });
        service.start();
        try {
            scheduler.advance(TICK);
            await vi.waitFor(() => expect(attempts).toEqual(['wake']));
            await api.clear('wake');
            reject(new Error('dispatch deadline'));
            await vi.waitFor(async () => expect(await entryOf(storage, 'wake')).toBeDefined());
        } finally {
            service.stop();
        }
    });

    it("re-arms a shard's failed dispatches in one write, not one per failure", async () => {
        // The re-arm is the write that happens under overload — one `#mutate`
        // per failure would queue N load+CAS round trips through the host's
        // single writer chain, ahead of every actor's own set/clear (#307).
        const inner = memoryStorage();
        let saves = 0;
        const storage: ActorStorage = {
            ...inner,
            save: (...args) => {
                saves++;
                return inner.save(...args);
            }
        };
        const { service, scheduler, attempts } = failing(storage, () => true);
        const api = service.apiFor(ref); // one actor: every name lands on one shard
        await api.set('a', { due: 0 });
        await api.set('b', { due: 0 });
        await api.set('c', { due: 0, period: 60_000 });
        saves = 0;
        service.start();
        try {
            scheduler.advance(TICK);
            await vi.waitFor(() => expect(attempts.sort()).toEqual(['a', 'b', 'c']));
            await vi.waitFor(async () =>
                expect((await api.list()).sort()).toEqual(['a', 'b', 'c'])
            );
            // The tick's own advance/delete, then a single re-arm.
            expect(saves).toBe(2);
        } finally {
            service.stop();
        }
    });

    it('counts undelivered reminders on the host (HostStats.remindersUndelivered)', async () => {
        let fails = 1;
        const attempts: string[] = [];
        const def = defineActor({
            type: 'Flaky',
            allowAnonymous: true,
            state: () => ({}),
            onReminder(_ctx, name) {
                attempts.push(name);
                if (fails-- > 0) throw new Error('not this time');
            },
            methods: (ctx) => ({
                async wakeMeIn(ms: number) {
                    await ctx.reminders.set('wake', { due: ms });
                }
            })
        });
        const host = createHost({
            actors: [def],
            storage: memoryStorage(),
            defaults: { reminderTickMs: 25, sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        running = host;
        await host.start();
        expect(host.stats().remindersUndelivered).toBe(0);
        expect(emptyHostStats().remindersUndelivered).toBe(0);
        await host.actor(def, 'f1').wakeMeIn(0);
        await vi.waitFor(() => expect(attempts).toEqual(['wake', 'wake']), { timeout: 3000 });
        expect(host.stats().remindersUndelivered).toBe(1);
    });
});

describe('reminders are pluggable', () => {
    it('accepts a custom ActorReminders instead of the sharded table', async () => {
        // Stands in for the Cloudflare shape: no shard table, no polling —
        // reminders held per actor and fired directly, the way a Durable
        // Object's alarm would.
        const set: string[] = [];
        let deliver: ActorRemindersContext['deliver'] | null = null;
        const perActor: ActorReminders = {
            bind(context) {
                deliver = context.deliver;
            },
            start() {},
            stop() {},
            apiFor(ref) {
                return {
                    set: async (name) => void set.push(`${ref.key}/${name}`),
                    clear: async () => {},
                    // Names only — the ReminderApi contract. The key is this
                    // stub's own bookkeeping, not part of the return value.
                    list: async () =>
                        set
                            .filter((entry) => entry.startsWith(`${ref.key}/`))
                            .map((entry) => entry.slice(ref.key.length + 1))
                };
            }
        };

        const events: string[] = [];
        const def = wakingActor(events);
        const host = createHost({
            actors: [def],
            reminders: perActor,
            defaults: { sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        await host.start();
        try {
            await host.actor(def, 'a').wakeMeIn(0);
            // Went to the plugin, not to `$sigx:reminders` in storage.
            expect(set).toEqual(['a/wake']);

            // And the host handed it a working delivery callback: firing it
            // re-activates the actor and runs onReminder.
            await deliver!({ type: 'Waking', key: 'a' }, 'wake');
            expect(events).toContain('reminder:wake');
            // The stub honours the ReminderApi contract: names, not keys.
            await expect(host.actor(def, 'a').listReminders()).resolves.toEqual(['wake']);
        } finally {
            await host.stop({ timeoutMs: 1000 });
        }
    });
});

describe('reminders lifecycle is strict', () => {
    it('leaves nothing running when reminders fail to start', async () => {
        const scheduler = manualScheduler();
        const exploding: ActorReminders = {
            bind() {},
            start() {
                return Promise.reject(new Error('alarm unavailable'));
            },
            stop() {},
            apiFor: () => ({
                set: async () => {},
                clear: async () => {},
                list: async () => []
            })
        };
        const host = createHost({
            actors: [wakingActor([])],
            reminders: exploding,
            scheduler,
            defaults: { sweepIntervalMs: 1_000, callTimeoutMs: 0 }
        });
        await expect(host.start()).rejects.toThrow('alarm unavailable');
        // The sweeper must NOT have been registered by a start that failed.
        expect(scheduler.pending).toBe(0);
        // ...and the seam must not be stamped with a host that is not running.
        expect(peekHost()).toBeUndefined();
    });

    it('makes concurrent start() calls a real barrier', async () => {
        let started = false;
        let release!: () => void;
        const gate = new Promise<void>((resolve) => {
            release = resolve;
        });
        const slow: ActorReminders = {
            bind() {},
            async start() {
                await gate;
                started = true;
            },
            stop() {},
            apiFor: () => ({
                set: async () => {},
                clear: async () => {},
                list: async () => []
            })
        };
        const host = createHost({
            actors: [wakingActor([])],
            reminders: slow,
            scheduler: manualScheduler(),
            defaults: { sweepIntervalMs: 1_000, callTimeoutMs: 0 }
        });
        const first = host.start();
        // Await ONLY the second caller: if it gets its own already-resolved
        // promise instead of the in-flight one, it returns before startup
        // has happened. (Awaiting both would hide that behind the first.)
        const second = host.start();
        setTimeout(release, 0);
        await second;
        expect(started).toBe(true);
        await first;
        await host.stop({ timeoutMs: 1000 });
    });

    it('lets a stop racing an in-flight start tear it down anyway', async () => {
        const scheduler = manualScheduler();
        let release!: () => void;
        const gate = new Promise<void>((resolve) => {
            release = resolve;
        });
        const slow: ActorReminders = {
            bind() {},
            start: () => gate,
            stop() {},
            apiFor: () => ({
                set: async () => {},
                clear: async () => {},
                list: async () => []
            })
        };
        const host = createHost({
            actors: [wakingActor([])],
            reminders: slow,
            scheduler,
            defaults: { sweepIntervalMs: 1_000, callTimeoutMs: 0 }
        });
        const starting = host.start();
        // Stop while the start is still in flight; it must not let that
        // start's continuation re-register the sweeper behind our back.
        const stopping = host.stop({ timeoutMs: 1000 });
        release();
        await Promise.all([starting, stopping]);
        expect(scheduler.pending).toBe(0);
        expect(peekHost()).toBeUndefined();
    });

    it('refuses to bind one reminders instance to two hosts', () => {
        const service = shardedReminders();
        const context = {
            storage: memoryStorage(),
            scheduler: manualScheduler(),
            tickMs: 1_000,
            ownsShard: () => true,
            deliver: async () => undefined
        };
        service.bind(context);
        expect(() => service.bind(context)).toThrow(/already bound/);
    });
});
