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
import { peekHost, type ActorReminders, type ActorRemindersContext } from '@sigx/actors';

let running: Host | null = null;
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
        const host = createHost({
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
