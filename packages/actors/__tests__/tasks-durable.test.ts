import { afterEach, describe, expect, it, vi } from 'vitest';
import { defineActor } from '@sigx/actors';
import {
    createHost,
    memoryStorage,
    REMINDER_TYPE,
    ROSTER_INDEX_KEY,
    ROSTER_TYPE,
    TASKS_TYPE,
    TASK_REMINDER,
    type Host,
    type TaskLedger
} from '@sigx/actors/host';
import { reminderShardOf } from '../src/host/reminder-shards';

const quiet = { sweepIntervalMs: 600_000, reminderTickMs: 600_000, callTimeoutMs: 0 };

function within<T>(promise: Promise<T>, ms: number): Promise<T> {
    return Promise.race([
        promise,
        new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error(`did not settle within ${ms}ms`)), ms).unref?.()
        )
    ]);
}

function gate(): { open: () => void; opened: Promise<void> } {
    let open!: () => void;
    const opened = new Promise<void>((r) => (open = r));
    return { open, opened };
}

function aborted(signal: AbortSignal): Promise<void> {
    return signal.aborted
        ? Promise.resolve()
        : new Promise((r) => signal.addEventListener('abort', () => r(), { once: true }));
}

async function until(cond: () => boolean | Promise<boolean>, ms = 3000): Promise<void> {
    for (let waited = 0; !(await cond()); waited += 5) {
        if (waited > ms) throw new Error('condition not reached');
        await new Promise((r) => setTimeout(r, 5));
    }
}

let running: Host | null = null;
afterEach(async () => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    await running?.stop({ timeoutMs: 1000 });
    running = null;
});

/** An actor whose task records each (re)start and parks until aborted. */
function parkingWorker(starts: { label: string; sinceIso: string; restarts: number }[]) {
    return defineActor({
        type: 'Durable',
        allowAnonymous: true,
        state: () => ({}),
        methods: (ctx) => ({
            begin: (input: { label: string; since: Date }) => ctx.tasks.start('run', input),
            cancel: () => ctx.tasks.cancel('run'),
            running: () => ctx.tasks.list(),
            reminders: () => ctx.reminders.list()
        }),
        tasks: (ctx) => ({
            async run(input: { label: string; since: Date }) {
                starts.push({
                    label: input.label,
                    // Proves the input rode the state codec (a real Date).
                    sinceIso: input.since.toISOString(),
                    restarts: ctx.tasks.list().find((t) => t.name === 'run')?.restarts ?? -1
                });
                await aborted(ctx.abortSignal);
            }
        })
    });
}

const ID = 'Durable\u0000a';
const SINCE = new Date('2026-07-29T10:00:00.000Z');

describe('task durability', () => {
    it('start is durable before it resolves: ledger entry + roster entry (#310)', async () => {
        const starts: never[] = [];
        const storage = memoryStorage();
        const def = parkingWorker(starts);
        const host = createHost({ actors: [def], storage, defaults: quiet });
        running = host;
        const client = host.actor(def, 'a');
        await client.begin({ label: 'sync', since: SINCE });

        const record = await storage.load(TASKS_TYPE, ID);
        expect(record).not.toBeNull();
        const ledger = record!.state as TaskLedger;
        expect(Object.keys(ledger)).toEqual(['run']);
        expect(ledger.run!.restarts).toBe(0);
        // Liveness is the per-host roster now, not a reminder per task.
        expect(await client.reminders()).not.toContain(TASK_REMINDER);
        const index = (await storage.load(ROSTER_TYPE, ROSTER_INDEX_KEY))!.state as Record<string, number>;
        const [hostId] = Object.keys(index);
        const roster = (await storage.load(ROSTER_TYPE, `${hostId}/${reminderShardOf(ID)}`))!.state as Record<string, number>;
        expect(Object.keys(roster)).toEqual([ID]);
    });

    it('two CONCURRENT starts of one name launch exactly one run', async () => {
        const entries: number[] = [];
        const release = gate();
        const storage = memoryStorage();
        const def = defineActor({
            type: 'Durable',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx) => ({
                // Both starts race inside one turn — the has() gate alone
                // cannot close this, because start awaits durable writes
                // before (previously) registering the run.
                beginTwice: () => Promise.all([ctx.tasks.start('run'), ctx.tasks.start('run')]),
                running: () => ctx.tasks.list()
            }),
            tasks: () => ({
                async run() {
                    entries.push(1);
                    await release.opened;
                }
            })
        });
        const host = createHost({ actors: [def], storage, defaults: quiet });
        running = host;
        const client = host.actor(def, 'a');
        await client.beginTwice();
        expect(entries).toHaveLength(1);
        expect(await client.running()).toHaveLength(1);
        const ledger = (await storage.load(TASKS_TYPE, ID))!.state as TaskLedger;
        expect(Object.keys(ledger)).toEqual(['run']);
        release.open();
    });

    it('completion clears the ledger record and disarms the reminder', async () => {
        const storage = memoryStorage();
        const done = gate();
        const def = defineActor({
            type: 'Durable',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('run'),
                reminders: () => ctx.reminders.list()
            }),
            tasks: () => ({
                async run() {
                    done.open();
                }
            })
        });
        const host = createHost({ actors: [def], storage, defaults: quiet });
        running = host;
        const client = host.actor(def, 'a');
        await client.begin();
        await within(done.opened, 1000);
        await until(async () => (await storage.load(TASKS_TYPE, ID)) === null);
        await until(async () => !(await client.reminders()).includes(TASK_REMINDER));
    });

    it('cancel forgets durably — a cancelled run does not resume', async () => {
        const starts: { label: string }[] = [];
        const storage = memoryStorage();
        const def = parkingWorker(starts as never);
        const hostA = createHost({ actors: [def], storage, defaults: quiet });
        const a = hostA.actor(def, 'a');
        await a.begin({ label: 'sync', since: SINCE });
        await a.cancel();
        await until(async () => (await storage.load(TASKS_TYPE, ID)) === null);
        await hostA.stop({ timeoutMs: 1000 });

        const hostB = createHost({ actors: [def], storage, defaults: quiet });
        running = hostB;
        expect(await hostB.actor(def, 'a').running()).toEqual([]);
        expect(starts).toHaveLength(1); // the original start only
    });

    it('a thrown task is terminal — no ledger entry survives to resume', async () => {
        vi.spyOn(console, 'error').mockImplementation(() => {});
        const storage = memoryStorage();
        const def = defineActor({
            type: 'Durable',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('run'),
                running: () => ctx.tasks.list()
            }),
            tasks: () => ({
                async run() {
                    throw new Error('kaboom');
                }
            })
        });
        const hostA = createHost({ actors: [def], storage, defaults: quiet });
        await hostA.actor(def, 'a').begin();
        await until(async () => (await storage.load(TASKS_TYPE, ID)) === null);
        await hostA.stop({ timeoutMs: 1000 });

        const hostB = createHost({ actors: [def], storage, defaults: quiet });
        running = hostB;
        expect(await hostB.actor(def, 'a').running()).toEqual([]);
    });

    it('a shutdown-interrupted run keeps its entry and resumes on the next activation', async () => {
        const starts: { label: string; sinceIso: string; restarts: number }[] = [];
        const storage = memoryStorage();
        const def = parkingWorker(starts);
        const hostA = createHost({ actors: [def], storage, defaults: quiet });
        await hostA.actor(def, 'a').begin({ label: 'sync', since: SINCE });
        await until(() => starts.length === 1);
        await within(hostA.stop({ timeoutMs: 5000 }), 2000);

        // Interrupted, not finished: the entry survived the stop.
        expect(await storage.load(TASKS_TYPE, ID)).not.toBeNull();

        const hostB = createHost({ actors: [def], storage, defaults: quiet });
        running = hostB;
        // ANY dispatch re-activates — activation itself restarts the run.
        const info = await hostB.actor(def, 'a').running();
        await until(() => starts.length === 2);
        expect(starts[1]).toEqual({
            label: 'sync',
            sinceIso: SINCE.toISOString(), // the Date round-tripped the codec
            restarts: 1
        });
        expect(info.find((t) => t.name === 'run')?.restarts).toBe(1);
    });

    it('the liveness reminder resumes a dead host\'s task with NO client call', async () => {
        const starts: { label: string; restarts: number }[] = [];
        const storage = memoryStorage();
        const def = parkingWorker(starts as never);
        const hostA = createHost({ actors: [def], storage, defaults: quiet });
        await hostA.start();
        await hostA.actor(def, 'a').begin({ label: 'sync', since: SINCE });
        await until(() => starts.length === 1);
        await within(hostA.stop({ timeoutMs: 5000 }), 2000);

        // Fake only Date (setTimeout stays real): jump past the reminder's
        // 60s due, then let host B's real 25ms tick find it due.
        vi.useFakeTimers({ toFake: ['Date'], now: Date.now() + 61_000 });
        const hostB = createHost({
            actors: [def],
            storage,
            defaults: { ...quiet, reminderTickMs: 25 }
        });
        running = hostB;
        await hostB.start();
        await until(() => starts.length === 2, 5000);
        expect(starts[1]!.restarts).toBe(1);
        expect(hostB.stats().activations).toBe(1); // reminder woke the actor
    });

    it('a stale liveness reminder with no ledger self-clears', async () => {
        const storage = memoryStorage();
        const def = parkingWorker([]);
        // Hand-craft the orphan: a due liveness reminder, no ledger record.
        const shard = reminderShardOf(ID);
        await storage.save(
            REMINDER_TYPE,
            shard,
            { [ID]: { [TASK_REMINDER]: { nextDue: Date.now() - 1000, period: 60_000 } } },
            null
        );
        const host = createHost({
            actors: [def],
            storage,
            defaults: { ...quiet, reminderTickMs: 25 }
        });
        running = host;
        await host.start();
        // The tick delivers, the interception finds nothing, and disarms.
        await until(async () => {
            const record = await storage.load(REMINDER_TYPE, shard);
            const table = (record?.state ?? {}) as Record<string, Record<string, unknown>>;
            return !table[ID] || !(TASK_REMINDER in table[ID]);
        }, 5000);
        // And it never invented a run.
        expect(await host.actor(def, 'a').running()).toEqual([]);
    });
});
