/**
 * Task liveness (#310): a running task is found again after its host dies
 * through a per-host ROSTER, not a reminder per task.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { defineActor } from '@sigx/actors';
import { defineJob } from '@sigx/actors/job';
import {
    createHost,
    memoryStorage,
    manualScheduler,
    reminderTaskLiveness,
    REMINDER_TYPE,
    rosterTaskLiveness,
    ROSTER_INDEX_KEY,
    ROSTER_TYPE,
    TASKS_TYPE,
    TASK_REMINDER,
    type Host
} from '@sigx/actors/host';
import { reminderShardOf } from '../src/host/reminder-shards';
import { createCluster, selfPolicy, type ClusterHarness } from './harness';

const quiet = { sweepIntervalMs: 600_000, reminderTickMs: 600_000, callTimeoutMs: 0 };
/** A host whose adoption tick is fast enough to observe. */
const ticking = { ...quiet, reminderTickMs: 25 };

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

function within<T>(promise: Promise<T>, ms: number): Promise<T> {
    return Promise.race([
        promise,
        new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error(`did not settle within ${ms}ms`)), ms).unref?.()
        )
    ]);
}

/** A job that parks until its attempt is interrupted, then completes on the next. */
function parkingJob(attempts: number[]) {
    return defineJob({
        type: 'Parked',
        allowAnonymous: true,
        run: async (job, input: { n: number }) => {
            attempts.push(job.attempt);
            if (job.attempt === 1) {
                await aborted(job.signal);
                return undefined as never;
            }
            return input.n;
        }
    });
}

const ID = 'Parked\u0000run-1';

async function hostIds(storage: ReturnType<typeof memoryStorage>): Promise<string[]> {
    const record = await storage.load(ROSTER_TYPE, ROSTER_INDEX_KEY);
    return Object.keys((record?.state as Record<string, number> | undefined) ?? {});
}

async function rosterOf(storage: ReturnType<typeof memoryStorage>, hostId: string): Promise<string[]> {
    const record = await storage.load(ROSTER_TYPE, `${hostId}/${reminderShardOf(ID)}`);
    return Object.keys((record?.state as Record<string, number> | undefined) ?? {});
}

async function reminderNames(storage: ReturnType<typeof memoryStorage>): Promise<string[]> {
    const record = await storage.load(REMINDER_TYPE, reminderShardOf(ID));
    const table = (record?.state ?? {}) as Record<string, Record<string, unknown>>;
    return Object.keys(table[ID] ?? {});
}

let running: (Host | ClusterHarness)[] = [];
afterEach(async () => {
    for (const r of running) {
        if ('hosts' in r) await Promise.all(r.hosts.map((h) => h.stop({ timeoutMs: 1000 })));
        else await r.stop({ timeoutMs: 1000 });
    }
    running = [];
});

describe('task liveness: the per-host roster', () => {
    it('a start joins the host roster and the index — and arms NO reminder', async () => {
        const storage = memoryStorage();
        const Job = parkingJob([]);
        const host = createHost({ actors: [Job], storage, defaults: quiet });
        running.push(host);
        await host.actor(Job, 'run-1').start({ n: 1 });

        const ids = await hostIds(storage);
        expect(ids).toHaveLength(1);
        expect(await rosterOf(storage, ids[0]!)).toEqual([ID]);
        expect(await reminderNames(storage)).not.toContain(TASK_REMINDER);
    });

    it('a finished run leaves the roster', async () => {
        const storage = memoryStorage();
        const Job = defineJob({ type: 'Parked', allowAnonymous: true, run: async () => 1 });
        const host = createHost({ actors: [Job], storage, defaults: quiet });
        running.push(host);
        const client = host.actor(Job, 'run-1');
        await client.start(undefined as never);
        await until(async () => (await client.status()).status === 'completed');
        const [hostId] = await hostIds(storage);
        await until(async () => (await rosterOf(storage, hostId!)).length === 0);
    });

    it('host.stop() right after a run completes waits for its roster clear (#313)', async () => {
        // The roster clear runs AFTER the run has left the task table, so a
        // stop that lands in between must still cover it. Modelled on a
        // store with a round trip: the untrack write is held for one
        // macrotask, and `host.stop()` is called the moment it starts —
        // exactly the window a rolling deploy hits right after a job
        // finishes. Without the fix, stop resolves before the write lands.
        const inner = memoryStorage();
        let stopping: Promise<void> | null = null;
        let onStop!: () => void;
        const stopCalled = new Promise<void>((r) => (onStop = r));
        let host!: Host;
        // Deliberately no `saveText`: the roster prefers it when present, so
        // the intercept below only sees the untrack because this wrapper
        // routes every write through `save`. Do not spread `...inner` here.
        const storage: ReturnType<typeof memoryStorage> = {
            load: (t, k) => inner.load(t, k),
            save: async (t, k, st, e) => {
                const untrack =
                    t === ROSTER_TYPE &&
                    k !== ROSTER_INDEX_KEY &&
                    Object.keys(st as object).length === 0;
                if (untrack && !stopping) {
                    stopping = host.stop({ timeoutMs: 1000 });
                    onStop();
                    await new Promise((r) => setTimeout(r, 0));
                }
                return inner.save(t, k, st, e);
            },
            clear: (t, k, e) => inner.clear(t, k, e)
        };
        const Job = defineJob({ type: 'Parked', allowAnonymous: true, run: async () => 1 });
        host = createHost({ actors: [Job], storage, defaults: quiet });
        running.push(host);
        await host.actor(Job, 'run-1').start(undefined as never);
        // Microtask-only from here to the assertion (no timer poll), so the
        // check runs BEFORE the held write's macrotask — a stop that did not
        // wait for it resolves first and is caught with the entry still there.
        await within(stopCalled, 2000);
        await within(stopping!, 2000);
        const [hostId] = await hostIds(inner);
        expect(await rosterOf(inner, hostId!)).toEqual([]);
    });

    it('bookkeeping that outruns taskGraceMs is warned as such, not as an ignored signal', async () => {
        // The grace-timeout warning splits by what is stuck: a body still in
        // the task table ignored its signal; a run that has returned and is
        // only waiting on its roster clear is a storage round trip, with no
        // body to advise. Here the body is long gone — the untrack write is
        // the only thing outstanding, held past the grace.
        const warns = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const inner = memoryStorage();
        let release!: () => void;
        const held = new Promise<void>((r) => (release = r));
        let onUntrack!: () => void;
        const untrackStarted = new Promise<void>((r) => (onUntrack = r));
        // No `saveText` (see above): every roster write goes through `save`.
        const storage: ReturnType<typeof memoryStorage> = {
            load: (t, k) => inner.load(t, k),
            save: async (t, k, st, e) => {
                const untrack =
                    t === ROSTER_TYPE &&
                    k !== ROSTER_INDEX_KEY &&
                    Object.keys(st as object).length === 0;
                if (untrack) {
                    onUntrack();
                    await held;
                }
                return inner.save(t, k, st, e);
            },
            clear: (t, k, e) => inner.clear(t, k, e)
        };
        const Job = defineJob({ type: 'Parked', allowAnonymous: true, run: async () => 1 });
        const host = createHost({ actors: [Job], storage, defaults: { ...quiet, taskGraceMs: 30 } });
        running.push(host);
        await host.actor(Job, 'run-1').start(undefined as never);
        // Stop only once the body has returned and its untrack is the one
        // thing outstanding — a stop before that interrupts the body
        // instead, and there is no bookkeeping to outrun the grace.
        await within(untrackStarted, 2000);
        try {
            await within(host.stop({ timeoutMs: 1000 }), 2000);
        } finally {
            release();
        }
        const messages = warns.mock.calls.map((c) => String(c[0]));
        expect(messages.some((m) => m.includes('still had task bookkeeping in flight'))).toBe(true);
        expect(messages.some((m) => m.includes('ignored their abort signal'))).toBe(false);
    });

    it('a restarted single host adopts its predecessor: the run resumes with no call', async () => {
        const attempts: number[] = [];
        const storage = memoryStorage();
        const Job = parkingJob(attempts);
        const hostA = createHost({ actors: [Job], storage, defaults: quiet });
        // Tracked from birth: if an assertion below throws, afterEach still
        // tears it down (a second stop() is a no-op on the happy path).
        running.push(hostA);
        await hostA.start();
        await hostA.actor(Job, 'run-1').start({ n: 7 });
        await until(() => attempts.length === 1);
        await within(hostA.stop({ timeoutMs: 5000 }), 2000);
        // Interrupted, not finished: the roster outlives the host.
        const [oldId] = await hostIds(storage);
        expect(await rosterOf(storage, oldId!)).toEqual([ID]);

        const hostB = createHost({ actors: [Job], storage, defaults: ticking });
        running.push(hostB);
        await hostB.start();
        // Nobody calls the job; B's tick finds A's roster, A is not B, A is
        // not live (single node: live ⇔ me), and B owns every shard.
        await until(() => attempts.length === 2, 5000);
        await until(async () => (await hostB.actor(Job, 'run-1').status()).status === 'completed');
        expect(await hostB.actor(Job, 'run-1').result()).toBe(7);
        // The dead roster and its index entry are gone; B's remains.
        await until(async () => !(await hostIds(storage)).includes(oldId!), 5000);
        expect(await storage.load(ROSTER_TYPE, `${oldId}/${reminderShardOf(ID)}`)).toBeNull();
    });

    it('in a cluster the survivor adopts an expired host and resumes the run itself', async () => {
        const attempts: number[] = [];
        const Job = parkingJob(attempts);
        const cluster = await createCluster(2, {
            actors: [Job],
            defaults: ticking,
            policy: selfPolicy
        });
        running.push(cluster);
        const [host0, host1] = cluster.hosts as [Host, Host];
        await host0.actor(Job, 'run-1').start({ n: 9 });
        await until(() => attempts.length === 1);
        const id0 = cluster.placements[0]!.identity.hostId;
        expect(await rosterOf(cluster.storage as ReturnType<typeof memoryStorage>, id0)).toEqual([ID]);

        // Host 0 vanishes from membership without being told (a TTL lapse).
        cluster.hub.expire(id0);
        await until(() => attempts.length === 2, 5000);
        const client = host1.actor(Job, 'run-1');
        await until(async () => (await client.status()).status === 'completed', 5000);
        expect(await client.result()).toBe(9);
        expect(host1.stats().activations).toBeGreaterThan(0);
    });

    it('reminderTaskLiveness() restores the per-task reminder (the Durable Object shape)', async () => {
        const storage = memoryStorage();
        const Job = parkingJob([]);
        const host = createHost({
            actors: [Job],
            storage,
            defaults: quiet,
            taskLiveness: reminderTaskLiveness()
        });
        running.push(host);
        await host.actor(Job, 'run-1').start({ n: 1 });
        expect(await reminderNames(storage)).toContain(TASK_REMINDER);
        expect(await storage.load(ROSTER_TYPE, ROSTER_INDEX_KEY)).toBeNull();
    });

    it('a failed index registration is retried — the roster does not wedge', async () => {
        const inner = memoryStorage();
        let failures = 1;
        const storage: ReturnType<typeof memoryStorage> = {
            load: (t, k) => inner.load(t, k),
            save: (t, k, st, e) => {
                if (t === ROSTER_TYPE && k === ROSTER_INDEX_KEY && failures-- > 0) {
                    return Promise.reject(new Error('index save transiently down'));
                }
                return inner.save(t, k, st, e);
            },
            clear: (t, k, e) => inner.clear(t, k, e)
        };
        const Job = parkingJob([]);
        const host = createHost({ actors: [Job], storage, defaults: quiet });
        running.push(host);
        // The first start fails at registration and must surface the error…
        await expect(host.actor(Job, 'run-1').start({ n: 1 })).rejects.toThrow(
            /transiently down/
        );
        // …and the next one registers and tracks — the memoized rejection
        // was dropped, not re-awaited.
        await host.actor(Job, 'run-2').start({ n: 1 });
        const ids = await hostIds(inner);
        expect(ids).toHaveLength(1);
    });

    it('a start whose liveness track fails takes its ledger entry back', async () => {
        const inner = memoryStorage();
        let failTracks = 1;
        const storage: ReturnType<typeof memoryStorage> = {
            load: (t, k) => inner.load(t, k),
            save: (t, k, st, e) => {
                if (t === ROSTER_TYPE && failTracks-- > 0) {
                    return Promise.reject(new Error('roster transiently down'));
                }
                return inner.save(t, k, st, e);
            },
            clear: (t, k, e) => inner.clear(t, k, e)
        };
        const def = defineActor({
            type: 'Parked',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('run'),
                running: () => ctx.tasks.list()
            }),
            tasks: (ctx) => ({
                async run() {
                    await aborted(ctx.abortSignal);
                }
            })
        });
        const host = createHost({ actors: [def], storage, defaults: quiet });
        running.push(host);
        await expect(host.actor(def, 'run-1').begin()).rejects.toThrow(/transiently down/);
        // The rejected start left NO durable run behind: the ledger entry
        // was taken back, so a restarted host resumes nothing.
        const record = await inner.load(TASKS_TYPE, ID);
        expect(Object.keys((record?.state ?? {}) as object)).toHaveLength(0);
        // And the same actor can start again once the roster recovers.
        await host.actor(def, 'run-1').begin();
        expect(await host.actor(def, 'run-1').running()).toHaveLength(1);
    });

    it('the roster refuses a host id that would alias its index', () => {
        const liveness = rosterTaskLiveness();
        const context = {
            storage: memoryStorage(),
            scheduler: manualScheduler(),
            tickMs: 0,
            hostId: ROSTER_INDEX_KEY,
            isHostLive: () => true,
            ownsShard: () => true,
            touch: async () => undefined,
            reminders: () => {
                throw new Error('unused');
            }
        };
        expect(() => liveness.bind(context)).toThrow(/reserved for the roster index/);
        expect(() => liveness.bind({ ...context, hostId: '' })).toThrow(/reserved/);
        // And a second bind of a good one is refused too — one instance per host.
        liveness.bind({ ...context, hostId: 'h.ok' });
        expect(() => liveness.bind({ ...context, hostId: 'h.other' })).toThrow(/already bound/);
    });

    it('a plain tasks: actor rides the same roster', async () => {
        const storage = memoryStorage();
        const def = defineActor({
            type: 'Parked',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx) => ({ begin: () => ctx.tasks.start('run') }),
            tasks: (ctx) => ({
                async run() {
                    await aborted(ctx.abortSignal);
                }
            })
        });
        const host = createHost({ actors: [def], storage, defaults: quiet });
        running.push(host);
        await host.actor(def, 'run-1').begin();
        const [hostId] = await hostIds(storage);
        expect(await rosterOf(storage, hostId!)).toEqual([ID]);
        expect(await reminderNames(storage)).not.toContain(TASK_REMINDER);
    });
});
