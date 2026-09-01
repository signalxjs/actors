/**
 * Task liveness (#310): a running task is found again after its host dies
 * through a per-host ROSTER, not a reminder per task.
 */
import { afterEach, describe, expect, it } from 'vitest';
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

    it('a restarted single host adopts its predecessor: the run resumes with no call', async () => {
        const attempts: number[] = [];
        const storage = memoryStorage();
        const Job = parkingJob(attempts);
        const hostA = createHost({ actors: [Job], storage, defaults: quiet });
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
