/**
 * What one reminder costs each provider, as counts (#385).
 *
 * `reminders/arm-cost` is Tier 1 and exact: the sharded provider on
 * `memoryStorage` behind a counting decorator, with P entries already
 * asleep in the shard records, and ONE `set` measured. The storage
 * operations per set are an invariant — one load and one CAS save
 * whatever P is — and the BYTES that one save writes are the O(table)
 * term made visible beside them: the whole shard record, every time.
 * That is the number `redisReminders()` exists to remove, and this arm
 * is what says so on a machine with no Redis at all.
 *
 * `reminders/redis-commands` is the same question of the due-time-indexed
 * provider against a real Redis, env-gated on `REDIS_URL` like
 * `cluster/redis-amplification`: commands per arm, per clear, per EMPTY
 * tick and per 200-fire claim with P members already asleep in the sorted
 * set, from `INFO commandstats` deltas. The claim counts include every
 * command the script issues. Latencies are informational — they depend on
 * the box and the store — and the counts carry a small noise floor
 * because a script that finds nothing due still issues its `TIME`.
 */
import Redis from 'ioredis';
import type { ActorReminders, ActorRef } from '@sigx/actors';
import { redisReminders, remEscape } from '@sigx/actors-redis';
import { manualScheduler, memoryStorage, REMINDER_TYPE, shardedReminders } from '@sigx/actors/host';
import { countingStorage } from '../host-fixture.ts';
import { Samples } from '../histogram.ts';
import type { Metric, RunContext, Scenario } from '../types.ts';

const SHARD_KEYS = Array.from({ length: 16 }, (_v, i) => `p${i}`);
const POPULATIONS = [1_000, 10_000, 100_000] as const;
const QUICK_POPULATIONS = [1_000] as const;
const REDIS_POPULATIONS = [10_000, 100_000, 1_000_000] as const;
const QUICK_REDIS_POPULATIONS = [10_000] as const;
const TICK_MS = 1_000;
const SAMPLE = 100;
const CLAIM_BATCH = 200;
const FAR_MS = 2 * 365 * 24 * 3600 * 1000;

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/** Bind a provider to a manual clock and a no-op deliver; the tick is
 *  driven by hand. */
function bindByHand(provider: ActorReminders, storage = memoryStorage()) {
    const scheduler = manualScheduler();
    let delivered = 0;
    provider.bind({
        storage,
        scheduler,
        tickMs: TICK_MS,
        ownsShard: () => true,
        deliver: async () => {
            delivered++;
        }
    });
    return { scheduler, delivered: () => delivered };
}

const armCost: Scenario = {
    name: 'reminders/arm-cost',
    description: 'storage ops and bytes ONE set costs the sharded provider with P entries asleep (exact ops, O(table) bytes)',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        for (const population of ctx.quick ? QUICK_POPULATIONS : POPULATIONS) {
            const inner = memoryStorage();
            // Seed straight into the shard records, as the runtime writes
            // its own table — exactly `population` entries, the remainder
            // over the first shards.
            const nextDue = Date.now() + FAR_MS;
            for (const [i, shard] of SHARD_KEYS.entries()) {
                const perShard =
                    Math.floor(population / SHARD_KEYS.length) + (i < population % SHARD_KEYS.length ? 1 : 0);
                const table: Record<string, Record<string, { nextDue: number }>> = {};
                for (let j = 0; j < perShard; j++) table[`BenchSleeper\u0000s${i}-${j}`] = { fire: { nextDue } };
                await inner.save(REMINDER_TYPE, shard, table, null);
            }
            const { storage, counts } = countingStorage(inner);
            const provider = shardedReminders();
            const { scheduler } = bindByHand(provider, storage);
            provider.start();
            try {
                const api = provider.apiFor({ type: 'BenchArmer', key: `k${population}` });
                // One warm set so the first measurement is not the cold path.
                await api.set('warm', { due: FAR_MS });
                counts.reset();
                const latency = new Samples();
                for (let i = 0; i < SAMPLE; i++) {
                    const at = performance.now();
                    await api.set(`fire${i}`, { due: FAR_MS });
                    latency.record(performance.now() - at);
                }
                const ops = (counts.loads + counts.saves + counts.clears) / SAMPLE;
                const last = counts.lastWrite(REMINDER_TYPE);
                // An empty tick: every owned shard is loaded and scanned
                // whether or not anything is due.
                counts.reset();
                scheduler.advance(TICK_MS);
                await sleep(50);
                const tickLoads = counts.loads;
                const p = latency.percentiles();
                const prefix = `p=${population}`;
                metrics.push(
                    {
                        // The invariant: a set is one load and one CAS save,
                        // however large the record.
                        name: `${prefix}/storage_ops_per_set`,
                        value: ops,
                        unit: 'count',
                        direction: 'lower',
                        exact: true
                    },
                    {
                        // The O(table) term: the bytes ONE set rewrites.
                        // Wall-clock digits inside, so informational.
                        name: `${prefix}/bytes_per_set`,
                        value: last?.bytes ?? 0,
                        unit: 'bytes',
                        direction: 'lower',
                        informational: true
                    },
                    {
                        name: `${prefix}/entries_rewritten_per_set`,
                        value: last?.entries ?? 0,
                        unit: 'count',
                        direction: 'lower',
                        informational: true
                    },
                    {
                        // Every owned shard, on every tick, due or not.
                        name: `${prefix}/loads_per_empty_tick`,
                        value: tickLoads,
                        unit: 'count',
                        direction: 'lower',
                        exact: true
                    },
                    {
                        name: `${prefix}/set_p50_us`,
                        value: p.p50 * 1000,
                        unit: 'us',
                        direction: 'lower',
                        informational: true
                    }
                );
            } finally {
                provider.stop();
            }
        }
        return metrics;
    }
};

async function commandCount(client: Redis): Promise<number> {
    const stats = await client.info('commandstats');
    let total = 0;
    for (const line of stats.split('\n')) {
        const match = /^cmdstat_(\S+?):calls=(\d+)/.exec(line.trim());
        if (!match) continue;
        if (match[1] === 'info' || match[1] === 'memory|usage') continue;
        total += Number(match[2]);
    }
    return total;
}

async function deleteNamespace(client: Redis, namespace: string): Promise<void> {
    let cursor = '0';
    do {
        const [next, keys] = await client.scan(cursor, 'MATCH', `${namespace}:*`, 'COUNT', 1000);
        cursor = next;
        if (keys.length > 0) await client.del(...keys);
    } while (cursor !== '0');
}

/** P one-shot members asleep two years out, each its own actor — the
 *  shape a million sleeping runs has. Pipelined in slices. */
async function seedRedis(client: Redis, namespace: string, population: number): Promise<void> {
    const due = String(Date.now() + FAR_MS);
    const SLICE = 5_000;
    for (let start = 0; start < population; start += SLICE) {
        const pipeline = client.pipeline();
        for (let i = start; i < Math.min(population, start + SLICE); i++) {
            const actor = `${remEscape('BenchSleeper')}\u0000${remEscape(`s${i}`)}`;
            pipeline.zadd(`${namespace}:rem:due`, due, `${actor}\u0000fire`);
            pipeline.sadd(`${namespace}:rem:a:${actor}`, 'fire');
        }
        await pipeline.exec();
    }
}

const skipped: Metric[] = [
    { name: 'skipped_no_REDIS_URL', value: 1, unit: 'count', direction: 'lower', informational: true, noiseFloor: 1 }
];

const redisCommands: Scenario = {
    name: 'reminders/redis-commands',
    description: 'Redis commands per set, clear, empty tick and 200-fire claim for redisReminders with P members asleep (needs REDIS_URL)',
    async run(ctx: RunContext): Promise<Metric[]> {
        const url = process.env['REDIS_URL'];
        if (!url) return skipped;
        const metrics: Metric[] = [];
        for (const population of ctx.quick ? QUICK_REDIS_POPULATIONS : REDIS_POPULATIONS) {
            const namespace = `bench-remcmd-${population}-${Date.now()}`;
            const admin = new Redis(url);
            const client = new Redis(url);
            try {
                await seedRedis(admin, namespace, population);
                const provider = redisReminders({ client, namespace, batchSize: CLAIM_BATCH });
                const { scheduler, delivered } = bindByHand(provider);
                provider.start();
                const ref: ActorRef = { type: 'BenchArmer', key: `k${population}` };
                const api = provider.apiFor(ref);
                await api.set('warm', { due: FAR_MS });
                await api.clear('warm');

                // Arms.
                let before = await commandCount(admin);
                const setLatency = new Samples();
                for (let i = 0; i < SAMPLE; i++) {
                    const at = performance.now();
                    await api.set(`fire${i}`, { due: FAR_MS });
                    setLatency.record(performance.now() - at);
                }
                const perSet = (await commandCount(admin) - before) / SAMPLE;

                // Clears.
                before = await commandCount(admin);
                for (let i = 0; i < SAMPLE; i++) await api.clear(`fire${i}`);
                const perClear = (await commandCount(admin) - before) / SAMPLE;

                // Empty ticks: nothing due among P members.
                before = await commandCount(admin);
                for (let i = 0; i < 5; i++) {
                    scheduler.advance(TICK_MS);
                    await sleep(30);
                }
                const perEmptyTick = (await commandCount(admin) - before) / 5;

                // One claim of exactly CLAIM_BATCH due members, each its own
                // actor (the fire-side SREM is part of the cost).
                for (let i = 0; i < CLAIM_BATCH; i++) {
                    await provider.apiFor({ type: 'BenchDue', key: `d${i}` }).set('fire', { due: 0 });
                }
                await sleep(5);
                before = await commandCount(admin);
                const at = performance.now();
                scheduler.advance(TICK_MS);
                const deadline = Date.now() + 10_000;
                while (delivered() < CLAIM_BATCH && Date.now() < deadline) await sleep(5);
                const claimMs = performance.now() - at;
                const perClaim = await commandCount(admin) - before;
                provider.stop();

                const p = setLatency.percentiles();
                const prefix = `p=${population}`;
                metrics.push(
                    { name: `${prefix}/commands_per_set`, value: perSet, unit: 'count', direction: 'lower', noiseFloor: 1 },
                    { name: `${prefix}/commands_per_clear`, value: perClear, unit: 'count', direction: 'lower', noiseFloor: 1 },
                    { name: `${prefix}/commands_per_empty_tick`, value: perEmptyTick, unit: 'count', direction: 'lower', noiseFloor: 1 },
                    { name: `${prefix}/commands_per_200_fire_claim`, value: perClaim, unit: 'count', direction: 'lower', noiseFloor: 20 },
                    { name: `${prefix}/fired_of_200`, value: delivered(), unit: 'count', direction: 'higher', noiseFloor: 1 },
                    { name: `${prefix}/set_p50_us`, value: p.p50 * 1000, unit: 'us', direction: 'lower', informational: true },
                    { name: `${prefix}/claim_200_ms`, value: claimMs, unit: 'ms', direction: 'lower', informational: true }
                );
            } finally {
                await deleteNamespace(admin, namespace);
                admin.disconnect();
                client.disconnect();
            }
        }
        return metrics;
    }
};

export const remindersCostScenarios: Scenario[] = [armCost, redisCommands];
