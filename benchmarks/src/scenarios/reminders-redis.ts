/**
 * Where the sharded reminder table's CAS ceiling is, against a real Redis
 * (#382 — the L2 rung of the scaling roadmap, `docs/architecture/
 * scaling-roadmap.md`).
 *
 * The default `shardedReminders()` keeps 16 fixed shard records in
 * `ActorStorage`; every `set` loads one, stringifies it twice and CAS-saves
 * it back, retrying three times on conflict before it throws
 * (`MUTATE_ATTEMPTS`, `packages/actors/src/host/reminders.ts`). With N
 * hosts every host writes every shard, so at some arm rate the third
 * attempt fails and the caller sees an error. `BASELINES.md` (2026-08-26)
 * measured that ratio at zero for 50 arms/s + 50 fires/s on three hosts and
 * named "the rate at which it leaves zero" as the next thing to find. This
 * finds it on loopback — the UPPER bound for any cloud figure: a longer
 * round trip WIDENS the load-to-save window in which another writer can
 * land, so conflicts are rarer here than they will ever be in a cluster.
 *
 * Shape: N in-process hosts (`createCluster`, `selfPolicy`, membership and
 * directory in memory) over ONE `redisStorage`, a 1 s reminder tick, and an
 * OPEN-LOOP arm ladder: R arms per second for a fixed window, each arm a
 * fresh actor key (so the entries spread over the shards the way real
 * actors do) whose one-shot reminder is due 2 s out. Arms are round-robined
 * across hosts, so N hosts means N writer chains against the same 16
 * records. Open-loop rather than closed: a `set` that is slow must not
 * throttle the arrival rate, or the ceiling hides behind its own backlog.
 *
 * What is counted: `set` failures (the caller's throw), `set` latency,
 * fires that landed within one tick of their due time, Redis commands per
 * arm (INCLUDING the ticks and the fire-side deletes — the whole cost of a
 * reminder's life), Redis CPU per thousand arms from `INFO cpu`, and the
 * peak byte size of the 16 shard records (`MEMORY USAGE`).
 *
 * `table-size` is the other axis, and the one the arm ladder cannot see:
 * every reminder above fires 2 s after it is armed, so the shard records
 * stay small. A million SLEEPING runs is a million entries the records
 * carry through every set and every tick. That arm pre-seeds P far-future
 * entries and runs one fixed rung against them, so the O(table) term of
 * the sharded design is a number rather than an argument.
 *
 * Env-gated on `REDIS_URL` exactly like `cluster/redis-amplification`;
 * skipped without it. Tier 1 with an external store: one process, real
 * Redis round trips, so timings depend on the box AND the store while the
 * failure ratio is the finding.
 */
import Redis from 'ioredis';
import { defineActor, isStorageConflict } from '@sigx/actors';
import { redisStorage } from '@sigx/actors-redis';
import { REMINDER_TYPE, type ActorStorage, type Host } from '@sigx/actors/host';
import { createCluster, selfPolicy } from '../cluster-harness.ts';
import { benchCall } from '../host-fixture.ts';
import { Samples } from '../histogram.ts';
import type { Metric, RunContext, Scenario } from '../types.ts';

const TICK_MS = 1000;
/** A one-shot due this far out: long enough that arms and fires overlap
 *  within a rung, short enough that the drain after it is quick. */
const DUE_MS = 2000;
const HOSTS = [1, 3, 8, 16] as const;
const QUICK_HOSTS = [1, 3] as const;
const RATES = [50, 100, 200, 500, 1000] as const;
const QUICK_RATES = [50, 200] as const;
const WINDOW_MS = 15_000;
const QUICK_WINDOW_MS = 3_000;
/** Arms are issued in slices this often; below ~10 ms `setTimeout` floors
 *  the spacing and a "rate" becomes a burst. */
const SLICE_MS = 10;
/** After the last arm: its due time, one tick to notice it, one more of slack. */
const DRAIN_SLACK_MS = DUE_MS + 2 * TICK_MS + 2000;
/** A rung whose failures pass this fraction is the ceiling; the ladder
 *  stops, because every rung above it would only measure the backlog. */
const STOP_FAILURE_RATIO = 0.25;

/** The `table-size` arm: entries already asleep in the 16 records. */
const POPULATIONS = [0, 10_000, 100_000] as const;
const QUICK_POPULATIONS = [0, 2_000] as const;
const POPULATION_HOSTS = 3;
const POPULATION_RATE = 200;

/** Duplicated from `reminder-shards.ts` (internal): the 16 shard keys. */
const SHARD_KEYS = Array.from({ length: 16 }, (_v, i) => `p${i}`);

/** Distinguishes clusters within a process; the clock separates runs. Two
 *  clusters minted in one millisecond would otherwise share shard records. */
let clusterSeq = 0;

/** Fires seen, keyed by actor key → wall time. Module scope because the
 *  actor body has no other channel back to the scenario. */
const fired = new Map<string, number>();

const Armer = defineActor({
    type: 'BenchArmer',
    allowAnonymous: true,
    state: () => ({}),
    onReminder(ctx) {
        fired.set(ctx.key, Date.now());
    },
    methods: (ctx) => ({
        async arm(due: number) {
            await ctx.reminders.set('fire', { due });
        }
    })
});

interface RedisSample {
    commands: number;
    cpuMs: number;
}

async function sampleRedis(client: Redis): Promise<RedisSample> {
    const [stats, cpu] = await Promise.all([client.info('commandstats'), client.info('cpu')]);
    let commands = 0;
    for (const line of stats.split('\n')) {
        const match = /^cmdstat_(\S+?):calls=(\d+)/.exec(line.trim());
        if (!match) continue;
        // The INFO and MEMORY USAGE calls this rig issues are not the
        // reminder's cost.
        if (match[1] === 'info' || match[1] === 'memory|usage') continue;
        commands += Number(match[2]);
    }
    let cpuMs = 0;
    for (const line of cpu.split('\n')) {
        const match = /^used_cpu_(?:sys|user):([\d.]+)/.exec(line.trim());
        if (match) cpuMs += Number(match[1]) * 1000;
    }
    return { commands, cpuMs };
}

async function shardBytes(client: Redis, namespace: string): Promise<number> {
    const keys = SHARD_KEYS.map((shard) => `${namespace}:st:${REMINDER_TYPE}\u0000${shard}`);
    const sizes = await Promise.all(keys.map((key) => client.memory('USAGE', key)));
    return sizes.reduce<number>((sum, size) => sum + (typeof size === 'number' ? size : 0), 0);
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/** Every key under the namespace, by SCAN and batched DEL — never KEYS,
 *  which blocks the server for the whole keyspace, ours or not. */
async function deleteNamespace(client: Redis, namespace: string): Promise<void> {
    let cursor = '0';
    do {
        const [next, keys] = await client.scan(cursor, 'MATCH', `${namespace}:*`, 'COUNT', 1000);
        cursor = next;
        if (keys.length > 0) await client.del(...keys).catch(() => {});
    } while (cursor !== '0');
}

interface RungResult {
    arms: number;
    failures: number;
    latency: Samples;
    firedWithinTick: number;
    firedTotal: number;
    commands: number;
    cpuMs: number;
    shardBytesPeak: number;
}

/** One rung: R arms/s for `windowMs`, then wait for the fires. */
async function runRung(
    hosts: readonly Host[],
    admin: Redis,
    namespace: string,
    rate: number,
    windowMs: number,
    label: string
): Promise<RungResult> {
    fired.clear();
    const due = new Map<string, number>();
    const latency = new Samples();
    let failures = 0;
    let arms = 0;
    /** Anything that is NOT the CAS giving up is the rig's problem, not a
     *  finding — kept apart and thrown after the window so a timeout or a
     *  Redis error fails the scenario instead of inflating the ratio. */
    let rigError: unknown = null;
    let shardBytesPeak = 0;
    const inFlight: Promise<void>[] = [];

    const before = await sampleRedis(admin);
    const started = Date.now();
    const perSlice = rate * (SLICE_MS / 1000);
    let issued = 0;
    let stopSampling = false;
    const sampler = (async () => {
        while (!stopSampling) {
            shardBytesPeak = Math.max(shardBytesPeak, await shardBytes(admin, namespace));
            await sleep(500);
        }
    })();

    for (let slice = 0; started + slice * SLICE_MS < started + windowMs; slice++) {
        const target = Math.floor((slice + 1) * perSlice);
        for (; issued < target; issued++) {
            const key = `${label}-${issued}`;
            const host = hosts[issued % hosts.length]!;
            const at = performance.now();
            arms++;
            inFlight.push(
                host.dispatch({ type: Armer.type, key }, 'arm', [DUE_MS], benchCall()).then(
                    () => {
                        latency.record(performance.now() - at);
                        // `due` is "ms from now" AT THE SET, and under a
                        // slow set that is long after the arm was issued;
                        // the dispatch resolving is the closest stamp for
                        // it, so the fire's lag is measured against the
                        // reminder the runtime actually recorded.
                        due.set(key, Date.now() + DUE_MS);
                    },
                    (error: unknown) => {
                        // The third CAS attempt lost: `reminders.set` threw
                        // the branded conflict and the actor method with
                        // it. This is the finding, not an error of the rig.
                        if (isStorageConflict(error)) failures++;
                        else rigError ??= error;
                    }
                )
            );
        }
        const next = started + (slice + 1) * SLICE_MS;
        const wait = next - Date.now();
        if (wait > 0) await sleep(wait);
    }
    await Promise.all(inFlight);
    if (rigError !== null) throw rigError;

    // Drain: every successful arm should fire by its due time plus a tick.
    // Counted as the intersection with THIS rung's arms — a late fire from
    // the previous rung lands in the same map and must not read as one of
    // ours, or the drain exits early and the ratio below is skewed.
    let lastDue = 0;
    for (const at of due.values()) if (at > lastDue) lastDue = at;
    const drainUntil = lastDue + DRAIN_SLACK_MS;
    const firedOfOurs = (): number => {
        let count = 0;
        for (const key of due.keys()) if (fired.has(key)) count++;
        return count;
    };
    while (Date.now() < drainUntil && firedOfOurs() < due.size) await sleep(200);
    stopSampling = true;
    await sampler;

    let firedWithinTick = 0;
    let firedTotal = 0;
    for (const [key, expected] of due) {
        const at = fired.get(key);
        if (at === undefined) continue;
        firedTotal++;
        if (at - expected <= TICK_MS) firedWithinTick++;
    }
    const after = await sampleRedis(admin);
    return {
        arms,
        failures,
        latency,
        firedWithinTick,
        firedTotal,
        commands: after.commands - before.commands,
        cpuMs: after.cpuMs - before.cpuMs,
        shardBytesPeak
    };
}

/**
 * Pre-seed P sleeping entries, round-robin over the shards. The assignment
 * need not match the runtime's hash: nothing looks these up by key, every
 * tick scans a whole record, and every set rewrites one — which is exactly
 * the cost being priced. Written straight through the storage as the
 * runtime writes its own table (`saveText`, a JSON-native record).
 */
async function seedPopulation(storage: ActorStorage, population: number): Promise<void> {
    if (population === 0) return;
    // Two years out: never due within a run, and the same digit count as a
    // real far-future reminder, so the bytes are representative.
    const nextDue = Date.now() + 2 * 365 * 24 * 3600 * 1000;
    const perShard = Math.ceil(population / SHARD_KEYS.length);
    for (const [i, shard] of SHARD_KEYS.entries()) {
        const table: Record<string, Record<string, { nextDue: number }>> = {};
        for (let j = 0; j < perShard; j++) {
            table[`BenchSleeper\u0000s${i}-${j}`] = { fire: { nextDue } };
        }
        await storage.saveText!(REMINDER_TYPE, shard, JSON.stringify(table), null);
    }
}

function rungMetrics(prefix: string, r: RungResult): Metric[] {
    const p = r.latency.percentiles();
    return [
        {
            // The finding. Gates at a floor of one failure in a thousand
            // arms — below that is a CAS race the retry absorbed, not a
            // ceiling.
            name: `${prefix}/set_failure_ratio`,
            value: r.arms === 0 ? 0 : r.failures / r.arms,
            unit: 'ratio',
            direction: 'lower',
            noiseFloor: 0.001
        },
        {
            name: `${prefix}/set_p50_ms`,
            value: p.p50,
            unit: 'ms',
            direction: 'lower',
            informational: true
        },
        {
            name: `${prefix}/set_p99_ms`,
            value: p.p99,
            unit: 'ms',
            direction: 'lower',
            informational: true
        },
        {
            // Of the arms that succeeded, how many fired within one tick
            // of their due time. Below 1.0 the tick is falling behind its
            // own table.
            name: `${prefix}/fired_within_tick_ratio`,
            value: r.arms - r.failures === 0 ? 0 : r.firedWithinTick / (r.arms - r.failures),
            unit: 'ratio',
            direction: 'higher',
            noiseFloor: 0.01
        },
        {
            // Whole-life cost: the set, every tick's load and scan while
            // it waited, the fire's delete. Informational — CAS retries
            // vary run to run.
            name: `${prefix}/commands_per_arm`,
            value: r.arms === 0 ? 0 : r.commands / r.arms,
            unit: 'count',
            direction: 'lower',
            informational: true
        },
        {
            name: `${prefix}/redis_cpu_ms_per_1k_arms`,
            value: r.arms === 0 ? 0 : (r.cpuMs / r.arms) * 1000,
            unit: 'ms',
            direction: 'lower',
            informational: true
        },
        {
            // Wall-clock digits in every entry, so bytes are only
            // deterministic until the digit count changes — informational.
            name: `${prefix}/shard_bytes_peak`,
            value: r.shardBytesPeak,
            unit: 'bytes',
            direction: 'lower',
            informational: true
        }
    ];
}

const skipped: Metric[] = [
    {
        name: 'skipped_no_REDIS_URL',
        value: 1,
        unit: 'count',
        direction: 'lower',
        informational: true,
        noiseFloor: 1
    }
];

const armFire: Scenario = {
    name: 'reminders-redis/arm-fire',
    description: 'arm rate at which the 16-shard reminder CAS starts failing, N hosts over one Redis (needs REDIS_URL)',
    async run(ctx: RunContext): Promise<Metric[]> {
        const url = process.env['REDIS_URL'];
        if (!url) return skipped;
        const metrics: Metric[] = [];
        const windowMs = ctx.quick ? QUICK_WINDOW_MS : WINDOW_MS;
        for (const n of ctx.quick ? QUICK_HOSTS : HOSTS) {
            // Fresh per cluster, across processes as well as within one: a
            // shard record left by a crashed run is loaded by every set.
            const namespace = `bench-rem-${n}-${Date.now()}-${++clusterSeq}`;
            const admin = new Redis(url);
            // Own the client: `url` would mint one the scenario cannot close.
            const storageClient = new Redis(url, { enableAutoPipelining: true });
            const storage = redisStorage({ client: storageClient, namespace });
            const harness = await createCluster(n, {
                actors: [Armer],
                storage,
                policy: selfPolicy,
                defaults: { reminderTickMs: TICK_MS }
            });
            try {
                for (const rate of ctx.quick ? QUICK_RATES : RATES) {
                    const r = await runRung(harness.hosts, admin, namespace, rate, windowMs, `n${n}r${rate}`);
                    metrics.push(...rungMetrics(`n=${n}/r=${rate}`, r));
                    if (r.arms > 0 && r.failures / r.arms > STOP_FAILURE_RATIO) break;
                }
            } finally {
                await harness.stop();
                await deleteNamespace(admin, namespace);
                admin.disconnect();
                storageClient.disconnect();
            }
        }
        return metrics;
    }
};

const tableSize: Scenario = {
    name: 'reminders-redis/table-size',
    description: 'one arm rung against P entries already asleep in the shard records (needs REDIS_URL)',
    async run(ctx: RunContext): Promise<Metric[]> {
        const url = process.env['REDIS_URL'];
        if (!url) return skipped;
        const metrics: Metric[] = [];
        const windowMs = ctx.quick ? QUICK_WINDOW_MS : WINDOW_MS;
        for (const population of ctx.quick ? QUICK_POPULATIONS : POPULATIONS) {
            const namespace = `bench-rempop-${population}-${Date.now()}-${++clusterSeq}`;
            const admin = new Redis(url);
            const storageClient = new Redis(url, { enableAutoPipelining: true });
            const storage = redisStorage({ client: storageClient, namespace });
            // Seeded BEFORE the hosts start, so no tick ever sees a
            // half-written table and every set from the first arm pays
            // the full record.
            await seedPopulation(storage, population);
            const harness = await createCluster(POPULATION_HOSTS, {
                actors: [Armer],
                storage,
                policy: selfPolicy,
                defaults: { reminderTickMs: TICK_MS }
            });
            try {
                const r = await runRung(
                    harness.hosts,
                    admin,
                    namespace,
                    POPULATION_RATE,
                    windowMs,
                    `pop${population}`
                );
                metrics.push(...rungMetrics(`n=${POPULATION_HOSTS}/pop=${population}/r=${POPULATION_RATE}`, r));
            } finally {
                await harness.stop();
                await deleteNamespace(admin, namespace);
                admin.disconnect();
                storageClient.disconnect();
            }
        }
        return metrics;
    }
};

export const remindersRedisScenarios: Scenario[] = [armFire, tableSize];
