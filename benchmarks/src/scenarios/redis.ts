/**
 * What a membership change actually costs against Redis.
 *
 * `cluster/membership-fanout` measures how many hosts get notified, which is
 * exact but provider-independent. The number people care about is what that
 * becomes in round trips, and until now `BASELINES.md` reported that as a
 * MODEL derived from reading `packages/actors-redis/src/index.ts`. The model
 * was corrected twice and was still wrong both times, which is a good sign
 * it should have been measured.
 *
 * So: measure it. `CONFIG RESETSTAT`, add one host, read `INFO commandstats`.
 *
 * Env-gated on `REDIS_URL`, the same convention as
 * `packages/actors-redis/__tests__/redis-cluster.test.ts`. Skipped entirely
 * without it, so the default suite still runs anywhere.
 *
 * Only the MEMBERSHIP provider is instantiated, not whole hosts: the refresh
 * fan-out is entirely a membership concern, and standing up 101 real hosts
 * would add a lot of noise to a command count.
 */
import Redis from 'ioredis';
import { redisMembership } from '@sigx/actors-redis';
import type { ClusterMembership } from '@sigx/actors/cluster';
import type { Metric, RunContext, Scenario } from '../types.ts';

/**
 * Heartbeat and poll pushed far beyond the measurement window, so the only
 * traffic counted is the join and the refreshes it triggers. Left at their
 * defaults, a 5s heartbeat across 100 members would swamp the signal.
 */
const QUIET_MEMBERSHIP = {
    heartbeatMs: 600_000,
    ttlMs: 1_200_000,
    pollMs: 600_000
};

/** Long enough for every subscriber's refresh to land, short enough to run. */
const SETTLE_MS = 400;

const SIZES = [1, 10, 50, 100] as const;
const QUICK_SIZES = [1, 10] as const;

interface CommandStats {
    total: number;
    byCommand: Record<string, number>;
}

async function readStats(client: Redis): Promise<CommandStats> {
    const raw = await client.info('commandstats');
    let total = 0;
    const byCommand: Record<string, number> = {};
    for (const line of raw.split('\n')) {
        // `\S+?`, not `\w+`: subcommands report as `client|setinfo`,
        // `cluster|info` and so on, and ioredis issues CLIENT SETINFO on
        // every new connection — including the one the measured join opens.
        // A `\w+` pattern drops those silently and undercounts the total.
        const match = /^cmdstat_(\S+?):calls=(\d+)/.exec(line.trim());
        if (!match) continue;
        const calls = Number(match[2]);
        total += calls;
        byCommand[match[1] as string] = calls;
    }
    return { total, byCommand };
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/** Distinguishes measurements within a process; the clock separates runs. */
let measurementSeq = 0;

/** Commands caused by ONE host joining a cluster that already has `n`. */
async function measureJoin(url: string, n: number): Promise<CommandStats> {
    // Genuinely fresh per measurement, across processes as well as within
    // one. Leftover keys are read by every refresh, so a reused namespace
    // inflates the count — and since host records carry a TTL far longer
    // than a benchmark run, a crashed run would otherwise poison the next.
    const namespace = `bench-amp-${n}-${Date.now()}-${++measurementSeq}`;
    const admin = new Redis(url);
    const clients: Redis[] = [];
    const members: ClusterMembership[] = [];

    const join = async (i: number): Promise<void> => {
        const client = new Redis(url);
        clients.push(client);
        const membership = redisMembership(client, { namespace, ...QUIET_MEMBERSHIP });
        members.push(membership);
        await membership.join({
            hostId: `s${i}`,
            epoch: 1,
            address: `http://s${i}.test`,
            status: 'active'
        });
    };

    try {
        for (let i = 0; i < n; i++) await join(i);
        await sleep(SETTLE_MS);

        await admin.config('RESETSTAT');
        await join(n);
        await sleep(SETTLE_MS);

        const stats = await readStats(admin);
        // `INFO commandstats` counts the INFO call itself; not ours to report.
        const info = stats.byCommand['info'] ?? 0;
        return { total: stats.total - info, byCommand: stats.byCommand };
    } finally {
        for (const membership of members) {
            try {
                await membership.leave();
            } catch {
                // Teardown of a measurement rig; a failed leave is not news.
            }
        }
        for (const client of clients) client.disconnect();
        // Every key, not just the index: `leave()` failures are swallowed
        // above, and an orphaned `{ns}:host:{id}` record outlives a run by
        // its TTL and would be read by every refresh if the namespace were
        // ever reused.
        const keys = [
            `${namespace}:hosts`,
            `${namespace}:mver`,
            ...Array.from({ length: n + 1 }, (_v, i) => `${namespace}:host:s${i}`)
        ];
        await admin.del(...keys).catch(() => {});
        admin.disconnect();
    }
}

const redisAmplification: Scenario = {
    name: 'cluster/redis-amplification',
    description: 'Redis commands caused by ONE host joining an N-member cluster (needs REDIS_URL)',
    async run(ctx: RunContext): Promise<Metric[]> {
        const url = process.env['REDIS_URL'];
        if (!url) {
            // Not an error: the default suite must run without a Redis.
            return [
                {
                    name: 'skipped_no_REDIS_URL',
                    value: 1,
                    unit: 'count',
                    direction: 'lower',
                    informational: true,
                    noiseFloor: 1
                }
            ];
        }

        const metrics: Metric[] = [];
        for (const n of ctx.quick ? QUICK_SIZES : SIZES) {
            const stats = await measureJoin(url, n);
            metrics.push(
                {
                    name: `n=${n}/commands`,
                    value: stats.total,
                    unit: 'count',
                    direction: 'lower',
                    noiseFloor: 2
                },
                {
                    // The quadratic term, isolated: one HGET per member, per
                    // refresh, and every member refreshes. If a debounce
                    // lands, this is the number that collapses.
                    name: `n=${n}/hget`,
                    value: stats.byCommand['hget'] ?? 0,
                    unit: 'count',
                    direction: 'lower',
                    noiseFloor: 2
                },
                {
                    // Refreshes performed, one SMEMBERS each — the linear
                    // term, and the count a debounce would reduce directly.
                    name: `n=${n}/refreshes`,
                    value: stats.byCommand['smembers'] ?? 0,
                    unit: 'count',
                    direction: 'lower',
                    noiseFloor: 2,
                    informational: true
                }
            );
        }
        return metrics;
    }
};

export const redisScenarios: Scenario[] = [redisAmplification];
