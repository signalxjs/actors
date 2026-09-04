/**
 * The shared `ActorReminders` conformance suite (#385), run against a REAL
 * Redis — the same cases `shardedReminders`, Postgres, SurrealDB and the
 * Durable Object alarm run, so "redisReminders is a reminder provider" is
 * one list of outcomes rather than this package's own reading of the
 * contract.
 *
 * Redis-specific mechanics — the key layout, the escaping, the claim as
 * one script — stay in `redis-reminders.test.ts`.
 *
 * Every harness gets its own key namespace on its own connection and
 * deletes it afterwards. The suite binds several providers per case (two
 * tickers, a restart), and they must share one table — the namespace is
 * per HARNESS, so they do.
 */
import { describe, expect, it } from 'vitest';
import { Redis } from 'ioredis';
import {
    remindersConformance,
    type RemindersConformanceFactory,
    type RemindersConformanceHarness
} from '@sigx/actors/testing';
import { redisReminders } from '@sigx/actors-redis';

const REDIS_URL = process.env.REDIS_URL;

const createRedisReminders: RemindersConformanceFactory =
    async (): Promise<RemindersConformanceHarness> => {
        const client = new Redis(REDIS_URL!);
        const namespace = `sigx-rem-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
        return {
            reminders: () => redisReminders({ client, namespace }),
            async stop() {
                try {
                    // SCAN, not KEYS: the latter walks the whole keyspace in
                    // one blocking call, which a shared instance notices.
                    let cursor = '0';
                    do {
                        const [next, keys] = await client.scan(cursor, 'MATCH', `${namespace}*`, 'COUNT', 500);
                        cursor = next;
                        if (keys.length) await client.del(...keys);
                    } while (cursor !== '0');
                } finally {
                    await client.quit();
                }
            }
        };
    };

describe.skipIf(!REDIS_URL)('redisReminders conformance', () => {
    for (const c of remindersConformance) {
        it(c.name, async () => {
            const outcome = await c.run(createRedisReminders);
            expect(outcome).toBeUndefined();
        }, 20_000);
    }
});

describe.runIf(!REDIS_URL)('redisReminders conformance (no REDIS_URL)', () => {
    it('skips when REDIS_URL is not set', () => {
        expect(REDIS_URL).toBeUndefined();
    });
});
