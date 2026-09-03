/**
 * The shared `ActorStorage` conformance suite (#65), run against a REAL
 * Redis — the same cases `memoryStorage`, `fileStorage`, Postgres, SurrealDB
 * and the Durable Object adapter run, so "redisStorage is a storage" is one
 * list of outcomes rather than this package's own reading of the contract.
 *
 * Redis-specific mechanics — the Lua CAS under a same-tick burst, auto-
 * pipelining on a url-constructed client, the CAS race — stay in
 * `redis-storage.test.ts`.
 *
 * Redis has no schema to bootstrap, so the harness omits `bootstrap()`.
 * Every case gets its own key namespace on its own connection and deletes
 * it afterwards.
 */
import { describe, expect, it } from 'vitest';
import { Redis } from 'ioredis';
import {
    storageConformance,
    type StorageConformanceFactory,
    type StorageConformanceHarness
} from '@sigx/actors/testing';
import { redisStorage } from '@sigx/actors-redis';

const REDIS_URL = process.env.REDIS_URL;

const createRedisStorage: StorageConformanceFactory =
    async (): Promise<StorageConformanceHarness> => {
        const client = new Redis(REDIS_URL!);
        const namespace = `sigx-stor-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
        return {
            storage: () => redisStorage({ client, namespace }),
            // redisStorage stores the serialized form and implements the
            // text path (#238): the saveText cases must run here, not skip.
            saveText: true,
            // ... and keeps a per-record log (#312): the append cases too.
            appendText: true,
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

describe.skipIf(!REDIS_URL)('storage conformance: redisStorage()', () => {
    for (const testCase of storageConformance) {
        it(testCase.name, async (ctx) => {
            const outcome = await testCase.run(createRedisStorage);
            if (outcome && 'skipped' in outcome) {
                ctx.skip(outcome.skipped);
            }
            expect(outcome).toBeUndefined();
        });
    }
});

describe.runIf(!REDIS_URL)('storage conformance (redis)', () => {
    it('skips the Redis suite when REDIS_URL is not set', () => {
        expect(REDIS_URL).toBeUndefined();
    });
});
