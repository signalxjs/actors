// @vitest-environment node
// Node, NOT the repo-default happy-dom: its `fetch` enforces browser CORS
// and refuses the host-to-host hop — the same reason `infra.test.ts` says so.
/**
 * The workflow engine under host loss (#297), on the database the cluster
 * actually deploys with (#383): the same cases as the memory-backed file,
 * over `redisStorage` — a real etag CAS, real reminder shards, a real
 * round trip under every save. What this file adds is that
 * `reminderSetFailures` and `wakesLost` stay zero on a store that can
 * genuinely lose a CAS, not one that never does.
 *
 * Env-gated on `REDIS_URL` like every provider suite; the `test-redis` CI
 * job provides it. Each run gets its own namespace and deletes it after.
 */
import { describe, expect, it } from 'vitest';
import { Redis } from 'ioredis';
import { redisStorage } from '@sigx/actors-redis';
import { workflowClusterSuite } from './helpers/wf-cluster.ts';

process.env.WF_TIMER_THRESHOLD_MS = '100';
process.env.WF_REMINDER_TICK_MS = '50';
process.env.WF_STALE_WAKE_MS = '300';
process.env.WF_CHILD_STALE_MS = '200';
process.env.WF_DEACTIVATE_ON_SLEEP = '0';
process.env.WF_IDLE_AFTER_MS = '600000';
process.env.WF_STATS_SAVE_EVERY = '1';
process.env.WF_NOTIFY_RETRY_MS = '300';

// Empty counts as unset (`REDIS_URL= pnpm test …`), as the provider suites read it.
const REDIS_URL = process.env.REDIS_URL || undefined;

if (REDIS_URL) {
    workflowClusterSuite('redis', async () => {
        const client = new Redis(REDIS_URL, { enableAutoPipelining: true });
        const namespace = `wf-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
        return {
            storage: redisStorage({ client, namespace }),
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
    });
}

describe.runIf(!REDIS_URL)('workflow cluster (redis)', () => {
    it('skips the Redis suite when REDIS_URL is not set', () => {
        expect(REDIS_URL).toBeUndefined();
    });
});
