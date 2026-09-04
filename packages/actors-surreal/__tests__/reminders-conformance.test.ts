/**
 * The shared `ActorReminders` conformance suite (#385), run against a REAL
 * SurrealDB — the same cases `shardedReminders`, Postgres, Redis and the
 * Durable Object alarm run. Env-gated on `SURREAL_URL` like the rest of
 * this package's suites; each harness gets its own namespace and drops it.
 *
 * SurrealDB-specific mechanics — shard partitioning, the transaction the
 * claim runs — stay in `surreal-reminders.test.ts`.
 */
import { describe, expect, it } from 'vitest';
import {
    remindersConformance,
    type RemindersConformanceFactory,
    type RemindersConformanceHarness
} from '@sigx/actors/testing';
import { ensureSurrealSchema, surrealReminders } from '@sigx/actors-surreal';
import { connect, dropNamespace, testNamespace, type TestDb } from './helpers';

const SURREAL_URL = process.env.SURREAL_URL;

const createSurrealReminders: RemindersConformanceFactory =
    async (): Promise<RemindersConformanceHarness> => {
        const namespace = testNamespace();
        let db: TestDb | null = null;
        return {
            async bootstrap() {
                db = await connect(namespace);
                await ensureSurrealSchema(db);
            },
            reminders: () => {
                if (!db) throw new Error('bootstrap() first');
                return surrealReminders({ db });
            },
            async stop() {
                if (db) await dropNamespace(db, namespace);
            }
        };
    };

describe.skipIf(!SURREAL_URL)('surrealReminders conformance', () => {
    for (const c of remindersConformance) {
        it(c.name, async () => {
            const outcome = await c.run(createSurrealReminders);
            expect(outcome).toBeUndefined();
        }, 30_000);
    }
});

describe.runIf(!SURREAL_URL)('surrealReminders conformance (no SURREAL_URL)', () => {
    it('skips when SURREAL_URL is not set', () => {
        expect(SURREAL_URL).toBeUndefined();
    });
});
