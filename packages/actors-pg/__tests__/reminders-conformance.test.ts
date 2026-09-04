/**
 * The shared `ActorReminders` conformance suite (#385), run against a REAL
 * Postgres — the same cases `shardedReminders`, Redis, SurrealDB and the
 * Durable Object alarm run. Env-gated on `PG_URL` like the rest of this
 * package's suites; each harness gets its own schema and drops it.
 *
 * Postgres-specific mechanics — the SQL the claim runs, the `SKIP LOCKED`
 * posture — stay in `pg-reminders.test.ts`.
 */
import { describe, expect, it } from 'vitest';
import pg from 'pg';
import {
    remindersConformance,
    type RemindersConformanceFactory,
    type RemindersConformanceHarness
} from '@sigx/actors/testing';
import { ensurePgSchema, pgReminders } from '@sigx/actors-pg';

const PG_URL = process.env.PG_URL;

const createPgReminders: RemindersConformanceFactory = async (): Promise<RemindersConformanceHarness> => {
    const pool = new pg.Pool({ connectionString: PG_URL });
    const schema = `sigx_rem_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
    return {
        bootstrap: () => ensurePgSchema(pool, { schema }),
        reminders: () => pgReminders({ pool, schema }),
        async stop() {
            try {
                await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
            } finally {
                await pool.end();
            }
        }
    };
};

describe.skipIf(!PG_URL)('pgReminders conformance', () => {
    for (const c of remindersConformance) {
        it(c.name, async () => {
            const outcome = await c.run(createPgReminders);
            expect(outcome).toBeUndefined();
        }, 20_000);
    }
});

describe.runIf(!PG_URL)('pgReminders conformance (no PG_URL)', () => {
    it('skips when PG_URL is not set', () => {
        expect(PG_URL).toBeUndefined();
    });
});
