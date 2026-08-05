/**
 * The shared bootstrap conformance suite, run against a REAL Postgres.
 *
 * The same cases the SurrealDB package runs — deliberately, because the two
 * converge by different mechanisms (an advisory lock here, jittered retry
 * there) and only the OUTCOME is common. The deterministic mechanics live in
 * `schema.test.ts`.
 *
 * Each racer gets its OWN pool, so the concurrency is real rather than
 * serialised inside one client.
 */
import { afterAll, describe, expect, it } from 'vitest';
import pg from 'pg';
import type {
    BootstrapConformanceFactory,
    BootstrapConformanceHarness
} from '@sigx/actors/testing';
import { bootstrapConformance, BOOTSTRAP_RACERS } from '@sigx/actors/testing';
import { ensurePgSchema, pgStorage } from '@sigx/actors-pg';

const PG_URL = process.env.PG_URL;

const createPgBootstrap: BootstrapConformanceFactory =
    async (): Promise<BootstrapConformanceHarness> => {
        const schema = `sigx_boot_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
        // The admin pool creates nothing and reads back through storage, so it
        // cannot serialise the racers by sharing their connection.
        const admin = new pg.Pool({ connectionString: PG_URL });
        // One pool per racer, each with its connection ALREADY OPEN — a
        // `pg.Pool` connects lazily, so without the warm-up query the first
        // `ensurePgSchema` would pay a TCP handshake and the racers would
        // stagger past each other instead of colliding.
        const ready = await Promise.all(
            Array.from({ length: BOOTSTRAP_RACERS }, async () => {
                const pool = new pg.Pool({ connectionString: PG_URL, max: 1 });
                await pool.query('SELECT 1');
                return pool;
            })
        );
        let next = 0;
        return {
            async bootstrap() {
                const pool = ready[next++];
                if (!pool) throw new Error('bootstrap conformance: ran out of pre-opened racers');
                await ensurePgSchema(pool, { schema });
            },
            storage: () => pgStorage({ pool: admin, schema }),
            async stop() {
                await admin.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
                await Promise.allSettled(ready.map((pool) => pool.end()));
                await admin.end();
            }
        };
    };

describe.skipIf(!PG_URL)('bootstrap conformance: ensurePgSchema()', () => {
    for (const testCase of bootstrapConformance) {
        it(testCase.name, async (ctx) => {
            const outcome = await testCase.run(createPgBootstrap);
            if (outcome && 'skipped' in outcome) {
                ctx.skip(outcome.skipped);
            }
            expect(outcome).toBeUndefined();
        });
    }
});

describe.runIf(!PG_URL)('bootstrap conformance (pg)', () => {
    it('skips the Postgres suite when PG_URL is not set', () => {
        expect(PG_URL).toBeUndefined();
    });
});
