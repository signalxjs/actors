/**
 * The shared `ActorStorage` conformance suite (#65), run against a REAL
 * Postgres — the same cases `memoryStorage`, `fileStorage`, Redis, SurrealDB
 * and the Durable Object adapter run, so "pgStorage is a storage" is one
 * list of outcomes rather than this package's own reading of the contract.
 *
 * Postgres-specific mechanics — the injective NUL escape, the jsonb array
 * trap, the CAS race under real concurrency — stay in `pg-storage.test.ts`.
 *
 * Every case gets a fresh schema through the same `ensurePgSchema` a booting
 * host runs, and drops it afterwards; the harness is the one
 * `bootstrap-conformance.test.ts` would want, minus the racers.
 */
import { describe, expect, it } from 'vitest';
import pg from 'pg';
import {
    storageConformance,
    type StorageConformanceFactory,
    type StorageConformanceHarness
} from '@sigx/actors/testing';
import { ensurePgSchema, pgStorage } from '@sigx/actors-pg';

const PG_URL = process.env.PG_URL;

const createPgStorage: StorageConformanceFactory = async (): Promise<StorageConformanceHarness> => {
    const schema = `sigx_stor_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
    const pool = new pg.Pool({ connectionString: PG_URL });
    return {
        bootstrap: () => ensurePgSchema(pool, { schema }),
        storage: () => pgStorage({ pool, schema }),
        // pgStorage stores the serialized form and implements the text path
        // (#238): the saveText cases must run here, not skip.
        saveText: true,
        async stop() {
            await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
            await pool.end();
        }
    };
};

describe.skipIf(!PG_URL)('storage conformance: pgStorage()', () => {
    for (const testCase of storageConformance) {
        it(testCase.name, async (ctx) => {
            const outcome = await testCase.run(createPgStorage);
            if (outcome && 'skipped' in outcome) {
                ctx.skip(outcome.skipped);
            }
            expect(outcome).toBeUndefined();
        });
    }
});

describe.runIf(!PG_URL)('storage conformance (pg)', () => {
    it('skips the Postgres suite when PG_URL is not set', () => {
        expect(PG_URL).toBeUndefined();
    });
});
