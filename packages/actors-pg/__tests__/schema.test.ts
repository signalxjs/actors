/**
 * `ensurePgSchema` under a concurrent bootstrap — #76's bug class in Postgres.
 *
 * Deterministic and server-free: `PgQueryable` is structural, and a real race
 * only reproduces by luck. The real-server proof is the bootstrap conformance
 * suite; these cases pin the mechanics, including the two details whose
 * violation would be silent — the lock riding the SAME string as the DDL, and
 * the call staying values-free so node-pg uses the simple query protocol.
 */
import { describe, expect, it } from 'vitest';
import { ensurePgSchema, pgSchemaSql, type PgQueryable } from '../src/index.ts';

interface Call {
    text: string;
    values: unknown[] | undefined;
}

interface FakePool extends PgQueryable {
    readonly calls: Call[];
}

/** Rejects the first `failures` calls with `error`, then resolves empty. */
function fakePool(failures: number, error: unknown): FakePool {
    const calls: Call[] = [];
    return {
        calls,
        query(text: string, values?: unknown[]) {
            calls.push({ text, values });
            if (calls.length <= failures) return Promise.reject(error);
            return Promise.resolve({ rows: [], rowCount: 0 });
        }
    };
}

/** A pg driver error carries its SQLSTATE on `.code`. */
function pgError(code: string, message: string): Error & { code: string } {
    return Object.assign(new Error(message), { code });
}

/**
 * The catalog races `CREATE … IF NOT EXISTS` can lose. `IF NOT EXISTS` is
 * check-then-create and is NOT atomic against a concurrent creator, so every
 * one of these is reachable from two hosts booting at once.
 */
const RACE_ERRORS = [
    pgError('23505', 'duplicate key value violates unique constraint "pg_namespace_nspname_index"'),
    pgError('23505', 'duplicate key value violates unique constraint "pg_type_typname_nsp_index"'),
    pgError('42P07', 'relation "sigx.state" already exists'),
    pgError('42710', 'index "reminders_due" already exists'),
    pgError('40P01', 'deadlock detected')
];

describe('ensurePgSchema under a concurrent bootstrap', () => {
    it.for(RACE_ERRORS)('survives $code from a racing bootstrap', async (error) => {
        const pool = fakePool(1, error);
        await expect(ensurePgSchema(pool)).resolves.toBeUndefined();
        expect(pool.calls.length).toBe(2);
    });

    it('gives up bounded, and rethrows the ORIGINAL error object', async () => {
        const boom = pgError('42501', 'permission denied for database postgres');
        const pool = fakePool(Number.POSITIVE_INFINITY, boom);
        await expect(ensurePgSchema(pool)).rejects.toBe(boom);
        expect(pool.calls.length).toBeLessThanOrEqual(12);
    });

    it('takes the advisory lock as the FIRST statement of the SAME query', async () => {
        // Both halves are load-bearing. `pg_advisory_xact_lock` is released at
        // the transaction's commit, and node-pg runs one values-free
        // multi-statement string as ONE implicit transaction — so the lock
        // covers the whole DDL and cannot leak into the pool. Split across two
        // `pool.query()` calls it could land on two different connections and
        // protect nothing.
        const pool = fakePool(0, new Error('unused'));
        await ensurePgSchema(pool);
        expect(pool.calls.length).toBe(1);
        expect(pool.calls[0]!.text.startsWith('SELECT pg_advisory_xact_lock(')).toBe(true);
        expect(pool.calls[0]!.text).toContain('CREATE TABLE IF NOT EXISTS sigx.state');
    });

    it('passes NO bind values — a values array would refuse the multi-statement query', async () => {
        // With `values`, node-pg switches to the extended query protocol,
        // which rejects multiple commands outright. That failure is a protocol
        // error at runtime rather than a visible bug, so it gets its own case.
        const pool = fakePool(0, new Error('unused'));
        await ensurePgSchema(pool);
        expect(pool.calls[0]!.values).toBeUndefined();
    });

    it('locks per schema, so unrelated schemas bootstrap concurrently', async () => {
        const a = fakePool(0, new Error('unused'));
        const b = fakePool(0, new Error('unused'));
        await ensurePgSchema(a, { schema: 'alpha' });
        await ensurePgSchema(b, { schema: 'beta' });
        const key = (call: Call): string => call.text.slice(0, call.text.indexOf('\n'));
        expect(key(a.calls[0]!)).not.toBe(key(b.calls[0]!));
    });

    it('keeps pgSchemaSql() pure DDL — a migration tool brings its own lock', async () => {
        expect(pgSchemaSql()).not.toContain('pg_advisory');
    });

    it('rejects an invalid schema immediately, without issuing anything', async () => {
        const pool = fakePool(0, new Error('unused'));
        await expect(ensurePgSchema(pool, { schema: 'Bad Schema!' })).rejects.toThrow(
            /must match/
        );
        expect(pool.calls.length).toBe(0);
    });
});
