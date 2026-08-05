/**
 * `ensureSurrealSchema` under a concurrent bootstrap — the #76 regression.
 *
 * Deterministic and server-free on purpose: `SurrealQueryable` is structural
 * (`src/connection.ts`) precisely so a fake can stand in, and a race against a
 * REAL server can only ever reproduce a conflict by luck. The real-server
 * proof is the bootstrap conformance suite; these cases pin the mechanics —
 * that a conflict is survived at all, how many attempts it costs, and which
 * error object comes back when it is not.
 */
import { describe, expect, it } from 'vitest';
import type { SurrealQueryable } from '../src/connection.ts';
import { ensureSurrealSchema, surrealSchemaSql } from '../src/index.ts';

interface FakeDb extends SurrealQueryable {
    readonly calls: string[];
}

/**
 * A queryable that rejects the first `failures` calls with `error`, then
 * resolves. `calls` records every statement string it was given, so a case can
 * assert on both the count and the shape of what was issued.
 */
function fakeDb(failures: number, error: unknown): FakeDb {
    const calls: string[] = [];
    return {
        calls,
        query<R extends unknown[] = unknown[]>(query: string): PromiseLike<R> {
            calls.push(query);
            if (calls.length <= failures) return Promise.reject(error);
            return Promise.resolve([] as unknown as R);
        }
    };
}

/** Every call rejects — the give-up path. */
function alwaysFails(error: unknown): FakeDb {
    return fakeDb(Number.POSITIVE_INFINITY, error);
}

describe('ensureSurrealSchema under a concurrent bootstrap', () => {
    it('survives the DDL conflict two booting hosts produce (#76)', async () => {
        // The verbatim wording from the field report. It is NOT what
        // `surrealRetryable` matches — which is the whole point: the fix must
        // not depend on the server's choice of words.
        const db = fakeDb(1, new Error('Multiple key errors'));
        await expect(ensureSurrealSchema(db)).resolves.toBeUndefined();
        expect(db.calls.length).toBe(2);
    });

    it('survives the conflict wording surrealRetryable already knew', async () => {
        const db = fakeDb(
            1,
            new Error(
                'Transaction conflict: Write conflict, retry the transaction. ' +
                    'This transaction can be retried'
            )
        );
        await expect(ensureSurrealSchema(db)).resolves.toBeUndefined();
        expect(db.calls.length).toBe(2);
    });

    it('survives a conflict on a caller-owned connection with retry disabled', async () => {
        // #76's actual environment: the SDK ships retry OFF, so a `db` the
        // caller built carries no retry at all. The bootstrap's own loop is
        // the only thing between that connection and a crashed boot.
        const db = fakeDb(3, new Error('Multiple key errors'));
        await expect(ensureSurrealSchema(db)).resolves.toBeUndefined();
        expect(db.calls.length).toBe(4);
    });

    it('gives up bounded, and rethrows the ORIGINAL error object', async () => {
        // Identity, not message-matching: the #76 consumer classifies on the
        // error it receives, so wrapping it would be a breaking change.
        const boom = new Error('Multiple key errors');
        const db = alwaysFails(boom);
        await expect(ensureSurrealSchema(db)).rejects.toBe(boom);
        // Bounded — a boot that cannot work must fail, not hang.
        expect(db.calls.length).toBeLessThanOrEqual(12);
    });

    it('succeeds when the tables exist despite every attempt failing', async () => {
        // The tail the retry cannot close: a racer finished the work while we
        // were losing every conflict. If the schema is THERE, there is nothing
        // left to do and crashing the boot would be a lie.
        const calls: string[] = [];
        const db: FakeDb = {
            calls,
            query<R extends unknown[] = unknown[]>(query: string): PromiseLike<R> {
                calls.push(query);
                // Only the DDL conflicts; a plain read of the tables works,
                // which is exactly what "someone else defined them" looks like.
                if (query.includes('DEFINE TABLE')) {
                    return Promise.reject(new Error('Multiple key errors'));
                }
                return Promise.resolve([] as unknown as R);
            }
        };
        await expect(ensureSurrealSchema(db)).resolves.toBeUndefined();
        expect(calls.some((q) => !q.includes('DEFINE TABLE'))).toBe(true);
    });

    it('rethrows when the probe shows the tables are NOT there', async () => {
        // Reading an undefined table is an error in SurrealDB 3 — so a probe
        // that also fails means the schema genuinely is not present, and the
        // original failure is the honest thing to report.
        const boom = new Error('IAM error: Not enough permissions');
        await expect(ensureSurrealSchema(alwaysFails(boom))).rejects.toBe(boom);
    });

    it('rejects an invalid prefix immediately, without issuing anything', async () => {
        // A caller bug must surface on the first pass, not after five
        // backoffs — so the SQL is built once, outside the loop.
        const db = fakeDb(0, new Error('unused'));
        await expect(ensureSurrealSchema(db, { prefix: 'Bad Prefix!' })).rejects.toThrow(
            /must match/
        );
        expect(db.calls.length).toBe(0);
    });

    it('issues the same DDL the pure builder produces', async () => {
        const db = fakeDb(0, new Error('unused'));
        await ensureSurrealSchema(db, { prefix: 'demo_' });
        expect(db.calls[0]).toContain(surrealSchemaSql({ prefix: 'demo_' }).trim());
    });
});
