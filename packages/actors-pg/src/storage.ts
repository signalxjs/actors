/**
 * `pgStorage` — Postgres-backed `ActorStorage` with etag CAS: one row per
 * actor in `{schema}.state`, no TTL — durability is the point.
 *
 * Etags are client-minted UUIDs (equality-compared only). Compare-and-set
 * is one statement each way — `INSERT … ON CONFLICT DO NOTHING` for
 * create, `UPDATE … WHERE etag = $expected` for update, `DELETE … WHERE
 * etag = $expected` for clear — so the row count IS the verdict and
 * nothing needs a transaction. A mismatch throws the branded
 * `ActorStorageConflict`, the integrity floor the cluster design leans on.
 * Reminders ride this storage automatically.
 *
 * State is stored as JSON in a `text` column, DELIBERATELY not `jsonb`:
 * actor state may legitimately contain NUL — a reminder shard record keys
 * entries by `type<NUL>key`, and user strings can hold anything — and
 * `jsonb` parses the backslash-u0000 escape into a real NUL byte and rejects it,
 * while the SERIALIZED form is plain ASCII and always storable. The
 * explicit `JSON.stringify` also sidesteps node-postgres coercing a JS
 * array parameter into a Postgres ARRAY literal. Type and key columns go
 * through `pgText` for the same NUL reason: a task-ledger storage key IS
 * an actor id.
 */
import pg from 'pg';
import { ActorStorageConflict, type ActorStorage } from '@sigx/actors';
import { pgText, type PgQueryable } from './index';

export interface PgStorageOptions {
    /** An existing pool (shared with the cluster providers). */
    pool?: PgQueryable;
    /** Or a connection string — the package constructs its own `pg.Pool`. */
    url?: string;
    /** The Postgres schema holding the tables. Default `sigx`. */
    schema?: string;
}

/** Same identifier rule as the providers — see `checkSchema` in index. */
function checkSchema(schema: string): string {
    if (!/^[a-z_][a-z0-9_]{0,62}$/.test(schema)) {
        throw new Error(
            `[sigx actors-pg] schema ${JSON.stringify(schema)} must match ` +
                `[a-z_][a-z0-9_]* (it is interpolated as an SQL identifier).`
        );
    }
    return schema;
}

export function pgStorage(options: PgStorageOptions): ActorStorage {
    const pool =
        options.pool ??
        (options.url !== undefined
            ? (new pg.Pool({ connectionString: options.url }) as PgQueryable)
            : undefined);
    if (!pool) {
        throw new Error('[sigx actors-pg] pass either `pool` (pg.Pool) or `url`.');
    }
    const s = checkSchema(options.schema ?? 'sigx');

    // The one CAS body, reached with JSON either way — from the host's own
    // single-walk emitter via `saveText`, or from a tree `save` stringifies
    // itself (#238). A local function rather than `this.saveText`, so the
    // two halves stay wired together even if the storage is destructured.
    // A `const` arrow, not a hoisted declaration: the `if (!pool) throw`
    // above narrows `pool` for what follows it, and a function declaration
    // could have been called before that check.
    const put = async (
        type: string,
        key: string,
        json: string,
        expectedEtag: string | null
    ): Promise<string> => {
        const etag = globalThis.crypto.randomUUID();
        const result =
            expectedEtag === null
                ? await pool.query(
                      `INSERT INTO ${s}.state (type, key, etag, state)
                       VALUES ($1, $2, $3, $4)
                       ON CONFLICT (type, key) DO NOTHING`,
                      [pgText(type), pgText(key), etag, json]
                  )
                : await pool.query(
                      `UPDATE ${s}.state SET etag = $3, state = $4
                       WHERE type = $1 AND key = $2 AND etag = $5`,
                      [pgText(type), pgText(key), etag, json, expectedEtag]
                  );
        if ((result.rowCount ?? 0) !== 1) throw new ActorStorageConflict(type, key);
        return etag;
    };

    return {
        async load(type, key) {
            const result = await pool.query(
                `SELECT etag, state FROM ${s}.state WHERE type = $1 AND key = $2`,
                [pgText(type), pgText(key)]
            );
            const row = result.rows[0];
            if (!row) return null;
            return { state: JSON.parse(row['state'] as string) as unknown, etag: row['etag'] as string };
        },
        save: (type, key, state, expectedEtag) =>
            put(type, key, JSON.stringify(state), expectedEtag),
        saveText: put,
        async clear(type, key, expectedEtag) {
            if (expectedEtag === null) {
                // "No record expected": success iff none exists. No write
                // happens either way, so the check needs no atomicity — a
                // concurrent create simply ordered itself after this clear.
                const exists = await pool.query(
                    `SELECT 1 FROM ${s}.state WHERE type = $1 AND key = $2`,
                    [pgText(type), pgText(key)]
                );
                if ((exists.rowCount ?? 0) > 0) throw new ActorStorageConflict(type, key);
                return;
            }
            const result = await pool.query(
                `DELETE FROM ${s}.state WHERE type = $1 AND key = $2 AND etag = $3`,
                [pgText(type), pgText(key), expectedEtag]
            );
            if ((result.rowCount ?? 0) !== 1) throw new ActorStorageConflict(type, key);
        }
    };
}
