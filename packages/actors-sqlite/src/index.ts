/**
 * `sqliteStorage` — embedded, single-node `ActorStorage` on `node:sqlite`
 * (#65): one row per actor in one table keyed by `(type, key)`, no TTL —
 * durability is the point. The persistence option for a host that wants a
 * real database in a file and nothing to operate: no server, no pool, no
 * DDL to run — the table is created on open, `IF NOT EXISTS`.
 *
 * Etags are the row's integer version, minted by the database and returned
 * as a decimal string: a create writes version 1, every update is
 * `version = version + 1`. Compare-and-set is ONE statement each way —
 * `INSERT … ON CONFLICT DO NOTHING` for create, `UPDATE … WHERE version =
 * ?` for update, `DELETE … WHERE version = ?` for clear — and SQLite runs
 * each statement as its own write transaction, so the row count IS the
 * verdict and nothing needs an explicit BEGIN. A mismatch throws the
 * branded `ActorStorageConflict`. Like `fileStorage`, and unlike the UUID
 * providers, the version restarts at 1 when a cleared key is re-created;
 * the runtime never carries an etag across a clear, so the chain is intact
 * for every writer that could hold one.
 *
 * State is stored as JSON text. `JSON.stringify` writes NUL as the
 * six-character `\u0000` escape, so the serialized form is always plain
 * text — but `type` and `key` are raw strings, and an actor key may carry
 * NUL (the runtime keys a task ledger by `type<NUL>key`). SQLite stores
 * such a string with its full length when bound as a parameter, and then
 * stops at the first NUL in every text function and every read — the row
 * is distinct in the index and unreadable everywhere else. So both
 * identifiers go through the same injective escape `@sigx/actors-pg` uses
 * (`\` doubles, NUL becomes the two characters `\0`), applied
 * symmetrically on writes and lookups; the store never decodes.
 *
 * `node:sqlite` is synchronous and in-process: every method here does its
 * work on the calling turn and returns a settled promise. That is the
 * trade this package makes deliberately — a save costs microseconds, not a
 * round-trip, and one process owns the file. It is NOT a cluster store: two
 * hosts pointing at one file serialize on SQLite's write lock (a
 * `busy_timeout` is set so the second waits instead of failing), but the
 * cluster-safe options remain `@sigx/actors-redis` and `@sigx/actors-pg`.
 */
import { DatabaseSync } from 'node:sqlite';
import { ActorStorageConflict, type ActorStorage } from '@sigx/actors';

export interface SqliteStorageOptions {
    /**
     * Path of the database file, created if missing — or `':memory:'` for a
     * database that lives as long as the storage does. The package opens it,
     * switches the journal to WAL and sets a 5 s `busy_timeout`.
     */
    path?: string;
    /**
     * Or an already-open `DatabaseSync` the caller owns and configures. No
     * pragma is touched on a database passed in.
     */
    database?: DatabaseSync;
    /** The table holding actor state. Default `sigx_state`. */
    table?: string;
}

export interface SqliteStorage extends ActorStorage {
    /** Always present here: this adapter stores the serialized form (#238). */
    saveText(type: string, key: string, json: string, expectedEtag: string | null): Promise<string>;
    /**
     * Close the underlying database. Every later call rejects; a second
     * `close()` is a no-op, so a `finally` and an explicit stop path can
     * both call it. A storage opened by `path` should be closed when its
     * host stops; one over a caller's `database` closes that database, so
     * call it only if the storage is its last user.
     */
    close(): void;
}

/** Same identifier rule as `@sigx/actors-pg` — the table name is interpolated as an SQL identifier. */
function checkTable(table: string): string {
    if (!/^[A-Za-z_][A-Za-z0-9_]{0,62}$/.test(table)) {
        throw new Error(
            `[sigx actors-sqlite] table ${JSON.stringify(table)} must match ` +
                `[A-Za-z_][A-Za-z0-9_]* (it is interpolated as an SQL identifier).`
        );
    }
    return table;
}

/**
 * The injective NUL escape (see the module comment): `\` doubles, NUL
 * becomes `\0`. Identical to `pgText`, so a key reads the same in both
 * stores' tables.
 */
function sqliteText(value: string): string {
    if (!value.includes(String.fromCharCode(0))) {
        return value.includes('\\') ? value.replaceAll('\\', '\\\\') : value;
    }
    return value.replaceAll('\\', '\\\\').replaceAll(String.fromCharCode(0), '\\0');
}

/**
 * An etag this store could have minted: the decimal form of a positive
 * safe integer, exactly. Anything else names no row — `'stale'`, `''`, a
 * UUID from another provider — and is a conflict before the database is
 * asked, which is the same answer the WHERE clause would give.
 */
function versionOf(etag: string): number | null {
    if (!/^[1-9][0-9]{0,15}$/.test(etag)) return null;
    const version = Number(etag);
    return Number.isSafeInteger(version) ? version : null;
}

export function sqliteStorage(options: SqliteStorageOptions): SqliteStorage {
    const table = checkTable(options.table ?? 'sigx_state');
    let db: DatabaseSync;
    if (options.database && options.path !== undefined) {
        // Not "database wins": a `path` that opened nothing would surface
        // later as an empty file nobody can explain.
        throw new Error('[sigx actors-sqlite] pass either `path` or `database` (DatabaseSync), not both.');
    } else if (options.database) {
        db = options.database;
    } else if (options.path !== undefined) {
        db = new DatabaseSync(options.path);
        // WAL lets readers proceed while a save commits and survives a crash
        // mid-write; `busy_timeout` makes a second writer on the same file
        // wait for the lock instead of throwing SQLITE_BUSY on the spot.
        db.exec('PRAGMA journal_mode = WAL');
        db.exec('PRAGMA busy_timeout = 5000');
    } else {
        throw new Error('[sigx actors-sqlite] pass either `path` or `database` (DatabaseSync).');
    }

    // `WITHOUT ROWID`: the composite primary key IS the table, so a load is
    // one B-tree probe and there is no second rowid index to maintain.
    db.exec(
        `CREATE TABLE IF NOT EXISTS ${table} (
            type TEXT NOT NULL,
            key TEXT NOT NULL,
            version INTEGER NOT NULL,
            state TEXT NOT NULL,
            PRIMARY KEY (type, key)
        ) WITHOUT ROWID`
    );

    const selectStmt = db.prepare(
        `SELECT version, state FROM ${table} WHERE type = ? AND key = ?`
    );
    const existsStmt = db.prepare(`SELECT 1 FROM ${table} WHERE type = ? AND key = ?`);
    const insertStmt = db.prepare(
        `INSERT INTO ${table} (type, key, version, state) VALUES (?, ?, 1, ?)
         ON CONFLICT (type, key) DO NOTHING`
    );
    const updateStmt = db.prepare(
        `UPDATE ${table} SET version = version + 1, state = ?
         WHERE type = ? AND key = ? AND version = ?`
    );
    const deleteStmt = db.prepare(
        `DELETE FROM ${table} WHERE type = ? AND key = ? AND version = ?`
    );

    // The one CAS body, reached with JSON either way — from the host's own
    // single-walk emitter via `saveText`, or from a tree `save` stringifies
    // itself (#238). A local function, so the two halves stay wired together
    // even if the storage is destructured. `Number(changes)`: a caller's
    // database may have `readBigInts` on, and `1n !== 1`.
    const put = async (
        type: string,
        key: string,
        json: string,
        expectedEtag: string | null
    ): Promise<string> => {
        const t = sqliteText(type);
        const k = sqliteText(key);
        if (expectedEtag === null) {
            if (Number(insertStmt.run(t, k, json).changes) !== 1) {
                throw new ActorStorageConflict(type, key);
            }
            return '1';
        }
        const version = versionOf(expectedEtag);
        if (version === null || Number(updateStmt.run(json, t, k, version).changes) !== 1) {
            throw new ActorStorageConflict(type, key);
        }
        return String(version + 1);
    };

    let closed = false;
    return {
        async load(type, key) {
            const row = selectStmt.get(sqliteText(type), sqliteText(key)) as
                | { version: number | bigint; state: string }
                | undefined;
            if (!row) return null;
            return { state: JSON.parse(row.state) as unknown, etag: String(row.version) };
        },
        save: (type, key, state, expectedEtag) =>
            put(type, key, JSON.stringify(state), expectedEtag),
        saveText: put,
        async clear(type, key, expectedEtag) {
            const t = sqliteText(type);
            const k = sqliteText(key);
            if (expectedEtag === null) {
                // "No record expected": success iff none exists. No write
                // happens either way, so the check needs no atomicity — a
                // concurrent create simply ordered itself after this clear.
                if (existsStmt.get(t, k) !== undefined) throw new ActorStorageConflict(type, key);
                return;
            }
            const version = versionOf(expectedEtag);
            if (version === null || Number(deleteStmt.run(t, k, version).changes) !== 1) {
                throw new ActorStorageConflict(type, key);
            }
        },
        close() {
            if (closed) return;
            closed = true;
            db.close();
        }
    };
}
