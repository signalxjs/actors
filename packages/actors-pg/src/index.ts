/**
 * `@sigx/actors-pg` — Postgres-backed providers for `@sigx/actors`:
 * etag-CAS actor storage, TTL-heartbeat membership, and the claim
 * directory. Postgres ≥ 13; `pg` (node-postgres) is a peer dependency.
 *
 * Table layout under one Postgres SCHEMA (default `sigx`):
 *
 *   {schema}.state               (type, key, etag, state JSON text)  PK (type, key)
 *   {schema}.directory           (actor_id, host_id, activation_id)  PK actor_id
 *   {schema}.hosts               (host_id, descriptor JSON text, expires_at)
 *   {schema}.membership_version  single row, bumped on any membership write
 *
 * Expiry runs on the DATABASE clock (`now()`), never the client's, so host
 * clock skew cannot fake a death or a survival. Directory entries carry no
 * TTL by design: one heartbeat per host, not per activation, and the
 * storage etag CAS in the actor runtime remains the integrity floor
 * underneath all of this.
 *
 * DDL is explicit: run `ensurePgSchema(pool)` from a migration or a dev
 * entry — the providers themselves never issue DDL, so a production role
 * needs only DML grants.
 */
import pg from 'pg';
import type {
    ActorDirectory,
    ClusterMembership,
    ClusterProviders,
    DirectoryEntry,
    MembershipView,
    HostDescriptor,
    HostStatus
} from '@sigx/actors/cluster';

/** One query result, as this package reads it. */
interface PgResult {
    rows: Record<string, unknown>[];
    rowCount: number | null;
}

/** The subset of `pg.Pool`/`pg.Client` the providers call — accepts a
 *  shared app pool. */
export interface PgQueryable {
    query(text: string, values?: unknown[]): Promise<PgResult>;
}

/** A checked-out connection, when LISTEN/NOTIFY push is available. */
interface PgListenClient extends PgQueryable {
    /** node-postgres: a truthy argument DESTROYS the connection instead of
     *  returning it to the pool. */
    release(destroy?: boolean): void;
    on(event: 'notification', listener: (message: { channel: string }) => void): unknown;
    on(event: 'error', listener: (error: unknown) => void): unknown;
}

/** A pool that can dedicate a connection to LISTEN. `pg.Pool` qualifies;
 *  a bare `pg.Client` does not, and membership then runs poll-only. */
export interface PgPoolLike extends PgQueryable {
    connect?(): Promise<PgListenClient>;
}

export interface PgClusterOptions {
    /** An existing pool (shared with the app). */
    pool?: PgPoolLike;
    /** Or a connection string — the package constructs its own `pg.Pool`. */
    url?: string;
    /** The Postgres schema holding the tables. Default `sigx`. */
    schema?: string;
    /** Heartbeat cadence, ms. Default 5000. */
    heartbeatMs?: number;
    /** Heartbeat row TTL, ms (missed beats past this = dead). Default 15000. */
    ttlMs?: number;
    /** Membership view poll cadence, ms — the fallback under the
     *  LISTEN/NOTIFY push. Default 5000. */
    pollMs?: number;
}

/**
 * Postgres `text` cannot carry a NUL byte — and the runtime's actor id is
 * exactly `type<NUL>key` (the compat-critical directory format), while a
 * task-ledger storage key IS an actor id. Every identifier crossing into a
 * text column goes through this injective escape (`\` doubles, NUL becomes
 * `\0` — two ASCII characters), applied symmetrically on writes and
 * lookups so nothing ever needs decoding back.
 */
export function pgText(value: string): string {
    if (!value.includes(String.fromCharCode(0))) {
        return value.includes('\\') ? value.replaceAll('\\', '\\\\') : value;
    }
    return value.replaceAll('\\', '\\\\').replaceAll(String.fromCharCode(0), '\\0');
}

/**
 * A schema name is an IDENTIFIER — it cannot ride a bind parameter, so it
 * is validated instead: the character class every generated statement can
 * safely interpolate.
 */
function checkSchema(schema: string): string {
    if (!/^[a-z_][a-z0-9_]{0,62}$/.test(schema)) {
        throw new Error(
            `[sigx actors-pg] schema ${JSON.stringify(schema)} must match ` +
                `[a-z_][a-z0-9_]* (it is interpolated as an SQL identifier).`
        );
    }
    return schema;
}

interface Resolved {
    pool: PgPoolLike;
    schema: string;
    heartbeatMs: number;
    ttlMs: number;
    pollMs: number;
}

function resolve(options: PgClusterOptions): Resolved {
    const pool =
        options.pool ??
        (options.url !== undefined
            ? (new pg.Pool({ connectionString: options.url }) as PgPoolLike)
            : undefined);
    if (!pool) {
        throw new Error('[sigx actors-pg] pass either `pool` (pg.Pool) or `url`.');
    }
    return {
        pool,
        schema: checkSchema(options.schema ?? 'sigx'),
        heartbeatMs: options.heartbeatMs ?? 5_000,
        ttlMs: options.ttlMs ?? 15_000,
        pollMs: options.pollMs ?? 5_000
    };
}

// ---------------------------------------------------------------------------
// Schema

/** The DDL for one schema — idempotent, safe to re-run. */
export function pgSchemaSql(schema = 'sigx'): string {
    const s = checkSchema(schema);
    return `
CREATE SCHEMA IF NOT EXISTS ${s};
CREATE TABLE IF NOT EXISTS ${s}.state (
    type text NOT NULL,
    key text NOT NULL,
    etag text NOT NULL,
    state text NOT NULL,
    PRIMARY KEY (type, key)
);
CREATE TABLE IF NOT EXISTS ${s}.directory (
    actor_id text PRIMARY KEY,
    host_id text NOT NULL,
    activation_id text NOT NULL
);
CREATE INDEX IF NOT EXISTS directory_host_id ON ${s}.directory (host_id);
CREATE TABLE IF NOT EXISTS ${s}.hosts (
    host_id text PRIMARY KEY,
    descriptor text NOT NULL,
    expires_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS ${s}.membership_version (
    id int PRIMARY KEY CHECK (id = 1),
    version bigint NOT NULL
);
INSERT INTO ${s}.membership_version (id, version) VALUES (1, 0)
    ON CONFLICT (id) DO NOTHING;
`;
}

/**
 * Run the DDL. For dev and tests; production migrations should carry
 * `pgSchemaSql()` through whatever tool already owns their schema.
 */
export async function ensurePgSchema(
    pool: PgQueryable,
    options: { schema?: string } = {}
): Promise<void> {
    await pool.query(pgSchemaSql(options.schema ?? 'sigx'));
}

/** Membership + directory for ONE host. */
export function pgCluster(options: PgClusterOptions): ClusterProviders {
    const resolved = resolve(options);
    return {
        membership: pgMembership(resolved.pool, {
            schema: resolved.schema,
            heartbeatMs: resolved.heartbeatMs,
            ttlMs: resolved.ttlMs,
            pollMs: resolved.pollMs
        }),
        directory: pgDirectory(resolved.pool, { schema: resolved.schema })
    };
}

// ---------------------------------------------------------------------------
// Membership

const noop = (): void => {};

export function pgMembership(
    pool: PgPoolLike,
    options: Omit<PgClusterOptions, 'pool' | 'url'> = {}
): ClusterMembership {
    const s = checkSchema(options.schema ?? 'sigx');
    const heartbeatMs = options.heartbeatMs ?? 5_000;
    const ttlMs = options.ttlMs ?? 15_000;
    const pollMs = options.pollMs ?? 5_000;
    // Postgres identifiers truncate at 63 bytes — but only LISTEN would
    // truncate (`pg_notify` takes a plain string and would not), so a long
    // schema would silently split the channel. Truncating HERE keeps both
    // sides on one name whatever the schema length.
    const channel = `${s}_membership`.slice(0, 63);

    let self: HostDescriptor | null = null;
    let cached: MembershipView = { version: 0, hosts: [] };
    /** hostId:status join of the last view — expiry changes the view
     *  without a version bump, so the version alone cannot detect change. */
    let signature = '';
    let beat: ReturnType<typeof setInterval> | null = null;
    let poll: ReturnType<typeof setInterval> | null = null;
    let listener: PgListenClient | null = null;
    let lastOkMs = 0;
    let suspected = false;
    const changeCbs = new Set<(view: MembershipView) => void>();
    const suspectCbs = new Set<() => void>();

    const writeSelf = async (): Promise<void> => {
        if (!self) return;
        await pool.query(
            `INSERT INTO ${s}.hosts (host_id, descriptor, expires_at)
             VALUES ($1, $2, now() + make_interval(secs => $3::float8 / 1000.0))
             ON CONFLICT (host_id) DO UPDATE
             SET descriptor = EXCLUDED.descriptor, expires_at = EXCLUDED.expires_at`,
            [self.hostId, JSON.stringify(self), ttlMs]
        );
        lastOkMs = Date.now();
        suspected = false;
    };

    // Bump the version, then push: listeners refresh immediately instead of
    // waiting out a poll interval. The poll stays as the safety net.
    const bumpVersion = async (): Promise<void> => {
        const result = await pool.query(
            `UPDATE ${s}.membership_version SET version = version + 1
             WHERE id = 1 RETURNING version`
        );
        const version = String(result.rows[0]?.['version'] ?? '');
        await pool.query(`SELECT pg_notify($1, $2)`, [channel, version]).catch(noop);
    };

    const refresh = async (): Promise<MembershipView> => {
        const [versionRow, live] = await Promise.all([
            pool.query(`SELECT version FROM ${s}.membership_version WHERE id = 1`),
            pool.query(`SELECT descriptor FROM ${s}.hosts WHERE expires_at > now()`)
        ]);
        // Lazy prune, best-effort: rows a TTL already excluded, kept only so
        // the table does not accumulate dead hosts forever. The grace keeps
        // a slow-beating-but-alive host's row from being deleted under it.
        void pool
            .query(
                `DELETE FROM ${s}.hosts
                 WHERE expires_at < now() - make_interval(secs => $1::float8 / 1000.0)`,
                [ttlMs * 4]
            )
            .catch(noop);
        const hosts = live.rows.map((row) => JSON.parse(row['descriptor'] as string) as HostDescriptor);
        const next: MembershipView = {
            version: Number(versionRow.rows[0]?.['version'] ?? 0),
            hosts
        };
        const nextSignature = hosts
            .map((h) => `${h.hostId}:${h.status}`)
            .sort()
            .join(',');
        const changed = next.version !== cached.version || nextSignature !== signature;
        cached = next;
        signature = nextSignature;
        if (changed) for (const cb of changeCbs) cb(next);
        return next;
    };

    return {
        async join(descriptor) {
            self = descriptor;
            await writeSelf();
            await bumpVersion();
            await refresh();
            beat = setInterval(() => {
                void writeSelf().catch(() => {
                    // Can't prove our own membership past the TTL → fence.
                    if (!suspected && Date.now() - lastOkMs > ttlMs) {
                        suspected = true;
                        for (const cb of suspectCbs) cb();
                    }
                });
            }, heartbeatMs);
            (beat as { unref?: () => void }).unref?.();
            poll = setInterval(() => void refresh().catch(noop), pollMs);
            (poll as { unref?: () => void }).unref?.();
            // Push, when the pool can dedicate a connection: LISTEN turns a
            // membership write anywhere into an immediate refresh here. Any
            // failure just leaves the poll in charge.
            if (pool.connect) {
                try {
                    const sub = await pool.connect();
                    sub.on('notification', () => void refresh().catch(noop));
                    sub.on('error', noop);
                    await sub.query(`LISTEN "${channel}"`);
                    listener = sub;
                } catch {
                    listener = null;
                }
            }
        },
        async setStatus(status: HostStatus) {
            if (!self) return;
            self = { ...self, status };
            await writeSelf();
            await bumpVersion();
        },
        async leave() {
            if (beat) clearInterval(beat);
            if (poll) clearInterval(poll);
            beat = poll = null;
            if (listener) {
                try {
                    // DESTROY, never pool: LISTEN state and the
                    // notification handler live on the CONNECTION, so a
                    // pooled return would hand the next checkout a client
                    // that still fires this membership's stale refresh.
                    listener.release(true);
                } catch {
                    // A broken connection releases however it can.
                }
                listener = null;
            }
            if (!self) return;
            const id = self.hostId;
            self = null;
            await pool.query(`DELETE FROM ${s}.hosts WHERE host_id = $1`, [id]);
            await bumpVersion().catch(noop);
        },
        view: () => cached,
        refresh,
        async isAlive(hostId) {
            const result = await pool.query(
                `SELECT 1 FROM ${s}.hosts WHERE host_id = $1 AND expires_at > now()`,
                [hostId]
            );
            return (result.rowCount ?? 0) > 0;
        },
        onChange(cb) {
            changeCbs.add(cb);
            return () => changeCbs.delete(cb);
        },
        onSelfSuspect(cb) {
            suspectCbs.add(cb);
            return () => suspectCbs.delete(cb);
        }
    };
}

// ---------------------------------------------------------------------------
// Directory

export function pgDirectory(
    pool: PgQueryable,
    options: { schema?: string } = {}
): ActorDirectory {
    const s = checkSchema(options.schema ?? 'sigx');

    const rowEntry = (row: Record<string, unknown>): DirectoryEntry => ({
        hostId: row['host_id'] as string,
        activationId: row['activation_id'] as string
    });

    return {
        async lookup(actorId) {
            const result = await pool.query(
                `SELECT host_id, activation_id FROM ${s}.directory WHERE actor_id = $1`,
                [pgText(actorId)]
            );
            return result.rows[0] ? rowEntry(result.rows[0]) : null;
        },
        async claim(actorId, mine) {
            // Create-if-absent AND read-the-winner in ONE statement: on
            // conflict, the no-op update locks the existing row and
            // RETURNING hands back its values — so the answer is atomic and
            // a lost race can never be misreported as a win (which would be
            // two hosts both believing they own the actor). The price of a
            // lost claim is one dead tuple; lost claims are rare.
            const result = await pool.query(
                `INSERT INTO ${s}.directory (actor_id, host_id, activation_id)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (actor_id) DO UPDATE
                 SET host_id = ${s}.directory.host_id
                 RETURNING host_id, activation_id`,
                [pgText(actorId), mine.hostId, mine.activationId]
            );
            return rowEntry(result.rows[0]!);
        },
        async release(actorId, expected) {
            await pool.query(
                `DELETE FROM ${s}.directory
                 WHERE actor_id = $1 AND host_id = $2 AND activation_id = $3`,
                [pgText(actorId), expected.hostId, expected.activationId]
            );
        },
        async evict(actorId, expected) {
            const result = await pool.query(
                `DELETE FROM ${s}.directory
                 WHERE actor_id = $1 AND host_id = $2 AND activation_id = $3`,
                [pgText(actorId), expected.hostId, expected.activationId]
            );
            return (result.rowCount ?? 0) === 1;
        },
        async evictHost(hostId) {
            const result = await pool.query(
                `DELETE FROM ${s}.directory WHERE host_id = $1`,
                [hostId]
            );
            return result.rowCount ?? 0;
        }
    };
}

// ---------------------------------------------------------------------------
// Storage

export { pgStorage, type PgStorageOptions } from './storage';
