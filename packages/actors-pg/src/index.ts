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
import {
    heartbeatClock,
    refreshCoalescer,
    type ActorDirectory,
    type ClusterMembership,
    type ClusterProviders,
    type DirectoryEntry,
    type MembershipView,
    type HostDescriptor,
    type HostStatus
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
    on(
        event: 'notification',
        listener: (message: { channel: string; payload?: string }) => void
    ): unknown;
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
    /**
     * Trailing quiet window for coalescing push notifications, ms. The
     * listener is single-flight either way (a burst of N changes costs one
     * refresh plus at most one trailing catch-up, not N); a non-zero window
     * widens the net past one round-trip at the price of that much extra
     * staleness. Default 0.
     */
    coalesceMs?: number;
}

/**
 * Postgres `text` cannot carry a NUL byte — and the runtime's actor id is
 * exactly `type<NUL>key` (the compat-critical directory format), while a
 * task-ledger storage key IS an actor id. Every identifier crossing into a
 * text column goes through this injective escape (`\` doubles, NUL becomes
 * `\0` — two ASCII characters), applied symmetrically on writes and
 * lookups. Storage and the directory never decode; the reminder table
 * must (a claimed row has to become a deliverable ref again), which is
 * what `pgTextDecode` exists for.
 */
export function pgText(value: string): string {
    if (!value.includes(String.fromCharCode(0))) {
        return value.includes('\\') ? value.replaceAll('\\', '\\\\') : value;
    }
    return value.replaceAll('\\', '\\\\').replaceAll(String.fromCharCode(0), '\\0');
}

/** The exact inverse of `pgText`. */
export function pgTextDecode(value: string): string {
    if (!value.includes('\\')) return value;
    let out = '';
    for (let i = 0; i < value.length; i++) {
        const c = value[i]!;
        if (c !== '\\') {
            out += c;
            continue;
        }
        const next = value[++i];
        out += next === '0' ? String.fromCharCode(0) : (next ?? '\\');
    }
    return out;
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
CREATE TABLE IF NOT EXISTS ${s}.reminders (
    type text NOT NULL,
    key text NOT NULL,
    name text NOT NULL,
    next_due timestamptz NOT NULL,
    period_ms bigint,
    PRIMARY KEY (type, key, name)
);
CREATE INDEX IF NOT EXISTS reminders_due ON ${s}.reminders (next_due);
`;
}

/**
 * The `classid` half of the bootstrap's advisory-lock key — an arbitrary
 * constant reserved for this package ("SGAC"), fitting int4. The `objid` half
 * hashes the schema name, so two DIFFERENT schemas bootstrap concurrently
 * while one schema does not. A collision with an unrelated application's key
 * costs milliseconds of extra serialisation — never correctness.
 */
const SCHEMA_LOCK_CLASS = 0x53474143;
/** Bounded, matching the surreal sibling. */
const SCHEMA_ATTEMPTS = 5;
const SCHEMA_BACKOFF_MS = 25;

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/** FNV-1a 32-bit, as a signed int4 — the `objid` half of the lock key. */
function schemaLockKey(schema: string): number {
    let hash = 0x811c9dc5;
    for (let i = 0; i < schema.length; i++) {
        hash ^= schema.charCodeAt(i);
        hash = Math.imul(hash, 0x01000193);
    }
    return hash | 0;
}

/**
 * Run the DDL. Safe to call from EVERY replica at boot, concurrently.
 *
 * Production migrations should still carry `pgSchemaSql()` through whatever
 * tool already owns their schema — but a booting host calling this directly is
 * the documented quickstart, and `CREATE … IF NOT EXISTS` is check-then-create,
 * NOT atomic against a concurrent creator: two hosts starting together race the
 * catalog and one of them takes a `23505`, `42P07`, `42710` or `40P01` and
 * crashes its boot (#78, the Postgres half of #76).
 *
 * Two mechanisms, with distinct jobs:
 *
 *  - **The advisory lock serialises**, so replicas queue for milliseconds and
 *    every one of them takes the clean path — nothing raises, nothing backs
 *    off. Three details make it work, and each is easy to break:
 *      1. It is the FIRST statement of the SAME string. node-pg sends a
 *         values-free multi-statement string as a SIMPLE query, which Postgres
 *         runs as ONE implicit transaction — so an xact-scoped lock covers the
 *         whole DDL and is released at its commit, before the pooled client is
 *         recycled. No session lock can leak into the pool, and no explicit
 *         `pg_advisory_unlock` is needed. Split into two `pool.query()` calls
 *         it can land on two different connections and protect nothing.
 *      2. The key is INTERPOLATED, not bound. A `values` array switches node-pg
 *         to the extended protocol, which refuses multiple commands outright.
 *      3. `pgSchemaSql()` stays pure DDL — it is the string you hand a
 *         migration tool, which splits statements, wraps them in its own
 *         transaction and brings its own migration lock. The lock belongs to
 *         *this* function, whose whole job is "run this now, from a booting
 *         process".
 *  - **The retry is the backstop**, for racers that never take the lock at all
 *    (a migration tool, psql, an older version of this package). It is blind to
 *    the error's shape for the same reasons as its surreal sibling: the whole
 *    DDL is idempotent and the implicit transaction is all-or-nothing, so a
 *    bounded retry cannot change a permanent failure's outcome — only its
 *    latency — and cannot be defeated by a SQLSTATE we failed to enumerate.
 *    Backoff is jittered so two racers do not then retry in lockstep.
 *
 * This function expects to OWN its transaction. Called with a `PgQueryable`
 * already inside a caller's open transaction, a failure poisons that
 * transaction and the retry burns its attempts on `25P02`.
 */
export async function ensurePgSchema(
    pool: PgQueryable,
    options: { schema?: string } = {}
): Promise<void> {
    // Built ONCE, outside the loop: an invalid schema is a caller bug and must
    // throw on the first pass, not after five backoffs.
    const schema = checkSchema(options.schema ?? 'sigx');
    const sql =
        `SELECT pg_advisory_xact_lock(${SCHEMA_LOCK_CLASS}, ${schemaLockKey(schema)});\n` +
        pgSchemaSql(schema);
    let last: unknown;
    for (let attempt = 1; attempt <= SCHEMA_ATTEMPTS; attempt++) {
        try {
            await pool.query(sql);
            return;
        } catch (error) {
            last = error;
            if (attempt < SCHEMA_ATTEMPTS) {
                await sleep(SCHEMA_BACKOFF_MS * 2 ** (attempt - 1) * (0.5 + Math.random()));
            }
        }
    }
    throw last;
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
    const coalesceMs = options.coalesceMs ?? 0;
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
    /** Heartbeat writes still on the wire. `leave()` drains them before its
     *  DELETE: `clearInterval` stops FUTURE ticks, but a write already
     *  handed to the pool is neither awaited nor ordered against the
     *  DELETE — it can commit AFTER it, resurrecting the row until its TTL
     *  lapses (#209). */
    const inflight = new Set<Promise<unknown>>();
    /** Set by `leave()`: a beat completing after it began must not confirm
     *  the clock, and nothing may issue a new write. */
    let left = false;
    /** Once per membership, not per refresh — the prune rides every poll
     *  tick, and a permanently failing one would warn forever (#268). */
    let pruneWarned = false;
    const changeCbs = new Set<(view: MembershipView) => void>();
    const suspectCbs = new Set<() => void>();
    const clock = heartbeatClock({
        ttlMs,
        onSuspect: () => {
            for (const cb of suspectCbs) cb();
        }
    });

    const writeSelf = async (): Promise<void> => {
        if (!self || left) return;
        const write = pool.query(
            `INSERT INTO ${s}.hosts (host_id, descriptor, expires_at)
             VALUES ($1, $2, now() + make_interval(secs => $3::float8 / 1000.0))
             ON CONFLICT (host_id) DO UPDATE
             SET descriptor = EXCLUDED.descriptor, expires_at = EXCLUDED.expires_at`,
            [self.hostId, JSON.stringify(self), ttlMs]
        );
        inflight.add(write);
        try {
            await write;
        } finally {
            inflight.delete(write);
        }
        // `leave()` began while this was on the wire: the row is about to
        // go, so the clock must not read this as a live confirmation.
        if (left) return;
        clock.confirmed();
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
        // Best-effort is not silent, though — a PERMANENTLY failing prune
        // (permissions, schema drift) accumulates dead rows forever, so the
        // first failure warns under dev, matching the reminders tick (#268).
        void pool
            .query(
                `DELETE FROM ${s}.hosts
                 WHERE expires_at < now() - make_interval(secs => $1::float8 / 1000.0)`,
                [ttlMs * 4]
            )
            .catch((error: unknown) => {
                if (__DEV__ && !pruneWarned) {
                    pruneWarned = true;
                    console.warn('[sigx actors-pg] membership prune failed:', error);
                }
            });
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

    // Coalesce the notification→refresh path (#26): a burst of NOTIFYs costs
    // one refresh plus at most one trailing catch-up per listener, not one
    // refresh per message. The version gate rides the NOTIFY payload; silent
    // expiries have no version, but they arrive via the poll, which also
    // goes through the coalescer (as an unskippable demand).
    const coalescer = refreshCoalescer<MembershipView>({
        refresh,
        version: () => cached.version,
        quietMs: coalesceMs
    });

    return {
        async join(descriptor) {
            left = false;
            self = descriptor;
            await writeSelf();
            await bumpVersion();
            await refresh();
            // Stamped HERE, not at the upsert above: `bumpVersion` and a full
            // refresh sit between them and can outlast `ttlMs` on a large or
            // slow database, which would make the first beat late by
            // construction — a terminal fence on every host at startup.
            clock.arm();
            beat = setInterval(() => {
                // Before the write, deliberately: if the window lapsed our
                // claims are already gone, and waiting out a round-trip only
                // lets a doomed activation take more turns (#45).
                clock.beat();
                void writeSelf().catch(() => clock.failed());
            }, heartbeatMs);
            (beat as { unref?: () => void }).unref?.();
            poll = setInterval(() => void coalescer.demand().catch(noop), pollMs);
            (poll as { unref?: () => void }).unref?.();
            // Push, when the pool can dedicate a connection: LISTEN turns a
            // membership write anywhere into an immediate refresh here. Any
            // failure just leaves the poll in charge.
            if (pool.connect) {
                try {
                    const sub = await pool.connect();
                    sub.on('notification', (message) => {
                        const noted = Number(message.payload);
                        coalescer.note(Number.isFinite(noted) && noted > 0 ? noted : undefined);
                    });
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
            left = true;
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
            // Listener destroyed FIRST (no new NOTIFYs can schedule work —
            // under churn they could hold `settled()` open indefinitely),
            // THEN drain what already started; refreshes read via the pool,
            // not the listener connection, so they finish fine without it.
            await coalescer.settled();
            if (!self) return;
            const id = self.hostId;
            self = null;
            // A beat the pool already holds must land BEFORE the DELETE, or
            // it lands after and the row comes back (#209).
            await Promise.allSettled(inflight);
            await pool.query(`DELETE FROM ${s}.hosts WHERE host_id = $1`, [id]);
            await bumpVersion().catch(noop);
        },
        view: () => cached,
        // Demand semantics: resolves with a refresh that started at-or-after
        // the call — placement's failure path relies on the refreshed view
        // excluding a leaver it just observed.
        refresh: () => coalescer.demand(),
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

// ---------------------------------------------------------------------------
// Reminders

export { pgReminders, type PgRemindersOptions } from './reminders';
