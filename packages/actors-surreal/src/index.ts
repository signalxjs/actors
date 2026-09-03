/**
 * `@sigx/actors-surreal` — SurrealDB-backed providers for `@sigx/actors`:
 * etag-CAS actor storage, TTL-heartbeat membership, the claim directory and
 * durable reminders. SurrealDB ≥ 3.0 (3.2.4+ recommended); the `surrealdb`
 * JS SDK (2.x) is a peer dependency.
 *
 * Table layout under one PREFIX (default `sigx_`):
 *
 *   {p}state:[type, key]           { e: etag, s: state JSON, l: [entry JSON, …] }
 *   {p}dir:<actorId>               { h: hostId, a: activationId }
 *   {p}host:<hostId>               { d: descriptor JSON, x: expires_at }
 *   {p}mver:1                      { v: version }
 *   {p}reminder:[type, key, name]  { t, k, n, tk, sh, d: due, p: period_ms }
 *
 * Every hot-path record is addressed by its RECORD ID — the primary index —
 * never by a `WHERE` scan. Composite array ids are v3's partitioning tool,
 * and they make `[type, key]` an O(1) lookup with no secondary index at all.
 * The three secondary indexes that do exist serve only cold sweeps.
 *
 * Expiry runs on the DATABASE clock (`time::now()`), never the client's, so
 * host clock skew cannot fake a death or a survival. Directory entries carry
 * no TTL by design: one heartbeat per host, not per activation, and the
 * storage etag CAS in the actor runtime remains the integrity floor
 * underneath all of this.
 *
 * DDL is explicit: run `ensureSurrealSchema(db)` from a migration or a dev
 * entry — the providers themselves never issue DDL. Unlike Postgres this is
 * NOT optional: reading an undefined table is an error in SurrealDB 3 (it
 * returned `[]` in 2.x).
 *
 * Three v3 facts shape everything below, each verified against a live 3.2.4
 * server rather than taken from the docs:
 *
 *  - **`UPSERT … WHERE` does not gate the create path.** Its create arm has
 *    no condition check, so `UPSERT $id CONTENT $c WHERE e = $etag` would
 *    RESURRECT a record another writer deleted. State CAS therefore uses
 *    `UPDATE … WHERE` plus an explicit create, never `UPSERT`.
 *  - **There is no `SELECT … FOR UPDATE`, no `SKIP LOCKED`, no advisory
 *    lock.** Snapshot isolation with a commit-time write–write conflict
 *    check is the only mutual exclusion — which makes retry load-bearing
 *    rather than cosmetic (see `surrealRetryable`).
 *  - **`UPDATE`/`DELETE` do not use indexes.** Every predicate-driven write
 *    here pushes its predicate through a `SELECT` subquery, which does.
 */
import { RecordId, Table } from 'surrealdb';
import {
    heartbeatClock,
    refreshCoalescer,
    type ActorDirectory,
    type ClusterMembership,
    type ClusterProviders,
    type DirectoryEntry,
    type HostDescriptor,
    type HostStatus,
    type MembershipView
} from '@sigx/actors/cluster';
import {
    surrealHandle,
    tablesFor,
    type LiveHandle,
    type SurrealConnectionOptions,
    type SurrealQueryable
} from './connection';

export {
    surrealRetryable,
    type SurrealConnectionOptions,
    type SurrealQueryable,
    type SurrealTables
} from './connection';

export interface SurrealClusterOptions extends SurrealConnectionOptions {
    /** Heartbeat cadence, ms. Default 5000. */
    heartbeatMs?: number;
    /** Heartbeat record TTL, ms (missed beats past this = dead). Default 15000. */
    ttlMs?: number;
    /** Membership view poll cadence, ms — the fallback under the live-query
     *  push. Default 5000. */
    pollMs?: number;
    /** Subscribe to a live query for immediate membership convergence.
     *  Default true; the poll remains the guarantee either way. */
    push?: boolean;
    /**
     * Trailing quiet window for coalescing push notifications, ms. The
     * subscriber is single-flight either way (a burst of N changes costs
     * one refresh plus at most one trailing catch-up, not N); a non-zero
     * window widens the net past one round-trip at the price of that much
     * extra staleness. Default 0.
     */
    coalesceMs?: number;
}

// ---------------------------------------------------------------------------
// Schema

/**
 * The DDL for one prefix — idempotent, safe to re-run.
 *
 * Every table is SCHEMAFULL: this package is the only writer and all five
 * shapes are fixed, so a typo becomes an error at the write rather than a
 * silently ignored field. Note that in v3 a SCHEMAFULL table REJECTS an
 * undefined field rather than dropping it, which is what makes that useful.
 */
export function surrealSchemaSql(options: { prefix?: string } = {}): string {
    const t = tablesFor(options.prefix);
    return `
DEFINE TABLE IF NOT EXISTS ${t.state} SCHEMAFULL TYPE NORMAL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS e ON ${t.state} TYPE string;
DEFINE FIELD IF NOT EXISTS s ON ${t.state} TYPE string;
DEFINE FIELD IF NOT EXISTS l ON ${t.state} TYPE array<string> DEFAULT [];

DEFINE TABLE IF NOT EXISTS ${t.dir} SCHEMAFULL TYPE NORMAL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS h ON ${t.dir} TYPE string;
DEFINE FIELD IF NOT EXISTS a ON ${t.dir} TYPE string;
DEFINE INDEX IF NOT EXISTS ${t.dir}_host ON TABLE ${t.dir} FIELDS h;

DEFINE TABLE IF NOT EXISTS ${t.host} SCHEMAFULL TYPE NORMAL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS d ON ${t.host} TYPE string;
DEFINE FIELD IF NOT EXISTS x ON ${t.host} TYPE datetime;
DEFINE INDEX IF NOT EXISTS ${t.host}_expiry ON TABLE ${t.host} FIELDS x;

DEFINE TABLE IF NOT EXISTS ${t.mver} SCHEMAFULL TYPE NORMAL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS v ON ${t.mver} TYPE int;

DEFINE TABLE IF NOT EXISTS ${t.reminder} SCHEMAFULL TYPE NORMAL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS t ON ${t.reminder} TYPE string;
DEFINE FIELD IF NOT EXISTS k ON ${t.reminder} TYPE string;
DEFINE FIELD IF NOT EXISTS n ON ${t.reminder} TYPE string;
DEFINE FIELD IF NOT EXISTS tk ON ${t.reminder} TYPE string;
DEFINE FIELD IF NOT EXISTS sh ON ${t.reminder} TYPE string;
DEFINE FIELD IF NOT EXISTS d ON ${t.reminder} TYPE datetime;
DEFINE FIELD IF NOT EXISTS p ON ${t.reminder} TYPE int;
DEFINE INDEX IF NOT EXISTS ${t.reminder}_due ON TABLE ${t.reminder} FIELDS sh, d;
DEFINE INDEX IF NOT EXISTS ${t.reminder}_actor ON TABLE ${t.reminder} FIELDS tk;
`;
}

/** Bounded like `CLAIM_ATTEMPTS` in `reminders.ts`, and for the same reason. */
const SCHEMA_ATTEMPTS = 5;
const SCHEMA_BACKOFF_MS = 25;

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Do the five tables actually exist?
 *
 * This parses NOTHING. Reading an undefined table is an ERROR in SurrealDB 3
 * (it returned `[]` in 2.x) — the same v3 rule that makes the DDL step
 * mandatory in the first place — so five reads that all resolve are proof the
 * tables are defined, on any 3.x server, in any wording.
 *
 * It proves tables, not fields or indexes. That is the right trade at the only
 * point it is consulted: after every attempt has already failed, where the
 * question is "did a racer finish the work?" rather than "is this schema
 * perfect?". A missing index would be a performance regression the next boot's
 * DDL repairs, not a correctness break — and the alternative, parsing
 * `INFO FOR DB`/`INFO FOR TABLE`, ties the check to one server version's
 * output shape.
 */
async function schemaPresent(db: SurrealQueryable, prefix: string | undefined): Promise<boolean> {
    const t = tablesFor(prefix);
    const probe = Object.values(t)
        .map((table) => `SELECT * FROM ${table} LIMIT 0;`)
        .join('\n');
    try {
        await db.query(probe);
        return true;
    } catch {
        return false;
    }
}

/**
 * Run the DDL. Safe to call from EVERY replica at boot, concurrently.
 *
 * Production migrations should still carry `surrealSchemaSql()` through
 * whatever tool already owns their schema — but a booting host calling this
 * directly is the documented quickstart, so two replicas starting together is
 * the normal case rather than the exotic one (#76).
 *
 * SurrealDB has no lock primitive (see the v3 rules at the top of this file),
 * so convergence is by retry — and the retry is deliberately **blind to the
 * error's shape**:
 *
 *  - It cannot change the outcome of a permanent failure, only its latency.
 *    Bad credentials, an unselected database, a server that is down: all still
 *    fail, with the SAME error object, ~0.4 s later. There is no error class a
 *    bounded retry converts into a wrong success — which is what makes the
 *    broad rule safe here and NOT safe as `surrealRetryable`'s connection-wide
 *    predicate (see the note there).
 *  - Every statement is `IF NOT EXISTS` and runs in its own transaction, so a
 *    re-run is a sequence of no-ops plus whatever is left. Each round has
 *    strictly less to conflict over than the last: this converges, it does not
 *    livelock. The backoff is jittered so two hosts that collide do not then
 *    retry in lockstep.
 *  - Matching on wording has already failed once, on this exact call. #76
 *    arrived as `Multiple key errors`; `surrealRetryable` knows a second
 *    wording and `reminders.ts` a third. A fourth would reopen the bug.
 *
 * A caller who installed `surrealRetryable` on their own connection now has
 * two nested bounded layers (5 × 5). That is intentional and harmless — both
 * are bounded — so do not "simplify" either one away.
 *
 * The connection's namespace and database must already exist — `connect()`
 * SELECTS them, it does not create them. `DEFINE NAMESPACE`/`DEFINE DATABASE`
 * are a deployment decision (and need root), so they are deliberately not
 * issued here.
 */
export async function ensureSurrealSchema(
    db: SurrealQueryable,
    options: { prefix?: string } = {}
): Promise<void> {
    // Built ONCE, outside the loop: an invalid prefix is a caller bug and must
    // throw on the first pass, not after five backoffs.
    const sql = surrealSchemaSql(options);
    let last: unknown;
    for (let attempt = 1; attempt <= SCHEMA_ATTEMPTS; attempt++) {
        try {
            await db.query(sql);
            return;
        } catch (error) {
            last = error;
            if (attempt < SCHEMA_ATTEMPTS) {
                await sleep(SCHEMA_BACKOFF_MS * 2 ** (attempt - 1) * (0.5 + Math.random()));
            }
        }
    }
    // The tail retrying cannot close: a racer may have completed the schema
    // while we lost every conflict. If the tables are there, the work is done
    // and crashing the boot would be a lie.
    if (await schemaPresent(db, options.prefix)) return;
    throw last;
}

/** Membership + directory for ONE host. */
export function surrealCluster(options: SurrealClusterOptions): ClusterProviders {
    // One handle, so a `url` opens ONE socket for both providers, not two.
    const db = surrealHandle(options);
    return {
        membership: surrealMembership({ ...options, db }),
        directory: surrealDirectory({ ...options, db })
    };
}

// ---------------------------------------------------------------------------
// Membership

const noop = (): void => {};

export function surrealMembership(options: SurrealClusterOptions): ClusterMembership {
    const db = surrealHandle(options);
    const t = tablesFor(options.prefix);
    const heartbeatMs = options.heartbeatMs ?? 5_000;
    const ttlMs = options.ttlMs ?? 15_000;
    const pollMs = options.pollMs ?? 5_000;
    const wantPush = options.push ?? true;

    const MVER = new RecordId(t.mver, 1);
    const hostRid = (id: string): RecordId => new RecordId(t.host, id);

    const WRITE_SELF = `UPSERT $id CONTENT { d: $d, x: time::now() + duration::from_millis($ttl) } RETURN NONE`;
    const BUMP = `RETURN (UPSERT $id SET v += 1 RETURN VALUE v)[0]`;
    const REFRESH =
        `SELECT VALUE v FROM ONLY $id;` + `SELECT VALUE d FROM ${t.host} WHERE x > time::now();`;
    // The predicate rides a SELECT subquery because DELETE ignores indexes.
    const PRUNE =
        `DELETE (SELECT VALUE id FROM ${t.host} ` +
        `WHERE x < time::now() - duration::from_millis($grace)) RETURN NONE`;
    // `WHERE id = $id` is recognised as a record-id lookup by the v3 planner,
    // so this is a primary-index hit and not a scan.
    const IS_ALIVE =
        `RETURN array::len((SELECT VALUE id FROM ${t.host} ` +
        `WHERE id = $id AND x > time::now())) > 0`;
    const LEAVE = `DELETE $id RETURN NONE`;

    let self: HostDescriptor | null = null;
    let cached: MembershipView = { version: 0, hosts: [] };
    /** `sigx_mver` as last read. Held apart from `cached.version`, which may
     *  run AHEAD of it: see `refresh`. */
    let storeVersion = 0;
    /** hostId:status join of the last view — an expiry changes the view
     *  without a counter bump, so the counter alone cannot detect change. */
    let signature = '';
    let beat: ReturnType<typeof setInterval> | null = null;
    let poll: ReturnType<typeof setInterval> | null = null;
    let live: LiveHandle | null = null;
    /** Heartbeat writes still on the wire. `leave()` drains them before its
     *  DELETE: `clearInterval` stops FUTURE ticks, but a write already
     *  handed to the connection is neither awaited nor ordered against the
     *  DELETE — it can commit AFTER it, resurrecting the row until its TTL
     *  lapses (#209). */
    const inflight = new Set<PromiseLike<unknown>>();
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
        const write = db.query(WRITE_SELF, {
            id: hostRid(self.hostId),
            d: JSON.stringify(self),
            ttl: ttlMs
        });
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

    // Bump the version, then let the live query push it: listeners refresh
    // immediately instead of waiting out a poll interval. The poll stays as
    // the safety net.
    const bumpVersion = async (): Promise<void> => {
        await db.query(BUMP, { id: MVER });
    };

    const refresh = async (): Promise<MembershipView> => {
        const [version, descriptors] = await db.query<[number | null, string[]]>(REFRESH, {
            id: MVER
        });
        // Lazy prune, best-effort: rows a TTL already excluded, kept only so
        // the table does not accumulate dead hosts forever. The grace keeps a
        // slow-beating-but-alive host's row from being deleted under it.
        // Best-effort is not silent, though — a PERMANENTLY failing prune
        // (permissions, schema drift) accumulates dead rows forever, so the
        // first failure warns under dev, matching the reminders tick (#268).
        void Promise.resolve(db.query(PRUNE, { grace: ttlMs * 4 })).catch((error: unknown) => {
            if (__DEV__ && !pruneWarned) {
                pruneWarned = true;
                console.warn('[sigx actors-surreal] membership prune failed:', error);
            }
        });
        const hosts = descriptors.map((row) => JSON.parse(row) as HostDescriptor);
        const stored = Number(version ?? 0);
        const nextSignature = hosts
            .map((h) => `${h.hostId}:${h.status}`)
            .sort()
            .join(',');
        const changed = stored !== storeVersion || nextSignature !== signature;
        // Unchanged: hand back the SAME object, so identity-keyed derived
        // data (`membersMemo()`, placement's caches) holds across polls.
        if (!changed) return cached;
        // The exposed version is a per-PROCESS change token, not `sigx_mver`:
        // an expiry changes `hosts` with no writer to bump anything, so every
        // observed change advances it — past the counter when it must — and
        // the two re-align only once written bumps carry the counter past it
        // (#267). A consumer keyed on `version` therefore sees the expiry too.
        const next: MembershipView = { version: Math.max(stored, cached.version + 1), hosts };
        storeVersion = stored;
        cached = next;
        signature = nextSignature;
        for (const cb of changeCbs) cb(next);
        return next;
    };

    // Coalesce the notification→refresh path (#26): a burst of live-query
    // events costs one refresh plus at most one trailing catch-up per
    // subscriber, not one refresh per event. Events are noted versionless —
    // live queries are at-most-once and unordered, so no event is skippable;
    // the gate still reads the STORE's counter, never the exposed version.
    const coalescer = refreshCoalescer<MembershipView>({
        refresh,
        version: () => storeVersion,
        quietMs: options.coalesceMs ?? 0
    });

    /**
     * Best-effort push. Subscribes to the version TABLE, not the version
     * RECORD: a record-scoped live query fails to listen on 3.2.4 even though
     * the feature probe reports live queries supported, while a table-scoped
     * one works — and this table holds exactly the one record, so the fan-out
     * is identical either way.
     *
     * Live queries are documented single-node-only, unordered and
     * at-most-once, and a silent expiry produces no write to notify on at
     * all. So this is strictly an accelerator: the poll above is the
     * guarantee, exactly as with pg's LISTEN/NOTIFY.
     */
    const startPush = async (): Promise<void> => {
        if (!wantPush || typeof db.live !== 'function') return;
        try {
            const handle = await db.live(new Table(t.mver));
            handle.subscribe(() => coalescer.note());
            live = handle;
        } catch {
            // The HTTP engine, a multi-node deployment, a server without the
            // capability — the poll covers all of them.
            live = null;
        }
    };

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
            await startPush();
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
            if (live) {
                try {
                    await live.kill();
                } catch {
                    // A dead socket unsubscribes itself.
                }
                live = null;
            }
            // Live query killed FIRST (no new events can schedule work —
            // under churn they could hold `settled()` open indefinitely),
            // THEN drain what already started; refreshes run on the shared
            // db handle, not the live query.
            await coalescer.settled();
            if (!self) return;
            const id = self.hostId;
            self = null;
            // A beat already on the wire must land BEFORE the DELETE, or it
            // lands after and the row comes back — UPSERT does not notice a
            // concurrent delete (rule 1 above) (#209).
            await Promise.allSettled(inflight);
            await db.query(LEAVE, { id: hostRid(id) });
            await bumpVersion().catch(noop);
        },
        view: () => cached,
        // Demand semantics: resolves with a refresh that started at-or-after
        // the call — placement's failure path relies on the refreshed view
        // excluding a leaver it just observed.
        refresh: () => coalescer.demand(),
        async isAlive(id) {
            const [alive] = await db.query<[boolean]>(IS_ALIVE, { id: hostRid(id) });
            return alive === true;
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

export function surrealDirectory(options: SurrealConnectionOptions): ActorDirectory {
    const db = surrealHandle(options);
    const t = tablesFor(options.prefix);

    /**
     * Create-if-absent AND read-the-winner in ONE statement, so the answer is
     * atomic and a lost race can never be misreported as a win (which would
     * be two hosts both believing they own the actor).
     *
     * Two racers both observe `!exists` and both CREATE the same key; the
     * commit-time write–write check aborts one, it retries, sees the record
     * and returns the winner. That retry is `surrealRetryable` — without it
     * the loser raises instead of resolving.
     */
    const CLAIM =
        `RETURN IF record::exists($id) { SELECT h, a FROM ONLY $id } ` +
        `ELSE { CREATE $id CONTENT { h: $h, a: $a } RETURN NONE; { h: $h, a: $a } }`;
    const LOOKUP = `SELECT h, a FROM ONLY $id`;
    const COMPARE_DEL = `RETURN array::len((DELETE $id WHERE h = $h AND a = $a RETURN BEFORE)) > 0`;
    // DELETE ignores indexes; the SELECT subquery hits `{dir}_host`.
    const EVICT_HOST = `RETURN array::len((DELETE (SELECT VALUE id FROM ${t.dir} WHERE h = $h) RETURN BEFORE))`;

    /** An actor id is `type<NUL>key`, and SurrealDB carries the NUL verbatim
     *  and injectively inside a record id — no escaping layer, unlike
     *  Postgres `text`. */
    const rid = (actorId: string): RecordId => new RecordId(t.dir, actorId);
    const entry = (row: { h: string; a: string }): DirectoryEntry => ({
        hostId: row.h,
        activationId: row.a
    });

    return {
        async lookup(actorId) {
            const [row] = await db.query<[{ h: string; a: string } | null]>(LOOKUP, {
                id: rid(actorId)
            });
            return row ? entry(row) : null;
        },
        async claim(actorId, mine) {
            const [row] = await db.query<[{ h: string; a: string }]>(CLAIM, {
                id: rid(actorId),
                h: mine.hostId,
                a: mine.activationId
            });
            return entry(row);
        },
        async release(actorId, expected) {
            await db.query(COMPARE_DEL, {
                id: rid(actorId),
                h: expected.hostId,
                a: expected.activationId
            });
        },
        async evict(actorId, expected) {
            const [removed] = await db.query<[boolean]>(COMPARE_DEL, {
                id: rid(actorId),
                h: expected.hostId,
                a: expected.activationId
            });
            return removed === true;
        },
        async evictHost(hostId) {
            const [count] = await db.query<[number]>(EVICT_HOST, { h: hostId });
            return Number(count ?? 0);
        }
    };
}

// ---------------------------------------------------------------------------
// Storage

export { surrealStorage, type SurrealStorageOptions } from './storage';

// ---------------------------------------------------------------------------
// Reminders

export { surrealReminders, type SurrealRemindersOptions } from './reminders';
