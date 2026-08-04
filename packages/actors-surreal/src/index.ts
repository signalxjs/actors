/**
 * `@sigx/actors-surreal` — SurrealDB-backed providers for `@sigx/actors`:
 * etag-CAS actor storage, TTL-heartbeat membership, the claim directory and
 * durable reminders. SurrealDB ≥ 3.0 (3.2.4+ recommended); the `surrealdb`
 * JS SDK (2.x) is a peer dependency.
 *
 * Table layout under one PREFIX (default `sigx_`):
 *
 *   {p}state:[type, key]           { e: etag, s: state JSON }
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
import type {
    ActorDirectory,
    ClusterMembership,
    ClusterProviders,
    DirectoryEntry,
    HostDescriptor,
    HostStatus,
    MembershipView
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

/**
 * Run the DDL. For dev and tests; production migrations should carry
 * `surrealSchemaSql()` through whatever tool already owns their schema.
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
    await db.query(surrealSchemaSql(options));
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
    /** hostId:status join of the last view — an expiry changes the view
     *  without a version bump, so the version alone cannot detect change. */
    let signature = '';
    let beat: ReturnType<typeof setInterval> | null = null;
    let poll: ReturnType<typeof setInterval> | null = null;
    let live: LiveHandle | null = null;
    let lastOkMs = 0;
    let suspected = false;
    const changeCbs = new Set<(view: MembershipView) => void>();
    const suspectCbs = new Set<() => void>();

    const writeSelf = async (): Promise<void> => {
        if (!self) return;
        await db.query(WRITE_SELF, {
            id: hostRid(self.hostId),
            d: JSON.stringify(self),
            ttl: ttlMs
        });
        lastOkMs = Date.now();
        suspected = false;
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
        void Promise.resolve(db.query(PRUNE, { grace: ttlMs * 4 })).catch(noop);
        const hosts = descriptors.map((row) => JSON.parse(row) as HostDescriptor);
        const next: MembershipView = { version: Number(version ?? 0), hosts };
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
            handle.subscribe(() => void refresh().catch(noop));
            live = handle;
        } catch {
            // The HTTP engine, a multi-node deployment, a server without the
            // capability — the poll covers all of them.
            live = null;
        }
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
            await startPush();
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
            if (live) {
                try {
                    await live.kill();
                } catch {
                    // A dead socket unsubscribes itself.
                }
                live = null;
            }
            if (!self) return;
            const id = self.hostId;
            self = null;
            await db.query(LEAVE, { id: hostRid(id) });
            await bumpVersion().catch(noop);
        },
        view: () => cached,
        refresh,
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
