/**
 * `surrealReminders` — durable reminders on a SurrealDB table with a
 * due-time index, replacing the default sharded scan for deployments that
 * already run SurrealDB. This is the reminder-scan answer for large tables:
 * the default `shardedReminders()` loads and scans fixed shard records in
 * `ActorStorage` every tick, which is honest work at dev scale and a
 * bottleneck at "many reminders"; here a tick is one indexed query for
 * exactly the due rows.
 *
 * **Shard ownership is honoured, and that is the whole design.** Its pg
 * sibling deliberately IGNORES `ownsShard`, because `FOR UPDATE SKIP LOCKED`
 * lets every host scan one table and claim disjoint rows — row locks replace
 * rendezvous hashing. SurrealDB has no row lock, no `SKIP LOCKED` and no
 * advisory lock, so the only alternatives are for hosts to contend and let
 * one abort, or for them not to overlap at all. This provider takes the
 * second: each row carries a shard, a host claims only shards it owns, and
 * the compound `(sh, d)` index serves the query. Steady-state cross-host
 * contention is zero.
 *
 * Transient divergence — two hosts briefly believing they own one shard —
 * stays safe: the claim is one transaction, so the loser aborts at commit
 * and re-selects an already-advanced set. The conflict IS the mutual
 * exclusion where partitioning has not converged yet.
 *
 * - **At-most-once per firing**: the advance/delete commits BEFORE any
 *   dispatch, so a crash between commit and dispatch skips one firing rather
 *   than double-firing — the same posture as `shardedReminders()`.
 * - **No catch-up bursts**: a periodic reminder advances to
 *   `time::now() + period`, so after downtime it fires once and resumes its
 *   cadence rather than replaying the gap.
 * - **Database clock throughout**: `due` is applied to `time::now()` on the
 *   server, so host clock skew cannot fire early or late.
 *
 * Delivery failures are `allSettled` — one bad reminder cannot kill the loop
 * — and the loop is single-flight per provider instance.
 */
import type { ActorReminders, ActorRemindersContext, ReminderApi } from '@sigx/actors';
import { RecordId } from 'surrealdb';
import { surrealHandle, tablesFor, type SurrealConnectionOptions } from './connection';

export interface SurrealRemindersOptions extends SurrealConnectionOptions {
    /** Rows claimed per query. A tick keeps claiming until a batch comes back
     *  short (bounded), so this caps memory, not throughput. Default 200. */
    batchSize?: number;
}

/** Same floor as the default provider: anything tighter is a timer's job. */
const MIN_PERIOD_MS = 60_000;

/** Claim batches per tick — a backlog drains fast without an unbounded loop
 *  if rows keep becoming due mid-tick. */
const MAX_BATCHES_PER_TICK = 10;

/**
 * COMPAT-CRITICAL, exactly as for the runtime's own shard table: the count
 * and the hash are storage identity. An actorId must map to the same shard
 * forever, or reminders silently vanish between records on upgrade.
 *
 * This is a shard space of this package's own — `ownsShard` accepts any
 * string and answers by rendezvous hashing over the active membership view,
 * so it need not (and does not) match the runtime's internal one.
 */
const SHARD_COUNT = 16;

/** FNV-1a 32-bit — tiny, deterministic, and pinned forever (see above). */
function fnv1a(value: string): number {
    let hash = 0x811c9dc5;
    for (let i = 0; i < value.length; i++) {
        hash ^= value.charCodeAt(i);
        hash = Math.imul(hash, 0x01000193) >>> 0;
    }
    return hash >>> 0;
}

const shardOf = (actorId: string): string => `p${fnv1a(actorId) % SHARD_COUNT}`;
const ALL_SHARDS: readonly string[] = Array.from({ length: SHARD_COUNT }, (_v, i) => `p${i}`);

/**
 * The `BEGIN…COMMIT` rewrite. A conflicting explicit transaction reports
 * `Transaction conflict: …` only on its COMMIT row; every earlier row is
 * rewritten to this, and the SDK throws on the FIRST failing statement — so
 * the retryable message never reaches `surrealRetryable`.
 *
 * Matching it is safe HERE and only here: this transaction contains no
 * `THROW`, so an abort can only mean a conflict, and re-running it is
 * idempotent — it simply re-selects whatever is due now. It is not safe as a
 * global predicate, which is why `surrealRetryable` omits it.
 */
const ABORTED = /The query was not executed due to a failed transaction/;
const CLAIM_ATTEMPTS = 5;

/** Statement index of the `RETURN $due` row: BEGIN, LET, UPDATE, DELETE,
 *  RETURN, COMMIT — a `query()` result carries one entry per statement,
 *  including the transaction-control ones. */
const DUE_ROW = 4;

interface DueRow {
    t: string;
    k: string;
    n: string;
}

export function surrealReminders(options: SurrealRemindersOptions = {}): ActorReminders {
    const db = surrealHandle(options);
    const t = tablesFor(options.prefix);
    const batchSize = Math.max(1, options.batchSize ?? 200);

    /**
     * Select the due rows, advance the periodic ones and delete the
     * one-shots, atomically. `d` is in the projection because v3 rejects an
     * `ORDER BY` over a field the selection omits; the two writes push their
     * predicates through `SELECT` subqueries because `UPDATE`/`DELETE` do not
     * use indexes.
     */
    const CLAIM = `
BEGIN;
LET $due = (SELECT id, t, k, n, p, d FROM ${t.reminder}
            WHERE sh IN $shards AND d <= time::now() ORDER BY d LIMIT $n);
UPDATE (SELECT VALUE id FROM $due WHERE p > 0) SET d = time::now() + duration::from_millis(p) RETURN NONE;
DELETE (SELECT VALUE id FROM $due WHERE p = 0) RETURN NONE;
RETURN $due;
COMMIT;
`;
    const SET = `UPSERT $id CONTENT { t: $t, k: $k, n: $n, tk: $tk, sh: $sh,
                d: time::now() + duration::from_millis($due), p: $p } RETURN NONE`;
    const CLEAR = `DELETE $id RETURN NONE`;
    const LIST = `SELECT VALUE n FROM ${t.reminder} WHERE tk = $tk ORDER BY n`;

    let ctx: ActorRemindersContext | null = null;
    let cancel: (() => void) | null = null;
    let ticking = false;
    let stopped = false;

    /** Committed the moment it returns — BEFORE any delivery. */
    const claim = async (shards: readonly string[]): Promise<DueRow[]> => {
        for (let attempt = 1; ; attempt++) {
            try {
                const result = await db.query<unknown[]>(CLAIM, { shards, n: batchSize });
                return (result[DUE_ROW] ?? []) as DueRow[];
            } catch (error) {
                const message = String((error as { message?: unknown } | null)?.message ?? error);
                if (attempt >= CLAIM_ATTEMPTS || !ABORTED.test(message)) throw error;
            }
        }
    };

    const tick = async (): Promise<void> => {
        const context = ctx;
        if (!context || ticking || stopped) return;
        ticking = true;
        try {
            const shards: string[] = [];
            for (const shard of ALL_SHARDS) {
                if (await context.ownsShard(shard)) shards.push(shard);
            }
            // Owning nothing is normal mid-rebalance — another host is
            // ticking these rows.
            if (shards.length === 0) return;
            for (let batch = 0; batch < MAX_BATCHES_PER_TICK; batch++) {
                const due = await claim(shards);
                if (due.length === 0) return;
                // The claims above are already durable; deliveries are
                // isolated so one failing actor cannot starve the rest.
                await Promise.allSettled(
                    due.map((row) => context.deliver({ type: row.t, key: row.k }, row.n))
                );
                if (due.length < batchSize) return;
            }
        } finally {
            ticking = false;
        }
    };

    return {
        bind(context) {
            // Fail fast, like the default provider: a reused instance would
            // keep one host's tick loop running while apiFor() writes for
            // another — one provider per host, always.
            if (ctx) {
                throw new Error(
                    '[sigx actors-surreal] surrealReminders is already bound — create ' +
                        'one provider per host.'
                );
            }
            ctx = context;
        },
        start() {
            const context = ctx;
            if (!context) {
                throw new Error('[sigx actors-surreal] surrealReminders.start() before bind().');
            }
            if (cancel) return; // idempotent — one interval, ever
            stopped = false;
            cancel = context.scheduler.every(context.tickMs, () =>
                // A failed claim (the database blinked) must neither surface
                // as an unhandled rejection nor stop the loop — the next tick
                // simply tries again.
                void tick().catch((error: unknown) => {
                    if (__DEV__) {
                        console.warn('[sigx actors-surreal] reminder tick failed:', error);
                    }
                })
            );
        },
        stop() {
            stopped = true;
            cancel?.();
            cancel = null;
        },
        apiFor(ref): ReminderApi {
            const actorId = `${ref.type}\u0000${ref.key}`;
            const shard = shardOf(actorId);
            const rid = (name: string): RecordId =>
                new RecordId(t.reminder, [ref.type, ref.key, name]);
            return {
                async set(name, opts) {
                    if (opts.period !== undefined && opts.period < MIN_PERIOD_MS) {
                        throw new Error(
                            `[sigx actors-surreal] reminder "${name}" period ${opts.period}ms ` +
                                `is under the ${MIN_PERIOD_MS}ms floor — use ctx.timer for ` +
                                `tighter cadences.`
                        );
                    }
                    await db.query(SET, {
                        id: rid(name),
                        t: ref.type,
                        k: ref.key,
                        n: name,
                        tk: actorId,
                        sh: shard,
                        due: Math.max(0, opts.due),
                        // 0 means one-shot. A JS `null` would encode as
                        // SurrealDB's NULL, which is a distinct value from
                        // NONE and would need its own comparison everywhere;
                        // an integer sentinel keeps `p > 0` / `p = 0` total.
                        p: opts.period ?? 0
                    });
                },
                async clear(name) {
                    await db.query(CLEAR, { id: rid(name) });
                },
                async list() {
                    const [names] = await db.query<[string[]]>(LIST, { tk: actorId });
                    return names;
                }
            };
        }
    };
}
