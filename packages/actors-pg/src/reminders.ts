/**
 * `pgReminders` — durable reminders on a Postgres table with a due-time
 * index, replacing the default sharded scan for deployments that already
 * run Postgres. This is the reminder-scan answer for large tables: the
 * default `shardedReminders()` loads and scans fixed shard records in
 * `ActorStorage` every tick, which is honest work at dev scale and a
 * bottleneck at "many reminders"; here a tick is one indexed query for
 * exactly the due rows.
 *
 * The claim is ONE data-modifying statement — `FOR UPDATE SKIP LOCKED`
 * selects the due rows, a CTE arm advances the periodic ones and another
 * deletes the one-shots, and the statement's atomicity IS the transaction:
 *
 * - **At-most-once per firing**: the advance/delete commits BEFORE any
 *   dispatch, so a crash between commit and dispatch skips one firing
 *   rather than double-firing — the same posture as `shardedReminders()`.
 * - **No shard ownership needed**: `SKIP LOCKED` lets every host tick the
 *   same table concurrently and claim disjoint rows, so
 *   `ActorRemindersContext.ownsShard` is deliberately ignored — row locks
 *   replace rendezvous hashing.
 * - **No catch-up bursts**: a periodic reminder advances to
 *   `GREATEST(next_due, now()) + period` — after downtime it fires once
 *   and resumes its cadence, never replaying the gap.
 * - **Database clock throughout**: `due` is applied to `now()` on the
 *   server, so host clock skew cannot fire early or late.
 *
 * Delivery failures are `allSettled` — one bad reminder cannot kill the
 * loop — and the loop is single-flight per provider instance.
 */
import pg from 'pg';
import type { ActorReminders, ActorRemindersContext, ReminderApi } from '@sigx/actors';
import { pgText, pgTextDecode, type PgQueryable } from './index';

export interface PgRemindersOptions {
    /** An existing pool (shared with the other providers). */
    pool?: PgQueryable;
    /** Or a connection string — the package constructs its own `pg.Pool`. */
    url?: string;
    /** The Postgres schema holding the tables. Default `sigx`. */
    schema?: string;
    /** Rows claimed per query. A tick keeps claiming until a batch comes
     *  back short (bounded), so this caps memory, not throughput. Default
     *  200. */
    batchSize?: number;
}

/** Same floor as the default provider: anything tighter is a timer's job. */
const MIN_PERIOD_MS = 60_000;

/** Claim batches per tick — a backlog drains fast without an unbounded
 *  loop if rows keep becoming due mid-tick. */
const MAX_BATCHES_PER_TICK = 10;

/** Same identifier rule as the other providers. */
function checkSchema(schema: string): string {
    if (!/^[a-z_][a-z0-9_]{0,62}$/.test(schema)) {
        throw new Error(
            `[sigx actors-pg] schema ${JSON.stringify(schema)} must match ` +
                `[a-z_][a-z0-9_]* (it is interpolated as an SQL identifier).`
        );
    }
    return schema;
}

export function pgReminders(options: PgRemindersOptions = {}): ActorReminders {
    const pool =
        options.pool ??
        (options.url !== undefined
            ? (new pg.Pool({ connectionString: options.url }) as PgQueryable)
            : undefined);
    if (!pool) {
        throw new Error('[sigx actors-pg] pass either `pool` (pg.Pool) or `url`.');
    }
    const s = checkSchema(options.schema ?? 'sigx');
    const batchSize = Math.max(1, options.batchSize ?? 200);

    let ctx: ActorRemindersContext | null = null;
    let cancel: (() => void) | null = null;
    let ticking = false;
    let stopped = false;

    /**
     * The whole claim in one atomic statement: select-and-lock the due
     * rows, advance the periodic ones, delete the one-shots. Committed the
     * moment it returns — BEFORE any delivery.
     */
    const claim = async (): Promise<{ type: string; key: string; name: string }[]> => {
        const result = await pool.query(
            `WITH due AS (
                 SELECT type, key, name, period_ms FROM ${s}.reminders
                 WHERE next_due <= now()
                 ORDER BY next_due
                 LIMIT $1
                 FOR UPDATE SKIP LOCKED
             ), advanced AS (
                 UPDATE ${s}.reminders r
                 SET next_due = GREATEST(r.next_due, now())
                     + make_interval(secs => d.period_ms::float8 / 1000.0)
                 FROM due d
                 WHERE r.type = d.type AND r.key = d.key AND r.name = d.name
                     AND d.period_ms IS NOT NULL
             ), removed AS (
                 DELETE FROM ${s}.reminders r
                 USING due d
                 WHERE r.type = d.type AND r.key = d.key AND r.name = d.name
                     AND d.period_ms IS NULL
             )
             SELECT type, key, name FROM due`,
            [batchSize]
        );
        return result.rows.map((row) => ({
            type: pgTextDecode(row['type'] as string),
            key: pgTextDecode(row['key'] as string),
            name: pgTextDecode(row['name'] as string)
        }));
    };

    const tick = async (): Promise<void> => {
        const context = ctx;
        if (!context || ticking || stopped) return;
        ticking = true;
        try {
            for (let batch = 0; batch < MAX_BATCHES_PER_TICK; batch++) {
                const due = await claim();
                if (due.length === 0) return;
                // The claims above are already durable; deliveries are
                // isolated so one failing actor cannot starve the rest.
                await Promise.allSettled(
                    due.map((row) =>
                        context.deliver({ type: row.type, key: row.key }, row.name)
                    )
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
                    '[sigx actors-pg] pgReminders is already bound — create one ' +
                        'provider per host.'
                );
            }
            ctx = context;
        },
        start() {
            const context = ctx;
            if (!context) {
                throw new Error('[sigx actors-pg] pgReminders.start() before bind().');
            }
            if (cancel) return; // idempotent — one interval, ever
            stopped = false;
            cancel = context.scheduler.every(context.tickMs, () =>
                // A failed claim (the database blinked) must neither surface
                // as an unhandled rejection nor stop the loop — the next
                // tick simply tries again.
                void tick().catch((error: unknown) => {
                    if (__DEV__) {
                        console.warn('[sigx actors-pg] reminder tick failed:', error);
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
            const type = pgText(ref.type);
            const key = pgText(ref.key);
            return {
                async set(name, opts) {
                    if (opts.period !== undefined && opts.period < MIN_PERIOD_MS) {
                        throw new Error(
                            `[sigx actors-pg] reminder "${name}" period ${opts.period}ms is ` +
                                `under the ${MIN_PERIOD_MS}ms floor — use ctx.timer for ` +
                                `tighter cadences.`
                        );
                    }
                    await pool.query(
                        `INSERT INTO ${s}.reminders (type, key, name, next_due, period_ms)
                         VALUES ($1, $2, $3,
                                 now() + make_interval(secs => $4::float8 / 1000.0), $5)
                         ON CONFLICT (type, key, name) DO UPDATE
                         SET next_due = EXCLUDED.next_due, period_ms = EXCLUDED.period_ms`,
                        [type, key, pgText(name), Math.max(0, opts.due), opts.period ?? null]
                    );
                },
                async clear(name) {
                    await pool.query(
                        `DELETE FROM ${s}.reminders
                         WHERE type = $1 AND key = $2 AND name = $3`,
                        [type, key, pgText(name)]
                    );
                },
                async list() {
                    const result = await pool.query(
                        `SELECT name FROM ${s}.reminders WHERE type = $1 AND key = $2
                         ORDER BY name`,
                        [type, key]
                    );
                    return result.rows.map((row) => pgTextDecode(row['name'] as string));
                }
            };
        }
    };
}
