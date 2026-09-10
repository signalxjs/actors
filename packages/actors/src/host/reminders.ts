/**
 * Durable reminders — sharded across the cluster.
 *
 * The reminder table rides `ActorStorage` under a reserved type, split into
 * 16 fixed shard records (`p0`..`p15`, shard = FNV-1a(actorId) — see
 * `reminder-shards.ts`, compat-critical): no second storage interface, no
 * `list()` requirement on providers. Each record is
 * `{ [actorId]: { [name]: { nextDue, period } } }`. The interface
 * deliberately promises no more than "fires at or after `nextDue`".
 *
 * A host ticks only the shards it OWNS (`ownsShard`, from the placement —
 * a cluster answers via rendezvous hashing over the membership view;
 * single-node owns everything). No lease is needed: even when two hosts
 * transiently both believe they own a shard, the per-record etag CAS makes
 * firing at-most-once — the losing ticker reloads an already-advanced
 * table and finds nothing due.
 *
 * Periodic reminders persist `nextDue += period` BEFORE dispatch: at most
 * one firing per tick, and a crash between persist and dispatch skips one
 * firing rather than double-firing (documented).
 *
 * A dispatch that FAILS is not a firing, though (#306). Under overload the
 * dispatch is exactly what fails — a deadline, a host mid-restart — and the
 * entry it belonged to was already advanced or deleted, so the wake was
 * simply gone. A rejected `deliver()` therefore re-arms its entry one tick
 * out (`nextDue = now + tickMs`; a one-shot is re-inserted, a periodic one
 * pulled forward) and is reported through `context.undelivered`. That keeps
 * the at-most-once-per-tick posture — a target that never answers costs one
 * attempt per tick, never a hot loop — while a failed dispatch costs a tick
 * rather than the wake. A reminder the actor SET again while its dispatch
 * was failing is left as the actor set it (a later decision wins); one it
 * CLEARED meanwhile may still be retried once — the tick had already
 * deleted the entry, so the clear left nothing for the re-arm to see, and a
 * tombstone would cost a write on every clear of an already-fired reminder.
 * The two deliberate doubles, then: that one, and a dispatch that timed out
 * AFTER `onReminder` started running, which is retried too. So `onReminder`
 * should be idempotent — which every at-least-once consumer already assumes.
 */
import { isStorageConflict } from '../errors';
import type {
    ActorReminders,
    ActorRemindersContext,
    ActorRef,
    ActorStorage,
    ReminderApi
} from '../types';
import { reminderShardKeys, reminderShardOf } from './reminder-shards';

export const REMINDER_TYPE = '$sigx:reminders';
const MIN_PERIOD_MS = 60_000;
/** Etag-conflict retries per mutation — shards have multiple writers
 *  once hosts cluster (every host mutates; the owner ticks). */
const MUTATE_ATTEMPTS = 3;

interface ReminderEntry {
    nextDue: number;
    period?: number;
}
type ReminderTable = Record<string, Record<string, ReminderEntry>>;
/** A due entry the tick collected — `advanced` is what it wrote for it. */
interface Due {
    id: string;
    ref: ActorRef;
    name: string;
    advanced: ReminderEntry | null;
}

/** The default `ActorReminders`: the sharded table described above. */
export function shardedReminders(): ActorReminders {
    return new ReminderService();
}

/** A shard record as this host last saw it — the table and the etag that CAS-guards it. */
interface ShardRecord {
    table: ReminderTable;
    etag: string | null;
}

const noop = (): void => {};

export class ReminderService implements ActorReminders {
    #context: ActorRemindersContext | null = null;
    /**
     * One writer chain PER SHARD (#441). A single host-wide chain made every
     * actor's `set`/`clear` and every shard's tick queue behind each other,
     * so one slow save on `p3` held a set on `p9` that shared nothing with
     * it. Shards are independent records with independent etags; only
     * writes to the SAME shard need an order.
     */
    #chains = new Map<string, Promise<unknown>>();
    /**
     * The table and etag of each shard as this host last loaded or wrote it
     * (#441). A `set`/`clear` applies to the cached table and CAS-saves
     * against the cached etag — one storage op instead of a load and a
     * save — and the CAS is what keeps that safe: if anyone else wrote the
     * shard meanwhile the etag is stale, the save is refused, and the edit
     * is replayed on a fresh load exactly as a conflict always was. The
     * tick never reads the cache — other hosts write into the shards this
     * host owns, and a due entry they armed must be found.
     */
    #cache = new Map<string, ShardRecord>();
    /**
     * Shards whose cached etag lost a CAS. A shard other hosts are writing
     * would otherwise pay THREE ops per set (a refused save, a reload, a
     * save) where the uncached path pays two, so once it loses it loads
     * before every write until the tick's own load re-primes it — a solo
     * host or a quiet shard costs one op per set, a contended one costs
     * what it costs today plus one refused save per tick.
     */
    #contended = new Set<string>();
    #stopTick: (() => void) | null = null;

    bind(context: ActorRemindersContext): void {
        if (this.#context) {
            // One instance per host. Re-binding would leave the tick loop
            // running against the previous host's storage and scheduler
            // while answering with the new one's — fail fast instead.
            throw new Error(
                '[sigx actors] this reminders instance is already bound to a host — ' +
                    'construct a new one per host.'
            );
        }
        this.#context = context;
    }

    get #storage(): ActorStorage {
        return this.#require().storage;
    }

    get #ownsShard(): (shard: string) => boolean | Promise<boolean> {
        const context = this.#require();
        return (shard) => context.ownsShard(shard);
    }

    #require(): ActorRemindersContext {
        if (!this.#context) {
            throw new Error('[sigx actors] reminders used before bind() — this is a host bug.');
        }
        return this.#context;
    }

    start(): void {
        if (this.#stopTick) return;
        const context = this.#require();
        this.#stopTick = context.scheduler.every(context.tickMs, () => {
            void this.#tick().catch((error) => {
                if (__DEV__) console.error('[sigx actors] reminder tick failed:', error);
            });
        });
    }

    stop(): void {
        this.#stopTick?.();
        this.#stopTick = null;
    }

    apiFor(ref: ActorRef): ReminderApi {
        const id = `${ref.type}\u0000${ref.key}`;
        const shard = reminderShardOf(id);
        return {
            set: (name, opts) => {
                if (opts.period !== undefined && opts.period < MIN_PERIOD_MS) {
                    return Promise.reject(
                        new Error(
                            `[sigx actors] reminder period must be >= ${MIN_PERIOD_MS}ms ` +
                                `(got ${opts.period}). Use ctx.timer() for tighter cadences.`
                        )
                    );
                }
                return this.#mutate(shard, (table) => {
                    (table[id] ??= {})[name] = {
                        nextDue: Date.now() + opts.due,
                        ...(opts.period !== undefined ? { period: opts.period } : {})
                    };
                    return true;
                });
            },
            clear: (name) =>
                this.#mutate(shard, (table) => {
                    const entries = table[id];
                    if (!entries || !(name in entries)) return false;
                    delete entries[name];
                    if (Object.keys(entries).length === 0) delete table[id];
                    return true;
                }),
            list: async () => {
                // Always the store's answer: a read must see what another
                // host armed, and it is never on a hot path.
                const { table } = await this.#load(shard);
                return Object.keys(table[id] ?? {});
            }
        };
    }

    /**
     * Serialize one shard's mutations behind each other. `edit` returns
     * whether it changed the table; a no-op writes nothing. `fresh` makes
     * the write start from the store rather than the cache — the tick's
     * posture, since other hosts arm reminders into the shards this host
     * owns.
     */
    #mutate(shard: string, edit: (table: ReminderTable) => boolean, fresh = false): Promise<void> {
        const work = (): Promise<void> => this.#mutateNow(shard, edit, fresh);
        const prev = this.#chains.get(shard) ?? Promise.resolve();
        const run = prev.then(work, work);
        this.#chains.set(shard, run.then(noop, noop));
        return run;
    }

    async #mutateNow(
        shard: string,
        edit: (table: ReminderTable) => boolean,
        fresh: boolean
    ): Promise<void> {
        // Reload-and-reapply on etag conflict: with N hosts over shared
        // storage a shard legitimately has concurrent writers, and every
        // edit here is expressed against the CURRENT table, so replaying it
        // on a fresh load is safe — whether the first attempt started from
        // the cache or from the store.
        for (let attempt = 1; ; attempt++) {
            const cached = fresh || this.#contended.has(shard) ? undefined : this.#cache.get(shard);
            const record = cached ?? (await this.#load(shard));
            if (fresh) this.#contended.delete(shard);
            const { table, etag } = record;
            // A no-op edit must not write. The tick loop reaches every owned
            // shard on every tick and most of them have nothing due, so
            // saving unconditionally would rewrite all 16 shard records
            // every `reminderTickMs` on a host with no reminders at all —
            // and bump an etag no reader can distinguish from a real change.
            // The edit says whether it changed anything; the table is not
            // serialized twice to find out (#441).
            if (!edit(table)) return;
            // A shard table is already JSON-native — it is stored unencoded
            // — so one stringify IS what a `saveText` store wants (#238).
            const json = JSON.stringify(table);
            try {
                const next = this.#storage.saveText
                    ? await this.#storage.saveText(REMINDER_TYPE, shard, json, etag)
                    : // `save` takes OWNERSHIP of its tree (#25), and this table
                      // stays cached for the next edit — so the store gets a
                      // copy. The parse is the price of an adapter without
                      // `saveText` (memory, file); the CAS stores all have one.
                      await this.#storage.save(REMINDER_TYPE, shard, JSON.parse(json), etag);
                this.#cache.set(shard, { table, etag: next });
                return;
            } catch (error) {
                // Whatever the failure, the table this attempt edited is no
                // longer what the store holds.
                this.#cache.delete(shard);
                if (!isStorageConflict(error) || attempt >= MUTATE_ATTEMPTS) throw error;
                // The cached etag lost: someone else writes this shard. Load
                // before every write until the tick re-primes it.
                if (cached !== undefined) this.#contended.add(shard);
            }
        }
    }

    /** The store's current record, and the cache primed with it. */
    async #load(shard: string): Promise<ShardRecord> {
        const record = await this.#storage.load(REMINDER_TYPE, shard);
        const loaded: ShardRecord = {
            table: (record?.state as ReminderTable) ?? {},
            etag: record?.etag ?? null
        };
        this.#cache.set(shard, loaded);
        return loaded;
    }

    // -----------------------------------------------------------------------

    /**
     * Every owned shard, CONCURRENTLY (#441). Ticking them one after another
     * made a tick sixteen round trips long for no reason — the shards are
     * independent records — and under a pipelining client (ioredis
     * auto-pipelines same-tick commands) sixteen concurrent loads are one
     * socket write. Ownership is resolved for all shards first so a placement
     * that answers asynchronously is asked once per shard, not in sequence.
     */
    async #tick(): Promise<void> {
        const shards = reminderShardKeys();
        const owned = await Promise.all(shards.map((shard) => this.#ownsShard(shard)));
        const results = await Promise.allSettled(
            shards.filter((_shard, i) => owned[i]).map((shard) => this.#tickShard(shard))
        );
        for (const result of results) {
            if (result.status === 'rejected') throw result.reason;
        }
    }

    async #tickShard(shard: string): Promise<void> {
        const now = Date.now();
        // `advanced` is what the tick wrote for the entry — `null` for a
        // deleted one-shot — so a failed dispatch can tell "still as I left
        // it" from "the actor has since set or cleared it" (see `#rearm`).
        const due: Due[] = [];
        let entriesInRecord = 0;
        // `fresh`: the tick reads the STORE, never this host's cache —
        // another host may have armed a reminder into this shard since.
        await this.#mutate(
            shard,
            (table) => {
                due.length = 0; // the mutation may retry after a CAS conflict
                entriesInRecord = 0;
                for (const [id, entries] of Object.entries(table)) {
                    // ONE enumeration per actor record: the scan's own snapshot
                    // is also the pre-deletion count for the gauge below and,
                    // through `remaining`, the emptiness test after it. The
                    // record this walk is longest for is exactly the outgrown
                    // one the gauge exists to name, so it must not pay three
                    // O(n) passes to say so.
                    const names = Object.entries(entries);
                    entriesInRecord += names.length;
                    const nul = id.indexOf('\u0000');
                    if (nul < 0) continue;
                    const ref: ActorRef = { type: id.slice(0, nul), key: id.slice(nul + 1) };
                    let remaining = names.length;
                    for (const [name, entry] of names) {
                        if (entry.nextDue > now) continue;
                        if (entry.period !== undefined) {
                            // Advance past `now` even after long downtime — one
                            // firing per tick, never a catch-up burst.
                            let next = entry.nextDue + entry.period;
                            if (next <= now) next = now + entry.period;
                            entry.nextDue = next;
                            due.push({ id, ref, name, advanced: { ...entry } });
                        } else {
                            delete entries[name];
                            remaining--;
                            due.push({ id, ref, name, advanced: null });
                        }
                    }
                    if (remaining === 0) delete table[id];
                }
                // Something was advanced or deleted iff something was due.
                return due.length > 0;
            },
            true
        );
        // Persisted first (above); now fire. The CAS is what keeps this
        // at-most-once per tick even if another host ticks the same shard:
        // the conflicting ticker reloads an advanced table and collects
        // nothing. A dispatch that fails is counted (#306) — logged in dev,
        // but never allowed to kill the loop — and collected, so the shard's
        // failures go back on the table in ONE write once every dispatch has
        // settled, rather than one `#mutate` per failure queued through the
        // host's single writer chain in front of every actor's own
        // `reminders.set/clear` (the overload that produces the failures is
        // exactly when that queue must stay short: the rung in #306 had 131
        // in one tick). The tick already waits for its slowest dispatch, so
        // its latency is unchanged; what moves is when a FAST failure's
        // re-arm lands — with the slowest one, at most `callTimeoutMs` later
        // — and `nextDue` is computed at write time, so that only shifts the
        // retry, never brings it inside a tick.
        const context = this.#require();
        // The size gauge (#384): counted on the scan the tick already does,
        // BEFORE this tick's deletions — what the CAS just rewrote.
        try {
            context.shardSize?.(shard, entriesInRecord);
        } catch {
            // A gauge must never fail a tick.
        }
        const failed: Due[] = [];
        await Promise.allSettled(
            due.map(async (entry) => {
                try {
                    // Awaited INSIDE the try: the context is pluggable, and a
                    // custom `deliver` that throws before it returns a
                    // promise must land here exactly like a rejection.
                    await context.deliver(entry.ref, entry.name);
                } catch (error) {
                    // Collected first — the re-arm never depends on the
                    // reporter behaving.
                    failed.push(entry);
                    if (__DEV__) {
                        console.error(
                            `[sigx actors] reminder "${entry.name}" on ` +
                                `${entry.ref.type}/${entry.ref.key} failed (retrying next tick):`,
                            error
                        );
                    }
                    try {
                        context.undelivered?.(entry.ref, entry.name, error);
                    } catch (reportError) {
                        if (__DEV__) {
                            console.error(
                                '[sigx actors] ActorRemindersContext.undelivered threw:',
                                reportError
                            );
                        }
                    }
                }
            })
        );
        if (failed.length > 0) await this.#rearm(shard, failed);
    }

    /**
     * Put the reminders whose dispatch failed this tick back on the table,
     * due one tick from now — so the retry happens on the next tick and
     * never sooner, whatever the failure was. Only where the entry is still
     * exactly as the tick left it: an actor that SET the reminder again
     * meanwhile (from another turn, or from the very `onReminder` a
     * timed-out dispatch went on to run) made a later decision, and that
     * decision wins. A one-shot the actor CLEARED meanwhile is the known
     * exception: the tick had already deleted it, so the clear was a no-op
     * on the table and absent is indistinguishable from untouched — it is
     * re-armed and delivered once more (see the module header for why).
     * A periodic one is not: its clear removed the advanced entry, and
     * nothing is pulled forward for an entry that is gone.
     */
    #rearm(shard: string, failed: readonly Due[]): Promise<void> {
        return this.#mutate(shard, (table) => {
            const nextDue = Date.now() + this.#require().tickMs;
            let changed = false;
            for (const { id, name, advanced } of failed) {
                const current = table[id]?.[name];
                if (advanced === null) {
                    // One-shot: deleted by the tick; absent means untouched.
                    if (current !== undefined) continue;
                    (table[id] ??= {})[name] = { nextDue };
                    changed = true;
                } else if (
                    current !== undefined &&
                    current.nextDue === advanced.nextDue &&
                    current.period === advanced.period
                ) {
                    // Periodic: pull the next firing forward, but never past
                    // the period the tick already scheduled.
                    if (nextDue < current.nextDue) {
                        current.nextDue = nextDue;
                        changed = true;
                    }
                }
            }
            return changed;
        }).catch((error) => {
            // Storage is down or the CAS lost three times — those wakes ARE
            // lost now, and the counter above already says so. Do not fail
            // the tick over it.
            if (__DEV__) {
                console.error(
                    `[sigx actors] could not re-arm ${failed.length} failed reminder(s) ` +
                        `on shard ${shard}:`,
                    error
                );
            }
        });
    }
}
