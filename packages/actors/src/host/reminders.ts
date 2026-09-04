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

export class ReminderService implements ActorReminders {
    #context: ActorRemindersContext | null = null;
    #chain: Promise<unknown> = Promise.resolve();
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
                });
            },
            clear: (name) =>
                this.#mutate(shard, (table) => {
                    const entries = table[id];
                    if (entries) {
                        delete entries[name];
                        if (Object.keys(entries).length === 0) delete table[id];
                    }
                }),
            list: async () => {
                const { table } = await this.#load(shard);
                return Object.keys(table[id] ?? {});
            }
        };
    }

    /** Serialize every mutation through one writer chain (all shards). */
    #mutate(shard: string, edit: (table: ReminderTable) => void): Promise<void> {
        const work = (): Promise<void> => this.#mutateNow(shard, edit);
        const run = this.#chain.then(work, work);
        this.#chain = run.catch(() => {});
        return run;
    }

    async #mutateNow(shard: string, edit: (table: ReminderTable) => void): Promise<void> {
        // Reload-and-reapply on etag conflict: with N hosts over shared
        // storage a shard legitimately has concurrent writers, and every
        // edit here is expressed against the CURRENT table, so replaying it
        // on a fresh load is safe.
        for (let attempt = 1; ; attempt++) {
            const { table, etag } = await this.#load(shard);
            const before = JSON.stringify(table);
            edit(table);
            // A no-op edit must not write. The tick loop reaches every owned
            // shard on every tick and most of them have nothing due, so
            // saving unconditionally would rewrite all 16 shard records
            // every `reminderTickMs` on a host with no reminders at all —
            // and bump an etag no reader can distinguish from a real change.
            // (Serializing costs no more than the `save` it replaces.)
            const json = JSON.stringify(table);
            if (json === before) return;
            try {
                // A shard table is already JSON-native — it is stored
                // unencoded — so the string the compare just produced IS
                // what the store wants. Handing over the object instead
                // would make the adapter serialize it a third time (#238).
                if (this.#storage.saveText) {
                    await this.#storage.saveText(REMINDER_TYPE, shard, json, etag);
                } else {
                    await this.#storage.save(REMINDER_TYPE, shard, table, etag);
                }
                return;
            } catch (error) {
                if (!isStorageConflict(error) || attempt >= MUTATE_ATTEMPTS) throw error;
            }
        }
    }

    async #load(shard: string): Promise<{ table: ReminderTable; etag: string | null }> {
        const record = await this.#storage.load(REMINDER_TYPE, shard);
        return {
            table: (record?.state as ReminderTable) ?? {},
            etag: record?.etag ?? null
        };
    }

    // -----------------------------------------------------------------------

    async #tick(): Promise<void> {
        for (const shard of reminderShardKeys()) {
            if (!(await this.#ownsShard(shard))) continue;
            await this.#tickShard(shard);
        }
    }

    async #tickShard(shard: string): Promise<void> {
        const now = Date.now();
        // `advanced` is what the tick wrote for the entry — `null` for a
        // deleted one-shot — so a failed dispatch can tell "still as I left
        // it" from "the actor has since set or cleared it" (see `#rearm`).
        const due: Due[] = [];
        let entriesInRecord = 0;
        await this.#mutate(shard, (table) => {
            due.length = 0; // the mutation may retry after a CAS conflict
            entriesInRecord = 0;
            for (const [id, entries] of Object.entries(table)) {
                entriesInRecord += Object.keys(entries).length;
                const nul = id.indexOf('\u0000');
                if (nul < 0) continue;
                const ref: ActorRef = { type: id.slice(0, nul), key: id.slice(nul + 1) };
                for (const [name, entry] of Object.entries(entries)) {
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
                        due.push({ id, ref, name, advanced: null });
                    }
                }
                if (Object.keys(entries).length === 0) delete table[id];
            }
        });
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
            for (const { id, name, advanced } of failed) {
                const current = table[id]?.[name];
                if (advanced === null) {
                    // One-shot: deleted by the tick; absent means untouched.
                    if (current !== undefined) continue;
                    (table[id] ??= {})[name] = { nextDue };
                } else if (
                    current !== undefined &&
                    current.nextDue === advanced.nextDue &&
                    current.period === advanced.period
                ) {
                    // Periodic: pull the next firing forward, but never past
                    // the period the tick already scheduled.
                    current.nextDue = Math.min(current.nextDue, nextDue);
                }
            }
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
