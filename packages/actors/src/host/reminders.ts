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
 * rather than the wake. The one deliberate double: a dispatch that timed out
 * AFTER `onReminder` started running is retried too, so `onReminder` should
 * be idempotent — which every at-least-once consumer already assumes.
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
        const due: { id: string; ref: ActorRef; name: string; advanced: ReminderEntry | null }[] =
            [];
        await this.#mutate(shard, (table) => {
            due.length = 0; // the mutation may retry after a CAS conflict
            for (const [id, entries] of Object.entries(table)) {
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
        // nothing. A dispatch that fails is put back for the next tick and
        // counted (#306) — logged in dev, but never allowed to kill the loop.
        const context = this.#require();
        await Promise.allSettled(
            due.map(({ id, ref, name, advanced }) =>
                context.deliver(ref, name).catch(async (error) => {
                    if (__DEV__) {
                        console.error(
                            `[sigx actors] reminder "${name}" on ${ref.type}/${ref.key} failed ` +
                                `(retrying next tick):`,
                            error
                        );
                    }
                    context.undelivered?.(ref, name, error);
                    await this.#rearm(shard, id, ref, name, advanced);
                })
            )
        );
    }

    /**
     * Put a reminder whose dispatch failed back on the table, due one tick
     * from now — so the retry happens on the next tick and never sooner,
     * whatever the failure was. Only if the entry is still exactly as this
     * tick left it: an actor that set or cleared the reminder meanwhile
     * (from another turn, or from the very `onReminder` a timed-out dispatch
     * went on to run) made a later decision, and that decision wins.
     */
    #rearm(
        shard: string,
        id: string,
        ref: ActorRef,
        name: string,
        advanced: ReminderEntry | null
    ): Promise<void> {
        const nextDue = Date.now() + this.#require().tickMs;
        return this.#mutate(shard, (table) => {
            const current = table[id]?.[name];
            if (advanced === null) {
                // One-shot: deleted by the tick; absent still means unchanged.
                if (current !== undefined) return;
                (table[id] ??= {})[name] = { nextDue };
            } else if (
                current !== undefined &&
                current.nextDue === advanced.nextDue &&
                current.period === advanced.period
            ) {
                // Periodic: pull the next firing forward, but never past the
                // period the tick already scheduled.
                current.nextDue = Math.min(current.nextDue, nextDue);
            }
        }).catch((error) => {
            // Storage is down or the CAS lost three times — the wake IS lost
            // now, and the counter above already says so. Do not fail the
            // tick over it.
            if (__DEV__) {
                console.error(
                    `[sigx actors] could not re-arm reminder "${name}" on ${ref.type}/${ref.key}:`,
                    error
                );
            }
        });
    }
}
