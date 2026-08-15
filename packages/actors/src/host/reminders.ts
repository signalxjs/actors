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
        const due: { ref: ActorRef; name: string }[] = [];
        await this.#mutate(shard, (table) => {
            due.length = 0; // the mutation may retry after a CAS conflict
            for (const [id, entries] of Object.entries(table)) {
                const nul = id.indexOf('\u0000');
                if (nul < 0) continue;
                const ref: ActorRef = { type: id.slice(0, nul), key: id.slice(nul + 1) };
                for (const [name, entry] of Object.entries(entries)) {
                    if (entry.nextDue > now) continue;
                    due.push({ ref, name });
                    if (entry.period !== undefined) {
                        // Advance past `now` even after long downtime — one
                        // firing per tick, never a catch-up burst.
                        let next = entry.nextDue + entry.period;
                        if (next <= now) next = now + entry.period;
                        entry.nextDue = next;
                    } else {
                        delete entries[name];
                    }
                }
                if (Object.keys(entries).length === 0) delete table[id];
            }
        });
        // Persisted first (above); now fire. The CAS is what keeps this
        // at-most-once even if another host ticks the same shard: the
        // conflicting ticker reloads an advanced table and collects nothing.
        // Failures are the actor's to log — a reminder dispatch error must
        // not kill the loop.
        await Promise.allSettled(
            due.map(({ ref, name }) =>
                this.#require().deliver(ref, name).catch((error) => {
                    if (__DEV__) {
                        console.error(
                            `[sigx actors] reminder "${name}" on ${ref.type}/${ref.key} failed:`,
                            error
                        );
                    }
                })
            )
        );
    }
}
