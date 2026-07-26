/**
 * Durable reminders — the single-node reminder service.
 *
 * The reminder table rides `ActorStorage` under a reserved type as ONE
 * record (`{ [actorId]: { [name]: { nextDue, period } } }`): no second
 * storage interface, no `list()` requirement on providers, and a future
 * Durable Objects backend maps it to its single-alarm API by computing
 * `min(nextDue)`. The interface deliberately promises no more than "fires
 * at or after `nextDue`".
 *
 * Mutations are serialized through an in-process chain (one silo = one
 * writer), and the etag still travels so a second process touching the
 * table is detected rather than silently clobbered.
 *
 * Periodic reminders persist `nextDue += period` BEFORE dispatch: at most
 * one firing per tick, and a crash between persist and dispatch skips one
 * firing rather than double-firing (documented).
 */
import type { ActorRef, ActorStorage, ReminderApi } from '../types';

export const REMINDER_TYPE = '$sigx:reminders';
const REMINDER_KEY = '$all';
const MIN_PERIOD_MS = 60_000;

interface ReminderEntry {
    nextDue: number;
    period?: number;
}
type ReminderTable = Record<string, Record<string, ReminderEntry>>;

export class ReminderService {
    #storage: ActorStorage;
    #fire: (ref: ActorRef, name: string) => Promise<unknown>;
    #chain: Promise<unknown> = Promise.resolve();
    #timer: ReturnType<typeof setInterval> | null = null;

    constructor(storage: ActorStorage, fire: (ref: ActorRef, name: string) => Promise<unknown>) {
        this.#storage = storage;
        this.#fire = fire;
    }

    start(tickMs: number): void {
        if (this.#timer) return;
        this.#timer = setInterval(() => {
            void this.#tick().catch((error) => {
                if (__DEV__) console.error('[sigx actors] reminder tick failed:', error);
            });
        }, tickMs);
        (this.#timer as { unref?: () => void }).unref?.();
    }

    stop(): void {
        if (this.#timer) clearInterval(this.#timer);
        this.#timer = null;
    }

    apiFor(ref: ActorRef): ReminderApi {
        const id = `${ref.type}\u0000${ref.key}`;
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
                return this.#mutate((table) => {
                    (table[id] ??= {})[name] = {
                        nextDue: Date.now() + opts.due,
                        ...(opts.period !== undefined ? { period: opts.period } : {})
                    };
                });
            },
            clear: (name) =>
                this.#mutate((table) => {
                    const entries = table[id];
                    if (entries) {
                        delete entries[name];
                        if (Object.keys(entries).length === 0) delete table[id];
                    }
                }),
            list: async () => {
                const { table } = await this.#load();
                return Object.keys(table[id] ?? {});
            }
        };
    }

    /** Serialize every table mutation through one writer chain. */
    #mutate(edit: (table: ReminderTable) => void): Promise<void> {
        const run = this.#chain.then(
            () => this.#mutateNow(edit),
            () => this.#mutateNow(edit)
        );
        this.#chain = run.catch(() => {});
        return run;
    }

    async #mutateNow(edit: (table: ReminderTable) => void): Promise<void> {
        const { table, etag } = await this.#load();
        edit(table);
        await this.#storage.save(REMINDER_TYPE, REMINDER_KEY, table, etag);
    }

    async #load(): Promise<{ table: ReminderTable; etag: string | null }> {
        const record = await this.#storage.load(REMINDER_TYPE, REMINDER_KEY);
        return {
            table: (record?.state as ReminderTable) ?? {},
            etag: record?.etag ?? null
        };
    }

    async #tick(): Promise<void> {
        const now = Date.now();
        const due: { ref: ActorRef; name: string }[] = [];
        await this.#mutate((table) => {
            due.length = 0; // the mutation may retry after a prior chain link
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
        // Persisted first (above); now fire. Failures are the actor's to
        // log — a reminder dispatch error must not kill the loop.
        await Promise.allSettled(
            due.map(({ ref, name }) =>
                this.#fire(ref, name).catch((error) => {
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
