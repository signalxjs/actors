/**
 * `ActorReminders` over a Durable Object alarm.
 *
 * The default `shardedReminders()` keeps one table split into fixed hash
 * shards and polls it, because a silo hosts many actors and needs to find
 * whose reminder is due. A DO hosts exactly ONE actor, so there is nothing
 * to search and nothing to poll: the reminders live in this object's own
 * storage and the platform wakes us at the earliest due time.
 *
 * That also removes the tick-cadence floor. `shardedReminders` can only
 * promise "at or after `nextDue`, checked every `reminderTickMs`"; an alarm
 * fires at the due time.
 */
import type {
    ActorReminders,
    ActorRemindersContext,
    ActorRef,
    ReminderApi
} from '@sigx/actors';
import type { BlockConcurrencyWhile, DurableStorage } from './storage';

/** Minimum period, mirroring the core runtime's own floor. */
const MIN_PERIOD_MS = 60_000;
const TABLE_KEY = 'sigx:reminders';

interface Entry {
    nextDue: number;
    period?: number;
}

/**
 * The persisted record. The owner ref rides ALONGSIDE the entries on
 * purpose: a Durable Object is evicted from memory when idle, and the
 * alarm that wakes it runs before any actor is activated — so an
 * in-memory-only owner would be null exactly when a reminder is due, and
 * reminders would silently stop firing after the first eviction.
 */
interface Table {
    owner?: ActorRef;
    entries: Record<string, Entry>;
}

/** The alarm surface used here — narrow, so tests need no Workers runtime. */
export interface DurableAlarms {
    getAlarm(): Promise<number | null>;
    setAlarm(scheduledTime: number): Promise<void>;
    deleteAlarm(): Promise<void>;
}

export interface DurableObjectRemindersOptions {
    storage: DurableStorage;
    alarms: DurableAlarms;
    /** Clock, injectable so tests need not wait. Default `Date.now`. */
    now?: () => number;
    /**
     * `state.blockConcurrencyWhile`. Recommended HERE in particular:
     * unlike actor state, which the mailbox serializes, `onAlarm()` is
     * invoked straight from the object's `alarm()` handler — outside any
     * turn — so its read-modify-write of the table can interleave with a
     * `ctx.reminders.set/clear` from a concurrent dispatch, and one of the
     * two writes would be lost.
     */
    blockConcurrencyWhile?: BlockConcurrencyWhile;
}

export interface DurableObjectReminders extends ActorReminders {
    /**
     * Call from the Durable Object's `alarm()` handler. Fires everything
     * due, re-arms periodics, and schedules the next alarm.
     */
    onAlarm(): Promise<void>;
}

export function durableObjectReminders(
    options: DurableObjectRemindersOptions
): DurableObjectReminders {
    const { storage, alarms } = options;
    const now = options.now ?? (() => Date.now());
    const gate: BlockConcurrencyWhile = options.blockConcurrencyWhile ?? ((fn) => fn());
    let context: ActorRemindersContext | null = null;

    const load = async (): Promise<Table> =>
        (await storage.get<Table>(TABLE_KEY)) ?? { entries: {} };

    const save = async (table: Table): Promise<void> => {
        await storage.put(TABLE_KEY, table);
        await rearm(table);
    };

    /**
     * Point the alarm at the earliest due entry, or clear it.
     *
     * `consumed` says the alarm we were armed for has just fired, so any
     * value still reported is stale and the next due time must win even
     * though it is LATER. Without that, a periodic reminder re-arms into
     * the past and never fires again.
     */
    const rearm = async (table: Table, consumed = false): Promise<void> => {
        const due = Object.values(table.entries).map((entry) => entry.nextDue);
        if (due.length === 0) {
            if (!consumed && (await alarms.getAlarm()) !== null) await alarms.deleteAlarm();
            return;
        }
        const earliest = Math.min(...due);
        if (consumed) {
            await alarms.setAlarm(earliest);
            return;
        }
        const current = await alarms.getAlarm();
        // Otherwise only move it EARLIER: a reminder set for later must not
        // push back one that is already armed and sooner.
        if (current === null || earliest < current) await alarms.setAlarm(earliest);
    };

    return {
        bind(bound) {
            if (context) {
                throw new Error(
                    '[sigx actors-cloudflare] these reminders are already bound to a silo.'
                );
            }
            context = bound;
        },

        // Nothing to start or stop: the platform owns the schedule. Arming
        // happens on write, and firing arrives via `onAlarm()`.
        start() {},
        stop() {},

        apiFor(ref: ActorRef): ReminderApi {
            const claim = async (table: Table): Promise<Table> => {
                const owner = table.owner;
                if (owner && (owner.type !== ref.type || owner.key !== ref.key)) {
                    // One DO holds one actor. A second identity here means
                    // the request was routed to the wrong object, and
                    // writing would corrupt the other actor's reminders.
                    throw new Error(
                        `[sigx actors-cloudflare] this Durable Object hosts ` +
                            `${owner.type}/${owner.key}, but ${ref.type}/${ref.key} tried to use ` +
                            `its reminders — check the placement's object id.`
                    );
                }
                return owner ? table : { ...table, owner: { type: ref.type, key: ref.key } };
            };

            return {
                set(name, opts) {
                    if (opts.period !== undefined && opts.period < MIN_PERIOD_MS) {
                        throw new Error(
                            `[sigx actors-cloudflare] reminder "${name}" period ${opts.period}ms ` +
                                `is below the ${MIN_PERIOD_MS}ms floor — use ctx.timer() for ` +
                                `anything tighter.`
                        );
                    }
                    return gate(async () => {
                    const table = await claim(await load());
                    table.entries[name] = {
                        nextDue: now() + opts.due,
                        ...(opts.period !== undefined ? { period: opts.period } : {})
                    };
                    await save(table);
                    });
                },
                clear(name) {
                    return gate(async () => {
                    const table = await claim(await load());
                    if (!(name in table.entries)) return;
                    delete table.entries[name];
                    await save(table);
                    });
                },
                async list() {
                    const table = await claim(await load());
                    return Object.keys(table.entries);
                }
            };
        },

        onAlarm() {
            return gate(async () => {
            if (!context) {
                // Returning quietly would DROP whatever was due, and the
                // platform has already cleared the alarm — so a periodic
                // reminder would never fire again. Start the silo before
                // wiring alarm() to this.
                throw new Error(
                    '[sigx actors-cloudflare] onAlarm() before the silo bound its reminders — ' +
                        'await the silo/app start inside the Durable Object before handling alarms.'
                );
            }
            const table = await load();
            // Read the owner back from storage: after an eviction this runs
            // before any activation, so there is nothing in memory to use.
            const ref = table.owner;
            const at = now();
            const due = Object.entries(table.entries).filter(([, e]) => e.nextDue <= at);

            // Advance BEFORE delivering, exactly as the sharded table does:
            // a crash between persist and dispatch skips one firing rather
            // than double-firing.
            for (const [name, entry] of due) {
                if (entry.period === undefined) {
                    delete table.entries[name];
                    continue;
                }
                // Advance from the SCHEDULED time, not from now, so an alarm
                // that fires a little late does not drift the cadence — and
                // clamp past `at` after real downtime, so we never queue a
                // catch-up burst. Same rule as the core sharded table.
                let next = entry.nextDue + entry.period;
                if (next <= at) next = at + entry.period;
                table.entries[name] = { ...entry, nextDue: next };
            }
            await storage.put(TABLE_KEY, table);

            if (ref) {
                for (const [name] of due) {
                    try {
                        await context.deliver(ref, name);
                    } catch (error) {
                        if (__DEV__) {
                            console.error(
                                `[sigx actors-cloudflare] reminder "${name}" failed:`,
                                error
                            );
                        }
                    }
                }
            } else if (__DEV__ && due.length > 0) {
                console.error(
                    '[sigx actors-cloudflare] an alarm fired with reminders due but no owner ' +
                        'recorded — the table was written by an older version.'
                );
            }
            // Re-read before arming: a delivered `onReminder` may itself
            // have called `ctx.reminders.set/clear`, which persisted a newer
            // table and armed an earlier alarm. Arming from the snapshot we
            // loaded before delivery would overwrite that with a later time
            // — and a reminder rescheduled from its own handler is a normal
            // pattern, not an edge case.
            await rearm(await load(), true);
            });
        }
    };
}
