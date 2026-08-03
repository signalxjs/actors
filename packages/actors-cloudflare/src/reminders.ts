/**
 * `ActorReminders` over a Durable Object alarm.
 *
 * The default `shardedReminders()` keeps one table split into fixed hash
 * shards and polls it, because a host hosts many actors and needs to find
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
     * unlike actor state, which turns serialize, `onAlarm()` is
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

    /**
     * A gated read-modify-write whose failure is raised AFTER the gate has
     * closed.
     *
     * An exception escaping `blockConcurrencyWhile` RESETS the Durable
     * Object: the isolate is torn down, every other in-flight call on it
     * dies, and the caller gets a generic failure instead of whatever we
     * were trying to tell them. So an expected, diagnosable failure travels
     * back as a value and is thrown once we are outside.
     */
    const gated = async (fn: () => Promise<Error | void>): Promise<void> => {
        const failure = await gate(fn);
        if (failure) throw failure;
    };

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
                    '[sigx actors-cloudflare] these reminders are already bound to a host.'
                );
            }
            context = bound;
        },

        // Nothing to start or stop: the platform owns the schedule. Arming
        // happens on write, and firing arrives via `onAlarm()`.
        start() {},
        stop() {},

        apiFor(ref: ActorRef): ReminderApi {
            // Returns the claimed table, or the reason it cannot be claimed.
            // Deliberately NOT a throw: this runs inside the gate, and see
            // `gated` above for why an exception must not escape one.
            const claim = (table: Table): Table | Error => {
                const owner = table.owner;
                if (owner && (owner.type !== ref.type || owner.key !== ref.key)) {
                    // One DO holds one actor. A second identity here means
                    // the request was routed to the wrong object, and
                    // writing would corrupt the other actor's reminders.
                    return new Error(
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
                    return gated(async () => {
                        const table = claim(await load());
                        if (table instanceof Error) return table;
                        table.entries[name] = {
                            nextDue: now() + opts.due,
                            ...(opts.period !== undefined ? { period: opts.period } : {})
                        };
                        await save(table);
                    });
                },
                clear(name) {
                    return gated(async () => {
                        const table = claim(await load());
                        if (table instanceof Error) return table;
                        if (!(name in table.entries)) return;
                        delete table.entries[name];
                        await save(table);
                    });
                },
                async list() {
                    // Read-only, so it never takes the gate — and a throw
                    // here cannot reset anything.
                    const table = claim(await load());
                    if (table instanceof Error) throw table;
                    return Object.keys(table.entries);
                }
            };
        },

        /**
         * Three phases, and the middle one is deliberately NOT gated.
         *
         * A handler is expected to call `ctx.reminders.set()` — rescheduling
         * from inside `onReminder` is the documented pattern — and that takes
         * the gate again.
         *
         * MEASURED, not assumed (`__tests__/workers/gate.test.ts`): the real
         * `blockConcurrencyWhile` PERMITS that re-entry. An earlier version
         * of this comment claimed it deadlocked; it does not, and the fake
         * that "proved" it was modelling a non-reentrant queue the platform
         * does not implement.
         *
         * The split stays, for the reason that survives measurement: the gate
         * blocks the whole OBJECT until its callback settles, so holding it
         * across an arbitrary user callback stalls every other event on that
         * object for as long as the handler runs. Delivery is exactly the
         * part whose duration we do not control.
         */
        async onAlarm() {
            const bound = context;
            if (!bound) {
                // Returning quietly would DROP whatever was due, and the
                // platform has already cleared the alarm — so a periodic
                // reminder would never fire again. Start the host before
                // wiring alarm() to this. Raised OUTSIDE the gate: a throw
                // inside one resets the object.
                throw new Error(
                    '[sigx actors-cloudflare] onAlarm() before the host bound its reminders — ' +
                        'await the host/app start inside the Durable Object before handling alarms.'
                );
            }

            // Phase 1 — gated: take what is due and persist the advance.
            const { ref, due } = await gate(async () => {
                const table = await load();
                // Read the owner back from storage: after an eviction this
                // runs before any activation, so there is nothing in memory.
                const owner = table.owner;
                const at = now();
                const ready = Object.entries(table.entries).filter(([, e]) => e.nextDue <= at);

                // Advance BEFORE delivering, exactly as the sharded table
                // does: a crash between persist and dispatch skips one
                // firing rather than double-firing.
                for (const [name, entry] of ready) {
                    if (entry.period === undefined) {
                        delete table.entries[name];
                        continue;
                    }
                    // Advance from the SCHEDULED time, not from now, so an
                    // alarm that fires a little late does not drift the
                    // cadence — and clamp past `at` after real downtime, so
                    // we never queue a catch-up burst. Same rule as core.
                    let next = entry.nextDue + entry.period;
                    if (next <= at) next = at + entry.period;
                    table.entries[name] = { ...entry, nextDue: next };
                }
                await storage.put(TABLE_KEY, table);
                return { ref: owner, due: ready.map(([name]) => name) };
            });

            // Phase 2 — UNGATED: deliver. A handler rescheduling itself now
            // finds the gate free, which is the entire point of the split.
            if (ref) {
                for (const name of due) {
                    try {
                        await bound.deliver(ref, name);
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

            // Phase 3 — gated: re-read and re-arm. The re-read matters: a
            // delivered `onReminder` may itself have called
            // `ctx.reminders.set/clear`, persisting a newer table and arming
            // an earlier alarm. Arming from the phase-1 snapshot would
            // overwrite that with a later time.
            await gate(async () => {
                await rearm(await load(), true);
            });
        }
    };
}
