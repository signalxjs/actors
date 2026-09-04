/**
 * Fault injection for the workflow engine's tests (#383) — decorators over
 * the two seams a run's durability rides on, so a test can make ONE write
 * fail on purpose and assert what the engine does about it. The engine
 * itself is untouched: `perf/aks/src/workflow` is the pinned Tier-3
 * workload, and every knob in it is part of `INFRA_SHAPE`.
 *
 * `faultStorage` rejects the writes that match a rule, N times, and
 * forwards everything else. The optional methods (`saveText`,
 * `appendText`) are forwarded ONLY when the inner storage has them — their
 * presence is what routes the host onto the single-walk and O(entry)
 * paths, so a wrapper that always declared them would change what is
 * under test, and one that dropped them would too (the `countingStorage`
 * rule in `benchmarks/src/host-fixture.ts`).
 *
 * The injected error is an `ActorStorageConflict` by default, not a plain
 * `Error`: a conflict is what a host death between "record claimed" and
 * "record written" looks like from the survivor's side, and it is the
 * branded path — the losing turn rejects and the activation faults, so
 * the next call re-activates from what storage actually holds. A plain
 * error leaves the activation alive with dirty state, which is a
 * different (and rarer) failure; pass `error` to inject that one.
 */
import type { ActorReminders, ActorStorage, ReminderApi } from '@sigx/actors';
import { ActorStorageConflict } from '@sigx/actors';

export interface StorageFaultRule {
    /** Which records to fail; `(type, key)` of the write. */
    match: (type: string, key: string) => boolean;
    /** How many matching writes to reject before forwarding again. */
    times: number;
    /** What to reject with; default: a storage conflict for that record. */
    error?: (type: string, key: string) => Error;
}

export interface StorageFaults {
    storage: ActorStorage;
    /** Writes rejected so far, in order. */
    readonly rejected: { type: string; key: string; op: 'save' | 'saveText' | 'appendText' }[];
    /** Swap or clear the rule mid-test. */
    setRule(rule: StorageFaultRule | null): void;
}

export function faultStorage(inner: ActorStorage, initial: StorageFaultRule | null = null): StorageFaults {
    let rule = initial;
    const rejected: StorageFaults['rejected'] = [];
    const maybeFail = (op: 'save' | 'saveText' | 'appendText', type: string, key: string): Error | null => {
        if (!rule || rule.times <= 0 || !rule.match(type, key)) return null;
        rule.times--;
        rejected.push({ type, key, op });
        return rule.error ? rule.error(type, key) : new ActorStorageConflict(type, key);
    };
    const storage: ActorStorage = {
        load: (t, k) => inner.load(t, k),
        save: (t, k, s, e) => {
            const error = maybeFail('save', t, k);
            return error ? Promise.reject(error) : inner.save(t, k, s, e);
        },
        clear: (t, k, e) => inner.clear(t, k, e),
        ...(inner.saveText
            ? {
                  saveText: (t: string, k: string, j: string, e: string | null) => {
                      const error = maybeFail('saveText', t, k);
                      return error ? Promise.reject(error) : inner.saveText!(t, k, j, e);
                  }
              }
            : {}),
        ...(inner.appendText
            ? {
                  appendText: (t: string, k: string, j: string, e: string) => {
                      const error = maybeFail('appendText', t, k);
                      return error ? Promise.reject(error) : inner.appendText!(t, k, j, e);
                  }
              }
            : {})
    };
    return {
        storage,
        rejected,
        setRule(next) {
            rule = next;
        }
    };
}

export interface ReminderFaultRule {
    /** Which `set` calls to fail; `(actorId, name)` where actorId is `type\0key`. */
    match: (actorId: string, name: string) => boolean;
    times: number;
    error?: (actorId: string, name: string) => Error;
}

export interface ReminderFaults {
    reminders: ActorReminders;
    readonly rejected: { actorId: string; name: string }[];
    setRule(rule: ReminderFaultRule | null): void;
}

/**
 * Decorate `apiFor(ref).set` so arming a reminder can lose — what the
 * runtime's 3-attempt shard CAS looks like from the actor's side when a
 * shard has more writers than attempts. `clear` and `list`, and the tick
 * loop itself, are untouched.
 */
export function faultReminders(inner: ActorReminders, initial: ReminderFaultRule | null = null): ReminderFaults {
    let rule = initial;
    const rejected: ReminderFaults['rejected'] = [];
    const reminders: ActorReminders = {
        bind: (context) => inner.bind(context),
        start: () => inner.start(),
        stop: () => inner.stop(),
        apiFor(ref) {
            const api = inner.apiFor(ref);
            const actorId = `${ref.type}\0${ref.key}`;
            const decorated: ReminderApi = {
                set(name, opts) {
                    if (rule && rule.times > 0 && rule.match(actorId, name)) {
                        rule.times--;
                        rejected.push({ actorId, name });
                        return Promise.reject(
                            rule.error
                                ? rule.error(actorId, name)
                                : new ActorStorageConflict('$sigx:reminders', `${ref.type}/${ref.key}/${name}`)
                        );
                    }
                    return api.set(name, opts);
                },
                clear: (name) => api.clear(name),
                list: () => api.list()
            };
            return decorated;
        }
    };
    return {
        reminders,
        rejected,
        setRule(next) {
            rule = next;
        }
    };
}
