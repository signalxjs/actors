/**
 * Host setup for benchmarks.
 *
 * Two decisions worth stating, because they change the numbers:
 *
 * 1. `manualScheduler()` by default. The sweeper and the reminder tick are
 *    real background work; leaving them on the wall clock means a scenario
 *    occasionally eats a 100k-activation directory scan mid-measurement and
 *    reports it as latency. Scenarios that MEASURE that work drive the
 *    manual clock explicitly instead.
 *
 * 3. A server app is stamped, by importing `./app` for its side effect.
 *    The runtime is fail-closed since core 0.15: with none, every actor
 *    that is not `allowAnonymous` answers 401 before dispatching, and a
 *    scenario that throws fails the Bench step outright. It is also the
 *    more honest measurement — a real deployment has an app, so the
 *    numbers keep the framework's per-request pipeline cost rather than
 *    measuring a shape nobody ships.
 *
 * 2. `callTimeoutMs` is a parameter, not a constant. The production default
 *    is 30s, and a non-zero deadline routes every dispatch through the
 *    call-deadline registry (`CallDeadlines`). Benchmarking only the `0`
 *    case would flatter the runtime; benchmarking only the default would
 *    hide what the deadline costs. So the dispatch scenarios run both.
 */
import './app.ts';
import { ActorStorageConflict } from '@sigx/actors';
import { createHost, manualScheduler, memoryStorage } from '@sigx/actors/host';
import type {
    ActorCallContext,
    ActorRef,
    ActorStorage,
    ManualScheduler,
    Host,
    HostDefaults
} from '@sigx/actors/host';
import type { AnyActorDefinition } from '@sigx/actors';

/** Production default; a per-call deadline race is paid on every dispatch. */
export const PRODUCTION_CALL_TIMEOUT_MS = 30_000;

export interface HostFixture {
    host: Host;
    storage: ActorStorage;
    clock: ManualScheduler;
    stop(): Promise<void>;
}

export interface HostFixtureOptions {
    actors: readonly AnyActorDefinition[];
    storage?: ActorStorage;
    /** Defaults to 0 — no deadline machinery on the measured path. */
    callTimeoutMs?: number;
    defaults?: HostDefaults;
}

export async function createBenchHost(options: HostFixtureOptions): Promise<HostFixture> {
    const clock = manualScheduler();
    const storage = options.storage ?? memoryStorage();
    const host = createHost({
        actors: [...options.actors],
        storage,
        scheduler: clock,
        defaults: {
            // Long enough that nothing collects mid-run; the sweeper scenario
            // calls the sweep path directly rather than waiting for it.
            idleAfterMs: 3_600_000,
            sweepIntervalMs: 3_600_000,
            reminderTickMs: 3_600_000,
            callTimeoutMs: options.callTimeoutMs ?? 0,
            devSerializeChecks: false,
            ...options.defaults
        }
    });
    await host.start();
    return {
        host,
        storage,
        clock,
        stop: () => host.stop({ timeoutMs: 5_000 })
    };
}

/**
 * `memoryStorage` semantics plus the one cost it deliberately skips: a full
 * `JSON.stringify(state)` on every save. That second walk is what every
 * real adapter pays on top of the host's `encodeWithHandlers` — pg
 * (`storage.ts:70`), redis (`storage.ts:88`), surreal (`storage.ts:67`) and
 * `fileStorage` (`file-storage.ts:71`) all stringify the encoded tree — so
 * the delta between this and `memoryStorage`, everything else identical, is
 * the adapter's serialize share and nothing else.
 *
 * Stringify-inside-save also satisfies the seam's ownership contract
 * (#25) trivially: the record keeps no reference to the caller's tree.
 *
 * Left EXACTLY as it was by #238, deliberately: it is the "before" arm, and
 * an arm that moves with the fix stops being a control. `textStorage()`
 * below is the "after".
 */
export function stringifyStorage(): ActorStorage {
    const records = new Map<string, { json: string; etag: string }>();
    let counter = 0;
    const id = (type: string, key: string) => `${type}\u0000${key}`;

    return {
        async load(type, key) {
            const record = records.get(id(type, key));
            return record ? { state: JSON.parse(record.json), etag: record.etag } : null;
        },
        async save(type, key, state, expectedEtag) {
            const existing = records.get(id(type, key));
            if ((existing?.etag ?? null) !== expectedEtag) {
                throw new ActorStorageConflict(type, key);
            }
            const etag = String(++counter);
            records.set(id(type, key), { json: JSON.stringify(state), etag });
            return etag;
        },
        async clear(type, key, expectedEtag) {
            const existing = records.get(id(type, key));
            if (!existing && expectedEtag === null) return;
            if ((existing?.etag ?? null) !== expectedEtag) {
                throw new ActorStorageConflict(type, key);
            }
            records.delete(id(type, key));
        }
    };
}

/**
 * `stringifyStorage` with the walk removed — the same store, opted into
 * `saveText` (#238), so the host emits the JSON in ONE pass instead of
 * building a tree this would then re-walk. Byte-identical records: it holds
 * the same strings, reached by a different route.
 *
 * The pairing is the measurement. `stringify` and `text` differ in exactly
 * one thing, so `stringify − text` IS the adapter's serialize share, and
 * `text` against `mem` says how much of it is left. `save` still stringifies
 * because the seam requires it to work either way — it is simply never the
 * path the host takes here.
 */
export function textStorage(): ActorStorage {
    // Keyed exactly as `stringifyStorage` keys — the two arms must differ
    // in the walk and in nothing else.
    const records = new Map<string, { json: string; etag: string }>();
    let counter = 0;
    const id = (type: string, key: string) => `${type}\u0000${key}`;

    const put = async (
        type: string,
        key: string,
        json: string,
        expectedEtag: string | null
    ): Promise<string> => {
        const existing = records.get(id(type, key));
        if ((existing?.etag ?? null) !== expectedEtag) {
            throw new ActorStorageConflict(type, key);
        }
        const etag = String(++counter);
        records.set(id(type, key), { json, etag });
        return etag;
    };

    return {
        async load(type, key) {
            const record = records.get(id(type, key));
            return record ? { state: JSON.parse(record.json), etag: record.etag } : null;
        },
        save: (type, key, state, expectedEtag) =>
            put(type, key, JSON.stringify(state), expectedEtag),
        saveText: put,
        async clear(type, key, expectedEtag) {
            const existing = records.get(id(type, key));
            if (!existing && expectedEtag === null) return;
            if ((existing?.etag ?? null) !== expectedEtag) {
                throw new ActorStorageConflict(type, key);
            }
            records.delete(id(type, key));
        }
    };
}

/**
 * A call context identical to the one the wire endpoint mints, minus the
 * per-call `mintCallId()`. Reused across a whole loop on purpose: the raw
 * dispatch scenario is measuring the dispatcher, and allocating a fresh
 * context per call would fold the caller's cost into the callee's number.
 * `dispatch/via-proxy` is where that cost gets priced.
 */
export function benchCall(overrides: Partial<ActorCallContext> = {}): ActorCallContext {
    return { callChain: [], callId: 'bench', ...overrides };
}

/**
 * `Host.dispatchStream` is optional on the public type — a dispatch
 * middleware that returns a bare `{ dispatch }` silently drops it, which is
 * a real failure mode the runtime has a dev warning for. A non-null
 * assertion here would surface that as `undefined is not a function`
 * somewhere inside a benchmark; this says what actually happened.
 */
export function requireStreamDispatch(host: Host): NonNullable<Host['dispatchStream']> {
    if (!host.dispatchStream) {
        throw new Error(
            'host.dispatchStream is missing — a dispatch middleware is not forwarding it, ' +
                'so no `streams:` method can run.'
        );
    }
    return host.dispatchStream.bind(host);
}

export function refsFor(type: string, count: number, prefix = 'k'): ActorRef[] {
    return Array.from({ length: count }, (_, i) => ({ type, key: `${prefix}${i}` }));
}

/** Activate `refs` so a measurement starts against warm activations. */
export async function warmActivations(
    host: Host,
    refs: readonly ActorRef[],
    method = 'noop'
): Promise<void> {
    const call = benchCall();
    // Sequential: 100k concurrent activations would measure the activation
    // storm rather than warming for the thing we actually want to measure.
    for (const ref of refs) {
        await host.dispatch(ref, method, [], call);
    }
}

/** Per-operation counts of everything the host asked a storage for. */
export interface StorageCounts {
    loads: number;
    saves: number;
    clears: number;
    /**
     * The LAST write to a reminder shard (`$sigx:reminders`): how many actor
     * entries the table carried and how many bytes went to the store. The
     * shard table is rewritten WHOLE on every `reminders.set`/`clear`, so
     * `entries` is the O(running-jobs) term a job start pays (#307).
     */
    lastReminderWrite: { entries: number; bytes: number } | null;
}

const REMINDER_TYPE = '$sigx:reminders';

/**
 * Wrap a storage so every call is counted — the same idea as
 * `cluster-harness.ts`'s counted providers, for the persistence seam. The
 * counts are invariants under serial dispatch on one host: the runtime asks
 * the store for exactly the same things on any machine, so a scenario may
 * report them `exact` (#307 pins the job lifecycle with them).
 *
 * `saveText` is forwarded ONLY when the inner storage has one — its presence
 * is what routes the host onto the single-walk path, so a wrapper that
 * always declared it would change what is being measured.
 */
export function countingStorage(inner: ActorStorage): { storage: ActorStorage; counts: StorageCounts } {
    const counts: StorageCounts = { loads: 0, saves: 0, clears: 0, lastReminderWrite: null };
    const noteReminder = (type: string, json: string): void => {
        if (type !== REMINDER_TYPE) return;
        counts.lastReminderWrite = {
            entries: Object.keys(JSON.parse(json) as object).length,
            bytes: Buffer.byteLength(json)
        };
    };
    const storage: ActorStorage = {
        load(type, key) {
            counts.loads++;
            return inner.load(type, key);
        },
        save(type, key, state, expectedEtag) {
            counts.saves++;
            // Stringified only for the shard, which is JSON-native already;
            // actor state never takes this branch.
            if (type === REMINDER_TYPE) noteReminder(type, JSON.stringify(state));
            return inner.save(type, key, state, expectedEtag);
        },
        clear(type, key, expectedEtag) {
            counts.clears++;
            return inner.clear(type, key, expectedEtag);
        }
    };
    if (inner.saveText) {
        const saveText = inner.saveText.bind(inner);
        storage.saveText = (type, key, json, expectedEtag) => {
            counts.saves++;
            noteReminder(type, json);
            return saveText(type, key, json, expectedEtag);
        };
    }
    return { storage, counts };
}
