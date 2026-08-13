/**
 * One live activation: state signal, method tables, turns, timers, and the
 * change feed. Created lazily by the local host on first dispatch; nothing
 * outside this file touches activation memory.
 */
import { effect, effectScope, signal, toRaw } from '@sigx/reactivity';
import { deepTrack } from '@sigx/reactivity/internals';
import {
    createSharedWatch,
    qualifyWatchKey,
    validateWatchDeclarations,
    watchKey,
    type SharedWatch
} from './watch';
import { createWatchReadPump, type WatchReadPump } from './watch-pump';
import { mintCallId } from '../call-id';
import { EMPTY_CALL_BAG } from '../call-bag-core';
import { decodePrincipal } from '../guards';
import { ownFn, warnIfInheritedTable } from '../own-member';
import type { Turns } from './turns';
import {
    ActorActivationError,
    ActorMethodNotFoundError,
    ActorStateConflictError,
    ActorWatchDeclarationError,
    HostShutdownError,
    isStorageConflict
} from '../errors';
import {
    actorId,
    actorLabel,
    type ActorScheduler,
    type ActorCallContext,
    type ActorClientWith,
    type ActorContext,
    type ActorRef,
    type ActorTurnObserver,
    type AnyActorDefinition,
    type DeactivationReason,
    type ReminderApi,
    type TaskApi,
    type TaskInfo,
    type TimerHandle,
    type TimerOptions,
    type Topic,
    type TopicEvent,
    type TopicPublishReport
} from '../types';
import {
    TASK_REMINDER,
    TASK_REMINDER_MS,
    type TaskLedgerApi,
    type TaskLedgerEntry
} from './tasks';
import { TOPIC_METHOD, subscriptionFor, subscriptionHandler } from './topics';
import {
    declaresInterleaving,
    loadCallStore,
    validateReentrancy,
    type CallStore
} from './reentrancy';

/** Reserved dispatch method routing to `onReminder`. */
export const REMINDER_METHOD = '$sigx:reminder';

/** Change-feed buffer bound — drop-oldest beyond this. */
const CHANGE_BUFFER = 16;

/** Decoded-principal memo bound — insertion-order eviction beyond this. */
const PRINCIPAL_MEMO_CAP = 256;

// Lives in watch-core so the cluster's watch coalescing can normalize
// throttles identically without importing the activation (#111);
// re-exported here because this is its historical home.
import { DEFAULT_WATCH_THROTTLE_MS, declaresPrincipalIndependent } from '../watch-core';
export { DEFAULT_WATCH_THROTTLE_MS };

/**
 * Marks a shared watch's invoke context with the watch's UNQUALIFIED key,
 * so the `ctx.principal` getter can record that the read consulted
 * identity — the discovery that splits that key's loop per principal
 * (#121). A symbol so no user code can collide with or observe it.
 */
const kWatchBase = Symbol('sigx.watch.base');

/**
 * Present when the watched method DECLARED `principalIndependent` (#138).
 *
 * The getter records the violation here and throws; the invoke wrapper
 * rethrows it after the await. Both are needed — a read body with a broad
 * `try/catch` around its `ctx.principal` access would otherwise swallow the
 * throw and return a value to a population the relay has already merged.
 */
const kWatchDeclared = Symbol('sigx.watch.declared');

interface WatchInvokeCall extends ActorCallContext {
    [kWatchBase]?: string;
    [kWatchDeclared]?: { method: string; violated: Error | null };
}

/** One shared watch loop plus its live subscriber handles — the set the
 * discovery sweep walks to evict mismatched principals (#121). */
interface WatchEntry {
    shared: SharedWatch;
    /** Live subscribers — a discovery sweep evicts the mismatched. */
    handles: Set<WatchHandle>;
}

interface WatchHandle {
    /** The encoded principal this subscriber presented. */
    principal: string | undefined;
    /** Detach from this entry and re-attach under the qualified key. */
    evict(): void;
}

/** Keys a context extension may never set — they reach the prototype. */
const UNSAFE_CONTEXT_KEYS = new Set(['__proto__', 'constructor', 'prototype']);

/** What the host provides to every activation. */
export interface ActivationHost {
    readonly idleAfterMs: number;
    readonly slowTurnMs: number;
    /**
     * How long deactivation waits for signalled tasks to settle before
     * closing the turns, ms. Bounded so a task that ignores its abort
     * signal cannot hold a deactivation hostage.
     */
    readonly taskGraceMs: number;
    /**
     * Optional turn observer. Its PRESENCE is what turns on the extra
     * timestamps — with none set the hot path is byte for byte what it was
     * before this seam existed.
     *
     * MUTABLE on purpose: the host clears it the moment the last observer
     * unsubscribes, which is what makes switching observation off at runtime
     * actually cheap. A boolean checked inside a still-registered observer
     * would keep paying for the two clock reads below — the larger half of
     * the cost — so "off" has to mean absent, not inert.
     */
    onTurn?: ActorTurnObserver;
    /** Clock for `ctx.timer` and write-behind flushes. */
    readonly scheduler: ActorScheduler;
    /**
     * `raw` is the codec-ENCODED record alongside the revived `state` — it is
     * already in hand at the call site, and `migrateState` hands it to the
     * hook so a migration can inspect on-disk tags the revive smooths over.
     */
    loadState(ref: ActorRef): Promise<{ state: object; raw: unknown; etag: string } | null>;
    saveState(ref: ActorRef, raw: object, expectedEtag: string | null): Promise<string>;
    /**
     * `saveState`'s two halves, split so `#doSave` can reuse the encoded
     * tree for the boundary snapshot BEFORE storage takes ownership of it
     * (#233). `reviveState` is the matching revive through the same codec.
     */
    encodeState(raw: object): unknown;
    storeState(ref: ActorRef, encoded: unknown, expectedEtag: string | null): Promise<string>;
    reviveState(encoded: unknown): object;
    clearStoredState(ref: ActorRef, expectedEtag: string | null): Promise<void>;
    /** Deep, detached copy through the codec vocabulary. */
    cloneState<S>(raw: S): S;
    /**
     * Call arguments in their codec-encoded form — the shape that would go
     * on the wire. `openWatch` keys shared loops off this rather than off
     * the raw values, because the codec is what makes a `bigint`, a `Date`
     * and an `undefined` representable and distinguishable at all.
     */
    encodeArgs(args: readonly unknown[]): unknown;
    reminders(ref: ActorRef): ReminderApi;
    /** The actor's task ledger — the reserved `$sigx:tasks` record. */
    tasks(ref: ActorRef): TaskLedgerApi;
    actorClient<D extends AnyActorDefinition>(
        def: D,
        key: string,
        parentCall: () => ActorCallContext | null
    ): ActorClientWith<D>;
    /**
     * The topics fan-out — `ctx.publish` delegates here. `call` carries the
     * publishing turn's chain (so a subscription cycling back is a detected
     * deadlock); null means no turn was in progress and the host builds a
     * fresh external context.
     */
    publish(
        topic: Topic,
        payload: unknown,
        call: ActorCallContext | null,
        publisher: ActorRef
    ): Promise<TopicPublishReport>;
    /** A save hit a conflict: forget this activation after the current turn. */
    onFault(activation: Activation): void;
    /** `ctx.deactivate()` was requested and the queue just emptied. */
    onIdleRequest(activation: Activation): void;
    /**
     * Plugin-contributed `ctx` members, merged onto the context of every
     * activation (`defineActorApp` folds every plugin's `extendContext`
     * into one call). Built-in members are never overwritten — a collision
     * is a plugin bug and is dev-warned, not silently honoured.
     */
    extendContext?(ref: ActorRef): object | undefined;
}

/**
 * Load the state an activation starts from, running the type's
 * `migrateState` hook when — and only when — storage HAD a record. A shape
 * that has never been anywhere cannot be out of date, which is why the fresh
 * `state(key)` path skips the hook (and why `ctx.clearState()`, which
 * re-seeds through the same factory, skips it too).
 *
 * The migrated value is not written back here by default: `#doSave` always
 * serializes the live state, so the new shape lands on the next save the
 * actor would have made anyway. That is what keeps read paths free of writes
 * and a rolling deploy free of write amplification; the cost is that a fleet
 * may migrate the same record more than once, which the etag CAS makes safe.
 * `persist: 'eager'` buys the write back for the records that would otherwise
 * never be saved at all.
 */
async function seedFromStorage(
    ref: ActorRef,
    opts: AnyActorDefinition['__sigxActor'],
    host: ActivationHost
): Promise<{ state: object; etag: string | null }> {
    let stored = await host.loadState(ref);
    if (!stored) return { state: opts.state(ref.key) as object, etag: null };
    const spec = opts.migrateState;
    if (!spec) return { state: stored.state, etag: stored.etag };

    const migrate = typeof spec === 'function' ? spec : spec.migrate;
    const migrated = checkMigrated(ref, migrate(stored.state, { raw: stored.raw, key: ref.key }));
    // Identity IS the "nothing to migrate" signal — the documented fast path.
    if (migrated === stored.state) return { state: stored.state, etag: stored.etag };
    if (typeof spec === 'function' || spec.persist !== 'eager') {
        // Lazy: the etag must stay the one the migration was derived from, or
        // the next save becomes a blind create and clobbers a concurrent winner.
        return { state: migrated, etag: stored.etag };
    }
    try {
        return { state: migrated, etag: await host.saveState(ref, migrated, stored.etag) };
    } catch (error) {
        if (!isStorageConflict(error)) throw error;
        // A peer migrated first. That is EXPECTED rather than exceptional —
        // an eager write-back fires precisely during a rolling deploy — so
        // adopt the winner instead of failing every parked caller: reload,
        // re-run the hook (a no-op against an already-migrated record), and
        // activate on that. One extra read and no second write; retrying the
        // write would only re-enter the same race.
        stored = await host.loadState(ref);
        if (!stored) return { state: opts.state(ref.key) as object, etag: null };
        return {
            state: checkMigrated(ref, migrate(stored.state, { raw: stored.raw, key: ref.key })),
            etag: stored.etag
        };
    }
}

/**
 * Both mistakes this catches surface far from their cause if they get
 * through: `signal(undefined)` from a forgotten `return` becomes a TypeError
 * inside an unrelated method, and `signal(promise)` fails naming neither the
 * hook nor storage.
 */
function checkMigrated(ref: ActorRef, value: unknown): object {
    if (typeof value !== 'object' || value === null) {
        throw new Error(
            `[sigx actors] the \`migrateState\` hook of actor "${ref.type}" returned ` +
                `${value === null ? 'null' : typeof value} — it must return the state OBJECT ` +
                `(return the input unchanged when there is nothing to migrate).`
        );
    }
    if (typeof (value as { then?: unknown }).then === 'function') {
        throw new Error(
            `[sigx actors] the \`migrateState\` hook of actor "${ref.type}" returned a promise. ` +
                `Migration is SYNCHRONOUS — it sits between the storage read and activation, and ` +
                `must not do I/O of its own.`
        );
    }
    return value;
}

interface ChangeSub {
    queue: object[];
    wake: (() => void) | null;
    done: boolean;
    /**
     * This consumer never reads the value, so the boundary must not build
     * one for it — see `CHANGE_TICK` (#129).
     */
    ticksOnly: boolean;
    /** 0 for an unthrottled feed: emit on every boundary, as always. */
    throttleMs: number;
    /** A boundary landed inside the open window; the window owes an emit. */
    pending: boolean;
    /** Closes the open throttle window, or null when none is open. */
    cancelWindow: (() => void) | null;
}

/**
 * What a value-free subscriber receives instead of a snapshot (#129).
 *
 * `createSharedWatch`'s pump reads `const { done } = await iterator.next()`
 * and re-invokes the actor's read method — it never touches `value`. So every
 * `$live` subscription (and therefore every `useActorState(…, { live: true })`)
 * was paying a full `encode` + `revive` of the whole state per mutating turn
 * to produce an object nothing looked at.
 *
 * One frozen sentinel, shared by every such subscriber forever: the queue
 * needs *something* to distinguish "a change is waiting" from empty, and its
 * identity is never observed.
 */
const CHANGE_TICK: object = Object.freeze({});

/**
 * `ctx.changes()`'s options plus `ticksOnly`, which is NOT public: it is how
 * `openWatch` says it wants the notification and not the state.
 */
interface ChangesOptions {
    initial?: boolean;
    throttleMs?: number;
    ticksOnly?: boolean;
}

/**
 * A bad `throttleMs` is refused rather than defaulted. It reaches here from
 * actor code, so silently reading `throttleMs: '50'` as "unthrottled" would
 * hide a real mistake behind a performance cliff nobody attributes correctly.
 */
function normalizeThrottleMs(value: number | undefined, ref: ActorRef): number {
    if (value === undefined) return 0;
    if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
        throw new TypeError(
            `[sigx actors] ${actorLabel(ref)}: ctx.changes({ throttleMs }) must be a ` +
                `non-negative finite number, got ${String(value)}.`
        );
    }
    return value;
}

/**
 * The change feeds one `streams:` body opened, and whether its consumer has
 * gone. `closed` is not merely bookkeeping: a body starts lazily, so it can
 * reach its first `ctx.changes()` AFTER the disconnect has already run its
 * cleanup. A feed opened then is born closed, which is what makes teardown
 * independent of the order those two land in.
 */
interface StreamFeeds {
    subs: Set<ChangeSub>;
    closed: boolean;
}

type AnyFn = (...args: unknown[]) => unknown;
type AnyStreamFn = (...args: unknown[]) => AsyncIterable<unknown>;
type AnyTaskFn = (input: unknown) => void | Promise<void>;

/** One running detached task. */
interface TaskRun {
    name: string;
    /** Fires on cancel (`'cancelled'`) or deactivation (the reason). */
    controller: AbortController;
    /** One id for the whole run, so its `turn()`s correlate in traces. */
    callId: string;
    startedAt: number;
    /** Times the runtime re-started this run (0 on a fresh start). */
    restarts: number;
    /** Settles when the body AND its ledger bookkeeping settle; never
     *  rejects. Deactivation's grace wait covers both. */
    settled: Promise<void>;
}

export class Activation {
    readonly ref: ActorRef;
    readonly def: AnyActorDefinition;
    readonly turns: Turns;

    #host: ActivationHost;
    #scope: ReturnType<typeof effectScope>;
    #state!: object;
    #etag: string | null = null;
    #ctx!: ActorContext<object>;
    #methods!: Record<string, AnyFn>;
    #abort = new AbortController();
    #timers = new Map<string, { clear(): void }>();
    #subs = new Set<ChangeSub>();
    #version = 0;
    #notifiedVersion = 0;
    #savedVersion = 0;
    /** Set synchronously by the tracking effect's scheduler on the first
     *  write since the last fold — the one bit change detection needs. */
    #dirty = false;
    /** The effect's re-run job, parked by the scheduler; invoking it walks
     *  the state once and re-subscribes anything added since the last walk. */
    #retrack: (() => void) | null = null;
    /** Install-once latch. Disposal is NOT held here: the tracking effect
     *  registers with `#scope`, and `#scope.stop()` in `deactivate()` is
     *  what tears it down. */
    #trackingInstalled = false;
    #cancelWriteBehind: (() => void) | null = null;
    #currentCall: ActorCallContext | null = null;
    /**
     * Per-turn call context for an INTERLEAVING activation. When set it is
     * authoritative and `#currentCall` is never read: interleaved turns
     * overlap, so a single mutable field would hand one turn's chain,
     * callId and deadline to another after an `await`. Null on the default
     * (serial) path, which never pays for it. See `#callContext`.
     */
    #als: CallStore<ActorCallContext | null> | null = null;
    /** `reentrant === 'always'` — every dispatched turn interleaves. */
    #interleaveAll: boolean;
    /** Own keys of `methodReentrancy`, else null. */
    #interleaveMethods: ReadonlySet<string> | null;
    /** The one in-flight storage write (single-flight save gate). */
    #savePending: Promise<void> | null = null;
    #faulted: unknown = null;
    #faultReported = false;
    #deactivateRequested = false;
    #keepAlive = 0;
    #warnedDroppedChanges = false;
    #warnedStreamState = false;
    #warnedTaskState = false;
    /** Running detached tasks, by name (single-flight per name). */
    #tasks = new Map<string, TaskRun>();
    /** Lazily bound — one instance per activation (one writer chain). */
    #ledgerApi: TaskLedgerApi | null = null;
    /** Shared watch loops, keyed by `method` + encoded args — and, once a
     * read is observed consulting `ctx.principal`, by the encoded principal
     * too. Entry keys are IMMUTABLE: an entry is never re-keyed, so
     * `onEmpty`'s delete-by-key stays exact (see `#resolveWatch`, #121). */
    #watches = new Map<string, WatchEntry>();
    /**
     * Base watch keys whose read has been OBSERVED consulting
     * `ctx.principal`. Sticky for the activation's lifetime; a fresh
     * activation rediscovers during its first invoke, before any value
     * is pushed.
     */
    #watchPrincipalDependent = new Set<string>();
    /** The batched lane for serial watch reads (#180) — lazy: an actor
     *  nobody watches never allocates it. */
    #watchPump: WatchReadPump | null = null;
    lastActivityMs = Date.now();
    /**
     * When this activation was constructed, MONOTONIC — the age an operator
     * reads is a duration, and `lastActivityMs` is wall-clock because idle
     * collection genuinely wants wall time. One `performance.now()` per
     * activation, not per turn.
     */
    readonly startedMs = performance.now();

    /** Stateless worker-pool member: no storage, no persistence surface. */
    readonly stateless: boolean;

    private constructor(
        ref: ActorRef,
        def: AnyActorDefinition,
        host: ActivationHost,
        turns: Turns
    ) {
        this.ref = ref;
        this.def = def;
        this.#host = host;
        this.turns = turns;
        this.#scope = effectScope();
        this.stateless = def.__sigxActor.stateless !== undefined;
        const opts = def.__sigxActor;
        this.#interleaveAll = opts.reentrant === 'always';
        this.#interleaveMethods = opts.methodReentrancy
            ? new Set(Object.keys(opts.methodReentrancy))
            : null;
    }

    /**
     * Build and activate. Any throw here (storage load, factories,
     * onActivate) is wrapped in ActorActivationError; the caller (local
     * host) fails every parked dispatch with it and forgets the slot.
     */
    static async create(
        ref: ActorRef,
        def: AnyActorDefinition,
        host: ActivationHost,
        turns: Turns
    ): Promise<Activation> {
        const a = new Activation(ref, def, host, turns);
        try {
            const opts = def.__sigxActor;
            // Interleaving needs the per-turn call store. Validated and
            // loaded here — not at definition time (the root entry's size
            // gate keeps define.ts lean) — so a malformed declaration or a
            // missing AsyncLocalStorage fails the first activation loudly
            // (as ActorActivationError, per type) and the serial path never
            // imports node:async_hooks at all.
            if (opts.reentrant !== undefined || opts.methodReentrancy !== undefined) {
                validateReentrancy(ref.type, opts, def.streamNames);
            }
            if (declaresInterleaving(opts)) {
                const Store = await loadCallStore();
                a.#als = new Store<ActorCallContext | null>();
            }
            // A stateless member never reads storage: there is no record to
            // find (and no identity to find it under), so `seedFromStorage`
            // — including any `migrateState` hook — is skipped whole.
            if (a.stateless) {
                a.#etag = null;
                a.#state = signal(opts.state(ref.key) as object);
            } else {
                const seed = await seedFromStorage(ref, opts, host);
                a.#etag = seed.etag;
                a.#state = signal(seed.state);
            }
            a.#ctx = a.#buildContext();
            // The factories (and onActivate) run inside the activation's
            // effect scope so computeds/watches they create die with it.
            a.#scope.run(() => {
                a.#methods = opts.methods(a.#ctx) as Record<string, AnyFn>;
            });
            if (__DEV__) warnIfInheritedTable(a.#methods, 'methods', ref.type);
            if (__DEV__ && a.#interleaveMethods) {
                // Definition time cannot check these (the methods factory
                // needs a live ctx) — a typo'd key would silently stay serial.
                for (const name of a.#interleaveMethods) {
                    if (!Object.hasOwn(a.#methods, name)) {
                        console.warn(
                            `[sigx actors] methodReentrancy of "${ref.type}" names "${name}", ` +
                                `which is not in its methods table — the entry does nothing.`
                        );
                    }
                }
            }
            // After the methods table, unlike `validateReentrancy` above:
            // the unknown-key warning needs real method names, and a sharing
            // declaration has no dispatch consequence before the first watch
            // — so nothing is lost by checking it a few lines later, and the
            // first activation of the type still fails before any turn.
            validateWatchDeclarations(
                ref.type,
                opts,
                def.streamNames,
                __DEV__ ? Object.keys(a.#methods) : []
            );
            // The `streams:` table is built per SUBSCRIPTION, not here — see
            // `#streamTable`. Its bodies get a derived context of their own:
            // bracketing the dispatch call sites cannot work, because an async
            // generator's return() awaits before resuming, so a `finally`
            // reading ctx.state runs in a later microtask. Handing each body
            // its own context makes both the dev `state` guard and change-feed
            // ownership exact and timing-independent — and no turn can
            // trip either, since `methods` still gets the real ctx.
            if (opts.persistence && typeof opts.persistence === 'object') {
                a.#ensureChangeTracking();
            }
            if (opts.onActivate) {
                await a.#scope.run(() => opts.onActivate!(a.#ctx));
            }
            // Crash recovery: restart every ledgered run. Only actors that
            // DECLARE tasks pay the storage read; everyone else activates
            // byte for byte as before. A liveness-reminder delivery lands
            // here too — activating the actor is the delivery's real work.
            if (opts.tasks) {
                const ledger = await a.#ledger.load();
                for (const [name, entry] of Object.entries(ledger)) {
                    await a.#resumeTask(name, entry);
                }
            }
            return a;
        } catch (cause) {
            a.#scope.stop();
            throw new ActorActivationError(actorLabel(ref), { cause });
        }
    }

    get faulted(): unknown {
        return this.#faulted;
    }

    get idle(): boolean {
        return this.turns.depth === 0 && this.#keepAlive === 0;
    }

    get keptAlive(): boolean {
        return this.#keepAlive > 0;
    }

    get deactivateRequested(): boolean {
        return this.#deactivateRequested;
    }

    /** Running detached task count (observability — `ActivationInfo.tasks`). */
    get tasks(): number {
        return this.#tasks.size;
    }

    /** Shared watch loop count (observability — `ActivationInfo.watchLoops`). */
    get watchLoops(): number {
        return this.#watches.size;
    }

    /** Watch subscribers across those loops (observability —
     *  `ActivationInfo.watchSubscribers`). O(loops), paid per ops poll. */
    get watchSubscribers(): number {
        let subscribers = 0;
        for (const entry of this.#watches.values()) subscribers += entry.handles.size;
        return subscribers;
    }

    /** The identity used in call chains. */
    get id(): string {
        return actorId(this.ref);
    }

    /**
     * Lazy `ctx.principal` decode, keyed on the ENCODED string so the memo
     * stays correct across the reused ctx object and across turns carrying
     * different identities. A bounded MAP, not a single slot: P watch
     * loops for P distinct identities (#121) interleave their read turns,
     * and a one-slot memo thrashed into one decode per read turn — O(P)
     * decodes per publish, pure waste (#180). Insertion-order eviction at
     * the cap; identities churning past 256 on one activation pay a decode
     * per eviction, which is the pre-map cost, not a new one. Lazy (null
     * until the first decode): the map is per activation, and an actor
     * that never reads `ctx.principal` must not pay ~200 bytes for it —
     * `mem/per-actor-footprint` gates exactly this.
     */
    #principalMemo: Map<string, unknown> | null = null;

    /**
     * The call context of the turn asking — the ONE reader `ctx.actor()`
     * and `ctx.publish()` go through. Interleaving activations answer from
     * the per-turn store; everyone else from the single field.
     */
    #callContext(): ActorCallContext | null {
        return this.#als ? (this.#als.getStore() ?? null) : this.#currentCall;
    }

    // -----------------------------------------------------------------------
    // Dispatch surface (called by the local host only)

    /** Enqueue one turn. */
    enqueue(method: string, args: readonly unknown[], call: ActorCallContext): Promise<unknown> {
        // One clock read per queued turn, and only when an observer exists.
        // Taken HERE rather than inside the turn because the whole point is
        // the gap between the two.
        //
        // MONOTONIC, unlike `lastActivityMs`: durations must not be derived
        // from a wall clock that NTP or a VM host can step backwards, which
        // would hand observers negative queue waits. `performance.now()` is
        // immune. The wall clock is still used for idle collection, which
        // genuinely wants wall time.
        const enqueuedAt = this.#host.onTurn ? performance.now() : 0;
        // The interleave decision lives HERE, not on the dispatch path: the
        // activation owns its def and the method name, so the local host's
        // warm-path shortcut needs no edits and can never drift. Two field
        // reads on the default path. Reserved deliveries ($sigx:reminder,
        // $sigx:topic) interleave iff the whole actor does — the per-method
        // map cannot name them.
        const interleave =
            this.#interleaveAll ||
            (this.#interleaveMethods !== null && this.#interleaveMethods.has(method));
        return this.turns.run(() => this.#turn(method, args, call, enqueuedAt), interleave);
    }

    /**
     * Call-chain reentrancy: run inline against the CURRENT turn (the
     * activation's own turn is up-stack awaiting this call, so no foreign
     * interleaving can occur). Swaps the current-call context so nested
     * `ctx.actor` chains keep growing.
     */
    async runInline(
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): Promise<unknown> {
        // A 'call-chain' actor with a methodReentrancy map is an
        // interleaving activation whose UNMAPPED in-chain calls still run
        // inline — those must establish the store, not the field.
        if (this.#als) return this.#als.run(call, () => this.#invoke(method, args));
        const prev = this.#currentCall;
        this.#currentCall = call;
        try {
            return await this.#invoke(method, args);
        } finally {
            this.#currentCall = prev;
        }
    }

    /**
     * A change-driven READ: the method's result now, and again after every
     * turn that mutated state.
     *
     * Subscriptions with the same `(method, args, throttleMs)` share one
     * loop, so a popular actor costs one re-invocation per turn rather than
     * one per viewer — which matters more here than anywhere else, because
     * turns are serialized and those would queue behind each other.
     *
     * `throttleMs` is part of the identity because it is part of the
     * behaviour: subscribers asking for different windows want different
     * emission rates, and one loop cannot honour both. Two viewers of the
     * same read share whenever they agree on it — which, since the option
     * is rarely passed, is nearly always.
     *
     * Unlike `openStream`, this takes NO inline form. Its reads are ordinary
     * turns, only the first of which could ever run inline — and a subscriber
     * joining a shared loop whose initial read is already queued would
     * deadlock regardless. So an in-chain open is refused up in
     * `LocalHost.dispatchWatch` rather than re-entered (#46).
     */
    openWatch(
        method: string,
        args: readonly unknown[],
        call: ActorCallContext,
        options?: { throttleMs?: number }
    ): AsyncIterable<unknown> {
        const throttleMs = options?.throttleMs ?? DEFAULT_WATCH_THROTTLE_MS;
        // Args are part of the identity: `recent(20)` and `recent(50)` are
        // different reads. Keyed by VALUE through the codec, not by
        // reference, so two callers passing equal-but-distinct arrays still
        // share one loop — and `watchKey`'s length-prefixed grammar
        // guarantees the converse, that two DIFFERENT arg lists never can.
        const base = watchKey(method, throttleMs, this.#host.encodeArgs(args ?? []));

        // One subscription's state, shared by every asyncIterator() call on
        // the returned iterable — one subscriber per openWatch, as before.
        let entry: WatchEntry | null = null;
        let handle: WatchHandle | null = null;
        let inner: AsyncIterator<unknown> | null = null;
        let migrating = false;
        let closed = false;

        const detach = (): void => {
            if (entry !== null && handle !== null) entry.handles.delete(handle);
            entry = null;
            handle = null;
            inner = null;
        };

        const attach = (): void => {
            migrating = false;
            const joined = this.#resolveWatch(base, method, args, call, throttleMs);
            handle = {
                principal: call.principal,
                evict: () => {
                    // Discovery found this subscriber on a loop invoking
                    // under someone ELSE's principal. Leave it; the next
                    // pull re-attaches under the qualified key. `return()`
                    // drops the inner subscriber SYNCHRONOUSLY, so the value
                    // the discovering invocation is about to push skips it.
                    migrating = true;
                    const dropped = inner;
                    detach();
                    void dropped?.return?.(undefined);
                }
            };
            joined.handles.add(handle);
            entry = joined;
            // The CALLER's signal, so an abandoned subscription releases
            // even when nothing ever calls `return()` on it — which is the
            // normal case across a host hop, where the serving generator is
            // parked at an `await` and cannot act on a queued `return()`.
            inner = joined.shared.subscribe(call.abortSignal)[Symbol.asyncIterator]();
        };

        // Attach NOW, not on the first pull: the shared loop (and with it
        // the initial read) starts when the watch is opened, as it always
        // has.
        attach();

        const iterator: AsyncIterator<unknown> = {
            next: async (): Promise<IteratorResult<unknown>> => {
                for (;;) {
                    if (closed) return { value: undefined, done: true };
                    if (inner === null) attach();
                    const pulled = inner!;
                    let result: IteratorResult<unknown>;
                    try {
                        result = await pulled.next();
                    } catch (error) {
                        detach();
                        closed = true;
                        throw error;
                    }
                    if (!result.done) return result;
                    // An evicted subscriber ends with `done`; re-attach —
                    // `#resolveWatch` now routes it to the qualified key —
                    // rather than surfacing an end the caller never caused.
                    // A real end (abort, deactivation) passes through.
                    if (migrating) continue;
                    detach();
                    closed = true;
                    return { value: undefined, done: true };
                }
            },
            return: async (): Promise<IteratorResult<unknown>> => {
                closed = true;
                const dropped = inner;
                detach();
                await dropped?.return?.(undefined);
                return { value: undefined, done: true };
            }
        };
        return { [Symbol.asyncIterator]: () => iterator };
    }

    /**
     * Join — or create — the shared entry a subscriber with this principal
     * belongs on. Until a base key is known principal-dependent, everyone
     * shares the entry at the base key; from the moment the read is
     * observed consulting `ctx.principal`, joins go to the key qualified by
     * the subscriber's encoded principal instead. An entry still sitting at
     * the base key merely drains — it is never re-keyed and never joined
     * again, so `onEmpty` deleting its own creation key can never evict a
     * newer entry (the hazard watch.ts's idempotent drop exists for).
     */
    #resolveWatch(
        base: string,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext,
        throttleMs: number
    ): WatchEntry {
        const dependent = this.#watchPrincipalDependent.has(base);
        const key = dependent ? qualifyWatchKey(base, call.principal) : base;
        let entry = this.#watches.get(key);
        if (entry === undefined) {
            entry = this.#createWatchEntry(key, base, method, args, call, throttleMs, dependent);
            this.#watches.set(key, entry);
        }
        return entry;
    }

    #createWatchEntry(
        key: string,
        base: string,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext,
        throttleMs: number,
        qualified: boolean
    ): WatchEntry {
        const handles = new Set<WatchHandle>();
        // The loop invokes under the CREATING subscriber's context — its
        // bag and abortSignal included, a known quirk tracked separately
        // from #121. The marker is how the `ctx.principal` getter reports
        // that this read consulted identity.
        // Declared principal-independent (#138)? Then the getter must FAIL
        // the read rather than record discovery — by the time it fires, a
        // relay may already have merged distinct identities onto one
        // coalesced stream, and this host (which sees one subscriber per
        // stream) cannot repair that. Fail closed instead of splitting.
        const declared = declaresPrincipalIndependent(this.def.__sigxActor, method)
            ? { method, violated: null as Error | null }
            : undefined;
        const invokeCall: WatchInvokeCall = {
            ...call,
            [kWatchBase]: base,
            ...(declared ? { [kWatchDeclared]: declared } : {})
        };
        const principal = call.principal;
        // Once true, every subscriber on this entry presents `principal`:
        // born true under a qualified key, made true for a base-key entry
        // by the one discovery sweep — after which `#resolveWatch` routes
        // every mismatched newcomer to a qualified key instead.
        let settled = qualified;
        // Serial-lane reads go through the watch read pump: the whole watch
        // population holds ONE queue slot, seeds drain first, and an
        // external call waits for at most one slice instead of O(loops)
        // read turns (#180). Interleaved methods keep the direct path —
        // they never contend on the serial lane, and folding them into the
        // batch would ADD the serialization they opted out of.
        const interleave =
            this.#interleaveAll ||
            (this.#interleaveMethods !== null && this.#interleaveMethods.has(method));
        let seeded = false;
        const shared = createSharedWatch(
            {
                // A NORMAL turn, not a privileged read: the
                // watch gets exactly the isolation every other call has —
                // the pump runs `#turn` per read, so call context, the
                // observer and the change boundary stay per-read.
                invoke: async () => {
                    const seed = !seeded;
                    seeded = true;
                    try {
                        let value: unknown;
                        if (interleave) {
                            value = await this.enqueue(method, args, invokeCall);
                        } else if (this.turns.depth === 0) {
                            // Uncontended fast path: zero depth means no turn
                            // is queued OR running — so no drain turn either,
                            // and a drain turn is the only thing that can hold
                            // pump jobs, so the pump is empty too. A direct
                            // enqueue is then semantically identical (there is
                            // nothing to be fair TO) and skips the pump's
                            // per-job bookkeeping — the single-watch hot path
                            // (`streams/live-watch`) measurably cares.
                            value = await this.enqueue(method, args, invokeCall);
                        } else {
                            this.#watchPump ??= createWatchReadPump({
                                enqueueTurn: (body) => this.turns.run(body),
                                closed: () => this.turns.closed
                            });
                            const enqueuedAt = this.#host.onTurn ? performance.now() : 0;
                            value = await this.#watchPump.schedule(
                                () => this.#turn(method, args, invokeCall, enqueuedAt),
                                seed
                            );
                        }
                        // Rethrown HERE and not left to the getter alone
                        // (#138): a read body that wraps its `ctx.principal`
                        // access in a broad `try/catch` would otherwise
                        // swallow the throw and hand a value to a population
                        // the relay already merged. Never heals — `violated`
                        // outlives the invoke, so a later read of the same
                        // entry fails the same way.
                        if (declared?.violated) throw declared.violated;
                        return value;
                    } finally {
                        // Discovery sweep (#121): the read consulted the
                        // principal, and this entry may hold subscribers who
                        // presented a different one. Evict them BEFORE the
                        // loop pushes or fails — this `finally` settles
                        // before `push(await deps.invoke())` resumes — so no
                        // subscriber ever receives a value or error computed
                        // under someone else's identity. Values delivered
                        // earlier were computed by invocations that never
                        // touched the principal, or this sweep would have
                        // run then.
                        // Safe to iterate live: `evict` only DELETES from
                        // this set (the re-attach lands on a different
                        // entry, and only on the consumer's next pull).
                        if (!settled && this.#watchPrincipalDependent.has(base)) {
                            settled = true;
                            for (const h of handles) {
                                if (h.principal !== principal) h.evict();
                            }
                        }
                    }
                },
                // `ticksOnly`: the pump reads `{ done }` and re-invokes
                // the read method — it never looks at the value, so a
                // snapshot built for it is pure waste, once per mutating
                // turn per shared watch, over the whole state (#129).
                // Straight to `#openChanges` rather than through
                // `ctx.changes()` because this is not a public option.
                changes: () => this.#openChanges({ ticksOnly: true }),
                keepAlive: () => {
                    this.#keepAlive++;
                    let released = false;
                    return () => {
                        if (released) return;
                        released = true;
                        this.#keepAlive--;
                        this.lastActivityMs = Date.now();
                    };
                },
                scheduler: this.#host.scheduler,
                throttleMs
            },
            () => void this.#watches.delete(key)
        );
        return { shared, handles };
    }

    /**
     * Stream dispatch. The setup turn only RESOLVES the generator and takes
     * a keep-alive ref — it must NOT pull the first chunk: a feed like
     * `yield* ctx.changes()` waits for a future turn of this same actor,
     * and holding the activation for that pull would self-deadlock. Iteration
     * (including the first pull) is therefore fully detached, and stream
     * bodies get no turn exclusivity by contract — they are observers,
     * reading `ctx.snapshot()` / `ctx.changes()`, never live state. The
     * keep-alive ref makes idle collection skip the activation until the
     * stream ends or the consumer disconnects.
     *
     * `inline` skips the setup turn entirely, for a call-chain-reentrant open
     * from inside this activation's own chain — see the comment on `setup`.
     */
    openStream(
        method: string,
        args: readonly unknown[],
        call: ActorCallContext,
        inline = false
    ): AsyncIterable<unknown> {
        // Every `ctx.changes()` this body opens, so a disconnect can close
        // them from OUTSIDE the generator — see `#streamContext`.
        const feeds: StreamFeeds = { subs: new Set(), closed: false };
        const resolveGen = async (): Promise<AsyncIterator<unknown>> => {
            if (this.#faulted) throw this.#faulted;
            const fn = ownFn<AnyStreamFn>(this.#streamTable(feeds), method);
            if (!fn) throw new ActorMethodNotFoundError(this.ref.type, method);
            const prev = this.#currentCall;
            this.#currentCall = call;
            this.#keepAlive++;
            try {
                // Async generator bodies are lazy: this runs no user code.
                return this.#als
                    ? this.#als.run(call, () => fn(...(args as unknown[]))[Symbol.asyncIterator]())
                    : fn(...(args as unknown[]))[Symbol.asyncIterator]();
            } catch (error) {
                this.#keepAlive--;
                throw error;
            } finally {
                this.#currentCall = prev;
                // Only a real turn has a boundary. Inline, the caller's turn
                // is still open and owns the next one: flushing dirty state
                // and notifying subscribers from in here would publish a
                // half-finished turn.
                if (!inline) this.#afterTurn(Date.now());
            }
        };
        // Inline is the call-chain-reentrancy rescue (#46), and it is exact
        // rather than merely expedient: the block above sets `#currentCall`,
        // resolves the generator and restores it with NO `await` in between —
        // calling an async generator function is synchronous and runs none of
        // its body — so there is no window for another turn to interleave. On
        // the serial lane the same work would queue behind the very turn that
        // is up-stack awaiting the first chunk, and neither could proceed.
        const setup = inline ? resolveGen() : this.turns.run(resolveGen);

        let released = false;
        const release = () => {
            if (!released) {
                released = true;
                this.#keepAlive--;
                this.lastActivityMs = Date.now();
            }
        };
        // If setup itself failed (unknown method, faulted), the keep-alive
        // was already rolled back — never release twice.
        setup.catch(() => {
            released = true;
        });
        const closeOwned = (): void => {
            // Latched, so a feed the body opens later is born closed. The
            // body is lazy: it can reach its first `ctx.changes()` after this
            // has run, and a sub registered then would otherwise be missed.
            feeds.closed = true;
            this.#closeSubs(feeds.subs);
        };

        return {
            [Symbol.asyncIterator](): AsyncIterator<unknown> {
                return {
                    async next(): Promise<IteratorResult<unknown>> {
                        try {
                            const gen = await setup;
                            const result = await gen.next();
                            if (result.done) {
                                // A body that walked away from its feed
                                // without returning it would otherwise leave
                                // the subscription queueing snapshots until
                                // the activation goes.
                                closeOwned();
                                release();
                            }
                            return result;
                        } catch (error) {
                            closeOwned();
                            release();
                            throw error;
                        }
                    },
                    async return(): Promise<IteratorResult<unknown>> {
                        // FIRST, before awaiting anything. A body parked
                        // inside `changes()` is suspended at an INTERNAL
                        // await, and the spec queues `gen.return()` there:
                        // the generator is never resumed, so the feed's own
                        // `return()` — the thing that would wake it — is
                        // never called. Closing the subscription from out
                        // here wakes the parked `next()`, the body unwinds,
                        // and the queued `gen.return()` finally settles.
                        closeOwned();
                        try {
                            const gen = await setup;
                            if (gen.return) await gen.return(undefined);
                        } catch {
                            // setup failure already handled via next()
                        } finally {
                            release();
                        }
                        return { value: undefined, done: true };
                    }
                };
            }
        };
    }

    // -----------------------------------------------------------------------
    // Lifecycle (driven by the local host)

    /**
     * Graceful deactivation: signal in-flight work, give detached tasks a
     * bounded grace to wind down, then close the turns, drain queued
     * turns, run onDeactivate, flush a pending write-behind save, tear
     * down.
     *
     * The aborts come FIRST — before the drain, not after it. An abort
     * signal that fires only once the queue has drained is unobservable by
     * exactly the long-running work it exists for: that work is what the
     * drain is waiting on. Turns stay open through the task grace so
     * a winding-down task can run a final `turn()` checkpoint.
     */
    async deactivate(reason: DeactivationReason): Promise<void> {
        this.#abort.abort();
        if (this.#tasks.size > 0) {
            for (const run of this.#tasks.values()) run.controller.abort(reason);
            await this.#settleTasks();
        }
        this.turns.close();
        await this.turns.drain();
        const opts = this.def.__sigxActor;
        if (opts.onDeactivate && reason !== 'activation-failed') {
            try {
                await opts.onDeactivate(this.#ctx, reason);
            } catch (error) {
                if (__DEV__) {
                    console.error(
                        `[sigx actors] onDeactivate of ${actorLabel(this.ref)} threw (ignored):`,
                        error
                    );
                }
            }
        }
        if (this.#cancelWriteBehind) {
            this.#cancelWriteBehind();
            this.#cancelWriteBehind = null;
        }
        // Fold writes with no turn boundary behind them — an `onActivate`
        // mutation on a never-called actor, or an `onDeactivate` that just
        // amended state above — so the flush check sees them.
        this.#consumeDirty();
        // A stale activation must not overwrite the winning state.
        if (reason !== 'conflict' && this.#version > this.#savedVersion && this.#isWriteBehind()) {
            try {
                await this.#gatedSave(this.#version);
            } catch (error) {
                if (__DEV__) {
                    console.error(
                        `[sigx actors] final write-behind flush of ${actorLabel(this.ref)} failed:`,
                        error
                    );
                }
            }
        }
        for (const t of this.#timers.values()) t.clear();
        this.#timers.clear();
        this.#scope.stop();
        this.#closeSubs(this.#subs);
    }

    /** Force-drop on the shutdown deadline: abort and tear down without
     *  drain. `reason` is whatever the stop was using — `'migrated'` on a
     *  cluster handoff — so task abort reasons keep their contract. */
    forceStop(reason: DeactivationReason = 'shutdown'): void {
        this.turns.close();
        for (const t of this.#timers.values()) t.clear();
        this.#timers.clear();
        this.#abort.abort();
        for (const run of this.#tasks.values()) run.controller.abort(reason);
        this.#scope.stop();
        this.#closeSubs(this.#subs);
    }

    /**
     * Wait for every signalled task to settle, bounded by `taskGraceMs`.
     * A host timer on purpose (not the scheduler): the grace is part of a
     * stop already in flight, same as the shutdown drain deadline — and on
     * a scheduler that never fires, a signal-ignoring task would otherwise
     * hold deactivation forever.
     */
    async #settleTasks(): Promise<void> {
        const pending = [...this.#tasks.values()].map((r) => r.settled);
        let timer: ReturnType<typeof setTimeout> | undefined;
        const deadline = new Promise<'timeout'>((r) => {
            timer = setTimeout(() => r('timeout'), this.#host.taskGraceMs);
            (timer as { unref?: () => void }).unref?.();
        });
        const outcome = await Promise.race([
            Promise.all(pending).then(() => 'done' as const),
            deadline
        ]);
        clearTimeout(timer);
        if (outcome === 'timeout' && __DEV__) {
            const names = [...this.#tasks.keys()].join('", "');
            console.warn(
                `[sigx actors] task(s) "${names}" of ${actorLabel(this.ref)} ignored their ` +
                    `abort signal past the ${this.#host.taskGraceMs}ms grace — proceeding ` +
                    `with deactivation. Long-running task bodies must observe ` +
                    `ctx.abortSignal.`
            );
        }
    }

    // -----------------------------------------------------------------------
    // Internals

    async #turn(
        method: string,
        args: readonly unknown[],
        call: ActorCallContext,
        enqueuedAt = 0
    ): Promise<unknown> {
        if (this.#faulted) throw this.#faulted;
        const started = Date.now();
        // Read once: the observer may be detached mid-turn (metrics can be
        // switched off at runtime), and start/end must agree about whether
        // they are timing at all.
        const observer = this.#host.onTurn;
        const timing = __DEV__ || observer !== undefined;
        const startedAt = timing ? performance.now() : 0;
        this.#currentCall = call;
        let failed = true;
        try {
            // On an interleaving activation the invoke runs under the call
            // store; the serial path is byte-for-byte what it was (one
            // null-check, no extra promise hop or allocation).
            const result = this.#als
                ? await this.#als.run(call, () => this.#invoke(method, args))
                : await this.#invoke(method, args);
            failed = false;
            return result;
        } finally {
            this.#currentCall = null;
            // The dev slow-turn warning and the observer want the same
            // number, so compute it once and only when someone reads it.
            const elapsed = timing ? performance.now() - startedAt : 0;
            if (__DEV__ && elapsed > this.#host.slowTurnMs) {
                const interleaved =
                    this.#interleaveAll ||
                    (this.#interleaveMethods !== null && this.#interleaveMethods.has(method));
                console.warn(
                    interleaved
                        ? `[sigx actors] slow turn: ${actorLabel(this.ref)}.${method}() ran for ` +
                              `${elapsed}ms. Interleaved turns block no queued messages, but a ` +
                              `slow one still delays deactivation and pins the activation.`
                        : `[sigx actors] slow turn: ${actorLabel(this.ref)}.${method}() held the ` +
                              `activation for ${elapsed}ms. Awaits inside a turn block every queued ` +
                              `message — move slow I/O out of the actor or split the method.`
                );
            }
            if (observer) {
                try {
                    // enqueuedAt is 0 when observation was switched ON
                    // between this message being queued and its turn
                    // running. `startedAt - 0` would report process uptime as
                    // a queue wait, so the first such turn reports 0 instead.
                    const queued = enqueuedAt === 0 ? 0 : startedAt - enqueuedAt;
                    observer(this.ref, method, queued, elapsed, failed, call);
                } catch (error) {
                    // A metrics plugin must never be able to fail a turn, nor
                    // mask the real error this `finally` may be unwinding.
                    if (__DEV__) {
                        console.error(
                            `[sigx actors] a turn observer threw for ` +
                                `${actorLabel(this.ref)}.${method}():`,
                            error
                        );
                    }
                }
            }
            this.#afterTurn(started);
        }
    }

    async #invoke(method: string, args: readonly unknown[]): Promise<unknown> {
        const opts = this.def.__sigxActor;
        if (method === REMINDER_METHOD) {
            const name = String(args[0]);
            if (name === TASK_REMINDER) {
                // The runtime's liveness reminder — never the user's. Its
                // real work already happened: delivery re-activated the
                // actor and `create` resumed the ledgered runs. Here, only
                // self-heal: restart an entry that somehow has no run, and
                // disarm a reminder whose ledger is gone.
                const ledger = await this.#ledger.load();
                const entries = Object.entries(ledger);
                if (entries.length === 0) {
                    if (this.#tasks.size === 0) {
                        await this.#ctx.reminders.clear(TASK_REMINDER);
                    }
                    return undefined;
                }
                for (const [taskName, entry] of entries) {
                    if (!this.#tasks.has(taskName)) await this.#resumeTask(taskName, entry);
                }
                return undefined;
            }
            if (!opts.onReminder) {
                if (__DEV__) {
                    console.warn(
                        `[sigx actors] reminder "${name}" fired on ${actorLabel(this.ref)}, which ` +
                            `has no onReminder handler — clearing it.`
                    );
                }
                await this.#ctx.reminders.clear(name);
                return undefined;
            }
            return opts.onReminder(this.#ctx, name);
        }
        if (method === TOPIC_METHOD) {
            const event = args[0] as TopicEvent;
            const handler = subscriptionHandler(
                subscriptionFor(this.def, event?.topic?.name ?? '')
            );
            if (!handler) {
                // A deploy skew: a peer still publishing to a subscription
                // this build removed. Best-effort delivery drops it — the
                // reminder posture, minus the clear (nothing is stored).
                if (__DEV__) {
                    console.warn(
                        `[sigx actors] a topic event for "${event?.topic?.name}" arrived on ` +
                            `${actorLabel(this.ref)}, which declares no subscription to it — ` +
                            `dropped.`
                    );
                }
                return undefined;
            }
            return handler(this.#ctx, event);
        }
        // OWN keys only — see `ownFn`. A prototype member is not a method.
        const fn = ownFn<AnyFn>(this.#methods, method);
        if (!fn) {
            if (this.def.streamNames.includes(method)) {
                throw new ActorMethodNotFoundError(
                    this.ref.type,
                    `${method} (it is a stream method — iterate it instead of awaiting it)`
                );
            }
            throw new ActorMethodNotFoundError(this.ref.type, method);
        }
        return fn(...(args as unknown[]));
    }

    #afterTurn(startedMs: number): void {
        this.lastActivityMs = Math.max(this.lastActivityMs, startedMs, Date.now());
        // Fold the turn's writes into #version FIRST — the walk that
        // re-subscribes anything this turn added happens here, once per
        // dirty boundary, before the comparisons below read #version.
        this.#consumeDirty();
        if (this.#version > this.#notifiedVersion) {
            this.#notifiedVersion = this.#version;
            if (this.#subs.size > 0) {
                // The snapshot is built at most once per boundary AND only if
                // someone actually wants one: a set of purely value-free or
                // throttled-and-inside-their-window subscribers costs zero
                // clones (#129). It is still shared by everyone who does get
                // one, exactly as before.
                let snap: object | undefined;
                for (const sub of this.#subs) {
                    if (this.#deferToWindow(sub)) continue;
                    this.#deliver(
                        sub,
                        sub.ticksOnly ? CHANGE_TICK : (snap ??= this.#takeBoundarySnapshot())
                    );
                }
            }
            if (this.#isWriteBehind() && this.#version > this.#savedVersion) {
                this.#scheduleWriteBehind();
            }
        }
        if (this.#faulted && !this.#faultReported) {
            this.#faultReported = true;
            this.#host.onFault(this);
        } else if (this.#deactivateRequested && this.turns.depth <= 1 && !this.#faulted) {
            // depth 1 = only the turn that is settling right now — nothing
            // else queued OR in flight (this finally runs before the
            // turns' own settlement decrement, in both lanes), so the
            // requested deactivation can begin.
            this.#host.onIdleRequest(this);
        }
    }

    #isWriteBehind(): boolean {
        const p = this.def.__sigxActor.persistence;
        return typeof p === 'object' && p.mode === 'write-behind';
    }

    #scheduleWriteBehind(): void {
        if (this.#cancelWriteBehind) return;
        const p = this.def.__sigxActor.persistence as { debounceMs?: number };
        this.#cancelWriteBehind = this.#host.scheduler.after(p.debounceMs ?? 50, () => {
            this.#cancelWriteBehind = null;
            // A system turn — serialized with user turns, so on a serial
            // actor the save always captures a between-turns state. On an
            // interleaving actor there IS no between-turns state: the
            // snapshot is synchronously consistent but may be mid-logical-
            // turn — the documented 'always' contract.
            this.turns.run(() => this.#gatedSave(this.#version)).catch(() => {
                // Save failures fault the activation via #doSave; a closed
                // turns at deactivation time is handled by the final flush.
            });
        });
    }

    /**
     * Single-flight save with trailing coalescing: at most one storage
     * write in flight per activation, and a caller returns once a snapshot
     * at-or-after version `wanted` is durable. A serial actor never
     * contends here; on an interleaving one, two turns saving concurrently
     * would otherwise CAS against the same etag and the loser would fault
     * the activation on its own sibling's write.
     */
    async #gatedSave(wanted: number): Promise<void> {
        while (this.#savedVersion < wanted) {
            if (this.#savePending) {
                // The in-flight save's failure belongs to ITS caller; a
                // persistent fault re-throws from our own #doSave below.
                await this.#savePending.catch(() => {});
                continue;
            }
            const run = this.#doSave();
            this.#savePending = run;
            try {
                await run;
            } finally {
                this.#savePending = null;
            }
        }
    }

    /**
     * Prepared at save time for the boundary emit: the snapshot revived
     * from the SAME encode the save produced, so a boundary that both
     * saves and emits costs one whole-state encode instead of two (#233).
     * Version-keyed; a stale entry is dropped, never delivered.
     */
    #preparedSnap: { version: number; snap: object } | null = null;

    /**
     * Will `#afterTurn` at `version` want a VALUE snapshot? A false
     * positive costs one wasted revive, a false negative one extra encode —
     * correctness never depends on this predicate, because
     * `#takeBoundarySnapshot` falls back to `#snapshot()` on any mismatch.
     */
    #wantsSnapshotAt(version: number): boolean {
        if (version <= this.#notifiedVersion) return false;
        for (const sub of this.#subs) {
            // Value-wanting and not parked inside a throttle window — the
            // same tests `#afterTurn`'s delivery loop applies, read without
            // its side effects.
            if (!sub.ticksOnly && !sub.done && sub.cancelWindow === null) return true;
        }
        return false;
    }

    async #doSave(): Promise<void> {
        if (this.#faulted) throw this.#faulted;
        const version = this.#version;
        try {
            const tree = this.#host.encodeState(toRaw(this.#state));
            if (this.#wantsSnapshotAt(version)) {
                // BEFORE `storeState`: storage takes ownership of the tree
                // with no defensive clone (#25), so nothing may be built
                // from it afterwards. Revive allocates fresh containers for
                // every codec-produced node, so the snapshot shares nothing
                // with the record storage now owns.
                this.#preparedSnap = { version, snap: this.#host.reviveState(tree) };
            }
            this.#etag = await this.#host.storeState(this.ref, tree, this.#etag);
            this.#savedVersion = version;
        } catch (error) {
            if (isStorageConflict(error)) {
                const conflict = new ActorStateConflictError(actorLabel(this.ref));
                this.#faulted = conflict;
                throw conflict;
            }
            throw error;
        }
    }

    /**
     * Change detection without a walk per mutation (#28). The old shape was
     * `watch(() => this.#state, () => this.#version++, { deep: true })` — a
     * full deep traversal of actor state on EVERY mutation, to learn one
     * bit, measured at 13% of the `state/*` profile and the cause of the
     * change-feed 0→1-subscriber cliff.
     *
     * Both consumers of `#version` read it only at a boundary (`#afterTurn`,
     * the deactivation flush, `ctx.save()`), so per-mutation granularity
     * buys nothing. The effect's `scheduler` is the seam: a write flips
     * `#dirty` synchronously (out-of-turn and `onActivate` mutations are
     * still caught at write time) and parks the re-run — the ONE deep walk
     * happens when `#consumeDirty()` folds at the next boundary, re-tracking
     * whatever the turn added. The floor is one walk per DIRTY boundary, not
     * per mutation.
     *
     * What that walk COSTS is reactivity's problem, and since #124 it is
     * reactivity's code. This used to call a private `trackDeep` copied from
     * upstream `traverse`, carrying a warning that divergence here would be
     * divergence in what counts as a change — and it diverged: upstream
     * stopped enumerating the proxy (signalxjs/core#642) and then stopped
     * reading keys back through it altogether (core#645, one any-write dep
     * per object, −75.4% on a 200-row fixture), while the copy stayed on the
     * original algorithm. It measured ~1.2 ms for ONE mutating turn over
     * 200-row state, with no subscriber and no write involved.
     *
     * `deepTrack` is that same upstream traversal, exported for this caller
     * (core#651) because `watch(deep)` is not usable here — `WatchOptions`
     * has no `scheduler`, and the parked re-run above is the whole design.
     * Calling core's walk rather than mirroring it is what keeps the two
     * from disagreeing again.
     */
    #ensureChangeTracking(): void {
        if (this.#trackingInstalled) return;
        this.#trackingInstalled = true;
        this.#scope.run(() => {
            // The first run is immediate and inline — tracking is
            // established (and every current node subscribed) right here.
            // `effect()` registers its own disposer with the active scope,
            // so `#scope.stop()` at deactivation is the teardown.
            effect(() => deepTrack(this.#state), {
                scheduler: (run) => {
                    this.#dirty = true;
                    this.#retrack = run;
                }
            });
        });
    }

    /**
     * Fold a pending dirty mark into `#version` and re-establish tracking —
     * one deep walk per dirty boundary. MUST run at every boundary before
     * the next turn's writes: an object added since the last walk is
     * untracked until the re-run subscribes it.
     */
    #consumeDirty(): void {
        if (!this.#dirty) return;
        this.#dirty = false;
        this.#version++;
        const retrack = this.#retrack;
        this.#retrack = null;
        // Re-runs the tracking walk (validated by the effect itself); a
        // no-op if the effect was stopped with the scope in the meantime.
        retrack?.();
    }

    #snapshot(): object {
        return this.#host.cloneState(toRaw(this.#state));
    }

    /**
     * The boundary's shared snapshot: the one `#doSave` prepared when it
     * saved this same version, else a fresh clone. Consumed either way —
     * a prepared snapshot for a version the boundary has moved past would
     * otherwise linger until the next save.
     */
    #takeBoundarySnapshot(): object {
        const prepared = this.#preparedSnap;
        this.#preparedSnap = null;
        if (prepared && prepared.version === this.#version) return prepared.snap;
        return this.#snapshot();
    }

    #buildContext(): ActorContext<object> {
        const base = this.#buildBaseContext();
        const extension = this.#host.extendContext?.(this.ref);
        if (!extension) return base;
        for (const key of Object.keys(extension)) {
            // Prototype-mutating keys are refused OUTRIGHT, ahead of the
            // built-in check. `__proto__` and `constructor` would already be
            // caught by the `in` test below (both live on Object.prototype),
            // but only implicitly — naming them here keeps the guard from
            // silently lapsing if that test ever narrows to `hasOwn`, and a
            // plugin that forwards a `JSON.parse`d object can carry an OWN
            // `__proto__` key.
            if (UNSAFE_CONTEXT_KEYS.has(key)) {
                if (__DEV__) {
                    console.warn(
                        `[sigx actors] a plugin's extendContext() returned the unsafe key ` +
                            `"${key}" for ${actorLabel(this.ref)} — ignored (it would mutate the ` +
                            `context prototype).`
                    );
                }
                continue;
            }
            // `in` also catches the accessor members (`state`), so a plugin
            // can never shadow a built-in with a plain data property.
            if (key in base) {
                if (__DEV__) {
                    console.warn(
                        `[sigx actors] a plugin's extendContext() tried to overwrite the built-in ` +
                            `ctx.${key} on ${actorLabel(this.ref)} — ignored. Rename the addition.`
                    );
                }
                continue;
            }
            (base as unknown as Record<string, unknown>)[key] = (
                extension as Record<string, unknown>
            )[key];
        }
        return base;
    }

    /**
     * The `streams:` table for ONE subscription, built with a context of its
     * own — which is what makes the feeds that body opens attributable to it,
     * and therefore closable when its consumer disconnects.
     *
     * Per-subscription rather than per-activation because a shared context
     * cannot tell two concurrent bodies apart: a body resumes from an
     * internal `await` in a microtask of its own, so no "currently running
     * stream" flag can be trusted. The factory is a pure table constructor by
     * contract (it must not touch `ctx` while constructing — `defineActor`
     * already calls it a second time to read `streamNames`), so calling it
     * again per subscription costs a handful of closures and nothing else.
     */
    #streamTable(feeds: StreamFeeds): Record<string, AnyStreamFn> {
        const opts = this.def.__sigxActor;
        if (!opts.streams) return {};
        // Inside the scope, so computeds/watches a body creates die with it.
        const table = this.#scope.run(() => opts.streams!(this.#streamContext(feeds)));
        // `run` returns undefined on a stopped scope — a turn that survived a
        // force-drop. Say so, rather than failing as an unknown method.
        if (!table) throw new HostShutdownError();
        return table as Record<string, AnyStreamFn>;
    }

    /**
     * Context for one `streams:` body: `changes()` records its subscriptions
     * in `feeds` so `openStream` can close them from outside the generator,
     * and — in dev — `state` is shadowed by a warning accessor. Stream bodies
     * run detached from the turns, so a turn can mutate underneath a live
     * read; they must use `snapshot()` / `changes()`. Everything else is
     * inherited unchanged (the built-in accessors close over `self`, not
     * `this`).
     */
    #streamContext(feeds: StreamFeeds): ActorContext<object> {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const self = this;
        const derived = Object.create(this.#ctx) as ActorContext<object>;
        // Stateless: the prototype's `changes`/`state` are already throwing
        // guards — shadowing them here would quietly re-open the surface.
        if (this.stateless) return derived;
        Object.defineProperty(derived, 'changes', {
            value: (options?: { initial?: boolean }): AsyncIterable<object> =>
                self.#openChanges(options, feeds),
            enumerable: true,
            configurable: true
        });
        if (!__DEV__) return derived;
        Object.defineProperty(derived, 'state', {
            get() {
                if (!self.#warnedStreamState) {
                    self.#warnedStreamState = true;
                    console.warn(
                        `[sigx actors] a streams: body on ${actorLabel(self.ref)} read live ` +
                            `ctx.state. Stream bodies run DETACHED, outside any sequence — a turn ` +
                            `can mutate underneath the read. Use ctx.snapshot() or ` +
                            `ctx.changes({ initial: true }) instead.`
                    );
                }
                return self.#state;
            },
            enumerable: true,
            configurable: true
        });
        return derived;
    }

    get #ledger(): TaskLedgerApi {
        return (this.#ledgerApi ??= this.#host.tasks(this.ref));
    }

    /**
     * Start one detached task run, DURABLY: the ledger entry is written and
     * the liveness reminder armed before the body launches, so a crash any
     * time after `start` resolves still resumes the run. Resolves once the
     * body is launched — settlement is the run's own business.
     * Single-flight per name.
     */
    async #startTask(name: string, input: unknown): Promise<void> {
        if (this.#faulted) throw this.#faulted;
        // Deactivation in progress: the actor is going away — starting new
        // detached work now would either be instantly aborted or escape the
        // settle the deactivation just awaited.
        if (this.#abort.signal.aborted) throw new HostShutdownError();
        if (this.#tasks.has(name)) return;
        const run: TaskRun = {
            name,
            controller: new AbortController(),
            callId: mintCallId(),
            startedAt: Date.now(),
            restarts: 0,
            settled: Promise.resolve()
        };
        const fn = this.#resolveTask(name, run);
        if (typeof fn !== 'function') {
            throw new ActorMethodNotFoundError(
                this.ref.type,
                this.def.__sigxActor.tasks
                    ? `${name} (not in the tasks: section)`
                    : `${name} (no tasks: section on this actor)`
            );
        }
        // Reserve SYNCHRONOUSLY, before the durable writes await: start is
        // callable from detached code, so two concurrent starts would both
        // pass the has() gate above and double-launch. Rolled back if the
        // durable half fails.
        this.#reserve(run);
        try {
            await this.#ledger.mutate((ledger) => {
                ledger[name] = {
                    ...(input !== undefined ? { input } : {}),
                    startedAt: run.startedAt,
                    restarts: 0
                };
            });
            await this.#host
                .reminders(this.ref)
                .set(TASK_REMINDER, { due: TASK_REMINDER_MS, period: TASK_REMINDER_MS });
        } catch (error) {
            this.#release(run);
            throw error;
        }
        this.#launch(run, fn, input);
    }

    /**
     * Restart one ledgered run — on activation (the crash-recovery path)
     * or from the liveness reminder's self-heal. An entry whose name is no
     * longer in the `tasks:` table is dev-warned and forgotten.
     */
    async #resumeTask(name: string, entry: TaskLedgerEntry): Promise<void> {
        if (this.#faulted || this.#abort.signal.aborted || this.#tasks.has(name)) return;
        const run: TaskRun = {
            name,
            controller: new AbortController(),
            callId: mintCallId(),
            startedAt: entry.startedAt,
            restarts: entry.restarts + 1,
            settled: Promise.resolve()
        };
        const fn = this.#resolveTask(name, run);
        if (typeof fn !== 'function') {
            if (__DEV__) {
                console.warn(
                    `[sigx actors] the task ledger of ${actorLabel(this.ref)} names "${name}", ` +
                        `which is not in the definition's tasks: section any more — dropping it.`
                );
            }
            await this.#forgetTask(name);
            return;
        }
        // Same synchronous reservation as #startTask: activation-resume and
        // the reminder self-heal can race for one name.
        this.#reserve(run);
        try {
            await this.#ledger.mutate((ledger) => {
                const persisted = ledger[name];
                if (persisted) persisted.restarts = run.restarts;
            });
        } catch (error) {
            this.#release(run);
            throw error;
        }
        this.#launch(run, fn, entry.input);
    }

    /** The single-flight gate. Synchronous on purpose — see #startTask. */
    #reserve(run: TaskRun): void {
        this.#tasks.set(run.name, run);
        this.#keepAlive++;
    }

    #release(run: TaskRun): void {
        if (this.#tasks.get(run.name) === run) this.#tasks.delete(run.name);
        this.#keepAlive--;
        this.lastActivityMs = Date.now();
    }

    /** Resolve `name` from a per-run `tasks:` table (own keys only — see `ownFn`). */
    #resolveTask(name: string, run: TaskRun): AnyTaskFn | undefined {
        const opts = this.def.__sigxActor;
        if (!opts.tasks) return undefined;
        // Per RUN, like #streamTable is per subscription: each run's table
        // closes over its own derived context (its own abortSignal).
        const table = this.#scope.run(() => opts.tasks!(this.#taskContext(run)));
        if (!table) throw new HostShutdownError();
        return ownFn<AnyTaskFn>(table, name);
    }

    /** Run an already-RESERVED task body (see #reserve). */
    #launch(run: TaskRun, fn: AnyTaskFn, input: unknown): void {
        // On an interleaving activation the launch site is usually INSIDE a
        // turn's call store, and async work created here would inherit it —
        // a detached body's ctx.actor() would then silently carry the
        // STARTING turn's chain instead of starting fresh. Clear it.
        const body = (): Promise<void> => this.#taskBody(run, fn, input);
        run.settled = this.#als ? this.#als.run(null, body) : body();
    }

    #taskBody(run: TaskRun, fn: AnyTaskFn, input: unknown): Promise<void> {
        return (async () => {
            try {
                await fn(input);
            } catch (error) {
                // Terminal: a thrown task is not restarted (retries belong
                // to layers above). An abort unwinding as a throw is the
                // expected wind-down path, not worth a warning.
                if (__DEV__ && !run.controller.signal.aborted) {
                    console.error(
                        `[sigx actors] task "${run.name}" of ${actorLabel(this.ref)} threw:`,
                        error
                    );
                }
            } finally {
                this.#release(run);
            }
            // Ledger bookkeeping is INSIDE `settled`, so deactivation's
            // grace wait covers it. An interrupted run — aborted by
            // deactivation, not cancel — KEEPS its entry: that entry is the
            // resume. This holds whether the body returned or threw during
            // the wind-down; a run that completes right as deactivation
            // fires restarts once more (at-least-once, documented).
            const signal = run.controller.signal;
            const interrupted = signal.aborted && signal.reason !== 'cancelled';
            if (!interrupted) {
                try {
                    await this.#forgetTask(run.name);
                } catch (error) {
                    if (__DEV__) {
                        console.error(
                            `[sigx actors] failed to clear the task ledger for "${run.name}" ` +
                                `of ${actorLabel(this.ref)} (the liveness reminder will ` +
                                `self-heal):`,
                            error
                        );
                    }
                }
            }
        })();
    }

    /** Drop one entry; when the ledger empties, disarm the reminder too. */
    async #forgetTask(name: string): Promise<void> {
        const ledger = await this.#ledger.mutate((l) => {
            delete l[name];
        });
        if (Object.keys(ledger).length === 0) {
            await this.#host.reminders(this.ref).clear(TASK_REMINDER);
        }
    }

    /**
     * A REQUEST, not a join: aborts the run's signal and returns. Awaiting
     * settlement here would deadlock — cancel is called from method turns,
     * and a winding-down task's final `turn()` queues behind exactly that
     * turn. The run leaves `list()` when its body settles. (Deactivation
     * CAN await settlement: its grace runs outside any turn.) An
     * INTERLEAVED turn could join without deadlocking, but cancel semantics
     * do not fork on the reentrancy mode — request everywhere.
     */
    #cancelTask(name: string): Promise<void> {
        this.#tasks.get(name)?.controller.abort('cancelled');
        return Promise.resolve();
    }

    #listTasks(): readonly TaskInfo[] {
        return [...this.#tasks.values()].map((r) => ({
            name: r.name,
            startedAt: r.startedAt,
            restarts: r.restarts
        }));
    }

    /**
     * Context for one task run: `abortSignal` is the RUN's signal, `turn()`
     * re-enters the turns, `changes()` feeds are owned by the run (its
     * abort closes them — a task parked in `for await (ctx.changes())` on a
     * quiet actor is waiting on a wake nothing else would ever deliver, so
     * cancellation must be able to end the feed from outside, exactly like
     * a stream consumer disconnect), and — in dev — `state` is shadowed by
     * a warning accessor (the type already omits it; this catches untyped
     * access). Everything else is inherited unchanged.
     */
    #taskContext(run: TaskRun): ActorContext<object> {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const self = this;
        const derived = Object.create(this.#ctx) as ActorContext<object>;
        const feeds: StreamFeeds = { subs: new Set(), closed: false };
        run.controller.signal.addEventListener(
            'abort',
            () => {
                // Latched (`closed`), so a feed the body opens after the
                // abort is born spent instead of parking forever.
                feeds.closed = true;
                self.#closeSubs(feeds.subs);
            },
            { once: true }
        );
        Object.defineProperty(derived, 'changes', {
            value: (options?: { initial?: boolean }): AsyncIterable<object> =>
                self.#openChanges(options, feeds),
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(derived, 'abortSignal', {
            value: run.controller.signal,
            enumerable: true,
            configurable: true
        });
        // Hard errors, not dev warnings: these WRITE. A detached save
        // captures whatever half-mutated state a concurrent turn left, and
        // races the etag another save is carrying — `turn()` is the door.
        for (const member of ['save', 'clearState'] as const) {
            Object.defineProperty(derived, member, {
                value: () =>
                    Promise.reject(
                        new Error(
                            `[sigx actors] ctx.${member}() called from a tasks: body on ` +
                                `${actorLabel(self.ref)}. Task bodies run DETACHED from the ` +
                                `turn — persistence must go through ctx.turn(), e.g. ` +
                                `await ctx.turn((c) => c.${member}()).`
                        )
                    ),
                enumerable: true,
                configurable: true
            });
        }
        Object.defineProperty(derived, 'turn', {
            value: <T,>(fn: (ctx: ActorContext<object>) => T | Promise<T>): Promise<T> =>
                self.turns.run(async () => {
                    if (self.#faulted) throw self.#faulted;
                    const started = Date.now();
                    const prev = self.#currentCall;
                    // The run's one callId, so ctx.actor() calls made inside
                    // the turn carry a chain and traces correlate the run.
                    // Deliberately NOT the starting call's context: a task
                    // outlives its starter, so neither traceparent nor the
                    // context bag flows in — ctx.bag reads empty here.
                    const turnCall = { callChain: [self.id], callId: run.callId };
                    self.#currentCall = turnCall;
                    try {
                        return self.#als
                            ? await self.#als.run(turnCall, () => fn(self.#ctx))
                            : await fn(self.#ctx);
                    } finally {
                        self.#currentCall = prev;
                        self.#afterTurn(started);
                    }
                }),
            enumerable: true,
            configurable: true
        });
        if (!__DEV__) return derived;
        Object.defineProperty(derived, 'state', {
            get() {
                if (!self.#warnedTaskState) {
                    self.#warnedTaskState = true;
                    console.warn(
                        `[sigx actors] a tasks: body on ${actorLabel(self.ref)} read live ` +
                            `ctx.state. Task bodies run DETACHED, outside any turn, sequence — a turn ` +
                            `can mutate underneath the read. Use ctx.snapshot(), ` +
                            `ctx.changes(), or mutate inside ctx.turn().`
                    );
                }
                return self.#state;
            },
            enumerable: true,
            configurable: true
        });
        return derived;
    }

    /**
     * Replace the identity-bound members with loud throws. They are already
     * typed away on `WorkerContext`, so the only way here is an untyped
     * cast — and a `save()` that silently succeeded would FAKE persistence,
     * which makes this a correctness commitment (throws in every build), not
     * a dev-ergonomics warning.
     */
    #guardStateless(base: ActorContext<object>): ActorContext<object> {
        const type = this.ref.type;
        const guard = (member: string) => (): never => {
            throw new Error(
                `[sigx actors] ctx.${member} is not available on stateless worker "${type}" — ` +
                    `workers have no state, persistence, reminders or tasks.`
            );
        };
        for (const member of ['save', 'clearState', 'snapshot', 'changes']) {
            Object.defineProperty(base, member, {
                value: guard(member),
                enumerable: true,
                configurable: true
            });
        }
        for (const member of ['state', 'reminders', 'tasks']) {
            Object.defineProperty(base, member, {
                get: guard(member),
                enumerable: true,
                configurable: true
            });
        }
        return base;
    }

    #buildBaseContext(): ActorContext<object> {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const self = this;
        const opts = this.def.__sigxActor;
        // A declared overload pair (not a widened `value?: unknown`), so the
        // subtree form keeps its `T` — including primitives: the argument
        // COUNT decides, and `snapshot(null)` is a legitimate value clone,
        // never a whole-state snapshot.
        function snapshot(): object;
        function snapshot<T>(value: T): T;
        function snapshot<T>(...args: [] | [T]): T | object {
            if (args.length > 0) {
                // The length check proves the [T] branch; tuple-union
                // narrowing does not, hence the assertion.
                return self.#host.cloneState(toRaw(args[0] as T));
            }
            return self.#snapshot();
        }
        const base: ActorContext<object> = {
            ref: this.ref,
            key: this.ref.key,
            get state() {
                return self.#state;
            },
            get bag(): Readonly<Record<string, string>> {
                // Resolved PER READ from the current turn's context (the ALS
                // store on interleaving activations), because this ctx object
                // is reused across turns. Empty outside any turn.
                return self.#callContext()?.bag ?? EMPTY_CALL_BAG;
            },
            get principal(): unknown {
                // Same per-read resolution as `bag`, then decoded lazily and
                // memoized against the ENCODED string — so an actor that
                // never reads it never pays the decode, and one that reads
                // it in a loop pays once per distinct identity. Keying on
                // the encoded value (not on the turn) is what makes the memo
                // correct across the reused ctx object.
                const current = self.#callContext();
                // Discovery (#121): a shared watch's read consulting the
                // principal means its loop cannot serve mixed identities.
                // Record the key BEFORE the memo can short-circuit anything;
                // `#resolveWatch` splits future joins and the invoke's sweep
                // evicts current mismatches before the next push.
                const watch = current === null ? undefined : (current as WatchInvokeCall);
                const base = watch?.[kWatchBase];
                if (base !== undefined) {
                    const declared = watch![kWatchDeclared];
                    if (declared !== undefined) {
                        // The method PROMISED this read ignores identity
                        // (#138). Fail it rather than record discovery: a
                        // relay may already be serving distinct identities
                        // from one merged stream, which the split cannot
                        // undo. Recorded on the marker so the invoke wrapper
                        // rethrows even if the read body catches this.
                        declared.violated ??= new ActorWatchDeclarationError(
                            self.ref.type,
                            declared.method
                        );
                        if (__DEV__) {
                            // The error surfaces at the SUBSCRIBER, possibly
                            // several hops from the actor whose declaration
                            // is wrong; this names the actor at the source.
                            console.error(declared.violated.message);
                        }
                        throw declared.violated;
                    }
                    self.#watchPrincipalDependent.add(base);
                }
                const encoded = current?.principal;
                if (encoded === undefined) return null;
                self.#principalMemo ??= new Map();
                if (self.#principalMemo.has(encoded)) return self.#principalMemo.get(encoded);
                const value = decodePrincipal(encoded);
                if (self.#principalMemo.size >= PRINCIPAL_MEMO_CAP) {
                    // Map iteration is insertion order, so the first key is
                    // the oldest entry — a bound, not an LRU: correctness
                    // never depends on WHICH entry goes, only that the map
                    // cannot grow with the identity population.
                    const oldest = self.#principalMemo.keys().next().value;
                    if (oldest !== undefined) self.#principalMemo.delete(oldest);
                }
                self.#principalMemo.set(encoded, value);
                return value;
            },
            async save(): Promise<void> {
                // Fold any tracked mutations first so `wanted` sits above
                // them and savedVersion bookkeeping stays consistent when a
                // subscriber's tracking runs alongside explicit saves.
                self.#consumeDirty();
                // Explicit-mode saves don't need change tracking; bump the
                // version so savedVersion bookkeeping stays consistent.
                // Through the gate: resolves once a snapshot at-or-after
                // this caller's mutations is durable.
                const wanted = ++self.#version;
                await self.#gatedSave(wanted);
            },
            async clearState(): Promise<void> {
                // Serialized through the save gate — a clear racing a save
                // (possible on an interleaving actor) would corrupt the
                // etag bookkeeping exactly like two racing saves.
                while (self.#savePending) await self.#savePending.catch(() => {});
                const run = (async () => {
                    await self.#host.clearStoredState(self.ref, self.#etag);
                    self.#etag = null;
                    const fresh = opts.state(self.ref.key) as Record<string, unknown>;
                    const live = self.#state as Record<string, unknown>;
                    // Reset in place — the proxy identity is captured by
                    // closures.
                    for (const k of Object.keys(live)) {
                        if (!(k in fresh)) delete live[k];
                    }
                    Object.assign(live, fresh);
                })();
                self.#savePending = run;
                try {
                    await run;
                } finally {
                    self.#savePending = null;
                }
            },
            timer(name: string, cb: () => void | Promise<void>, options: TimerOptions): TimerHandle {
                self.#timers.get(name)?.clear();
                let queued = false;
                const tick = () => {
                    // Coalesce: a tick behind a slow turn must not pile up.
                    if (queued) return;
                    queued = true;
                    self.turns
                        .run(async () => {
                            queued = false;
                            if (self.#faulted) return;
                            const started = Date.now();
                            const prev = self.#currentCall;
                            // Fresh context: a tick has no caller, so it
                            // inherits neither traceparent nor the context
                            // bag — ctx.bag reads empty in a timer callback.
                            const tickCall = {
                                callChain: [self.id],
                                callId: mintCallId()
                            };
                            self.#currentCall = tickCall;
                            try {
                                // The tick closure was created inside the
                                // REGISTERING turn's call store — without the
                                // explicit run it would inherit that stale
                                // context on an interleaving activation.
                                await (self.#als ? self.#als.run(tickCall, () => cb()) : cb());
                            } finally {
                                self.#currentCall = prev;
                                if (options.keepAlive) self.#afterTurn(started);
                                else {
                                    const keep = self.lastActivityMs;
                                    self.#afterTurn(started);
                                    self.lastActivityMs = keep;
                                }
                            }
                        })
                        .catch((error) => {
                            if (__DEV__) {
                                console.error(
                                    `[sigx actors] timer "${name}" of ${actorLabel(self.ref)} threw:`,
                                    error
                                );
                            }
                        });
                };
                let cancelInterval: (() => void) | null = null;
                const cancelDue = self.#host.scheduler.after(options.due, () => {
                    tick();
                    if (options.period !== undefined) {
                        cancelInterval = self.#host.scheduler.every(options.period, tick);
                    }
                });
                const handle = {
                    clear() {
                        cancelDue();
                        cancelInterval?.();
                    }
                };
                self.#timers.set(name, handle);
                return { cancel: () => handle.clear() };
            },
            // A worker member never builds the reminder API — the member is
            // replaced by a throwing guard below, and constructing it would
            // touch reminder machinery a stateless type must never reach.
            reminders: this.stateless ? (undefined as never) : this.#host.reminders(this.ref),
            actor<D extends AnyActorDefinition>(def: D, key: string): ActorClientWith<D> {
                // The outbound context appends SELF to the chain — that is
                // what lets the target detect A→B→A cycles.
                return self.#host.actorClient(def, key, () => {
                    const current = self.#callContext();
                    if (!current) {
                        if (__DEV__) {
                            console.warn(
                                `[sigx actors] ctx.actor() called on ${actorLabel(self.ref)} with ` +
                                    `no turn in progress (a detached callback?) — the call starts ` +
                                    `a fresh chain, so reentrancy detection cannot see this hop.`
                            );
                        }
                        return null;
                    }
                    return {
                        callChain: [...current.callChain, self.id],
                        callId: current.callId,
                        deadline: current.deadline,
                        // Relayed even on untraced hosts, so a trace that
                        // crosses an uninstrumented middle hop still joins.
                        traceparent: current.traceparent,
                        // Same relay rule: the edge-stamped identity is the
                        // only one inner hops will ever see. Authentication
                        // is per-REQUEST and authorization per ENTRY POINT,
                        // and a hop is neither — so the caller a downstream
                        // actor sees is the original one, not the actor that
                        // called it.
                        bag: current.bag,
                        principal: current.principal,
                        abortSignal: current.abortSignal
                    };
                });
            },
            publish<T>(topicRef: Topic<T>, payload: T): Promise<TopicPublishReport> {
                // Same chain rule as ctx.actor(): the outbound context
                // appends SELF, so a subscription that cycles back into this
                // actor is a detected deadlock in the report, not a hang —
                // the publishing turn is awaiting the fan-out, so an
                // undetected cycle could never complete.
                const current = self.#callContext();
                if (!current && __DEV__) {
                    console.warn(
                        `[sigx actors] ctx.publish() called on ${actorLabel(self.ref)} with ` +
                            `no turn in progress (a detached callback?) — the publish starts ` +
                            `a fresh chain, so a subscription cycling back here cannot be ` +
                            `detected as a deadlock.`
                    );
                }
                const call = current
                    ? {
                          callChain: [...current.callChain, self.id],
                          callId: current.callId,
                          deadline: current.deadline,
                          traceparent: current.traceparent,
                          bag: current.bag,
                          // A subscriber is attributed to whoever published,
                          // which is whoever entered the system.
                          principal: current.principal,
                          abortSignal: current.abortSignal
                      }
                    : null;
                return self.#host.publish(topicRef, payload, call, self.ref);
            },
            deactivate(): void {
                self.#deactivateRequested = true;
            },
            abortSignal: this.#abort.signal,
            tasks: {
                start: (name: string, input?: unknown) => self.#startTask(name, input),
                cancel: (name: string) => self.#cancelTask(name),
                list: () => self.#listTasks()
            } satisfies TaskApi,
            snapshot,
            changes(options?: { initial?: boolean; throttleMs?: number }): AsyncIterable<object> {
                return self.#openChanges(options);
            }
        };
        return this.stateless ? this.#guardStateless(base) : base;
    }

    /**
     * One `ctx.changes()` subscription. `feeds` is the opening stream body's
     * record, when there is one — the link `openStream` needs to close a feed
     * whose generator can no longer be resumed to close it itself.
     */
    #openChanges(
        options: ChangesOptions | undefined,
        feeds?: StreamFeeds
    ): AsyncIterable<object> {
        const sub: ChangeSub = {
            queue: [],
            wake: null,
            done: false,
            ticksOnly: options?.ticksOnly === true,
            throttleMs: normalizeThrottleMs(options?.throttleMs, this.ref),
            pending: false,
            cancelWindow: null
        };
        if (feeds?.closed) {
            // The consumer went away before the body got this far. Hand it a
            // spent feed — no seed, no registration — so the body unwinds on
            // its first pull instead of parking on a subscription nothing
            // will ever close.
            sub.done = true;
            return this.#changeFeed(sub);
        }
        // The feed needs change detection even in explicit mode.
        this.#ensureChangeTracking();
        // Seed BEFORE the subscription goes live, in this same synchronous
        // call: a `yield ctx.snapshot()` prologue would instead subscribe
        // only after the consumer resumes, losing every mutation in that
        // window.
        if (options?.initial) sub.queue.push(sub.ticksOnly ? CHANGE_TICK : this.#snapshot());
        this.#subs.add(sub);
        feeds?.subs.add(sub);
        return this.#changeFeed(sub, feeds);
    }

    /** The iterable side of one `ChangeSub`. */
    #changeFeed(sub: ChangeSub, feeds?: StreamFeeds): AsyncIterable<object> {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const self = this;
        return {
            [Symbol.asyncIterator](): AsyncIterator<object> {
                return {
                    async next(): Promise<IteratorResult<object>> {
                        for (;;) {
                            if (sub.queue.length > 0) {
                                return { value: sub.queue.shift()!, done: false };
                            }
                            if (sub.done) return { value: undefined, done: true };
                            await new Promise<void>((r) => {
                                sub.wake = r;
                            });
                            sub.wake = null;
                        }
                    },
                    async return(): Promise<IteratorResult<object>> {
                        sub.done = true;
                        // No trailing flush here, unlike `#closeSubs`: this
                        // consumer is leaving, so a final snapshot would be
                        // built for nobody.
                        self.#closeWindow(sub);
                        self.#subs.delete(sub);
                        feeds?.subs.delete(sub);
                        // Wake a parked next(). Marking `done` is not
                        // enough: a consumer that disconnects while the feed
                        // is quiet is sitting on the wake promise, and
                        // nothing else will ever resolve it — the actor may
                        // never mutate again. The await inside `return()`
                        // then never settles and teardown hangs.
                        sub.wake?.();
                        return { value: undefined, done: true };
                    }
                };
            }
        };
    }

    /** Queue one value for a subscriber and wake it, dropping oldest at the bound. */
    #deliver(sub: ChangeSub, value: object): void {
        sub.queue.push(value);
        if (sub.queue.length > CHANGE_BUFFER) {
            sub.queue.shift();
            if (__DEV__ && !this.#warnedDroppedChanges) {
                this.#warnedDroppedChanges = true;
                console.warn(
                    `[sigx actors] ${actorLabel(this.ref)} change feed dropped ` +
                        `updates — a stream consumer is slower than the actor's ` +
                        `mutation rate (buffer: ${CHANGE_BUFFER}).`
                );
            }
        }
        sub.wake?.();
    }

    /**
     * `true` if this boundary is absorbed by an open throttle window, so the
     * caller must not emit (and must not build a snapshot).
     *
     * Leading edge plus trailing edge: the first boundary emits at once and
     * opens the window, everything inside it collapses into the single emit
     * the window makes on closing. That last emit takes a FRESH snapshot, so
     * a throttled consumer is never handed a state older than the window it
     * waited out.
     */
    #deferToWindow(sub: ChangeSub): boolean {
        if (sub.throttleMs <= 0) return false;
        if (sub.cancelWindow) {
            sub.pending = true;
            return true;
        }
        this.#openWindow(sub);
        return false;
    }

    #openWindow(sub: ChangeSub): void {
        sub.cancelWindow = this.#host.scheduler.after(sub.throttleMs, () => {
            sub.cancelWindow = null;
            if (!sub.pending || sub.done) return;
            sub.pending = false;
            // Out of turn, like the write-behind flush: on a serial actor
            // this lands between turns, so the snapshot is a settled state.
            // On `reentrant: 'always'` there is no between-turns state and
            // it may be mid-logical-turn — the same documented trade the
            // 'always' contract already makes for write-behind.
            //
            // `ticksOnly` is honoured here as everywhere else. No caller
            // combines it with a throttle today — `openWatch` passes only
            // `ticksOnly` — but a value-free subscriber that silently got a
            // full snapshot from this one path would be a contradiction
            // waiting for the first caller that does.
            this.#deliver(sub, sub.ticksOnly ? CHANGE_TICK : this.#snapshot());
            // A trailing emit starts its own window, so two bursts a
            // microsecond apart cannot produce two emits. It closes empty
            // and stops if nothing else lands.
            this.#openWindow(sub);
        });
    }

    /** Cancel a subscriber's throttle window, if one is open. */
    #closeWindow(sub: ChangeSub): void {
        sub.cancelWindow?.();
        sub.cancelWindow = null;
        sub.pending = false;
    }

    /**
     * Close subscriptions from outside their consumer: mark done, wake a
     * parked `next()`, and drop them from the activation's set. Safe to pass
     * `#subs` itself — deleting the current element mid-iteration is fine.
     */
    #closeSubs(subs: Set<ChangeSub>): void {
        for (const sub of subs) {
            // Flush a throttled subscriber's outstanding change before the
            // feed ends, or a run that finishes inside its own window loses
            // its final state — for a job's progress feed that is the one
            // value that matters most. `next()` drains the queue ahead of
            // `done`, so a value queued here is still delivered.
            if (sub.pending && !sub.done) {
                try {
                    this.#deliver(sub, sub.ticksOnly ? CHANGE_TICK : this.#snapshot());
                } catch {
                    // Best-effort: a codec that throws here must not take
                    // deactivation down with it.
                }
            }
            this.#closeWindow(sub);
            sub.done = true;
            sub.wake?.();
            this.#subs.delete(sub);
        }
        subs.clear();
    }
}

export { mintCallId };
