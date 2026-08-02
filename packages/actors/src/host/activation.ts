/**
 * One live activation: state signal, method tables, mailbox, timers, and the
 * change feed. Created lazily by the local host on first dispatch; nothing
 * outside this file touches activation memory.
 */
import { effectScope, signal, toRaw, watch } from '@sigx/reactivity';
import { createSharedWatch, watchKey, type SharedWatch } from './watch';
import { mintCallId } from '../call-id';
import { ownFn, warnIfInheritedTable } from '../own-member';
import type { Mailbox } from './mailbox';
import {
    ActorActivationError,
    ActorMethodNotFoundError,
    ActorStateConflictError,
    HostShutdownError,
    isStorageConflict
} from '../errors';
import {
    actorId,
    actorLabel,
    type ActorScheduler,
    type ActorCallContext,
    type ActorClient,
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

/** Reserved dispatch method routing to `onReminder`. */
export const REMINDER_METHOD = '$sigx:reminder';

/** Change-feed buffer bound — drop-oldest beyond this. */
const CHANGE_BUFFER = 16;

/** Trailing-throttle window for a change-driven read (`openWatch`). */
export const DEFAULT_WATCH_THROTTLE_MS = 50;

/** Keys a context extension may never set — they reach the prototype. */
const UNSAFE_CONTEXT_KEYS = new Set(['__proto__', 'constructor', 'prototype']);

/** What the host provides to every activation. */
export interface ActivationHost {
    readonly idleAfterMs: number;
    readonly slowTurnMs: number;
    /**
     * How long deactivation waits for signalled tasks to settle before
     * closing the mailbox, ms. Bounded so a task that ignores its abort
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
    ): ActorClient<D>;
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
    readonly mailbox: Mailbox;

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
    #deepWatchStop: (() => void) | null = null;
    #cancelWriteBehind: (() => void) | null = null;
    #currentCall: ActorCallContext | null = null;
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
    /** Shared watch loops, keyed by `method` + encoded args — see `./watch`. */
    #watches = new Map<string, SharedWatch>();
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
        mailbox: Mailbox
    ) {
        this.ref = ref;
        this.def = def;
        this.#host = host;
        this.mailbox = mailbox;
        this.#scope = effectScope();
        this.stateless = def.__sigxActor.stateless !== undefined;
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
        mailbox: Mailbox
    ): Promise<Activation> {
        const a = new Activation(ref, def, host, mailbox);
        try {
            const opts = def.__sigxActor;
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
            // The `streams:` table is built per SUBSCRIPTION, not here — see
            // `#streamTable`. Its bodies get a derived context of their own:
            // bracketing the dispatch call sites cannot work, because an async
            // generator's return() awaits before resuming, so a `finally`
            // reading ctx.state runs in a later microtask. Handing each body
            // its own context makes both the dev `state` guard and change-feed
            // ownership exact and timing-independent — and no mailbox turn can
            // trip either, since `methods` still gets the real ctx.
            if (opts.persistence && typeof opts.persistence === 'object') {
                a.#ensureDeepWatch();
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
        return this.mailbox.depth === 0 && this.#keepAlive === 0;
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

    /** The identity used in call chains. */
    get id(): string {
        return actorId(this.ref);
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
        return this.mailbox.run(() => this.#turn(method, args, call, enqueuedAt));
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
        const prev = this.#currentCall;
        this.#currentCall = call;
        try {
            return await this.#invoke(method, args);
        } finally {
            this.#currentCall = prev;
        }
    }

    /**
     * Stream dispatch. The setup turn only RESOLVES the generator and takes
     * a keep-alive ref — it must NOT pull the first chunk: a feed like
     * `yield* ctx.changes()` waits for a future turn of this same actor,
     * and holding the mailbox for that pull would self-deadlock. Iteration
     * (including the first pull) is therefore fully detached, and stream
     * bodies get no turn exclusivity by contract — they are observers,
     * reading `ctx.snapshot()` / `ctx.changes()`, never live state. The
     * keep-alive ref makes idle collection skip the activation until the
     * stream ends or the consumer disconnects.
     */
    /**
     * A change-driven READ: the method's result now, and again after every
     * turn that mutated state.
     *
     * Subscriptions with the same `(method, args, throttleMs)` share one
     * loop, so a popular actor costs one re-invocation per turn rather than
     * one per viewer — which matters more here than anywhere else, because
     * the mailbox is single-threaded and those turns would serialise.
     *
     * `throttleMs` is part of the identity because it is part of the
     * behaviour: subscribers asking for different windows want different
     * emission rates, and one loop cannot honour both. Two viewers of the
     * same read share whenever they agree on it — which, since the option
     * is rarely passed, is nearly always.
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
        const id = watchKey(method, throttleMs, this.#host.encodeArgs(args ?? []));
        let shared = this.#watches.get(id);
        if (!shared) {
            shared = createSharedWatch(
                {
                    // A NORMAL mailbox turn, not a privileged read: the
                    // watch gets exactly the isolation every other call has.
                    invoke: () => this.enqueue(method, args, call),
                    changes: () =>
                        (this.#ctx as ActorContext<object>).changes() as AsyncIterable<unknown>,
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
                () => void this.#watches.delete(id)
            );
            this.#watches.set(id, shared);
        }
        // The CALLER's signal, so an abandoned subscription releases even
        // when nothing ever calls `return()` on it — which is the normal
        // case across a host hop, where the serving generator is parked at
        // an `await` and cannot act on a queued `return()` at all.
        return shared.subscribe(call.abortSignal);
    }

    openStream(
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): AsyncIterable<unknown> {
        // Every `ctx.changes()` this body opens, so a disconnect can close
        // them from OUTSIDE the generator — see `#streamContext`.
        const feeds: StreamFeeds = { subs: new Set(), closed: false };
        const setup = this.mailbox.run(async () => {
            if (this.#faulted) throw this.#faulted;
            const fn = ownFn<AnyStreamFn>(this.#streamTable(feeds), method);
            if (!fn) throw new ActorMethodNotFoundError(this.ref.type, method);
            const prev = this.#currentCall;
            this.#currentCall = call;
            this.#keepAlive++;
            try {
                // Async generator bodies are lazy: this runs no user code.
                return fn(...(args as unknown[]))[Symbol.asyncIterator]();
            } catch (error) {
                this.#keepAlive--;
                throw error;
            } finally {
                this.#currentCall = prev;
                this.#afterTurn(Date.now());
            }
        });

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
     * bounded grace to wind down, then close the mailbox, drain queued
     * turns, run onDeactivate, flush a pending write-behind save, tear
     * down.
     *
     * The aborts come FIRST — before the drain, not after it. An abort
     * signal that fires only once the queue has drained is unobservable by
     * exactly the long-running work it exists for: that work is what the
     * drain is waiting on. The mailbox stays open through the task grace so
     * a winding-down task can run a final `turn()` checkpoint.
     */
    async deactivate(reason: DeactivationReason): Promise<void> {
        this.#abort.abort();
        if (this.#tasks.size > 0) {
            for (const run of this.#tasks.values()) run.controller.abort(reason);
            await this.#settleTasks();
        }
        this.mailbox.close();
        await this.mailbox.drain();
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
        // A stale activation must not overwrite the winning state.
        if (reason !== 'conflict' && this.#version > this.#savedVersion && this.#isWriteBehind()) {
            try {
                await this.#doSave();
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
        this.mailbox.close();
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
            const result = await this.#invoke(method, args);
            failed = false;
            return result;
        } finally {
            this.#currentCall = null;
            // The dev slow-turn warning and the observer want the same
            // number, so compute it once and only when someone reads it.
            const elapsed = timing ? performance.now() - startedAt : 0;
            if (__DEV__ && elapsed > this.#host.slowTurnMs) {
                console.warn(
                    `[sigx actors] slow turn: ${actorLabel(this.ref)}.${method}() held the ` +
                        `mailbox for ${elapsed}ms. Awaits inside a turn block every queued ` +
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
                    observer(this.ref, method, queued, elapsed, failed);
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
        if (this.#version > this.#notifiedVersion) {
            this.#notifiedVersion = this.#version;
            if (this.#subs.size > 0) {
                const snap = this.#snapshot();
                for (const sub of this.#subs) {
                    sub.queue.push(snap);
                    if (sub.queue.length > CHANGE_BUFFER) {
                        sub.queue.shift();
                        if (__DEV__ && !this.#warnedDroppedChanges) {
                            this.#warnedDroppedChanges = true;
                            console.warn(
                                `[sigx actors] ${actorLabel(this.ref)} change feed dropped ` +
                                    `snapshots — a stream consumer is slower than the actor's ` +
                                    `mutation rate (buffer: ${CHANGE_BUFFER}).`
                            );
                        }
                    }
                    sub.wake?.();
                }
            }
            if (this.#isWriteBehind() && this.#version > this.#savedVersion) {
                this.#scheduleWriteBehind();
            }
        }
        if (this.#faulted && !this.#faultReported) {
            this.#faultReported = true;
            this.#host.onFault(this);
        } else if (this.#deactivateRequested && this.mailbox.depth <= 1 && !this.#faulted) {
            // depth 1 = only the turn that is settling right now — the
            // queue is empty, so the requested deactivation can begin.
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
            // A system turn — serialized with user turns, so the save always
            // captures a between-turns state, never a mid-turn one.
            this.mailbox.run(() => this.#doSave()).catch(() => {
                // Save failures fault the activation via #doSave; a closed
                // mailbox at deactivation time is handled by the final flush.
            });
        });
    }

    async #doSave(): Promise<void> {
        if (this.#faulted) throw this.#faulted;
        const version = this.#version;
        try {
            this.#etag = await this.#host.saveState(this.ref, toRaw(this.#state), this.#etag);
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

    #ensureDeepWatch(): void {
        if (this.#deepWatchStop) return;
        this.#scope.run(() => {
            const stop = watch(
                () => this.#state,
                () => {
                    this.#version++;
                },
                { deep: true }
            );
            this.#deepWatchStop = () => stop.stop();
        });
    }

    #snapshot(): object {
        return this.#host.cloneState(toRaw(this.#state));
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
     * run detached from the mailbox, so a turn can mutate underneath a live
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
                            `ctx.state. Stream bodies run DETACHED from the mailbox — a turn ` +
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
        run.settled = (async () => {
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
     * CAN await settlement: its grace runs outside any turn.)
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
     * re-enters the mailbox, `changes()` feeds are owned by the run (its
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
                                `mailbox — persistence must go through ctx.turn(), e.g. ` +
                                `await ctx.turn((c) => c.${member}()).`
                        )
                    ),
                enumerable: true,
                configurable: true
            });
        }
        Object.defineProperty(derived, 'turn', {
            value: <T,>(fn: (ctx: ActorContext<object>) => T | Promise<T>): Promise<T> =>
                self.mailbox.run(async () => {
                    if (self.#faulted) throw self.#faulted;
                    const started = Date.now();
                    const prev = self.#currentCall;
                    // The run's one callId, so ctx.actor() calls made inside
                    // the turn carry a chain and traces correlate the run.
                    self.#currentCall = { callChain: [self.id], callId: run.callId };
                    try {
                        return await fn(self.#ctx);
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
                            `ctx.state. Task bodies run DETACHED from the mailbox — a turn ` +
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
        const base: ActorContext<object> = {
            ref: this.ref,
            key: this.ref.key,
            get state() {
                return self.#state;
            },
            async save(): Promise<void> {
                // Explicit-mode saves don't need the deep watch; bump the
                // version so savedVersion bookkeeping stays consistent.
                self.#version++;
                await self.#doSave();
            },
            async clearState(): Promise<void> {
                await self.#host.clearStoredState(self.ref, self.#etag);
                self.#etag = null;
                const fresh = opts.state(self.ref.key) as Record<string, unknown>;
                const live = self.#state as Record<string, unknown>;
                // Reset in place — the proxy identity is captured by closures.
                for (const k of Object.keys(live)) {
                    if (!(k in fresh)) delete live[k];
                }
                Object.assign(live, fresh);
            },
            timer(name: string, cb: () => void | Promise<void>, options: TimerOptions): TimerHandle {
                self.#timers.get(name)?.clear();
                let queued = false;
                const tick = () => {
                    // Coalesce: a tick behind a slow turn must not pile up.
                    if (queued) return;
                    queued = true;
                    self.mailbox
                        .run(async () => {
                            queued = false;
                            if (self.#faulted) return;
                            const started = Date.now();
                            const prev = self.#currentCall;
                            self.#currentCall = {
                                callChain: [self.id],
                                callId: mintCallId()
                            };
                            try {
                                await cb();
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
            actor<D extends AnyActorDefinition>(def: D, key: string): ActorClient<D> {
                // The outbound context appends SELF to the chain — that is
                // what lets the target detect A→B→A cycles.
                return self.#host.actorClient(def, key, () => {
                    const current = self.#currentCall;
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
                const current = self.#currentCall;
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
            snapshot(): object {
                return self.#snapshot();
            },
            changes(options?: { initial?: boolean }): AsyncIterable<object> {
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
        options: { initial?: boolean } | undefined,
        feeds?: StreamFeeds
    ): AsyncIterable<object> {
        const sub: ChangeSub = { queue: [], wake: null, done: false };
        if (feeds?.closed) {
            // The consumer went away before the body got this far. Hand it a
            // spent feed — no seed, no registration — so the body unwinds on
            // its first pull instead of parking on a subscription nothing
            // will ever close.
            sub.done = true;
            return this.#changeFeed(sub);
        }
        // The feed needs change detection even in explicit mode.
        this.#ensureDeepWatch();
        // Seed BEFORE the subscription goes live, in this same synchronous
        // call: a `yield ctx.snapshot()` prologue would instead subscribe
        // only after the consumer resumes, losing every mutation in that
        // window.
        if (options?.initial) sub.queue.push(this.#snapshot());
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

    /**
     * Close subscriptions from outside their consumer: mark done, wake a
     * parked `next()`, and drop them from the activation's set. Safe to pass
     * `#subs` itself — deleting the current element mid-iteration is fine.
     */
    #closeSubs(subs: Set<ChangeSub>): void {
        for (const sub of subs) {
            sub.done = true;
            sub.wake?.();
            this.#subs.delete(sub);
        }
        subs.clear();
    }
}

export { mintCallId };
