/**
 * Shared actor types — the dispatch seam, storage contract, and definition
 * shapes. Split from index.ts so the silo, wire, and client entries can
 * import types without pulling the public API module in.
 */
import type { ServerFnGuard } from '@sigx/server';
import type { ActorOwnerHint } from './errors';

/** Identity of one virtual actor. Serializable; never holds memory. */
export interface ActorRef {
    readonly type: string;
    readonly key: string;
}

/** `type\u0000key` — NUL separator so real keys can contain `/` or `:`. */
export function actorId(ref: ActorRef): string {
    return `${ref.type}\u0000${ref.key}`;
}

/** Human-readable identity for error messages. */
export function actorLabel(ref: ActorRef): string {
    return `${ref.type}/${ref.key}`;
}

/**
 * Per-call metadata riding EVERY dispatch. Serializable by construction
 * (minus `abortSignal`, which a remote transport maps to fetch abort) so a
 * future remote placement can forward it and reentrancy detection keeps
 * working across hops.
 */
export interface ActorCallContext {
    /**
     * Chain of `actorId()` identities from the original external call to
     * here. Empty for external calls; each actor-to-actor hop appends the
     * caller. Drives deadlock detection and call-chain reentrancy.
     */
    readonly callChain: readonly string[];
    /** Correlates one logical request across hops. */
    readonly callId: string;
    /** Absolute epoch-ms deadline; local dispatch races the caller against it. */
    readonly deadline?: number;
    /** Cooperative cancellation from the outermost caller. NOT serialized. */
    readonly abortSignal?: AbortSignal;
}

/**
 * THE dispatch seam. The wire endpoint, `actor()`, `ctx.actor()`, timers and
 * reminders all go through this and only this — nothing outside an
 * activation ever touches activation memory. That invariant is what lets a
 * remote placement (Durable Objects, a cluster directory) replace the local
 * host without any public API change.
 */
export interface ActorDispatcher {
    dispatch(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): Promise<unknown>;
    /**
     * Async-generator (`streams:`) methods. Optional: a transport that
     * cannot stream simply doesn't declare it and the runtime rejects with
     * a descriptive error.
     */
    dispatchStream?(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): AsyncIterable<unknown>;
    /**
     * A change-driven READ: the method's result now, and again after every
     * turn that mutated state. Optional for the same reason as
     * `dispatchStream` — a transport that cannot stream simply omits it.
     *
     * Distinct from `dispatchStream` because it is not a user-authored
     * generator: the runtime re-invokes an ordinary read method, which is
     * what lets a subscriber ask for `total()` rather than for state it
     * would have to re-derive itself.
     */
    dispatchWatch?(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext,
        options?: { throttleMs?: number }
    ): AsyncIterable<unknown>;
}

/**
 * Observes each completed turn, splitting the two things a dispatch
 * middleware cannot tell apart: `queuedMs`, how long the message waited for
 * the mailbox, and `elapsedMs`, how long it then HELD the mailbox. That
 * split is the difference between "this grain is slow" (elapsed) and "this
 * grain is hot" (queued), which are opposite problems with opposite fixes —
 * and only the activation knows both, since a middleware sees just the sum.
 *
 * Positional parameters, not an options object: this runs on every turn, and
 * an object literal per turn would be an allocation on the hot path.
 *
 * Fires for DISPATCHED turns only — the ones a caller waited for, including
 * reminder delivery (as `$sigx:reminder`). Volatile `ctx.timer` ticks and
 * write-behind flushes are excluded: they have no caller and no queue wait
 * of their own, and their cost is already visible as queue wait on whatever
 * was behind them. Reentrant (`ctx.actor` A→B→A) calls run inline against
 * the caller's turn and are excluded too, since the outer turn already
 * covers that time.
 *
 * Called from a `finally`. Throwing from here is swallowed (dev-logged) —
 * an observer must never be able to fail a turn.
 *
 * `failed` means THE METHOD INVOCATION THREW, which is deliberately narrower
 * than "the caller saw an error": the runtime's own post-turn bookkeeping
 * (change fan-out, write-behind scheduling) runs after this and could in
 * principle fail on its own, and such a turn is reported as succeeded. The
 * narrow meaning is the one an actor author can act on; widening it is
 * tracked separately rather than left ambiguous here.
 */
export type ActorTurnObserver = (
    ref: ActorRef,
    method: string,
    queuedMs: number,
    elapsedMs: number,
    failed: boolean
) => void;

/**
 * A per-actor-type placement strategy — Orleans's `[PlacementStrategy]`
 * attribute, declared ON the actor rather than in a central map.
 *
 * Deliberately opaque here: choosing a host needs a membership view, which
 * is a cluster concept, and core must not depend on `./cluster`. Cluster
 * narrows this to `PlacementPolicy` (adding `choose()`); the single-node
 * host has one silo and ignores it. That keeps the declaration next to the
 * actor while the algorithm stays in the layer that can implement it.
 */
export interface ActorPlacementStrategy {
    /**
     * Diagnostic name, e.g. `'prefer-local'` — surfaced in placement
     * warnings. Optional on purpose: a custom strategy should not have to
     * carry boilerplate that only exists for logging.
     */
    readonly name?: string;
    /**
     * Which placement backend understands this strategy, e.g. `'cluster'`.
     *
     * This exists because the type here is deliberately OPAQUE — choosing a
     * host needs a membership view, which is a cluster concept, and core must
     * not depend on `./cluster`. That opacity is load-bearing (it is what lets
     * a Durable Objects backend define its own strategies) but it left a
     * backend unable to tell two very different things apart: a strategy meant
     * for someone ELSE, which must be ignored silently, and one meant for IT
     * but malformed, which must fail.
     *
     * With the tag a backend distinguishes three cases instead of one, so a
     * missing `choose()` stops being a dev-only warning and a silent
     * misplacement in production. Set by each backend's own factories; a
     * hand-written strategy may omit it, and is then judged on shape alone.
     */
    readonly backend?: string;
}

/**
 * Where a grain currently lives — the answer to `ActorPlacement.locate()`.
 *
 * A discriminated union rather than a nullable owner, so "it is here" cannot
 * be confused with "I don't know". "I don't know" is `undefined` INSTEAD of
 * an `ActorLocation` — either by not implementing `locate()` or by returning
 * undefined from it, which a composing placement has to be able to do.
 */
export type ActorLocation =
    | { readonly local: true }
    | { readonly local: false; readonly owner: ActorOwnerHint };

/**
 * Placement: given a ref, WHO dispatches it. The single-node provider
 * always answers "the local host"; a Durable Objects provider answers with
 * a stub whose dispatch() is a fetch to the DO. Sync-or-promise so the
 * local fast path stays allocation-free.
 */
export interface ActorPlacement {
    dispatcherFor(ref: ActorRef): ActorDispatcher | Promise<ActorDispatcher>;
    /**
     * Where this grain lives, WITHOUT dispatching and WITHOUT activating —
     * what a mount asks before deciding to redirect rather than proxy.
     *
     * A placement that cannot answer either omits this or returns
     * `undefined`, and every mount then falls back to proxying. Both, rather
     * than only the first, because a COMPOSING placement has to define the
     * method to forward it and only discovers at call time whether the inner
     * one implements it. Sync-or-promise so a placement holding the claim
     * can answer without allocating.
     *
     * The answer is a HINT and is allowed to be stale — by the time the
     * caller acts on it the grain may have moved. That is safe because the
     * directory, not this, is the arbiter of single-activation.
     */
    locate?(ref: ActorRef): ActorLocation | Promise<ActorLocation> | undefined;
    /**
     * Called once by `createSilo` with the silo's own local dispatcher and
     * the silo itself, before `start()`. A distributed placement returns
     * bindings hooking the activation lifecycle (directory claims) and the
     * reminder tick; the default local placement doesn't implement it.
     */
    bind?(local: ActorDispatcher, silo: Silo): PlacementBindings | void;
    start?(): void | Promise<void>;
    /**
     * Called by `silo.stop()` BEFORE the drain begins — a cluster placement
     * announces `leaving` here so peers stop placing new actors on this
     * silo while it hands its activations off.
     */
    beginStop?(): void | Promise<void>;
    stop?(): void | Promise<void>;
}

/**
 * What a placement's `bind()` hands back to the silo. Every member is
 * optional; omitting all of them is the single-node behavior.
 */
export interface PlacementBindings {
    /**
     * Runs inside the activation reserve, after the definition resolves and
     * before any state loads — the distributed directory's claim point.
     * Throwing (e.g. a wrong-host error carrying the winning owner) refuses
     * the activation: every parked caller rejects and nothing is remembered.
     */
    beforeActivate?(ref: ActorRef): void | Promise<void>;
    /**
     * Runs after an activation is fully deactivated and forgotten — the
     * claim release point. Errors are swallowed (dev-logged): a failed
     * release must not break callers parked on re-activation. Not called
     * for activations force-dropped at the shutdown deadline.
     */
    afterDeactivate?(ref: ActorRef, reason: DeactivationReason): void | Promise<void>;
    /**
     * Cluster posture for a call-chain hit on a REENTRANT actor with no
     * local activation: the chain proves the target is mid-turn somewhere,
     * so activating a second copy here would violate single-activation.
     * `true` = throw a retryable deadlock error; default (single-node)
     * falls back to a normal dispatch.
     */
    strictChainPresence?: boolean;
    /**
     * Durable-reminder shard ownership — the reminder table is split into
     * fixed hash shards and each tick a silo processes only the shards it
     * owns (a cluster answers via rendezvous hashing over the membership
     * view, spreading reminder load; every silo still mutates any shard).
     * Default: own every shard.
     */
    ownsReminderShard?(shard: string): boolean | Promise<boolean>;
    /**
     * The deactivation reason a graceful `silo.stop()` uses. A cluster
     * placement answers `'migrated'`: the stop is a HANDOFF — claims are
     * released as activations drain and peers re-place them — not the end
     * of the actor system. Default `'shutdown'`.
     */
    stopReason?: Extract<DeactivationReason, 'shutdown' | 'migrated'>;
}

/**
 * The clock seam for BACKGROUND work — the idle sweeper, the reminder tick,
 * `ctx.timer`, and write-behind flushes. Those are the jobs that must keep
 * running between requests, so they are the ones a runtime has to be able
 * to redirect.
 *
 * Deliberately NOT everything that touches a timer: call deadlines and the
 * shutdown drain (`local-host.ts`) stay on host timers, because they are
 * scoped to a request or a stop that is already in flight and work as-is
 * wherever that request runs.
 *
 * The default (`timerScheduler()`) uses the host timers. A runtime with no
 * background execution replaces it: a Cloudflare Worker only runs while
 * handling a request, so a `setInterval` registered at startup never fires
 * and a Durable Object drives the same work from alarms instead.
 *
 * Both methods return a cancel function; cancelling twice is a no-op.
 */
export interface ActorScheduler {
    /** Run `tick` every `intervalMs`. */
    every(intervalMs: number, tick: () => void): () => void;
    /** Run `run` once, after `delayMs`. */
    after(delayMs: number, run: () => void): () => void;
}

// ---------------------------------------------------------------------------
// Storage

export interface ActorStorageRecord {
    /** Codec-encoded (JSON-safe) state as `@sigx/serialize` produced it. */
    state: unknown;
    etag: string;
}

/**
 * Pluggable persistence with optimistic concurrency. `expectedEtag: null`
 * means "no stored record yet" (first save). A mismatch must throw the
 * `ActorStorageConflict` brand — the runtime translates it into
 * `ActorStateConflictError` and discards the stale activation.
 */
export interface ActorStorage {
    load(type: string, key: string): Promise<ActorStorageRecord | null>;
    save(type: string, key: string, state: unknown, expectedEtag: string | null): Promise<string>;
    clear(type: string, key: string, expectedEtag: string | null): Promise<void>;
}

/**
 * What the silo hands a reminder implementation at bind time — everything
 * it would otherwise have to be told twice (and could be told wrongly).
 */
export interface ActorRemindersContext {
    /** The silo's storage, AFTER any plugin decorators. */
    readonly storage: ActorStorage;
    /** The silo's clock, so reminder ticks are drivable like everything else. */
    readonly scheduler: ActorScheduler;
    /** Requested tick cadence, ms (`SiloDefaults.reminderTickMs`). */
    readonly tickMs: number;
    /**
     * Does THIS silo own the given reminder shard? A cluster answers via
     * rendezvous hashing so N silos split the load; single-node owns all.
     * Meaningless to an implementation that does not shard.
     */
    ownsShard(shard: string): boolean | Promise<boolean>;
    /** Deliver a due reminder to its actor, activating it if idle. */
    deliver(ref: ActorRef, name: string): Promise<unknown>;
}

/**
 * Durable reminders, as a seam.
 *
 * The default (`shardedReminders()`) keeps the table in `ActorStorage` under
 * a reserved type, split into fixed hash shards that silos divide between
 * them — which assumes MANY actors per silo. A runtime where that is false
 * replaces it: under Cloudflare's one-Durable-Object-per-actor model each
 * actor's reminders live in its own DO and fire from its own alarm, so
 * there is nothing to shard and nothing to poll.
 *
 * `bind()` runs once before `start()`, mirroring `ActorPlacement.bind()`.
 */
export interface ActorReminders {
    bind(context: ActorRemindersContext): void;
    start(): void | Promise<void>;
    stop(): void | Promise<void>;
    /** The per-actor API behind `ctx.reminders`. */
    apiFor(ref: ActorRef): ReminderApi;
}

// ---------------------------------------------------------------------------
// Definition

export interface TimerOptions {
    /** Delay to first tick, ms. */
    due: number;
    /** Repeat period, ms; omit for one-shot. */
    period?: number;
    /**
     * Whether ticks count as activity for idle collection. Default false —
     * Orleans posture: timers don't keep grains alive.
     */
    keepAlive?: boolean;
}

export interface TimerHandle {
    cancel(): void;
}

export interface ReminderApi {
    /**
     * Register or overwrite a durable reminder. Coarse resolution: fires at
     * or after `due` ms from now, re-activating the actor if idle. Minimum
     * `period` is 60s (enforced) — anything tighter is a timer's job.
     */
    set(name: string, opts: { due: number; period?: number }): Promise<void>;
    clear(name: string): Promise<void>;
    list(): Promise<string[]>;
}

/** One running detached task, as `ctx.tasks.list()` reports it. */
export interface TaskInfo {
    name: string;
    /** Epoch-ms this run first started. */
    startedAt: number;
    /**
     * Times the runtime re-started the run after a deactivation or crash
     * (0 on a fresh start). At-least-once: a restarted body replays from
     * its own checkpointed state.
     */
    restarts: number;
}

/**
 * Detached long-running work — `ctx.tasks`. A task runs OUTSIDE the mailbox
 * (reads interleave while it works) and holds a keep-alive ref so idle
 * collection skips the grain. State access happens only through the task
 * context's `turn()`, which is an ordinary serialized mailbox turn.
 */
export interface TaskApi {
    /**
     * Start the named task from the definition's `tasks:` table. Resolves
     * once the run is launched — NOT when it finishes. Single-flight per
     * name: starting an already-running task is a no-op. A task that throws
     * is terminal (it is not restarted); a cancelled task ends with its
     * abort reason.
     */
    start(name: string, input?: unknown): Promise<void>;
    /**
     * REQUEST cancellation: abort the run's signal (reason `'cancelled'`)
     * and return. Deliberately not a join — awaiting settlement from a
     * method turn would deadlock with the task's own wind-down `turn()`.
     * The run leaves `list()` once its body settles; restart the name only
     * after it has.
     */
    cancel(name: string): Promise<void>;
    /** The tasks currently running on this activation. */
    list(): readonly TaskInfo[];
}

/** `'migrated'` is reserved for cluster rebalancing — not yet emitted. */
export type DeactivationReason =
    | 'idle'
    | 'explicit'
    | 'shutdown'
    | 'conflict'
    | 'activation-failed'
    | 'migrated';

/**
 * The built-in half of the per-activation context — created once per
 * activation and closed over by the `methods`/`streams` factories.
 * Activation-scoped (not call-scoped), which is why it is a closure and not
 * a first parameter.
 *
 * Actors see `ActorContext` (below), which is this plus whatever the app's
 * plugins contribute.
 */
export interface ActorContextBase<S extends object> {
    readonly ref: ActorRef;
    readonly key: string;
    /**
     * Actor state as a deep sigx signal proxy — mutate it directly
     * (`ctx.state.count++`). `computed`/`watch` created in the `methods`
     * factory work against it and are disposed with the activation.
     */
    readonly state: S;
    /**
     * Persist now (Orleans WriteStateAsync). Resolves when stored; throws
     * `ActorStateConflictError` on an etag mismatch, which also discards
     * this activation.
     */
    save(): Promise<void>;
    /** Delete the stored record; in-memory state resets to `state(key)`. */
    clearState(): Promise<void>;
    /** Volatile timer — dies with the activation; ticks are mailbox turns. */
    timer(name: string, cb: () => void | Promise<void>, opts: TimerOptions): TimerHandle;
    /** Durable reminders — survive deactivation, re-activate the actor. */
    readonly reminders: ReminderApi;
    /** Typed client for another actor; carries the call chain. */
    actor<D extends AnyActorDefinition>(def: D, key: string): ActorClient<D>;
    /** Orleans DeactivateOnIdle: finish the queue, then deactivate. */
    deactivate(): void;
    /**
     * Aborts when this activation begins deactivating (any reason,
     * including silo shutdown) — BEFORE the mailbox drain, so long-running
     * work can observe it and wind down inside the drain window.
     */
    readonly abortSignal: AbortSignal;
    /** Detached long-running work declared in the `tasks:` section. */
    readonly tasks: TaskApi;
    /** Deep, detached copy of the current state (safe outside the mailbox). */
    snapshot(): S;
    /**
     * Change feed: yields a `snapshot()` after every turn that mutated
     * state. Bounded buffer (drop-oldest); made for `streams:` methods,
     * which must not touch live state after their setup turn returns.
     *
     * `{ initial: true }` queues the CURRENT snapshot as the first value,
     * synchronously, in the same call that registers the subscription — so
     * `yield* ctx.changes({ initial: true })` has no gap. Prefer it to the
     * `yield ctx.snapshot()` prologue, which subscribes only after the
     * consumer resumes past that first yield and therefore loses every
     * mutation in between.
     */
    changes(options?: { initial?: boolean }): AsyncIterable<S>;
}

/**
 * What a `methods`/`streams` factory receives: the built-in members plus
 * `Ext`, the members this app's plugins contribute
 * (`PluginRegistry.extendContext`).
 *
 * `Ext` is threaded from `ActorApp` through the app-bound `defineActor`
 * (`defineActorApp(...).defineActor`), so a plugin's additions are typed
 * inside every actor that imports it — no global declaration merging, and
 * the types stay per-app rather than per-process.
 */
export type ActorContext<
    S extends object,
    Ext extends object = Record<never, never>
> = ActorContextBase<S> & Ext;

export type ActorMethod = (...args: never[]) => unknown;
export type ActorMethodTable = Record<string, ActorMethod>;
export type ActorStreamMethod = (...args: never[]) => AsyncIterable<unknown>;
export type ActorStreamTable = Record<string, ActorStreamMethod>;
export type ActorTask = (input: never) => void | Promise<void>;
export type ActorTaskTable = Record<string, ActorTask>;

/**
 * What a `tasks:` body sees. Tasks run DETACHED from the mailbox, so the
 * live-state members are typed away: no `state`, no `save()`, no
 * `clearState()`. State access goes through `turn()` — one ordinary
 * serialized mailbox turn with the full context — and reads through
 * `snapshot()` / `changes()`. `abortSignal` is the RUN's own signal: it
 * fires on `ctx.tasks.cancel()` (reason `'cancelled'`) and on deactivation
 * (reason: the `DeactivationReason`), before the mailbox drain, so a task
 * can run a final `turn()` checkpoint while winding down.
 */
export type ActorTaskContext<
    S extends object,
    Ext extends object = Record<never, never>
> = Omit<ActorContextBase<S>, 'state' | 'save' | 'clearState' | 'abortSignal'> & {
    /** Re-enter the mailbox: run `fn` as one ordinary serialized turn. */
    turn<T>(fn: (ctx: ActorContext<S, Ext>) => T | Promise<T>): Promise<T>;
    /** This run's signal — see the type doc. */
    readonly abortSignal: AbortSignal;
} & Ext;

export interface ActorOptions<
    S extends object,
    M extends ActorMethodTable,
    St extends ActorStreamTable,
    Ext extends object = Record<never, never>
> {
    /**
     * Stable type id — the actor's wire, directory, and storage name.
     * A string literal (the build transform reads it statically); renaming
     * it is a wire and storage break.
     */
    type: string;
    /**
     * Transport-independent guard chain, run for every method on every
     * transport (wire and in-process), OUTSIDE the mailbox. Required unless
     * `unguarded: true` when the build gate is on (its default).
     */
    use?: readonly ServerFnGuard[];
    /** The explicit opt-out word for a public actor. */
    unguarded?: boolean;
    /** Per-method guard chains, run after `use`. Static map — the method
     *  table itself is per-activation and cannot carry wire metadata. */
    methodUse?: Record<string, readonly ServerFnGuard[]>;
    /** Initial state factory — used when storage has no record (the
     *  virtual-actor "always exists" default). */
    state: (key: string) => S;
    /**
     * `'explicit'` (default): only `ctx.save()` writes. `'write-behind'`:
     * a deep watch schedules a debounced save; acked ≠ persisted — use only
     * for lossy-tolerant state.
     */
    persistence?: 'explicit' | { mode: 'write-behind'; debounceMs?: number };
    /** Call-chain reentrancy. Default false: A→B→A throws ActorDeadlockError. */
    reentrant?: boolean;
    /** Idle collection age for this type; overrides the silo default. */
    idleAfterMs?: number;
    /**
     * Where NEW activations of this type go — Orleans's placement attribute
     * (`consistentHashPolicy()`, `preferLocalPolicy()`, or your own). Read by
     * a cluster placement when it resolves a target, and it WINS over the
     * central `typePolicies` map; ignored single-node.
     */
    placement?: ActorPlacementStrategy;
    /** Runs before the first message; throwing fails all queued callers. */
    onActivate?(ctx: ActorContext<S, Ext>): void | Promise<void>;
    /** Runs after the queue drains, before state teardown. */
    onDeactivate?(ctx: ActorContext<S, Ext>, reason: DeactivationReason): void | Promise<void>;
    /** Durable-reminder callback. */
    onReminder?(ctx: ActorContext<S, Ext>, name: string): void | Promise<void>;
    /**
     * The method-table factory — called once per ACTIVATION, closing over
     * ctx. Free to create `computed`/`watch` at construction; they die with
     * the activation.
     */
    methods: (ctx: ActorContext<S, Ext>) => M;
    /**
     * Stream-method factory. Each entry runs its body as ONE mailbox turn
     * and must return an async iterable that does NOT touch live state —
     * use `ctx.snapshot()` / `ctx.changes()`. Unlike `methods`, this
     * factory must not touch ctx during construction: its keys are
     * enumerated at definition time (for wire routing) with an inert probe.
     */
    streams?: (ctx: ActorContext<S, Ext>) => St;
    /**
     * Detached long-running work, started via `ctx.tasks.start(name,
     * input?)`. A task body runs OUTSIDE the mailbox — other calls
     * interleave while it works — and touches state only through the task
     * context's `turn()`. The factory is called once per RUN with that
     * run's derived context (its own `abortSignal`), and must be a pure
     * table constructor. No wire surface: start/cancel go through your own
     * methods, so the guard chain governs them.
     */
    tasks?: (ctx: ActorTaskContext<S, Ext>) => ActorTaskTable;
}

export interface ActorDefinition<
    S extends object = object,
    M extends ActorMethodTable = ActorMethodTable,
    St extends ActorStreamTable = ActorStreamTable
> {
    readonly type: string;
    /** Stream-method names, enumerated at definition time. */
    readonly streamNames: readonly string[];
    /**
     * @internal the raw options — the silo's activation hook. The context
     * extension is erased to `any` here: a definition built against an app's
     * `Ext` must still be a plain `ActorDefinition`, and the silo only ever
     * *calls* these factories with the context it built.
     */
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    readonly __sigxActor: ActorOptions<S, M, St, any>;
}

// `any` variants: `never[]`-typed parameters make the table types invariant
// enough that concrete definitions won't assign to the defaulted shape.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AnyActorDefinition = ActorDefinition<any, any, any>;

/** Per-call options for `actor(...).with()`, mirroring `fn.with()`. */
export interface ActorCallOptions {
    signal?: AbortSignal;
    /** Extra request headers (wire transport only). */
    headers?: Record<string, string>;
    /** Explicit server context for in-process calls (`fn.with({ context })`). */
    context?: unknown;
}

/** What callers see: methods promise-wrapped, streams as AsyncIterable. */
export type ActorClient<D> = D extends ActorDefinition<infer _S, infer M, infer St>
    ? {
          [K in keyof M]: M[K] extends (...a: infer A) => infer R
              ? (...a: A) => Promise<Awaited<R>>
              : never;
      } & {
          [K in keyof St]: St[K] extends (...a: infer A) => AsyncIterable<infer T>
              ? (...a: A) => AsyncIterable<T>
              : never;
      }
    : never;

export type ActorClientWith<D> = ActorClient<D> & {
    /** Bind per-call options; returns the same client shape. */
    with(options?: ActorCallOptions): ActorClient<D>;
};

/**
 * The names of a definition's `methods` — what `useActorState` reads and
 * `useActorAction` runs. Stream methods are deliberately excluded: they
 * return an `AsyncIterable`, not a value a data key can hold.
 */
export type ActorReadName<D> = D extends ActorDefinition<infer _S, infer M, infer _St>
    ? keyof M & string
    : never;

/** The parameters of one of a definition's methods. */
export type ActorArgs<D, M extends PropertyKey> = D extends ActorDefinition<
    infer _S,
    infer T,
    infer _St
>
    ? M extends keyof T
        ? T[M] extends (...a: infer A) => unknown
            ? A
            : never
        : never
    : never;

/** The awaited result of one of a definition's methods. */
export type ActorResult<D, M extends PropertyKey> = D extends ActorDefinition<
    infer _S,
    infer T,
    infer _St
>
    ? M extends keyof T
        ? // Same `(...a: infer _A)` shape as ActorArgs on purpose: a
          // `never[]` parameter list also matches (never is the bottom
          // type, verified for both method-shorthand and arrow-property
          // declarations), but having the two helpers read identically
          // keeps the question from being asked again.
          T[M] extends (...a: infer _A) => infer R
            ? Awaited<R>
            : never
        : never
    : never;

// ---------------------------------------------------------------------------
// The silo seam contract (what the wire layer and `actor()` consume)

export interface SiloStats {
    activations: number;
    queued: number;
    perType: Record<string, number>;
    /**
     * Slots mid-activation and mid-deactivation. NOT counted in
     * `activations`, which only sees settled ones — so a silo in an
     * activation storm reads as idle without these, which is precisely when
     * you are looking at it.
     */
    transitional: { activating: number; deactivating: number };
}

/**
 * Resolve a caller-supplied cap to a whole, non-negative, FINITE number.
 *
 * `Math.max(0, Math.floor(x))` is the obvious spelling and it has a hole:
 * `Math.floor(NaN)` is `NaN`, `Math.max(0, NaN)` is `NaN`, and every
 * subsequent comparison against `NaN` is false — so `limit === 0` misses,
 * `rows.length > limit` misses, and a bound meant to protect a walk over
 * millions of activations silently becomes no bound at all. A cap that
 * fails OPEN is worse than no cap, because nothing looks wrong until the
 * one call that matters.
 *
 * Non-finite input therefore falls back to the default rather than being
 * honoured: `Infinity` reads as "no limit", which is a request this API
 * deliberately does not offer.
 */
export function resolveLimit(value: number | undefined, fallback: number): number {
    if (value === undefined || !Number.isFinite(value)) return fallback;
    return Math.max(0, Math.floor(value));
}

/**
 * What a silo that is not running reports. A FACTORY, not a shared frozen
 * constant: this lands in an ops snapshot and a `SiloReport`, both of which
 * a caller may reasonably transform in place.
 */
export const emptySiloStats = (): SiloStats => ({
    activations: 0,
    queued: 0,
    perType: {},
    transitional: { activating: 0, deactivating: 0 }
});

/** One live activation, as `Silo.activations()` reports it. */
export interface ActivationInfo {
    type: string;
    key: string;
    /** Mailbox depth: queued turns plus the one running. */
    queued: number;
    /** Since activation, ms. Monotonic. */
    ageMs: number;
    /** Since the last turn, ms. Wall clock — this is what idle collection reads. */
    idleMs: number;
    /** Held open by a stream or an explicit keep-alive, so idle sweeping skips it. */
    keptAlive: boolean;
}

export interface ActivationsOptions {
    /**
     * How many to return. Default 100.
     *
     * Bounded on purpose, and low: a silo can hold millions of activations,
     * and this walks them all to sort. It is a "top N" view, not an export.
     */
    limit?: number;
    /**
     * `'queued'` (deepest mailbox first) — the hot grains.
     * `'age'` (oldest first) — the long-lived ones.
     * `'idle'` (most idle first) — the next sweep's candidates.
     * Default `'queued'`.
     */
    sortBy?: 'queued' | 'age' | 'idle';
    /** Only this actor type. */
    type?: string;
}

export interface Silo extends ActorDispatcher {
    /** Definition lookup — the wire resolver's 404 authority. May load lazily. */
    definition(type: string): AnyActorDefinition | Promise<AnyActorDefinition | null> | null;
    /**
     * Where a grain lives, without dispatching or activating — delegates to
     * the placement's `locate()`.
     *
     * `undefined` means "cannot answer", which is what a single-node silo
     * and any placement without `locate()` return. Callers must treat that
     * as "assume local / just dispatch", never as an error — one check
     * covers both a placement that opts out and a silo built before the
     * seam existed.
     *
     * The endpoint uses this to answer 421 with the owner instead of
     * proxying; nothing else should need it, because dispatching already
     * routes correctly on its own.
     */
    locate?(ref: ActorRef): ActorLocation | Promise<ActorLocation> | undefined;
    actor<D extends AnyActorDefinition>(def: D, key: string): ActorClientWith<D>;
    /** Starts sweeper + reminders and stamps the silo seam. Idempotent. */
    start(): Promise<void>;
    /** Drain, flush, clear the seam. Default timeout 30s. */
    stop(opts?: { timeoutMs?: number }): Promise<void>;
    /**
     * Gracefully deactivate ONE activation (drain its mailbox, flush,
     * forget). No-op if the actor isn't active here. `reason` defaults to
     * `'explicit'`; a cluster rebalancer passes `'migrated'`.
     */
    deactivate(ref: ActorRef, reason?: DeactivationReason): Promise<void>;
    /** Deactivate every activation of one type (dev/HMR hook). */
    deactivateType(type: string): Promise<void>;
    stats(): SiloStats;
    /**
     * The live activations themselves, bounded and sorted — what `stats()`
     * collapses into counts.
     *
     * This is the "top grains" view: which keys are hot, which are old,
     * which are about to be swept. Without it a dashboard can say a silo
     * holds 12,000 activations with 400 queued turns and cannot say WHERE,
     * which is the only question worth asking at that point.
     *
     * Walks the directory, so it costs O(activations) and allocates one
     * record per candidate — hence the default limit of 100. Poll it at
     * human rates, not per request.
     */
    activations(options?: ActivationsOptions): readonly ActivationInfo[];
    /**
     * Observe every dispatched turn; returns an unsubscribe.
     *
     * When the LAST observer unsubscribes the runtime stops taking the
     * per-turn timestamps altogether, so observation can be switched on for
     * an investigation and off again without leaving a permanent cost
     * behind. That is the difference between disabling the work and merely
     * discarding its results.
     */
    observeTurns(observer: ActorTurnObserver): () => void;
}
