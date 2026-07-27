/**
 * Shared actor types — the dispatch seam, storage contract, and definition
 * shapes. Split from index.ts so the silo, wire, and client entries can
 * import types without pulling the public API module in.
 */
import type { ServerFnGuard } from '@sigx/server';

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
}

/**
 * Placement: given a ref, WHO dispatches it. The single-node provider
 * always answers "the local host"; a Durable Objects provider answers with
 * a stub whose dispatch() is a fetch to the DO. Sync-or-promise so the
 * local fast path stays allocation-free.
 */
export interface ActorPlacement {
    dispatcherFor(ref: ActorRef): ActorDispatcher | Promise<ActorDispatcher>;
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

/** `'migrated'` is reserved for cluster rebalancing — not yet emitted. */
export type DeactivationReason =
    | 'idle'
    | 'explicit'
    | 'shutdown'
    | 'conflict'
    | 'activation-failed'
    | 'migrated';

/**
 * The per-activation context — created once per activation and closed over
 * by the `methods`/`streams` factories. Activation-scoped (not call-scoped),
 * which is why it is a closure and not a first parameter.
 */
export interface ActorContext<S extends object> {
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
    /** Aborts on silo shutdown — long-running work should observe it. */
    readonly abortSignal: AbortSignal;
    /** Deep, detached copy of the current state (safe outside the mailbox). */
    snapshot(): S;
    /**
     * Change feed: yields a `snapshot()` after every turn that mutated
     * state. Bounded buffer (drop-oldest); made for `streams:` methods,
     * which must not touch live state after their setup turn returns.
     */
    changes(): AsyncIterable<S>;
}

export type ActorMethod = (...args: never[]) => unknown;
export type ActorMethodTable = Record<string, ActorMethod>;
export type ActorStreamMethod = (...args: never[]) => AsyncIterable<unknown>;
export type ActorStreamTable = Record<string, ActorStreamMethod>;

export interface ActorOptions<
    S extends object,
    M extends ActorMethodTable,
    St extends ActorStreamTable
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
    /** Runs before the first message; throwing fails all queued callers. */
    onActivate?(ctx: ActorContext<S>): void | Promise<void>;
    /** Runs after the queue drains, before state teardown. */
    onDeactivate?(ctx: ActorContext<S>, reason: DeactivationReason): void | Promise<void>;
    /** Durable-reminder callback. */
    onReminder?(ctx: ActorContext<S>, name: string): void | Promise<void>;
    /**
     * The method-table factory — called once per ACTIVATION, closing over
     * ctx. Free to create `computed`/`watch` at construction; they die with
     * the activation.
     */
    methods: (ctx: ActorContext<S>) => M;
    /**
     * Stream-method factory. Each entry runs its body as ONE mailbox turn
     * and must return an async iterable that does NOT touch live state —
     * use `ctx.snapshot()` / `ctx.changes()`. Unlike `methods`, this
     * factory must not touch ctx during construction: its keys are
     * enumerated at definition time (for wire routing) with an inert probe.
     */
    streams?: (ctx: ActorContext<S>) => St;
}

export interface ActorDefinition<
    S extends object = object,
    M extends ActorMethodTable = ActorMethodTable,
    St extends ActorStreamTable = ActorStreamTable
> {
    readonly type: string;
    /** Stream-method names, enumerated at definition time. */
    readonly streamNames: readonly string[];
    /** @internal the raw options — the silo's activation hook. */
    readonly __sigxActor: ActorOptions<S, M, St>;
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

// ---------------------------------------------------------------------------
// The silo seam contract (what the wire layer and `actor()` consume)

export interface SiloStats {
    activations: number;
    queued: number;
    perType: Record<string, number>;
}

export interface Silo extends ActorDispatcher {
    /** Definition lookup — the wire resolver's 404 authority. May load lazily. */
    definition(type: string): AnyActorDefinition | Promise<AnyActorDefinition | null> | null;
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
}
