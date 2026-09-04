/**
 * Shared actor types — the dispatch seam, storage contract, and definition
 * shapes. Split from index.ts so the host, wire, and client entries can
 * import types without pulling the public API module in.
 */
import type { ServerPolicy, ServerFnReadCache } from '@sigx/server';

import type { ActorErrorKind, ActorOwnerHint } from './errors';

/**
 * A policy as an actor DECLARES it.
 *
 * The principal is `any` here, and only here, for a variance reason rather
 * than a laziness one: a definition cannot know the app's principal type
 * (that lives on `createServerApp<P>`), so the option's type is
 * `ServerPolicy<unknown>` — and a parameter is contravariant, so the
 * natural spelling
 *
 * ```ts
 * authorize: (user: User | null, _rq, op) => op.resource.key === user?.id
 * ```
 *
 * would not be assignable to it. The alternative is a cast at every policy,
 * which is worse: it moves a compile-time annoyance into runtime-shaped
 * boilerplate, and a cast that appears on every policy stops being read.
 *
 * The narrowing an author actually wants is the one they write in their own
 * parameter annotation, which this preserves. `null` still reaches a policy
 * only on an `allowAnonymous` operation.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type ActorPolicy = ServerPolicy<any>;

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
    /**
     * W3C `traceparent` of the span this call runs under. Unauthenticated
     * observability metadata for tracing middleware — the runtime only
     * relays it (envelope, `ctx.actor` hops), never reads it for control
     * flow. Optional and additive: peers that predate it ignore it.
     */
    readonly traceparent?: string;
    /**
     * The dispatch resolves at ACCEPTANCE (scheduled on the target activation;
     * for a remote call, the receiving host's enqueue — the transport reply
     * is the ack) instead of turn settlement. Failures after acceptance are
     * dropped-with-counter (`oneWayFailures` in `metrics()`), never
     * delivered. `true`-only so the envelope stays additive (`ow`): absent
     * means a normal awaited call. A receiving peer that predates the flag
     * ignores it and delivers the call as a normal awaited one — still
     * exactly once; the sender just resolves at turn completion instead of
     * acceptance.
     */
    readonly oneWay?: true;
    /**
     * The request-context bag (#246): a small, string-only key/value bag
     * stamped at the server edge (typically a guard calling `stampCallBag`),
     * read by methods via `ctx.bag`, inherited by `ctx.actor`/`ctx.publish`
     * hops, and carried host-to-host on the envelope (`bag`, additive within
     * v1). Size-capped (see `CALL_BAG_MAX_*`), frozen, never client-settable
     * over the public wire. NOT integrity-protected between hosts — the
     * envelope rides outside the cluster HMAC, so its trust is the same
     * perimeter posture as the rest of the envelope (mTLS/VPC between
     * hosts). A malformed bag is dropped WHOLE en route, never a 400 — so an
     * actor must treat a missing entry as unauthenticated, never as a
     * different principal.
     */
    readonly bag?: Readonly<Record<string, string>>;
    /**
     * The authenticated principal, encoded with the app's `codec`
     * (rfc-server-v4 §3.1/§7) — a FIRST-CLASS slot, deliberately not a bag
     * key.
     *
     * Populated ONLY by an entry point from its own authentication, after
     * the pipeline ran: the wire endpoint, the live endpoint, and the
     * in-process `actor()` client. Never read from a request header, and
     * never writable through `.with({ bag })` — which is the point. The
     * pre-v4 failure mode was a guard author forgetting `stampCallBag` and
     * silently dropping identity across hops; identity that is not in the
     * bag cannot be forgotten, spoofed, or overwritten by app data.
     *
     * Inherited unchanged by `ctx.actor`/`ctx.publish` hops and carried
     * host-to-host on the envelope, where — like `bag` — it rides outside
     * the cluster HMAC and therefore carries the perimeter's trust, not
     * cryptographic proof. Callee side reads it as `ctx.principal`, decoded
     * lazily. A missing slot or a failed decode is `null`: anonymous, never
     * a different principal.
     */
    readonly principal?: string;
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
 * its turn, and `elapsedMs`, how long it then HELD the activation. That
 * split is the difference between "this actor is slow" (elapsed) and "this
 * actor is hot" (queued), which are opposite problems with opposite fixes —
 * and only the activation knows both, since a middleware sees just the sum.
 *
 * Positional parameters, not an options object: this runs on every turn, and
 * an object literal per turn would be an allocation on the hot path.
 *
 * Fires for DISPATCHED turns only — the ones a caller waited for, including
 * reminder delivery (as `$sigx:reminder`). Volatile `ctx.timer` ticks and
 * write-behind flushes are excluded: they have no caller and no queue wait
 * of their own, and their cost is already visible as queue wait on whatever
 * was behind them. Call-chain-reentrant (`ctx.actor` A→B→A) calls run inline
 * against the caller's turn and are excluded too, since the outer turn
 * already covers that time.
 *
 * INTERLEAVED turns (`reentrant: 'always'` / `methodReentrancy`) fire once
 * per turn like any other, but launch immediately: `queuedMs` is ~0 and
 * `elapsedMs` is how long the turn RAN, not how long it held the activation —
 * their `[start, end]` intervals can overlap.
 *
 * Called from a `finally`. Throwing from here is swallowed (dev-logged) —
 * an observer must never be able to fail a turn.
 *
 * `failed` means THE METHOD INVOCATION THREW — deliberately narrower than
 * "the caller saw an error" (#53). The observer fires the moment the method
 * settles; the runtime's own post-turn bookkeeping (change-feed fan-out,
 * write-behind scheduling) runs AFTER it, inside the same `finally`, and
 * can fail on its own — a boundary snapshot whose codec throws, say. Such
 * a turn is reported with `failed: false` even though its caller receives
 * the bookkeeping error. Nor does `elapsedMs` include that bookkeeping.
 * This is the contract, not a gap waiting to close: the narrow meaning is
 * the one an actor author can act on (their code threw, or it did not),
 * and it is pinned by a test. A dashboard that needs "the caller errored"
 * measures it at the dispatch seam — a dispatch middleware sees every
 * rejection the caller sees, which is where the `metrics` plugin's
 * `calls.failed` already counts — and reports storage failures through
 * their own channels: a write-behind flush failure has no caller and
 * reaches the definition's `onStateError` (#54), and any storage error
 * at all is visible to a decorating `ActorStorage`.
 *
 * `call` is the turn's `ActorCallContext` — what lets an observer correlate
 * the turn with the dispatch that caused it (`callId`, `traceparent`).
 * Trailing and optional-by-position so existing five-parameter observers
 * keep compiling; the object already exists per dispatch, so passing it
 * allocates nothing.
 */
export type ActorTurnObserver = (
    ref: ActorRef,
    method: string,
    queuedMs: number,
    elapsedMs: number,
    failed: boolean,
    call?: ActorCallContext
) => void;

/**
 * A per-actor-type placement strategy, declared ON the actor rather than
 * in a central map.
 *
 * Deliberately opaque here: choosing a host needs a membership view, which
 * is a cluster concept, and core must not depend on `./cluster`. Cluster
 * narrows this to `PlacementPolicy` (adding `choose()`); the single-node
 * host has one host and ignores it. That keeps the declaration next to the
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
     * a backend define its own strategies — or, like `durableObjects()`,
     * declare that it reads none) but it left a
     * backend unable to tell two very different things apart: a strategy meant
     * for someone ELSE, which must be ignored silently, and one meant for IT
     * but malformed, which must fail.
     *
     * With the tag a backend distinguishes three cases instead of one, so a
     * missing `choose()` stops being a dev-only warning and a silent
     * misplacement in production. Set by each backend's own factories; a
     * hand-written strategy may omit it, and is then judged on shape alone.
     *
     * A backend's plugin can also narrow `ActorOptions.placement` to its own
     * strategy type structurally, through `ActorPlugin<Ext, Placement>`, so
     * the app-bound `defineActor` catches the mismatch at compile time (#58).
     */
    readonly backend?: string;
}

/**
 * Where an actor currently lives — the answer to `ActorPlacement.locate()`.
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
     * Where this actor lives, WITHOUT dispatching and WITHOUT activating —
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
     * caller acts on it the actor may have moved. That is safe because the
     * directory, not this, is the arbiter of single-activation.
     */
    locate?(ref: ActorRef): ActorLocation | Promise<ActorLocation> | undefined;
    /**
     * Called once by `createHost` with the host's own local dispatcher and
     * the host itself, before `start()`. A distributed placement returns
     * bindings hooking the activation lifecycle (directory claims) and the
     * reminder tick; the default local placement doesn't implement it.
     */
    bind?(local: ActorDispatcher, host: Host): PlacementBindings | void;
    start?(): void | Promise<void>;
    /**
     * Called by `host.stop()` BEFORE the drain begins — a cluster placement
     * announces `leaving` here so peers stop placing new actors on this
     * host while it hands its activations off.
     */
    beginStop?(): void | Promise<void>;
    stop?(): void | Promise<void>;
}

/**
 * What a placement's `bind()` hands back to the host. Every member is
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
     * fixed hash shards and each tick a host processes only the shards it
     * owns (a cluster answers via rendezvous hashing over the membership
     * view, spreading reminder load; every host still mutates any shard).
     * Default: own every shard.
     */
    ownsReminderShard?(shard: string): boolean | Promise<boolean>;
    /**
     * This host's cluster identity, for the task roster's records (#310).
     * Default: an id `createHost()` mints per host instance.
     */
    hostId?: string;
    /**
     * Is `hostId` still a member of the cluster? Task-roster adoption acts
     * only on hosts this says are gone. Default: only this host is live.
     */
    isHostLive?(hostId: string): boolean | Promise<boolean>;
    /**
     * The deactivation reason a graceful `host.stop()` uses. A cluster
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
    /** Codec-encoded (JSON-safe) state as `@sigx/serialize` produced it — the last FULL save. */
    state: unknown;
    etag: string;
    /**
     * The entries `appendText` added since that full save, parsed, oldest
     * first (#312). PRESENT — an empty array when nothing was appended —
     * only from a storage that implements `appendText`; absent from one
     * that does not, which is how the host tells "nothing to replay" from
     * "this store never says".
     */
    log?: unknown[];
}

/**
 * Pluggable persistence with optimistic concurrency. `expectedEtag: null`
 * means "no stored record yet" (first save). A mismatch must throw the
 * `ActorStorageConflict` brand — the runtime translates it into
 * `ActorStateConflictError` and discards the stale activation.
 *
 * Ownership contract (#25):
 * - `save` takes ownership of `state` at the moment of the CALL, and keeps
 *   it however the call settles: the caller must not mutate the tree while
 *   the promise is pending or after it rejects — a retry (e.g. after a
 *   conflict) builds a fresh tree rather than reusing the handed-over one,
 *   which is what the host does (it re-encodes state per attempt). An
 *   implementation may store the tree by reference (`memoryStorage`) or
 *   serialize it (`JSON.stringify` inside `save` trivially satisfies
 *   this). The host always passes a codec-fresh tree it built for the
 *   call, so no defensive copy is required.
 * - `load` must return a record the caller may freely mutate — an
 *   implementation that holds records in memory must hand out a copy
 *   (deserializing from a stored string trivially satisfies this).
 *
 * A DECORATOR of this seam (`decorateStorage`) must forward every member it
 * does not deliberately replace, `saveText` and `appendText` included — and
 * forward each CONDITIONALLY, so an inner storage without one does not
 * appear to have it. A decorator that returns a fixed three-method literal
 * silently drops both optional paths, and the host falls back — to the
 * two-walk save, and to a full save per append — correct, just quietly
 * slower.
 *
 * Every rule above is pinned by `storageConformance` in `@sigx/actors/testing`
 * (workspace-only) — run it against a new adapter before trusting it.
 */
export interface ActorStorage {
    load(type: string, key: string): Promise<ActorStorageRecord | null>;
    save(type: string, key: string, state: unknown, expectedEtag: string | null): Promise<string>;
    clear(type: string, key: string, expectedEtag: string | null): Promise<void>;
    /**
     * OPTIONAL: save state the host has ALREADY serialized to JSON text
     * (#238), so an adapter that wants a string stops re-walking the tree
     * the host just built.
     *
     * A durable save was two full walks of the same state: the host's
     * `encodeWithHandlers` to a JSON-safe tree, then the adapter's
     * `JSON.stringify` over that tree. The second walk measured at +51% on
     * top of the first (`state/save-growth`, signalxjs/actors#227), and
     * `@sigx/serialize/stringify` can now emit the string in ONE walk.
     *
     * Implementing it is a promise of EQUIVALENCE, not merely of validity:
     * `saveText(type, key, json, etag)` must be observably identical to
     * `save(type, key, JSON.parse(json), etag)` — same CAS semantics, same
     * `ActorStorageConflict` brand on a mismatch, and a later `load()` must
     * return the same record either way. Implement `save` in terms of this
     * one (`save(…, state) => saveText(…, JSON.stringify(state))`) so the
     * two cannot drift.
     *
     * The same ownership contract applies, trivially: a string is immutable,
     * so there is nothing for the caller to mutate out from under the store.
     *
     * ABSENT is the right answer for an adapter that genuinely wants the
     * tree, and the host is correct either way — it keeps its encoded-tree
     * path for exactly that case. `memoryStorage` stores the tree by
     * reference and `durableObjectStorage` hands a structured value to the
     * platform; for both, a string would force a parse back on load.
     */
    saveText?(
        type: string,
        key: string,
        json: string,
        expectedEtag: string | null
    ): Promise<string>;
    /**
     * OPTIONAL: append ONE JSON entry to the record's log, in O(entry)
     * rather than O(state) (#312), so a step of a long-running job costs
     * what the step wrote and not a re-encode of the whole run.
     *
     * The record is a snapshot plus a log: `state` is the last full save,
     * `log` the entries appended since, oldest first, and `load()` returns
     * both. The host folds the log into the state at load through the
     * actor's reducer — the adapter never interprets an entry.
     *
     * It is a write under the same CAS as `save`: `expectedEtag` must equal
     * the record's current etag, and the call mints and returns a NEW one,
     * so a writer holding the old etag — an activation that had not seen
     * the append — conflicts on its next save, clear or append. It is never
     * `null`: there is nothing to append to, so a MISSING record is a
     * conflict, exactly like a mismatch, and both throw the
     * `ActorStorageConflict` brand and append nothing.
     *
     * A full save is the compaction: `save` and `saveText` TRUNCATE the log
     * as part of the same write (the state they store already contains
     * whatever the entries folded to). `clear` removes both, and a record
     * re-created from `null` starts with an empty log. Implement
     * `save`/`saveText`/`clear` so that this holds atomically — a log that
     * survives its snapshot is replayed onto a state that already contains
     * it.
     *
     * ABSENT is the right answer for a store where an append would be a
     * rewrite anyway — `fileStorage`'s pretty-printed envelope,
     * `durableObjectStorage`'s structured `put` — and the host is correct
     * either way: without it, every append is a full save.
     */
    appendText?(type: string, key: string, json: string, expectedEtag: string): Promise<string>;
}

/**
 * What the host hands a reminder implementation at bind time — everything
 * it would otherwise have to be told twice (and could be told wrongly).
 */
export interface ActorRemindersContext {
    /** The host's storage, AFTER any plugin decorators. */
    readonly storage: ActorStorage;
    /** The host's clock, so reminder ticks are drivable like everything else. */
    readonly scheduler: ActorScheduler;
    /** Requested tick cadence, ms (`HostDefaults.reminderTickMs`). */
    readonly tickMs: number;
    /**
     * Does THIS host own the given reminder shard? A cluster answers via
     * rendezvous hashing so N hosts split the load; single-node owns all.
     * Meaningless to an implementation that does not shard.
     */
    ownsShard(shard: string): boolean | Promise<boolean>;
    /** Deliver a due reminder to its actor, activating it if idle. */
    deliver(ref: ActorRef, name: string): Promise<unknown>;
    /**
     * Report a due reminder whose `deliver()` rejected (#306). The host
     * counts these as `HostStats.remindersUndelivered`, so a fleet that is
     * missing wakes says so. Called per failed ATTEMPT — an implementation
     * that retries still reports each one — and optional so a context built
     * by hand (tests, an older host) need not carry it.
     */
    undelivered?(ref: ActorRef, name: string, error: unknown): void;
    /**
     * Report the size of a reminder record the implementation just ticked
     * (#384): the host keeps the maximum as
     * `HostStats.reminderShardEntriesMax`, the gauge that says when the
     * sharded table has outgrown itself. Optional, and meaningless to an
     * implementation that keeps no such record.
     */
    shardSize?(shard: string, entries: number): void;
}

/**
 * What a task-liveness implementation is handed at `bind()` (#310).
 */
export interface ActorTaskLivenessContext {
    /** The host's storage, AFTER any plugin decorators. */
    readonly storage: ActorStorage;
    readonly scheduler: ActorScheduler;
    /** The adoption cadence — `HostDefaults.reminderTickMs`. */
    readonly tickMs: number;
    /**
     * This host's id — the cluster identity when there is one, else an id
     * `createHost()` mints for this host INSTANCE. Never reused: a restart is
     * a new host, which is what lets a successor tell its predecessor's
     * roster from its own.
     */
    readonly hostId: string;
    /** Is that host still a member? Single-node: only this one is. */
    isHostLive(hostId: string): boolean | Promise<boolean>;
    /** Reminder-shard ownership, reused to pick ONE adopter per dead host. */
    ownsShard(shard: string): boolean | Promise<boolean>;
    /** Re-activate an actor and let it resume its runs — the liveness touch. */
    touch(ref: ActorRef): Promise<unknown>;
    /** The per-actor reminder API, for the reminder-based implementation. */
    reminders(ref: ActorRef): ReminderApi;
}

/**
 * Task liveness, as a seam (#310): how the runtime finds a dead host's
 * in-flight detached runs so a survivor re-activates their actors. The
 * default `rosterTaskLiveness()` keeps one roster record per host (the
 * host is its only writer — one CAS per start and per finish, nothing
 * periodic) and adopts dead hosts' rosters on the reminder tick.
 * `reminderTaskLiveness()` is the previous mechanism — one durable reminder
 * per running task — and the right one where a reminder IS the platform's
 * wake-up, as on a Durable Object.
 */
export interface ActorTaskLiveness {
    bind(context: ActorTaskLivenessContext): void;
    start(): void | Promise<void>;
    stop(): void | Promise<void>;
    /** Make `ref`'s in-flight run findable. Durable before it resolves. */
    track(ref: ActorRef): Promise<void>;
    /** `ref` has no run in flight any more. */
    untrack(ref: ActorRef): Promise<void>;
}

/**
 * Durable reminders, as a seam.
 *
 * The default (`shardedReminders()`) keeps the table in `ActorStorage` under
 * a reserved type, split into fixed hash shards that hosts divide between
 * them — which assumes MANY actors per host. A runtime where that is false
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
     * timers don't keep actors alive.
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

/** @internal One in-flight run as `ActorOptions.resumeTasks` derives it. */
export interface TaskResumeEntry {
    /** The start input, replayed on resume. */
    input?: unknown;
    /** Epoch-ms the run FIRST started (not this attempt). */
    startedAt: number;
    /** Times the runtime has re-started the run so far (0 = never). */
    restarts: number;
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
 * Detached long-running work — `ctx.tasks`. A task runs OUTSIDE any turn
 * (reads interleave while it works) and holds a keep-alive ref so idle
 * collection skips the actor. State access happens only through the task
 * context's `turn()`, which is an ordinary serialized turn.
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

/**
 * Why an activation is going away. `'idle'` is the idle sweep;
 * `'capacity'` the max-activations LRU shed (same sweeper, different
 * pressure); `'explicit'` a `ctx.deactivate()` / `host.deactivate()`
 * request; `'shutdown'` the host stopping; `'conflict'` a storage etag
 * mismatch (the activation is discarded and the next call reloads the
 * winning state; under `retryQueuedOnConflict` a TURN-path conflict
 * instead reloads in place and reaches this reason only if that reload
 * fails — a flush conflict deactivates either way); `'activation-failed'`
 * an `onActivate` throw (nothing to tear down — `onDeactivate` is
 * skipped); `'migrated'` a cluster handoff or rebalance — a peer will
 * re-place the actor.
 */
export type DeactivationReason =
    | 'idle'
    | 'capacity'
    | 'explicit'
    | 'shutdown'
    | 'conflict'
    | 'activation-failed'
    | 'migrated';

/**
 * Which deferred save failed with no caller to throw to (#54) — a
 * `write-behind` actor's, or an explicit-persistence actor's
 * `ctx.save({ durability: 'eventual' })` (#320). `'flush'` is the
 * debounced save between turns. A transient storage error there leaves
 * the state dirty but schedules NO retry of its own: on a write-behind
 * actor the next write (a new dirty boundary re-arms the debounce) or the
 * final flush at deactivation carries it — a non-mutating turn does not;
 * on an explicit actor only the next `save()` (immediate or eventual) or
 * the deactivation flush does — no dirty boundary re-arms anything there.
 * A crash in between loses the write either way. An etag conflict there
 * instead faults the activation (`ActorStateConflictError`) and its
 * unsaved writes are discarded: once the hook returns the activation is
 * torn down — `onDeactivate('conflict')` follows with no further call, the
 * final flush is skipped so the stale state never overwrites the winner,
 * and the next call reloads the winning state (#336) — so the hook is the
 * last time that state is observable on this activation. `'final-flush'`
 * is the save at deactivation — the last chance for those writes, so a
 * failure there IS lost data.
 */
export type StateErrorPhase = 'flush' | 'final-flush';

// ---------------------------------------------------------------------------
// Topics — actor-to-actor pub/sub

/**
 * A topic identity: a NAME (the namespace a `subscriptions:` entry binds to)
 * and a KEY (which subscriber instance receives the event — by default the
 * subscriber's own actor key). Built by `topic()`, which validates both.
 *
 * The type parameter is phantom: it types the payload on the publish side
 * and never exists at runtime.
 */
export interface Topic<T = unknown> {
    readonly name: string;
    readonly key: string;
    /** @internal phantom — payload type only; never set. */
    readonly __payload?: T;
}

/** What a subscription handler receives — the whole delivery, not just the
 *  payload, so one handler can serve several topic keys. */
export interface TopicEvent<T = unknown> {
    readonly topic: { readonly name: string; readonly key: string };
    readonly payload: T;
    /** Epoch-ms at publish. */
    readonly at: number;
    /** The publishing actor, when published from a turn (`ctx.publish`);
     *  absent for `host.publish()` / `publishTopic()`. */
    readonly publisher?: ActorRef;
}

/** One subscriber a publish could not deliver to. The publisher NEVER
 *  throws for a subscriber's failure — it lands here instead. */
export interface TopicDeliveryFailure {
    readonly type: string;
    readonly key: string;
    readonly message: string;
    /** The branded classification, when the failure was an actor error —
     *  `'deadlock'`, `'call-timeout'`, `'unreachable'`, … */
    readonly kind?: ActorErrorKind;
}

/**
 * What `publish()` resolves to. Delivery is BEST-EFFORT, at-most-once:
 * "delivered" means the subscriber's handler turn settled without throwing,
 * bounded by the call deadline. Nothing is persisted, retried, or replayed.
 */
export interface TopicPublishReport {
    /** Subscriber refs targeted (the deploy's subscribing types). */
    readonly subscribers: number;
    readonly delivered: number;
    readonly failures: readonly TopicDeliveryFailure[];
}

/** Options for `host.publish()` / `publishTopic()`. A bag from day one so a
 *  future delivery mode has somewhere to live. */
export interface PublishOptions {
    signal?: AbortSignal;
}

/** A `subscriptions:` handler — an ordinary turn, free to mutate
 *  state and `ctx.save()`. */
export type TopicSubscriptionHandler<
    S extends object,
    Ext extends object = Record<never, never>
> = (ctx: ActorContext<S, Ext>, event: TopicEvent) => void | Promise<void>;

/**
 * One `subscriptions:` entry: the handler alone (subscriber key = topic
 * key), or `{ key, handle }` where `key` maps the topic key to the
 * subscriber key — `() => 'aggregate'` makes one singleton receive every
 * key's events.
 */
export type TopicSubscription<S extends object, Ext extends object = Record<never, never>> =
    | TopicSubscriptionHandler<S, Ext>
    | {
          /** Map topic key → subscriber key. Default: identity. */
          key?: (topicKey: string) => string;
          handle: TopicSubscriptionHandler<S, Ext>;
      };

/** How durable a `ctx.save()` must be before it resolves. */
export interface SaveOptions {
    /**
     * `'immediate'` (default): the record is stored when the promise
     * resolves. `'eventual'`: the state is marked for the activation's
     * write-behind debounce and the promise resolves at once — a burst of
     * eventual saves costs one write.
     */
    durability?: 'immediate' | 'eventual';
}

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
     * Persist. `'immediate'` (default): resolves when stored; throws
     * `ActorStateConflictError` on an etag mismatch, which also discards
     * this activation. `'eventual'`: resolves at once and lets the
     * activation's write-behind debounce carry the state — a burst of
     * eventual saves costs one write. A later immediate `save()` or the
     * deactivation flush picks up whatever is still pending, so an
     * explicit-persistence actor stays explicit: no WRITE happens that was
     * not asked for. The write itself is like any other save — it stores
     * the state as it stands when it runs, not a snapshot taken at the
     * call, so mutations made between the call and the flush ride along.
     * What it gives up is the acknowledgement: a host death inside the
     * debounce window loses the eventual saves since the last durable one,
     * and a flush failure reports through `onStateError` rather than to a
     * caller.
     */
    save(options?: SaveOptions): Promise<void>;
    /**
     * Persist ONE entry instead of the whole state (#312): the entry is
     * folded into `ctx.state` through the definition's `applyEntry` reducer
     * and appended to the record's log under the record's etag — O(entry),
     * where `save()` is O(state). Durable when the promise resolves, and a
     * write like any other: a stale peer's later `save()` conflicts, and a
     * conflict here faults the activation (or parks the #368 reload)
     * exactly as a save's would. Throws if the definition declares no
     * `applyEntry`. Where there is no record yet, or the storage has no
     * `appendText`, this IS a full save — same result, today's cost.
     *
     * What it makes durable is the entry. State written directly (not
     * through the reducer) since the last full save is not in the log; the
     * next `save()` — or the write-behind / eventual flush that owes it —
     * carries it, so nothing is lost, only not yet O(entry).
     */
    append(entry: unknown): Promise<void>;
    /** Delete the stored record; in-memory state resets to `state(key)`. */
    clearState(): Promise<void>;
    /** Volatile timer — dies with the activation; ticks are ordinary turns. */
    timer(name: string, cb: () => void | Promise<void>, opts: TimerOptions): TimerHandle;
    /** Durable reminders — survive deactivation, re-activate the actor. */
    readonly reminders: ReminderApi;
    /** Typed client for another actor; carries the call chain. */
    actor<D extends AnyActorDefinition>(def: D, key: string): ActorClientWith<D>;
    /**
     * The CURRENT turn's request-context bag — the edge-stamped, string-only
     * key/value metadata riding this call chain (`ActorCallContext.bag`).
     * Frozen; resolved per read, so it is turn-correct even on interleaving
     * activations. An empty frozen object outside any turn and in contexts
     * that deliberately do not inherit it (detached task bodies, volatile
     * timer ticks — the same rule as `traceparent`). A missing entry means
     * unauthenticated: en route a malformed bag is dropped whole, never
     * partially delivered.
     */
    readonly bag: Readonly<Record<string, string>>;
    /**
     * The authenticated principal for the request that entered the system,
     * decoded with the app's `codec` (rfc-server-v4 §7) — or `null` when
     * the caller was anonymous, no codec is configured, or the encoded slot
     * failed to decode.
     *
     * Resolved LAZILY and memoized per turn: an actor that never asks pays
     * nothing. It flows unchanged through `ctx.actor`/`ctx.publish` hops and
     * between hosts, so a downstream actor sees the identity of the ORIGINAL
     * caller rather than of the actor that called it — authentication is
     * per-request, authorization is per entry point.
     *
     * Empty in contexts that deliberately inherit nothing: detached task
     * bodies, volatile timer ticks, reminders (the same rule as `bag` and
     * `traceparent` — a reminder outlives the request that scheduled it, so
     * there is no caller left to name).
     *
     * **Treat `null` as unauthenticated, never as a different principal.**
     * Between hosts this rides outside the cluster HMAC, exactly like the
     * bag, so its trust is the deployment's perimeter — not a proof.
     */
    readonly principal: unknown;
    /**
     * Publish to a topic. Settles when every subscriber's handler turn has
     * settled; subscriber failures land in the report, never here. Carries
     * this turn's call chain, so a subscription cycling back into this
     * actor is a detected deadlock (a `failures` entry), not a hang.
     */
    publish<T>(topic: Topic<T>, payload: T): Promise<TopicPublishReport>;
    /** Finish the queue, then deactivate. */
    deactivate(): void;
    /**
     * Aborts when this activation begins deactivating (any reason,
     * including host shutdown) — BEFORE the turn drain, so long-running
     * work can observe it and wind down inside the drain window.
     */
    readonly abortSignal: AbortSignal;
    /** Detached long-running work declared in the `tasks:` section. */
    readonly tasks: TaskApi;
    /** Deep, detached copy of the current state (safe outside a turn). */
    snapshot(): S;
    /**
     * Deep, detached copy of an arbitrary VALUE through the host codec —
     * the same encode+revive a state snapshot uses, so custom `types:`
     * handlers round-trip where a `structuredClone` would throw or strip
     * them. Made for cloning a SUBTREE (`ctx.snapshot(ctx.state.items)`)
     * when a read must return part of a large state detached: the no-arg
     * form clones everything, and on state that grows through a run that
     * is exactly the O(state) read cost #229 removed from `defineJob`.
     */
    snapshot<T>(value: T): T;
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
    changes(options?: {
        initial?: boolean;
        /**
         * Coalesce bursts: at most one snapshot per `throttleMs`, leading
         * edge plus trailing edge, and the trailing one is taken fresh so it
         * is never staler than the window. Omit (or 0) for a snapshot per
         * mutating turn — the default, unchanged.
         *
         * Worth setting when the consumer redraws rather than accumulates,
         * because a snapshot is a full encode+revive of the WHOLE state and
         * a boundary inside an open window builds none at all. An actor
         * whose state grows through a run — a job appending a step's output
         * per turn, reporting progress as it goes — otherwise clones
         * everything it has accumulated on every step (#129).
         *
         * Never drops the final state: a window still owing an emit when the
         * actor deactivates is flushed before the feed ends. Must be a
         * non-negative finite number; anything else throws.
         */
        throttleMs?: number;
    }): AsyncIterable<S>;
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
 * What a `tasks:` body sees. Tasks run DETACHED, outside any turn, so the
 * live-state members are typed away: no `state`, no `save()`/`append()`,
 * no `clearState()`. State access goes through `turn()` — one ordinary
 * serialized turn with the full context — and reads through
 * `snapshot()` / `changes()`. `abortSignal` is the RUN's own signal: it
 * fires on `ctx.tasks.cancel()` (reason `'cancelled'`) and on deactivation
 * (reason: the `DeactivationReason`), before the turn drain, so a task
 * can run a final `turn()` checkpoint while winding down.
 */
export type ActorTaskContext<
    S extends object,
    Ext extends object = Record<never, never>
> = Omit<ActorContextBase<S>, 'state' | 'save' | 'append' | 'clearState' | 'abortSignal'> & {
    /** Re-enter: run `fn` as one ordinary serialized turn. */
    turn<T>(fn: (ctx: ActorContext<S, Ext>) => T | Promise<T>): Promise<T>;
    /** This run's signal — see the type doc. */
    readonly abortSignal: AbortSignal;
} & Ext;

/**
 * The second argument to a `migrateState` hook — the same record, in the
 * form the hook's first argument is NOT.
 */
export interface MigrateStateInfo {
    /**
     * The codec-ENCODED record, exactly as storage holds it. Reach for it
     * when the revived view cannot distinguish two stored versions — an old
     * field whose tag no longer revives to anything useful, say. Read-only
     * by contract: mutating it does not change what is stored.
     */
    readonly raw: unknown;
    /** The actor key, as `state(key)` receives it. */
    readonly key: string;
}

/**
 * A state-migration hook. The return type is `NoInfer<S>` on purpose: `S` is
 * inferred from `state:` ALONE, and this is a CHECK site, not a second
 * inference site. Without that, a migration written over `any` — which is
 * what casting a `stored: unknown` naturally produces — would infer `S = any`
 * and silently erase `ctx.state`'s type everywhere, with no error anywhere.
 * It also makes an `async` hook a type error (`Promise<S>` is not `S`), which
 * is the sync-only rule enforcing itself.
 */
export type MigrateStateFn<S extends object> = (
    stored: unknown,
    info: MigrateStateInfo
) => NoInfer<S>;

/**
 * `migrateState:` — the bare hook, or the hook plus its write-back policy.
 * `'lazy'` (the default) writes nothing on its own; `'eager'` issues one CAS
 * write-back at activation, for records that would otherwise never be saved
 * and so would be re-migrated on every activation forever.
 */
export type MigrateState<S extends object> =
    | MigrateStateFn<S>
    | { migrate: MigrateStateFn<S>; persist?: 'lazy' | 'eager' };

/**
 * `Ext` is the plugin-contributed shape of `ctx`, `Placement` the strategy
 * type the app's placement backend accepts — both default to the widest
 * reading for the bare `defineActor`, and both are narrowed only by the
 * app-bound one (`ActorApp<Ext, Placement>['defineActor']`).
 */
export interface ActorOptions<
    S extends object,
    M extends ActorMethodTable,
    St extends ActorStreamTable,
    Ext extends object = Record<never, never>,
    Placement extends ActorPlacementStrategy = ActorPlacementStrategy
> {
    /**
     * Stable type id — the actor's wire, directory, and storage name.
     * A string literal (the build transform reads it statically); renaming
     * it is a wire and storage break.
     */
    type: string;
    /**
     * This actor's authorization policy chain (rfc-server-v4 §7), decided
     * at every ENTRY POINT — the wire endpoint, the live endpoint, an
     * in-process `actor()` call — on every transport, OUTSIDE any turn.
     * Replaces the app default where declared (most-specific-wins).
     *
     * Policies receive the resolved principal and
     * `op.resource = { kind: 'actor', type, key, method }`, so the dominant
     * actor policy — per-INSTANCE, "may this user read cart `u_123`?" — is
     * expressible for the first time:
     *
     * ```ts
     * authorize: (user, _rq, op) => op.resource!.key === user.id
     * ```
     *
     * STRICT-`true`: anything else denies (403; 401 when the principal is
     * null). Required unless `allowAnonymous: true` when the build gate is
     * on (its default), or an app default policy is configured.
     */
    authorize?: ActorPolicy | readonly ActorPolicy[];
    /**
     * The explicit word for an actor reachable without a principal. Waives
     * ONLY the identity gate: app middleware and authentication still run,
     * and a declared `authorize` chain still runs against a nullable
     * principal.
     */
    allowAnonymous?: true;
    /**
     * A server-internal type: the PUBLIC wire never serves it (#74). The
     * actor endpoint, the `$live` mount and the socket session all answer
     * a call, watch or subscription for it exactly as they do for a type
     * that is not registered — same 404, same envelope — so a probe learns
     * nothing. In-process `actor()` / `ctx.actor()` and the cluster's
     * authenticated host-to-host mount keep serving it, and the Vite build
     * emits a `__serverOnly` stand-in instead of a client stub.
     *
     * Orthogonal to access: the pipeline still runs for the callers that
     * can reach it, so an internal actor declares `authorize` /
     * `allowAnonymous` like any other (or relies on the app default).
     */
    internal?: true;
    /** Per-method policy chains, ANDed AFTER `authorize`. Static map — the
     *  method table itself is per-activation and cannot carry wire metadata. */
    methodAuthorize?: Record<string, ActorPolicy | readonly ActorPolicy[]>;
    /**
     * @internal Set by `defineWorker` / `defineJob` so a policy's
     * `op.resource.kind` names what it is actually deciding about. Absent
     * means `'actor'`; a value written by hand is overwritten by those
     * factories.
     */
    kind?: 'worker' | 'job';
    /**
     * Per-method interleaving, alongside `methodAuthorize` (same static-map shape,
     * own keys only): a method mapped to `'always'` is exempt from turn
     * exclusivity regardless of the actor-level `reentrant` setting — it
     * never waits for in-flight turns and is never waited for, and an
     * in-chain call to it never throws `ActorDeadlockError`. Unlisted
     * methods keep the actor-level behavior exactly.
     *
     * The canonical use: read-only `get`-style methods on an otherwise
     * serial actor, so reads never queue behind a slow write (pairs well
     * with `reads:` for the HTTP-cacheable ones). The contract is the same
     * as `reentrant: 'always'`, scoped to the listed methods: their turns
     * may observe state changes across any `await`.
     *
     * Keys must name `methods:` entries — a stream or reserved name fails
     * the type's first activation loudly, a name matching nothing is
     * dev-warned there. Redundant on a `reentrant: 'always'` actor, so that
     * combination fails the same way.
     */
    methodReentrancy?: Record<string, 'always'>;
    /** Initial state factory — used when storage has no record (the
     *  virtual-actor "always exists" default). */
    state: (key: string) => S;
    /**
     * Evolve a STORED state shape whose layout predates this deploy — the
     * answer to "the `state:` shape changed and the records didn't".
     *
     * Runs between the storage read and activation, and ONLY on a load that
     * FOUND a record: never on the `state(key)` fresh path, never on
     * `ctx.clearState()` (which re-seeds through `state(key)` for the same
     * reason). It also runs BEFORE `onActivate`, which therefore always sees
     * migrated state — an ordering commitment, not an accident.
     *
     * ```ts
     * migrateState: (stored) => {
     *     const s = stored as CartV1 | CartV2;
     *     if ('v' in s) return s;                              // fast path
     *     return { v: 2, items: s.items ?? [], coupons: [] };  // v1 → v2
     * }
     * ```
     *
     * `stored` is already CODEC-REVIVED — `Date`/`Map`/`Set` are real objects
     * again — so `unknown` means unknown SHAPE, not raw JSON. When the revived
     * view cannot tell two versions apart, `info.raw` is the codec-ENCODED
     * record as it sits in storage.
     *
     * **Returning the input unchanged is the fast path**, and identity is how
     * that is detected — so to migrate, return a NEW object. Mutating `stored`
     * in place and returning it works (the value has not been made reactive
     * yet), but reads as "nothing migrated" to `persist: 'eager'`.
     *
     * By default the migrated shape is written back LAZILY: it rides the next
     * save the actor would have made anyway, so read paths still issue zero
     * writes and a rolling deploy costs nothing extra. The rule holds in BOTH
     * persistence modes — `migrateState` never causes a write by itself, so a
     * `write-behind` actor that is only ever read after a migration does not
     * persist it. `{ persist: 'eager', migrate }` opts into an immediate CAS
     * write-back instead, for records that would otherwise never be saved.
     *
     * The trade is stated rather than hidden: a fleet mid-rolling-deploy can
     * migrate the same record on several hosts. That is safe because the hook
     * is a pure function of the stored value and every write is etag-CAS'd —
     * the first save wins, and the loser either adopts the winner (eager) or
     * gets `ActorStateConflictError` and re-activates against it (lazy).
     *
     * Synchronous, and a throw fails the activation with
     * `ActorActivationError` — the same posture as a throwing `onActivate`.
     * Corrupt state is loud, and the stored record is never silently reset.
     *
     * Version bookkeeping is YOUR convention: the runtime neither reads nor
     * writes a version field, and this is deliberately not a scheme for
     * versioning an actor's INTERFACE across a mixed-version fleet.
     */
    migrateState?: MigrateState<S>;
    /**
     * The reducer behind `ctx.append(entry)` (#312): fold one entry into the
     * state, IN PLACE. An entry is whatever `append` was handed — a step's
     * output, an event — anything the state codec carries. It runs twice
     * per entry over the record's life: once at `append`, on the live
     * state, and once per activation, replaying the record's log onto the
     * stored snapshot in append order, AFTER `migrateState` and before
     * `onActivate`. So it must be a pure function of (state, entry): same
     * state and entry, same result, on any host.
     *
     * A full save (`ctx.save()`, a write-behind or eventual flush, the
     * deactivation flush) stores the folded state and truncates the log —
     * a full save is the compaction, and there is no other. On a storage
     * without `appendText` (`fileStorage`, Durable Objects) every append
     * IS a full save; the reducer still runs, the log is simply never
     * longer than nothing.
     *
     * A record carrying a non-empty log fails activation under a
     * definition without this — it was written by one that had it.
     */
    applyEntry?(state: S, entry: unknown): void;
    /**
     * `'explicit'` (default): only `ctx.save()` writes. `'write-behind'`:
     * a deep watch schedules a debounced save; acked ≠ persisted — use only
     * for lossy-tolerant state.
     */
    persistence?: 'explicit' | { mode: 'write-behind'; debounceMs?: number };
    /**
     * Re-run queued turns after an etag conflict instead of failing them
     * (#368). Default (off): a turn whose `ctx.save()` loses the CAS rejects
     * with `ActorStateConflictError`, every turn queued behind it rejects
     * the same way, the activation is discarded (`onDeactivate('conflict')`)
     * and the next call re-activates on the winning state. With `true` the
     * losing turn still rejects — its writes were computed on stale state
     * and are discarded — but the activation stays: before the next turn
     * runs, the winning record is reloaded in place (through `migrateState`,
     * as on activation) and the queued turns then run in their original
     * order against it, as if they had arrived after the other writer.
     * Change-feed subscribers never see the losing turn's writes: its
     * boundary is suppressed and the winning state is the next one.
     *
     * Only for methods that are idempotent or commutative: a queued turn
     * is re-run against state it did not see when it was queued, so a
     * method whose effect depends on what was there before (read-then-
     * decide) can act twice or act on the wrong premise. The losing turn
     * itself is never re-run.
     *
     * Applies to the turn-path save only. A write-behind / eventual-save
     * flush conflict still deactivates (`StateErrorPhase`). If the reload
     * itself fails, the activation faults exactly as without the option.
     * Serial actors only: on an interleaving activation — `reentrant:
     * 'always'`, or a `methodReentrancy` map naming any method — the first
     * activation fails: interleaved turns are never queued, so there is
     * nothing to re-run, and a reload landing under an in-flight turn
     * would silently discard its writes.
     * `reentrant: 'call-chain'` is fine (an in-chain call runs inside its
     * caller's turn and never reloads).
     */
    retryQueuedOnConflict?: true;
    /**
     * Admission cap on this actor's turn queue (#384). A call that would
     * push queued-plus-running turns past this is refused at once with
     * `ActorOverloadedError` (`scope: 'actor'`) instead of queued — so a
     * saturated activation sheds in microseconds rather than holding every
     * call for `callTimeoutMs` and failing all of them after doing the
     * work. `0` = unlimited. Unset = `HostDefaults.maxQueuedPerActor`.
     * Never applied to the runtime's own turns (watch reads, the
     * write-behind flush, a conflict reload). Size it as
     * `callTimeoutMs / p50 turn ms`: never admit more than the queue can
     * drain inside the deadline.
     */
    maxQueued?: number;
    /**
     * Reentrancy. Default `false`: turns are strictly serial and A→B→A
     * throws `ActorDeadlockError`.
     *
     * - `'call-chain'` (alias: `true`, the v1 spelling): A→B→A runs inline
     *   against this actor's own up-stack turn. Unrelated calls still
     *   serialize — no foreign interleaving.
     * - `'always'`: FULL interleaving. Every call is its own turn, launched
     *   immediately — unrelated calls interleave at every `await`, and
     *   in-chain calls complete as concurrent turns instead of running
     *   inline. The single-threaded guarantee narrows to "no two turns run
     *   simultaneously between awaits": your state can change across EVERY
     *   `await`, and a save captures a synchronously-consistent frame that
     *   may be mid-logical-turn. Deadlock by self-cycle is impossible by
     *   construction. Needs `AsyncLocalStorage` (on Cloudflare Workers:
     *   the `nodejs_compat` flag, which the DO package already requires).
     *
     * For interleaving only SOME methods, see `methodReentrancy`.
     */
    reentrant?: boolean | 'call-chain' | 'always';
    /** Idle collection age for this type; overrides the host default. */
    idleAfterMs?: number;
    /**
     * Where NEW activations of this type go (`consistentHashPolicy()`,
     * `preferLocalPolicy()`, or your own). Read by
     * a cluster placement when it resolves a target, and it WINS over the
     * central `typePolicies` map; ignored single-node.
     *
     * Through an app-bound `defineActor` this is narrowed to what the app's
     * placement plugin understands (`cluster()` → `PlacementPolicy`; a
     * backend that reads no strategy, like `durableObjects()`, carries
     * `never`, so the option must be omitted), so a strategy tagged for
     * another backend is a compile error where the runtime would have
     * ignored it silently, and a malformed untagged one is a compile error
     * where the runtime would have thrown (#58, #351). The bare
     * `defineActor` accepts any strategy.
     */
    placement?: Placement;
    /** Runs before the first message; throwing fails all queued callers. */
    onActivate?(ctx: ActorContext<S, Ext>): void | Promise<void>;
    /** Runs after the queue drains, before state teardown. */
    onDeactivate?(ctx: ActorContext<S, Ext>, reason: DeactivationReason): void | Promise<void>;
    /**
     * A deferred save failed and there is no caller to throw to (#54): a
     * `write-behind` save, or an explicit-persistence actor's
     * `ctx.save({ durability: 'eventual' })` (#320), which resolved before
     * the write. An immediate `ctx.save()` failure still rejects the turn
     * that called it and never comes here. Report it (page, metric,
     * dead-letter `ctx.snapshot()`) instead of polling for faults; see
     * `StateErrorPhase` for what each phase means for the state. The hook
     * only observes: a `'flush'` failure schedules no retry of its own —
     * the next write (write-behind) or the next `save()` (explicit), or
     * the deactivation flush, carries the dirty state — and a
     * `'final-flush'` failure is the last word on it. Runs
     * serialized with turns in the `'flush'` phase and after the drain in
     * `'final-flush'`, and is awaited in both. A throw is dev-logged and
     * ignored. Without the hook, a dev build logs the failure instead.
     */
    onStateError?(
        ctx: ActorContext<S, Ext>,
        error: unknown,
        phase: StateErrorPhase
    ): void | Promise<void>;
    /** Durable-reminder callback. */
    onReminder?(ctx: ActorContext<S, Ext>, name: string): void | Promise<void>;
    /**
     * The method-table factory — called once per ACTIVATION, closing over
     * ctx. Free to create `computed`/`watch` at construction; they die with
     * the activation.
     */
    methods: (ctx: ActorContext<S, Ext>) => M;
    /**
     * Methods that are HTTP-CACHEABLE READS: the endpoint accepts `GET` for
     * them and emits the `Cache-Control` these values describe, so browser,
     * CDN and reverse-proxy caches can absorb read traffic that would
     * otherwise reach an actor.
     *
     * ```ts
     * reads: { summary: { maxAge: 5 }, price: { maxAge: 60, public: true } }
     * ```
     *
     * The declaration is a PROMISE about the method, and the runtime cannot
     * check it: a listed method must be side-effect-free and idempotent. One
     * that mutates re-opens CSRF completely, exactly as core's `cache`
     * declaration does on a serverFn — same vocabulary
     * ({@link ActorReadCache}), same contract.
     *
     * **It also trades away turn ordering for `maxAge` seconds.** A cached
     * response can be older than the actor's state, and nothing can
     * invalidate it — not `ctx.save()`, not `useActorAction`, not
     * `cells.invalidate()`, which reach this page's cells and never a CDN's
     * copy. Declare it where staleness is a product decision, not a bug.
     */
    reads?: { [K in keyof M & string]?: ActorReadCache };
    /**
     * How watched reads of this actor may be SHARED across callers (#138).
     *
     * ```ts
     * watches: { feed: { principalIndependent: true } }
     * ```
     *
     * Static map beside `reads` / `methodAuthorize` / `methodReentrancy`, own
     * keys only, for the same reason they are: the method table is built per
     * ACTIVATION and cannot carry wire metadata, but the relay deciding how
     * to coalesce has only the definition.
     *
     * See {@link ActorWatchDeclaration} for what the promise covers, what
     * polices it, and the two channels it deliberately says nothing about.
     * Validated at the type's first activation (as `methodReentrancy` is,
     * and for the same reason — the `methods` factory needs a live ctx).
     */
    watches?: { [K in keyof M & string]?: ActorWatchDeclaration };
    /**
     * Stream-method factory. Each entry runs its body as ONE turn
     * and must return an async iterable that does NOT touch live state —
     * use `ctx.snapshot()` / `ctx.changes()`. Unlike `methods`, this
     * factory must not touch ctx during construction: its keys are
     * enumerated at definition time (for wire routing) with an inert probe.
     */
    streams?: (ctx: ActorContext<S, Ext>) => St;
    /**
     * Implicit topic subscriptions — this type receives every publish to the
     * named topics, with no registration and nothing stored: the subscriber
     * set is a pure function of the deploy. A publish ACTIVATES an idle
     * subscriber, exactly as a reminder delivery does.
     *
     * ```ts
     * subscriptions: {
     *   'chat-messages': (ctx, event) => { ... },   // subscriber key = topic key
     *   'presence': { key: () => 'aggregate', handle: (ctx, event) => { ... } },
     * }
     * ```
     *
     * Handlers run as ordinary turns (mutate state, `ctx.save()`).
     * They are NOT wire-callable and never appear on the client. A handler
     * that throws fails only its own delivery — the publisher sees a
     * `failures` entry, other subscribers are untouched.
     */
    subscriptions?: Record<string, TopicSubscription<S, Ext>>;
    /**
     * Detached long-running work, started via `ctx.tasks.start(name,
     * input?)`. A task body runs OUTSIDE any turn — other calls
     * interleave while it works — and touches state only through the task
     * context's `turn()`. The factory is called once per RUN with that
     * run's derived context (its own `abortSignal`), and must be a pure
     * table constructor. No wire surface: start/cancel go through your own
     * methods, so the guard chain governs them.
     */
    tasks?: (ctx: ActorTaskContext<S, Ext>) => ActorTaskTable;
    /**
     * @internal Set by `defineJob` only, never by hand. Derives the task
     * ledger — which runs are in flight, and how many times each has been
     * restarted — from the actor's OWN state record, so the runtime keeps
     * no `$sigx:tasks` record for this type: no ledger load on activation,
     * no ledger CAS on start, resume or finish (#309). A job already
     * persists `status`/`input`/`attempts`; a second record saying the
     * same thing was two extra round trips per start and per finish.
     *
     * Called with the live state (read-only) wherever the runtime would
     * have read the record: on activation, inside the liveness reminder's
     * turn, and after a run settles. `input` is passed through the state
     * codec before it reaches the resumed body, so the body never aliases
     * live state. An empty table means nothing to resume. The liveness
     * reminder is unchanged by this hook.
     */
    resumeTasks?: (state: S) => Record<string, TaskResumeEntry>;
    /**
     * @internal Stateless-worker marker — set by `defineWorker` only, never
     * by hand. Presence flips the runtime to pooled multi-activation
     * dispatch for this type: no directory claim, no state load, up to
     * `maxLocal` interchangeable activations per (type, key) on a host.
     * `defineActor` throws when it appears alongside any identity-bound
     * option (`persistence`, `tasks`, `subscriptions`, `onReminder`,
     * `placement`, `reentrant`, `methodReentrancy`).
     */
    stateless?: { maxLocal?: number };
}

/**
 * What a stateless worker's `methods`/`streams` factories receive. Workers
 * have no identity-bound surface, so the persistence members are typed away:
 * no `state`, no `save()`/`clearState()`, no `reminders`, no `tasks`, no
 * `snapshot()`/`changes()`. `ctx.key` remains — it is the key the caller
 * addressed — but two calls to the same key may run concurrently on
 * different pool members: that is the stateless contract.
 */
export type WorkerContext<Ext extends object = Record<never, never>> = Omit<
    ActorContextBase<Record<never, never>>,
    'state' | 'save' | 'append' | 'clearState' | 'reminders' | 'tasks' | 'snapshot' | 'changes'
> &
    Ext;

/**
 * The options bag of `defineWorker` — deliberately a hand-written subset of
 * {@link ActorOptions}, not a derived type: everything identity-bound
 * (`state`, `persistence`, `tasks`, `onReminder`, `subscriptions`,
 * `placement`, `reentrant`, `methodReentrancy`) is structurally absent, so a
 * worker cannot declare it even untyped. Method/stream typing is inferred exactly as for
 * `defineActor` — from the factories, never from state.
 */
export interface WorkerOptions<
    M extends ActorMethodTable,
    St extends ActorStreamTable = Record<never, never>,
    Ext extends object = Record<never, never>
> {
    /** Stable type id — the worker's wire and registry name. */
    type: string;
    /** Policy chain, exactly as on `ActorOptions.authorize`; `op.resource`
     *  arrives with `kind: 'worker'`. */
    authorize?: ActorPolicy | readonly ActorPolicy[];
    /** The explicit word for a worker reachable without a principal. */
    allowAnonymous?: true;
    /** Server-internal, exactly as on `ActorOptions.internal`: the public
     *  wire never serves it. */
    internal?: true;
    /** Per-method policy chains, ANDed after `authorize`. */
    methodAuthorize?: Record<string, ActorPolicy | readonly ActorPolicy[]>;
    /**
     * Pool cap: max concurrent activations per (type, key) on one host.
     * Positive integer. Default: `navigator.hardwareConcurrency` clamped to
     * [4, 16], falling back to 4 where that global does not exist. The floor
     * exists because a cgroup CPU quota makes `hardwareConcurrency` report
     * the quota rather than the machine — inside a CPU-limited container it
     * is typically 1, and members are activations that overlap at `await`
     * points, not threads, so a single core still benefits from a pool.
     */
    maxLocal?: number;
    /** Idle collection age per pool member; overrides the host default. */
    idleAfterMs?: number;
    /** Per-member warm-up (load a model, open a client); throwing fails the
     *  callers queued on this member only. */
    onActivate?(ctx: WorkerContext<Ext>): void | Promise<void>;
    /** Per-member teardown, after that member's queue drains. */
    onDeactivate?(ctx: WorkerContext<Ext>, reason: DeactivationReason): void | Promise<void>;
    /** The method-table factory — called once per pool MEMBER. */
    methods: (ctx: WorkerContext<Ext>) => M;
    /**
     * HTTP-cacheable reads, exactly as on `ActorOptions.reads`. A pure
     * worker read is the ideal candidate: idempotent by construction.
     */
    reads?: { [K in keyof M & string]?: ActorReadCache };
    /**
     * Stream-method factory, exactly as on `ActorOptions.streams` — but with
     * no `ctx.changes()`/`snapshot()` to lean on: worker streams are pure
     * generators. An open stream pins its pool member (counts against
     * `maxLocal`, exempt from the idle sweep) until it closes.
     */
    streams?: (ctx: WorkerContext<Ext>) => St;
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
     * @internal the raw options — the host's activation hook. The context
     * extension is erased to `any` here: a definition built against an app's
     * `Ext` must still be a plain `ActorDefinition`, and the host only ever
     * *calls* these factories with the context it built.
     */
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    readonly __sigxActor: ActorOptions<S, M, St, any>;
}

// `any` variants: `never[]`-typed parameters make the table types invariant
// enough that concrete definitions won't assign to the defaulted shape.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AnyActorDefinition = ActorDefinition<any, any, any>;

/**
 * The cache declaration for one actor read — core's `ServerFnReadCache`,
 * deliberately by ALIAS rather than by imitation.
 *
 * `maxAge` (seconds, no invented default), plus optional
 * `staleWhileRevalidate`, `public` and `sMaxAge`. A second vocabulary for the
 * same HTTP headers would drift from the one an app already learned for
 * serverFns, and the endpoint composing the header is literally core's.
 */
export type ActorReadCache = ServerFnReadCache;

/**
 * What one watched method promises about how it is shared (#138).
 *
 * The only member today is {@link ActorWatchDeclaration.principalIndependent};
 * the wrapping object exists so a later property (a forced per-principal
 * split, say) does not need a second `ActorOptions` key.
 */
export interface ActorWatchDeclaration {
    /**
     * This read's result does NOT depend on `ctx.principal`.
     *
     * The cluster relay keys its coalesced cross-host stream on the caller's
     * principal **unconditionally**, because it cannot observe the owner's
     * per-principal discovery (#121): 10 000 signed-in subscribers across 3
     * hosts therefore cost 10 000 cross-host streams even on a read that
     * never consults identity, and each one pins a pooled host-to-host
     * connection for the life of the subscription. Declaring this drops the
     * principal from that key, so they cost one.
     *
     * Unlike `reads:` — whose promise the runtime cannot check — **this one
     * is policed**: a declared read observed consulting `ctx.principal`
     * fails the watch with `ActorWatchDeclarationError`, in every build, on
     * the owner, whether or not any relay coalesced. It fails closed and
     * does not heal on re-subscribe or failover; remove the flag or the
     * read.
     *
     * Two limits to know. Enforcement covers exactly the channel #121's
     * discovery covers — identity reached through `ctx.actor()` into
     * ANOTHER actor, or smuggled through a closure, is invisible to it. And
     * this is a promise about *identity only*: `ctx.bag` is the empty bag
     * inside any watch turn (#137), declared or not — a shared read never
     * sees a subscriber's per-request context.
     *
     * Touching `ctx.principal` merely to authorize trips this too — #121
     * marks one touch anywhere, even a discarded one. Authorization belongs
     * in `authorize` / `methodAuthorize`, which run per subscriber at the
     * entry point, outside any turn.
     */
    principalIndependent: true;
}

/** Per-call options for `actor(...).with()`, mirroring `fn.with()`. */
export interface ActorCallOptions {
    signal?: AbortSignal;
    /**
     * This call's deadline as a BUDGET in milliseconds from now, in place of
     * the host's `callTimeoutMs` (#75). Wins over the host default in both
     * directions — a 50ms budget against a 30s default rejects at 50ms, and
     * a 5-minute budget lets one long awaited method past a 30s default —
     * but never EXTENDS a deadline the call inherits from an enclosing turn:
     * on a `ctx.actor()` hop the effective deadline is the earlier of the
     * inherited one and `now + deadlineMs`, so a budget deep in a chain can
     * only tighten what the entry point allowed. Must be a positive finite
     * number. The HTTP client sends it as remaining-ms
     * (`x-sigx-deadline-ms`) and the endpoint re-anchors it on its own clock,
     * the same discipline as the cluster envelope; the server rejects the
     * call with the usual `call-timeout` error (504 on the wire) when it
     * passes. Socket transports do not carry it in v1. A stream's
     * consumption is not raced by it — a stream is consumed, not awaited —
     * but the stream body runs under this call context, so an awaited
     * `ctx.actor()` hop made from inside it inherits the deadline as usual.
     */
    deadlineMs?: number;
    /** Extra request headers (wire transport only). */
    headers?: Record<string, string>;
    /** Explicit server context for in-process calls (`fn.with({ context })`). */
    context?: unknown;
    /**
     * Override the carrier for a method declared in `reads:`, which the client
     * otherwise sends as a cacheable `GET`. `false` sends it as a POST — the
     * endpoint accepts both — and the response is then not cacheable.
     *
     * Two reasons to reach for it, both real: arguments too large for a URL
     * (intermediaries cap the query, and the endpoint answers 414), and
     * arguments that should stay OUT of access logs, proxy traces and referrer
     * headers. A GET puts the actor key and every argument in the URL, which
     * is exactly what the hashed routing token exists to avoid for the key.
     *
     * Wire transport only, and inert on an undeclared method: the server
     * accepts GET solely for declared reads.
     */
    get?: boolean;
    /**
     * Fire-and-forget: the call resolves as `Promise<void>` when it is
     * ACCEPTED by the target activation (locally: scheduled; remotely: the
     * transport ack), not when the turn completes. Failures after acceptance
     * are dropped-with-counter (`oneWayFailures` in `metrics()`), never
     * delivered; failures BEFORE acceptance (a failing guard, auth, unknown
     * type, host shutdown, unreachable peer) still reject. Forces POST on a
     * method declared in `reads:` (a one-way never rides a cacheable GET),
     * and streams refuse it — a stream is consumed, not fired-and-forgotten.
     */
    oneWay?: true;
    /**
     * Explicit request-context bag entries for this call — the server-side
     * escape hatch (scripts, tests, cluster ops) beside the usual path of a
     * guard calling `stampCallBag`. Merged OVER whatever the call would
     * otherwise carry (edge-stamped or hop-inherited), explicit entries
     * winning; validated against the `CALL_BAG_MAX_*` caps (throws — this is
     * developer input). Server-side only: the browser wire client ignores it
     * in v1, exactly as `context` is in-process-only and `headers` is
     * wire-only.
     */
    bag?: Readonly<Record<string, string>>;
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

/**
 * The client shape behind `.with({ oneWay: true })`: every method resolves
 * `void` at acceptance, and stream methods are `never` — a stream cannot be
 * one-way.
 */
export type ActorOneWayClient<D> = D extends ActorDefinition<infer _S, infer M, infer St>
    ? {
          [K in keyof M]: M[K] extends (...a: infer A) => unknown
              ? (...a: A) => Promise<void>
              : never;
      } & { [K in keyof St]: never }
    : never;

export type ActorClientWith<D> = ActorClient<D> & {
    /**
     * Bind per-call options; returns the same client shape — except
     * `oneWay: true`, which narrows methods to `Promise<void>`. The overload
     * needs the literal at the call site: an options value statically typed
     * as plain `ActorCallOptions` falls through to the second overload even
     * if it carries `oneWay` at runtime (types then over-promise; the calls
     * still behave one-way).
     */
    with(options: ActorCallOptions & { oneWay: true }): ActorOneWayClient<D>;
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
// The host seam contract (what the wire layer and `actor()` consume)

export interface HostStats {
    activations: number;
    queued: number;
    perType: Record<string, number>;
    /**
     * Slots mid-activation and mid-deactivation. NOT counted in
     * `activations`, which only sees settled ones — so a host in an
     * activation storm reads as idle without these, which is precisely when
     * you are looking at it.
     */
    transitional: { activating: number; deactivating: number };
    /**
     * Shared watch loops across all activations. OPTIONAL because
     * `HostStats` crosses the wire inside a cluster `HostReport`, and a
     * mixed-version fleet must keep parsing reports that predate the field.
     *
     * The number to watch against `activations` and subscriber counts: a
     * popular read costs ONE loop however many subscribers share it, but a
     * read that consults `ctx.principal` splits per distinct identity
     * (#121) — a loop count tracking the subscriber count on a hot actor is
     * the fan-out cliff of #180 building.
     */
    watchLoops?: number;
    /**
     * Due reminders whose dispatch FAILED on this host — a deadline, a host
     * mid-restart, an `onReminder` that threw — counted per attempt (#306).
     * The default `shardedReminders()` re-arms a failed one for the next
     * tick, so this is the rate at which wakes are being missed, not the
     * number lost for good: a value that keeps climbing is a target that
     * never comes back. Monotonic for the host's lifetime. OPTIONAL for the
     * same mixed-fleet reason as `watchLoops`.
     *
     * Fed only by an `ActorReminders` that calls
     * `ActorRemindersContext.undelivered` — `shardedReminders()` does, and
     * so do `pgReminders`, `surrealReminders` and `durableObjectReminders`
     * (#326); on a custom implementation that does not, a `0` here is "said
     * nothing", not "lost none".
     */
    remindersUndelivered?: number;
    /**
     * Calls this host REFUSED at admission (#384) — a queue at its
     * `maxQueued`, or the host at its `maxInflightTurns` — counted per
     * refusal, monotonic for the host's lifetime. A climbing value is a host
     * shedding load, which is the designed behaviour past capacity; read it
     * against `queued` and the caller's own error rate. OPTIONAL for the
     * mixed-fleet reason above.
     */
    overloadRefusals?: number;
    /**
     * The largest sharded reminder record this host has ticked, in entries
     * (#384, from #396). The default `shardedReminders()` loads, scans and
     * CAS-rewrites a whole record per tick and per `set`/`clear`, so this is
     * the number its cost grows with: at ~10 000 entries a set is ~5 ms, at
     * ~100 000 it is ~1 s and most wakes miss their tick. Past ~1 000 the
     * host dev-warns once that a due-time-indexed provider is the answer.
     * `0` under a provider that does not shard. OPTIONAL as above.
     */
    reminderShardEntriesMax?: number;
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
 * What a host that is not running reports. A FACTORY, not a shared frozen
 * constant: this lands in an ops snapshot and a `HostReport`, both of which
 * a caller may reasonably transform in place.
 */
export const emptyHostStats = (): HostStats => ({
    activations: 0,
    queued: 0,
    perType: {},
    transitional: { activating: 0, deactivating: 0 },
    watchLoops: 0,
    remindersUndelivered: 0,
    overloadRefusals: 0,
    reminderShardEntriesMax: 0
});

/** One live activation, as `Host.activations()` reports it. */
export interface ActivationInfo {
    type: string;
    key: string;
    /** Turns depth: unsettled turns — queued plus running (an
     *  interleaving actor can have several running). */
    queued: number;
    /** Since activation, ms. Monotonic. */
    ageMs: number;
    /** Since the last turn, ms. Wall clock — this is what idle collection reads. */
    idleMs: number;
    /** Held open — by a stream, a watch, or a running detached task — so
     *  idle sweeping skips it. When `tasks > 0`, the tasks are the reason. */
    keptAlive: boolean;
    /** Detached task runs currently held by this activation — the actors
     *  hosting long-running work. Counted in `keptAlive` too. */
    tasks: number;
    /** Shared watch loops on this activation — one per watched
     *  `(method, throttleMs, args)`, or per distinct principal once the
     *  read is observed consulting `ctx.principal` (#121). */
    watchLoops: number;
    /** Live watch subscribers across those loops. Healthy sharing is many
     *  subscribers per loop; a ratio near 1 with a high loop count is the
     *  per-identity split that collapses fan-out (#180). */
    watchSubscribers: number;
}

export interface ActivationsOptions {
    /**
     * How many to return. Default 100.
     *
     * Bounded on purpose, and low: a host can hold millions of activations,
     * and this walks them all to sort. It is a "top N" view, not an export.
     */
    limit?: number;
    /**
     * `'queued'` (most queued turns first) — the hot actors.
     * `'age'` (oldest first) — the long-lived ones.
     * `'idle'` (most idle first) — the next sweep's candidates.
     * Default `'queued'`.
     */
    sortBy?: 'queued' | 'age' | 'idle';
    /** Only this actor type. */
    type?: string;
}

export interface Host extends ActorDispatcher {
    /** Definition lookup — the wire resolver's 404 authority. May load lazily. */
    definition(type: string): AnyActorDefinition | Promise<AnyActorDefinition | null> | null;
    /**
     * Every registered actor type name — array- and lazy-registered alike,
     * workers included — sorted, stable for the host's lifetime, and
     * gathered WITHOUT loading lazy modules (registry keys, not
     * definitions).
     *
     * A cluster placement publishes this in the membership descriptor
     * (`HostDescriptor.types`) so placement can be registration-aware
     * (#212). Optional so a hand-rolled `Host` predating it keeps
     * compiling; a placement treats its absence like a descriptor without
     * `types` — eligible for everything, the legacy direction.
     */
    registeredTypes?(): readonly string[];
    /**
     * Where an actor lives, without dispatching or activating — delegates to
     * the placement's `locate()`.
     *
     * `undefined` means "cannot answer", which is what a single-node host
     * and any placement without `locate()` return. Callers must treat that
     * as "assume local / just dispatch", never as an error — one check
     * covers both a placement that opts out and a host built before the
     * seam existed.
     *
     * The endpoint uses this to answer 421 with the owner instead of
     * proxying; nothing else should need it, because dispatching already
     * routes correctly on its own.
     */
    locate?(ref: ActorRef): ActorLocation | Promise<ActorLocation> | undefined;
    actor<D extends AnyActorDefinition>(def: D, key: string): ActorClientWith<D>;
    /**
     * Publish to a topic from OUTSIDE any actor (a serverFn, a script, a
     * timer). An external call: fresh chain, default deadline. See
     * `ActorContextBase.publish` for the in-turn form, which carries the
     * caller's chain.
     */
    publish<T>(topic: Topic<T>, payload: T, options?: PublishOptions): Promise<TopicPublishReport>;
    /** Starts sweeper + reminders and stamps the host seam. Idempotent. */
    start(): Promise<void>;
    /** Drain, flush, clear the seam. Default timeout 30s. */
    stop(opts?: { timeoutMs?: number }): Promise<void>;
    /**
     * Gracefully deactivate ONE activation (drain its turns, flush,
     * forget). No-op if the actor isn't active here. `reason` defaults to
     * `'explicit'`; a cluster rebalancer passes `'migrated'`.
     */
    deactivate(ref: ActorRef, reason?: DeactivationReason): Promise<void>;
    /** Deactivate every activation of one type (dev/HMR hook). */
    deactivateType(type: string): Promise<void>;
    stats(): HostStats;
    /**
     * The live activations themselves, bounded and sorted — what `stats()`
     * collapses into counts.
     *
     * This is the "top actors" view: which keys are hot, which are old,
     * which are about to be swept. Without it a dashboard can say a host
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
