/**
 * The single-node host: directory (type+key → activation slot), lazy
 * activation, deadlock detection, and deactivation orchestration. This is
 * the v1 `ActorDispatcher`; a Durable Objects or cluster placement replaces
 * exactly this class with a fetch-forwarding stub.
 */
import {
    ActorActivationError,
    ActorDeadlockError,
    ActorError,
    HostShutdownError
} from '../errors';
import {
    actorId,
    actorLabel,
    resolveLimit,
    type ActivationInfo,
    type ActivationsOptions,
    type ActorCallContext,
    type ActorDispatcher,
    type ActorRef,
    type AnyActorDefinition,
    type DeactivationReason,
    type PlacementBindings,
    type HostStats
} from '../types';
import { Activation, type ActivationHost } from './activation';
import { CallDeadlines } from './deadlines';
import { Turns } from './turns';
import { effectiveReentrancy } from './reentrancy';

/** Bounded low: this is a "top N" view, and the walk is O(activations). */
const DEFAULT_ACTIVATIONS_LIMIT = 100;

/**
 * Directory id of one stateless pool member: `NUL + actorId + NUL + seq`.
 * The LEADING NUL cannot collide with any `actorId` — types are non-empty
 * and NUL-free, so every actorId starts with a non-NUL character — and the
 * trailing token is digits-only, so `baseIdOf` decomposes uniquely even for
 * keys that themselves contain NUL.
 */
const NUL = String.fromCharCode(0); // a literal would put raw bytes in the source
const memberId = (id: string, seq: number): string => NUL + id + NUL + seq;
const isMemberId = (id: string): boolean => id.charCodeAt(0) === 0;
const baseIdOf = (mid: string): string => mid.slice(1, mid.lastIndexOf(NUL));

/**
 * Default pool cap, WinterCG-safe: `navigator.hardwareConcurrency` exists in
 * browsers, Deno, Bun and Node ≥ 21; workerd may omit it. Clamped to
 * [4, 16]: a 128-core host should not default to 128 members of one worker
 * key, and — the floor (#147) — a CPU-limited container should not default to
 * a single member. Under a cgroup CPU quota `hardwareConcurrency` is the
 * QUOTA, not the machine (Node derives it from `os.availableParallelism()`),
 * so `resources.limits.cpu: 1` reports 1 — and members are activations
 * overlapping at `await` points, not threads, so they need no second core.
 * The floor matches the no-navigator fallback of 4. No `node:os` anywhere on
 * this path: this class runs on workerd too.
 */
function defaultMaxLocal(): number {
    const n = (globalThis as { navigator?: { hardwareConcurrency?: number } }).navigator
        ?.hardwareConcurrency;
    return typeof n === 'number' && n >= 1 ? Math.min(Math.max(Math.trunc(n), 4), 16) : 4;
}

type Slot =
    | { phase: 'activating'; promise: Promise<Activation> }
    | { phase: 'active'; activation: Activation }
    | { phase: 'deactivating'; drained: Promise<void> };

/**
 * One stateless type+key's pool of interchangeable members. Members are
 * ordinary directory slots under `memberId(...)` ids, so every walk of
 * `#directory` (stats, sweep, stopAll) sees them for free; this record only
 * carries the pick/spin bookkeeping. Invariant: a member id is listed here
 * exactly while its slot exists (or is being reserved) in the directory.
 */
interface WorkerPool {
    /** `stateless.maxLocal ?? defaultMaxLocal()`, resolved once per pool. */
    readonly cap: number;
    readonly def: AnyActorDefinition;
    /**
     * Member counter, monotonic within THIS pool record. A re-created pool
     * (deleted once empty, rebuilt on the next dispatch) restarts at 0 —
     * safe, because `#forgetMember` runs only after the member's directory
     * slot is deleted, so an empty pool proves no slot under its ids
     * remains and a fresh `memberId(id, 0)` cannot collide.
     */
    seq: number;
    /** Live member directory ids, in creation order (the pick tiebreak). */
    readonly members: string[];
}

export class LocalHost implements ActorDispatcher {
    #directory = new Map<string, Slot>();
    /** Stateless pools, keyed by `actorId(ref)`. */
    #pools = new Map<string, WorkerPool>();
    #deadlines = new CallDeadlines();
    #host: ActivationHost;
    #resolveDefinition: (type: string) => AnyActorDefinition | Promise<AnyActorDefinition | null> | null;
    #shuttingDown = false;
    /** Read per use, not captured: `placement.bind()` runs after construction. */
    #bindings: () => PlacementBindings | undefined;

    constructor(
        host: ActivationHost,
        resolveDefinition: (
            type: string
        ) => AnyActorDefinition | Promise<AnyActorDefinition | null> | null,
        bindings: () => PlacementBindings | undefined = () => undefined
    ) {
        this.#host = host;
        this.#resolveDefinition = resolveDefinition;
        this.#bindings = bindings;
    }

    // Deliberately NOT `async`: `#dispatchInner` cannot throw synchronously
    // (its try/catch converts the prologue's throws to rejections), so an
    // outer promise would only add an allocation and a tick to every call.
    dispatch(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): Promise<unknown> {
        // One sync compare on the normal hot path — zero added microtasks.
        if (call.oneWay === true) return this.#dispatchOneWay(ref, method, args, call);
        const work = this.#dispatchInner(ref, method, args, call);
        return call.deadline === undefined
            ? work
            : this.#deadlines.race(work, call.deadline, ref, method);
    }

    /**
     * One-way: resolve at ACCEPTANCE — the moment the turn is enqueued —
     * and detach from its settlement.
     *
     * Deliberate differences from `#dispatchInner`:
     *  - No `#checkReentrancy`: an unawaited call cannot deadlock. A one-way
     *    self-call on a non-reentrant actor queues behind the current turn
     *    ("schedule more work for myself") instead of throwing
     *    `ActorDeadlockError`, and it must not run inline — inline is the
     *    awaited-cycle rescue, and nothing awaits here.
     *  - No `#deadlines.race` for the caller: the deadline still rides the
     *    context (bounding the turn's own nested awaited calls), but the
     *    caller has already resolved.
     *  - The synchronous `.catch` keeps a post-acceptance failure out of
     *    unhandled-rejection reporting; `metrics()` counts it via the turn
     *    observer (`oneWayFailures`). Everything before `enqueue` — shutdown,
     *    unknown type, activation failure — is pre-acceptance and still
     *    rejects the caller through this async method's own promise.
     */
    async #dispatchOneWay(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): Promise<unknown> {
        this.#checkShutdown(call);
        const activation = await this.#activationFor(ref);
        // `Turns.run` on closed turns returns a rejection; check first
        // so shutdown stays a PRE-acceptance failure, not a swallowed one.
        // No await separates this check from the enqueue below, and
        // `Activation.enqueue` is synchronous straight into `Turns.run`,
        // so turns cannot close in between — the closed-refusal
        // rejection is unreachable past this line, and the catch below can
        // only ever swallow a genuine post-acceptance turn failure.
        if (activation.turns.closed) throw new HostShutdownError();
        activation.enqueue(method, args, call).catch((error) => {
            if (__DEV__) {
                console.warn(
                    `[sigx actors] one-way call ${actorLabel(ref)}.${method}() failed after ` +
                        `acceptance — the error is dropped (counted as oneWayFailures when ` +
                        `metrics() is attached).`,
                    error
                );
            }
        });
        return undefined;
    }

    dispatchStream(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): AsyncIterable<unknown> {
        // Must return synchronously; resolve the activation on first pull.
        return this.#lazyIterable(async () => {
            this.#checkShutdown(call);
            // An in-chain open against a call-chain-reentrant target resolves
            // the generator INLINE. Dropping this result queued the setup turn
            // behind the up-stack turn awaiting the first chunk — a hang where
            // the mode promised a re-entry (#46). `inline` IS the active
            // activation for this id, so it also saves a lookup.
            const inline = await this.#checkReentrancy(ref, call, method);
            const activation = inline ?? (await this.#activationFor(ref));
            return activation
                .openStream(method, args, call, inline !== null)
                [Symbol.asyncIterator]();
        });
    }

    dispatchWatch(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext,
        options?: { throttleMs?: number }
    ): AsyncIterable<unknown> {
        return this.#lazyIterable(async () => {
            this.#checkShutdown(call);
            if ((await this.#definition(ref.type)).__sigxActor.stateless !== undefined) {
                // A watch is a state-change feed, and a worker has no state:
                // the subscription would attach to one arbitrary pool member
                // and never fire. Refuse loudly — and BEFORE activating, so
                // the refusal does not spin a member.
                throw new ActorError(
                    'method-not-found',
                    `[sigx actors] ${actorLabel(ref)} is a stateless worker — workers have no ` +
                        `state to watch.`
                );
            }
            // A watch cannot take the inline rescue a stream does, so an
            // in-chain open is refused rather than left to hang (#46). A
            // stream's setup only resolves a generator; a watch is a
            // long-lived subscription whose reads are turns of their own
            // ("the isolation every other call has" — `openWatch`), so only
            // its FIRST read could ever run inline. And the loop is shared per
            // (method, args, throttleMs): a subscriber joining one whose
            // initial read is already queued behind the up-stack turn would
            // deadlock however that first read ran.
            if (await this.#checkReentrancy(ref, call, method)) {
                throw new ActorError(
                    'deadlock',
                    `[sigx actors] ${actorLabel(ref)}.${method}() — a watch cannot re-enter an ` +
                        `up-stack turn: a watch is a long-lived subscription whose reads are ` +
                        `turns of their own, so only its first read could ever run inline. Open ` +
                        `the watch outside the turn.`
                );
            }
            const activation = await this.#activationFor(ref);
            return activation.openWatch(method, args, call, options)[Symbol.asyncIterator]();
        });
    }

    #lazyIterable(open: () => Promise<AsyncIterator<unknown>>): AsyncIterable<unknown> {
        let inner: Promise<AsyncIterator<unknown>> | null = null;
        return {
            [Symbol.asyncIterator]: () => ({
                next: async () => {
                    inner ??= open();
                    return (await inner).next();
                },
                return: async () => {
                    if (inner) {
                        // A failed open (deadlock, unknown method, shutdown)
                        // already rejected the pull that triggered it. The
                        // consumer's `finally` lands here next, and rethrowing
                        // the same rejection out of `return()` would REPLACE
                        // the error it is unwinding from with a duplicate.
                        const it = await inner.catch(() => null);
                        if (it?.return) await it.return(undefined);
                    }
                    return { value: undefined, done: true as const };
                }
            })
        };
    }


    // A plain function, not `async` (#24): the warm path below returns the
    // enqueue promise straight through, and the `async` wrapper used to cost
    // a promise allocation and ~2 microtask ticks on every call for nothing.
    // The try/catch keeps the frame's contract intact — the prologue's sync
    // throws (`#checkShutdown`) still surface as rejections, tick-identical
    // to what an async function throwing before its first await produced.
    #dispatchInner(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): Promise<unknown> {
        try {
            return this.#dispatchWarmOrSlow(ref, method, args, call);
        } catch (error) {
            return Promise.reject(error);
        }
    }

    #dispatchWarmOrSlow(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): Promise<unknown> {
        this.#checkShutdown(call);
        const id = actorId(ref);
        // The warm path, which is nearly every dispatch. Both helpers below
        // are `async`, so entering either allocates a promise and burns a
        // microtask tick even when it returns without awaiting anything —
        // measured at ~12% of `dispatch/warm-actor` (see BASELINES.md, "Where
        // the time goes"). This condition is exactly the conjunction of their
        // two early returns — `#checkReentrancy`'s chain miss and
        // `#activationFor`'s `active` slot hit — so the outcome is identical
        // and the slow paths below are untouched.
        //
        // The call-chain test is NOT optional: on a self-call the slot is
        // `active` too, and skipping it would enqueue behind the turn that is
        // up-stack awaiting us — a permanent hang where the runtime owes a
        // loud `ActorDeadlockError`.
        //
        // Interleaving (`reentrant: 'always'` / `methodReentrancy`) needs no
        // branch here: the interleave decision lives inside
        // `Activation.enqueue`, so this shortcut serves those actors
        // unchanged, and their in-chain calls fall through to
        // `#checkReentrancy`, which admits them as normal (interleaved)
        // dispatches instead of inline runs.
        if (!call.callChain.includes(id)) {
            const warm = this.#directory.get(id);
            if (warm?.phase === 'active') return warm.activation.enqueue(method, args, call);
            // Stateless pool: a synchronous fewest-queued-turns pick. Ordered
            // AFTER the warm hit above so the stateful fast path is
            // byte-for-byte untouched (a base-id slot and a pool are mutually
            // exclusive); the uncontended stateless path is itself two sync
            // Map reads + enqueue — zero added microtasks.
            const pool = this.#pools.get(id);
            if (pool) {
                const target = this.#poolTarget(pool, ref, id);
                return target instanceof Activation
                    ? target.enqueue(method, args, call)
                    : target.then((a) => a.enqueue(method, args, call));
            }
        }
        return this.#dispatchSlow(ref, method, args, call, id);
    }

    /** The cold/reentrant tail, hoisted verbatim — the only frame that awaits. */
    async #dispatchSlow(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext,
        id: string
    ): Promise<unknown> {
        const inline = await this.#checkReentrancy(ref, call, method, id);
        if (inline) return inline.runInline(method, args, call);
        const activation = await this.#activationFor(ref, id);
        return activation.enqueue(method, args, call);
    }

    #checkShutdown(call: ActorCallContext): void {
        // In-chain dispatches still pass — a draining turn must be able to
        // finish its actor-to-actor calls.
        if (this.#shuttingDown && call.callChain.length === 0) throw new HostShutdownError();
    }

    /**
     * The target is already in this call chain → its turn is up-stack
     * awaiting us. Call-chain reentrant: run inline against that turn.
     * Interleaving ('always', or the method is mapped in
     * `methodReentrancy`): fall through to a normal dispatch — the enqueue
     * launches immediately instead of waiting behind the tail, so the cycle
     * completes as a concurrent turn, never inline. Non-reentrant: throw
     * now with the full chain instead of hanging until a timeout.
     *
     * EVERY caller must act on the returned activation, not merely on the
     * throw. Three do, and each answers "run inline against that turn"
     * differently: `#dispatchSlow` calls `runInline`, `dispatchStream`
     * resolves the generator without a setup turn, and `dispatchWatch` — the
     * one shape that has no inline form — refuses. Discarding it is the #46
     * bug: the cycle is admitted and then queued behind the turn awaiting it.
     */
    async #checkReentrancy(
        ref: ActorRef,
        call: ActorCallContext,
        method: string,
        knownId?: string
    ): Promise<Activation | null> {
        const id = knownId ?? actorId(ref);
        if (!call.callChain.includes(id)) return null;
        const slot = this.#directory.get(id);
        const active = slot?.phase === 'active' ? slot.activation : null;
        const mode = effectiveReentrancy(
            active ? active.def.__sigxActor : (await this.#definition(ref.type)).__sigxActor,
            method
        );
        if (mode === 'none') {
            throw new ActorDeadlockError([...call.callChain, id]);
        }
        if (!active) {
            // Chain says it's up-stack but the slot is gone. Single-node
            // this shouldn't happen; fall back to a normal dispatch. In a
            // cluster it means the turn runs on ANOTHER host — activating a
            // second copy here would break single-activation, so a strict
            // placement makes it a loud, retryable error instead. Applies
            // to interleaving targets too: 'always' changes turn
            // scheduling, never where the single activation lives.
            if (this.#bindings()?.strictChainPresence) {
                throw new ActorError(
                    'deadlock',
                    `[sigx actors] ${actorLabel(ref)} is mid-turn in this call chain but has ` +
                        `no activation on this host — the activation moved mid-call. Retry.`
                );
            }
            return null;
        }
        return mode === 'always' ? null : active;
    }

    async #definition(type: string): Promise<AnyActorDefinition> {
        const def = await this.#resolveDefinition(type);
        if (!def) {
            throw new ActorActivationError(`${type}/?`, {
                cause: new Error(
                    `[sigx actors] unknown actor type "${type}" — is it registered with createHost({ actors })?`
                )
            });
        }
        return def;
    }

    async #activationFor(ref: ActorRef, knownId?: string): Promise<Activation> {
        const id = knownId ?? actorId(ref);
        for (;;) {
            // Stateless types never occupy a base-id slot — they live as a
            // pool of member slots and dispatch through the pick below.
            const pool = this.#pools.get(id);
            if (pool) return this.#poolTarget(pool, ref, id);
            const slot = this.#directory.get(id);
            if (!slot) {
                // The definition is resolved BEFORE the reservation now: the
                // stateless branch must not create a base-id slot. For
                // array-registered hosts this is a synchronous Map hit, so
                // the stateful reservation below is still synchronous from
                // function entry — the race invariant is intact. A lazy
                // registry pays one await, then re-enters the loop because
                // the slot may have been reserved while it waited.
                const resolved = this.#resolveDefinition(ref.type);
                if (resolved && typeof (resolved as PromiseLike<unknown>).then === 'function') {
                    const loaded = await (resolved as Promise<AnyActorDefinition | null>);
                    if (!loaded) throw this.#unknownType(ref);
                    continue;
                }
                const def = resolved as AnyActorDefinition | null;
                if (!def) throw this.#unknownType(ref);
                if (def.__sigxActor.stateless !== undefined) {
                    return this.#poolTarget(this.#ensurePool(id, def), ref, id);
                }
                // Reserve SYNCHRONOUSLY so a racing second dispatch joins
                // this promise — the single-activation invariant. (`reserved`
                // is only read after the first await inside the closure.)
                const turns = new Turns();
                let reserved!: Slot;
                const promise = (async () => {
                    // The distributed directory's claim point: a throw here
                    // (wrong-host) rejects every parked caller and the slot
                    // is dropped below — nothing activates.
                    await this.#bindings()?.beforeActivate?.(ref);
                    const activation = await Activation.create(ref, def, this.#host, turns);
                    const current = this.#directory.get(id);
                    if (current === reserved) {
                        this.#directory.set(id, { phase: 'active', activation });
                    }
                    return activation;
                })();
                reserved = { phase: 'activating', promise };
                this.#directory.set(id, reserved);
                promise.catch(() => {
                    // Activation failed: every parked caller rejects with the
                    // ActorActivationError; nothing is remembered.
                    if (this.#directory.get(id) === reserved) this.#directory.delete(id);
                });
                return promise;
            }
            if (slot.phase === 'activating') return slot.promise;
            if (slot.phase === 'active') return slot.activation;
            // Deactivating: park until drained, then re-activate fresh
            // (calls during deactivation wait, not fail).
            await slot.drained;
        }
    }

    #unknownType(ref: ActorRef): ActorActivationError {
        return new ActorActivationError(`${ref.type}/?`, {
            cause: new Error(
                `[sigx actors] unknown actor type "${ref.type}" — is it registered with createHost({ actors })?`
            )
        });
    }

    #ensurePool(id: string, def: AnyActorDefinition): WorkerPool {
        // Idempotent: two racing cold dispatches both land here; the second
        // adopts the first's pool.
        let pool = this.#pools.get(id);
        if (!pool) {
            pool = {
                cap: def.__sigxActor.stateless?.maxLocal ?? defaultMaxLocal(),
                def,
                seq: 0,
                members: []
            };
            this.#pools.set(id, pool);
        }
        return pool;
    }

    /**
     * The pool pick — SYNCHRONOUS unless it has to spin. The rule:
     *
     *  1. an idle active member (depth 0) → take it; ties break on creation
     *     order, so the pick is deterministic;
     *  2. everyone busy and room under the cap → spin a fresh member and ride
     *     it (this is what makes same-key concurrency real — queueing behind
     *     a busy member and spinning one nobody uses would leave the second
     *     caller serialized);
     *  3. at the cap → queue on the activation with the fewest queued turns (backpressure, exactly
     *     like a stateful actor);
     *  4. nothing active yet → join the first activating member.
     *
     * The cap counts active + activating members. A DEACTIVATING member is
     * not counted: it no longer accepts work, so replacing it while it
     * drains is the same transient overlap a stateful re-activation has.
     */
    #poolTarget(pool: WorkerPool, ref: ActorRef, id: string): Activation | Promise<Activation> {
        let best: Activation | null = null;
        let firstActivating: Promise<Activation> | null = null;
        let live = 0;
        for (const mid of pool.members) {
            const slot = this.#directory.get(mid);
            if (!slot || slot.phase === 'deactivating') continue;
            if (slot.phase === 'active') {
                live++;
                if (!best || slot.activation.turns.depth < best.turns.depth) {
                    best = slot.activation;
                }
            } else {
                live++;
                firstActivating ??= slot.promise;
            }
        }
        if (best && best.turns.depth === 0) return best;
        if (live < pool.cap) return this.#spinMember(pool, ref, id);
        if (best) return best;
        return firstActivating ?? this.#spinMember(pool, ref, id);
    }

    /** Reserve and activate one fresh pool member (synchronous reservation,
     *  same shape as `#activationFor` — minus the directory claim: a
     *  stateless member is never claimed, locality is the contract). */
    #spinMember(pool: WorkerPool, ref: ActorRef, id: string): Promise<Activation> {
        const mid = memberId(id, pool.seq++);
        pool.members.push(mid);
        const turns = new Turns();
        let reserved!: Slot;
        const promise = (async () => {
            const activation = await Activation.create(ref, pool.def, this.#host, turns);
            const current = this.#directory.get(mid);
            if (current === reserved) {
                this.#directory.set(mid, { phase: 'active', activation });
            }
            return activation;
        })();
        reserved = { phase: 'activating', promise };
        this.#directory.set(mid, reserved);
        promise.catch(() => {
            if (this.#directory.get(mid) === reserved) this.#directory.delete(mid);
            this.#forgetMember(mid);
        });
        return promise;
    }

    /** Drop a member id from its pool; an empty pool record is removed (the
     *  next dispatch re-creates it), so idle collection returns a quiet
     *  worker key to zero footprint. */
    #forgetMember(mid: string): void {
        const pool = this.#pools.get(baseIdOf(mid));
        if (!pool) return;
        const i = pool.members.indexOf(mid);
        if (i >= 0) pool.members.splice(i, 1);
        if (pool.members.length === 0) this.#pools.delete(baseIdOf(mid));
    }

    // -----------------------------------------------------------------------
    // Lifecycle orchestration (host-facing)

    /**
     * Begin (or join) graceful deactivation. For a stateless pool the ref
     * names the whole (type, key): every member drains.
     */
    deactivate(ref: ActorRef, reason: DeactivationReason): Promise<void> {
        const id = actorId(ref);
        const pool = this.#pools.get(id);
        if (pool) {
            // Snapshot: `#deactivateById` mutates `pool.members` as each
            // member finishes draining.
            return Promise.all(
                [...pool.members].map((mid) => this.#deactivateById(mid, reason))
            ).then(() => {});
        }
        return this.#deactivateById(id, reason);
    }

    /**
     * Deactivate exactly ONE activation — the host's fault/idle-request
     * callbacks target the activation object, and for a pool member that
     * must not take the whole pool down with it.
     */
    deactivateOne(activation: Activation, reason: DeactivationReason): Promise<void> {
        return this.#deactivateById(this.#idFor(activation), reason);
    }

    /** The directory id of an activation: its member id inside a pool, the
     *  plain actorId otherwise. The pool scan is tiny (≤ cap) and only runs
     *  on the rare fault/idle-request paths. */
    #idFor(activation: Activation): string {
        const id = actorId(activation.ref);
        const pool = this.#pools.get(id);
        if (!pool) return id;
        for (const mid of pool.members) {
            const slot = this.#directory.get(mid);
            if (slot?.phase === 'active' && slot.activation === activation) return mid;
        }
        return id; // not found — resolves to a no-op below
    }

    #deactivateById(id: string, reason: DeactivationReason): Promise<void> {
        const slot = this.#directory.get(id);
        if (!slot) return Promise.resolve();
        if (slot.phase === 'deactivating') return slot.drained;
        if (slot.phase === 'activating') {
            // Let the activation finish, then deactivate it.
            return slot.promise.then(() => this.#deactivateById(id, reason)).catch(() => {});
        }
        const activation = slot.activation;
        const member = isMemberId(id);
        let next!: Slot & { phase: 'deactivating' };
        const drained = (async () => {
            try {
                await activation.deactivate(reason);
            } finally {
                if (this.#directory.get(id) === next) this.#directory.delete(id);
                if (member) {
                    // A pool member was never claimed, so there is no
                    // afterDeactivate release to run — only bookkeeping.
                    this.#forgetMember(id);
                } else {
                    // Directory-claim release. Swallowed: a failed release
                    // must not reject callers parked on `drained` — a stale
                    // remote entry is reclaimed lazily by its owner's
                    // liveness.
                    try {
                        await this.#bindings()?.afterDeactivate?.(activation.ref, reason);
                    } catch (error) {
                        if (__DEV__) {
                            console.error(
                                `[sigx actors] afterDeactivate hook for ` +
                                    `${actorLabel(activation.ref)} failed:`,
                                error
                            );
                        }
                    }
                }
            }
        })();
        next = { phase: 'deactivating', drained };
        this.#directory.set(id, next);
        return drained;
    }

    /**
     * The sweep: idle collection, then capacity shedding.
     *
     * Stateless pool members collect INDIVIDUALLY in both passes — each has
     * its own turns and `lastActivityMs` — so this is also how a pool
     * shrinks (both passes are by directory id, never by ref: a by-ref
     * deactivation of one member would drain its whole pool).
     *
     * `maxActivations` is a SOFT cap (0 = unlimited): once the idle pass
     * has run, if more than `maxActivations` activations remain active, the
     * least-recently-used idle, unheld ones are deactivated with reason
     * `'capacity'` — LRU pressure relief before memory pressure does it for
     * us. Soft, deliberately: an activation that is mid-turn, has queued
     * work, or is kept alive by a stream/watch/task is never shed, so a
     * host where every actor is busy can sit over the cap until it quiets.
     * Correctness never depends on the cap — a shed actor re-activates on
     * its next call, state intact.
     */
    sweep(now: number, defaultIdleMs: number, maxActivations = 0): void {
        for (const [id, slot] of this.#directory) {
            if (slot.phase !== 'active') continue;
            const a = slot.activation;
            const idleAfter = a.def.__sigxActor.idleAfterMs ?? defaultIdleMs;
            if (a.idle && !a.keptAlive && now - a.lastActivityMs >= idleAfter) {
                void this.#deactivateById(id, 'idle');
            }
        }
        if (maxActivations <= 0) return;
        // Fresh scan: everything the idle pass just started deactivating has
        // left the 'active' phase, so it neither counts nor sheds twice.
        let active = 0;
        const candidates: { id: string; lastActivityMs: number }[] = [];
        for (const [id, slot] of this.#directory) {
            if (slot.phase !== 'active') continue;
            active++;
            const a = slot.activation;
            if (a.idle && !a.keptAlive) candidates.push({ id, lastActivityMs: a.lastActivityMs });
        }
        const excess = active - maxActivations;
        if (excess <= 0) return;
        // Least recently used first — the ordering that makes the cap an
        // LRU cache rather than a random cull.
        candidates.sort((a, b) => a.lastActivityMs - b.lastActivityMs);
        for (const victim of candidates.slice(0, excess)) {
            void this.#deactivateById(victim.id, 'capacity');
        }
    }

    /** Deactivate every activation of one type (dev/HMR). */
    async deactivateType(type: string): Promise<void> {
        const waits: Promise<void>[] = [];
        for (const [id, slot] of this.#directory) {
            if (slot.phase === 'active' && slot.activation.ref.type === type) {
                waits.push(this.#deactivateById(id, 'explicit'));
            }
        }
        await Promise.all(waits);
    }

    /**
     * Shutdown: close the front door, drain everyone, force-drop
     * stragglers at the deadline. `reason` is `'shutdown'` for a plain
     * stop and `'migrated'` when a cluster placement hands its
     * activations off (peers re-place them).
     */
    async stopAll(timeoutMs: number, reason: DeactivationReason = 'shutdown'): Promise<void> {
        this.#shuttingDown = true;
        const waits: Promise<void>[] = [];
        for (const [id, slot] of this.#directory) {
            if (slot.phase === 'active') {
                waits.push(this.#deactivateById(id, reason));
            } else if (slot.phase === 'deactivating') {
                waits.push(slot.drained);
            } else {
                waits.push(slot.promise.then(
                    () => this.#deactivateById(id, reason),
                    () => {}
                ));
            }
        }
        let timer: ReturnType<typeof setTimeout> | undefined;
        const deadline = new Promise<'timeout'>((r) => {
            timer = setTimeout(() => r('timeout'), timeoutMs);
            (timer as { unref?: () => void }).unref?.();
        });
        const outcome = await Promise.race([
            Promise.allSettled(waits).then(() => 'done' as const),
            deadline
        ]);
        clearTimeout(timer);
        if (outcome === 'timeout') {
            for (const [id, slot] of this.#directory) {
                if (slot.phase === 'active') {
                    if (__DEV__) {
                        console.warn(
                            `[sigx actors] force-dropping ${actorLabel(slot.activation.ref)} at ` +
                                `the shutdown deadline (${timeoutMs}ms).`
                        );
                    }
                    slot.activation.forceStop(reason);
                }
                this.#directory.delete(id);
            }
            this.#pools.clear();
        }
        this.#deadlines.dispose();
    }

    stats(): HostStats {
        let activations = 0;
        let queued = 0;
        let activating = 0;
        let deactivating = 0;
        let watchLoops = 0;
        const perType: Record<string, number> = {};
        for (const [, slot] of this.#directory) {
            // A slot mid-transition has no Activation to read, but it is
            // very much work in progress — counted separately rather than
            // skipped, so an activation storm does not read as an idle host.
            if (slot.phase === 'activating') {
                activating++;
                continue;
            }
            if (slot.phase === 'deactivating') {
                deactivating++;
                continue;
            }
            activations++;
            queued += slot.activation.turns.depth;
            watchLoops += slot.activation.watchLoops;
            perType[slot.activation.ref.type] = (perType[slot.activation.ref.type] ?? 0) + 1;
        }
        return { activations, queued, perType, transitional: { activating, deactivating }, watchLoops };
    }

    activations(options: ActivationsOptions = {}): readonly ActivationInfo[] {
        const limit = resolveLimit(options.limit, DEFAULT_ACTIVATIONS_LIMIT);
        if (limit === 0) return [];
        const sortBy = options.sortBy ?? 'queued';
        const wanted = options.type;
        const now = Date.now();
        const nowMonotonic = performance.now();

        const rows: { activation: Activation; info: ActivationInfo }[] = [];
        for (const [, slot] of this.#directory) {
            if (slot.phase !== 'active') continue;
            const { activation } = slot;
            if (wanted !== undefined && activation.ref.type !== wanted) continue;
            rows.push({
                activation,
                info: {
                    type: activation.ref.type,
                    key: activation.ref.key,
                    queued: activation.turns.depth,
                    ageMs: Math.max(0, Math.round(nowMonotonic - activation.startedMs)),
                    // Clamped: `lastActivityMs` is wall-clock, so an NTP step
                    // backwards mid-activation would otherwise report an actor
                    // that was last used in the future.
                    idleMs: Math.max(0, now - activation.lastActivityMs),
                    keptAlive: activation.keptAlive,
                    tasks: activation.tasks,
                    watchLoops: activation.watchLoops,
                    // Filled below, after the top-N cut: summing handle sets
                    // is O(watch loops), and a poll must pay for the rows it
                    // RETURNS, not every activation on the host.
                    watchSubscribers: 0
                }
            });
        }

        // Descending on the interesting end of each axis: the deepest
        // turns, the oldest activation, the most idle. Ties break on the
        // actor id so the order is stable between polls — a table that
        // reshuffles rows with equal values is unreadable.
        rows.sort(({ info: a }, { info: b }) => {
            const primary =
                sortBy === 'queued'
                    ? b.queued - a.queued
                    : sortBy === 'age'
                      ? b.ageMs - a.ageMs
                      : b.idleMs - a.idleMs;
            if (primary !== 0) return primary;
            return a.type === b.type ? (a.key < b.key ? -1 : a.key > b.key ? 1 : 0) : a.type < b.type ? -1 : 1;
        });
        const top = rows.length > limit ? rows.slice(0, limit) : rows;
        return top.map(({ activation, info }) => {
            info.watchSubscribers = activation.watchSubscribers;
            return info;
        });
    }
}

