/**
 * One live activation: state signal, method tables, mailbox, timers, and the
 * change feed. Created lazily by the local host on first dispatch; nothing
 * outside this file touches activation memory.
 */
import { effectScope, signal, toRaw, watch } from '@sigx/reactivity';
import { createSharedWatch, watchKey, type SharedWatch } from './watch';
import { mintCallId } from '../call-id';
import type { Mailbox } from './mailbox';
import {
    ActorActivationError,
    ActorMethodNotFoundError,
    ActorStateConflictError,
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
    type TimerHandle,
    type TimerOptions
} from '../types';

/** Reserved dispatch method routing to `onReminder`. */
export const REMINDER_METHOD = '$sigx:reminder';

/** Change-feed buffer bound — drop-oldest beyond this. */
const CHANGE_BUFFER = 16;

/** Trailing-throttle window for a change-driven read (`openWatch`). */
export const DEFAULT_WATCH_THROTTLE_MS = 50;

/** Keys a context extension may never set — they reach the prototype. */
const UNSAFE_CONTEXT_KEYS = new Set(['__proto__', 'constructor', 'prototype']);

/** What the silo provides to every activation. */
export interface ActivationHost {
    readonly idleAfterMs: number;
    readonly slowTurnMs: number;
    /**
     * Optional turn observer. Its PRESENCE is what turns on the extra
     * timestamps — with none set the hot path is byte for byte what it was
     * before this seam existed.
     *
     * MUTABLE on purpose: the silo clears it the moment the last observer
     * unsubscribes, which is what makes switching observation off at runtime
     * actually cheap. A boolean checked inside a still-registered observer
     * would keep paying for the two clock reads below — the larger half of
     * the cost — so "off" has to mean absent, not inert.
     */
    onTurn?: ActorTurnObserver;
    /** Clock for `ctx.timer` and write-behind flushes. */
    readonly scheduler: ActorScheduler;
    loadState(ref: ActorRef): Promise<{ state: object; etag: string } | null>;
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
    actorClient<D extends AnyActorDefinition>(
        def: D,
        key: string,
        parentCall: () => ActorCallContext | null
    ): ActorClient<D>;
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

interface ChangeSub {
    queue: object[];
    wake: (() => void) | null;
    done: boolean;
}

type AnyFn = (...args: unknown[]) => unknown;
type AnyStreamFn = (...args: unknown[]) => AsyncIterable<unknown>;

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
    #streams: Record<string, AnyStreamFn> = {};
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
            const stored = await host.loadState(ref);
            const initial = stored ? stored.state : (opts.state(ref.key) as object);
            a.#etag = stored ? stored.etag : null;
            a.#state = signal(initial);
            a.#ctx = a.#buildContext();
            // The factories (and onActivate) run inside the activation's
            // effect scope so computeds/watches they create die with it.
            a.#scope.run(() => {
                a.#methods = opts.methods(a.#ctx) as Record<string, AnyFn>;
                if (opts.streams) {
                    // Stream bodies get a DERIVED context whose `state` warns:
                    // bracketing the dispatch call sites cannot work, because
                    // an async generator's return() awaits before resuming, so
                    // a `finally` reading ctx.state runs in a later microtask.
                    // Handing the factory its own context makes the guard exact
                    // and timing-independent — and no mailbox turn can trip it,
                    // since `methods` still gets the real ctx.
                    a.#streams = opts.streams(
                        __DEV__ ? a.#streamContext() : a.#ctx
                    ) as Record<string, AnyStreamFn>;
                }
            });
            if (opts.persistence && typeof opts.persistence === 'object') {
                a.#ensureDeepWatch();
            }
            if (opts.onActivate) {
                await a.#scope.run(() => opts.onActivate!(a.#ctx));
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
        return shared.subscribe();
    }

    openStream(
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): AsyncIterable<unknown> {
        const setup = this.mailbox.run(async () => {
            if (this.#faulted) throw this.#faulted;
            const fn = this.#streams[method];
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

        return {
            [Symbol.asyncIterator](): AsyncIterator<unknown> {
                return {
                    async next(): Promise<IteratorResult<unknown>> {
                        try {
                            const gen = await setup;
                            const result = await gen.next();
                            if (result.done) release();
                            return result;
                        } catch (error) {
                            release();
                            throw error;
                        }
                    },
                    async return(): Promise<IteratorResult<unknown>> {
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
     * Graceful deactivation: close the mailbox, drain queued turns, run
     * onDeactivate, flush a pending write-behind save, tear down.
     */
    async deactivate(reason: DeactivationReason): Promise<void> {
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
        this.#abort.abort();
        this.#scope.stop();
        for (const sub of this.#subs) {
            sub.done = true;
            sub.wake?.();
        }
        this.#subs.clear();
    }

    /** Force-drop on shutdown deadline: abort and tear down without drain. */
    forceStop(): void {
        this.mailbox.close();
        for (const t of this.#timers.values()) t.clear();
        this.#timers.clear();
        this.#abort.abort();
        this.#scope.stop();
        for (const sub of this.#subs) {
            sub.done = true;
            sub.wake?.();
        }
        this.#subs.clear();
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
        const fn = this.#methods[method];
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
     * Dev-only context for `streams:` bodies: the real context with `state`
     * shadowed by a warning accessor. Stream bodies run detached from the
     * mailbox, so a turn can mutate underneath a live read — they must use
     * `snapshot()` / `changes()`. Everything else is inherited unchanged
     * (the built-in accessors close over `self`, not `this`).
     */
    #streamContext(): ActorContext<object> {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const self = this;
        const derived = Object.create(this.#ctx) as ActorContext<object>;
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

    #buildBaseContext(): ActorContext<object> {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const self = this;
        const opts = this.def.__sigxActor;
        return {
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
            reminders: this.#host.reminders(this.ref),
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
            deactivate(): void {
                self.#deactivateRequested = true;
            },
            abortSignal: this.#abort.signal,
            snapshot(): object {
                return self.#snapshot();
            },
            changes(options?: { initial?: boolean }): AsyncIterable<object> {
                // The feed needs change detection even in explicit mode.
                self.#ensureDeepWatch();
                const sub: ChangeSub = { queue: [], wake: null, done: false };
                // Seed BEFORE the subscription goes live, in this same
                // synchronous call: a `yield ctx.snapshot()` prologue would
                // instead subscribe only after the consumer resumes, losing
                // every mutation in that window.
                if (options?.initial) sub.queue.push(self.#snapshot());
                self.#subs.add(sub);
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
                                // Wake a parked next(). Marking `done` is not
                                // enough: a consumer that disconnects while
                                // the feed is quiet is sitting on the wake
                                // promise, and nothing else will ever resolve
                                // it — the actor may never mutate again. The
                                // await inside `return()` then never settles
                                // and teardown hangs.
                                sub.wake?.();
                                return { value: undefined, done: true };
                            }
                        };
                    }
                };
            }
        };
    }
}

export { mintCallId };
