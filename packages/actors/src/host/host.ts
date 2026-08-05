/**
 * `createHost` — composition root: registry, codec-bound storage, local
 * host, sweeper, reminders, and the `__SIGX_ACTOR_HOST__` seam lifecycle.
 */
import {
    encodeWithHandlers,
    reviveWithHandlers,
    type TypeHandler
} from '@sigx/serialize';
import { mergeCallBag } from '../call-context-bag';
import { isActorDefinition } from '../define';
import { hasAuthorization, serverAppConfigured } from '../guards';
import { clearHost, stampHost } from '../seam';
import { introspectionMember, isIntrospectionProp } from '../proxy-introspection';
import { assertTopic } from '../topics';
import {
    buildTopicIndex,
    publishToSubscribers,
    type TopicSubscriberEntry
} from './topics';
import type {
    ActivationInfo,
    ActivationsOptions,
    ActorCallContext,
    ActorClientWith,
    ActorPlacement,
    ActorLocation,
    ActorRef,
    ActorReminders,
    ActorStorage,
    ActorTurnObserver,
    AnyActorDefinition,
    DeactivationReason,
    PlacementBindings,
    ActorScheduler,
    Host,
    HostStats,
    PublishOptions,
    Topic,
    TopicPublishReport
} from '../types';
import { actorId } from '../types';
import { mintCallId, REMINDER_METHOD, type Activation, type ActivationHost } from './activation';
import { LocalHost } from './local-host';
import { shardedReminders } from './reminders';
import { taskLedger } from './tasks';
import { memoryStorage } from './storage-memory';
import { timerScheduler } from './scheduler';

export interface HostDefaults {
    /** Idle collection age, ms. Default 20 min — single-node processes
     *  redeploy often and reactivation is one storage load. */
    idleAfterMs?: number;
    /** External-call deadline, ms (becomes `ActorCallContext.deadline`).
     *  Default 30s. `0` disables. */
    callTimeoutMs?: number;
    /**
     * Sweeper cadence, ms. Default 60s. `0` disables idle collection —
     * for a runtime that evicts the whole host when it goes idle, where a
     * sweeper can never usefully fire. A Cloudflare Durable Object is the
     * case: eviction destroys the isolate, the host and the activation
     * together, and nothing keeps the object resident except in-flight work,
     * which is exactly what the sweeper skips. There a permanent interval is
     * worse than useless — a pending timer holds the object in memory and
     * billable, so it would be one billing pin per actor.
     */
    sweepIntervalMs?: number;
    /**
     * SOFT cap on live activations. Default 0 = unlimited. When the sweep
     * finds more than this many active, it deactivates the
     * least-recently-used idle, unheld ones (reason `'capacity'`) — LRU
     * pressure relief before memory pressure does it for us. Soft on
     * purpose: busy, queued, or kept-alive activations are never shed, so
     * the count can sit over the cap while the host is genuinely loaded,
     * and correctness never depends on it — a shed actor re-activates on
     * its next call, state intact. Rides the sweeper, so it needs
     * `sweepIntervalMs > 0`.
     */
    maxActivations?: number;
    /** Reminder loop cadence, ms. Default 30s. */
    reminderTickMs?: number;
    /** __DEV__ slow-turn warning threshold, ms. Default 5s. */
    slowTurnMs?: number;
    /**
     * How long deactivation waits for a detached task to observe its abort
     * signal and settle, ms, before refusing further turns anyway. Default
     * 10s. Counts against `host.stop()`'s own `timeoutMs`, so keep it
     * smaller than that.
     */
    taskGraceMs?: number;
    /**
     * __DEV__: round-trip dispatch args through the codec so same-process
     * code cannot accidentally depend on object identity a remote placement
     * would break. Default false (costs an encode per call).
     */
    devSerializeChecks?: boolean;
}

export interface CreateHostOptions {
    /**
     * Explicit registration — an array of definitions, or the lazy
     * `virtual:sigx-actors` map (`{ [type]: () => import(...) }`).
     */
    actors:
        | readonly AnyActorDefinition[]
        | Record<string, () => Promise<AnyActorDefinition | Record<string, unknown>>>;
    /** Omit = in-memory (dev-warned: state dies with the process). */
    storage?: ActorStorage;
    /** Placement seam; default = the local single-node host. */
    placement?: ActorPlacement;
    /** Extra codec handlers for state persistence and dev checks. */
    types?: readonly TypeHandler[];
    /**
     * Durable-reminder implementation. Default `shardedReminders()` — the
     * table in `ActorStorage`, split into hash shards. Replace it where
     * that model does not fit: a Cloudflare Durable Object holds ONE actor,
     * so its reminders live in its own storage and fire from its own alarm.
     */
    reminders?: ActorReminders;
    /**
     * The clock for the sweeper, reminder tick, `ctx.timer` and write-behind
     * flushes. Default `timerScheduler()` (host timers). Replace it on a
     * runtime without background execution — a Worker never fires an
     * interval registered at startup — or with `manualScheduler()` in tests.
     */
    scheduler?: ActorScheduler;
    /**
     * Extra members merged onto every activation's `ctx`. Built-in members
     * are never overwritten. `defineActorApp` folds every plugin's
     * `extendContext` into this one function; hand-rolled hosts can pass it
     * directly.
     */
    extendContext?: (ref: ActorRef) => object | undefined;
    /**
     * Observe every dispatched turn's queue wait and execution time.
     * `defineActorApp` folds every plugin's `observeTurns` into this one
     * function; hand-rolled hosts can pass it directly. Omit it and the
     * dispatch path is unchanged — the timestamps are only taken when an
     * observer exists.
     */
    onTurn?: ActorTurnObserver;
    defaults?: HostDefaults;
}

interface ResolvedDefaults {
    idleAfterMs: number;
    callTimeoutMs: number;
    sweepIntervalMs: number;
    maxActivations: number;
    reminderTickMs: number;
    slowTurnMs: number;
    taskGraceMs: number;
    devSerializeChecks: boolean;
}

/** What the host client's `.with()` accepts — the server-side subset of
 *  `ActorCallOptions` the host itself can honour. */
interface HostCallOptions {
    signal?: AbortSignal;
    oneWay?: true;
    /** Explicit context-bag entries, merged (explicit wins) over whatever
     *  the call inherits; validated by `mergeCallBag` (throws). */
    bag?: Readonly<Record<string, string>>;
    /**
     * The entry point's ENCODED principal (rfc-server-v4 §7) — set by the
     * endpoints and the in-process client after the pipeline ran, never by
     * an actor. It rides its own envelope slot rather than the bag
     * precisely so `.with({ bag })` cannot forge or clobber identity.
     */
    principal?: string;
}

export function createHost(options: CreateHostOptions): Host {
    return new HostImpl(options);
}

class HostImpl implements Host {
    #defaults: ResolvedDefaults;
    #storage: ActorStorage;
    #types: readonly TypeHandler[];
    #placement: ActorPlacement;
    #bindings: PlacementBindings | undefined;
    #local: LocalHost;
    #reminders: ActorReminders;
    #registry = new Map<
        string,
        AnyActorDefinition | (() => Promise<AnyActorDefinition | Record<string, unknown>>)
    >();
    #resolved = new Map<string, AnyActorDefinition>();
    #scheduler: ActorScheduler;
    #host!: ActivationHost;
    #turnObservers = new Set<ActorTurnObserver>();
    #topicIndex: Promise<Map<string, TopicSubscriberEntry[]>> | null = null;
    #stopSweeper: (() => void) | null = null;
    #started = false;
    #startPromise: Promise<void> | null = null;
    #stopped = false;

    constructor(options: CreateHostOptions) {
        this.#defaults = {
            idleAfterMs: options.defaults?.idleAfterMs ?? 20 * 60_000,
            callTimeoutMs: options.defaults?.callTimeoutMs ?? 30_000,
            sweepIntervalMs: options.defaults?.sweepIntervalMs ?? 60_000,
            maxActivations: options.defaults?.maxActivations ?? 0,
            reminderTickMs: options.defaults?.reminderTickMs ?? 30_000,
            slowTurnMs: options.defaults?.slowTurnMs ?? 5_000,
            taskGraceMs: options.defaults?.taskGraceMs ?? 10_000,
            devSerializeChecks: options.defaults?.devSerializeChecks ?? false
        };
        if (!options.storage && __DEV__) {
            console.warn(
                '[sigx actors] createHost() without `storage` uses in-memory storage — actor ' +
                    'state and reminders die with the process. Pass a storage provider for ' +
                    'anything beyond tests.'
            );
        }
        this.#storage = options.storage ?? memoryStorage();
        this.#scheduler = options.scheduler ?? timerScheduler();
        this.#types = options.types ?? [];

        if (Array.isArray(options.actors)) {
            for (const def of options.actors as readonly AnyActorDefinition[]) {
                if (!isActorDefinition(def)) {
                    throw new Error('[sigx actors] createHost({ actors }) got a non-definition.');
                }
                const claimed = this.#registry.get(def.type);
                if (claimed && claimed !== def) {
                    // `type` is the wire, directory AND storage name, so two
                    // definitions under one type is not a shadowing nuisance
                    // — the loser's callers silently reach the winner's
                    // state. Registering the SAME definition twice is fine.
                    throw new Error(
                        `[sigx actors] two different actors are registered as "${def.type}". ` +
                            'The type is the wire and storage key, so one would silently serve ' +
                            "the other's callers — rename one of them."
                    );
                }
                if (!hasAuthorization(def) && !serverAppConfigured()) {
                    // POLARITY FLIPPED for rfc-server-v4: the runtime is
                    // fail-closed now, so an actor declaring nothing is
                    // DENIED rather than exposed. The warning stopped being
                    // a security alarm and became a UX aid — "your calls
                    // will 401, here is why" — which is also why it only
                    // fires when no server app is configured: with one, the
                    // app default is the answer and there is nothing wrong.
                    //
                    // Still NOT `__DEV__`-gated. The Vite
                    // `requireAuthorization` build error sees only
                    // first-party source and only exists if you use the
                    // plugin; a plain `createHost` + `createActorHandler`
                    // deployment and any actor arriving from a package both
                    // bypass it. One string per registered type, at startup.
                    console.warn(
                        `[sigx actors] actor "${def.type}" declares no \`authorize\` policy and ` +
                            'no server app is configured in this process, so every call to it ' +
                            'will be DENIED (401). Configure one with createServerApp({ ' +
                            'authenticate, … }), declare `authorize:`, or mark it ' +
                            '`allowAnonymous: true` if it is deliberately public.'
                    );
                }
                this.#registry.set(def.type, def);
                this.#resolved.set(def.type, def);
            }
        } else {
            for (const [type, loader] of Object.entries(options.actors)) {
                this.#registry.set(type, loader);
            }
        }

        const host: ActivationHost = this.#host = {
            idleAfterMs: this.#defaults.idleAfterMs,
            slowTurnMs: this.#defaults.slowTurnMs,
            taskGraceMs: this.#defaults.taskGraceMs,
            scheduler: this.#scheduler,
            loadState: async (ref) => {
                const record = await this.#storage.load(ref.type, ref.key);
                if (!record) return null;
                return {
                    state: reviveWithHandlers(record.state, this.#types) as object,
                    raw: record.state,
                    etag: record.etag
                };
            },
            saveState: (ref, raw, expectedEtag) =>
                this.#storage.save(
                    ref.type,
                    ref.key,
                    encodeWithHandlers(raw, this.#types),
                    expectedEtag
                ),
            clearStoredState: (ref, expectedEtag) =>
                this.#storage.clear(ref.type, ref.key, expectedEtag),
            cloneState: <S,>(raw: S): S =>
                reviveWithHandlers(encodeWithHandlers(raw, this.#types), this.#types) as S,
            encodeArgs: (args: readonly unknown[]): unknown =>
                encodeWithHandlers(args, this.#types),
            reminders: (ref) => this.#reminders.apiFor(ref),
            tasks: (ref) =>
                taskLedger(this.#storage, actorId(ref), {
                    encode: (value) => encodeWithHandlers(value, this.#types),
                    revive: (value) => reviveWithHandlers(value, this.#types)
                }),
            actorClient: (def, key, outbound) => this.#client(def, key, outbound),
            publish: (topic, payload, call, publisher) =>
                this.#publish(topic, payload, call ?? this.#externalCall(), publisher),
            // `deactivateOne`, not `deactivate`: both callbacks target the
            // ACTIVATION that faulted / asked to go — for a stateless pool
            // member, taking the whole pool down would punish its siblings.
            onFault: (activation: Activation) => {
                void this.#local.deactivateOne(activation, 'conflict');
            },
            onIdleRequest: (activation: Activation) => {
                void this.#local.deactivateOne(activation, 'explicit');
            },
            ...(options.extendContext ? { extendContext: options.extendContext } : {})
        };
        if (options.onTurn) this.observeTurns(options.onTurn);
        this.#local = new LocalHost(
            host,
            (type) => this.definition(type),
            () => this.#bindings
        );
        this.#placement = options.placement ?? { dispatcherFor: () => this.#local };
        this.#bindings = this.#placement.bind?.(this.#local, this) ?? undefined;
        this.#reminders = options.reminders ?? shardedReminders();
        this.#reminders.bind({
            storage: this.#storage,
            scheduler: this.#scheduler,
            tickMs: this.#defaults.reminderTickMs,
            ownsShard: (shard) => this.#bindings?.ownsReminderShard?.(shard) ?? true,
            deliver: (ref, name) =>
                this.dispatch(ref, REMINDER_METHOD, [name], this.#externalCall())
        });
    }

    // -----------------------------------------------------------------------
    // ActorDispatcher (the wire layer's entry)

    // A plain function, not `async` (#24): the default placement's
    // `dispatcherFor` is synchronous, and `await`ing its non-promise result
    // cost a wrapper promise and microtask ticks on every dispatch. The seam
    // is declared sync-or-promise for exactly this branch; the try/catch
    // keeps the prologue's throws (dev arg check, a third-party placement
    // throwing) surfacing as rejections, as the `async` wrapper did.
    dispatch(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): Promise<unknown> {
        try {
            const checked = this.#devCheckArgs(ref, method, args);
            const dispatcher = this.#placement.dispatcherFor(ref);
            // Deadline stamped per branch, AFTER an async placement resolves —
            // time spent choosing a dispatcher never eats the call's budget,
            // same as when this frame awaited unconditionally.
            return isPromise(dispatcher)
                ? dispatcher.then((d) =>
                      d.dispatch(ref, method, checked, this.#withDefaultDeadline(call))
                  )
                : dispatcher.dispatch(ref, method, checked, this.#withDefaultDeadline(call));
        } catch (error) {
            return Promise.reject(error);
        }
    }

    /**
     * Where an actor lives, without dispatching.
     *
     * Always PRESENT — it is the return value that says whether there is an
     * answer. A single-node host, or any placement without `locate()`,
     * yields `undefined`, which callers read as "cannot answer, just
     * dispatch". Testing for the method alone is therefore not enough.
     */
    locate(ref: ActorRef): ActorLocation | Promise<ActorLocation> | undefined {
        return this.#placement.locate?.(ref);
    }

    /**
     * External calls without a deadline (the wire endpoint builds a bare
     * context) inherit the host's `callTimeoutMs`, same as in-process
     * external calls. In-chain calls keep their inherited deadline as-is.
     */
    #withDefaultDeadline(call: ActorCallContext): ActorCallContext {
        const timeout = this.#defaults.callTimeoutMs;
        if (call.deadline !== undefined || call.callChain.length > 0 || timeout <= 0) return call;
        return { ...call, deadline: Date.now() + timeout };
    }

    dispatchStream(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): AsyncIterable<unknown> {
        const open = async (): Promise<AsyncIterator<unknown>> => {
            const checked = this.#devCheckArgs(ref, method, args);
            const dispatcher = await this.#placement.dispatcherFor(ref);
            if (!dispatcher.dispatchStream) {
                throw new Error(
                    `[sigx actors] the placement for ${ref.type} cannot stream ` +
                        `(no dispatchStream on its dispatcher).`
                );
            }
            return dispatcher.dispatchStream(ref, method, checked, call)[Symbol.asyncIterator]();
        };
        let inner: Promise<AsyncIterator<unknown>> | null = null;
        return {
            [Symbol.asyncIterator]: () => ({
                next: async () => {
                    inner ??= open();
                    return (await inner).next();
                },
                return: async () => {
                    if (inner) {
                        const it = await inner;
                        if (it.return) await it.return(undefined);
                    }
                    return { value: undefined, done: true as const };
                }
            })
        };
    }

    dispatchWatch(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext,
        options?: { throttleMs?: number }
    ): AsyncIterable<unknown> {
        const open = async (): Promise<AsyncIterator<unknown>> => {
            const checked = this.#devCheckArgs(ref, method, args);
            const dispatcher = await this.#placement.dispatcherFor(ref);
            if (!dispatcher.dispatchWatch) {
                throw new Error(
                    `[sigx actors] the placement for ${ref.type} cannot watch ` +
                        `(no dispatchWatch on its dispatcher).`
                );
            }
            return dispatcher
                .dispatchWatch(ref, method, checked, call, options)
                [Symbol.asyncIterator]();
        };
        let inner: Promise<AsyncIterator<unknown>> | null = null;
        return {
            [Symbol.asyncIterator]: () => ({
                next: async () => {
                    inner ??= open();
                    return (await inner).next();
                },
                return: async () => {
                    if (inner) {
                        const it = await inner;
                        if (it.return) await it.return(undefined);
                    }
                    return { value: undefined, done: true as const };
                }
            })
        };
    }

    // -----------------------------------------------------------------------
    // Registry

    definition(type: string): AnyActorDefinition | Promise<AnyActorDefinition | null> | null {
        const hit = this.#resolved.get(type);
        if (hit) return hit;
        const entry = this.#registry.get(type);
        if (!entry) return null;
        if (typeof entry !== 'function') return entry;
        return entry().then((loaded: AnyActorDefinition | Record<string, unknown>) => {
            const def = unwrapDefinition(loaded, type);
            if (def) this.#resolved.set(type, def);
            return def;
        });
    }

    // -----------------------------------------------------------------------
    // Clients

    actor<D extends AnyActorDefinition>(def: D, key: string): ActorClientWith<D> {
        return this.#client(def, key, () => null) as ActorClientWith<D>;
    }

    // -----------------------------------------------------------------------
    // Topics

    publish<T>(
        topic: Topic<T>,
        payload: T,
        options?: PublishOptions
    ): Promise<TopicPublishReport> {
        return this.#publish(topic, payload, this.#externalCall(options?.signal), undefined);
    }

    async #publish(
        topic: Topic,
        payload: unknown,
        call: ActorCallContext,
        publisher: ActorRef | undefined
    ): Promise<TopicPublishReport> {
        // Re-checked here, not only in topic(): a publish may be handed a
        // hand-built object, and a malformed name must fail the caller, not
        // reach the wire.
        assertTopic(topic);
        const index = await this.#subscriberIndex();
        const entries = index.get(topic.name) ?? [];
        return publishToSubscribers(entries, topic, payload, call, publisher, (ref, m, args, c) =>
            this.dispatch(ref, m, args, c)
        );
    }

    /**
     * topic name → subscribing types, derived from the registry. Memoized in
     * production (the deploy is static); rebuilt per publish in `__DEV__`,
     * where HMR can add or edit a definition after the first publish. A
     * rejected build (a lazy loader failed) is never cached — the loader
     * must stay retryable, same rule as `#startPromise`.
     */
    #subscriberIndex(): Promise<Map<string, TopicSubscriberEntry[]>> {
        if (__DEV__) {
            return buildTopicIndex(this.#registry.keys(), (type) => this.definition(type));
        }
        if (!this.#topicIndex) {
            const building = buildTopicIndex(this.#registry.keys(), (type) =>
                this.definition(type)
            ).catch((error: unknown) => {
                if (this.#topicIndex === building) this.#topicIndex = null;
                throw error;
            });
            this.#topicIndex = building;
        }
        return this.#topicIndex;
    }

    #externalCall(signal?: AbortSignal): ActorCallContext {
        const timeout = this.#defaults.callTimeoutMs;
        return {
            callChain: [],
            callId: mintCallId(),
            ...(timeout > 0 ? { deadline: Date.now() + timeout } : {}),
            ...(signal ? { abortSignal: signal } : {})
        };
    }

    /**
     * The typed Proxy client. `outbound()` supplies the call context for
     * actor-to-actor hops; null means an external call (fresh chain +
     * default deadline).
     */
    #client<D extends AnyActorDefinition>(
        def: D,
        key: string,
        outbound: () => ActorCallContext | null,
        callOptions?: HostCallOptions
    ): ActorClientWith<D> {
        const ref: ActorRef = { type: def.type, key };
        const streamNames = new Set<string>(def.streamNames);
        const cache = new Map<string | symbol, unknown>();
        const proxy = new Proxy(Object.create(null) as object, {
            get: (_target, prop) => {
                if (typeof prop === 'symbol') return undefined;
                if (prop === 'then') return undefined; // never thenable
                if (isIntrospectionProp(prop)) return introspectionMember(prop, def.type, key);
                const cached = cache.get(prop);
                if (cached) return cached;
                let member: unknown;
                if (prop === 'with') {
                    member = (opts?: HostCallOptions) =>
                        this.#client(def, key, outbound, opts);
                } else if (streamNames.has(prop)) {
                    member = (...args: unknown[]) => {
                        if (callOptions?.oneWay) {
                            throw new Error(
                                '[sigx actors] streams cannot be one-way — a stream is ' +
                                    'consumed, not fired-and-forgotten.'
                            );
                        }
                        return this.dispatchStream(
                            ref,
                            prop,
                            args,
                            this.#call(outbound, callOptions)
                        );
                    };
                } else {
                    member = (...args: unknown[]) =>
                        this.dispatch(ref, prop, args, this.#call(outbound, callOptions));
                }
                cache.set(prop, member);
                return member;
            }
        });
        return proxy as ActorClientWith<D>;
    }

    #call(
        outbound: () => ActorCallContext | null,
        callOptions?: HostCallOptions
    ): ActorCallContext {
        const fromChain = outbound();
        let base = fromChain ?? this.#externalCall();
        if (callOptions?.signal) base = { ...base, abortSignal: callOptions.signal };
        if (callOptions?.oneWay) base = { ...base, oneWay: true };
        if (callOptions?.bag) {
            const bag = mergeCallBag(base.bag, callOptions.bag);
            if (bag) base = { ...base, bag };
        }
        // An entry point's principal wins over anything inherited: this IS
        // the boundary that just authenticated. In-chain hops pass none and
        // therefore keep the chain's, which is what makes identity flow
        // unchanged through `ctx.actor`.
        if (callOptions?.principal !== undefined) {
            base = { ...base, principal: callOptions.principal };
        }
        return base;
    }

    #devCheckArgs(
        ref: ActorRef,
        method: string,
        args: readonly unknown[]
    ): readonly unknown[] {
        if (!__DEV__ || !this.#defaults.devSerializeChecks) return args;
        try {
            return reviveWithHandlers(
                encodeWithHandlers(args, this.#types),
                this.#types
            ) as unknown[];
        } catch (error) {
            throw new Error(
                `[sigx actors] arguments to ${ref.type}/${ref.key}.${method}() do not survive ` +
                    `the codec — a remote placement could not forward this call. ` +
                    `(devSerializeChecks is on.)`,
                { cause: error }
            );
        }
    }

    // -----------------------------------------------------------------------
    // Lifecycle

    start(): Promise<void> {
        if (this.#started) return Promise.resolve();
        // Concurrent callers share ONE start, so `await host.start()` is a
        // real barrier for every one of them rather than resolving early
        // for all but the first. Same shape as ActorApp.start().
        if (this.#startPromise) return this.#startPromise;
        const starting = this.#start().catch((error: unknown) => {
            // Never cache a rejection — a start that failed on a bad
            // placement or reminders must stay retryable.
            if (this.#startPromise === starting) this.#startPromise = null;
            throw error;
        });
        this.#startPromise = starting;
        return starting;
    }

    async #start(): Promise<void> {
        // Everything that can FAIL runs before anything is registered or
        // claimed, so a rejected start leaves nothing behind: no sweeper
        // ticking, no `#started`, no stamped seam.
        await this.#placement.start?.();
        try {
            // Awaited: the seam allows async start, and an implementation
            // that opens an alarm or a connection must be up before
            // start() resolves.
            await this.#reminders.start();
        } catch (error) {
            // The placement already joined — undo that rather than leave
            // this host advertised but not really running.
            try {
                await this.#placement.stop?.();
            } catch (stopError) {
                if (__DEV__) {
                    console.error(
                        '[sigx actors] rolling back a failed start did not stop the placement:',
                        stopError
                    );
                }
            }
            throw error;
        }
        // `0` = no idle collection (see `HostDefaults.sweepIntervalMs`).
        if (this.#defaults.sweepIntervalMs > 0) {
            this.#stopSweeper = this.#scheduler.every(this.#defaults.sweepIntervalMs, () =>
                this.#local.sweep(
                    Date.now(),
                    this.#defaults.idleAfterMs,
                    this.#defaults.maxActivations
                )
            );
        }
        this.#started = true;
        this.#stopped = false;
        stampHost(this);
    }

    async stop(opts?: { timeoutMs?: number }): Promise<void> {
        if (this.#stopped) return;
        // Let an in-flight start finish first. Otherwise its continuation
        // runs AFTER this returns and re-registers the sweeper and the seam,
        // leaving background work behind a stop that already resolved.
        if (this.#startPromise) {
            try {
                await this.#startPromise;
            } catch {
                // A failed start left nothing running to tear down.
            }
            // Another stop may have won while we waited.
            if (this.#stopped) return;
        }
        this.#stopped = true;
        this.#started = false;
        // Drop the cached start so a later start() really restarts.
        this.#startPromise = null;
        this.#stopSweeper?.();
        this.#stopSweeper = null;
        // Awaited so an async teardown really finishes, but guarded for the
        // same reason `placement.beginStop()` is: failing to stop reminders
        // must never cost us the drain.
        try {
            await this.#reminders.stop();
        } catch (error) {
            if (__DEV__) {
                console.error('[sigx actors] reminders.stop() failed:', error);
            }
        }
        // Announce the departure BEFORE draining, so cluster peers stop
        // placing new actors here while activations hand off. Best-effort:
        // a failed announcement must never abort the drain itself.
        try {
            await this.#placement.beginStop?.();
        } catch (error) {
            if (__DEV__) {
                console.error('[sigx actors] placement.beginStop() failed:', error);
            }
        }
        await this.#local.stopAll(
            opts?.timeoutMs ?? 30_000,
            this.#bindings?.stopReason ?? 'shutdown'
        );
        await this.#placement.stop?.();
        clearHost(this);
    }

    deactivate(ref: ActorRef, reason?: DeactivationReason): Promise<void> {
        return this.#local.deactivate(ref, reason ?? 'explicit');
    }

    deactivateType(type: string): Promise<void> {
        // Dev/HMR contract: also drop the resolved-definition cache so a
        // lazy-registered type reloads its (possibly edited) module on the
        // next dispatch. Array-registered definitions stay reachable via
        // the registry map.
        this.#resolved.delete(type);
        return this.#local.deactivateType(type);
    }

    observeTurns(observer: ActorTurnObserver): () => void {
        this.#turnObservers.add(observer);
        this.#refreshTurnObserver();
        return () => {
            if (this.#turnObservers.delete(observer)) this.#refreshTurnObserver();
        };
    }

    /**
     * Recompute the host's single observer slot.
     *
     * Setting it to `undefined` when nobody is listening is the whole point:
     * the activation branches on its presence to decide whether to take the
     * per-turn timestamps, so an empty subscriber set restores the original
     * hot path exactly. That is what lets observation be switched off at
     * runtime and actually cost nothing, rather than merely doing nothing.
     */
    #refreshTurnObserver(): void {
        const observers = [...this.#turnObservers];
        if (observers.length === 0) {
            this.#host.onTurn = undefined;
            return;
        }
        if (observers.length === 1) {
            this.#host.onTurn = observers[0] as ActorTurnObserver;
            return;
        }
        // Each isolated: one plugin's broken observer must not stop the rest
        // from being told. (The activation guards the whole call too; this
        // is about not losing observers two through N.)
        this.#host.onTurn = (ref, method, queuedMs, elapsedMs, failed, call) => {
            for (const observe of observers) {
                try {
                    observe(ref, method, queuedMs, elapsedMs, failed, call);
                } catch (error) {
                    if (__DEV__) console.error('[sigx actors] a turn observer threw:', error);
                }
            }
        };
    }

    stats(): HostStats {
        return this.#local.stats();
    }

    activations(options?: ActivationsOptions): readonly ActivationInfo[] {
        return this.#local.activations(options);
    }
}

function unwrapDefinition(
    loaded: AnyActorDefinition | Record<string, unknown>,
    type: string
): AnyActorDefinition | null {
    if (isActorDefinition(loaded)) {
        return loaded.type === type ? (loaded as AnyActorDefinition) : null;
    }
    for (const value of Object.values(loaded)) {
        if (isActorDefinition(value) && value.type === type) return value as AnyActorDefinition;
    }
    return null;
}

function isPromise<T>(value: T | Promise<T>): value is Promise<T> {
    return typeof (value as { then?: unknown })?.then === 'function';
}
