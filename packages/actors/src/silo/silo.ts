/**
 * `createSilo` — composition root: registry, codec-bound storage, local
 * host, sweeper, reminders, and the `__SIGX_ACTOR_SILO__` seam lifecycle.
 */
import {
    encodeWithHandlers,
    reviveWithHandlers,
    type TypeHandler
} from '@sigx/serialize';
import { isActorDefinition } from '../define';
import { clearSilo, stampSilo } from '../seam';
import type {
    ActorCallContext,
    ActorClientWith,
    ActorPlacement,
    ActorRef,
    ActorStorage,
    AnyActorDefinition,
    DeactivationReason,
    PlacementBindings,
    ActorScheduler,
    Silo,
    SiloStats
} from '../types';
import { mintCallId, REMINDER_METHOD, type Activation, type ActivationHost } from './activation';
import { LocalHost } from './local-host';
import { ReminderService } from './reminders';
import { memoryStorage } from './storage-memory';
import { timerScheduler } from './scheduler';

export interface SiloDefaults {
    /** Idle collection age, ms. Default 20 min — single-node processes
     *  redeploy often and reactivation is one storage load. */
    idleAfterMs?: number;
    /** External-call deadline, ms (becomes `ActorCallContext.deadline`).
     *  Default 30s. `0` disables. */
    callTimeoutMs?: number;
    /** Sweeper cadence, ms. Default 60s. */
    sweepIntervalMs?: number;
    /** Reminder loop cadence, ms. Default 30s. */
    reminderTickMs?: number;
    /** __DEV__ slow-turn warning threshold, ms. Default 5s. */
    slowTurnMs?: number;
    /**
     * __DEV__: round-trip dispatch args through the codec so same-process
     * code cannot accidentally depend on object identity a remote placement
     * would break. Default false (costs an encode per call).
     */
    devSerializeChecks?: boolean;
}

export interface CreateSiloOptions {
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
     * The clock for the sweeper, reminder tick, `ctx.timer` and write-behind
     * flushes. Default `timerScheduler()` (host timers). Replace it on a
     * runtime without background execution — a Worker never fires an
     * interval registered at startup — or with `manualScheduler()` in tests.
     */
    scheduler?: ActorScheduler;
    /**
     * Extra members merged onto every activation's `ctx`. Built-in members
     * are never overwritten. `defineActorApp` folds every plugin's
     * `extendContext` into this one function; hand-rolled silos can pass it
     * directly.
     */
    extendContext?: (ref: ActorRef) => object | undefined;
    defaults?: SiloDefaults;
}

interface ResolvedDefaults {
    idleAfterMs: number;
    callTimeoutMs: number;
    sweepIntervalMs: number;
    reminderTickMs: number;
    slowTurnMs: number;
    devSerializeChecks: boolean;
}

export function createSilo(options: CreateSiloOptions): Silo {
    return new SiloImpl(options);
}

class SiloImpl implements Silo {
    #defaults: ResolvedDefaults;
    #storage: ActorStorage;
    #types: readonly TypeHandler[];
    #placement: ActorPlacement;
    #bindings: PlacementBindings | undefined;
    #local: LocalHost;
    #reminders: ReminderService;
    #registry = new Map<
        string,
        AnyActorDefinition | (() => Promise<AnyActorDefinition | Record<string, unknown>>)
    >();
    #resolved = new Map<string, AnyActorDefinition>();
    #scheduler: ActorScheduler;
    #stopSweeper: (() => void) | null = null;
    #started = false;
    #stopped = false;

    constructor(options: CreateSiloOptions) {
        this.#defaults = {
            idleAfterMs: options.defaults?.idleAfterMs ?? 20 * 60_000,
            callTimeoutMs: options.defaults?.callTimeoutMs ?? 30_000,
            sweepIntervalMs: options.defaults?.sweepIntervalMs ?? 60_000,
            reminderTickMs: options.defaults?.reminderTickMs ?? 30_000,
            slowTurnMs: options.defaults?.slowTurnMs ?? 5_000,
            devSerializeChecks: options.defaults?.devSerializeChecks ?? false
        };
        if (!options.storage && __DEV__) {
            console.warn(
                '[sigx actors] createSilo() without `storage` uses in-memory storage — actor ' +
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
                    throw new Error('[sigx actors] createSilo({ actors }) got a non-definition.');
                }
                this.#registry.set(def.type, def);
                this.#resolved.set(def.type, def);
            }
        } else {
            for (const [type, loader] of Object.entries(options.actors)) {
                this.#registry.set(type, loader);
            }
        }

        const host: ActivationHost = {
            idleAfterMs: this.#defaults.idleAfterMs,
            slowTurnMs: this.#defaults.slowTurnMs,
            scheduler: this.#scheduler,
            loadState: async (ref) => {
                const record = await this.#storage.load(ref.type, ref.key);
                if (!record) return null;
                return {
                    state: reviveWithHandlers(record.state, this.#types) as object,
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
            reminders: (ref) => this.#reminders.apiFor(ref),
            actorClient: (def, key, outbound) => this.#client(def, key, outbound),
            onFault: (activation: Activation) => {
                void this.#local.deactivate(activation.ref, 'conflict');
            },
            onIdleRequest: (activation: Activation) => {
                void this.#local.deactivate(activation.ref, 'explicit');
            },
            ...(options.extendContext ? { extendContext: options.extendContext } : {})
        };
        this.#local = new LocalHost(
            host,
            (type) => this.definition(type),
            () => this.#bindings
        );
        this.#placement = options.placement ?? { dispatcherFor: () => this.#local };
        this.#bindings = this.#placement.bind?.(this.#local, this) ?? undefined;
        this.#reminders = new ReminderService(
            this.#storage,
            (ref, name) => this.dispatch(ref, REMINDER_METHOD, [name], this.#externalCall()),
            {
                ownsShard: (shard) => this.#bindings?.ownsReminderShard?.(shard) ?? true,
                scheduler: this.#scheduler
            }
        );
    }

    // -----------------------------------------------------------------------
    // ActorDispatcher (the wire layer's entry)

    async dispatch(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): Promise<unknown> {
        const checked = this.#devCheckArgs(ref, method, args);
        const dispatcher = await this.#placement.dispatcherFor(ref);
        return dispatcher.dispatch(ref, method, checked, this.#withDefaultDeadline(call));
    }

    /**
     * External calls without a deadline (the wire endpoint builds a bare
     * context) inherit the silo's `callTimeoutMs`, same as in-process
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
        callOptions?: { signal?: AbortSignal }
    ): ActorClientWith<D> {
        const ref: ActorRef = { type: def.type, key };
        const streamNames = new Set<string>(def.streamNames);
        const cache = new Map<string | symbol, unknown>();
        const proxy = new Proxy(Object.create(null) as object, {
            get: (_target, prop) => {
                if (typeof prop === 'symbol') return undefined;
                if (prop === 'then') return undefined; // never thenable
                const cached = cache.get(prop);
                if (cached) return cached;
                let member: unknown;
                if (prop === 'with') {
                    member = (opts?: { signal?: AbortSignal }) =>
                        this.#client(def, key, outbound, opts);
                } else if (streamNames.has(prop)) {
                    member = (...args: unknown[]) =>
                        this.dispatchStream(ref, prop, args, this.#call(outbound, callOptions));
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
        callOptions?: { signal?: AbortSignal }
    ): ActorCallContext {
        const fromChain = outbound();
        const base = fromChain ?? this.#externalCall();
        return callOptions?.signal ? { ...base, abortSignal: callOptions.signal } : base;
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

    async start(): Promise<void> {
        if (this.#started) return;
        this.#started = true;
        this.#stopped = false;
        await this.#placement.start?.();
        this.#stopSweeper = this.#scheduler.every(this.#defaults.sweepIntervalMs, () =>
            this.#local.sweep(Date.now(), this.#defaults.idleAfterMs)
        );
        this.#reminders.start(this.#defaults.reminderTickMs);
        stampSilo(this);
    }

    async stop(opts?: { timeoutMs?: number }): Promise<void> {
        if (this.#stopped) return;
        this.#stopped = true;
        this.#started = false;
        this.#stopSweeper?.();
        this.#stopSweeper = null;
        this.#reminders.stop();
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
        clearSilo(this);
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

    stats(): SiloStats {
        return this.#local.stats();
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
