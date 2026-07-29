/**
 * `defineActorApp` — the composition root over `createSilo`.
 *
 * `createSilo` takes exactly one `placement` and one `storage`, and
 * `ActorPlacement.bind()` → `PlacementBindings` is the only lifecycle-hook
 * shape in the package. That makes the seams EXCLUSIVE: two things that both
 * want `beforeActivate` (a cluster directory and an audit log) cannot
 * coexist. An app fixes that by folding every plugin's contributions into
 * the single composite placement / storage / context that `createSilo`
 * already understands — so this layer is purely additive and `createSilo`
 * stays the documented low-level primitive.
 *
 * The app is an inert DESCRIPTION, not a running silo. That is what lets the
 * same module be started by a Node entry, the Vite dev server, or a Worker.
 */
import type { TypeHandler } from '@sigx/serialize';
import { defineActor as defineActorFn } from '../define';
import type {
    ActorDefinition,
    ActorDispatcher,
    ActorMethodTable,
    ActorOptions,
    ActorPlacement,
    ActorRef,
    ActorStorage,
    ActorReminders,
    ActorScheduler,
    ActorStreamTable,
    ActorTurnObserver,
    AnyActorDefinition,
    DeactivationReason,
    PlacementBindings,
    Silo
} from '../types';
import { createSilo, type CreateSiloOptions, type SiloDefaults } from './silo';
import { memoryStorage } from './storage-memory';

// ---------------------------------------------------------------------------
// Plugin contract

/**
 * Wraps the dispatcher a placement resolved for a ref — the seam for
 * tracing, metrics, retry, and logging. Composed outside-in in registration
 * order: with `.use(a).use(b)`, `a`'s wrapper is the outermost.
 *
 * MUST forward `dispatchStream` when `next` has it. It is optional on
 * `ActorDispatcher`, so returning a bare `{ dispatch }` drops streaming and
 * every `streams:` method then fails — blaming the transport, not the
 * middleware. Dev-warns when a wrapper loses it.
 */
export type DispatchMiddleware = (next: ActorDispatcher) => ActorDispatcher;

/**
 * What a placement factory gets. Deliberately narrow: `bind()` already
 * hands the placement the silo itself, so this exposes only what a backend
 * needs BEFORE binding — reading the per-type placement strategies actors
 * declared.
 */
export interface PlacementSetupContext {
    /**
     * Resolve an actor definition, to read `__sigxActor.placement`. May be
     * async and may return null: a lazy (`virtual:sigx-actors`) registry
     * loads a type's module on demand, so resolve per type at dispatch
     * time rather than enumerating up front.
     */
    definition(type: string): AnyActorDefinition | Promise<AnyActorDefinition | null> | null;
}

/**
 * One plugin's answer to "should this silo be receiving traffic?".
 *
 * SYNC on purpose: a readiness probe must never wait on a store round-trip,
 * which is the same reason `ClusterMembership.view()` is sync.
 */
export interface HealthCheck {
    /** False = take this silo out of rotation. */
    ready: boolean;
    /** Short state word for the probe body, e.g. `'active'`, `'leaving'`. */
    detail?: string;
}

/** The aggregate of every contributed check. */
export interface HealthReport {
    /** True iff every check passed. An app with no checks is ready. */
    ready: boolean;
    /** By contributor name, in registration order. */
    checks: Record<string, HealthCheck>;
}

/**
 * What a failing ops provider reports in place of its section.
 *
 * The section is still PRESENT and still named — an absent key would read as
 * "this plugin contributes nothing", which is the opposite of the truth.
 */
export interface OpsProviderError {
    error: string;
}

/** The aggregate of every contributed ops section, by contributor name. */
export type OpsReport = Record<string, unknown>;

/** An HTTP route a plugin contributes to the app's mounts. */
export interface ActorRoute {
    /** Diagnostic label, e.g. `'cluster:internal'`. */
    readonly name: string;
    match(request: Request): boolean;
    handle(request: Request, silo: Silo): Promise<Response>;
}

/**
 * What a plugin may contribute, handed to `setup()`. Everything here
 * composes across plugins EXCEPT `setPlacement`, which is exclusive by
 * nature (there is one answer to "who hosts this actor").
 */
export interface PluginRegistry {
    /** Codec handlers for state persistence and dev checks; concatenated. */
    addTypeHandlers(...handlers: TypeHandler[]): void;
    /**
     * Wrap the storage chain — caching, encryption, metrics. Applied in
     * registration order, so the LAST registered decorator is outermost and
     * therefore sees a call first.
     */
    decorateStorage(decorate: (inner: ActorStorage) => ActorStorage): void;
    /**
     * Claim the placement seam — the backend that answers WHO hosts an
     * actor (the local host, a cluster, Durable Objects). EXCLUSIVE: a
     * second claim throws, naming both plugins, because two placements
     * would mean two answers to where an actor lives.
     *
     * A FACTORY, not an instance: it runs once the silo exists, so a custom
     * placement can resolve per-type strategies declared on the actors
     * (`defineActor({ placement })` — Orleans's attribute form) instead of
     * being limited to a map fixed at plugin-construction time.
     */
    setPlacement(factory: (context: PlacementSetupContext) => ActorPlacement): void;
    /**
     * Runs inside the activation reserve, before any state loads. Throwing
     * REFUSES the activation (every parked caller rejects and nothing is
     * remembered) — deliberately not caught, so admission control works.
     */
    onBeforeActivate(hook: (ref: ActorRef) => void | Promise<void>): void;
    /**
     * Runs after an activation is fully deactivated and forgotten. Errors
     * are caught per hook and dev-logged: one bad plugin must not block a
     * placement's claim release.
     */
    onAfterDeactivate(
        hook: (ref: ActorRef, reason: DeactivationReason) => void | Promise<void>
    ): void;
    useDispatch(middleware: DispatchMiddleware): void;
    /**
     * Observe every dispatched turn's queue wait and execution time — the
     * split `useDispatch` cannot see, because a middleware only measures
     * enqueue-to-settle and cannot tell a slow turn from a long queue.
     *
     * Composes: every registered observer is called. Registering NONE
     * leaves the dispatch path exactly as it was, so this costs nothing
     * when unused.
     *
     * Returns an unsubscribe. Calling it removes the observer and, if it was
     * the last one, switches the per-turn timestamps back off — so a plugin
     * can offer runtime on/off rather than paying for observation forever.
     * Safe to call before `start()`, in which case the subscription simply
     * never happens.
     */
    observeTurns(observer: ActorTurnObserver): () => void;
    /**
     * After the silo is live (post `start()`), in registration order.
     * Throwing ROLLS THE START BACK: the silo is stopped again and the
     * `onStop` hooks run, so a rejected `start()` leaks no background work.
     */
    onStart(hook: (silo: Silo) => void | Promise<void>): void;
    /**
     * After the silo has drained (post `stop()`), in REVERSE order, each
     * isolated. Also runs when a start is rolled back — so treat it as
     * best-effort cleanup that may fire even if this plugin's `onStart`
     * never completed.
     */
    onStop(hook: (silo: Silo) => void | Promise<void>): void;
    route(route: ActorRoute): void;
    /**
     * Contribute a readiness check — the seam a health endpoint aggregates.
     * Composes: EVERY contributed check must pass for the silo to report
     * ready, so any plugin can take its silo out of rotation without the
     * endpoint knowing that plugin exists.
     *
     * `check` is SYNC and must stay cheap: it runs per probe, and a probe
     * that blocks is worse than one that fails. Throwing is caught and
     * reported as not-ready carrying the reason — a broken check must
     * never 500 the endpoint.
     *
     * `name` keys the report, so it must be non-empty and unique across
     * plugins; a clash throws at setup naming both, because a silently
     * overwritten entry could hide a FAILING check behind a passing one.
     */
    reportHealth(name: string, check: () => HealthCheck): void;
    /**
     * The aggregate of every contributed check — LIVE, including checks
     * registered after this call. That is what makes `.use(health())` work
     * regardless of where it sits relative to the plugins it reports on, so
     * hold this and call it per request rather than at setup time.
     */
    health(): HealthReport;
    /**
     * Contribute a named section to the ops snapshot — the seam `ops()`
     * aggregates. The counterpart to `reportHealth`: readiness answers "may
     * I take traffic?" in a status code, this answers "what is going on in
     * here?" in a body.
     *
     * `provider` runs PER READ, so return live numbers rather than a value
     * captured at setup. Unlike a readiness check it is allowed to be
     * expensive-ish — an ops read is an operator polling at 1 Hz, not a load
     * balancer probing every second — but it must stay SYNC, for the same
     * reason: the endpoint is what you reach for when the silo is already
     * unwell, and it must not be able to hang.
     *
     * A throwing provider is caught and its section replaced with
     * `{ error }`, leaving every other section intact. The one tool that
     * explains a broken silo must not be broken BY it.
     *
     * `name` keys the report, so it must be non-empty and unique across
     * plugins; a clash throws at setup naming both.
     */
    reportOps(name: string, provider: () => unknown): void;
    /**
     * The aggregate of every contributed section — LIVE, including providers
     * registered after this call, so `.use()` order does not matter. Hold
     * this and call it per request rather than at setup time.
     */
    ops(): OpsReport;
    /**
     * Extra members merged onto every activation's `ctx`. Pair it with an
     * `ActorPlugin<Ext>` type argument so they are typed inside actors.
     *
     * Returning `undefined` contributes nothing for that ref — a plugin can
     * extend only the actor types it cares about without a cast.
     */
    extendContext(factory: (ref: ActorRef) => object | undefined): void;
}

/**
 * A plugin. `Ext` is the shape this plugin adds to `ctx`; it is a phantom
 * type parameter (`__ext` is never assigned at runtime) that `app.use()`
 * accumulates so the app-bound `defineActor` types it.
 */
export interface ActorPlugin<Ext extends object = Record<never, never>> {
    readonly name: string;
    setup(registry: PluginRegistry): void;
    /** @internal phantom — carries `Ext` for inference only. */
    readonly __ext?: Ext | undefined;
}

// ---------------------------------------------------------------------------
// App

export interface ActorAppOptions {
    /**
     * Definitions, or the lazy `{ type: () => import() }` map.
     *
     * OPTIONAL, so an app module can stay runtime-neutral: the Vite plugin
     * supplies the registry it already builds (`sigxActors({ app })`),
     * which means your app module need not import `virtual:sigx-actors`
     * and therefore loads under plain Node too. A non-Vite entry passes
     * them here as before.
     */
    actors?: CreateSiloOptions['actors'];
    /** Base storage, before any plugin decorators. Omit = in-memory. */
    storage?: ActorStorage;
    types?: readonly TypeHandler[];
    /** The clock for background work. Default: host timers. */
    scheduler?: ActorScheduler;
    /** Durable-reminder implementation. Default: the sharded table. */
    reminders?: ActorReminders;
    defaults?: SiloDefaults;
}

export interface ActorApp<Ext extends object = Record<never, never>> {
    /** Register a plugin. Mutates and returns THIS app, widened by `Ext`. */
    use<E extends object>(plugin: ActorPlugin<E>): ActorApp<Ext & E>;
    /**
     * Supply the actor registry when the app was built without one — what
     * a HOST does: the Vite plugin hands over the registry it already
     * builds, so the app module itself stays runtime-neutral.
     *
     * Throws if the app already has actors, so a host can never silently
     * replace what the author configured — and, like `use()`, once the app
     * has been started or its `routes` read: by then the registry is
     * already baked into a running silo.
     */
    withActors(actors: NonNullable<ActorAppOptions['actors']>): ActorApp<Ext>;
    /** Whether a registry has been supplied yet. */
    readonly hasActors: boolean;
    /**
     * `defineActor` bound to this app's plugin set — the same function at
     * runtime, with `ctx` typed as the built-ins plus every plugin's
     * additions. Destructure it (`export const { defineActor } = app`) and
     * import it from your actor modules.
     */
    defineActor<
        S extends object,
        M extends ActorMethodTable,
        St extends ActorStreamTable = Record<never, never>
    >(
        options: ActorOptions<S, M, St, Ext>
    ): ActorDefinition<S, M, St>;
    /** Plugin-contributed routes. Reading this seals the app. */
    readonly routes: readonly ActorRoute[];
    /** The running silo — null before `start()` and again after `stop()`. */
    readonly silo: Silo | null;
    /** Build the silo, start it, then run every `onStart`. Idempotent. */
    start(): Promise<Silo>;
    /** Stop the silo, then run every `onStop` in reverse. Idempotent. */
    stop(opts?: { timeoutMs?: number }): Promise<void>;
}

/**
 * A plugin's turn subscription, which outlives the silo's construction: it
 * is declared during `setup()` but can only really be attached once the
 * silo exists, and may be cancelled from either side of that boundary.
 */
interface TurnSubscription {
    observer: ActorTurnObserver;
    cancelled: boolean;
    detach: (() => void) | null;
}

interface Contributions {
    typeHandlers: TypeHandler[];
    storageDecorators: ((inner: ActorStorage) => ActorStorage)[];
    placement: {
        plugin: string;
        factory: (context: PlacementSetupContext) => ActorPlacement;
    } | null;
    beforeActivate: ((ref: ActorRef) => void | Promise<void>)[];
    afterDeactivate: ((ref: ActorRef, reason: DeactivationReason) => void | Promise<void>)[];
    dispatch: DispatchMiddleware[];
    turnObservers: TurnSubscription[];
    onStart: ((silo: Silo) => void | Promise<void>)[];
    onStop: ((silo: Silo) => void | Promise<void>)[];
    routes: ActorRoute[];
    health: { name: string; check: () => HealthCheck; plugin: string }[];
    ops: { name: string; provider: () => unknown; plugin: string }[];
    contextFactories: ((ref: ActorRef) => object | undefined)[];
}

/**
 * Fold the contributed checks into one report. A throwing check is
 * not-ready rather than an exception: an endpoint that 500s when a check is
 * broken tells an operator nothing about the silo.
 */
function healthReport(c: Contributions): HealthReport {
    const checks: Record<string, HealthCheck> = {};
    let ready = true;
    for (const { name, check } of c.health) {
        let result: HealthCheck;
        try {
            result = check();
        } catch (error) {
            // `String(error)` rather than only `.message`: a check that
            // throws a string or a number still has to say why, and the
            // whole point of catching is that the reason survives.
            const message = (error as Error)?.message ?? String(error);
            result = { ready: false, detail: message || 'check failed' };
        }
        checks[name] = result;
        if (!result.ready) ready = false;
    }
    return { ready, checks };
}

/**
 * Fold the contributed ops sections into one report.
 *
 * Same rule as `healthReport`, and for a sharper reason: this endpoint is
 * what an operator opens when a silo is already misbehaving, so one plugin
 * whose provider throws must cost its own section and nothing else. The
 * section stays present, carrying the reason.
 */
function opsReport(c: Contributions): OpsReport {
    const sections: OpsReport = {};
    for (const { name, provider } of c.ops) {
        try {
            const value = provider();
            // `undefined` is normalized to `null` because `JSON.stringify`
            // DROPS an undefined-valued key — the section would vanish from
            // the wire entirely, which is the one thing the catch below
            // exists to prevent.
            sections[name] = value === undefined ? null : value;
        } catch (error) {
            // `String(error)` as a fallback, like healthReport: a provider
            // that throws a string still has to say why.
            const message = (error as Error)?.message ?? String(error);
            const failure: OpsProviderError = { error: message || 'ops provider failed' };
            sections[name] = failure;
        }
    }
    return sections;
}

export function defineActorApp(options: ActorAppOptions): ActorApp {
    return new ActorAppImpl(options) as ActorApp;
}

class ActorAppImpl implements ActorApp<Record<never, never>> {
    #options: ActorAppOptions;
    #plugins: ActorPlugin<object>[] = [];
    #contributions: Contributions | null = null;
    #silo: Silo | null = null;
    #starting: Promise<Silo> | null = null;
    #stopped = false;

    constructor(options: ActorAppOptions) {
        this.#options = options;
    }

    withActors(actors: NonNullable<ActorAppOptions['actors']>): ActorApp<Record<never, never>> {
        if (this.#options.actors) {
            throw new Error(
                '[sigx actors] this app already has actors — withActors() is for supplying a ' +
                    'registry to an app built without one, not for replacing it.'
            );
        }
        if (this.#contributions) {
            throw new Error(
                '[sigx actors] cannot withActors() after the app has been started (or its ' +
                    'routes read).'
            );
        }
        this.#options = { ...this.#options, actors };
        return this;
    }

    get hasActors(): boolean {
        return this.#options.actors !== undefined;
    }

    use<E extends object>(plugin: ActorPlugin<E>): ActorApp<E> {
        if (this.#contributions) {
            throw new Error(
                `[sigx actors] cannot .use(${plugin.name}) after the app has been started ` +
                    '(or its routes read) — register every plugin before starting.'
            );
        }
        this.#plugins.push(plugin as ActorPlugin<object>);
        return this as unknown as ActorApp<E>;
    }

    // The app-bound `defineActor` IS the root one — only its type differs,
    // so the build transform sees the very same call it always did.
    readonly defineActor: ActorApp['defineActor'] = defineActorFn;

    get routes(): readonly ActorRoute[] {
        return this.#seal().routes;
    }

    get silo(): Silo | null {
        return this.#silo;
    }

    /** Run every plugin's `setup()` exactly once and freeze the result. */
    #seal(): Contributions {
        if (this.#contributions) return this.#contributions;
        const c: Contributions = {
            typeHandlers: [...(this.#options.types ?? [])],
            storageDecorators: [],
            placement: null,
            beforeActivate: [],
            afterDeactivate: [],
            dispatch: [],
            turnObservers: [],
            onStart: [],
            onStop: [],
            routes: [],
            health: [],
            ops: [],
            contextFactories: []
        };
        for (const plugin of this.#plugins) {
            plugin.setup(registryFor(plugin.name, c));
        }
        this.#contributions = c;
        return c;
    }

    start(): Promise<Silo> {
        if (this.#stopped) {
            return Promise.reject(
                new Error(
                    '[sigx actors] this app has been stopped and cannot be restarted — a ' +
                        'plugin-provided placement carries per-run identity (a cluster silo id ' +
                        'is minted once and its membership entry is gone after stop). Build a ' +
                        'new app with defineActorApp().'
                )
            );
        }
        if (this.#starting) return this.#starting;
        const starting = this.#start().catch((error: unknown) => {
            // Never cache a REJECTION: a start that failed on bad storage or
            // a refused membership join must stay retryable rather than
            // poisoning the app with a stale error on every later call.
            if (this.#starting === starting) this.#starting = null;
            this.#silo = null;
            throw error;
        });
        this.#starting = starting;
        return starting;
    }

    async #start(): Promise<Silo> {
        if (!this.#options.actors) {
            throw new Error(
                '[sigx actors] this app has no actors. Pass them to defineActorApp({ actors }), ' +
                    'or let the Vite plugin supply them with sigxActors({ app }).'
            );
        }
        const c = this.#seal();
        // A decorator has to wrap something concrete, so materialize the
        // in-memory default here rather than leaving it to `createSilo` —
        // and carry its warning across, since it can no longer fire there.
        let storage = this.#options.storage;
        if (c.storageDecorators.length) {
            if (!storage) {
                if (__DEV__) {
                    console.warn(
                        '[sigx actors] defineActorApp() without `storage` uses in-memory ' +
                            'storage — actor state and reminders die with the process. Pass a ' +
                            'storage provider for anything beyond tests.'
                    );
                }
                storage = memoryStorage();
            }
            storage = c.storageDecorators.reduce((inner, decorate) => decorate(inner), storage);
        }
        const placement = compositePlacement(c);
        const silo = createSilo({
            actors: this.#options.actors!,
            ...(storage ? { storage } : {}),
            ...(placement ? { placement } : {}),
            ...(c.typeHandlers.length ? { types: c.typeHandlers } : {}),
            ...(this.#options.scheduler ? { scheduler: this.#options.scheduler } : {}),
            ...(this.#options.reminders ? { reminders: this.#options.reminders } : {}),
            ...(c.contextFactories.length
                ? { extendContext: contextExtender(c.contextFactories) }
                : {}),

            ...(this.#options.defaults ? { defaults: this.#options.defaults } : {})
        });
        this.#silo = silo;
        // Attach turn observers now the silo exists. Each keeps its own
        // detach so a plugin can switch observation off later, and the last
        // one leaving restores the untouched dispatch path.
        for (const subscription of c.turnObservers) {
            if (subscription.cancelled) continue;
            subscription.detach = silo.observeTurns(subscription.observer);
        }
        await silo.start();
        try {
            for (const hook of c.onStart) await hook(silo);
        } catch (error) {
            // The silo is ALREADY live here — sweeper, reminder tick, a
            // cluster join, and the ambient `__SIGX_ACTOR_SILO__` seam. A
            // rejected start that just walked away would leak all of it and
            // leave the seam pointing at a silo this app no longer tracks,
            // so roll the start back before surfacing the failure.
            try {
                await silo.stop();
            } catch (stopError) {
                if (__DEV__) {
                    console.error(
                        '[sigx actors] rolling back a failed start did not stop cleanly:',
                        stopError
                    );
                }
            }
            await this.#runStopHooks(silo, c);
            throw error;
        }
        return silo;
    }

    /** `onStop`, reverse order, each isolated — one bad plugin must not
     *  strand the rest of the teardown. */
    async #runStopHooks(silo: Silo, c: Contributions): Promise<void> {
        for (const hook of [...c.onStop].reverse()) {
            try {
                await hook(silo);
            } catch (error) {
                if (__DEV__) {
                    console.error('[sigx actors] a plugin onStop hook threw (ignored):', error);
                }
            }
        }
    }

    async stop(opts?: { timeoutMs?: number }): Promise<void> {
        if (this.#stopped) return;
        // A stop racing an in-flight start must still tear that silo down.
        if (this.#starting) {
            try {
                await this.#starting;
            } catch {
                // A failed start left nothing running to stop.
            }
        }
        const silo = this.#silo;
        // Never started (or the start failed): nothing to tear down, and the
        // app stays startable.
        if (!silo) return;
        this.#stopped = true;
        // Actors drain FIRST: an `onStop` hook may close a connection the
        // draining turns still need.
        try {
            await silo.stop(opts);
        } catch (error) {
            // Teardown did NOT complete — a placement's membership leave can
            // throw against a dead store. Stay exactly as we were rather
            // than reporting a shutdown that did not happen: `app.silo`
            // remains reachable so the failure is inspectable, and `stop()`
            // can be retried (the silo's own stop is idempotent, so a retry
            // proceeds to the hooks below).
            this.#stopped = false;
            throw error;
        }
        this.#starting = null;
        this.#silo = null;
        await this.#runStopHooks(silo, this.#seal());
    }
}

function registryFor(pluginName: string, c: Contributions): PluginRegistry {
    return {
        addTypeHandlers(...handlers) {
            c.typeHandlers.push(...handlers);
        },
        decorateStorage(decorate) {
            c.storageDecorators.push(decorate);
        },
        setPlacement(factory) {
            if (c.placement) {
                throw new Error(
                    `[sigx actors] two plugins claim the placement seam: "${c.placement.plugin}" ` +
                        `and "${pluginName}". An actor has exactly one host — use only one of them.`
                );
            }
            c.placement = { plugin: pluginName, factory };
        },
        onBeforeActivate(hook) {
            c.beforeActivate.push(hook);
        },
        onAfterDeactivate(hook) {
            c.afterDeactivate.push(hook);
        },
        useDispatch(middleware) {
            c.dispatch.push(middleware);
        },
        observeTurns(observer) {
            const subscription: TurnSubscription = { observer, cancelled: false, detach: null };
            c.turnObservers.push(subscription);
            return () => {
                subscription.cancelled = true;
                subscription.detach?.();
                subscription.detach = null;
            };
        },
        onStart(hook) {
            c.onStart.push(hook);
        },
        onStop(hook) {
            c.onStop.push(hook);
        },
        route(route) {
            c.routes.push(route);
        },
        reportHealth(name, check) {
            if (!name.trim()) {
                throw new Error(
                    `[sigx actors] ${pluginName} called reportHealth() with an empty name — ` +
                        'a readiness check is identified by its name in the probe body.'
                );
            }
            const clash = c.health.find((entry) => entry.name === name);
            if (clash) {
                // Names key the `checks` map, so a duplicate would overwrite
                // — and could hide a FAILING check behind a passing one,
                // which is the one thing a readiness report must never do.
                throw new Error(
                    `[sigx actors] two plugins both contributed a readiness check named ` +
                        `"${name}" (${clash.plugin} and ${pluginName}) — names key the ` +
                        'readiness report, so give one of them a distinct name.'
                );
            }
            c.health.push({ name, check, plugin: pluginName });
        },
        health() {
            // Reads `c` at CALL time, not registration time — that is what
            // makes the aggregate independent of `.use()` order.
            return healthReport(c);
        },
        reportOps(name, provider) {
            if (!name.trim()) {
                throw new Error(
                    `[sigx actors] ${pluginName} called reportOps() with an empty name — ` +
                        'an ops section is identified by its name in the snapshot.'
                );
            }
            const clash = c.ops.find((entry) => entry.name === name);
            if (clash) {
                // Names key the snapshot, so a duplicate would overwrite —
                // and a consumer reading `ops.metrics` would silently get
                // somebody else's numbers.
                throw new Error(
                    `[sigx actors] two plugins both contributed an ops section named ` +
                        `"${name}" (${clash.plugin} and ${pluginName}) — names key the ` +
                        'ops snapshot, so give one of them a distinct name.'
                );
            }
            c.ops.push({ name, provider, plugin: pluginName });
        },
        ops() {
            // Same call-time read as `health()`, for the same reason.
            return opsReport(c);
        },
        extendContext(factory) {
            c.contextFactories.push(factory);
        }
    };
}

/**
 * Fold N `extendContext` factories into the single one `createSilo` takes.
 *
 * Deliberately NOT `Object.assign`: that copies through [[Set]], so a
 * `__proto__` key (which a plugin forwarding a `JSON.parse`d object really
 * can carry as an OWN property) would silently re-point the merged object's
 * prototype instead of surviving as a plain key. `defineProperty` keeps the
 * merge faithful, which leaves the ONE refusal point in the activation —
 * where the key is reported rather than quietly absorbed here.
 */
function contextExtender(
    factories: readonly ((ref: ActorRef) => object | undefined)[]
): (ref: ActorRef) => object | undefined {
    return (ref) => {
        const merged: Record<string, unknown> = {};
        for (const factory of factories) {
            const produced = factory(ref) as Record<string, unknown> | null | undefined;
            if (!produced) continue;
            for (const key of Object.keys(produced)) {
                Object.defineProperty(merged, key, {
                    value: produced[key],
                    enumerable: true,
                    writable: true,
                    configurable: true
                });
            }
        }
        // Undefined, not `{}`: the activation treats any object as an
        // extension worth merging, so factories that produced nothing
        // should cost nothing per activation.
        return Object.keys(merged).length > 0 ? merged : undefined;
    };
}

/**
 * Fold the plugins' placement, lifecycle hooks and dispatch middleware into
 * ONE `ActorPlacement`. Returns undefined when nothing was contributed, so a
 * plugin-free app gets `createSilo`'s own default local placement rather
 * than a pointless wrapper.
 */
function compositePlacement(c: Contributions): ActorPlacement | undefined {
    const factory = c.placement?.factory;
    if (
        !factory &&
        !c.dispatch.length &&
        !c.beforeActivate.length &&
        !c.afterDeactivate.length
    ) {
        return undefined;
    }

    // dispatcherFor is the hot path — wrap each distinct dispatcher once.
    const wrapped = new WeakMap<ActorDispatcher, ActorDispatcher>();
    const wrap = (dispatcher: ActorDispatcher): ActorDispatcher => {
        if (!c.dispatch.length) return dispatcher;
        let hit = wrapped.get(dispatcher);
        if (!hit) {
            // reduceRight so the FIRST registered middleware ends up outermost.
            hit = c.dispatch.reduceRight((next, middleware) => middleware(next), dispatcher);
            if (__DEV__ && dispatcher.dispatchStream && !hit.dispatchStream) {
                // `dispatchStream` is optional, so a middleware returning
                // just `{ dispatch }` silently drops it — and the silo then
                // fails every `streams:` method blaming the TRANSPORT, which
                // sends you looking in the wrong place entirely.
                console.warn(
                    '[sigx actors] a dispatch middleware returned a dispatcher without ' +
                        '`dispatchStream`, so every `streams:` method will now fail. Forward it: ' +
                        '`...(next.dispatchStream && { dispatchStream: (...a) => ' +
                        'next.dispatchStream!(...a) })`.'
                );
            }
            wrapped.set(dispatcher, hit);
        }
        return hit;
    };

    let local: ActorDispatcher | null = null;
    let inner: ActorPlacement | undefined;

    return {
        dispatcherFor(ref: ActorRef): ActorDispatcher | Promise<ActorDispatcher> {
            if (!inner) return wrap(local!);
            const resolved = inner.dispatcherFor(ref);
            return isPromise(resolved) ? resolved.then(wrap) : wrap(resolved);
        },
        bind(localDispatcher: ActorDispatcher, silo: Silo): PlacementBindings {
            local = localDispatcher;
            // Built HERE, not at plugin setup: `createSilo` calls bind() from
            // its constructor, so this is the first moment a placement can be
            // handed a definition resolver — which is what lets a custom
            // placement read the per-type strategies actors declared.
            inner = factory?.({ definition: (type) => silo.definition(type) });
            return mergeBindings(inner?.bind?.(localDispatcher, silo) || undefined, c);
        },
        // Forwarded, not wrapped: `locate` answers WHERE, and dispatch
        // middleware wraps HOW — a middleware has no say in ownership, and
        // running one here would let it observe calls that never happen.
        // Undefined when the inner placement has no opinion, which is what
        // tells a mount to proxy instead of redirect.
        locate: (ref: ActorRef) => inner?.locate?.(ref),
        start: () => inner?.start?.(),
        beginStop: () => inner?.beginStop?.(),
        stop: () => inner?.stop?.()
    };
}

/**
 * The composition that makes the exclusive seam composable. The placement's
 * own hooks bracket the plugins': its `beforeActivate` (the directory claim)
 * runs FIRST, and its `afterDeactivate` (the claim release) runs LAST, so
 * plugin hooks always observe an activation the cluster already owns.
 */
function mergeBindings(
    inner: PlacementBindings | undefined,
    c: Contributions
): PlacementBindings {
    const before = [
        ...(inner?.beforeActivate ? [inner.beforeActivate.bind(inner)] : []),
        ...c.beforeActivate
    ];
    const after = [
        ...[...c.afterDeactivate].reverse(),
        ...(inner?.afterDeactivate ? [inner.afterDeactivate.bind(inner)] : [])
    ];
    return {
        ...inner,
        ...(before.length
            ? {
                  // NOT caught: a throw must still refuse the activation.
                  async beforeActivate(ref: ActorRef): Promise<void> {
                      for (const hook of before) await hook(ref);
                  }
              }
            : {}),
        ...(after.length
            ? {
                  async afterDeactivate(ref: ActorRef, reason: DeactivationReason): Promise<void> {
                      for (const hook of after) {
                          try {
                              await hook(ref, reason);
                          } catch (error) {
                              if (__DEV__) {
                                  console.error(
                                      '[sigx actors] an afterDeactivate hook threw (ignored):',
                                      error
                                  );
                              }
                          }
                      }
                  }
              }
            : {})
    };
}

function isPromise<T>(value: T | Promise<T>): value is Promise<T> {
    return typeof (value as { then?: unknown })?.then === 'function';
}
