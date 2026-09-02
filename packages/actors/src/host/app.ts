/**
 * `defineActorApp` — the composition root over `createHost`.
 *
 * `createHost` takes exactly one `placement` and one `storage`, and
 * `ActorPlacement.bind()` → `PlacementBindings` is the only lifecycle-hook
 * shape in the package. That makes the seams EXCLUSIVE: two things that both
 * want `beforeActivate` (a cluster directory and an audit log) cannot
 * coexist. An app fixes that by folding every plugin's contributions into
 * the single composite placement / storage / context that `createHost`
 * already understands — so this layer is purely additive and `createHost`
 * stays the documented low-level primitive.
 *
 * The app is an inert DESCRIPTION, not a running host. That is what lets the
 * same module be started by a Node entry, the Vite dev server, or a Worker.
 */
import type { TypeHandler } from '@sigx/serialize';
import { defineActor as defineActorFn } from '../define';
import { defineWorker as defineWorkerFn } from '../define-worker';
import type {
    ActorDefinition,
    ActorDispatcher,
    ActorMethodTable,
    ActorOptions,
    WorkerOptions,
    ActorPlacement,
    ActorPlacementStrategy,
    ActorRef,
    ActorStorage,
    ActorReminders,
    ActorScheduler,
    ActorStreamTable,
    ActorTurnObserver,
    AnyActorDefinition,
    DeactivationReason,
    PlacementBindings,
    Host,
    ActorTaskLiveness
} from '../types';
import { createHost, type CreateHostOptions, type HostDefaults } from './host';
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
 * hands the placement the host itself, so this exposes only what a backend
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
 * One plugin's answer to "should this host be receiving traffic?".
 *
 * SYNC on purpose: a readiness probe must never wait on a store round-trip,
 * which is the same reason `ClusterMembership.view()` is sync.
 */
export interface HealthCheck {
    /** False = take this host out of rotation. */
    ready: boolean;
    /**
     * This process cannot recover — fail LIVENESS, not just readiness, so
     * the orchestrator restarts it. Reserve it for terminal states (a
     * fenced host, a poisoned runtime): `ready: false` alone means "drain
     * me, I am still alive", and conflating the two turns every drain into
     * a restart. Implies not-ready.
     */
    fatal?: boolean;
    /** Short state word for the probe body, e.g. `'active'`, `'leaving'`. */
    detail?: string;
}

/** The aggregate of every contributed check. */
export interface HealthReport {
    /** True iff every check passed. An app with no checks is ready. */
    ready: boolean;
    /** True if ANY check declared the process unrecoverable. */
    fatal: boolean;
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
    handle(request: Request, host: Host): Promise<Response>;
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
     *
     * Forward every member you do not deliberately replace, `saveText`
     * included — and forward it CONDITIONALLY, so an inner storage without
     * it does not appear to have it. Returning a fixed three-method literal
     * silently drops the single-walk save path (#238): the host reverts to
     * encoding a tree the adapter then re-walks, correct but slower, with
     * nothing to say it happened.
     */
    decorateStorage(decorate: (inner: ActorStorage) => ActorStorage): void;
    /**
     * Claim the placement seam — the backend that answers WHO hosts an
     * actor (the local host, a cluster, Durable Objects). EXCLUSIVE: a
     * second claim throws, naming both plugins, because two placements
     * would mean two answers to where an actor lives.
     *
     * A FACTORY, not an instance: it runs once the host exists, so a custom
     * placement can resolve per-type strategies declared on the actors
     * (`defineActor({ placement })` — declared on the actor itself) instead of
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
     * After the host is live (post `start()`), in registration order.
     * Throwing ROLLS THE START BACK: the host is stopped again and the
     * `onStop` hooks run, so a rejected `start()` leaks no background work.
     */
    onStart(hook: (host: Host) => void | Promise<void>): void;
    /**
     * After the host has drained (post `stop()`), in REVERSE order, each
     * isolated. Also runs when a start is rolled back — so treat it as
     * best-effort cleanup that may fire even if this plugin's `onStart`
     * never completed.
     */
    onStop(hook: (host: Host) => void | Promise<void>): void;
    route(route: ActorRoute): void;
    /**
     * Contribute a readiness check — the seam a health endpoint aggregates.
     * Composes: EVERY contributed check must pass for the host to report
     * ready, so any plugin can take its host out of rotation without the
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
     * reason: the endpoint is what you reach for when the host is already
     * unwell, and it must not be able to hang.
     *
     * A throwing provider is caught and its section replaced with
     * `{ error }`, leaving every other section intact. The one tool that
     * explains a broken host must not be broken BY it.
     *
     * `name` keys the report, so it must be non-empty and unique across
     * plugins; a clash throws at setup naming both.
     */
    reportOps(name: string, provider: () => unknown): void;
    /**
     * Contribute a named DIGEST — a mergeable, wire-safe summary of this
     * plugin's counters, for cluster-wide aggregation.
     *
     * The counterpart to `reportOps`, and deliberately not the same seam.
     * An ops section is read by a human and may carry anything, including
     * derived percentiles; a digest is read by `clusterStats()` and has to
     * carry the raw distribution those percentiles are re-derived FROM,
     * because averaging percentiles across hosts produces a number that is
     * not the p99 of anything. Reading the ops section instead would hand
     * the aggregator exactly the un-mergeable shape.
     *
     * Same rules as `reportOps` otherwise: runs per read, must stay sync,
     * unique non-empty name, and a throwing provider costs its own digest
     * rather than the whole report.
     */
    reportDigest(name: string, provider: (options?: unknown) => unknown): void;
    /**
     * The aggregate of every contributed section — LIVE, including providers
     * registered after this call, so `.use()` order does not matter. Hold
     * this and call it per request rather than at setup time.
     */
    ops(): OpsReport;
    /**
     * Read ONE named digest, live — including providers registered after
     * this call, so `.use()` order does not matter.
     *
     * `undefined` when nobody publishes that name, which is the ordinary
     * case for a host with no `metrics()` attached rather than an error.
     *
     * **Never throws** — not for a missing name, not for a provider that
     * fails, and not for one that reads its own digest. Whatever is wrong
     * costs that digest and nothing else, because the read is usually part
     * of an ops or cluster report, and a report that dies on a broken
     * provider is the report you cannot get during the incident.
     */
    digest(name: string, options?: unknown): unknown;
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
 *
 * `Placement` is the same device for `ActorOptions.placement`: a plugin
 * that installs a placement backend names the strategy type it understands
 * (`cluster()` → `PlacementPolicy`), `app.use()` intersects it into the
 * app, and the app-bound `defineActor` accepts only that — so a strategy
 * tagged for another backend fails to compile where the runtime would have
 * ignored it silently, and a malformed untagged one fails to compile where
 * the runtime would have thrown at dispatch (#58). Plugins that install no
 * placement leave it at the widest reading, `ActorPlacementStrategy`.
 */
export interface ActorPlugin<
    Ext extends object = Record<never, never>,
    Placement extends ActorPlacementStrategy = ActorPlacementStrategy
> {
    readonly name: string;
    setup(registry: PluginRegistry): void;
    /** @internal phantom — carries `Ext` for inference only. */
    readonly __ext?: Ext | undefined;
    /** @internal phantom — carries `Placement` for inference only. */
    readonly __placement?: Placement | undefined;
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
    actors?: CreateHostOptions['actors'];
    /** Base storage, before any plugin decorators. Omit = in-memory. */
    storage?: ActorStorage;
    types?: readonly TypeHandler[];
    /** The clock for background work. Default: host timers. */
    scheduler?: ActorScheduler;
    /** Durable-reminder implementation. Default: the sharded table. */
    reminders?: ActorReminders;
    /** Task-liveness implementation (#310). Default: the per-host roster. */
    taskLiveness?: ActorTaskLiveness;
    defaults?: HostDefaults;
}

export interface ActorApp<
    Ext extends object = Record<never, never>,
    Placement extends ActorPlacementStrategy = ActorPlacementStrategy
> {
    /**
     * Register a plugin. Mutates and returns THIS app, widened by `Ext` and
     * narrowed to the plugin's `Placement` (see `ActorPlugin`). The
     * intersection is deliberate: a second placement backend with a
     * different tag narrows `placement` to nothing, because no one strategy
     * can satisfy two backends.
     */
    use<E extends object, P extends ActorPlacementStrategy = ActorPlacementStrategy>(
        plugin: ActorPlugin<E, P>
    ): ActorApp<Ext & E, Placement & P>;
    /**
     * Supply the actor registry when the app was built without one — what
     * a HOST does: the Vite plugin hands over the registry it already
     * builds, so the app module itself stays runtime-neutral.
     *
     * Throws if the app already has actors, so a host can never silently
     * replace what the author configured — and, like `use()`, once the app
     * has been started or its `routes` read: by then the registry is
     * already baked into a running host.
     */
    withActors(actors: NonNullable<ActorAppOptions['actors']>): ActorApp<Ext, Placement>;
    /** Whether a registry has been supplied yet. */
    readonly hasActors: boolean;
    /**
     * `defineActor` bound to this app's plugin set — the same function at
     * runtime, with `ctx` typed as the built-ins plus every plugin's
     * additions, and `placement` narrowed to what the app's placement
     * plugin understands. Destructure it
     * (`export const { defineActor } = app`) and import it from your actor
     * modules.
     */
    defineActor<
        S extends object,
        M extends ActorMethodTable,
        St extends ActorStreamTable = Record<never, never>
    >(
        options: ActorOptions<S, M, St, Ext, Placement>
    ): ActorDefinition<S, M, St>;
    /**
     * `defineWorker` bound to this app's plugin set — same deal as the
     * bound `defineActor`: identical function at runtime, with the worker
     * ctx widened by every plugin's additions.
     */
    defineWorker<M extends ActorMethodTable, St extends ActorStreamTable = Record<never, never>>(
        options: WorkerOptions<M, St, Ext>
    ): ActorDefinition<Record<never, never>, M, St>;
    /** Plugin-contributed routes. Reading this seals the app. */
    readonly routes: readonly ActorRoute[];
    /** The running host — null before `start()` and again after `stop()`. */
    readonly host: Host | null;
    /** Build the host, start it, then run every `onStart`. Idempotent. */
    start(): Promise<Host>;
    /** Stop the host, then run every `onStop` in reverse. Idempotent. */
    stop(opts?: { timeoutMs?: number }): Promise<void>;
}

/**
 * A plugin's turn subscription, which outlives the host's construction: it
 * is declared during `setup()` but can only really be attached once the
 * host exists, and may be cancelled from either side of that boundary.
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
    onStart: ((host: Host) => void | Promise<void>)[];
    onStop: ((host: Host) => void | Promise<void>)[];
    routes: ActorRoute[];
    health: { name: string; check: () => HealthCheck; plugin: string }[];
    ops: { name: string; provider: () => unknown; plugin: string }[];
    digests: { name: string; provider: (options?: unknown) => unknown; plugin: string }[];
    contextFactories: ((ref: ActorRef) => object | undefined)[];
}

/**
 * Fold the contributed checks into one report. A throwing check is
 * not-ready rather than an exception: an endpoint that 500s when a check is
 * broken tells an operator nothing about the host.
 */
function healthReport(c: Contributions): HealthReport {
    const checks: Record<string, HealthCheck> = {};
    let ready = true;
    let fatal = false;
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
        // Fatal implies not-ready: a check that says "restart me" must
        // never leave the host in rotation on a disagreeing `ready`.
        if (result.fatal) result = { ...result, ready: false };
        checks[name] = result;
        if (!result.ready) ready = false;
        if (result.fatal) fatal = true;
    }
    return { ready, fatal, checks };
}

/**
 * Fold the contributed ops sections into one report.
 *
 * Same rule as `healthReport`, and for a sharper reason: this endpoint is
 * what an operator opens when a host is already misbehaving, so one plugin
 * whose provider throws must cost its own section and nothing else. The
 * section stays present, carrying the reason.
 */
/**
 * Invoke ONE digest provider.
 *
 * Only `c.digests` is ever walked, never `c.ops` — which is what makes it
 * structurally impossible for this to re-enter an ops provider. That
 * matters because `cluster()` publishes `placement.report()` AS an ops
 * section, and `report()` is the caller here: routing this through
 * `registry.ops()` would recurse until the stack gave out.
 *
 * A provider that throws costs its own digest and nothing else, for the
 * same reason a failing ops section does: the one tool that explains a
 * broken host must not be broken by it.
 */
function readDigest(c: Contributions, name: string, options?: unknown): unknown {
    const entry = c.digests.find((candidate) => candidate.name === name);
    if (!entry) return undefined;
    if (reading.has(name)) {
        // A provider that reads its own digest is a bug, but it is ITS bug.
        // Throwing here would take out the surrounding ops or cluster read,
        // which is the same mistake as letting a failing section 500 the
        // endpoint that exists to explain a sick host. Say so loudly in
        // dev, answer `undefined` in production.
        if (__DEV__) {
            console.warn(
                `[sigx actors] the "${name}" digest provider read its own digest. ` +
                    'A digest provider must not call registry.digest() for the name it ' +
                    'provides; the read answered undefined to break the cycle.'
            );
        }
        return undefined;
    }
    reading.add(name);
    try {
        return entry.provider(options);
    } catch {
        return undefined;
    } finally {
        reading.delete(name);
    }
}

/** Names currently being produced, so a provider cannot re-enter itself. */
const reading = new Set<string>();

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
    #host: Host | null = null;
    #starting: Promise<Host> | null = null;
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

    use<E extends object, P extends ActorPlacementStrategy = ActorPlacementStrategy>(
        plugin: ActorPlugin<E, P>
    ): ActorApp<E, P> {
        if (this.#contributions) {
            throw new Error(
                `[sigx actors] cannot .use(${plugin.name}) after the app has been started ` +
                    '(or its routes read) — register every plugin before starting.'
            );
        }
        this.#plugins.push(plugin as ActorPlugin<object>);
        return this as unknown as ActorApp<E, P>;
    }

    // The app-bound `defineActor` IS the root one — only its type differs,
    // so the build transform sees the very same call it always did.
    readonly defineActor: ActorApp['defineActor'] = defineActorFn;
    readonly defineWorker: ActorApp['defineWorker'] = defineWorkerFn;

    get routes(): readonly ActorRoute[] {
        return this.#seal().routes;
    }

    get host(): Host | null {
        return this.#host;
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
            digests: [],
            contextFactories: []
        };
        for (const plugin of this.#plugins) {
            plugin.setup(registryFor(plugin.name, c));
        }
        this.#contributions = c;
        return c;
    }

    start(): Promise<Host> {
        if (this.#stopped) {
            return Promise.reject(
                new Error(
                    '[sigx actors] this app has been stopped and cannot be restarted — a ' +
                        'plugin-provided placement carries per-run identity (a cluster host id ' +
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
            this.#host = null;
            throw error;
        });
        this.#starting = starting;
        return starting;
    }

    async #start(): Promise<Host> {
        if (!this.#options.actors) {
            throw new Error(
                '[sigx actors] this app has no actors. Pass them to defineActorApp({ actors }), ' +
                    'or let the Vite plugin supply them with sigxActors({ app }).'
            );
        }
        const c = this.#seal();
        // A decorator has to wrap something concrete, so materialize the
        // in-memory default here rather than leaving it to `createHost` —
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
        const host = createHost({
            actors: this.#options.actors!,
            ...(storage ? { storage } : {}),
            ...(placement ? { placement } : {}),
            ...(c.typeHandlers.length ? { types: c.typeHandlers } : {}),
            ...(this.#options.scheduler ? { scheduler: this.#options.scheduler } : {}),
            ...(this.#options.reminders ? { reminders: this.#options.reminders } : {}),
            ...(this.#options.taskLiveness ? { taskLiveness: this.#options.taskLiveness } : {}),
            ...(c.contextFactories.length
                ? { extendContext: contextExtender(c.contextFactories) }
                : {}),

            ...(this.#options.defaults ? { defaults: this.#options.defaults } : {})
        });
        this.#host = host;
        // Attach turn observers now the host exists. Each keeps its own
        // detach so a plugin can switch observation off later, and the last
        // one leaving restores the untouched dispatch path.
        for (const subscription of c.turnObservers) {
            if (subscription.cancelled) continue;
            subscription.detach = host.observeTurns(subscription.observer);
        }
        await host.start();
        try {
            for (const hook of c.onStart) await hook(host);
        } catch (error) {
            // The host is ALREADY live here — sweeper, reminder tick, a
            // cluster join, and the ambient `__SIGX_ACTOR_HOST__` seam. A
            // rejected start that just walked away would leak all of it and
            // leave the seam pointing at a host this app no longer tracks,
            // so roll the start back before surfacing the failure.
            try {
                await host.stop();
            } catch (stopError) {
                if (__DEV__) {
                    console.error(
                        '[sigx actors] rolling back a failed start did not stop cleanly:',
                        stopError
                    );
                }
            }
            await this.#runStopHooks(host, c);
            throw error;
        }
        return host;
    }

    /** `onStop`, reverse order, each isolated — one bad plugin must not
     *  strand the rest of the teardown. */
    async #runStopHooks(host: Host, c: Contributions): Promise<void> {
        for (const hook of [...c.onStop].reverse()) {
            try {
                await hook(host);
            } catch (error) {
                if (__DEV__) {
                    console.error('[sigx actors] a plugin onStop hook threw (ignored):', error);
                }
            }
        }
    }

    async stop(opts?: { timeoutMs?: number }): Promise<void> {
        if (this.#stopped) return;
        // A stop racing an in-flight start must still tear that host down.
        if (this.#starting) {
            try {
                await this.#starting;
            } catch {
                // A failed start left nothing running to stop.
            }
        }
        const host = this.#host;
        // Never started (or the start failed): nothing to tear down, and the
        // app stays startable.
        if (!host) return;
        this.#stopped = true;
        // Actors drain FIRST: an `onStop` hook may close a connection the
        // draining turns still need.
        try {
            await host.stop(opts);
        } catch (error) {
            // Teardown did NOT complete — a placement's membership leave can
            // throw against a dead store. Stay exactly as we were rather
            // than reporting a shutdown that did not happen: `app.host`
            // remains reachable so the failure is inspectable, and `stop()`
            // can be retried (the host's own stop is idempotent, so a retry
            // proceeds to the hooks below).
            this.#stopped = false;
            throw error;
        }
        this.#starting = null;
        this.#host = null;
        await this.#runStopHooks(host, this.#seal());
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
        reportDigest(name, provider) {
            if (!name.trim()) {
                throw new Error(
                    `[sigx actors] ${pluginName} called reportDigest() with an empty name — ` +
                        'a digest is identified by its name.'
                );
            }
            const clash = c.digests.find((entry) => entry.name === name);
            if (clash) {
                throw new Error(
                    `[sigx actors] two plugins both contributed a digest named ` +
                        `"${name}" (${clash.plugin} and ${pluginName}) — names address a ` +
                        'digest, so give one of them a distinct name.'
                );
            }
            c.digests.push({ name, provider, plugin: pluginName });
        },
        digest(name, options) {
            return readDigest(c, name, options);
        },
        extendContext(factory) {
            c.contextFactories.push(factory);
        }
    };
}

/**
 * Fold N `extendContext` factories into the single one `createHost` takes.
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
 * plugin-free app gets `createHost`'s own default local placement rather
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
            // `dispatchStream` and `dispatchWatch` are both OPTIONAL on
            // ActorDispatcher, so a middleware returning just `{ dispatch }`
            // silently drops them — and the host then fails every `streams:`
            // method, or every watch, blaming the TRANSPORT, which sends you
            // looking in the wrong place entirely. Warning on both because
            // warning on only one is precisely how `metrics()` shipped
            // forwarding the stream and eating every `$live` subscription.
            if (__DEV__) {
                for (const [name, breaks] of [
                    ['dispatchStream', 'every `streams:` method'],
                    ['dispatchWatch', 'every watch, and with it `$live`']
                ] as const) {
                    if (!dispatcher[name] || hit[name]) continue;
                    console.warn(
                        `[sigx actors] a dispatch middleware returned a dispatcher without ` +
                            `\`${name}\`, so ${breaks} will now fail. Forward it: ` +
                            `\`...(next.${name} && { ${name}: (...a) => next.${name}!(...a) })\`.`
                    );
                }
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
        bind(localDispatcher: ActorDispatcher, host: Host): PlacementBindings {
            local = localDispatcher;
            // Built HERE, not at plugin setup: `createHost` calls bind() from
            // its constructor, so this is the first moment a placement can be
            // handed a definition resolver — which is what lets a custom
            // placement read the per-type strategies actors declared.
            inner = factory?.({ definition: (type) => host.definition(type) });
            return mergeBindings(inner?.bind?.(localDispatcher, host) || undefined, c);
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
