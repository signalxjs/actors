/**
 * `ActorPlacement` over Durable Objects: a ref resolves to the object that
 * holds it, and a call is a `fetch` on that object's stub.
 *
 * **One placement runs on BOTH sides**, distinguished only by `isSelf`.
 *
 * That is the load-bearing decision. Giving the object's own host the plain
 * local host instead looks obvious and corrupts state: `ctx.actor(Cart, 'x')`
 * called from `Counter/alice` would resolve through `LocalHost` and activate
 * `Cart/x` INSIDE `Counter/alice`'s Durable Object, writing that actor's
 * record into the wrong object's storage. Every calling object would get its
 * own copy — single activation violated, with nothing to point at.
 *
 * So a Durable Object hands in `isSelf`, and everything that is not its own
 * actor goes back out to the object that owns it. In the Worker `isSelf` is
 * absent and everything is remote. Self-recursion is impossible by
 * construction: a self-call short-circuits to the local dispatcher before any
 * stub is derived.
 *
 * The wire is `httpTransport()` with its `fetch` swapped for a stub call —
 * its own header says "riding a route rather than its own socket is exactly
 * why this one works on Cloudflare Workers". Re-deriving it would mean
 * re-deriving the envelope (with its skew-proof remaining-ms deadline), the
 * NDJSON reader, the abort linking, and `fromHostWireError`'s branded
 * re-creation — and that last one is a conformance requirement, not a
 * nicety: a forked wire is how a remote `state-conflict` quietly stops
 * satisfying `isActorError` and the runtime stops discarding stale
 * activations.
 */
import type {
    ActorDispatcher,
    ActorPlacement,
    ActorPlacementStrategy,
    ActorRef,
    AnyActorDefinition,
    Host
} from '@sigx/actors';
import {
    fromHostWireError,
    httpTransport,
    hostWireCodec,
    toHostWireError,
    type HostTransportConfig
} from '@sigx/actors/cluster';
import type { ActorPlugin } from '@sigx/actors/host';
import type { DurableObjectNamespaceLike, DurableObjectStubLike } from './types';

/**
 * A DO stub is not routed, but `fetch` still needs a parseable absolute URL.
 * `.invalid` is reserved by RFC 2606 and can never resolve, so a request
 * that somehow escaped to the network would fail loudly rather than reach
 * something real.
 */
const SYNTHETIC_ORIGIN = 'https://sigx.invalid';
const DEFAULT_BASE = '/_sigx/do';

/** NUL separator, matching `actorId()` and this package's storage keys. */
const SEP = '\u0000';

/**
 * The `backend` tag every `@sigx/actors/cluster` policy carries
 * (`CLUSTER_BACKEND` in `cluster/placement.ts`, not exported). A strategy
 * tagged with it is a cluster `PlacementPolicy`, which this backend cannot
 * honour — there is no host to choose.
 */
const CLUSTER_BACKEND = 'cluster';

/** Types already warned about — once per isolate, since objects of one
 *  class share an isolate and every one of them would otherwise repeat it. */
const warnedTypes = new Set<string>();

/**
 * The runtime floor for `defineActor({ placement })` on a DO-hosted actor
 * (#362) — what the types cannot see. `durableObjects()` narrows `placement`
 * to `never`, but only in an app module that installs it itself, and the
 * documented `app` factory never does (the host installs it after the
 * factory runs). So a strategy declared through that factory compiles, and
 * this is where it is caught:
 *
 *  - tagged for the cluster → THROW. A cluster policy here is unambiguously
 *    wrong, not merely unread: its author asked for a host to be chosen,
 *    and none ever will be. Same posture as the cluster placement's own
 *    floor for a tag it does not own (#350). Deliberately not memoized by
 *    the caller: every dispatch to the type fails, not only the first.
 *  - anything else → dev-only warning, once per type. An untagged or
 *    foreign-tagged strategy MAY be meant for a backend this actor is also
 *    deployed on, so it is ignored, but not silently.
 */
function checkDeclaredPlacement(type: string, declared: ActorPlacementStrategy | undefined): void {
    if (!declared) return;
    const name = declared.name ?? 'unnamed';
    if (declared.backend === CLUSTER_BACKEND) {
        throw new Error(
            `[sigx actors-cloudflare] actor "${type}" declares placement "${name}", a cluster ` +
                `PlacementPolicy — Durable Objects cannot honour it: a ref maps to its object ` +
                `by name, and there is no host to choose. Remove \`placement\` from the ` +
                `definition (an app built on durableObjectsHosted() refuses it at compile ` +
                `time), or host the actor on cluster().`
        );
    }
    if (!__DEV__ || warnedTypes.has(type)) return;
    warnedTypes.add(type);
    console.warn(
        `[sigx actors-cloudflare] actor "${type}" declares placement "${name}" — ` +
            `Durable Objects ignore it; a ref maps to its object by name`
    );
}

export interface DurableObjectPlacementOptions {
    /**
     * The Durable Object namespace binding. A function form covers a
     * per-type class (different classes need different migrations), and
     * must answer for every type the host can reach.
     */
    namespace:
        | DurableObjectNamespaceLike
        | ((ref: ActorRef) => DurableObjectNamespaceLike | undefined);
    /**
     * True when THIS host hosts `ref` — i.e. we are the Durable Object that
     * owns it. Absent (the Worker) means nothing is local and every call is
     * a stub fetch.
     *
     * A Durable Object answers `ctx.id.name === objectName(ref)`, which
     * reads no storage and so is available on a cold `alarm()` before
     * anything has been activated.
     */
    isSelf?(ref: ActorRef): boolean;
    /**
     * Override the object name a ref derives.
     *
     * **Changing this repoints every actor at a different object**, which is
     * a state migration, not a config tweak — and it must be byte-identical
     * in the Worker and in every Durable Object, or the two disagree about
     * where an actor lives.
     */
    objectName?(ref: ActorRef): string;
    /**
     * Where a NEW object is first created. A hint only: it does not change
     * the derived id, so it is safe to vary and safe to change later.
     */
    locationHint?: string | ((ref: ActorRef) => string | undefined);
    /**
     * Data-residency jurisdiction. Unlike `locationHint` this DOES change
     * the derived id, so it is part of an actor's identity and must match on
     * both sides. Per-ref because residency is a property of the customer,
     * not of the deployment.
     */
    jurisdiction?: string | ((ref: ActorRef) => string | undefined);
    /** Path prefix of the object's internal mount. Default `/_sigx/do`. */
    base?: string;
    /** Identity stamped into the envelope's `from`. Default `cf`. */
    hostId?: string;
}

/**
 * The object name a ref derives by default: `type<NUL>key`, the runtime's own
 * actor id. Real keys may contain `/` or `:`, so neither is a safe separator.
 *
 * Exported because the Worker and the Durable Object must agree on it byte
 * for byte — the object uses it to recognise its OWN actor, and a divergence
 * means the two disagree about where an actor lives.
 */
export function durableObjectName(ref: ActorRef): string {
    return `${ref.type}${SEP}${ref.key}`;
}

/** Ref → object name and stub, exactly as the placement derives them. */
export interface DurableObjectStubResolver {
    name(ref: ActorRef): string;
    stub(ref: ActorRef): DurableObjectStubLike;
}

/**
 * The ref → object derivation, extracted so every surface that needs a stub
 * — the placement's dispatchers and the object-terminated socket's
 * forwarding route — shares ONE implementation. Two copies of this logic is
 * how a Worker and a route come to disagree about where an actor lives,
 * which is precisely the failure `#guardIdentity` exists to catch late; this
 * removes the way to cause it early.
 *
 * Stubs are derived fresh per call and never cached — a stub is an I/O
 * object bound to the request context that created it.
 */
export function durableObjectStubResolver(
    options: Pick<
        DurableObjectPlacementOptions,
        'namespace' | 'objectName' | 'locationHint' | 'jurisdiction'
    >
): DurableObjectStubResolver {
    const objectName = options.objectName ?? durableObjectName;

    const namespaceFor = (ref: ActorRef): DurableObjectNamespaceLike => {
        const resolved =
            typeof options.namespace === 'function'
                ? options.namespace(ref)
                : options.namespace;
        if (!resolved) {
            throw new Error(
                `[sigx actors-cloudflare] no Durable Object namespace for actor type ` +
                    `"${ref.type}" — the namespace resolver returned nothing. Check the ` +
                    `binding names in wrangler.jsonc against the types you registered.`
            );
        }
        const jurisdiction =
            typeof options.jurisdiction === 'function'
                ? options.jurisdiction(ref)
                : options.jurisdiction;
        if (jurisdiction === undefined) return resolved;
        if (!resolved.jurisdiction) {
            throw new Error(
                `[sigx actors-cloudflare] a jurisdiction ("${jurisdiction}") was requested ` +
                    `for ${ref.type}/${ref.key}, but this namespace binding does not support ` +
                    `jurisdictions.`
            );
        }
        return resolved.jurisdiction(jurisdiction);
    };

    return {
        name: objectName,
        stub(ref: ActorRef): DurableObjectStubLike {
            const namespace = namespaceFor(ref);
            const hint =
                typeof options.locationHint === 'function'
                    ? options.locationHint(ref)
                    : options.locationHint;
            const id = namespace.idFromName(objectName(ref));
            return hint === undefined
                ? namespace.get(id)
                : namespace.get(id, { locationHint: hint });
        }
    };
}

/** What one definition read tells the router about a type. */
interface TypeShape {
    readonly stateless: boolean;
}

/** Before the host is bound, or when a lazy load failed: route as a plain
 *  stateful actor and let the dispatch path report whatever is wrong. */
const UNKNOWN_SHAPE: TypeShape = { stateless: false };

export function durableObjectPlacement(
    options: DurableObjectPlacementOptions
): ActorPlacement {
    const base = options.base ?? DEFAULT_BASE;
    if (!base.startsWith('/')) {
        // It is compared against a URL pathname, which always starts with
        // "/", so a base without one matches nothing and every dispatch
        // fails somewhere far from the cause. Same check `cluster()` makes
        // of `internalBase`.
        throw new Error(
            `[sigx actors-cloudflare] durableObjectPlacement({ base }) must start with "/" ` +
                `— got ${JSON.stringify(base)}.`
        );
    }
    const config: HostTransportConfig = {
        hostId: options.hostId ?? 'cf',
        epoch: 0,
        // No `secret`: a stub is not network-reachable, and the only way to
        // obtain one is to hold the namespace binding — a Worker-level
        // capability grant. Guards therefore run ONCE, at the public edge,
        // where the request still carries real client headers.
        internalBase: base,
        codec: hostWireCodec,
        toWireError: toHostWireError,
        fromWireError: fromHostWireError
    };

    let local: ActorDispatcher | null = null;
    let boundHost: Host | null = null;
    /** Per-type memo of what the definition says — same lazy shape as the
     *  cluster placement's. One definition read per type, then a map hit. */
    const shapes = new Map<string, TypeShape>();

    const requireLocal = (): ActorDispatcher => {
        if (!local) {
            throw new Error(
                '[sigx actors-cloudflare] the placement has no local dispatcher yet — ' +
                    'createHost() binds it, so this ran before the host was built.'
            );
        }
        return local;
    };

    /**
     * One definition read answers two questions: is the type stateless,
     * and did it declare a `placement` this backend cannot honour. The
     * placement check runs BEFORE the memo is written, so a throw is not
     * remembered as "checked" and the next dispatch fails the same way.
     */
    const inspect = (type: string, def: AnyActorDefinition | null): TypeShape => {
        checkDeclaredPlacement(type, def?.__sigxActor.placement);
        const shape: TypeShape = { stateless: def?.__sigxActor.stateless !== undefined };
        shapes.set(type, shape);
        return shape;
    };

    const shapeOf = (type: string): TypeShape | Promise<TypeShape> => {
        const memo = shapes.get(type);
        if (memo !== undefined) return memo;
        if (!boundHost) return UNKNOWN_SHAPE; // pre-bind: nothing dispatches yet
        const resolved = boundHost.definition(type);
        if (resolved && typeof (resolved as PromiseLike<unknown>).then === 'function') {
            return (resolved as Promise<AnyActorDefinition | null>).then(
                (def) => inspect(type, def),
                // A failed module load is the dispatch path's problem to
                // report, not the placement's — and not memoized, so a
                // later successful load is still inspected.
                () => UNKNOWN_SHAPE
            );
        }
        return inspect(type, resolved as AnyActorDefinition | null);
    };

    const resolver = durableObjectStubResolver(options);

    const remoteFor = (ref: ActorRef): ActorDispatcher => {
        const stub = resolver.stub(ref);
        // One transport per target: building it is closure creation, and
        // capturing the stub directly beats smuggling the object name
        // through a synthetic address.
        const transport = httpTransport({
            fetch: ((input: string, init?: RequestInit) =>
                stub.fetch(input, init)) as unknown as typeof globalThis.fetch
        })(config);
        const dispatcher = transport.dispatcherFor({
            // Diagnostics only — this rides error messages, and an actor
            // label reads better there than a synthetic host id.
            hostId: `do:${ref.type}/${ref.key}`,
            epoch: 0,
            address: SYNTHETIC_ORIGIN,
            status: 'active'
        });
        if (!dispatcher) {
            // `HostTransport.dispatcherFor` is nullable so a transport can
            // decline a peer it cannot reach and fall through a chain. HTTP
            // never declines — every descriptor carries an address, which is
            // why it is only ever valid last in such a chain — and there is
            // no chain here anyway.
            throw new Error(
                `[sigx actors-cloudflare] the HTTP transport declined to dispatch to ` +
                    `${ref.type}/${ref.key}, which it is not supposed to be able to do.`
            );
        }
        return dispatcher;
    };

    return {
        /**
         * Capture the local dispatcher for the `isSelf` case. No
         * `PlacementBindings`: there is no directory to claim (the platform
         * IS the directory), no reminder shard to own (a DO holds one
         * actor), and a present call chain inside a DO is genuinely local.
         */
        bind(dispatcher: ActorDispatcher, host: Host): void {
            local = dispatcher;
            boundHost = host;
        },

        dispatcherFor(ref: ActorRef): ActorDispatcher | Promise<ActorDispatcher> {
            // The definition read comes FIRST, ahead of `isSelf`: the
            // declared-placement floor has to cover a Durable Object's own
            // actor too, and a check hung off the stateless lookup alone
            // would miss it. After the first dispatch per type this is one
            // map hit and synchronous.
            const shape = shapeOf(ref.type);
            return shape instanceof Promise
                ? shape.then((resolved) => route(ref, resolved))
                : route(ref, shape);
        }
    };

    function route(ref: ActorRef, shape: TypeShape): ActorDispatcher {
        if (options.isSelf?.(ref)) return requireLocal();
        // Stateless workers never map to an object — they run right here,
        // in whichever isolate is dispatching. Note there is no idle sweep
        // on Workers (`sweepIntervalMs: 0`), so a pool shrinks only when the
        // isolate is evicted — which the platform does anyway, and a worker
        // loses nothing when it happens.
        if (shape.stateless) return requireLocal();
        // Built fresh every time, and NOT cached. A stub is an I/O object
        // bound to the request context that created it, and workerd refuses
        // to use one from a different request — so a cache that outlives a
        // request turns every call after the first into "unreachable".
        // Rebuilding costs one `idFromName` hash and a closure; a cache
        // costs correctness.
        return remoteFor(ref);
    }
}

/**
 * The placement as an `ActorPlugin`, for `defineActorApp().use(...)`.
 *
 * `setPlacement` is EXCLUSIVE, and that exclusivity is the recursion guard:
 * if an app already installed this plugin, the host adding its own throws
 * naming both, instead of a Durable Object quietly dispatching to itself.
 *
 * Carries `never` as its `Placement` (#351): this placement reads no
 * strategy — a ref maps to its object by name, and the platform IS the
 * directory — so the app-bound `defineActor` of a DO app refuses
 * `placement` outright, where a cluster policy used to compile and be a
 * silent no-op at runtime.
 *
 * That narrowing reaches only an app whose module calls
 * `.use(durableObjects(...))` itself. The `app` factory handed to
 * `createHostDurableObject()` / `createWorkerHandler()` never does — the
 * host installs this plugin after the factory runs, and refuses a factory
 * that installed it already — so a `defineActor` bound from that factory
 * still carries the wide `Placement`. `durableObjectsHosted()` is the
 * factory's way to get the same narrowing, and the placement's own floor
 * (`checkDeclaredPlacement`) catches at dispatch whatever the types did not
 * (#362).
 */
export function durableObjects(
    options: DurableObjectPlacementOptions
): ActorPlugin<Record<never, never>, never> {
    return {
        name: 'cloudflare:durable-objects',
        setup(registry) {
            registry.setPlacement(() => durableObjectPlacement(options));
        }
    };
}

/**
 * The type-level twin of `durableObjects()` for the `app` FACTORY — a
 * plugin that installs nothing and only narrows.
 *
 * A factory handed to `createHostDurableObject()` / `createWorkerHandler()`
 * cannot install `durableObjects()`: it has no `env` (and so no namespace
 * binding), and the host claims the placement seam itself after the factory
 * runs — `setPlacement` is exclusive, so a factory that claimed it too is
 * refused. This plugin makes no claim, which is exactly why the host accepts
 * it, and carries `never` as its `Placement`, so
 *
 *     export const createApp = (base) =>
 *         defineActorApp(base).use(durableObjectsHosted());
 *     export const { defineActor } = createApp({});
 *
 * gives every actor module a `defineActor` that refuses `placement` at
 * compile time — the same contract a `.use(durableObjects(...))` app has,
 * reached without a binding in hand. Whatever still slips past (the bare
 * `defineActor` from `@sigx/actors`, JavaScript) meets the runtime floor in
 * `durableObjectPlacement()`.
 */
export function durableObjectsHosted(): ActorPlugin<Record<never, never>, never> {
    return {
        name: 'cloudflare:durable-objects-hosted',
        setup() {
            // Nothing to register: the host installs the real placement.
        }
    };
}
