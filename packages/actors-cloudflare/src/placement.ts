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
import type { ActorDispatcher, ActorPlacement, ActorRef, Host } from '@sigx/actors';
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
    const objectName = options.objectName ?? durableObjectName;
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

    const stubFor = (ref: ActorRef, name: string): DurableObjectStubLike => {
        const namespace = namespaceFor(ref);
        const hint =
            typeof options.locationHint === 'function'
                ? options.locationHint(ref)
                : options.locationHint;
        const id = namespace.idFromName(name);
        return hint === undefined
            ? namespace.get(id)
            : namespace.get(id, { locationHint: hint });
    };

    const remoteFor = (ref: ActorRef, name: string): ActorDispatcher => {
        const stub = stubFor(ref, name);
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
        bind(dispatcher: ActorDispatcher, _host: Host): void {
            local = dispatcher;
        },

        dispatcherFor(ref: ActorRef): ActorDispatcher {
            if (options.isSelf?.(ref)) {
                if (!local) {
                    throw new Error(
                        '[sigx actors-cloudflare] the placement has no local dispatcher yet — ' +
                            'createHost() binds it, so this ran before the host was built.'
                    );
                }
                return local;
            }
            // Built fresh every time, and NOT cached. A stub is an I/O
            // object bound to the request context that created it, and
            // workerd refuses to use one from a different request — so a
            // cache that outlives a request turns every call after the first
            // into "unreachable". Rebuilding costs one `idFromName` hash and
            // a closure; a cache costs correctness.
            return remoteFor(ref, objectName(ref));
        }
    };
}

/**
 * The placement as an `ActorPlugin`, for `defineActorApp().use(...)`.
 *
 * `setPlacement` is EXCLUSIVE, and that exclusivity is the recursion guard:
 * if an app already installed this plugin, the host adding its own throws
 * naming both, instead of a Durable Object quietly dispatching to itself.
 */
export function durableObjects(options: DurableObjectPlacementOptions): ActorPlugin {
    return {
        name: 'cloudflare:durable-objects',
        setup(registry) {
            registry.setPlacement(() => durableObjectPlacement(options));
        }
    };
}
