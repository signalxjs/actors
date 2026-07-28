/**
 * The silo-to-silo transport seam.
 *
 * `SiloTransport` owns BOTH halves of a link: the outbound dispatcher for a
 * peer, and the listener that answers peers dispatching at us. That is why
 * this is a lifecycle object and not just `dispatcherFor` — a
 * connection-oriented transport has sockets to open, departed peers to
 * forget, and a port to advertise before membership publishes it.
 *
 * HTTP (`httpTransport()`) is the default and the only one that is
 * WinterCG-clean; WebSocket and raw TCP ship as separate packages so this
 * entry stays zero-dep and Workers-safe. Nothing here imports them — a
 * transport package depends on `@sigx/actors/cluster`, never the reverse.
 */
import type { ActorCallContext, ActorDispatcher, ActorRef } from '../types';
import type { ActorRoute } from '../silo/app';
import type { MembershipView, SiloDescriptor } from './types';

/**
 * Values ⇄ wire form. THE codec the public wire uses, handed over rather
 * than re-derived: a transport that `JSON.stringify`s raw values silently
 * drops every `__SIGX_SERVERFN_CODEC__` type handler and re-opens
 * `__proto__`.
 */
export interface SiloWireCodec {
    encode(value: unknown): unknown;
    decode(value: unknown): unknown;
    /** `JSON.parse` reviver that DROPS `__proto__`/`constructor`/`prototype`. */
    reviver(key: string, value: unknown): unknown;
}

/** A wire-shaped failure: what crosses the hop in place of an Error. */
export interface SiloWireError {
    message?: string;
    /**
     * HTTP-shaped numeric code. Not an HTTP detail even off HTTP:
     * `clusterStats` classifies peer failures by this number (403 →
     * `unauthorized`, 404 → `unsupported`), so a transport that drops it
     * degrades every auth failure to a bare `'error'` in the ops surface.
     */
    status?: number;
    /** `{ kind, owner? }` for actor errors — the re-branding contract. */
    data?: unknown;
}

/**
 * What a transport is BUILT with. Everything here is known before the silo
 * exists, which is what lets `cluster()` construct the transport eagerly
 * and read `routes` from it during plugin setup.
 */
export interface SiloTransportConfig {
    /** This silo's id — stamp it into every envelope as `from`. */
    readonly siloId: string;
    /** This silo's start epoch. Orders incarnations. */
    readonly epoch: number;
    /**
     * The shared cluster secret, or undefined for an unauthenticated
     * cluster. Sign per call (what HTTP does), or authenticate once per
     * connection — a long-lived link is exactly the chance to stop paying
     * an HMAC per request.
     */
    readonly secret?: string;
    /** Path prefix of the internal HTTP mount, for transports that ride it. */
    readonly internalBase: string;
    readonly codec: SiloWireCodec;
    /** Actor error → wire form, on the RECEIVING side. */
    readonly toWireError: (error: unknown) => SiloWireError;
    /**
     * Wire form → error, on the CALLING side. Re-brands actor kinds so a
     * caller cannot tell a remote hop from a local dispatch (`isActorError`
     * holds, `kind` survives, `owner` survives). Never re-implement it:
     * "the error is the same error" is a conformance requirement, and the
     * kind list is versioned here.
     */
    readonly fromWireError: (
        status: number,
        wire: SiloWireError | undefined,
        fallback: string
    ) => Error;
}

/** What an inbound wire symbol resolved to on this silo. */
export interface SiloCallTarget {
    readonly type: string;
    readonly method: string;
    /** True for `streams:` methods — dispatch through `dispatchStream`. */
    readonly stream: boolean;
}

/**
 * The LIVE silo behind this transport, handed over at `start()`.
 *
 * Separate from the config on purpose: none of it exists at construction
 * time — a transport built inside `cluster()`'s body has no silo to
 * dispatch into yet. Splitting the two removes a whole class of
 * called-too-early bug instead of documenting it away.
 */
export interface SiloTransportRuntime {
    /** This silo's membership descriptor as it stands right now. */
    descriptor(): SiloDescriptor;
    /** The current membership view. SYNC — `dispatcherFor` is on the hot path. */
    view(): MembershipView;
    /**
     * Resolve an inbound wire symbol (`Cart#addItem`) against the running
     * silo. `null` = no such type or method: answer this transport's
     * equivalent of `kind: 'method-not-found'`. Async because a lazy
     * (`virtual:sigx-actors`) registry loads a type's module on demand.
     *
     * Reserved silo-level symbols (`$sigx:silo#stats`) resolve here too and
     * dispatch through `dispatch()` like anything else — a transport never
     * special-cases them, and a future reserved symbol needs no transport
     * change.
     */
    resolve(symbol: string): SiloCallTarget | null | Promise<SiloCallTarget | null>;
    /**
     * Dispatch an inbound call LOCALLY — never forwarded onward
     * (redirect-not-proxy). A call for an actor this silo does not own
     * throws a wrong-host error carrying the owner hint; put it through
     * `toWireError` and the caller re-routes.
     */
    dispatch(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): Promise<unknown>;
    dispatchStream(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): AsyncIterable<unknown>;
    /**
     * Report a refused authentication. Counted, because a rejection here is
     * otherwise completely silent — and during a secret rotation it is the
     * only signal that half the cluster has not rotated yet.
     */
    noteAuthFailure(): void;
}

export interface SiloTransport {
    /**
     * Short, stable identifier — `'http'`, `'ws'`, `'tcp'`. It keys this
     * silo's entry in `SiloDescriptor.addresses`, so RENAMING IT IS A WIRE
     * BREAK: peers on the old name stop finding an address for the new one
     * and fall through the chain (or off the end of it).
     */
    readonly name: string;

    /**
     * Mounts this transport needs on the app's HTTP listener. `cluster()`
     * contributes them through `PluginRegistry.route()`, so the internal
     * silo-to-silo endpoint stops being special-cased: HTTP's receiving
     * half is simply the route it declares here. A transport with its own
     * socket declares none — and a cluster configured without an HTTP
     * transport then has no internal HTTP mount at all.
     *
     * Read at plugin-setup time, so it must be stable from construction.
     */
    readonly routes?: readonly ActorRoute[];

    /**
     * Open the listener. Runs BEFORE `membership.join()`, so no peer can
     * learn this silo's address before something answers on it.
     *
     * Returns the peer-reachable address to publish under
     * `descriptor.addresses[name]` — a transport binding an ephemeral port
     * learns its address here and nowhere earlier. Return nothing when the
     * transport has no address of its own: the placement then publishes
     * `advertise` for it, which is exactly right for HTTP and for a WS
     * transport upgrading the app's own listener.
     */
    start?(runtime: SiloTransportRuntime): string | void | Promise<string | void>;

    /**
     * A dispatcher whose calls cross the wire to `target`, or `null` when
     * this transport cannot reach that peer (it publishes no address under
     * `name`). `null` is a ROUTING answer, not a failure — the placement
     * moves to the next transport in the chain. Throw only when reaching
     * the peer failed for a reason the next transport would hit too.
     *
     * Hot path: called per dispatch. Cache per `target.siloId`.
     */
    dispatcherFor(target: SiloDescriptor): ActorDispatcher | null;

    /**
     * Membership changed. Fired once at start and on every later view, so a
     * connection-oriented transport can close links to silos that are gone.
     * Silo ids are minted per START and never reused, so a departed id
     * never returns and dropping its connection is unconditionally safe.
     */
    onMembership?(view: MembershipView): void;

    /**
     * Close the listener and every peer connection. Runs AFTER the drain,
     * at the end of `placement.stop()` — peers keep calling in throughout
     * `'leaving'`, which is the whole point of announcing it first.
     */
    stop?(): void | Promise<void>;
}

export type SiloTransportFactory = (config: SiloTransportConfig) => SiloTransport;
