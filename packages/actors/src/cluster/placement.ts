/**
 * `clusterPlacement` — the distributed `ActorPlacement`. Ownership lives in
 * the directory (claim-on-activate, release-on-deactivate through the
 * `PlacementBindings` seam); routing resolves local-first, then the route
 * cache, then the directory, then the placement policy. Misdirected calls
 * come back as wrong-host redirects (never proxied onward) and the loop
 * here re-resolves and retries, bounded.
 *
 * Correctness never rests on this file being right: the storage etag CAS
 * remains the integrity floor — a briefly-wrong route or a split-brain
 * window costs a rejected save and a fault-and-reload, not corruption.
 */
import {
    ActorActivationError,
    ActorUnreachableError,
    ActorWrongHostError,
    isActorError,
    type ActorOwnerHint
} from '../errors';
import {
    actorId,
    actorLabel,
    emptyHostStats,
    type ActorCallContext,
    type ActorDispatcher,
    type ActorLocation,
    type ActorPlacement,
    type ActorRef,
    type AnyActorDefinition,
    type PlacementBindings,
    type Host
} from '../types';
import { mintCallId } from '../call-id';
import { fnv1a, reminderShardKeys } from '../host/reminder-shards';
import {
    createCounters,
    type ClusterCounters,
    type ClusterCounterTotals
} from './counters';
import {
    HOST_STATS_METHOD,
    HOST_STATS_TYPE,
    type HostReport,
    type HostReportOptions
} from './stats';
import { httpTransport } from './transport';
import type { ActorRoute, HealthReport } from '../host/app';
import type { MetricsDigest } from '../host/digest';
import type {
    HostTransport,
    HostTransportConfig,
    HostTransportFactory,
    HostTransportRuntime
} from './seam';
import { resolveHostSymbol } from './host-endpoint';
import { fromHostWireError, hostWireCodec, toHostWireError } from './wire-errors';
import type {
    ClusterProviders,
    DirectoryEntry,
    MembershipView,
    PlacementPolicy,
    HostDescriptor,
    HostIdentity
} from './types';

export interface ClusterPlacementOptions extends ClusterProviders {
    /** Peer-reachable origin of this host's HTTP listener. */
    advertise: string;
    /**
     * Origin a CLIENT can reach this host's PUBLIC actor mount on, e.g.
     * `https://host-3.example.com`. Published in the membership descriptor
     * so peers can redirect callers here.
     *
     * Distinct from `advertise`, which is the INTERNAL origin: redirecting
     * an external client to a pod IP hangs at best and discloses internal
     * topology at worst. Leave it unset unless hosts are individually
     * reachable from outside — `onMiss: 'redirect'` then proxies instead,
     * which is correct rather than broken.
     */
    publicAddress?: string;
    /** Shared cluster secret for the internal mount. */
    secret?: string;
    /** Path prefix of the internal mount. Default `/_sigx/host`. */
    internalBase?: string;
    /**
     * How this host reaches its peers. Default `httpTransport()`.
     *
     * A LIST is a fallback chain, tried in order — the rolling-deploy
     * story: `[tcpTransport(), httpTransport()]` upgrades link by link as
     * peers gain a `tcp` address, with no window where half the cluster is
     * unreachable. A SINGLE transport is strict: a peer publishing no
     * address for it is unreachable, loudly. `httpTransport()` reaches
     * every peer, so it is only ever valid as the LAST entry.
     */
    transport?: HostTransportFactory | readonly HostTransportFactory[];
    /**
     * Fetch implementation (tests pipe it straight into peers' handlers).
     * Sugar for `transport: httpTransport({ fetch })`; passing both throws.
     */
    fetch?: typeof globalThis.fetch;
    /** Placement policy for NEW activations. Default: uniform random. */
    policy?: PlacementPolicy;
    /** Per-actor-type policy overrides (e.g. pin hot session types local). */
    typePolicies?: Record<string, PlacementPolicy>;
    /** Wrong-host / unreachable re-resolve attempts. Default 3. */
    retries?: number;
    /**
     * Backoff between UNREACHABLE retries, ms (linear: n × this) — rides
     * out transient network blips without burning every attempt at once.
     * Wrong-host redirects retry immediately. Default 100.
     */
    retryBackoffMs?: number;
    /** Free-form placement hints published in the membership descriptor. */
    meta?: Record<string, string>;
    /**
     * This host's readiness, for `HostReport.health`.
     *
     * Wired by `cluster()` from `registry.health()`. A hand-rolled
     * placement may leave it out; the field is then simply absent, which is
     * what an older peer looks like too.
     */
    health?: () => HealthReport | undefined;
    /**
     * This host's mergeable metrics, for `HostReport.metrics`.
     *
     * Wired by `cluster()` from `registry.digest('metrics')` — a seam that
     * only walks digest providers, so this cannot re-enter the ops section
     * that publishes `report()` itself.
     */
    metrics?: (options?: unknown) => MetricsDigest | undefined;
}

/**
 * Ceilings the RESPONDER enforces on what a peer asked for.
 *
 * A stats request arrives over the internal wire. HMAC proves who is
 * asking; it says nothing about whether `activations: 1e9` is a reasonable
 * thing to ask a host with millions of actors to walk and serialise. So the
 * caps live here, on the answering side, and are not negotiable.
 */
const MAX_REPORT_ACTIVATIONS = 200;
const MAX_DIGEST_TYPES = 64;
const MAX_DIGEST_METHODS = 256;
const MAX_DIGEST_ERRORS = 32;
/** What a digest carries when the caller expressed no preference. */
const DIGEST_TYPES = 32;
const DIGEST_METHODS = 32;

/** A requested limit, or the default — never above the ceiling. */
function clampRequest(value: unknown, fallback: number, ceiling: number): number {
    if (typeof value !== 'number' || !Number.isFinite(value)) return fallback;
    return Math.max(0, Math.min(ceiling, Math.floor(value)));
}

export interface ClusterPlacement extends ActorPlacement {
    readonly identity: HostIdentity;
    /** This host's current membership descriptor. */
    descriptor(): HostDescriptor;
    /** Whether THIS host owns a reminder shard under the current view. */
    ownsReminderShard(shard: string): boolean;
    /**
     * Explicit rebalance primitive: gracefully deactivate ONE locally-owned
     * activation with reason `'migrated'` and release its claim — the next
     * call re-places it under the current policy. No-op if the actor is
     * not active on this host. Automatic load-driven rebalancing composes
     * on top of this.
     */
    migrate(ref: ActorRef): Promise<void>;
    /**
     * Where this actor lives, resolved WITHOUT dispatching and WITHOUT
     * activating — the public endpoint's `onMiss: 'redirect'` asks this
     * before it decides whether to answer 421 or proxy.
     *
     * Uses the same resolution order as dispatch (claim → route cache →
     * directory → policy), so a redirect and a proxy would go to the same
     * host. Note it can PLACE a not-yet-placed actor in the route cache,
     * exactly as a dispatch would — placement is sticky so concurrent
     * callers agree — but it never activates anything.
     */
    locate(ref: ActorRef): Promise<ActorLocation>;
    /**
     * Inbound side, consumed by `handleHostRequest`: dispatch LOCALLY or
     * throw wrong-host — never forward (redirect-not-proxy).
     */
    dispatchInbound(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): Promise<unknown>;
    dispatchInboundStream(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): AsyncIterable<unknown>;
    dispatchInboundWatch(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext,
        options?: { throttleMs?: number }
    ): AsyncIterable<unknown>;
    /** The membership view this host currently holds. */
    view(): MembershipView;
    /** Pull-based routing/directory counters for THIS host. Fresh object. */
    counters(): ClusterCounters;
    /**
     * This host's operational snapshot — the local half of `clusterStats()`,
     * and exactly what a peer's `$sigx:host#stats` answers. Safe before
     * `start()`: reports `'joining'` and zeroes.
     */
    report(options?: HostReportOptions): HostReport;
    /**
     * The HTTP mounts the configured transports need. `cluster()`
     * contributes these through `PluginRegistry.route()`; a hand-rolled
     * mount routes them itself. Stable from construction, so it is safe to
     * read before `start()` — and EMPTY for a cluster configured with only
     * socket transports, which then has no internal HTTP surface at all.
     */
    routes(): readonly ActorRoute[];
    /**
     * Fetch a peer's report over the internal transport.
     *
     * @internal — the fan-out helper behind `clusterStats()`. It goes
     * straight to the transport, never through the routing loop, so reading
     * the counters cannot move them.
     */
    peerReport(
        target: HostDescriptor,
        timeoutMs: number,
        signal?: AbortSignal,
        /**
         * What to ask the peer to include. Appended rather than folded into
         * an options object on purpose: `ClusterPlacement` is exported, so
         * reshaping these parameters would break a hand-rolled placement
         * for no gain.
         */
        request?: HostReportOptions
    ): Promise<HostReport>;
    /**
     * @internal — the internal mount reports HMAC rejections here. Optional
     * on the interface so a hand-rolled placement passed to
     * `handleHostRequest` need not implement it.
     */
    noteAuthFailure?(): void;
}

/**
 * Highest-random-weight (rendezvous) selection: every host independently
 * picks the SAME member for a key, with a lexical tie-break so equal hashes
 * cannot split the answer. Shared by new-activation placement and reminder
 * shard ownership — two uses of one rule, not two rules that agree.
 */
function rendezvous(key: string, hosts: readonly HostDescriptor[]): HostDescriptor | null {
    let best: HostDescriptor | null = null;
    let bestScore = -1;
    for (const host of hosts) {
        const score = fnv1a(`${key}|${host.hostId}`);
        if (score > bestScore || (score === bestScore && best !== null && host.hostId < best.hostId)) {
            bestScore = score;
            best = host;
        }
    }
    return best;
}

const ROUTE_CACHE_MAX = 10_000;

/** Shared, frozen — `locate()` answers this on every local hit, and the hot
 *  path should not allocate to say "it's here". */
const LOCAL: ActorLocation = Object.freeze({ local: true as const });

/**
 * The `backend` tag this placement answers to. A strategy carrying a
 * DIFFERENT tag belongs to another backend and is ignored silently; one
 * carrying this tag, or none at all, must be usable here or it is an error.
 */
export const CLUSTER_BACKEND = 'cluster';

function randBase36(length: number): string {
    const cryptoObj = (globalThis as { crypto?: Crypto }).crypto;
    if (cryptoObj?.getRandomValues) {
        const bytes = new Uint8Array(length);
        cryptoObj.getRandomValues(bytes);
        return Array.from(bytes, (b) => (b % 36).toString(36)).join('');
    }
    let out = '';
    while (out.length < length) out += Math.random().toString(36).slice(2);
    return out.slice(0, length);
}

/** Uniform random over active hosts — the default posture. */
const randomPolicy: PlacementPolicy = {
    name: 'random',
    backend: CLUSTER_BACKEND,
    choose(_ref, view, self) {
        const active = view.hosts.filter((s) => s.status === 'active');
        if (active.length === 0) return self;
        return active[Math.floor(Math.random() * active.length)]!;
    }
};

/** Uniform random over active hosts (the default). Exported for
 *  `typePolicies` maps that override only some types. */
export function randomPlacementPolicy(): PlacementPolicy {
    return randomPolicy;
}

/**
 * Rendezvous (highest-random-weight) placement: every host deterministically
 * picks the SAME target for a new key, so racing activations agree without
 * a directory round-trip and misses stay rare. The directory remains the
 * arbiter; membership changes only re-home keys that hashed to the departed
 * host.
 */
export function consistentHashPolicy(): PlacementPolicy {
    return {
        name: 'consistent-hash',
        backend: CLUSTER_BACKEND,
        choose(ref, view, self) {
            const active = view.hosts.filter((s) => s.status === 'active');
            if (active.length === 0) return self;
            return rendezvous(actorId(ref), active) ?? self;
        }
    };
}

/** Pin new activations to the calling host — sticky-LB-friendly for hot
 *  session-shaped types. */
export function preferLocalPolicy(): PlacementPolicy {
    return {
        name: 'prefer-local',
        backend: CLUSTER_BACKEND,
        choose: (_ref, _view, self) => self
    };
}

export function clusterPlacement(options: ClusterPlacementOptions): ClusterPlacement {
    return new ClusterPlacementImpl(options);
}

class ClusterPlacementImpl implements ClusterPlacement {
    readonly identity: HostIdentity;
    #options: ClusterPlacementOptions;
    #policy: PlacementPolicy;
    #retries: number;
    #retryBackoffMs: number;
    /** Host ids seen in a membership view — the departure diff base. */
    #seenHosts = new Set<string>();
    /** The configured chain, in order. First non-null dispatcher wins. */
    #transports: readonly HostTransport[];
    /** hostId → the chain entry that reaches it. Dropped on a view change. */
    #transportFor = new Map<string, HostTransport>();
    /** Peers already dev-warned about a fallback — warn once, not per call. */
    #warnedFallback = new Set<string>();
    /**
     * Per-transport advertised addresses, filled at `start()` BEFORE the
     * membership join. Undefined until then, so a descriptor read before
     * start looks exactly like a pre-`addresses` build — which is the same
     * thing it means.
     */
    #addresses: Record<string, string> | undefined;
    #local: ActorDispatcher | null = null;
    #host: Host | null = null;
    /** type → its `defineActor({ placement })` policy, or null if none. */
    #declaredPolicies = new Map<string, PlacementPolicy | null>();
    /** Our live directory claims: actorId → the entry we wrote. */
    #claimed = new Map<string, DirectoryEntry>();
    /** actorId → hostId hint. Insertion-ordered Map as a cheap LRU. */
    #routeCache = new Map<string, string>();
    #seq = 0;
    #fenced = false;
    #status: HostDescriptor['status'] = 'joining';
    #unsubscribe: (() => void)[] = [];
    /** Mutable counters — never handed out; `counters()` copies. */
    #counters: ClusterCounterTotals = createCounters();
    /** `performance.now()` at `start()`; 0 before. */
    #startedAt = 0;

    constructor(options: ClusterPlacementOptions) {
        this.identity = { hostId: `s.${randBase36(8)}`, epoch: Date.now() };
        this.#options = options;
        this.#policy = options.policy ?? randomPolicy;
        this.#retries = options.retries ?? 3;
        this.#retryBackoffMs = options.retryBackoffMs ?? 100;
        if (options.transport && options.fetch) {
            // Both would silently disagree: `fetch` only reaches the HTTP
            // transport, so a chain that does not end in one would ignore it.
            throw new Error(
                `[sigx actors] cluster({ transport }) and cluster({ fetch }) are mutually ` +
                    `exclusive — pass the fetch to the transport instead: ` +
                    `transport: httpTransport({ fetch }).`
            );
        }
        const config: HostTransportConfig = {
            hostId: this.identity.hostId,
            epoch: this.identity.epoch,
            internalBase: options.internalBase ?? '/_sigx/host',
            codec: hostWireCodec,
            toWireError: toHostWireError,
            fromWireError: fromHostWireError,
            ...(options.secret !== undefined ? { secret: options.secret } : {})
        };
        const factories = options.transport
            ? Array.isArray(options.transport)
                ? options.transport
                : [options.transport as HostTransportFactory]
            : [httpTransport(options.fetch ? { fetch: options.fetch } : {})];
        if (factories.length === 0) {
            throw new Error(
                `[sigx actors] cluster({ transport: [] }) has no transports — this host ` +
                    `could not reach any peer.`
            );
        }
        this.#transports = factories.map((factory) => factory(config));
    }

    descriptor(): HostDescriptor {
        return {
            ...this.identity,
            address: this.#options.advertise,
            status: this.#status,
            ...(this.#addresses ? { addresses: this.#addresses } : {}),
            ...(this.#options.publicAddress !== undefined
                ? { publicAddress: this.#options.publicAddress }
                : {}),
            ...(this.#options.meta ? { meta: this.#options.meta } : {})
        };
    }

    /**
     * The HTTP mounts the configured transports need — `cluster()`
     * contributes these through `PluginRegistry.route()`; a hand-rolled
     * mount routes them itself. Stable from construction, so it is safe to
     * read before `start()`.
     *
     * A cluster configured with only socket transports contributes NOTHING
     * here, and therefore has no internal HTTP surface at all.
     */
    routes(): readonly ActorRoute[] {
        return this.#transports.flatMap((t) => t.routes ?? []);
    }

    // -----------------------------------------------------------------------
    // ActorPlacement

    bind(local: ActorDispatcher, host: Host): PlacementBindings {
        this.#local = local;
        this.#host = host;
        return {
            beforeActivate: async (ref) => {
                if (this.#fenced) {
                    throw new ActorUnreachableError(
                        `${this.identity.hostId} (self, fenced — membership lost)`
                    );
                }
                const id = actorId(ref);
                const mine: DirectoryEntry = {
                    hostId: this.identity.hostId,
                    activationId: `${this.identity.hostId}/${this.identity.epoch}/${++this.#seq}`
                };
                this.#counters.directoryClaims++;
                let winner = await this.#options.directory.claim(id, mine);
                if (
                    winner.activationId !== mine.activationId &&
                    winner.hostId === this.identity.hostId &&
                    !this.#claimed.has(id)
                ) {
                    // A stale entry naming US without a live claim behind it
                    // (leftover from a lost release) — reclaim, don't bounce
                    // callers off our own ghost.
                    this.#counters.directoryEvictions++;
                    await this.#options.directory.evict(id, winner);
                    this.#counters.directoryClaims++;
                    winner = await this.#options.directory.claim(id, mine);
                }
                if (winner.activationId !== mine.activationId) {
                    this.#counters.claimConflicts++;
                    throw new ActorWrongHostError(
                        actorLabel(ref),
                        this.#ownerHint(winner.hostId)
                    );
                }
                this.#claimed.set(id, mine);
            },
            afterDeactivate: async (ref) => {
                const id = actorId(ref);
                const entry = this.#claimed.get(id);
                if (!entry) return;
                this.#claimed.delete(id);
                this.#counters.directoryReleases++;
                await this.#options.directory.release(id, entry);
            },
            strictChainPresence: true,
            ownsReminderShard: (shard) => this.ownsReminderShard(shard),
            stopReason: 'migrated'
        };
    }

    async migrate(ref: ActorRef): Promise<void> {
        if (!this.#claimed.has(actorId(ref))) return;
        await this.#host?.deactivate(ref, 'migrated');
    }

    /**
     * Rendezvous hashing over the ACTIVE membership view: for each shard,
     * the host with the highest hash(shard, hostId) owns it. Deterministic
     * per view — no stored assignment, no lease. Transient view divergence
     * (two hosts both claiming a shard) is safe: the reminder shard's etag
     * CAS keeps firing at-most-once regardless.
     */
    ownsReminderShard(shard: string): boolean {
        const active = this.#options.membership
            .view()
            .hosts.filter((s) => s.status === 'active');
        if (active.length === 0) return true; // solo / not started
        return rendezvous(shard, active)?.hostId === this.identity.hostId;
    }

    async start(): Promise<void> {
        this.#status = 'active';
        // Transports listen FIRST: a peer must never learn an address before
        // something answers on it, and a transport binding an ephemeral port
        // only learns its own address here. This is why the transport is an
        // option on `cluster()` rather than a plugin — no registry hook runs
        // before the join below.
        const runtime = this.#runtime();
        const addresses: Record<string, string> = {};
        for (const transport of this.#transports) {
            const address = await transport.start?.(runtime);
            if (typeof address === 'string') addresses[transport.name] = address;
        }
        if (Object.keys(addresses).length > 0) this.#addresses = addresses;

        await this.#options.membership.join(this.descriptor());
        this.#startedAt = performance.now();
        this.#seenHosts = new Set(
            this.#options.membership.view().hosts.map((s) => s.hostId)
        );
        this.#notifyTransports(this.#options.membership.view());
        this.#unsubscribe.push(
            this.#options.membership.onChange((view) => {
                this.#counters.membershipChanges++;
                this.#pruneRoutes(view);
                this.#notifyTransports(view);
                void this.#sweepDeparted(view);
            }),
            this.#options.membership.onSelfSuspect(() => void this.#fence())
        );
    }

    /**
     * The live-host half of the seam, handed to transports at `start()`.
     * Built here so a transport in another package never touches the
     * placement's internals — the dependency arrow points one way.
     */
    #runtime(): HostTransportRuntime {
        return {
            descriptor: () => this.descriptor(),
            view: () => this.view(),
            resolve: (symbol) => resolveHostSymbol(this.#host!, symbol),
            dispatch: (ref, method, args, call) => {
                // The ops channel answers before any activation lookup, and
                // deliberately does NOT count as an inbound dispatch —
                // reading the counters must not move them.
                if (ref.type === HOST_STATS_TYPE) {
                    return Promise.resolve(this.report(args[0] as HostReportOptions | undefined));
                }
                return this.dispatchInbound(ref, method, args, call);
            },
            dispatchStream: (ref, method, args, call) =>
                this.dispatchInboundStream(ref, method, args, call),
            dispatchWatch: (ref, method, args, call, options) =>
                this.dispatchInboundWatch(ref, method, args, call, options),
            noteAuthFailure: () => this.noteAuthFailure()
        };
    }

    /**
     * Host ids are minted per START and never reused, so a departed id never
     * returns and a connection-oriented transport can drop its link
     * unconditionally.
     */
    #notifyTransports(view: MembershipView): void {
        // A descriptor can in principle gain an address without a new
        // hostId (a status rewrite republishes it), so the resolved chain
        // entry is not durable across views.
        this.#transportFor.clear();
        for (const transport of this.#transports) transport.onMembership?.(view);
    }

    async beginStop(): Promise<void> {
        // Runs BEFORE the drain: peers must stop placing new actors here
        // while activations hand off (their policies filter on 'active').
        this.#status = 'leaving';
        try {
            await this.#options.membership.setStatus('leaving');
        } catch {
            // Leaving is best-effort; the heartbeat TTL is the backstop.
        }
    }

    async stop(): Promise<void> {
        // host.stop() has already drained activations (releasing claims,
        // reason 'migrated') between beginStop() and here.
        for (const unsub of this.#unsubscribe) unsub();
        this.#unsubscribe = [];
        this.#status = 'leaving';
        await this.#options.membership.leave();
        // Transports close LAST: peers keep calling in throughout the drain,
        // which is the whole point of announcing `'leaving'` first.
        for (const transport of this.#transports) {
            try {
                await transport.stop?.();
            } catch {
                // One transport failing to close must not strand the others.
            }
        }
        this.#transportFor.clear();
    }

    dispatcherFor(ref: ActorRef): ActorDispatcher {
        // Local fast path: we hold the claim, no store reads, no routing.
        if (this.#claimed.has(actorId(ref))) return this.#local!;
        return this.#routing;
    }

    async locate(ref: ActorRef): Promise<ActorLocation> {
        this.#counters.locates++;
        // Same fast path as dispatcherFor: holding the claim answers with
        // no store read at all.
        if (this.#claimed.has(actorId(ref))) return LOCAL;
        const target = await this.#resolveTarget(ref);
        if (target === 'local') return LOCAL;
        this.#counters.locateRemote++;
        return { local: false, owner: this.#ownerHint(target.hostId) };
    }

    // -----------------------------------------------------------------------
    // Inbound (redirect-not-proxy: local or wrong-host, never forward)

    dispatchInbound(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): Promise<unknown> {
        // Counted separately from `remoteDispatches` and NEVER summed with
        // it: this is the same logical call the caller already counted on
        // its own side. Note that `#local` is the RAW local dispatcher, so
        // an inbound call does not pass through `useDispatch` middleware —
        // `metrics()` counts a cross-host call once, on the caller.
        this.#counters.inboundDispatches++;
        return this.#local!.dispatch(ref, method, args, call);
    }

    dispatchInboundStream(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): AsyncIterable<unknown> {
        this.#counters.inboundStreams++;
        return this.#local!.dispatchStream!(ref, method, args, call);
    }

    dispatchInboundWatch(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext,
        options?: { throttleMs?: number }
    ): AsyncIterable<unknown> {
        this.#counters.inboundWatches++;
        return this.#local!.dispatchWatch!(ref, method, args, call, options);
    }

    // -----------------------------------------------------------------------
    // Ops

    view(): MembershipView {
        return this.#options.membership.view();
    }

    /** @internal — the internal mount reports HMAC rejections here. */
    noteAuthFailure(): void {
        this.#counters.authFailures++;
    }

    counters(): ClusterCounters {
        return {
            ...this.#counters,
            // Gauges are read here rather than tracked, so they cannot drift.
            claimed: this.#claimed.size,
            routeCacheSize: this.#routeCache.size,
            membershipVersion: this.#options.membership.view().version,
            status: this.#fenced ? 'fenced' : this.#status
        };
    }

    report(options?: HostReportOptions): HostReport {
        const shards = reminderShardKeys().filter((shard) => this.ownsReminderShard(shard));
        // Clamped HERE, at the responder, not at the caller. The request
        // arrives over a wire; `activations: 1e9` on a host with millions
        // of actors is a remote CPU-and-memory amplifier, and HMAC proves
        // who is asking, not that what they asked for is sane.
        const actors = clampRequest(options?.activations, 0, MAX_REPORT_ACTIVATIONS);
        const digest = options?.metrics
            ? this.#options.metrics?.({
                  types: clampRequest(options.types, DIGEST_TYPES, MAX_DIGEST_TYPES),
                  methods: clampRequest(options.methods, DIGEST_METHODS, MAX_DIGEST_METHODS),
                  errors: clampRequest(options.errors, 0, MAX_DIGEST_ERRORS)
              })
            : undefined;
        const health = this.#options.health?.();
        return {
            v: 1,
            hostId: this.identity.hostId,
            epoch: this.identity.epoch,
            address: this.#options.advertise,
            status: this.#fenced ? 'fenced' : this.#status,
            stats: this.#host?.stats() ?? emptyHostStats(),
            counters: this.counters(),
            reminderShards: shards,
            uptimeMs: this.#startedAt === 0 ? 0 : Math.round(performance.now() - this.#startedAt),
            transports: this.#transports.map((t) => t.name),
            ...(this.#options.meta ? { meta: this.#options.meta } : {}),
            ...(digest ? { metrics: digest } : {}),
            ...(health ? { health } : {}),
            ...(actors > 0 && this.#host
                ? { activations: [...this.#host.activations({ limit: actors, sortBy: 'queued' })] }
                : {})
        };
    }

    /** @internal — straight to the transport, never through the routing loop,
     *  so reading the counters cannot move them. */
    async peerReport(
        target: HostDescriptor,
        timeoutMs: number,
        signal?: AbortSignal,
        request?: HostReportOptions
    ): Promise<HostReport> {
        const timeout = AbortSignal.timeout(timeoutMs);
        const call: ActorCallContext = {
            callChain: [],
            callId: mintCallId(),
            // Bounded from both ends: the abort stops a hung socket here,
            // the deadline lets the peer give up on its own clock.
            deadline: Date.now() + timeoutMs,
            abortSignal: signal ? AbortSignal.any([timeout, signal]) : timeout
        };
        const report = (await this.#transportDispatcher(target)
            .dispatch(
                { type: HOST_STATS_TYPE, key: HOST_STATS_METHOD },
                HOST_STATS_METHOD,
                // A peer that predates this argument ignores it and answers
                // the payload it always did — still `v: 1`, just without the
                // new optional fields. That is why the version does not bump.
                request ? [request] : [],
                call
            )) as HostReport | undefined;
        if (!report || report.v !== 1) {
            // A rolling deploy IS a mixed-version cluster, and the ops tool
            // has to keep working during the deploy that broke things.
            throw Object.assign(
                new Error(
                    `[sigx actors] ${target.hostId} answered an unsupported stats payload ` +
                        `(v${String(report?.v)})`
                ),
                { status: 404 }
            );
        }
        return report;
    }

    // -----------------------------------------------------------------------
    // Routing

    /**
     * Walk the transport chain for a peer: the first that can reach it wins.
     * `null` from `dispatcherFor` is a ROUTING answer ("I publish no address
     * for that host"), not a failure — it is what lets a mixed-transport
     * cluster exist at all, and therefore what makes a rolling deploy of a
     * new transport possible.
     */
    #transportDispatcher(target: HostDescriptor): ActorDispatcher {
        const cached = this.#transportFor.get(target.hostId);
        if (cached) {
            const hit = cached.dispatcherFor(target);
            if (hit) return hit;
            this.#transportFor.delete(target.hostId);
        }
        for (let i = 0; i < this.#transports.length; i++) {
            const transport = this.#transports[i]!;
            const dispatcher = transport.dispatcherFor(target);
            if (!dispatcher) continue;
            this.#transportFor.set(target.hostId, transport);
            if (i > 0) {
                this.#counters.transportFallbacks++;
                if (__DEV__ && !this.#warnedFallback.has(target.hostId)) {
                    this.#warnedFallback.add(target.hostId);
                    console.warn(
                        `[sigx actors] ${target.hostId} is not reachable over ` +
                            `"${this.#transports[0]!.name}" — falling back to ` +
                            `"${transport.name}". Expected mid-rollout; permanent means ` +
                            `that host never advertised the preferred transport.`
                    );
                }
            }
            return dispatcher;
        }
        // Strict by design: no silent HTTP fallback, because a silent one
        // means you deploy a transport, benchmark it, and measure the old
        // one without ever knowing.
        throw new ActorUnreachableError(
            `${target.hostId} — no configured transport reaches it (tried ` +
                `${this.#transports.map((t) => t.name).join(', ')}; it advertises ` +
                `${Object.keys(target.addresses ?? {}).join(', ') || 'none'})`
        );
    }

    /** One shared dispatcher that resolves + retries per call. */
    #routing: ActorDispatcher = {
        dispatch: (ref, method, args, call) => this.#routedDispatch(ref, method, args, call),
        dispatchStream: (ref, method, args, call) =>
            this.#routedStream(ref, method, args, call),
        dispatchWatch: (ref, method, args, call, options) =>
            this.#routedStreamed(ref, method, args, call, 'watch', options)
    };

    /**
     * The owner hint for a peer — the ONE place both the wrong-host throw
     * and `locate()` build it, so the internal and public answers cannot
     * drift into disagreeing about who owns an actor.
     *
     * Carries both origins; it is the consumer's job to pick. The internal
     * mount uses `address`, the public endpoint uses `publicAddress` and
     * must never leak `address` to a client.
     */
    #ownerHint(hostId: string): ActorOwnerHint {
        const member = this.#member(hostId);
        return {
            hostId,
            ...(member?.address !== undefined ? { address: member.address } : {}),
            ...(member?.publicAddress !== undefined
                ? { publicAddress: member.publicAddress }
                : {})
        };
    }

    #member(hostId: string): HostDescriptor | undefined {
        return this.#options.membership.view().hosts.find((s) => s.hostId === hostId);
    }

    #cacheRoute(id: string, hostId: string): void {
        if (this.#routeCache.size >= ROUTE_CACHE_MAX) {
            const oldest = this.#routeCache.keys().next().value;
            if (oldest !== undefined) this.#routeCache.delete(oldest);
        }
        this.#routeCache.delete(id);
        this.#routeCache.set(id, hostId);
    }

    #pruneRoutes(view: MembershipView): void {
        const live = new Set(view.hosts.map((s) => s.hostId));
        for (const [id, hostId] of this.#routeCache) {
            if (!live.has(hostId)) this.#routeCache.delete(id);
        }
    }

    /**
     * Proactive directory hygiene: when a host we have seen disappears
     * from the view AND the store confirms it dead (a graceful leaver
     * already released its claims), sweep its entries so callers never
     * trip over them. Racing survivors are fine — eviction is idempotent.
     * Lazy eviction on lookup remains the backstop.
     */
    async #sweepDeparted(view: MembershipView): Promise<void> {
        const live = new Set(view.hosts.map((s) => s.hostId));
        const departed = [...this.#seenHosts].filter(
            (id) => !live.has(id) && id !== this.identity.hostId
        );
        for (const id of live) this.#seenHosts.add(id);
        if (departed.length === 0 || !this.#options.directory.evictHost) return;
        for (const id of departed) {
            // A host only stops being "seen" once its sweep completed (or
            // the store says it is in fact alive-and-absent-from-view for
            // now) — a transient view drop or a failed sweep keeps it on
            // the list, so the next membership change tries again.
            try {
                if (await this.#options.membership.isAlive(id)) continue;
                this.#counters.hostSweeps++;
                this.#counters.sweptEntries += await this.#options.directory.evictHost(id);
                this.#seenHosts.delete(id);
            } catch (error) {
                if (__DEV__) {
                    console.error(`[sigx actors] directory sweep for ${id} failed:`, error);
                }
            }
        }
    }

    /** Self-fence: membership lost — stop claiming, drop what we hold. */
    async #fence(): Promise<void> {
        if (this.#fenced) return;
        this.#fenced = true;
        this.#counters.selfFences++;
        const types = new Set<string>();
        for (const id of this.#claimed.keys()) {
            const nul = id.indexOf('\u0000');
            if (nul > 0) types.add(id.slice(0, nul));
        }
        for (const type of types) {
            await this.#host?.deactivateType(type).catch(() => {});
        }
    }

    /** Resolve who should serve `ref` right now: us, or a peer. */
    async #resolveTarget(ref: ActorRef): Promise<'local' | HostDescriptor> {
        const id = actorId(ref);
        if (this.#claimed.has(id)) return 'local';

        const cached = this.#routeCache.get(id);
        if (cached !== undefined) {
            if (cached === this.identity.hostId) {
                this.#counters.routeCacheHits++;
                return 'local';
            }
            const member = this.#member(cached);
            if (member) {
                this.#counters.routeCacheHits++;
                return member;
            }
            this.#routeCache.delete(id);
        }
        this.#counters.routeCacheMisses++;

        this.#counters.directoryLookups++;
        const entry = await this.#options.directory.lookup(id);
        if (entry) {
            if (entry.hostId === this.identity.hostId) return 'local';
            const member = this.#member(entry.hostId);
            if (member) {
                this.#cacheRoute(id, entry.hostId);
                return member;
            }
            if (!(await this.#options.membership.isAlive(entry.hostId))) {
                // Dead owner: reclaim lazily and place fresh below.
                this.#counters.directoryEvictions++;
                await this.#options.directory.evict(id, entry);
            }
        }

        const view = this.#options.membership.view();
        if (view.hosts.length === 0) return 'local'; // not started / solo
        const policy =
            (await this.#declaredPolicy(ref.type)) ??
            this.#options.typePolicies?.[ref.type] ??
            this.#policy;
        const chosen = policy.choose(ref, view, this.descriptor());
        // Sticky: concurrent activations of one key must agree on a target
        // so racing dispatches join one claim instead of splitting.
        this.#cacheRoute(id, chosen.hostId);
        return chosen.hostId === this.identity.hostId ? 'local' : chosen;
    }

    /**
     * The strategy an actor declared with `defineActor({ placement })` —
     * the per-actor placement attribute, and the highest-precedence answer.
     *
     * Resolved lazily and memoized per TYPE: a `virtual:sigx-actors`
     * registry only loads a type's module on demand, so this cannot be
     * gathered up front. Types with no declaration memoize `null`, making
     * this one map lookup per dispatch after the first.
     */
    async #declaredPolicy(type: string): Promise<PlacementPolicy | undefined> {
        const memo = this.#declaredPolicies.get(type);
        if (memo !== undefined) return memo ?? undefined;

        const host = this.#host;
        if (!host) return undefined;
        let def: AnyActorDefinition | null;
        try {
            def = (await host.definition(type)) ?? null;
        } catch {
            // A failed module load is the dispatch path's problem to report,
            // not the placement's — fall through to the configured policy.
            return undefined;
        }
        const declared = def?.__sigxActor.placement;
        // Three cases, not one. Before the `backend` tag existed the runtime
        // could not tell them apart, so EVERY unusable declaration was
        // ignored with a dev-only warning — and a typo'd strategy silently
        // placed actors somewhere other than where its author said, in
        // production, with nothing pointing at the cause.
        if (declared) {
            // TAGGED FOR SOMEONE ELSE — checked before shape, and on the tag
            // alone. A foreign strategy that happens to expose a `choose()`
            // must still not be used here: it was written against a different
            // backend's view of the world, and running it would be worse than
            // ignoring it. This is the case the opacity of
            // `ActorPlacementStrategy` exists to allow, so it is silent.
            if (declared.backend !== undefined && declared.backend !== CLUSTER_BACKEND) {
                this.#declaredPolicies.set(type, null);
                return undefined;
            }
            // OURS, OR UNTAGGED AND UNRECOGNISED. Either way it cannot do the
            // job it was declared for, and failing loudly beats placing the
            // actor somewhere the author did not ask for.
            if (typeof (declared as PlacementPolicy).choose !== 'function') {
                throw new Error(
                    `[sigx actors] actor "${type}" declares placement ` +
                        `"${declared.name ?? 'unnamed'}", which is not a usable cluster ` +
                        `PlacementPolicy — it has no choose(). A strategy for a different ` +
                        `backend must set \`backend\` so it can be told apart from a broken one.`
                );
            }
        }
        const policy = (declared as PlacementPolicy | undefined) ?? null;
        this.#declaredPolicies.set(type, policy);
        return policy ?? undefined;
    }

    /**
     * Consume a routing failure. Returns how to retry: wrong-host retries
     * immediately (the redirect told us where); unreachable and a REMOTE
     * peer's shutdown retry after a backoff (a blip, or a rolling deploy
     * releasing its claims as it drains); null = not ours, rethrow. A
     * LOCAL shutdown is never retried — this host really is stopping.
     */
    async #noteFailure(
        id: string,
        error: unknown,
        remote: boolean
    ): Promise<'wrong-host' | 'unreachable' | 'draining' | null> {
        if (isActorError(error) && error.kind === 'wrong-host') {
            this.#counters.wrongHostRedirects++;
            this.#routeCache.delete(id);
            const owner = (error as ActorWrongHostError).owner;
            if (owner?.hostId && owner.hostId !== this.identity.hostId) {
                this.#cacheRoute(id, owner.hostId);
            }
            return 'wrong-host';
        }
        if (isActorError(error) && error.kind === 'unreachable') {
            this.#counters.unreachableRetries++;
            this.#routeCache.delete(id);
            this.#counters.directoryLookups++;
            const entry = await this.#options.directory.lookup(id);
            if (entry && !(await this.#options.membership.isAlive(entry.hostId))) {
                this.#counters.directoryEvictions++;
                await this.#options.directory.evict(id, entry);
            }
            await this.#options.membership.refresh();
            return 'unreachable';
        }
        if (remote && isActorError(error) && error.kind === 'host-shutdown') {
            this.#counters.drainingRetries++;
            // The owner is handing off: its claim releases as the actor
            // drains — don't evict, just re-resolve after a backoff (the
            // refreshed view excludes the leaver from placement).
            this.#routeCache.delete(id);
            await this.#options.membership.refresh();
            return 'draining';
        }
        return null;
    }

    #backoff(attempt: number): Promise<void> {
        const ms = this.#retryBackoffMs * (attempt + 1);
        if (ms <= 0) return Promise.resolve();
        return new Promise((r) => setTimeout(r, ms));
    }

    async #routedDispatch(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): Promise<unknown> {
        const id = actorId(ref);
        let lastError: unknown;
        for (let attempt = 0; attempt <= this.#retries; attempt++) {
            if (attempt > 0) this.#counters.retries++;
            const target = await this.#resolveTarget(ref);
            try {
                if (target === 'local') {
                    this.#counters.routedLocal++;
                    return await this.#local!.dispatch(ref, method, args, call);
                }
                this.#counters.remoteDispatches++;
                return await this.#transportDispatcher(target).dispatch(ref, method, args, call);
            } catch (error) {
                const failure = await this.#noteFailure(id, error, target !== 'local');
                if (!failure) throw error;
                if (failure !== 'wrong-host' && attempt < this.#retries) {
                    await this.#backoff(attempt);
                }
                lastError = error;
            }
        }
        this.#counters.routingFailures++;
        throw new ActorActivationError(actorLabel(ref), {
            cause:
                lastError ??
                new Error(`placement did not converge after ${this.#retries + 1} attempts`)
        });
    }

    #routedStream(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): AsyncIterable<unknown> {
        return this.#routedStreamed(ref, method, args, call, 'stream');
    }

    /**
     * Route a call that answers with a chunk stream — a `streams:` method or
     * a watch.
     *
     * One body for both because the hard part is identical and worth having
     * once: resolve, pull the FIRST value inside the retry loop so a
     * `wrong-host` or `unreachable` surfaces as a re-route rather than as a
     * failure halfway through, and hand the iterator back its `return()` on
     * an early exit. If anything, retry matters more for a watch — a watch
     * outlives the rebalance a stream would have finished before.
     */
    #routedStreamed(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext,
        mode: 'stream' | 'watch',
        options?: { throttleMs?: number }
    ): AsyncIterable<unknown> {
        const id = actorId(ref);
        const resolveTarget = (): Promise<'local' | HostDescriptor> => this.#resolveTarget(ref);
        const noteFailure = (
            error: unknown,
            remote: boolean
        ): Promise<'wrong-host' | 'unreachable' | 'draining' | null> =>
            this.#noteFailure(id, error, remote);
        const backoff = (attempt: number): Promise<void> => this.#backoff(attempt);
        const local = this.#local!;
        const remoteDispatcher = (target: HostDescriptor): ActorDispatcher =>
            this.#transportDispatcher(target);
        const retries = this.#retries;
        // Captured like `local`/`remoteDispatcher`: `run()` is a free
        // generator, so `this` must not leak into it.
        const counters = this.#counters;
        // Placement errors (wrong-host, unreachable) surface on the FIRST
        // pull — the endpoint pump pulls the first chunk before responding,
        // so buffering one chunk here matches established stream semantics.
        async function* run(): AsyncGenerator<unknown> {
            let lastError: unknown;
            for (let attempt = 0; attempt <= retries; attempt++) {
                if (attempt > 0) counters.retries++;
                const target = await resolveTarget();
                let iterable: AsyncIterable<unknown>;
                if (target === 'local') {
                    counters.routedLocal++;
                    iterable =
                        mode === 'watch'
                            ? local.dispatchWatch!(ref, method, args, call, options)
                            : local.dispatchStream!(ref, method, args, call);
                } else {
                    const dispatcher = remoteDispatcher(target);
                    if (mode === 'watch') {
                        if (!dispatcher.dispatchWatch) {
                            // Names the TRANSPORT, not the placement: the
                            // placement can watch perfectly well, and the
                            // fix is to configure a transport that can.
                            throw new Error(
                                `[sigx actors] cannot watch ${actorLabel(ref)} on ` +
                                    `${target.hostId}: the transport reaching it does not ` +
                                    `implement dispatchWatch.`
                            );
                        }
                        counters.remoteWatches++;
                        iterable = dispatcher.dispatchWatch(ref, method, args, call, options);
                    } else {
                        counters.remoteStreams++;
                        iterable = dispatcher.dispatchStream!(ref, method, args, call);
                    }
                }
                const iterator = iterable[Symbol.asyncIterator]();
                let first: IteratorResult<unknown>;
                try {
                    first = await iterator.next();
                } catch (error) {
                    const failure = await noteFailure(error, target !== 'local');
                    if (!failure) throw error;
                    if (failure !== 'wrong-host' && attempt < retries) await backoff(attempt);
                    lastError = error;
                    continue;
                }
                let finished = false;
                try {
                    if (first.done) {
                        finished = true;
                        return;
                    }
                    yield first.value;
                    for (;;) {
                        const next = await iterator.next();
                        if (next.done) {
                            finished = true;
                            return;
                        }
                        yield next.value;
                    }
                } finally {
                    if (!finished && iterator.return) await iterator.return(undefined);
                }
            }
            counters.routingFailures++;
            throw new ActorActivationError(actorLabel(ref), {
                cause:
                    lastError ??
                    new Error(`placement did not converge after ${retries + 1} attempts`)
            });
        }
        return run();
    }
}
