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
    ActorUnplaceableError,
    ActorUnreachableError,
    ActorWrongHostError,
    isActorError,
    isActorErrorKind,
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
    type ActivationInfo,
    type ActorRef,
    type AnyActorDefinition,
    type PlacementBindings,
    type Host
} from '../types';
import { mintCallId } from '../call-id';
import {
    canonicalKey,
    createFanOut,
    declaresPrincipalIndependent,
    DEFAULT_WATCH_THROTTLE_MS,
    type FanOut
} from '../watch-core';
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
import { resolveClusterSymbol } from './host-endpoint';
import { fromHostWireError, hostWireCodec, toHostWireError } from './wire-errors';
import type {
    ClusterProviders,
    DirectoryEntry,
    MembershipView,
    PlacementPolicy,
    PolicyRuntime,
    HostDescriptor,
    HostIdentity
} from './types';

/**
 * One coalesced cross-host watch stream and everything needed to police it
 * (#111): the abort that is the ONLY signal able to release a remote
 * generator parked at an `await` (see `FanOut.subscribe`), plus the two
 * indexes teardown sweeps match on.
 */
interface CoalescedWatch {
    fanOut: FanOut;
    /** Aborting this is what cancels the remote stream. */
    controller: AbortController;
    /** `actorId(ref)` — matched by `#noteFailure` route invalidation. */
    id: string;
    /** Target host at creation — matched by the membership prune. */
    hostId: string;
    /**
     * True once a value arrived. Guards `#noteFailure` drops: the entry's
     * OWN first-pull retry reports failures through `#noteFailure`, and
     * dropping an un-established entry there would abort its own re-route.
     */
    established: boolean;
}

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

export interface RebalanceOptions {
    /** Act only when own load exceeds `threshold × cluster mean`. Default
     *  1.2 — a 20% overshoot, wide enough that ordinary churn never trips
     *  it. */
    threshold?: number;
    /** Most activations shed per round. Default 10 — rebalancing is a slow
     *  correction, not a stampede. */
    maxMoves?: number;
    /** Only activations idle at least this long are candidates. Default
     *  60 000 — an actor answering traffic is where it should be. */
    minIdleMs?: number;
    /** Per-peer load-probe budget, ms. Default 1000. */
    timeoutMs?: number;
}

export interface RebalanceReport {
    /** This host's activation count when the round ran. */
    own: number;
    /** Peers that answered a load probe (the mean's denominator, minus 1). */
    peers: number;
    /** Cluster mean over this host plus the answering peers. */
    mean: number;
    /** Activations actually shed this round. */
    moved: number;
    /** Why nothing moved, when nothing did. */
    reason?: 'balanced' | 'no-peers' | 'no-candidates' | 'not-active';
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
     * ONE load-driven rebalance round, for this host's own activations only
     * (a host can shed, never steal): probe peer loads, and if this host is
     * over `threshold × mean`, `migrate()` a bounded batch of its idlest
     * unheld activations. Total — never throws; the report says what
     * happened and, when nothing moved, why. `cluster({ rebalance })` runs
     * it on a cadence; ops tooling and tests call it directly.
     */
    rebalance(options?: RebalanceOptions): Promise<RebalanceReport>;
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

/**
 * The filtered active-host list, memoized per VIEW OBJECT (#27). Policies
 * and reminder-shard ownership rebuilt this array on every call — the
 * allocation behind `choose()` degrading 63× from N=1 to N=100. Keyed on
 * object identity, not `version`: two clusters in one process can share a
 * version number. Best-effort by design — the in-repo providers (and the
 * memory hub, since #27) return a stable view object between membership
 * changes so the memo hits, but a provider minting a fresh object per
 * `view()` call is still correct: it just misses, and pays the per-call
 * filter it always paid. What IS assumed is that a view object is a
 * snapshot — its `hosts` array must not be mutated in place. `filter` is
 * deterministic and order-preserving, so `rendezvous()` sees byte-identical
 * input and the winner per key — pinned storage identity for reminder
 * shards — cannot change.
 */
const activeHostsCache = new WeakMap<MembershipView, readonly HostDescriptor[]>();
function activeHosts(view: MembershipView): readonly HostDescriptor[] {
    let active = activeHostsCache.get(view);
    if (active === undefined) {
        active = view.hosts.filter((s) => s.status === 'active');
        activeHostsCache.set(view, active);
    }
    return active;
}

/** Shared, frozen — `locate()` answers this on every local hit, and the hot
 *  path should not allocate to say "it's here". */
const LOCAL: ActorLocation = Object.freeze({ local: true as const });

/** Does this member register `type`? An absent list is an older build's
 *  descriptor and reads as "registers everything" — the legacy behavior,
 *  and the only safe direction (#212). */
function hostRegisters(host: HostDescriptor, type: string): boolean {
    return host.types === undefined || host.types.includes(type);
}

/**
 * The per-type eligibility slice of a view (#212) — the hosts registering
 * `type` — memoized per VIEW OBJECT and then per type, the `activeHosts`
 * posture. When every host registers the type (a homogeneous cluster, and
 * any all-legacy view) the ORIGINAL view object is handed back, so the
 * `activeHosts` memo still hits, `rendezvous` sees byte-identical input,
 * and the fast path allocates nothing per call after the first look.
 */
interface EligibleHosts {
    /** What `choose()` is handed — the original view when nothing filtered. */
    readonly view: MembershipView;
    has(hostId: string): boolean;
}
const eligibleCache = new WeakMap<MembershipView, Map<string, EligibleHosts>>();
/** All hostIds of a view, memoized per view object — post-choose validation
 *  on the all-eligible fast path, shared across types. */
const viewIdsCache = new WeakMap<MembershipView, Set<string>>();
function viewIds(view: MembershipView): Set<string> {
    let ids = viewIdsCache.get(view);
    if (ids === undefined) {
        ids = new Set(view.hosts.map((h) => h.hostId));
        viewIdsCache.set(view, ids);
    }
    return ids;
}
function eligibleFor(view: MembershipView, type: string): EligibleHosts {
    let perType = eligibleCache.get(view);
    if (perType === undefined) {
        perType = new Map();
        eligibleCache.set(view, perType);
    }
    let eligible = perType.get(type);
    if (eligible === undefined) {
        const hosts = view.hosts.filter((h) => hostRegisters(h, type));
        if (hosts.length === view.hosts.length) {
            // Everything registers the type — hand back the ORIGINAL view,
            // but still validate membership: a policy fabricating a host
            // outside the view must be caught here too, not only when
            // filtering narrowed the view.
            eligible = { view, has: (hostId) => viewIds(view).has(hostId) };
        } else {
            const ids = new Set(hosts.map((h) => h.hostId));
            // Order-preserving filter, so rendezvous stays deterministic.
            const narrowed: MembershipView = { version: view.version, hosts };
            eligible = { view: narrowed, has: (hostId) => ids.has(hostId) };
        }
        perType.set(type, eligible);
    }
    return eligible;
}

/** Errors on the FIRST pull — where the wire pump and the caller's retry
 *  loop both look for placement failures. */
function failingIterable(error: unknown): AsyncIterable<unknown> {
    return {
        [Symbol.asyncIterator]: () => ({
            next: () => Promise.reject(error),
            return: () => Promise.resolve({ value: undefined, done: true as const })
        })
    };
}

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
        const active = activeHosts(view);
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
            const active = activeHosts(view);
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
        choose(ref, view, self) {
            const active = activeHosts(view);
            // Self may not be in the (eligibility-filtered, #212) view — a
            // host placing a type it does not register cannot pin it local.
            // Rendezvous keeps the fall-through deterministic, so racing
            // non-registering callers agree without a directory round-trip;
            // the directory stays the arbiter either way.
            if (active.some((h) => h.hostId === self.hostId)) return self;
            if (active.length === 0) return self;
            return rendezvous(actorId(ref), active) ?? self;
        }
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
    /** The bound host's registered type names — published in the descriptor
     *  (#212). Undefined before `bind()`, and for a hand-rolled `Host`
     *  without `registeredTypes()`, which then publishes a legacy-shaped
     *  descriptor (eligible for everything). */
    #registeredTypes: readonly string[] | undefined;
    /** type → its `defineActor({ placement })` policy, or null if none. */
    #declaredPolicies = new Map<string, PlacementPolicy | null>();
    /** Per-type stateless memo — same lazy shape as `#declaredPolicies`. */
    #statelessTypes = new Map<string, boolean>();
    /**
     * `type + '#' + method` → did it declare its watched read
     * principal-independent (#138)? Same lazy shape again. `#` cannot occur
     * in a type name (`defineActor` refuses it), so the composite is
     * injective.
     */
    #principalIndependentWatches = new Map<string, boolean>();
    /** Our live directory claims: actorId → the entry we wrote. */
    #claimed = new Map<string, DirectoryEntry>();
    /** actorId → hostId hint. Insertion-ordered Map as a cheap LRU. */
    #routeCache = new Map<string, string>();
    #seq = 0;
    #fenced = false;
    /** Single-flight guard for `#checkSelfPresence` — see the method. */
    #checkingSelf = false;
    /** Have we ever seen ourselves in a view? Absence only counts after. */
    #seenSelf = false;
    #status: HostDescriptor['status'] = 'joining';
    #unsubscribe: (() => void)[] = [];
    /** Teardowns returned by attached policies, run at `stop()`. */
    #policyTeardowns: (() => void)[] = [];
    /** Policies already attached — one object may serve several types. */
    #attachedPolicies = new Set<PlacementPolicy>();
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
            ...(this.#registeredTypes ? { types: this.#registeredTypes } : {}),
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
        // Captured HERE, before `start()` joins membership, so the first
        // descriptor a peer ever sees already carries the list (#212).
        this.#registeredTypes = host.registeredTypes?.();
        return {
            // CONTRACT: neither hook ever runs for a stateless worker type —
            // the local host skips them when it spins a pool member, so no
            // claim is written, `#claimed` never lists a worker, and the
            // directory counters stay untouched by stateless traffic. That
            // also keeps workers invisible to `#fence` (fencing defends the
            // single-activation invariant, which workers don't have — a
            // fenced host keeps serving pure compute) and to `rebalance()`
            // (migration moves claims; a pool has none).
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

    async rebalance(options: RebalanceOptions = {}): Promise<RebalanceReport> {
        const threshold = options.threshold ?? 1.2;
        const maxMoves = Math.max(1, options.maxMoves ?? 10);
        const minIdleMs = options.minIdleMs ?? 60_000;
        const timeoutMs = options.timeoutMs ?? 1_000;

        const host = this.#host;
        if (!host || this.#fenced || this.#status !== 'active') {
            return { own: 0, peers: 0, mean: 0, moved: 0, reason: 'not-active' };
        }
        const stats = host.stats();
        const own = stats.activations + stats.transitional.activating;
        const peers = this.view().hosts.filter(
            (h) => h.status === 'active' && h.hostId !== this.identity.hostId
        );
        // Probe loads; only ANSWERS enter the mean. Acting on missing data
        // is how a partitioned host would dump its actors on nobody.
        const answers: number[] = [];
        let next = 0;
        const worker = async (): Promise<void> => {
            while (next < peers.length) {
                const peer = peers[next++]!;
                try {
                    const report = await this.peerReport(peer, timeoutMs);
                    answers.push(
                        report.stats.activations + report.stats.transitional.activating
                    );
                } catch {
                    // Unreachable or mixed-version — excluded, not zeroed.
                }
            }
        };
        await Promise.all(Array.from({ length: Math.min(8, peers.length) }, worker));
        // Counted from here on — including a round that found nobody to
        // ask — so a partitioned host's cadence stays visible in the
        // counters rather than reading as a loop that never ran.
        this.#counters.rebalanceRounds++;
        if (answers.length === 0) {
            return { own, peers: 0, mean: own, moved: 0, reason: 'no-peers' };
        }

        const mean = (own + answers.reduce((a, b) => a + b, 0)) / (1 + answers.length);
        // Both guards matter: the ratio keeps small absolute imbalances
        // quiet, and the `- 1` keeps a two-host cluster from trading one
        // actor back and forth forever.
        if (own <= threshold * mean || own - mean < 1) {
            return { own, peers: answers.length, mean, moved: 0, reason: 'balanced' };
        }
        // Shed only down to the mean, never past it — the receiving hosts'
        // own rounds handle the rest. Idlest first; an actor answering
        // traffic, holding a stream/watch/task, or with queued turns is
        // where it should be.
        const budget = Math.min(maxMoves, Math.floor(own - mean));
        const candidates: ActivationInfo[] = [];
        for (const a of host.activations({ sortBy: 'idle', limit: maxMoves * 4 })) {
            if (candidates.length >= budget) break;
            if (a.keptAlive || a.queued !== 0 || a.idleMs < minIdleMs) continue;
            // A stateless pool member holds no claim, so `migrate()` would
            // no-op — counting it as a move would report a rebalance that
            // never happened. Workers shed load by idling out, not by
            // moving. (Memoized: sync from the second look at a type.)
            if (await this.#isStateless(a.type)) continue;
            candidates.push(a);
        }
        if (candidates.length === 0) {
            return { own, peers: answers.length, mean, moved: 0, reason: 'no-candidates' };
        }
        let moved = 0;
        for (const candidate of candidates) {
            try {
                // Sequential: each migrate drains an activation's turns, and a round is
                // a slow correction — parallel drains would spike exactly
                // the host that is already the busiest.
                await this.migrate({ type: candidate.type, key: candidate.key });
                moved++;
                this.#counters.rebalanceMigrations++;
            } catch (error) {
                if (__DEV__) {
                    console.warn(
                        `[sigx actors] rebalance: migrating ${candidate.type}/` +
                            `${candidate.key} failed:`,
                        error
                    );
                }
            }
        }
        return { own, peers: answers.length, mean, moved };
    }

    /**
     * Rendezvous hashing over the ACTIVE membership view: for each shard,
     * the host with the highest hash(shard, hostId) owns it. Deterministic
     * per view — no stored assignment, no lease. Transient view divergence
     * (two hosts both claiming a shard) is safe: the reminder shard's etag
     * CAS keeps firing at-most-once regardless.
     */
    ownsReminderShard(shard: string): boolean {
        const active = activeHosts(this.#options.membership.view());
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
                void this.#checkSelfPresence(view);
            }),
            this.#options.membership.onSelfSuspect(() => void this.#fence())
        );
        // Stateful policies attach AFTER the join, so their first view()
        // already contains this host. Declared policies attach when they
        // resolve (`#declaredPolicy`) — lazily, like everything about them.
        this.#attachPolicy(this.#policy);
        for (const policy of Object.values(this.#options.typePolicies ?? {})) {
            this.#attachPolicy(policy);
        }
    }

    /**
     * Hand a stateful policy its runtime seam. Once per policy OBJECT —
     * the same instance may be the default and serve several types — and
     * never fatally: a policy can cost throughput, not the placement.
     */
    #attachPolicy(policy: PlacementPolicy): void {
        if (!policy.attach || this.#attachedPolicies.has(policy)) return;
        this.#attachedPolicies.add(policy);
        try {
            const teardown = policy.attach(this.#policyRuntime());
            if (typeof teardown === 'function') this.#policyTeardowns.push(teardown);
        } catch (error) {
            if (__DEV__) {
                console.warn(
                    `[sigx actors] placement policy "${policy.name ?? 'unnamed'}" threw in ` +
                        `attach() — it will run un-attached:`,
                    error
                );
            }
        }
    }

    /** See `PolicyRuntime` — the narrow, hint-only seam a policy gets. */
    #policyRuntime(): PolicyRuntime {
        return {
            hostId: this.identity.hostId,
            view: () => this.view(),
            selfLoad: () => {
                const stats = this.#host?.stats();
                return stats ? stats.activations + stats.transitional.activating : 0;
            },
            peerLoad: async (target, timeoutMs, signal) => {
                const report = await this.peerReport(target, timeoutMs, signal);
                return report.stats.activations + report.stats.transitional.activating;
            }
        };
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
            resolve: (symbol) => resolveClusterSymbol(this.#host!, symbol),
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
        for (const teardown of this.#policyTeardowns) {
            try {
                teardown();
            } catch {
                // One policy's broken teardown must not strand the others.
            }
        }
        this.#policyTeardowns = [];
        this.#attachedPolicies.clear();
        for (const unsub of this.#unsubscribe) unsub();
        this.#unsubscribe = [];
        // Coalesced streams end cleanly BEFORE transports close, so the
        // cancel can still reach the (former) owners.
        for (const [key, entry] of this.#coalescedWatches) {
            this.#settleEntry(key, entry, 'finish');
        }
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

    dispatcherFor(ref: ActorRef): ActorDispatcher | Promise<ActorDispatcher> {
        // Local fast path: we hold the claim, no store reads, no routing.
        if (this.#claimed.has(actorId(ref))) return this.#local!;
        // Stateless workers always run HERE: no directory lookup, no route
        // cache, no policy. A remote hop to run a pure function is a bug,
        // and skipping `#resolveTarget` entirely is what makes the
        // `directory_ops == 0` bench invariant structural rather than a
        // counter special case. Sync after the first resolution per type
        // (the memo), so the hot path stays promise-free.
        const stateless = this.#isStateless(ref.type);
        if (stateless === true) return this.#local!;
        if (stateless === false) return this.#routing;
        return stateless.then((is) => (is ? this.#local! : this.#routing));
    }

    /**
     * Is this type a stateless worker pool? Lazily memoized per type, the
     * `#declaredPolicy` shape: sync (a boolean) from the second dispatch on;
     * the first dispatch of a lazily-registered type pays one await. A
     * failed module load answers `false` un-memoized — the dispatch path is
     * the one that reports load failures.
     */
    #isStateless(type: string): boolean | Promise<boolean> {
        const memo = this.#statelessTypes.get(type);
        if (memo !== undefined) return memo;
        const host = this.#host;
        if (!host) return false; // not bound yet — nothing dispatches before bind
        const resolved = host.definition(type);
        if (resolved && typeof (resolved as PromiseLike<unknown>).then === 'function') {
            return (resolved as Promise<AnyActorDefinition | null>).then(
                (def) => {
                    const is = def?.__sigxActor.stateless !== undefined;
                    this.#statelessTypes.set(type, is);
                    return is;
                },
                () => false
            );
        }
        const is = (resolved as AnyActorDefinition | null)?.__sigxActor.stateless !== undefined;
        this.#statelessTypes.set(type, is);
        return is;
    }

    /**
     * Did this method declare its watched read principal-independent (#138)?
     * Lazily memoized, the `#isStateless` shape.
     *
     * CONSERVATIVE on every unknown — no bound host, no definition (a
     * routing-only host, a lazily-registered type not yet loaded), or a
     * failed load — because `false` is today's behaviour: one stream per
     * principal. Being wrong in that direction costs connections; being
     * wrong in the other would merge identities the owner never agreed to
     * merge. A load failure is left un-memoized for the same reason
     * `#declaredPolicy` leaves it: the dispatch path is what reports it.
     */
    async #declaresPrincipalIndependent(type: string, method: string): Promise<boolean> {
        const key = `${type}#${method}`;
        const memo = this.#principalIndependentWatches.get(key);
        if (memo !== undefined) return memo;
        const host = this.#host;
        if (!host) return false;
        let def: AnyActorDefinition | null;
        try {
            def = (await host.definition(type)) ?? null;
        } catch {
            return false;
        }
        const declared = def !== null && declaresPrincipalIndependent(def.__sigxActor, method);
        this.#principalIndependentWatches.set(key, declared);
        return declared;
    }

    async locate(ref: ActorRef): Promise<ActorLocation> {
        this.#counters.locates++;
        // Same fast path as dispatcherFor: holding the claim answers with
        // no store read at all.
        if (this.#claimed.has(actorId(ref))) return LOCAL;
        // Stateless workers are local on EVERY host — this is what keeps the
        // public endpoint's `redirectIfRemote` from ever answering 421 for a
        // worker type.
        if (await this.#isStateless(ref.type)) return LOCAL;
        const target = await this.#resolveTarget(ref);
        if (target === 'local') return LOCAL;
        this.#counters.locateRemote++;
        return { local: false, owner: this.#ownerHint(target.hostId) };
    }

    // -----------------------------------------------------------------------
    // Inbound (redirect-not-proxy: local or wrong-host, never forward)

    /**
     * Refuse a type this host does not register (#212) — with the
     * `wrong-host` kind, so the caller evicts its route and re-places
     * against the eligibility-filtered view, instead of the unbranded 404
     * a caller can only fail on. Deliberately NO owner hint: this host has
     * no idea who owns it, and a fabricated hint would be worse than none.
     *
     * Sync — the registry answers `null` for an unregistered type without
     * loading anything; a LAZY type's key is registered, so it passes.
     */
    #inboundRefusal(ref: ActorRef): ActorWrongHostError | null {
        const host = this.#host;
        if (host && host.definition(ref.type) === null) {
            return new ActorWrongHostError(actorLabel(ref));
        }
        return null;
    }

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
        const refusal = this.#inboundRefusal(ref);
        if (refusal) return Promise.reject(refusal);
        return this.#local!.dispatch(ref, method, args, call);
    }

    dispatchInboundStream(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): AsyncIterable<unknown> {
        this.#counters.inboundStreams++;
        const refusal = this.#inboundRefusal(ref);
        if (refusal) return failingIterable(refusal);
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
        const refusal = this.#inboundRefusal(ref);
        if (refusal) return failingIterable(refusal);
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
            ...(this.#registeredTypes ? { types: [...this.#registeredTypes] } : {}),
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
            this.#coalescedWatch(ref, method, args, call, options)
    };

    /**
     * Cross-host watches coalesce (#111): ONE remote stream per
     * (actor, method, throttleMs, args, principal), fanned out locally.
     *
     * This is what makes live fan-out scale with hosts rather than
     * subscribers: n local subscribers to a remote read cost the owner one
     * serialized write per emission instead of n. The shared stream is
     * pulled at the fastest consumer's rate; a subscriber slower than the
     * feed drops oldest at the fan-out's bounded buffer — deliberate, since
     * a live read's superseded values are worthless by definition, and one
     * stalled consumer must not hold the stream (it DOES remove the
     * per-subscriber transport backpressure the uncoalesced path had).
     */
    #coalescedWatches = new Map<string, CoalescedWatch>();

    #coalescedWatch(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext,
        options?: { throttleMs?: number }
    ): AsyncIterable<unknown> {
        // Lazy first-pull open, like `Host.dispatchWatch`: placement errors
        // must surface on the first `next()`, not at subscribe time.
        const open = async (): Promise<AsyncIterator<unknown>> => {
            const target = await this.#resolveTarget(ref);
            if (target === 'local') {
                // Locally-placed watches keep activation-level sharing; a
                // second fan-out here would just double the buffering.
                return this.#routedStreamed(ref, method, args, call, 'watch', options)[
                    Symbol.asyncIterator
                ]();
            }
            // Normalized as the owner will normalize it (absent == 50), so
            // an absent and an explicit default coalesce. Args go through
            // the WIRE codec: two arg lists this encoding cannot tell apart
            // are indistinguishable to the owner too, so sharing them is
            // sound by construction. The principal is in the key by DEFAULT
            // because the owner splits identity-dependent reads per
            // principal (#121) and the relay cannot see that discovery —
            // carrying it keeps coalescing sound at the cost of not sharing
            // across DISTINCT principals.
            //
            // Unless the method declared `principalIndependent` (#138), in
            // which case identity is not an input to the read and every
            // subscriber may share one stream whoever they are. The owner
            // polices that promise where the read RUNS — a declared read
            // observed consulting `ctx.principal` fails the watch — so this
            // relay does not have to trust its own view of the definition.
            const shared = await this.#declaresPrincipalIndependent(ref.type, method);
            const throttleMs = options?.throttleMs ?? DEFAULT_WATCH_THROTTLE_MS;
            const key = canonicalKey([
                actorId(ref),
                method,
                throttleMs,
                hostWireCodec.encode(args),
                // `true` rather than reusing the anonymous `null` slot:
                // `canonicalKey` tags booleans ('t') apart from null ('z')
                // and from every encoded principal ('sN:'), so the shared
                // key is injective by grammar rather than by case analysis —
                // and a memo that flips cannot silently graft a shared
                // population onto an anonymous stream.
                shared ? true : (call.principal ?? null)
            ]);
            // Synchronous check+set after the await, so concurrent first
            // pulls race to one entry.
            let entry = this.#coalescedWatches.get(key);
            if (entry !== undefined) {
                this.#counters.coalescedWatches++;
            } else {
                // A shared stream carries NO principal, rather than the first
                // subscriber's: the key and the call context must agree about
                // what identity this stream has. If the declaration turns out
                // to be a lie, the read then runs under the LEAST-privileged
                // identity for everyone — strictly better than handing one
                // arbitrary subscriber's view to the rest — and it costs an
                // honest read nothing, by declaration.
                const principal = shared ? undefined : call.principal;
                entry = this.#openCoalesced(key, ref, method, args, options, principal, {
                    hostId: target.hostId
                });
            }
            return entry.fanOut.subscribe(call.abortSignal)[Symbol.asyncIterator]();
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

    #openCoalesced(
        key: string,
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        options: { throttleMs?: number } | undefined,
        principal: string | undefined,
        target: { hostId: string }
    ): CoalescedWatch {
        const entry: CoalescedWatch = {
            controller: new AbortController(),
            id: actorId(ref),
            hostId: target.hostId,
            established: false,
            fanOut: createFanOut(() => {
                // Last local subscriber left: the abort is what reaches a
                // remote generator parked at an `await` and releases the
                // owner's keep-alive; `return()` never would.
                this.#settleEntry(key, entry, 'finish');
            })
        };
        this.#coalescedWatches.set(key, entry);
        void this.#pumpCoalesced(key, entry, ref, method, args, options, principal);
        return entry;
    }

    async #pumpCoalesced(
        key: string,
        entry: CoalescedWatch,
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        options: { throttleMs?: number } | undefined,
        principal: string | undefined
    ): Promise<void> {
        // Per-subscriber context (deadline, bag, traceparent) cannot ride a
        // shared stream. Nothing sound is lost: the owner's shared watch
        // loop already re-invokes under its FIRST subscriber's context only
        // (`Activation.openWatch`), and the principal is in the coalescing
        // key — so every subscriber on this stream shares it by
        // construction, EXCEPT on a `principalIndependent` stream (#138),
        // where `principal` is undefined and the read has declared it cannot
        // tell the difference. Either way the identity this runs under is
        // the one the key promises, never one subscriber's borrowed by the
        // rest.
        //
        // Authorization is unaffected in both cases: policies are decided
        // per subscriber at the ENTRY point on this host, and the internal
        // host-to-host mount never re-runs them (see `host-endpoint.ts`).
        // Coalescing shares a READ, never a decision.
        const sharedCall: ActorCallContext = {
            callChain: [],
            callId: mintCallId(),
            ...(principal !== undefined ? { principal } : {}),
            abortSignal: entry.controller.signal
        };
        // Driving the EXISTING retry generator means first-pull wrong-host/
        // unreachable re-routing now protects the shared stream for every
        // subscriber at once, and `remoteWatches` increments where it
        // always did — once per stream attempt.
        const source = this.#routedStreamed(ref, method, args, sharedCall, 'watch', options)[
            Symbol.asyncIterator
        ]();
        try {
            for (;;) {
                const next = await source.next();
                if (next.done) return this.#settleEntry(key, entry, 'finish');
                entry.established = true;
                entry.fanOut.push(next.value);
            }
        } catch (error) {
            // v1: a shared-stream failure fails EVERY subscriber and drops
            // the entry — exact parity with the per-subscriber behavior
            // this replaces (the retry loop only ever re-routed the FIRST
            // pull). The `$live` channel's reconnect is the re-subscribe
            // path. Shared re-establishment is future work, not v1.
            this.#settleEntry(key, entry, 'fail', error);
        }
    }

    /**
     * The single coalesced-watch teardown. Identity-checked: a stale settle
     * (the aborted pump's late catch, a duplicate sweep) must never evict a
     * NEWER entry that has since taken the same key — the same hazard the
     * fan-out's idempotent drop guards one layer down.
     */
    #settleEntry(
        key: string,
        entry: CoalescedWatch,
        how: 'finish' | 'fail',
        error?: unknown
    ): void {
        if (this.#coalescedWatches.get(key) === entry) this.#coalescedWatches.delete(key);
        entry.controller.abort();
        if (how === 'fail') entry.fanOut.fail(error);
        else entry.fanOut.finish();
    }

    /**
     * Fail every established coalesced stream to this actor NOW — its route
     * just proved bad (wrong-host, unreachable, draining), and v1 does not
     * re-establish. Failing promptly hands subscribers their re-subscribe
     * signal instead of leaving them parked on a dead stream.
     */
    #dropCoalescedFor(id: string, error: unknown): void {
        for (const [key, entry] of this.#coalescedWatches) {
            if (entry.id === id && entry.established) {
                this.#settleEntry(key, entry, 'fail', error);
            }
        }
    }

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
        // Coalesced watch streams to a departed host fail NOW rather than
        // waiting out a transport timeout: subscribers get their
        // re-subscribe signal (the `$live` channel reconnects on error),
        // and a stale entry surviving here would serve nothing forever.
        for (const [key, entry] of this.#coalescedWatches) {
            if (!live.has(entry.hostId)) {
                this.#settleEntry(
                    key,
                    entry,
                    'fail',
                    new ActorUnreachableError(`${entry.hostId} left the membership view mid-watch`)
                );
            }
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

    /**
     * The provider-agnostic half of self-fencing (#45): a host that is not
     * in its own membership view has had its claims evicted, whether or not
     * anything here ever failed.
     *
     * The provider's heartbeat clock is the primary detector — it knows the
     * TTL, and a provider whose write re-registers the host (pg's upsert,
     * surreal's UPSERT, redis' re-`sadd`) puts it back in the view before
     * anyone here could look. This is the backstop for the rest: a store
     * wiped or failed over, an operator deleting a row, a third-party
     * provider with no clock of its own.
     *
     * It fences a live pod, so every guard below is load-bearing:
     *
     * - **A non-empty view.** `hosts.length === 0` already means "solo / not
     *   started" throughout placement. Fencing on it would turn a membership
     *   store failing over to a cold replica into EVERY host fencing at
     *   once — every pod failing liveness, the cluster gone (#141).
     * - **Self seen at least once.** Protects join ordering, and any
     *   provider whose view legitimately excludes self.
     * - **A fresh read.** A cached view that merely lags must never be the
     *   evidence; only a refresh that still omits us counts. A refresh that
     *   THROWS means the store is unreachable, which is the heartbeat path's
     *   business, not ours.
     * - **Single flight.** `refresh()` fires `onChange` in every provider,
     *   which re-enters this method; without the latch a genuine absence
     *   would spiral into a refresh storm.
     */
    async #checkSelfPresence(view: MembershipView): Promise<void> {
        if (this.#fenced || this.#checkingSelf) return;
        // Not while joining (no view yet) or leaving — the drain owns that
        // exit, and fencing mid-handoff would abort it.
        if (this.#status !== 'active' || this.#startedAt === 0) return;
        if (view.hosts.length === 0) return;
        if (view.hosts.some((s) => s.hostId === this.identity.hostId)) {
            this.#seenSelf = true;
            return;
        }
        if (!this.#seenSelf) return;
        this.#checkingSelf = true;
        try {
            const fresh = await this.#options.membership.refresh().catch(() => null);
            if (
                fresh &&
                fresh.hosts.length > 0 &&
                !fresh.hosts.some((s) => s.hostId === this.identity.hostId)
            ) {
                await this.#fence();
            }
        } finally {
            this.#checkingSelf = false;
        }
    }

    /** Self-fence: membership lost — stop claiming, drop what we hold. */
    async #fence(): Promise<void> {
        if (this.#fenced) return;
        this.#fenced = true;
        this.#counters.selfFences++;
        // Withdraw, so peers stop routing to a host that refuses every
        // activation instead of waiting out their own TTL — and so a
        // provider whose heartbeat re-registers this host cannot keep
        // advertising it as active forever. Idempotent (every provider
        // guards on "am I joined"), so `stop()`'s own leave stays fine.
        //
        // Deliberately NOT awaited: we fence precisely when the membership
        // store may be unreachable, and a leave that hangs must not keep us
        // from dropping the activations. That is the correctness action;
        // this is a courtesy.
        void Promise.resolve(this.#options.membership.leave()).catch(() => {});
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
            if (member && hostRegisters(member, ref.type)) {
                this.#counters.routeCacheHits++;
                return member;
            }
            // Departed — or live but not registering the type (#212): a hint
            // learned from a peer that knew less than the view does. Either
            // way the route is unusable; drop it and resolve fresh.
            this.#routeCache.delete(id);
        }
        this.#counters.routeCacheMisses++;

        this.#counters.directoryLookups++;
        const entry = await this.#options.directory.lookup(id);
        if (entry) {
            if (entry.hostId === this.identity.hostId) return 'local';
            const member = this.#member(entry.hostId);
            if (member) {
                if (hostRegisters(member, ref.type)) {
                    this.#cacheRoute(id, entry.hostId);
                    return member;
                }
                // Live but structurally unable to serve it (#212):
                // descriptors are immutable per incarnation, so this entry
                // is stale or poisoned and can only ever bounce callers.
                // Evict and place fresh below, the dead-owner treatment.
                this.#counters.directoryEvictions++;
                await this.#options.directory.evict(id, entry);
            } else if (!(await this.#options.membership.isAlive(entry.hostId))) {
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
        let chosen: HostDescriptor;
        const activeAll = activeHosts(view);
        if (activeAll.length === 0) {
            // Nothing active ANYWHERE (a cluster mid-drain): eligibility has
            // no opinion to offer — hand the policy the full view and let
            // its own fallback answer, exactly as before #212.
            chosen = policy.choose(ref, view, this.descriptor());
        } else {
            const eligible = eligibleFor(view, ref.type);
            const activeEligible =
                eligible.view === view ? activeAll : activeHosts(eligible.view);
            if (activeEligible.length === 0) {
                // Default-deny, loudly: silently widening to the full view
                // is how a type lands on a host that never registered it.
                throw new ActorUnplaceableError(ref.type, {
                    hosts: view.hosts.length,
                    active: activeAll.length
                });
            }
            chosen = policy.choose(ref, eligible.view, this.descriptor());
            if (chosen.hostId !== this.identity.hostId && !eligible.has(chosen.hostId)) {
                // A policy bug, not a routing condition — re-placing
                // silently would hide it (the `#declaredPolicy` unusable-
                // strategy posture). Answering SELF is always allowed, even
                // when self is not in the handed view (expired from
                // membership, or not registering the type): self means
                // 'local', and the local path has its own authoritative
                // guards — the fence, the claim, the registry — where a
                // remote answer would be dialed blind.
                throw new Error(
                    `[sigx actors] placement policy "${policy.name ?? 'unnamed'}" chose ` +
                        `${chosen.hostId} for "${ref.type}", which is not an eligible ` +
                        `member of the view it was handed. A policy must answer with a ` +
                        `member of that view, or with self.`
                );
            }
        }
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
        if (policy) this.#attachPolicy(policy);
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
            this.#dropCoalescedFor(id, error);
            const owner = (error as ActorWrongHostError).owner;
            if (owner?.hostId && owner.hostId !== this.identity.hostId) {
                this.#cacheRoute(id, owner.hostId);
            }
            return 'wrong-host';
        }
        if (isActorError(error) && error.kind === 'unreachable') {
            this.#counters.unreachableRetries++;
            this.#routeCache.delete(id);
            this.#dropCoalescedFor(id, error);
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
            this.#dropCoalescedFor(id, error);
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
            let target: 'local' | HostDescriptor;
            try {
                target = await this.#resolveTarget(ref);
            } catch (error) {
                if (!isActorErrorKind(error, 'unplaceable')) throw error;
                // The one pod registering this type may be mid-join (a
                // rolling deploy) — refresh and retry rather than failing
                // the call into the deploy window.
                lastError = error;
                if (attempt < this.#retries) {
                    await this.#options.membership.refresh().catch(() => {});
                    await this.#backoff(attempt);
                }
                continue;
            }
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
        const refreshMembership = (): Promise<unknown> =>
            this.#options.membership.refresh().catch(() => {});
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
                let target: 'local' | HostDescriptor;
                try {
                    target = await resolveTarget();
                } catch (error) {
                    if (!isActorErrorKind(error, 'unplaceable')) throw error;
                    // Same posture as `#routedDispatch`: a registering host
                    // may be mid-join — refresh and retry.
                    lastError = error;
                    if (attempt < retries) {
                        await refreshMembership();
                        await backoff(attempt);
                    }
                    continue;
                }
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
