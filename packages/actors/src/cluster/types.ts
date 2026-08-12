/**
 * Cluster provider contracts — membership and the distributed actor
 * directory. Domain interfaces rather than a generic KV so a real store
 * can use its native atomics (key TTLs for heartbeats, create-if-absent
 * and compare-and-delete for claims). In-memory providers live in
 * `./memory`; Redis providers ship as `@sigx/actors-redis`; a Kubernetes
 * or gossip provider is a pure addition behind the same interfaces.
 */
import type { ActorPlacementStrategy, ActorRef } from '../types';

/** Minted once per host START and never reused — a restart is a new host. */
export interface HostIdentity {
    /** `s.<random base36>` — short because it tags every claim and call. */
    readonly hostId: string;
    /** Start instant, epoch-ms. Orders incarnations; tie-break for claims. */
    readonly epoch: number;
}

export type HostStatus = 'joining' | 'active' | 'leaving';

export interface HostDescriptor extends HostIdentity {
    /** Peer-reachable origin of this host's internal endpoint. */
    readonly address: string;
    readonly status: HostStatus;
    /**
     * Peer-reachable address PER TRANSPORT, keyed by `HostTransport.name` —
     * `{ http: 'http://10.0.4.7:7311', tcp: 'tcp://10.0.4.7:11111' }`.
     *
     * Published so a MIXED-transport cluster can exist, which is the only
     * way a new transport ever gets deployed: during the rolling deploy
     * that introduces it, half the cluster publishes no `tcp` key and the
     * other half must still be able to reach them. Absent on a host from a
     * build predating the field, which reads as "HTTP only" — the safe
     * direction.
     *
     * `address` remains the ops-facing origin (wrong-host hints,
     * `HostReport.address`, `clusterStats` failures) and is what a
     * transport with no address of its own is published under.
     */
    readonly addresses?: Readonly<Record<string, string>>;
    /**
     * Origin a CLIENT can reach this host's PUBLIC actor mount on —
     * `https://host-3.example.com`. An origin only: the mount path is a
     * mount concern, so the endpoint composes `publicAddress + base`.
     *
     * A typed field rather than a `meta` key on purpose. `meta` is
     * free-form and is read by placement POLICIES; a field the endpoint
     * redirects clients to must not be indistinguishable from a zone label,
     * and must be findable by anyone auditing what gets disclosed.
     *
     * Absent on hosts whose operator did not configure one, which reads as
     * "never redirect a client here" — see `ActorOwnerHint.publicAddress`.
     */
    readonly publicAddress?: string;
    /**
     * Every actor type this host REGISTERS (workers and lazy types
     * included), sorted — what makes placement registration-aware (#212): a
     * host is only ever chosen to host a type in this list.
     *
     * A typed field rather than a `meta` key for the `publicAddress`
     * reason: placement filters on it default-deny, so it must not be
     * indistinguishable from a free-form zone label.
     *
     * ABSENT on a host from a build predating the field, which reads as
     * "eligible for every type" — the legacy behavior and the only safe
     * direction (absent-means-ineligible would empty every view mid-rolling
     * -deploy). Present is authoritative, and immutable for the host's
     * incarnation: hostIds are minted per start, so a host can never gain
     * or lose a type without also being a NEW host.
     */
    readonly types?: readonly string[];
    /** Free-form placement hints (zone, appVersion, weight …). */
    readonly meta?: Readonly<Record<string, string>>;
}

export interface MembershipView {
    /** Monotonic per cluster; bumps on any join/leave/status change. */
    readonly version: number;
    /** Hosts currently believed live. */
    readonly hosts: readonly HostDescriptor[];
}

/**
 * One host's handle on cluster membership. Liveness is the provider's
 * business (the store baseline uses TTL heartbeats: a host is live iff its
 * heartbeat record exists) — consumers only read views and probe liveness.
 */
export interface ClusterMembership {
    /** Register self and begin heartbeating. Resolves once visible. */
    join(self: HostDescriptor): Promise<void>;
    /** Update this host's advertised status (e.g. `'leaving'` on drain). */
    setStatus(status: HostStatus): Promise<void>;
    /** Graceful exit: stop heartbeating and remove the entry. */
    leave(): Promise<void>;
    /**
     * Current cached view — SYNC; `dispatcherFor` is on the hot path.
     * Treat the returned object as an immutable snapshot: build a NEW view
     * per membership change rather than mutating one in place. Returning
     * the SAME object between changes is recommended (placement memoizes
     * derived data per view object — a fresh object per call is still
     * correct but pays the derivation on every call).
     *
     * **A live host MUST appear in its own view.** Placement reads its own
     * absence as "my claims have been evicted" and self-fences on it, so a
     * provider that omits self would fence every host it serves. An empty
     * view is exempt — that reads as solo/not-started, never as lost
     * membership.
     */
    view(): MembershipView;
    /** Force a store round-trip. */
    refresh(): Promise<MembershipView>;
    /** Authoritative point-liveness probe (store read, not cache). */
    isAlive(hostId: string): Promise<boolean>;
    onChange(cb: (view: MembershipView) => void): () => void;
    /**
     * Fires when THIS host can no longer prove CONTINUOUS membership — the
     * self-fencing hook.
     *
     * Two ways to lose it, and a provider must report both. A heartbeat
     * that FAILS past the TTL is the obvious one. A heartbeat that merely
     * LANDS past it is the one that bit us (#45): a stalled event loop
     * resumes, writes late, succeeds — while peers already expired this
     * host and released every directory claim it held. Judge the gap since
     * the last confirmed write, not the outcome of the current one;
     * `heartbeatClock` does exactly that and is shared by every provider
     * in this repo.
     */
    onSelfSuspect(cb: () => void): () => void;
}

/**
 * A directory claim. `activationId` is unique per activation
 * (`{hostId}/{epoch}/{seq}`) so release/evict are compare-and-delete — a
 * slow release can never remove a successor's claim.
 */
export interface DirectoryEntry {
    readonly hostId: string;
    readonly activationId: string;
}

/**
 * The distributed activation directory. Entries carry NO TTL: an entry is
 * valid iff its host is live in the membership view (one heartbeat per
 * host, not per activation). The storage etag CAS remains the integrity
 * floor underneath — a briefly-wrong directory costs a rejected save, not
 * corruption.
 */
export interface ActorDirectory {
    lookup(actorId: string): Promise<DirectoryEntry | null>;
    /**
     * Atomic create-if-absent. Returns the WINNING entry — `mine` on
     * success, the existing entry on a lost race.
     */
    claim(actorId: string, mine: DirectoryEntry): Promise<DirectoryEntry>;
    /** Compare-and-delete: remove only if the stored entry matches. */
    release(actorId: string, expected: DirectoryEntry): Promise<void>;
    /** Compare-and-delete for dead-host entries; true if removed. */
    evict(actorId: string, expected: DirectoryEntry): Promise<boolean>;
    /**
     * Sweep every entry owned by one host (proactive hygiene when a host
     * dies) — idempotent, safe for racing survivors. Optional: without it
     * dead entries are still reclaimed lazily on lookup. Returns the
     * number removed.
     */
    evictHost?(hostId: string): Promise<number>;
}

/** What a cluster-store package provides for one host. */
export interface ClusterProviders {
    membership: ClusterMembership;
    directory: ActorDirectory;
}

/**
 * What an attached policy may ask the placement for — the one legal channel
 * from the runtime into a policy beyond `choose()`'s own arguments.
 *
 * Deliberately narrow: the load numbers are the piece a policy cannot get
 * anywhere else (`view()` it already receives per call), and everything
 * here is a HINT — a policy acting on a stale or missing answer must still
 * only cost throughput, never correctness. The directory stays the sole
 * arbiter of single-activation.
 */
export interface PolicyRuntime {
    /** This placement's host id. */
    readonly hostId: string;
    /** The live membership view — same source `choose()` is handed. */
    view(): MembershipView;
    /**
     * This host's own activation count (settled plus mid-activation).
     * Local, but O(activations) — call it on a refresh cadence, never per
     * placement decision.
     */
    selfLoad(): number;
    /**
     * A peer's activation count, over the authenticated host-to-host ops
     * channel (one round trip plus one directory walk on the peer). Throws
     * on an unreachable or mixed-version peer — keep the stale value and
     * carry on; never act on missing data.
     */
    peerLoad(target: HostDescriptor, timeoutMs: number, signal?: AbortSignal): Promise<number>;
}

/**
 * Picks the host where a NEW activation goes.
 * Custom strategies implement this and are applied either centrally
 * (`clusterPlacement({ policy, typePolicies })`) or per actor
 * (`defineActor({ placement })`), which is the attribute-style form and
 * takes precedence.
 *
 * `choose` is SYNC on purpose: it sits under `dispatcherFor` on the hot
 * path, which is also why `membership.view()` is sync.
 *
 * The `view` handed to `choose()` is pre-filtered to the hosts that
 * REGISTER `ref.type` (#212) — a descriptor without `types` counts as
 * registering everything — so a custom policy is registration-safe without
 * doing anything. Three consequences to write policies against: the view
 * is never empty (an empty eligible set throws `unplaceable` before any
 * policy runs); `self` is always this host's full descriptor but may NOT
 * be in `view.hosts` (a host may place types it does not register — fall
 * through to an eligible host rather than answering `self` blindly); and
 * the answer MUST be a member of `view.hosts` — anything else is a policy
 * bug and fails the dispatch loudly. `PolicyRuntime.view()` remains the
 * FULL view: load probes are host-level, not per-type.
 */
export interface PlacementPolicy extends ActorPlacementStrategy {
    choose(
        ref: ActorRef,
        view: MembershipView,
        self: HostDescriptor
    ): HostDescriptor;
    /**
     * Optional lifecycle for a STATEFUL policy (`activationCountPolicy()`
     * keeps a load cache; a zone-aware policy might watch the view). Called
     * once per placement when it starts — or on first resolution, for a
     * policy declared on a definition — with the runtime seam above; the
     * returned function is the teardown, run at placement stop.
     *
     * A throwing `attach` is dev-warned and otherwise ignored: a policy can
     * never fail the placement. `choose()` must keep working un-attached —
     * single-node hosts and foreign backends never call this.
     */
    attach?(runtime: PolicyRuntime): void | (() => void);
}
