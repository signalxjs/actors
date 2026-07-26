/**
 * Cluster provider contracts — membership, the distributed actor directory,
 * and the reminder-leader lease. Domain interfaces rather than a generic KV
 * so a real store can use its native atomics (key TTLs for heartbeats,
 * create-if-absent and compare-and-delete for claims). In-memory providers
 * live in `./memory`; Redis providers ship as `@sigx/actors-redis`; a
 * Kubernetes or gossip provider is a pure addition behind the same
 * interfaces.
 */
import type { ActorRef } from '../types';

/** Minted once per silo START and never reused — a restart is a new silo. */
export interface SiloIdentity {
    /** `s.<random base36>` — short because it tags every claim and call. */
    readonly siloId: string;
    /** Start instant, epoch-ms. Orders incarnations; tie-break for claims. */
    readonly epoch: number;
}

export type SiloStatus = 'joining' | 'active' | 'leaving';

export interface SiloDescriptor extends SiloIdentity {
    /** Peer-reachable origin of this silo's internal endpoint. */
    readonly address: string;
    readonly status: SiloStatus;
    /** Free-form placement hints (zone, appVersion, weight …). */
    readonly meta?: Readonly<Record<string, string>>;
}

export interface MembershipView {
    /** Monotonic per cluster; bumps on any join/leave/status change. */
    readonly version: number;
    /** Silos currently believed live. */
    readonly silos: readonly SiloDescriptor[];
}

/**
 * One silo's handle on cluster membership. Liveness is the provider's
 * business (the store baseline uses TTL heartbeats: a silo is live iff its
 * heartbeat record exists) — consumers only read views and probe liveness.
 */
export interface ClusterMembership {
    /** Register self and begin heartbeating. Resolves once visible. */
    join(self: SiloDescriptor): Promise<void>;
    /** Update this silo's advertised status (e.g. `'leaving'` on drain). */
    setStatus(status: SiloStatus): Promise<void>;
    /** Graceful exit: stop heartbeating and remove the entry. */
    leave(): Promise<void>;
    /** Current cached view — SYNC; `dispatcherFor` is on the hot path. */
    view(): MembershipView;
    /** Force a store round-trip. */
    refresh(): Promise<MembershipView>;
    /** Authoritative point-liveness probe (store read, not cache). */
    isAlive(siloId: string): Promise<boolean>;
    onChange(cb: (view: MembershipView) => void): () => void;
    /**
     * Fires when THIS silo can no longer prove its own membership
     * (heartbeat failures past the TTL) — the self-fencing hook.
     */
    onSelfSuspect(cb: () => void): () => void;
}

/**
 * A directory claim. `activationId` is unique per activation
 * (`{siloId}/{epoch}/{seq}`) so release/evict are compare-and-delete — a
 * slow release can never remove a successor's claim.
 */
export interface DirectoryEntry {
    readonly siloId: string;
    readonly activationId: string;
}

/**
 * The distributed activation directory. Entries carry NO TTL: an entry is
 * valid iff its silo is live in the membership view (one heartbeat per
 * silo, not per activation). The storage etag CAS remains the integrity
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
    /** Compare-and-delete for dead-silo entries; true if removed. */
    evict(actorId: string, expected: DirectoryEntry): Promise<boolean>;
    /**
     * Sweep every entry owned by one silo (proactive hygiene when a silo
     * dies) — idempotent, safe for racing survivors. Optional: without it
     * dead entries are still reclaimed lazily on lookup. Returns the
     * number removed.
     */
    evictSilo?(siloId: string): Promise<number>;
}

/**
 * The reminder-leader lease: exactly one silo ticks the reminder table.
 * `tryHold` acquires the lease if free (or renews if held by `siloId`) and
 * answers whether this silo is the leader right now.
 */
export interface ReminderLease {
    tryHold(siloId: string): Promise<boolean>;
    release(siloId: string): Promise<void>;
}

/** What a cluster-store package provides for one silo. */
export interface ClusterProviders {
    membership: ClusterMembership;
    directory: ActorDirectory;
    reminderLease?: ReminderLease;
}

/** Picks the silo to host a NEW activation. */
export interface PlacementPolicy {
    choose(
        ref: ActorRef,
        view: MembershipView,
        self: SiloDescriptor
    ): SiloDescriptor;
}
