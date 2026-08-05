/**
 * In-process cluster providers — the `memoryStorage()` of clustering. One
 * hub is shared by every host in the process (the test topology); each
 * host takes its own `providers()` handle. No heartbeats or TTLs: a host
 * is live iff it joined and neither left nor was `kill()`ed. `kill()`
 * simulates a crash — the member vanishes without releasing anything and
 * its own membership handle fires `onSelfSuspect`. `expire()` simulates a
 * TTL lapse instead: the member vanishes the same way but is NEVER told,
 * which is the case #45 was about.
 */
import type {
    ActorDirectory,
    ClusterMembership,
    ClusterProviders,
    DirectoryEntry,
    MembershipView,
    HostDescriptor,
    HostStatus
} from './types';

export interface MemoryClusterHub {
    /** A fresh provider set for ONE host — call once per `clusterPlacement`. */
    providers(): ClusterProviders;
    /** Simulate a crash: drop the member without cleanup, fire its self-suspect. */
    kill(hostId: string): void;
    /**
     * Simulate a TTL EXPIRY: drop the member without cleanup and WITHOUT
     * firing its self-suspect — the host is never told.
     *
     * The difference from `kill` is the whole of #45. A crash the provider
     * noticed fires `onSelfSuspect`; a host whose event loop merely stalled
     * past the TTL is evicted by its peers and then carries on, its late
     * heartbeat succeeding, with nothing anywhere having failed. Use this to
     * exercise what a host does when the only evidence is its own absence
     * from the view.
     */
    expire(hostId: string): void;
    /** Test handle on the shared directory (seed/poison entries). */
    readonly directory: ActorDirectory;
}

export function memoryClusterHub(): MemoryClusterHub {
    let version = 0;
    const members = new Map<string, HostDescriptor>();
    const changeCbs = new Set<(view: MembershipView) => void>();
    const suspectCbs = new Map<string, Set<() => void>>();
    const entries = new Map<string, DirectoryEntry>();

    // Cached per version bump (#27): every mutation goes through `bump()`,
    // so a snapshot is valid until the next one — and a STABLE object
    // identity is what lets placement's active-host memo (a WeakMap keyed
    // on the view) hit. The snapshot arrays are typed readonly and never
    // mutated; a captured view stays a faithful picture of its version.
    let cachedView: MembershipView | null = null;
    const view = (): MembershipView => {
        if (cachedView === null || cachedView.version !== version) {
            cachedView = { version, hosts: [...members.values()] };
        }
        return cachedView;
    };
    const bump = (): void => {
        version++;
        const snapshot = view();
        for (const cb of changeCbs) cb(snapshot);
    };

    const directory: ActorDirectory = {
        async lookup(actorId) {
            return entries.get(actorId) ?? null;
        },
        async claim(actorId, mine) {
            const existing = entries.get(actorId);
            if (existing) return existing;
            entries.set(actorId, mine);
            return mine;
        },
        async release(actorId, expected) {
            const current = entries.get(actorId);
            if (current?.activationId === expected.activationId) entries.delete(actorId);
        },
        async evict(actorId, expected) {
            const current = entries.get(actorId);
            if (current?.activationId === expected.activationId) {
                entries.delete(actorId);
                return true;
            }
            return false;
        },
        async evictHost(hostId) {
            let removed = 0;
            for (const [actorId, entry] of entries) {
                if (entry.hostId === hostId) {
                    entries.delete(actorId);
                    removed++;
                }
            }
            return removed;
        }
    };

    const membershipFor = (): ClusterMembership => {
        let selfId: string | null = null;
        const selfSuspect = new Set<() => void>();
        return {
            async join(self) {
                selfId = self.hostId;
                members.set(self.hostId, self);
                suspectCbs.set(self.hostId, selfSuspect);
                bump();
            },
            async setStatus(status: HostStatus) {
                const current = selfId ? members.get(selfId) : undefined;
                if (!current) return;
                members.set(current.hostId, { ...current, status });
                bump();
            },
            async leave() {
                if (!selfId) return;
                suspectCbs.delete(selfId);
                if (members.delete(selfId)) bump();
            },
            view,
            async refresh() {
                return view();
            },
            async isAlive(hostId) {
                return members.has(hostId);
            },
            onChange(cb) {
                changeCbs.add(cb);
                return () => changeCbs.delete(cb);
            },
            onSelfSuspect(cb) {
                selfSuspect.add(cb);
                return () => selfSuspect.delete(cb);
            }
        };
    };

    return {
        providers: () => ({
            membership: membershipFor(),
            directory
        }),
        kill(hostId) {
            if (!members.delete(hostId)) return;
            bump();
            const cbs = suspectCbs.get(hostId);
            suspectCbs.delete(hostId);
            for (const cb of cbs ?? []) cb();
        },
        expire(hostId) {
            // The victim's `suspectCbs` stay registered and unfired — it is
            // not told. `bump()` reaches every subscriber including the
            // victim's own handle, which is the only signal it gets.
            if (!members.delete(hostId)) return;
            bump();
        },
        directory
    };
}
