/**
 * In-process cluster providers — the `memoryStorage()` of clustering. One
 * hub is shared by every silo in the process (the test topology); each
 * silo takes its own `providers()` handle. No heartbeats or TTLs: a silo
 * is live iff it joined and neither left nor was `kill()`ed. `kill()`
 * simulates a crash — the member vanishes without releasing anything and
 * its own membership handle fires `onSelfSuspect`.
 */
import type {
    ActorDirectory,
    ClusterMembership,
    ClusterProviders,
    DirectoryEntry,
    MembershipView,
    ReminderLease,
    SiloDescriptor,
    SiloStatus
} from './types';

export interface MemoryClusterHub {
    /** A fresh provider set for ONE silo — call once per `clusterPlacement`. */
    providers(): ClusterProviders;
    /** Simulate a crash: drop the member without cleanup, fire its self-suspect. */
    kill(siloId: string): void;
    /** Test handle on the shared directory (seed/poison entries). */
    readonly directory: ActorDirectory;
}

export function memoryClusterHub(): MemoryClusterHub {
    let version = 0;
    const members = new Map<string, SiloDescriptor>();
    const changeCbs = new Set<(view: MembershipView) => void>();
    const suspectCbs = new Map<string, Set<() => void>>();
    const entries = new Map<string, DirectoryEntry>();
    let leaseOwner: string | null = null;

    const view = (): MembershipView => ({ version, silos: [...members.values()] });
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
        async evictSilo(siloId) {
            let removed = 0;
            for (const [actorId, entry] of entries) {
                if (entry.siloId === siloId) {
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
                selfId = self.siloId;
                members.set(self.siloId, self);
                suspectCbs.set(self.siloId, selfSuspect);
                bump();
            },
            async setStatus(status: SiloStatus) {
                const current = selfId ? members.get(selfId) : undefined;
                if (!current) return;
                members.set(current.siloId, { ...current, status });
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
            async isAlive(siloId) {
                return members.has(siloId);
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

    const lease: ReminderLease = {
        async tryHold(siloId) {
            if (leaseOwner === null || !members.has(leaseOwner)) leaseOwner = siloId;
            return leaseOwner === siloId;
        },
        async release(siloId) {
            if (leaseOwner === siloId) leaseOwner = null;
        }
    };

    return {
        providers: () => ({
            membership: membershipFor(),
            directory,
            reminderLease: lease
        }),
        kill(siloId) {
            if (!members.delete(siloId)) return;
            bump();
            const cbs = suspectCbs.get(siloId);
            suspectCbs.delete(siloId);
            for (const cb of cbs ?? []) cb();
        },
        directory
    };
}
