/**
 * `ClusterMembership` on Kubernetes primitives. Liveness is this host's
 * own Lease, renewed every `heartbeatMs`; peers arrive through the
 * label-selected Lease watch and count as live while their `renewTime` is
 * within their declared duration. The membership view is materialized
 * locally: `view()` is sync (it sits under `dispatcherFor` on the hot
 * path) and `version` is a local monotonic counter — resourceVersion is
 * an opaque watch bookmark, never a number.
 *
 * Self-fencing mirrors the Redis provider exactly: renewal failures past
 * `ttlMs` fire `onSelfSuspect` once (latched); the next successful renewal
 * clears the latch. Watch health never fences — a broken watch is stale
 * peers, not lost membership.
 */
import type { ClusterMembership, MembershipView, HostDescriptor } from '@sigx/actors/cluster';
import { kubeClient, type KubeClientOptions } from './client';
import {
    buildLease,
    CLUSTER_LABEL,
    isFresh,
    leaseName,
    parseLease,
    renewPatch,
    type LeaseObject
} from './lease';
import { listWatchLoop, type ListWatchLoop } from './watch';

export interface K8sMembershipOptions extends KubeClientOptions {
    /** Extra labels stamped on this host's Lease AND selecting peers. */
    labels?: Record<string, string>;
    /** Value of the `sigx.dev/cluster` label — two clusters can share a
     *  namespace. Default `default`. */
    clusterName?: string;
    /** Lease names are `${leasePrefix}-${hostId}`. Default `sigx`. */
    leasePrefix?: string;
    /** Lease renewal cadence, ms. Default 5000. */
    heartbeatMs?: number;
    /** Liveness TTL, ms — serialized as `spec.leaseDurationSeconds`
     *  (ceiled to whole seconds). Missed renewals past this fire
     *  `onSelfSuspect`; peers past their own are dropped from the view.
     *  Default 15000. */
    ttlMs?: number;
    /** Allowance for peer↔local clock skew in freshness checks — renewTime
     *  is written by the peer's clock. Default 2000. */
    clockSkewMs?: number;
    /** Reconciling LIST cadence, ms — the safety net under the watch.
     *  0 disables. Default 60000. */
    relistMs?: number;
    /** Watch reconnect backoff bounds, ms. Default 250/5000. */
    watchBackoff?: { minMs?: number; maxMs?: number };
}

const noop = (): void => {};

/** Release the body of a response we only care about the status of. */
function drain(res: Response): Response {
    res.body?.cancel().catch(noop);
    return res;
}

type Timer = ReturnType<typeof setInterval>;

function unref(timer: Timer): Timer {
    (timer as { unref?: () => void }).unref?.();
    return timer;
}

export function k8sMembership(options: K8sMembershipOptions = {}): ClusterMembership {
    const heartbeatMs = options.heartbeatMs ?? 5_000;
    const ttlMs = options.ttlMs ?? 15_000;
    const clockSkewMs = options.clockSkewMs ?? 2_000;
    const relistMs = options.relistMs ?? 60_000;
    const leasePrefix = options.leasePrefix ?? 'sigx';
    const labels = {
        [CLUSTER_LABEL]: options.clusterName ?? 'default',
        ...options.labels
    };
    const labelSelector = Object.entries(labels)
        .map(([key, value]) => `${key}=${value}`)
        .sort()
        .join(',');
    const client = kubeClient(options);

    let self: HostDescriptor | null = null;
    let selfLease = '';
    /** Descriptor changed but not yet on the wire — next renewal carries it. */
    let descriptorDirty = false;
    let cached: MembershipView = { version: 0, hosts: [] };
    let fingerprint = '';
    let loop: ListWatchLoop | null = null;
    let beat: Timer | null = null;
    let sweep: Timer | null = null;
    let relist: Timer | null = null;
    let lastOkMs = 0;
    let suspected = false;
    const leases = new Map<string, LeaseObject>();
    const changeCbs = new Set<(view: MembershipView) => void>();
    const suspectCbs = new Set<() => void>();

    // The view is a pure function of the lease map and the clock; a version
    // bump means the DESCRIPTOR SET changed. Renewals move renewTime, which
    // is deliberately outside the fingerprint — n hosts heartbeating
    // produce n² watch events and zero onChange storms.
    const materialize = (): void => {
        const now = Date.now();
        const hosts: HostDescriptor[] = [];
        for (const lease of leases.values()) {
            if (!isFresh(lease, now, clockSkewMs)) continue;
            const descriptor = parseLease(lease);
            if (descriptor) hosts.push(descriptor);
        }
        hosts.sort((a, b) => (a.hostId < b.hostId ? -1 : a.hostId > b.hostId ? 1 : 0));
        const next = JSON.stringify(hosts);
        if (next === fingerprint) return;
        fingerprint = next;
        cached = { version: cached.version + 1, hosts };
        for (const cb of changeCbs) cb(cached);
    };

    const selfLeaseBody = (): LeaseObject =>
        buildLease(self!, {
            name: selfLease,
            namespace: client.namespace,
            labels,
            ttlMs,
            nowMs: Date.now()
        });

    const renew = async (): Promise<void> => {
        if (!self) return;
        const res = drain(
            await client.patch(selfLease, renewPatch(Date.now(), descriptorDirty ? self : undefined))
        );
        if (res.status === 404) {
            // Someone deleted our Lease (operator cleanup, namespace churn):
            // peers already think we're gone — recreate rather than fence.
            const created = drain(await client.create(selfLeaseBody()));
            if (!created.ok) throw new Error(`lease recreate failed: HTTP ${created.status}`);
        } else if (!res.ok) {
            throw new Error(`lease renew failed: HTTP ${res.status}`);
        }
        descriptorDirty = false;
        lastOkMs = Date.now();
        suspected = false;
    };

    const onRenewFailure = (): void => {
        // Can't prove our own membership past the TTL → fence, once.
        if (!suspected && Date.now() - lastOkMs > ttlMs) {
            suspected = true;
            for (const cb of suspectCbs) cb();
        }
    };

    return {
        async join(descriptor) {
            self = descriptor;
            selfLease = leaseName(leasePrefix, descriptor.hostId);
            const res = drain(await client.create(selfLeaseBody()));
            if (res.status === 409) {
                // A predecessor left its Lease behind (hostIds are minted per
                // start, so this is ours to take over) — replace it wholesale.
                const replaced = drain(await client.replace(selfLease, selfLeaseBody()));
                if (!replaced.ok) throw new Error(`lease takeover failed: HTTP ${replaced.status}`);
            } else if (!res.ok) {
                throw new Error(`lease create failed: HTTP ${res.status}`);
            }
            lastOkMs = Date.now();

            loop = listWatchLoop({
                client,
                labelSelector,
                target: {
                    replaceAll(items) {
                        leases.clear();
                        for (const item of items) leases.set(item.metadata.name, item);
                        materialize();
                    },
                    apply(type, lease) {
                        if (type === 'DELETED') leases.delete(lease.metadata.name);
                        else leases.set(lease.metadata.name, lease);
                        materialize();
                    }
                },
                backoff: {
                    minMs: options.watchBackoff?.minMs ?? 250,
                    maxMs: options.watchBackoff?.maxMs ?? 5_000
                }
            });
            loop.start();
            // Resolve once visible: an explicit LIST lands our own Lease in
            // the map even if the watch is still connecting.
            await loop.relist();

            beat = unref(setInterval(() => void renew().catch(onRenewFailure), heartbeatMs));
            // Freshness decays with the clock, not with events: the sweep
            // drops a peer that silently stopped renewing.
            sweep = unref(setInterval(materialize, heartbeatMs));
            if (relistMs > 0 && loop) {
                relist = unref(setInterval(() => void loop!.relist().catch(noop), relistMs));
            }
        },
        async setStatus(status) {
            if (!self) return;
            self = { ...self, status };
            descriptorDirty = true;
            // Immediate patch — drain must be visible now, not next beat.
            // On failure the dirty flag rides along on the next renewal.
            const res = drain(await client.patch(selfLease, renewPatch(Date.now(), self)));
            if (!res.ok) throw new Error(`lease status update failed: HTTP ${res.status}`);
            descriptorDirty = false;
            lastOkMs = Date.now();
            suspected = false;
        },
        async leave() {
            if (beat) clearInterval(beat);
            if (sweep) clearInterval(sweep);
            if (relist) clearInterval(relist);
            beat = sweep = relist = null;
            loop?.stop();
            loop = null;
            if (!self) return;
            const name = selfLease;
            self = null;
            drain(await client.delete(name));
        },
        view: () => cached,
        async refresh() {
            if (loop) await loop.relist();
            return cached;
        },
        async isAlive(hostId) {
            const res = await client.get(leaseName(leasePrefix, hostId));
            if (res.status === 404) {
                drain(res);
                return false;
            }
            if (!res.ok) {
                drain(res);
                throw new Error(`lease read failed: HTTP ${res.status}`);
            }
            const lease = (await res.json()) as LeaseObject;
            return isFresh(lease, Date.now(), clockSkewMs);
        },
        onChange(cb) {
            changeCbs.add(cb);
            return () => changeCbs.delete(cb);
        },
        onSelfSuspect(cb) {
            suspectCbs.add(cb);
            return () => suspectCbs.delete(cb);
        }
    };
}
