/**
 * Lease ↔ HostDescriptor mapping. One Lease per host: `spec.renewTime` is
 * the heartbeat, `spec.leaseDurationSeconds` the declared TTL, and the full
 * descriptor rides an annotation — Leases are the one Kubernetes object
 * whose whole job is exactly this (kubelet node heartbeats), so liveness
 * and discovery need no second object kind.
 */
import type { HostDescriptor } from '@sigx/actors/cluster';

export const DESCRIPTOR_ANNOTATION = 'sigx.dev/descriptor';
export const CLUSTER_LABEL = 'sigx.dev/cluster';

export interface LeaseObject {
    apiVersion?: string;
    kind?: string;
    metadata: {
        name: string;
        namespace?: string;
        labels?: Record<string, string>;
        annotations?: Record<string, string>;
        resourceVersion?: string;
    };
    spec: {
        holderIdentity?: string;
        leaseDurationSeconds?: number;
        acquireTime?: string;
        renewTime?: string;
    };
}

export function leaseName(prefix: string, hostId: string): string {
    return `${prefix}-${hostId}`;
}

/** RFC 3339 with microseconds — the wire form of k8s MicroTime. */
export function microTime(ms: number): string {
    return new Date(ms).toISOString().replace('Z', '000Z');
}

export function buildLease(
    self: HostDescriptor,
    options: {
        name: string;
        namespace: string;
        labels: Record<string, string>;
        ttlMs: number;
        nowMs: number;
    }
): LeaseObject {
    return {
        apiVersion: 'coordination.k8s.io/v1',
        kind: 'Lease',
        metadata: {
            name: options.name,
            namespace: options.namespace,
            labels: options.labels,
            annotations: { [DESCRIPTOR_ANNOTATION]: JSON.stringify(self) }
        },
        spec: {
            holderIdentity: self.hostId,
            leaseDurationSeconds: Math.ceil(options.ttlMs / 1000),
            acquireTime: microTime(options.nowMs),
            renewTime: microTime(options.nowMs)
        }
    };
}

/**
 * The heartbeat: a merge-patch touching only `renewTime` — no
 * read-modify-write, no resourceVersion race. When the descriptor changed
 * (status flip on drain), the annotation goes along in the same patch.
 */
export interface LeasePatch {
    metadata?: { annotations: Record<string, string> };
    spec: { renewTime: string };
}

export function renewPatch(nowMs: number, descriptor?: HostDescriptor): LeasePatch {
    const patch: LeasePatch = { spec: { renewTime: microTime(nowMs) } };
    if (descriptor) {
        patch.metadata = {
            annotations: { [DESCRIPTOR_ANNOTATION]: JSON.stringify(descriptor) }
        };
    }
    return patch;
}

const STATUSES = new Set(['joining', 'active', 'leaving']);

/**
 * Parse a peer's descriptor off its Lease; null for foreign/malformed
 * leases. The full `HostDescriptor` shape is enforced — anything matching
 * the labelSelector lands in the membership view, and a half-formed
 * descriptor (missing epoch, unknown status) would violate what placement
 * assumes about every view entry.
 */
export function parseLease(lease: LeaseObject): HostDescriptor | null {
    const raw = lease.metadata?.annotations?.[DESCRIPTOR_ANNOTATION];
    if (!raw) return null;
    try {
        const descriptor = JSON.parse(raw) as HostDescriptor;
        if (
            typeof descriptor.hostId !== 'string' ||
            !descriptor.hostId ||
            typeof descriptor.address !== 'string' ||
            typeof descriptor.epoch !== 'number' ||
            !Number.isFinite(descriptor.epoch) ||
            !STATUSES.has(descriptor.status)
        ) {
            return null;
        }
        return descriptor;
    } catch {
        return null;
    }
}

/**
 * Liveness: renewTime within the lease's OWN declared duration (peers may
 * run different TTLs), plus a skew allowance — renewTime is written by the
 * peer's clock and compared against ours. NTP-synced nodes are assumed,
 * exactly as kubelet leases assume.
 */
export function isFresh(lease: LeaseObject, nowMs: number, clockSkewMs: number): boolean {
    const renewed = lease.spec?.renewTime ? Date.parse(lease.spec.renewTime) : NaN;
    const durationMs = (lease.spec?.leaseDurationSeconds ?? 0) * 1000;
    if (!Number.isFinite(renewed) || durationMs <= 0) return false;
    return nowMs - renewed <= durationMs + clockSkewMs;
}
