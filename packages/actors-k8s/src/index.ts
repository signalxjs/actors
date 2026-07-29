/**
 * @sigx/actors-k8s — Kubernetes membership provider for `@sigx/actors/cluster`.
 *
 * Silo liveness rides a coordination.k8s.io Lease per silo; peers are
 * discovered through a label-selected Lease watch. No client library — the
 * API server is plain HTTPS + JSON. The actor directory is a separate seam
 * and stays store-backed:
 *
 * ```ts
 * clusterPlacement({
 *     membership: k8sMembership({ clusterName: 'orders' }),
 *     directory: redisDirectory(client),
 *     advertise, secret
 * })
 * ```
 */
export { k8sMembership, type K8sMembershipOptions } from './membership';
export { kubeClient, type KubeClient, type KubeClientOptions } from './client';
export { CLUSTER_LABEL, DESCRIPTOR_ANNOTATION } from './lease';
