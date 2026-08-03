# Changelog

## [Unreleased]

### Changed

- **silo → host** (#233): types and options follow the renamed
  `@sigx/actors` seams (`HostDescriptor`, membership view `hosts`).
  Lease naming (`{leasePrefix}-{id}`) and the `sigx.dev/*` annotations
  are unchanged.

### Added

- **Initial release** (#37): `k8sMembership()` — the `ClusterMembership`
  seam implemented on Kubernetes primitives. Each host owns one
  `coordination.k8s.io/v1` Lease (renewed on the heartbeat cadence, the
  descriptor riding an annotation) and watches its peers' Leases through a
  labelSelector, so a cluster inside Kubernetes needs no Redis for
  liveness. The actor directory stays store-backed and composes freely:
  `{ membership: k8sMembership(...), directory: redisDirectory(client) }`.
