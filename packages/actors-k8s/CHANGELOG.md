# Changelog

## [Unreleased]

### Changed

- README trimmed to a pointer at https://sigx.dev/actors (#113): thesis,
  install, peer-dependency and minimum-version requirements, and links. The
  reference material is on the docs site; relative links (which npm does not
  resolve) are gone. No code or API change.

## [0.3.0] - 2026-08-05

### Fixed

- **A deleted Lease was silently recreated instead of fencing the host
  (#69).** The Lease is this host's membership token. If something removes
  it — operator cleanup, namespace churn — peers have already aged the host
  out of their views, `evictHost` has released every directory claim it
  held, and a survivor may be serving those actors. `renew()` answered a
  404 by recreating the Lease, which re-advertised the host as healthy while
  its claims were forfeit: the single-activation violation of #45 reached by
  another route, and one no elapsed-time check can catch, because the
  recreate succeeds promptly.

  A 404 now fences (via the new `heartbeatClock().lost()` — proof that
  membership is gone, rather than a suspicion inferred from timing) and the
  Lease is *not* recreated. Fenced stays terminal: liveness fails and the
  restart mints a fresh identity that rejoins cleanly.

  A 404 caused by this host's own `leave()` racing an in-flight renewal is
  a graceful exit, not lost membership, and does not fence.

- **A stalled host kept serving actors a survivor had taken over (#45).**
  Self-suspicion fired only from a Lease renewal *failure*, so a host whose
  event loop stalled past `ttlMs` resumed, patched `renewTime` late and
  succeeded — looking healthy again while peers had already aged its Lease
  out and released its directory claims. Renewals now run on the shared
  `heartbeatClock()`: a beat starting more than `ttlMs` after the last
  confirmed renewal fires `onSelfSuspect` before it patches. The window is
  stamped when the beat is armed rather than at the Lease create, so the
  LIST between them cannot fence a host at startup.

### Changed

- **Peers `@sigx/actors@^0.3.0`.** The actor URL grammar is breaking
  (#96), so the whole family moves together — see the `@sigx/actors`
  changelog. **Upgrade every host before any client or peer starts
  emitting**: a host still on 0.2.x refuses calls from an upgraded one.

## [0.2.0] - 2026-08-05

### Changed

- **Peers `@sigx/actors@^0.2.0`.** The guard split is breaking, so the
  whole family moves together — see the `@sigx/actors` changelog and core's
  [0.15 migration guide](https://github.com/signalxjs/core/blob/main/docs/migrations/0.15-guard-split.md).
  Actors, workers and jobs defined against this package declare access with
  `authorize` / `methodAuthorize` / `allowAnonymous` now, and the runtime is
  fail-closed: one that declares nothing, in a process with no server app,
  denies with 401.

## [0.1.0] - 2026-08-03

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
