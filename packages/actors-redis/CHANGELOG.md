# Changelog

## [Unreleased]

## [0.5.0] - 2026-08-07

### Changed

- **Peers `@sigx/actors@^0.5.0`.** The family versions in lockstep, so the
  range moves with the release. 0.5.0 only ADDS `ctx.changes({ throttleMs })`
  and removes a snapshot a `$live` watch never read (#129) — no wire or API
  break, so a 0.4.x host interoperates fine.

## [0.4.0] - 2026-08-07

### Changed

- **Peers `@sigx/actors@^0.4.0`.** The family versions in lockstep, so the
  range moves with the release. Nothing else to do: 0.4.0 only ADDS
  `onSettled` to `defineJob` (#125), so unlike the 0.2.0 and 0.3.0 moves
  there is no wire or API break and a 0.3.x host interoperates fine.

- README trimmed to a pointer at https://sigx.dev/actors (#113): thesis,
  install, peer-dependency and minimum-version requirements, and links. The
  reference material is on the docs site; relative links (which npm does not
  resolve) are gone. No code or API change.

## [0.3.0] - 2026-08-05

### Fixed

- **A stalled host kept serving actors a survivor had taken over (#45).**
  Two bugs, both here:

  - Self-suspicion fired only from a heartbeat write *rejection*. A host
    whose event loop stalled past `ttlMs` resumed, wrote late and
    succeeded, so it never fenced — while peers had already expired it and
    released its directory claims. The beat now runs on the shared
    `heartbeatClock()`: a beat starting more than `ttlMs` after the last
    confirmed write fires `onSelfSuspect` before it writes.
  - The heartbeat never re-`sadd`ed `{ns}:hosts`. Since `refresh` lazily
    `srem`s a member whose host key expired, a host pruned while it was
    away kept heartbeating into a set it was no longer in — invisible to
    every peer's view forever, while `isAlive` (which reads the host key it
    kept recreating) still answered `true`. `SADD` now rides the heartbeat
    MULTI.

- `refresh` now compares a host **signature**, matching the pg and surreal
  providers. A host rejoining the set writes no version bump, so `onChange`
  would have stayed silent while the cached view gained a member — leaving
  transports unnotified and the departed-host sweep un-run.

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

- **Membership pushes are coalesced** (#26): the pub/sub subscriber is now
  single-flight — a burst of N membership changes costs one refresh plus at
  most one trailing catch-up instead of N full member-list re-reads, and a
  message whose published version the view has already caught up past costs
  nothing. This was the measured O(N²): ~10 400 commands cluster-wide for
  one join at n=100. New `coalesceMs` option (default 0) widens the
  coalescing window; `refresh()` keeps its contract (resolves with a
  refresh that started at-or-after the call).

## [0.1.0] - 2026-08-03

### Changed

- **silo → host** (#233): membership registry keys moved from
  `{ns}:silos` / `{ns}:silo:{id}` to `{ns}:hosts` / `{ns}:host:{id}`,
  and exported types follow `@sigx/actors` (`HostDescriptor`, …).
  Existing dev data is not migrated. The directory key format
  (`{ns}:dir:{actorId}`) is unchanged.

### Added

- `redisStorage({ client | url, namespace })` — a Redis-backed
  `ActorStorage` with atomic etag compare-and-set (Lua/`EVALSHA`): one
  HASH per actor under `{ns}:st:`, client-minted UUID etags, branded
  `ActorStorageConflict` on mismatch. The first cluster-safe storage
  provider; reminders ride it automatically.

- Membership changes push over a `{ns}:membership` pub/sub channel (via a
  duplicated subscriber connection): views converge in sub-second time
  instead of waiting out the poll interval; the poll remains the safety
  net for missed messages.

- `redisDirectory` implements `evictHost(hostId)` — a cursor `SCAN` over
  the directory prefix with a per-key owner-checked Lua delete (assumes a
  single logical Redis; on Redis Cluster run per node).

- Initial release of `@sigx/actors-redis` — Redis (≥ 7) cluster providers
  for `@sigx/actors/cluster`: `redisCluster({ client | url })` bundling
  `redisMembership` (TTL-heartbeat liveness with self-suspect) and
  `redisDirectory` (SET NX GET claims, Lua compare-and-delete
  release/evict). ioredis ≥ 5 as a peer dependency.
