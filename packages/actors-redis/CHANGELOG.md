# Changelog

## [Unreleased]

### Changed

- **Membership pushes are coalesced** (#26): the pub/sub subscriber is now
  single-flight — a burst of N membership changes costs one refresh plus at
  most one trailing catch-up instead of N full member-list re-reads, and a
  message whose published version the view has already caught up past costs
  nothing. This was the measured O(N²): ~10 400 commands cluster-wide for
  one join at n=100. New `coalesceMs` option (default 0) widens the
  coalescing window; `refresh()` keeps its contract (resolves with a
  refresh that started at-or-after the call).

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
