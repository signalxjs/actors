# Changelog

## [Unreleased]

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
