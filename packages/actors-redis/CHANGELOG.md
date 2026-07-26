# Changelog

## [Unreleased]

### Added

- Initial release of `@sigx/actors-redis` — Redis (≥ 7) cluster providers
  for `@sigx/actors/cluster`: `redisCluster({ client | url })` bundling
  `redisMembership` (TTL-heartbeat liveness with self-suspect), `redisDirectory`
  (SET NX GET claims, Lua compare-and-delete release/evict), and
  `redisReminderLease` (Lua acquire-or-renew leader lease). ioredis ≥ 5 as
  a peer dependency.
