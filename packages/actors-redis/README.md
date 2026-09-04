# @sigx/actors-redis

Redis providers for [`@sigx/actors`](https://sigx.dev/actors) — the cluster
state a multi-host deployment needs, plus the first cluster-safe persistence
option.

- **`redisCluster()`** — membership and the actor directory, bundled.
- **`redisMembership()`** — TTL-heartbeat host liveness, with pub/sub change
  push and a poll fallback.
- **`redisDirectory()`** — the single-activation claim directory.
- **`redisStorage()`** — `ActorStorage` with etag compare-and-set.
- **`redisReminders()`** — durable reminders on a due-time index: O(log N)
  to arm, O(due) to tick, at any number of sleeping actors.

```sh
pnpm add @sigx/actors-redis ioredis
```

Requires **Redis ≥ 7**. [`ioredis`](https://github.com/redis/ioredis) (≥ 5) is
a peer dependency, as is `@sigx/actors` itself. One client can be shared across
`redisCluster()`, `redisStorage()` and `redisReminders()`.

## Documentation

**https://sigx.dev/actors/packages/actors-redis/overview/**

Clustering guide: https://sigx.dev/actors/docs/clustering/ ·
Storage seam: https://sigx.dev/actors/docs/storage/

Source, examples and architecture notes:
https://github.com/signalxjs/actors

MIT © Andreas Ekdahl
