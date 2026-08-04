# @sigx/actors-redis

Redis-backed cluster providers for [`@sigx/actors`](../actors) — membership
(TTL heartbeats), the distributed actor directory (claim CAS), and
`redisStorage`, an `ActorStorage` with etag compare-and-set.
Requires **Redis ≥ 7**; [`ioredis`](https://github.com/redis/ioredis)
(≥ 5) is a peer dependency.

```ts
import { createHost } from '@sigx/actors/host';
import { clusterPlacement } from '@sigx/actors/cluster';
import { redisCluster } from '@sigx/actors-redis';

const host = createHost({
    actors,
    storage,
    placement: clusterPlacement({
        ...redisCluster({ url: process.env.REDIS_URL }),
        advertise: 'http://10.0.4.7:7311',
        secret: process.env.HOST_SECRET
    })
});
```

Pass an existing `ioredis` client instead of `url` to share the app's
connection: `redisCluster({ client })`. Call `redisCluster()` once per host
— each call returns that host's own membership handle (the directory is
safely shared).

## Options

| Option | Default | Meaning |
|---|---|---|
| `client` / `url` | — | ioredis client, or a URL to construct one |
| `namespace` | `sigx` | key prefix |
| `heartbeatMs` | `5000` | membership heartbeat cadence |
| `ttlMs` | `15000` | heartbeat key TTL — missed beats past this = dead |
| `pollMs` | `5000` | membership view poll cadence |
| `coalesceMs` | `0` | trailing quiet window for coalescing membership pushes — the subscriber is single-flight either way (a burst of N changes costs one refresh plus at most one catch-up, not N); a non-zero window widens the net at the price of that much staleness |

## Key layout

```
{ns}:host:{hostId}    HASH {d: descriptor JSON}   PX ttlMs, renewed each beat
{ns}:hosts            SET of hostIds              lazily pruned
{ns}:mver             INCR'd version counter      cheap view-poll compare
{ns}:dir:{actorId}    "hostId\nactivationId"      no TTL (validity = owner liveness)
{ns}:membership       pub/sub channel             change push (poll = fallback)
```

A host that cannot heartbeat past `ttlMs` self-fences (stops claiming
actors, deactivates what it holds); directory entries of dead hosts are
evicted lazily on lookup. State integrity never rests on any of this — the
actor runtime's storage etag CAS is the floor.

Individual providers are exported too: `redisMembership(client, opts)`
and `redisDirectory(client, opts)`.

## Storage

`redisStorage()` persists actor state in the same Redis — the first
cluster-safe `ActorStorage` (`memoryStorage` dies with the process,
`fileStorage` is single-process dev-only):

```ts
import { defineActorApp } from '@sigx/actors/host';
import { redisStorage } from '@sigx/actors-redis';

const app = defineActorApp({
    storage: redisStorage({ url: process.env.REDIS_URL, namespace: 'sigx' })
});
```

| Option | Default | Meaning |
|---|---|---|
| `client` / `url` | — | ioredis client, or a URL to construct one |
| `namespace` | `sigx` | key prefix (share it with the cluster providers) |

One HASH per actor, no TTL. The record key joins actor type and key with
a NUL byte — the same actorId shape the directory uses (Redis keys are
binary-safe; a NUL renders invisibly in `redis-cli SCAN` output):

```
{ns}:st:{type}\x00{key}     HASH {e: etag, s: state JSON}
```

Etags are client-minted UUIDs; `save`/`clear` compare-and-set atomically
in Redis (Lua, registered as `EVALSHA` commands — `save` is the hot path)
and throw the branded `ActorStorageConflict` on mismatch, which the
runtime turns into discard-and-reload. That CAS is the cluster's
integrity floor: a briefly-wrong directory entry costs a rejected save,
never corruption. Reminders ride the same storage automatically.

Sharing one `ioredis` client across `redisCluster()` and `redisStorage()`
is safe and saves connections.

## Tests

The provider suite is gated on `REDIS_URL`:

```sh
REDIS_URL=redis://localhost:6379 pnpm test actors-redis
```
