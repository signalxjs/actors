# @sigx/actors-redis

Redis-backed cluster providers for [`@sigx/actors`](../actors) — membership
(TTL heartbeats) and the distributed actor directory (claim CAS).
Requires **Redis ≥ 7**; [`ioredis`](https://github.com/redis/ioredis)
(≥ 5) is a peer dependency.

```ts
import { createSilo } from '@sigx/actors/silo';
import { clusterPlacement } from '@sigx/actors/cluster';
import { redisCluster } from '@sigx/actors-redis';

const silo = createSilo({
    actors,
    storage,
    placement: clusterPlacement({
        ...redisCluster({ url: process.env.REDIS_URL }),
        advertise: 'http://10.0.4.7:7311',
        secret: process.env.SILO_SECRET
    })
});
```

Pass an existing `ioredis` client instead of `url` to share the app's
connection: `redisCluster({ client })`. Call `redisCluster()` once per silo
— each call returns that silo's own membership handle (the directory is
safely shared).

## Options

| Option | Default | Meaning |
|---|---|---|
| `client` / `url` | — | ioredis client, or a URL to construct one |
| `namespace` | `sigx` | key prefix |
| `heartbeatMs` | `5000` | membership heartbeat cadence |
| `ttlMs` | `15000` | heartbeat key TTL — missed beats past this = dead |
| `pollMs` | `5000` | membership view poll cadence |

## Key layout

```
{ns}:silo:{siloId}    HASH {d: descriptor JSON}   PX ttlMs, renewed each beat
{ns}:silos            SET of siloIds              lazily pruned
{ns}:mver             INCR'd version counter      cheap view-poll compare
{ns}:dir:{actorId}    "siloId\nactivationId"      no TTL (validity = owner liveness)
{ns}:membership       pub/sub channel             change push (poll = fallback)
```

A silo that cannot heartbeat past `ttlMs` self-fences (stops claiming
actors, deactivates what it holds); directory entries of dead silos are
evicted lazily on lookup. State integrity never rests on any of this — the
actor runtime's storage etag CAS is the floor.

Individual providers are exported too: `redisMembership(client, opts)`
and `redisDirectory(client, opts)`.

## Tests

The provider suite is gated on `REDIS_URL`:

```sh
REDIS_URL=redis://localhost:6379 pnpm test -- actors-redis
```
