# chat example

Actors inside a **real SignalX app** — SSR with `useActorState`, guards
that run on both transports, serverFns beside the actor endpoint,
hydration with no refetch, and `useActorState(…, { live: true })` keeping
every open tab live over ONE connection. Open two tabs and type in one.

It also shows the **topics projection pattern**: every room publishes to
the `room-activity` topic (`ctx.publish` in `room.actor.ts`), one singleton
`ActivityFeed` actor subscribes with a key mapping (`key: () => 'all'`,
`activity.actor.ts`) and folds the events into a bounded recent list, and
the page observes that projection live. Open `/room/other` in a second tab
and post — the "across all rooms" panel in the first tab updates, though
that page never heard of the other room.

Production-shaped: with `REDIS_URL` set the same app clusters — Redis
membership/directory (`redisCluster`), Redis actor state (`redisStorage`),
and a **dual listener**: the public port serves only SSR, assets,
serverFns and the actor endpoint (default same-origin policy, HMAC-signed
HttpOnly sessions), while health/ops/host mounts live on an internal port
for probes, peers and `kubectl port-forward` only.

## Dev (single node, fileStorage, no env)

```sh
pnpm install && pnpm build
pnpm --filter chat-example dev          # Vite dev server on :5173
```

## Prod, local (single node)

```sh
pnpm --filter chat-example build
pnpm --filter chat-example start        # :3000 public, :7311 internal
```

## Prod, local 2-node cluster

```sh
redis-server --daemonize yes
REDIS_URL=redis://localhost:6379 CLUSTER_SECRET=dev OPS_SECRET=dev \
  PORT=8080 INTERNAL_PORT=7311 pnpm --filter chat-example start:prod &
REDIS_URL=redis://localhost:6379 CLUSTER_SECRET=dev OPS_SECRET=dev \
  PORT=8081 INTERNAL_PORT=7312 pnpm --filter chat-example start:prod &
# browse :8080 and :8081 — same rooms, live across both
```

Sessions: the footer's sign-in calls a serverFn that mints an HMAC-signed
(`AUTH_SECRET`) HttpOnly cookie; the `requireUser` guard verifies it with
a timing-safe compare on every actor call and serverFn. Rooms are URLs:
`/r/<name>` (default `#general`).

## Runtime knobs

Every one of these defaults to the shipped behaviour, so an unconfigured
deployment behaves exactly as it did before they existed. They exist to be
**measured on a real cluster** — each changes the perf curve.

| env | default | what it does |
|---|---|---|
| `PLACEMENT` | `prefer-local` | `prefer-local`, `activation-count` or `random`. `prefer-local` is half of the locality pair with the ingress's `upstream-hash-by`; `activation-count` deliberately opposes it, steering new activations at the least-loaded host instead of the hashed one |
| `PLACEMENT_REFRESH_MS` | policy default (5000) | load-view refresh cadence for `activation-count` |
| `REBALANCE` | off | `1` runs one `placement.rebalance()` round per interval — each host sheds only its OWN idlest activations, only down to the cluster mean |
| `REBALANCE_INTERVAL_MS` / `_THRESHOLD` / `_MIN_IDLE_MS` / `_MAX_MOVES` | 60000 / 1.2 / 60000 / 10 | the round's bounds |
| `MAX_ACTIVATIONS` | `0` (unlimited) | soft LRU cap per host. Rides the sweeper, so a cap with `SWEEP_INTERVAL_MS=0` is inert. Soft: busy, queued and kept-alive activations are never shed, and a shed room re-activates with its state intact |
| `SWEEP_INTERVAL_MS` | 60000 | how often the idle + capacity passes run |
| `DIGEST_MAX_LOCAL` | runtime default | pool members per key for the `Digest` worker — unset means `hardwareConcurrency` clamped to 16, which is the interesting case |
| `DIGEST_ITERS` / `DIGEST_MAX_ITERS` | 2000 / 200000 | default and ceiling for the per-call hash chain |
| `MIGRATE_PERSIST` | `lazy` | `Room`'s `migrateState` write-back. Lazy rides the next save the room would have made anyway (so a rolling deploy adds no write amplification, and a room only ever READ never persists its migration); `eager` buys one CAS write-back at activation |

`Digest` (`digest.actor.ts`) is a `defineWorker` pool — pure compute, many
interchangeable members per key, so two calls to the SAME key overlap —
next to `DigestActor`, the identical body on `defineActor` for contrast.
The work is chunked with a yield between slices, which is the load-bearing
detail: a pool gives a key many activations, not many threads, so one
unbroken synchronous loop overlaps with nothing.

## Image + AKS

```sh
TAG=$(git rev-parse --short HEAD)
az acr build --registry <acr> --image sigx-chat:$TAG \
  --platform linux/amd64 --file examples/chat/Dockerfile .
```
