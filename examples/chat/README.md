# chat example

Actors inside a **real SignalX app** — SSR with `useActorState`, guards
that run on both transports, serverFns beside the actor endpoint,
hydration with no refetch, and a `ctx.changes()` stream keeping every open
tab live. Open two tabs and type in one.

Production-shaped: with `REDIS_URL` set the same app clusters — Redis
membership/directory (`redisCluster`), Redis actor state (`redisStorage`),
and a **dual listener**: the public port serves only SSR, assets,
serverFns and the actor endpoint (default same-origin policy, HMAC-signed
HttpOnly sessions), while health/ops/silo mounts live on an internal port
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

## Image + AKS

```sh
TAG=$(git rev-parse --short HEAD)
az acr build --registry <acr> --image sigx-chat:$TAG \
  --platform linux/amd64 --file examples/chat/Dockerfile .
```

The chart, the public ingress (NDJSON streaming annotations, sealed
internal mounts) and the outside-world test matrix live in
[`deploy/`](deploy/) — scenario (l) of the
[AKS runbook](../aks-cluster/deploy/RUNBOOK.md).
