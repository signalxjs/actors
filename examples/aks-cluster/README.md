# aks-cluster example

A production-shaped `@sigx/actors` deployment: N identical silo pods on
Kubernetes, cluster state in Redis, one container image that runs both the
silo (`server.mjs`) and the closed-loop load generator (`loadgen.mjs`).
This is the app half of the AKS scale-out/perf test; the Helm chart, the
Azure setup and the full scenario runbook land in `deploy/` with
[#129](https://github.com/andtii/actors/issues/129).

Nothing is hardcoded — both entries read their entire configuration from
environment variables (documented in each file's header). The silo:

- `cluster()` with `MEMBERSHIP=redis` (`redisCluster`) or `MEMBERSHIP=k8s`
  (`k8sMembership` Leases + `redisDirectory`) — same image, values toggle
- `redisStorage` for actor state — the etag CAS makes hard-kill failover
  recover committed state, which the runbook's verify scenario proves
- `metrics()` + `health()` (k8s probes) + `ops()` (the `sigx actors top`
  endpoint), a bounded undici pool for silo-to-silo fetch, listen-before-
  start ordering, and `attachSignalHandlers` for drain-on-SIGTERM

## Local run (no Kubernetes)

```sh
pnpm install && pnpm build
redis-server --daemonize yes            # or any Redis >= 7 on :6379

REDIS_URL=redis://localhost:6379 CLUSTER_SECRET=dev OPS_SECRET=dev \
  pnpm --filter aks-cluster-example start

# second terminal — a 10s smoke load, one JSON summary line on stdout
TARGET_URL=http://127.0.0.1:7311 DURATION_S=10 CONCURRENCY=8 \
  pnpm --filter aks-cluster-example loadgen
```

Multiple local silos: run more instances with the same `REDIS_URL` and a
different `PORT` — `POD_IP` defaults to `127.0.0.1`, so they find each
other through Redis exactly as the pods do.

## Load generator

`MODE=counter` (state churn — every call is a Redis CAS), `crunch`
(CPU-bound sha256 chains), `mixed`, or `verify` (reads every counter back;
compare against the `acked` counts of earlier runs — state loss shows as
`actual < acked`). `SWEEP=1,2,4,8,...` walks a concurrency ladder and
emits one JSON line per rung. Progress and error windows go to stderr;
stdout carries only the JSON summaries, so
`kubectl logs job/<name> | jq -s` is the whole result pipeline.

## Image

```sh
TAG=$(git rev-parse --short HEAD)
az acr build --registry <acr> --image sigx-actors-test:$TAG \
  --platform linux/amd64 --file examples/aks-cluster/Dockerfile .
```

Multi-stage: `pnpm install` + `pnpm build` + `pnpm deploy --prod` produce a
self-contained tree; the runtime stage runs `node --conditions=production`
(the prod dist — `ops()` only enforces its bearer secret there) and proves
at build time that the deployed tree resolves the runtime packages.
