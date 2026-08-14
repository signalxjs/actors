# perf/aks — the AKS scale-out harness

A production-shaped `@sigx/actors` deployment: N identical host pods on
Kubernetes, cluster state in Redis, one container image that runs the host
(`server.mjs`), the closed-loop HTTP load generator (`loadgen.mjs`) and the
WebSocket connection-scale generator (`ws-loadgen.mjs`).
This is the app half of the AKS scale-out/perf test; the Helm chart lives
in [`deploy/chart/`](deploy/chart/) and the Azure setup plus the full
scenario runbook in [`deploy/RUNBOOK.md`](deploy/RUNBOOK.md).

> **This is a rig, not an example.** It exists to be measured and to be
> broken on purpose, so it optimises for knobs and instrumentation rather
> than for being read. If you are looking for something to copy, start
> from [`examples/`](../../examples/) instead — see [`../README.md`](../README.md).

Nothing is hardcoded — both entries read their entire configuration from
environment variables (documented in each file's header). The host:

- `cluster()` with `MEMBERSHIP=redis` (`redisCluster`) or `MEMBERSHIP=k8s`
  (`k8sMembership` Leases + `redisDirectory`) — same image, values toggle
- `redisStorage` for actor state — the etag CAS makes hard-kill failover
  recover committed state, which the runbook's verify scenario proves
- `metrics()` + `health()` (k8s probes) + `ops()` (the `sigx actors top`
  endpoint), a bounded undici pool for host-to-host fetch, listen-before-
  start ordering, and `attachSignalHandlers` for drain-on-SIGTERM

## Local run (no Kubernetes)

```sh
pnpm install && pnpm build
redis-server --daemonize yes            # or any Redis >= 7 on :6379

REDIS_URL=redis://localhost:6379 CLUSTER_SECRET=dev OPS_SECRET=dev \
  pnpm --filter sigx-perf-aks start

# second terminal — a 10s smoke load, one JSON summary line on stdout
TARGET_URL=http://127.0.0.1:7311 DURATION_S=10 CONCURRENCY=8 \
  pnpm --filter sigx-perf-aks loadgen
```

### WebSocket connection scale, with no Redis at all

`server.mjs` is a cluster member and requires Redis. `ws-dev.mjs` is the
single-host entry for the socket workload, so the whole delivery path can
be checked on a laptop:

```sh
node perf/aks/ws-dev.mjs                # host + /_sigx/socket on :7311

# second terminal — 5000 subscribers on one actor, 10 publishes/s
TARGET_URL=http://127.0.0.1:7311 MODE=hot CONNECTIONS=5000 \
  node perf/aks/ws-loadgen.mjs
```

One JSON line per rung. `connected` is the headline; `deliveries` is how
many of those clients actually received a message; `deliveriesPerPublish`
is a coalescing ratio rather than a constant, because every client
subscription runs at the runtime's fixed 50 ms watch throttle. Cross-check
`connected` against `ops.sockets.open`:

```sh
curl -s -H 'authorization: Bearer dev-ops-secret' \
  http://127.0.0.1:7311/_sigx/ops | jq .ops.sockets
```

Measured on one laptop running BOTH ends: 10 000 connections dialed in
987 ms with zero failures, 10 000 deliveries per publish, and
`maxBufferedBytes` flat at 0. Set `READ=mine PRINCIPAL=per-user` for the
arm that matters most — a read consulting `ctx.principal` gets one watch
loop per identity, which at 200 subscribers is 6200 actor turns instead of
31. See scenario (q) in the runbook.

Multiple local hosts: run more instances with the same `REDIS_URL` and a
different `PORT` — `POD_IP` defaults to `127.0.0.1`, so they find each
other through Redis exactly as the pods do.

### How to profile the fan-out path (#245)

`ws-dev.mjs` prints one `[ws-dev] load` JSON line per `SAMPLE_MS` window
(default 5 s) carrying `cpu` — a fraction of ONE core, so ~1.0 is the
saturation line for the `limits.cpu: 1000m` shape this rig stands in for —
plus event-loop-delay percentiles and RSS.

**Read the utilisation before the profile.** A delivery rate divided by a
host count is a throughput reciprocal; it bounds per-delivery CPU only if
the host was saturated. A profile taken on an unsaturated host says where
a *minority* of the wall clock went, and every share in it is a wrong
attribution.

```sh
node --conditions=production --enable-source-maps \
     --cpu-prof --cpu-prof-dir=./profiles --cpu-prof-interval=200 \
     perf/aks/ws-dev.mjs

# second terminal, one arm at a time
TARGET_URL=http://127.0.0.1:7311 MODE=hot CONNECTIONS=2000 \
PUBLISH_RATE=20 DURATION_S=120 PAYLOAD_BYTES=0 \
  node perf/aks/ws-loadgen.mjs
```

`--conditions=production` is load-bearing: it selects `dist/*.prod.js`, so
`__DEV__` is stripped — and `__DEV__` adds a whole second encode walk.
Stop the host with Ctrl-C so `attachSignalHandlers` exits cleanly, then
check the `.cpuprofile` actually landed before trusting the run.

`PUBLISH_RATE=20` is at least one publish per 50 ms throttle window, so
deliveries/s is `20 × CONNECTIONS` whatever the publisher does — the arms
then differ in one variable instead of two.

| arm | knob | separates |
|---|---|---|
| baseline | `PAYLOAD_BYTES=0` | the fixed per-delivery cost |
| bytes | `PAYLOAD_BYTES=0,256,4096,16384` | fit `1/rate = a + b·bytes` |
| ping off | `SOCKET_PING_MS=0` | the per-frame `armPing` re-arm |
| multiplexed | `CONNECTIONS=250 SUBS_PER_CONN=8` | the per-SOCKET cost from the per-subscription one, at a constant 2 000 subscriptions |
| idle | `MODE=idle` | what holding the sockets costs at zero traffic |
| dev dist | drop `--conditions=production` | the `__DEV__` second encode walk |

Keep the subscription count constant across the multiplexed pair or it
measures nothing: `CONNECTIONS × SUBS_PER_CONN` is the quantity the host
fans out to, and `CONNECTIONS` alone is the quantity it pays syscalls for.

Load the `.cpuprofile` in Chrome DevTools, select the steady-state range
(the ~1 s dial phase is not what you are measuring), and read **self
time** bucketed into: `@sigx/serialize` encode · native `JSON.stringify` ·
`socket-session.ts` + `node:internal/timers` · `ws` sender and `node:net` ·
`watch-core` promise scaffolding · GC. Those six buckets are what decides
which fix is worth building; the thresholds are recorded in the
`BASELINES.md` section this recipe produced.

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
  --platform linux/amd64 --file perf/aks/Dockerfile .
```

Multi-stage: `pnpm install` + `pnpm build` + `pnpm deploy --prod` produce a
self-contained tree; the runtime stage runs `node --conditions=production`
(the prod dist — `ops()` only enforces its bearer secret there) and proves
at build time that the deployed tree resolves the runtime packages.
