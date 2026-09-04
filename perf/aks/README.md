# perf/aks — the AKS scale-out harness

A production-shaped `@sigx/actors` deployment: N identical host pods on
Kubernetes, cluster state in Redis, one container image that runs the host
(`server.mjs`), the closed-loop HTTP load generator (`loadgen.mjs` — which
also carries the open-loop **workflow-engine** workload, `MODE=workflow`)
and the WebSocket connection-scale generator (`ws-loadgen.mjs`).
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
is a coalescing ratio rather than a constant, because this generator sends
no `w` (#247) and so every subscription it opens runs at the runtime's
default 50 ms watch throttle. Cross-check
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

### The workflow engine, with no Redis at all (#297)

`src/workflow/` is a small but realistic workflow engine built on the
runtime — the headless Tier-3 workload #85 asked for. One `WorkflowRun`
actor per run, event-driven (no `tasks:`): every event — start, signal,
child done, timer, reminder — is a turn that records, saves and arms an
`advance` tick, which drives the run from its cursor until it blocks.
Node types: `task` (on a `defineWorker` pool, compute or io), `delay`
(a volatile timer under `WF_TIMER_THRESHOLD_MS`, a durable reminder above
it — the run leaves memory and comes back on a tick), `branch`, `parallel`,
`fanout`/`subworkflow` (child RUNS with an idempotent durable join, or pool
tasks), `wait` (an external signal with a timeout edge), `end`; retries
with backoff; saga compensation walked backwards; every completion
published on a topic to a singleton `WorkflowStats` aggregator. Four
templates (`order`, `approval`, `etl`, `saga`) exercise one axis each.

```sh
node perf/aks/wf-dev.mjs                # single host, memory storage, :7311

# second terminal — 20 runs/s of open-loop arrivals for 20 s
TARGET_URL=http://127.0.0.1:7311 MODE=workflow WF_START_RATE=20 \
  DURATION_S=20 WF_DELAY_MS=1500 node perf/aks/loadgen.mjs
```

One JSON line per rate rung: runs started/completed per second, per-
template latency percentiles (host-stamped), per-node-type durations, the
delay-node **wake lag**, timers vs reminders fired, lost wakes, signals
delivered/buffered/late, child runs, compensations — and `stuck`, which
must be all zero for the exit code to be 0. The engine's mechanism
counters ride `ops()` as the `workflow` section; `deploy/wf-load.mjs`
sums them across pods. Every host knob (`WF_*`) is documented in
`src/workflow/config.ts` and is part of the `wf` INFRA_SHAPE. See scenario
(t) in the runbook.

Multiple local hosts: run more instances with the same `REDIS_URL` and a
different `PORT` — `POD_IP` defaults to `127.0.0.1`, so they find each
other through Redis exactly as the pods do. `wf-fleet.mjs` does exactly
that for you (#381): N copies of `server.mjs` on one box, the real load
generator pointed at them through a round-robin proxy (the Service's
stand-in), the `ops.workflow` counters read before and after, and each
process's CPU sampled — the multi-core rig, and the local rung of every
scaling question before it costs a paid session.

```sh
redis-server &                                          # or any local Redis
node --conditions=production perf/aks/wf-fleet.mjs hosts=4 rate=100 durationS=20 \
  WF_DELAY_MS=2000                                      # one JSON line per rung
REDIS_URL=redis://127.0.0.1:6379 pnpm bench:wf-local   # the recorded wf-local/* scenarios
```

One generator, not one per host, on purpose: every generator resets the
`WorkflowStats` ring when it seeds, so N of them starting together wipe
each other's completions (#380 makes that index-0-only). The generator's
own CPU is in the row, so a rung it bottlenecks says so. Nothing here is
`exact` and every timing is informational (N processes share the cores);
the number to read is the RATIO between fleet sizes measured back to back
on one box — see the tier legend in `benchmarks/BASELINES.md`.

Several generator pods (`parallelism`) run the same rung each; the rows
carry `offeredRate = rate × pods`. The chart's loadgen Job is **Indexed**,
which is what makes that safe: the generator reads `JOB_COMPLETION_INDEX`
and `JOB_NAME`, index 0 resets the aggregator and seeds, the rest wait for
its seed marker (`WF_SEED_WAIT_MS`, default 180 s). A non-indexed Job with
`parallelism > 1` has no index, so every pod would seed and reset — the
bug this exists to prevent. Locally, with neither set, a generator seeds
as it always did.

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
(CPU-bound sha256 chains), `mixed`, `verify` (reads every counter back;
compare against the `acked` counts of earlier runs — state loss shows as
`actual < acked`), `jobs` (durable `SweepJob`s to kill hosts under) or
`workflow` (the engine above — open-loop, `SWEEP` is a list of RATE rungs). `SWEEP=1,2,4,8,...` walks a concurrency ladder and
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
