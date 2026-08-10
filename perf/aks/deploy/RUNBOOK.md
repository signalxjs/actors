# AKS test runbook

The full scale-out / perf / failure test for `@sigx/actors` on AKS, using
the [`sigx-actors-test` chart](chart/) and the
[`perf/aks` harness](../README.md).

Set once, used by every command below:

```sh
SUBSCRIPTION=<subscription>  # the Azure subscription holding the cluster
RG=<resource-group>          # the AKS cluster's resource group
CLUSTER=<aks-cluster-name>
ACR=<acr-name>               # attached to (or pull-permitted for) the cluster
NS=sigx-actors-test          # namespace
RELEASE=sigx                 # helm release; resources are $RELEASE-host etc.
# "loadgen JSON" = the one-line summary a load Job prints on stdout:
kubectl -n $NS logs job/<job> | tail -1 | jq
# "cluster snapshot" = the ops fan-out, via the port-forward from §2:
curl -s -H "Authorization: Bearer $OPS_SECRET" http://127.0.0.1:7311/_sigx/ops/cluster | jq
```

Keep `sigx actors top` (§2) running in a second terminal for **every**
scenario — that live view is scenario (j).

## 0. One-time setup

```sh
brew install helm                          # if missing
az account set --subscription $SUBSCRIPTION
az aks get-credentials -g $RG -n $CLUSTER

# Dedicated, tainted node pool — 4 nodes: 3 host + 1 redis (the chart's
# mutual required anti-affinity partitions them). Autoscale up to 8 for
# the HPA scenario; min 0 so idle sessions cost nothing.
az aks nodepool add \
  --resource-group $RG --cluster-name $CLUSTER \
  --name sigxactors --mode User \
  --node-vm-size Standard_D2ls_v6 \
  --node-count 4 \
  --enable-cluster-autoscaler --min-count 0 --max-count 8 \
  --labels workload=sigx-actors-test \
  --node-taints workload=sigx-actors-test:NoSchedule
# SKU fallback if D2ls_v6 is unavailable in your region: Standard_D2als_v6.

kubectl create namespace $NS

# The edge hash, half of the locality composition, needs ONE cluster-wide
# controller setting that no chart can make. ingress-nginx >= 1.12 rates
# `upstream-hash-by` as Critical risk (its value is an nginx variable) and
# defaults the annotation risk level to `High`, so it silently strips the
# value, logs nothing, and load-balances the actor path round-robin.
#
# It is a ConfigMap option, NOT a CLI flag: passing
# `--annotations-risk-level=Critical` as a controller arg crash-loops
# v1.15.1 with `unknown flag`. And a running controller does not re-read
# the key, so the restart is part of the step, not a precaution.
kubectl -n ingress-nginx patch configmap ingress-nginx-controller \
  -p '{"data":{"annotations-risk-level":"Critical"}}'
kubectl -n ingress-nginx rollout restart deploy/ingress-nginx-controller
kubectl -n ingress-nginx rollout status deploy/ingress-nginx-controller

# Verify it took, against the controller's PROGRAMMED backends — the
# Ingress object is not evidence, since the annotation survives on it
# either way. `upstreamHashByConfig` must contain `upstream-hash-by`; a
# bare `{"upstream-hash-by-subset-size":3}` means it is still being
# dropped. (`/dbg` is the controller image's own debug binary — it needs
# no curl in the container.)
kubectl -n ingress-nginx exec deploy/ingress-nginx-controller -- /dbg backends all \
  | jq '.[] | select(.name | test("host-actor")) | {name, upstreamHashByConfig, loadBalancing}'
```

Because this is cluster-wide, it applies to every Ingress the controller
serves — check that before flipping it on a shared cluster. The chart's
side of the same fix (a dedicated Service so the actor Ingress gets its own
backend) is in [`ingress.yaml`](../../app/deploy/chart/templates/ingress.yaml);
`infra.test.ts` asserts the result, so a missed step here fails the suite
rather than quietly reading as "locality does not help".

## 1. Build + deploy — one command

`testenv.mjs` owns the whole environment so a measurement is reproducible
and a teardown is not a checklist. Shape values (`POOL`, `POOL_SIZE`,
`LOAD_VM_SIZE`, namespaces, …) have generic defaults; identity values
(`RG`, `CLUSTER`, `ACR`, `LOCATION`, `CHAT_HOST`, `DNS_ZONE`, `DNS_RG`,
`LOAD_RG`, `LOAD_VM`) deliberately have none — they name YOUR Azure
estate, and this file is public. Export them once in your shell profile;
a verb fails fast naming exactly what it is missing. `up` is idempotent:

```sh
cd perf/aks/deploy
node testenv.mjs up       # node pool + both images + both releases + DNS
node testenv.mjs status   # replicas, how many NODES they span, VM, endpoint
node testenv.mjs load     # the edge ladder from a same-region VM
node testenv.mjs down     # releases, DNS, load VM, node pool — all of it
```

`status` prints the node spread deliberately: replicas sharing a node add
far less capacity than their count suggests (see #183), and that is
invisible in `kubectl get deploy`. It also flags the load VM as billable,
because an idle VM is the thing you forget.

`load` mints a signed session cookie from the release's own `authSecret`,
so the guarded endpoints are exercised the way a browser exercises them,
and passes anything you append as ladder env — e.g.
`node testenv.mjs load LADDER=64,128,256 MIX=0.2 DURATION_MS=60000`.
The generator itself is [`edge-ladder.mjs`](edge-ladder.mjs): forked
workers (one Node process is single-core for this shape), closed-loop, and
it mints the **routing token** exactly as the client library does — without
it an edge hash has nothing to hash and locality measurements silently
read as no-ops.

**Measure from the same region.** A laptop over the Atlantic reports
`concurrency ÷ RTT` and nothing about the deployment: 8 workers × 71 ms is
106 ops/s no matter how fast the cluster is.

## 1b. Build + deploy by hand

```sh
cd <repo root>
TAG=$(git rev-parse --short HEAD)
az acr build --registry $ACR \
  --image sigx-actors-test:$TAG \
  --platform linux/amd64 \
  --file perf/aks/Dockerfile .

helm install $RELEASE perf/aks/deploy/chart \
  -n $NS --set image.tag=$TAG
# upgrades: helm upgrade $RELEASE ... --reuse-values --set image.tag=$NEWTAG
```

Secrets are generated on first install and preserved across upgrades.

## 1c. Running from GitHub Actions

The `Cluster test` workflow (`.github/workflows/cluster-test.yml`) dispatches
any `testenv.mjs` verb from the Actions tab, on a plain `ubuntu-latest`
runner — every step is orchestration; the measurements happen in-cluster
(loadgen Jobs) and on the same-region load VM. The estate's identity comes
from Actions **secrets**, one per required env var, so GitHub masks each
value wherever it appears in a log:

| Secret | testenv env var |
|---|---|
| `TESTENV_RG` | `RG` |
| `TESTENV_CLUSTER` | `CLUSTER` |
| `TESTENV_ACR` | `ACR` |
| `TESTENV_LOCATION` | `LOCATION` |
| `TESTENV_DNS_ZONE` | `DNS_ZONE` |
| `TESTENV_DNS_RG` | `DNS_RG` |
| `TESTENV_CHAT_HOST` | `CHAT_HOST` |
| `TESTENV_LOAD_RG` | `LOAD_RG` |
| `TESTENV_LOAD_VM` | `LOAD_VM` |

Azure auth is the same OIDC federated identity the Bench workflow uses
(`AZURE_CLIENT_ID` / `AZURE_TENANT_ID` / `AZURE_SUBSCRIPTION_ID` — no
client secret exists to leak). Beyond what Bench needs, that identity
requires:

- **Azure Kubernetes Service Cluster User** + **Contributor** on the AKS
  cluster (get-credentials, node-pool add/delete);
- **AcrPush** plus `Microsoft.ContainerRegistry/registries/scheduleRun/action`
  on the registry (`az acr build`);
- **DNS Zone Contributor** on the zone;
- **Contributor scoped to the load-VM resource group**, pre-created once out
  of band — `az group create` on an existing RG is an idempotent PUT the
  scoped role permits, `down` can delete it, and `run-command` works, all
  without any subscription-scope rights.
- If the cluster is AAD-enabled with Azure RBAC: **Azure Kubernetes Service
  RBAC Cluster Admin** on the cluster. `testenv.mjs` converts the kubeconfig
  with `kubelogin -l azurecli` automatically when needed.

`down` is guarded by a typed confirmation input (it deletes the node pool,
the DNS record and the load-VM resource group), and the workflow's
concurrency group queues runs rather than overlapping them — the cluster is
one shared environment.

## 2. Monitoring from your own terminal

```sh
kubectl -n $NS port-forward svc/$RELEASE-host 7311:7311 &
OPS_SECRET=$(kubectl -n $NS get secret $RELEASE-secrets \
  -o jsonpath='{.data.opsSecret}' | base64 -d)

# The live dashboard — the ops fan-out reaches the whole cluster from
# whichever host the forward lands on. Run from this repo (the sigx CLI
# discovers its plugins from the project's dependencies):
pnpm --filter sigx-perf-aks exec sigx actors top \
  --url http://127.0.0.1:7311 --secret "$OPS_SECRET"

# One-shot snapshots for the record:
pnpm --filter sigx-perf-aks exec sigx actors stats \
  --url http://127.0.0.1:7311 --secret "$OPS_SECRET" --json
```

## 3. Running load

Every load run is a fresh Job (Jobs are immutable — unique suffix):

```sh
load() {  # load <mode> <concurrency> <durationS> [extra --set flags...]
  local mode=$1 c=$2 dur=$3; shift 3
  helm template $RELEASE perf/aks/deploy/chart -n $NS \
    -s templates/loadgen-job.yaml \
    --set image.tag=$TAG --set loadgen.enabled=true \
    --set loadgen.nameSuffix=$(date +%s)-$RANDOM \
    --set loadgen.mode=$mode --set loadgen.concurrency=$c \
    --set loadgen.durationS=$dur "$@" | kubectl apply -n $NS -f -
}
load counter 32 60
kubectl -n $NS get jobs             # find the suffix
kubectl -n $NS logs -f job/$RELEASE-loadgen-<suffix>   # stderr = progress
kubectl -n $NS logs job/$RELEASE-loadgen-<suffix> | tail -1 | jq   # result
```

## 4. Scenarios

Run in order; each assumes the previous left the cluster healthy.

### (a) Deploy + membership convergence

`kubectl -n $NS get pods -w` until 3 hosts + redis are Ready. Then:

- Cluster snapshot: `view.size == 3`, `view.active == 3`, every host
  `status: "active"`, reminder shards 16/16 covered.
- Convergence < 30 s from the last pod turning Ready.
- **Negative check (proves the prod dist shipped):**
  `curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:7311/_sigx/ops`
  without a bearer → must be **401**. A 200 means the dev dist is running
  and every perf number after this is invalid.

### (b) Smoke

`load counter 4 15 --set loadgen.keyCount=50`. Pass:

- loadgen JSON `errors.total == 0`;
- snapshot shows activations on **all three** hosts (placement spread);
- state landed in Redis:
  `kubectl -n $NS exec deploy/$RELEASE-redis -- redis-cli --scan --pattern 'sigx:st:*' | head`.

### (c) Baseline perf sweep — the reference curve

```sh
load counter 0 60 --set loadgen.sweep="1\,2\,4\,8\,16\,32\,64\,128" \
  --set loadgen.keyCount=1000 --set loadgen.hotRatio=0.05
load crunch 0 60 --set loadgen.sweep="1\,2\,4\,8\,16\,32\,64"
```

(One JSON line per rung. Note the escaped commas inside `--set`.) Repeat
each sweep twice; rungs should agree within ±15%. While it runs, watch
`kubectl top pods -n $NS` — **which saturates first, host CPU or Redis
CPU, is a headline finding** (counter mode does one Redis CAS per call).
Record the knee: the rung where ops/s stops scaling and p99 starts
climbing. Everything later is judged against this curve.

### (d) Scale-out under load (3 → 5)

Start a long run: `load counter 64 600`. Mid-run:

```sh
helm upgrade $RELEASE perf/aks/deploy/chart -n $NS \
  --reuse-values --set replicaCount=5
```

Pass: `top` shows the view going 3→5 with zero loadgen errors during the
join (new hosts are readiness-gated); post-join steady-state ops/s ≥ the
3-host figure from (c); new keys activate on the new hosts. Existing
actors stay put — spread evens out via new activations, not migration.

That last sentence is the DEFAULT, not a law: `cluster({ rebalance })`
migrates idle activations toward the mean when it is switched on, and
`activationCountPolicy()` steers new ones at the coldest host. Both are off
here. Scenario (n) is this same scale-out with them on.

### (e) Rolling restart under load — the zero-drop check

Under `load counter 64 600`:

```sh
kubectl -n $NS rollout restart deploy/$RELEASE-host
kubectl -n $NS rollout status deploy/$RELEASE-host
```

Mechanics under test: surge pod joins → old pod turns ready-503 and
announces `leaving` → preStop sleep lets the endpoints notice → 30 s
drain hands actors off → terminate. Pass: **loadgen `errors.total == 0`**
— this is the drain design's promise. Record the p99 blip during
hand-off. Afterwards the view is back to size 5 (or 3) with all-new
hostIds (a restart is a new host by design).

### (f) Hard pod kill under load — state recovery

Under `load counter 64 300 --set loadgen.keyCount=200`:

```sh
kubectl -n $NS delete pod <one host pod> --grace-period=0 --force
```

Expect an error window ≈ membership TTL (15 s) + directory eviction while
the dead host's actors fail over; the progress lines on the Job's stderr
timestamp it. After the run, verify **no committed state was lost**:

```sh
load verify 1 0 --set loadgen.keyCount=200
# actuals vs the counter run's acked counts:
#   actual >= acked  → pass (an ack means the CAS committed)
#   actual  < acked  → STATE LOSS — file a bug
#   actual  > acked  → legal: an increment can commit after its response died
```

Also record from the snapshot: `counters.hostSweeps`, `wrongHostRedirects`
climbing during the window, and that the cluster reconverged with no
human action.

### (g) Scale-in / drain under load (5 → 3)

Under load: `helm upgrade ... --reuse-values --set replicaCount=3`. Pass:
zero loadgen errors (drain = the rolling-restart path), drained hosts'
actors reappear on survivors with state intact (spot-check a key's
`current()` against its acked count), PDB never violated
(`kubectl -n $NS get pdb`).

### (h) Redis outage — self-fencing

Under load: `kubectl -n $NS delete pod <redis pod>`. Expected sequence:

1. Hosts fail heartbeats; past `ttlMs` each fires `onSelfSuspect` and
   **self-fences** (refuses activations).
2. Readiness flips 503 (`fenced`) — the Service empties; loadgen errors
   hard. This is correct behavior, record it.
3. Redis restarts on its node (Recreate + PVC), AOF replays state.
4. **Record what recovery actually looks like** — do fenced hosts rejoin
   on their own once heartbeats succeed, or do they stay fenced until the
   kubelet recycles them? This scenario exists to answer that; if the
   answer is "stuck until restart", file a runtime issue.

Pass: reconvergence without helm intervention in ~1–2 min after Redis is
back; `load verify ...` shows no committed-state loss (AOF held it);
`ActorStorageConflict` entries in host logs during the window are
*evidence fencing worked*, not failures.

### (i) HPA + cluster autoscaler — node scale-out

```sh
helm upgrade $RELEASE perf/aks/deploy/chart -n $NS \
  --reuse-values --set hpa.enabled=true --set affinity.hostSelfSpread=required
load crunch 64 600 --set loadgen.crunchIters=2000
kubectl -n $NS get hpa -w     # and: kubectl get nodes -w
```

Expected chain: host CPU > 60% → HPA adds replicas → required self-spread
makes them Pending → cluster autoscaler adds tainted D2ls_v6 nodes → pods
schedule (never onto the Redis node — required anti-affinity). Measure
time from HPA scale decision to the new host Ready (node boot + image
pull + membership join). After the load ends: HPA scales in, drained
hosts hand off cleanly, the autoscaler reclaims empty nodes.

### (j) Continuous monitoring

Not a separate run — `sigx actors top` stays attached through (c)–(i).
Note what the dashboard shows that the JSON summaries miss (queue-depth
spikes, error kinds, transport fallbacks — should stay 0 with http-only)
and capture one `stats --json` snapshot per scenario for the record.

### (k) Long-running jobs under failover

The workload the whole setup exists to protect: `MODE=jobs` starts
`JOB_COUNT` SweepJob runs (`JOB_STEPS` × `JOB_STEP_MS` each, one
checkpoint per step — a Redis CAS each) and polls until every one is
terminal.

```sh
# ~5-minute jobs, enough runway to kill things mid-flight:
load jobs 1 0 --set loadgen.mode=jobs   # then, via the raw job template,
# env: JOB_COUNT=50 JOB_STEPS=300 JOB_STEP_MS=1000
# Mid-run, do BOTH:
kubectl -n $NS rollout restart deploy/$RELEASE-host
kubectl -n $NS delete pod <one host> --grace-period=0 --force
```

Pass: `completed == jobs` and `stuck == 0` (a job stranded `running` on a
dead host means directory eviction failed to revive it);
`crashResumes > 0` proves the kill landed on live runs and they resumed
from their checkpoints; observed progress regresses at most one step per
resume. Wall time stretches by roughly the membership TTL per hard kill —
that detection window is the price of crash-resume.

### (l) The real app, from the outside world

The chat example ([`perf/app/deploy`](../../app/deploy)) publicly
exposed: SSR + browser client + signed sessions + live NDJSON streams
through the ingress. Deploy:

```sh
TAG=$(git rev-parse --short HEAD)
az acr build --registry $ACR --image sigx-chat:$TAG \
  --platform linux/amd64 --file perf/app/Dockerfile .
kubectl create namespace sigx-chat
helm install chat perf/app/deploy/chart -n sigx-chat --set image.tag=$TAG
az network dns record-set a add-record -g <dns-rg> -z <zone> \
  -n chat -a <ingress-lb-ip> --ttl 300
# pre-DNS smoke: curl -sI --resolve chat.<zone>:443:<lb-ip> https://chat.<zone>/
```

**Most of this matrix is now automated** — `node testenv.mjs test` runs the
assertion suite (`perf/aks/__tests__/infra.test.ts`) and then
the Tier-3 perf comparison, and prints one verdict. Add `INFRA_CHAOS=1` for
the destructive rows. What remains manual is the browser: two tabs, a live
cross-tab update, and a reconnect through a rolling restart.

The matrix — every test runs from OUTSIDE the cluster:

| # | Test | Pass |
|---|---|---|
| 1 | `curl -v https://chat.<zone>/` | 200 SSR HTML over h2, wildcard cert |
| 2 | real browser, two tabs, different users, post in one | appears live in the other — no reload |
| 3 | `/r/general` + `/r/random` | isolated rooms; ops (port-forward internal port) shows actors spread |
| 4 | `GET /_sigx/health`, `GET /_sigx/ops`, `POST /_sigx/host/x` on the public host | all 404 — the dual listener seals them; never JSON, never SSR HTML |
| 5 | `POST /_sigx/fn/... -H 'Origin: https://evil.example'` | 403 (same-origin via x-forwarded-*) |
| 6 | no cookie → 401; forged `user=ada.deadbeef` → 401; signed cookie → 200 | the guard chain, end to end |
| 7 | `POST /_sigx/actor/Room/topic` (signed) | 200 — nginx passed the multi-segment symbol through without merging or dot-normalizing it |
| 8 | hold a quiet `$live` connection ≥ 120 s | stays open — a `{"chunk":{"p":1}}` keepalive every 30 s, plus the 3600 s ingress timeouts; the client also reconnects on its own |
| 9 | `kubectl -n sigx-chat rollout restart deploy/chat-host` with a tab streaming | tab reconnects by itself and resumes receiving |
| 10 | post → `kubectl -n sigx-chat delete pod chat-redis-...` | AOF recovery, history intact |
| 11 | WAN load from a laptop (signed cookie + correct Origin, recent+post loop) | p50/p99 vs the in-cluster curve; no 5xx |
| 12 | post in three rooms, then read `ActivityFeed#recent('all', 50)` | all three appear — a publish reached the singleton subscriber on whichever pod owns it, over the internal HMAC mount |
| 13 | `POST /_sigx/fn/<postMessage id>` (signed) | 200 — the id is derived from the build (`deploy/post-fn.mjs`), never pasted; a stale one 404s and reads as extra throughput |

Rows 1 and 3–8 and 11–13 are the automated suite; rows 2 and 9 are the
browser ones that stay manual.

Teardown: `az network dns record-set a delete ... -n chat -y` and
`helm uninstall chat -n sigx-chat` (the room-history PVC survives until
the namespace goes).

### (m) migrateState across a rolling deploy

The one assertion that needs two images: `migrateState` runs only on a
record the PREVIOUS one wrote, and a v1 record is simply what an older
chat image writes — no fixture, no test-only endpoint.

```sh
node perf/aks/deploy/testenv.mjs migrate-check [<old-tag>]
```

Writes a room against the old image, rolls the release forward to HEAD,
then asserts the room reads its full history back and `Room#version()`
answers `2`. Pass: green, with `INFRA_MIGRATED_ROOM` named in the output.

Worth watching while it rolls, because these are the numbers no unit test
can produce: `storage.conflicts` in `sigx actors stats --json` (a fleet
mid-deploy migrates the same record more than once and the etag CAS is what
makes that safe), and whether `storage.saves` moves at all — lazy
write-back must add NO writes to a read-only room. Re-run with
`--set env.migratePersist=eager` to see the opposite trade: one CAS per
activation, and a read-only room that does persist its migration.

### (n) Rebalance and load-aware placement under scale-out

Scenario (d) with the knobs on. Both are off by default and both change the
steady-state spread, so `testenv.mjs` folds them into `INFRA_SHAPE` and the
perf comparison refuses to cross them — deliberate, not an obstacle.

```sh
helm upgrade chat perf/app/deploy/chart -n sigx-chat --reset-then-reuse-values \
  --set env.rebalance=1 --set env.rebalanceIntervalMs=30000 \
  --set env.rebalanceMinIdleMs=15000
INFRA_CHAOS=1 node perf/aks/deploy/testenv.mjs test -t rebalance
```

Pass: `counters.rebalanceMigrations` climbs, the joined hosts end up
holding activations they did not activate, and max/mean converges under
`rebalanceThreshold`. For the placement half, `--set
env.placement=activation-count` and `-t activationCountPolicy`: fresh keys
placed WITHOUT a routing token should favour the cold hosts, because a host
with no known load reads as cold.

Record what neither can be unit-tested for: whether the correction
converges or oscillates across repeated scaling events, and what the
per-peer probe traffic costs at this fleet size.

### (o) maxActivations — shedding under a cap

```sh
helm upgrade chat perf/app/deploy/chart -n sigx-chat --reset-then-reuse-values \
  --set env.maxActivations=200 --set env.sweepIntervalMs=15000
node perf/aks/deploy/testenv.mjs test -t maxActivations
```

Pass: total activations settle at ≤ cap × hosts, `activations.byReason.
capacity` is non-zero, and a shed room still reads its history back — the
cap is a memory knob, never a correctness one.

The measurement to take by hand alongside it: `kubectl top pods` before and
after, because the premise ("shed before the heap makes you") is the one
thing no test asserts. Note also the soft-cap failure mode — a genuinely
busy host sits OVER the cap indefinitely, because busy, queued and
kept-alive activations are never shed.

### (p) Worker-pool saturation

```sh
helm upgrade chat perf/app/deploy/chart -n sigx-chat --reset-then-reuse-values \
  --set env.digestMaxLocal=8 --set env.digestIters=20000
node perf/aks/deploy/testenv.mjs load ACTOR_TYPE=Digest \
  READ_METHOD=summarize 'READ_ARGS=["payload",20000]' ROOMS=1 LADDER=8,16,32,64
```

One key, so every call lands in one pool. Pass: throughput keeps climbing
past a concurrency of 1 (a single activation would flatten immediately),
and `directoryLookups` in the ops snapshot does not move at all — a worker
is always placed locally and never claims.

The headline finding to record is where it stops scaling and why: the pool
grows to `maxLocal` ACTIVATIONS, but the container's CPU limit is what
decides how many of them make progress. Leaving `digestMaxLocal` unset is
the more interesting run — the pool then sizes itself from the node's real
`hardwareConcurrency`, which no unit test has ever exercised.

### (q) WebSocket connection scale — how many clients receive messages (#172)

The one axis nothing else here touches: HTTP scenarios measure ops/s, this
one measures CONNECTIONS HELD and MESSAGES DELIVERED to them. It runs
entirely in-cluster against the actors Service — no ingress, no TLS, no
load VM — so it measures the runtime rather than the edge.

```sh
# 1. mount the socket (a rollout — do it before, never between rungs)
node perf/aks/deploy/testenv.mjs ws-up socket.sessions=true

# 2. the idle baseline: what a held connection costs when nothing happens
node perf/aks/deploy/testenv.mjs ws-load mode=idle ladder=1000,5000,10000

# 3. the headline: N subscribers on ONE actor, 10 publishes/s
node perf/aks/deploy/testenv.mjs ws-load mode=hot ladder=1000,5000,10000 \
  actors=1 publishRate=10 durationS=60

# 4. the same, spread 1:1 over N actors
node perf/aks/deploy/testenv.mjs ws-load mode=spread ladder=1000,5000 publishRate=50

# 5. THE ARM THAT CHANGES THE ANSWER — signed-in instead of anonymous
node perf/aks/deploy/testenv.mjs ws-load mode=hot ladder=1000,5000 \
  read=mine principal=per-user

# 6. the #138 pair — SAME identities, one declared principal-independent.
#    Run both at the DEFAULT pool: the point is that the declared arm does
#    not need it sized. `current` and `shared` are the same read body.
node perf/aks/deploy/testenv.mjs ws-load mode=hot ladder=100,250,500,1000 \
  read=current principal=per-user      # undeclared control
node perf/aks/deploy/testenv.mjs ws-load mode=hot ladder=100,250,500,1000 \
  read=shared  principal=per-user      # declared (#138)
```

**A generator pod caps at ~28k connections — measured, not predicted.** A
rung of 40 000 from one pod produced 28 232 connected and 11 768 failures,
with the hosts refusing NONE of them: that is the pod's ephemeral port
range, not a server limit. Add pods rather than raising the rung:
`parallelism=4` runs four subscriber pods (indexed, so pod 0 is the only
publisher and the only one carrying latency probes). Keep each pod's rung
under ~25 000 or the failures are the generator's own.

`n` in the output is PER POD; `totalConnections` sums the pods. The number
to quote for concurrency is `peakOpen` from the "peak concurrency" section —
it is the `open` GAUGE sampled off the hosts while the job ran, so it is
the servers' own count rather than the generator's claim. It is a sample
(~5 s), so a peak between samples is missed; the monotonic totals cannot
substitute, because they count every connection ever opened across every
rung rather than how many were up at once.

Pass/fail, and what to record:

- **`connected` must equal the hosts' own `ops.sockets.open`** (plus one
  for the publisher's connection — the merged row states the expected
  total). The verb prints the summed `ops.sockets` delta after every run;
  if the two disagree, one of them is wrong and the run is void.
- **`protocolBreaches` must be 0.** The verb fails the run on any. A breach
  means the client and the session disagree about the vocabulary.
- **`maxBufferedBytes` climbing is the real ceiling.** The socket send path
  is fire-and-forget — nothing in the runtime reads `bufferedAmount` — so
  a slow client shows up as memory, not as backpressure. Record it.
- **`deliveriesPerPublish` is a coalescing ratio, not a constant.** Client
  subscriptions cannot set `throttleMs`, so every one runs at the runtime's
  fixed 50 ms watch throttle: a subscriber receives at most ~20 pushes/s,
  and above that rate the ratio falls below the subscriber count BY DESIGN.
  Expect latency p50 to sit at ~50 ms plus fan-out for the same reason.
- **Report the anonymous and per-user arms separately, never averaged.**
  A live read that consults `ctx.principal` gets one watch loop per
  identity (#121), and a cross-host watch coalesces per principal unless
  the method declared `principalIndependent` (#138) — so signed-in fan-out
  costs O(identities) actor turns where anonymous costs O(1). Measured
  locally at 200 subscribers on one actor: 31 turns anonymous vs 6200
  per-user, with publish p50 moving 0.18 ms → 1.02 ms. On a cluster it also
  multiplies the cross-host streams. **Since #202 you no longer have to go
  and look**: the verb reports `cluster/remoteWatches`,
  `cluster/coalescedWatches` and `cluster/inboundWatches` in the summed
  delta, taken from `clusterStats()` (already fleet-wide — read once, never
  summed per pod). If they are ABSENT, a cluster-stats fan-out missed a host
  at one end of the run or the other — a partial baseline overstates the
  delta and a partial end snapshot understates it, so the harness drops them
  rather than print a number whose error direction is unknown.
- **`Fanout` carries three reads, and picking the wrong one silently
  measures something else.** `current()` is identity-blind and UNDECLARED,
  `shared()` is the same body DECLARED `principalIndependent`, `mine()`
  genuinely consults identity. `read=` is validated and the generator exits
  on an unknown value rather than defaulting — a typo used to run a
  different experiment under the name you asked for.
- **Size `env.fetchConnections` before a per-user run, or the pool is what
  you measure (#194).** Every per-principal cross-host stream PINS one
  pooled host-to-host connection for the life of the subscription, and at
  the default 64/peer the pool — not the runtime — was the 100–250
  identity cliff of #180: identities × ⅔ streams across (replicas−1)
  pools. Deploy with
  `testenv.mjs ws-up socket.sessions=true env.fetchConnections=1024`
  for identity ladders past ~150, and expect a rung to read starved
  establishment (504 subscription errors, publishes barely landing) when
  the pool binds. The pool size is part of `INFRA_SHAPE`, so a recorded
  run under a different pool is refused rather than compared.
- **The `read=shared` arm is the exception, and that is the whole point.**
  #138 removes the per-identity stream instead of budgeting for it, so that
  arm should walk the ladder at the untouched default 64. Sizing the pool
  for it would measure #194's mitigation a second time and prove nothing:
  run the pair at the default, and only then, if you want, repeat with a
  sized pool to confirm the two answers converge.

### Recording it instead of reading it (#184)

Everything above is the hand-run: it prints JSON and you read it. To get a
baseline file, a shape guard and a spread instead:

```sh
node perf/aks/deploy/testenv.mjs ws-bench --save-baseline   # record THIS shape
node perf/aks/deploy/testenv.mjs ws-bench --compare         # against it
```

`ws-bench` reads the deployed image and the live socket caps off the
release and exports them as `INFRA_SHAPE`, so a run under a different
`SOCKET_MAX_SUBSCRIPTIONS` (or a different replica count, or a different
image) is REFUSED rather than compared — the same protection `infra/*` has.
It runs `--runs=1` deliberately: the harness discards a warmup before the
measured runs, and each run here is a full ladder holding tens of thousands
of sockets on a paid cluster.

`sockets/principal-cliff` is the one to watch. It climbs distinct
identities until dialling fails and records `max_healthy_identities` —
the number #180 existed to move. It sat between 100 and 250 while the
fetch pool bound (#194); with the pool sized (`env.fetchConnections=1024`)
and the #193 pump in the image, every rung through 1000 measured clean —
the recorded 500 is the ladder's old ceiling, not a cliff.

Sockets are HOST-AFFINE: the client is pinned to whichever pod the Service
gave it, and every call inside re-dispatches through placement. The edge
hash buys nothing here, so cross-host rates read like `random` placement
whatever `PLACEMENT` says — expected, and not comparable to the HTTP rows.

### Optional: Lease-based membership (`@sigx/actors-k8s`)

Re-run (a), (d)–(h) with membership on coordination Leases instead of
Redis — same image, one value:

```sh
helm upgrade $RELEASE perf/aks/deploy/chart -n $NS \
  --reuse-values --set membership=k8s
```

(Adds the ServiceAccount + Lease Role/RoleBinding; the directory and
actor state stay in Redis — independent seams.) Differences worth
recording: convergence latency vs Redis pub/sub, behavior in (h) — with
`membership=k8s` a Redis outage no longer fences hosts, it only stalls
directory claims and saves.

Switching back (`--set membership=redis`) deletes the Lease RBAC while
the old k8s-membership pods are still draining: their renewals 403, they
fence, and fencing is fatal — expect the outgoing pods to restart once
or twice before the rollout replaces them. Noisy but harmless; the new
pods never touch Leases.

## 5. Teardown between sessions

`node testenv.mjs down` does all of the below (releases, DNS record, load
VM resource group, node pool). The manual equivalents:

```sh
helm uninstall $RELEASE -n $NS      # the Redis PVC (actor state) survives
# The pool autoscaler (min 0) reclaims empty nodes in ~10 min; to force:
az aks nodepool scale -g $RG --cluster-name $CLUSTER \
  --name sigxactors --node-count 0 \
  || echo "disable the autoscaler first: az aks nodepool update --disable-cluster-autoscaler"
# Full cleanup (kills the PVC and the pool):
# kubectl delete namespace $NS
# az aks nodepool delete -g $RG --cluster-name $CLUSTER --name sigxactors
```
