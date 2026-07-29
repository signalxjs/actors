# AKS test runbook

The full scale-out / perf / failure test for `@sigx/actors` on AKS, using
the [`sigx-actors-test` chart](chart/) and the
[`aks-cluster` example](../README.md).

Set once, used by every command below:

```sh
SUBSCRIPTION=<subscription>  # the Azure subscription holding the cluster
RG=<resource-group>          # the AKS cluster's resource group
CLUSTER=<aks-cluster-name>
ACR=<acr-name>               # attached to (or pull-permitted for) the cluster
NS=sigx-actors-test          # namespace
RELEASE=sigx                 # helm release; resources are $RELEASE-silo etc.
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

# Dedicated, tainted node pool — 4 nodes: 3 silo + 1 redis (the chart's
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
```

## 1. Build + deploy

```sh
cd <repo root>
TAG=$(git rev-parse --short HEAD)
az acr build --registry $ACR \
  --image sigx-actors-test:$TAG \
  --platform linux/amd64 \
  --file examples/aks-cluster/Dockerfile .

helm install $RELEASE examples/aks-cluster/deploy/chart \
  -n $NS --set image.tag=$TAG
# upgrades: helm upgrade $RELEASE ... --reuse-values --set image.tag=$NEWTAG
```

Secrets are generated on first install and preserved across upgrades.

## 2. Monitoring from your own terminal

```sh
kubectl -n $NS port-forward svc/$RELEASE-silo 7311:7311 &
OPS_SECRET=$(kubectl -n $NS get secret $RELEASE-secrets \
  -o jsonpath='{.data.opsSecret}' | base64 -d)

# The live dashboard — the ops fan-out reaches the whole cluster from
# whichever silo the forward lands on. Run from this repo (the sigx CLI
# discovers its plugins from the project's dependencies):
pnpm --filter aks-cluster-example exec sigx actors top \
  --url http://127.0.0.1:7311 --secret "$OPS_SECRET"

# One-shot snapshots for the record:
pnpm --filter aks-cluster-example exec sigx actors stats \
  --url http://127.0.0.1:7311 --secret "$OPS_SECRET" --json
```

## 3. Running load

Every load run is a fresh Job (Jobs are immutable — unique suffix):

```sh
load() {  # load <mode> <concurrency> <durationS> [extra --set flags...]
  local mode=$1 c=$2 dur=$3; shift 3
  helm template $RELEASE examples/aks-cluster/deploy/chart -n $NS \
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

`kubectl -n $NS get pods -w` until 3 silos + redis are Ready. Then:

- Cluster snapshot: `view.size == 3`, `view.active == 3`, every silo
  `status: "active"`, reminder shards 16/16 covered.
- Convergence < 30 s from the last pod turning Ready.
- **Negative check (proves the prod dist shipped):**
  `curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:7311/_sigx/ops`
  without a bearer → must be **401**. A 200 means the dev dist is running
  and every perf number after this is invalid.

### (b) Smoke

`load counter 4 15 --set loadgen.keyCount=50`. Pass:

- loadgen JSON `errors.total == 0`;
- snapshot shows activations on **all three** silos (placement spread);
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
`kubectl top pods -n $NS` — **which saturates first, silo CPU or Redis
CPU, is a headline finding** (counter mode does one Redis CAS per call).
Record the knee: the rung where ops/s stops scaling and p99 starts
climbing. Everything later is judged against this curve.

### (d) Scale-out under load (3 → 5)

Start a long run: `load counter 64 600`. Mid-run:

```sh
helm upgrade $RELEASE examples/aks-cluster/deploy/chart -n $NS \
  --reuse-values --set replicaCount=5
```

Pass: `top` shows the view going 3→5 with zero loadgen errors during the
join (new silos are readiness-gated); post-join steady-state ops/s ≥ the
3-silo figure from (c); new keys activate on the new silos. Existing
actors stay put — spread evens out via new activations, not migration.

### (e) Rolling restart under load — the zero-drop check

Under `load counter 64 600`:

```sh
kubectl -n $NS rollout restart deploy/$RELEASE-silo
kubectl -n $NS rollout status deploy/$RELEASE-silo
```

Mechanics under test: surge pod joins → old pod turns ready-503 and
announces `leaving` → preStop sleep lets the endpoints notice → 30 s
drain hands actors off → terminate. Pass: **loadgen `errors.total == 0`**
— this is the drain design's promise. Record the p99 blip during
hand-off. Afterwards the view is back to size 5 (or 3) with all-new
siloIds (a restart is a new silo by design).

### (f) Hard pod kill under load — state recovery

Under `load counter 64 300 --set loadgen.keyCount=200`:

```sh
kubectl -n $NS delete pod <one silo pod> --grace-period=0 --force
```

Expect an error window ≈ membership TTL (15 s) + directory eviction while
the dead silo's actors fail over; the progress lines on the Job's stderr
timestamp it. After the run, verify **no committed state was lost**:

```sh
load verify 1 0 --set loadgen.keyCount=200
# actuals vs the counter run's acked counts:
#   actual >= acked  → pass (an ack means the CAS committed)
#   actual  < acked  → STATE LOSS — file a bug
#   actual  > acked  → legal: an increment can commit after its response died
```

Also record from the snapshot: `counters.siloSweeps`, `wrongHostRedirects`
climbing during the window, and that the cluster reconverged with no
human action.

### (g) Scale-in / drain under load (5 → 3)

Under load: `helm upgrade ... --reuse-values --set replicaCount=3`. Pass:
zero loadgen errors (drain = the rolling-restart path), drained silos'
actors reappear on survivors with state intact (spot-check a key's
`current()` against its acked count), PDB never violated
(`kubectl -n $NS get pdb`).

### (h) Redis outage — self-fencing

Under load: `kubectl -n $NS delete pod <redis pod>`. Expected sequence:

1. Silos fail heartbeats; past `ttlMs` each fires `onSelfSuspect` and
   **self-fences** (refuses activations).
2. Readiness flips 503 (`fenced`) — the Service empties; loadgen errors
   hard. This is correct behavior, record it.
3. Redis restarts on its node (Recreate + PVC), AOF replays state.
4. **Record what recovery actually looks like** — do fenced silos rejoin
   on their own once heartbeats succeed, or do they stay fenced until the
   kubelet recycles them? This scenario exists to answer that; if the
   answer is "stuck until restart", file a runtime issue.

Pass: reconvergence without helm intervention in ~1–2 min after Redis is
back; `load verify ...` shows no committed-state loss (AOF held it);
`ActorStorageConflict` entries in silo logs during the window are
*evidence fencing worked*, not failures.

### (i) HPA + cluster autoscaler — node scale-out

```sh
helm upgrade $RELEASE examples/aks-cluster/deploy/chart -n $NS \
  --reuse-values --set hpa.enabled=true --set affinity.siloSelfSpread=required
load crunch 64 600 --set loadgen.crunchIters=2000
kubectl -n $NS get hpa -w     # and: kubectl get nodes -w
```

Expected chain: silo CPU > 60% → HPA adds replicas → required self-spread
makes them Pending → cluster autoscaler adds tainted D2ls_v6 nodes → pods
schedule (never onto the Redis node — required anti-affinity). Measure
time from HPA scale decision to the new silo Ready (node boot + image
pull + membership join). After the load ends: HPA scales in, drained
silos hand off cleanly, the autoscaler reclaims empty nodes.

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
kubectl -n $NS rollout restart deploy/$RELEASE-silo
kubectl -n $NS delete pod <one silo> --grace-period=0 --force
```

Pass: `completed == jobs` and `stuck == 0` (a job stranded `running` on a
dead silo means directory eviction failed to revive it);
`crashResumes > 0` proves the kill landed on live runs and they resumed
from their checkpoints; observed progress regresses at most one step per
resume. Wall time stretches by roughly the membership TTL per hard kill —
that detection window is the price of crash-resume.

### Optional: Lease-based membership (`@sigx/actors-k8s`)

Re-run (a), (d)–(h) with membership on coordination Leases instead of
Redis — same image, one value:

```sh
helm upgrade $RELEASE examples/aks-cluster/deploy/chart -n $NS \
  --reuse-values --set membership=k8s
```

(Adds the ServiceAccount + Lease Role/RoleBinding; the directory and
actor state stay in Redis — independent seams.) Differences worth
recording: convergence latency vs Redis pub/sub, behavior in (h) — with
`membership=k8s` a Redis outage no longer fences silos, it only stalls
directory claims and saves.

Switching back (`--set membership=redis`) deletes the Lease RBAC while
the old k8s-membership pods are still draining: their renewals 403, they
fence, and fencing is fatal — expect the outgoing pods to restart once
or twice before the rollout replaces them. Noisy but harmless; the new
pods never touch Leases.

## 5. Teardown between sessions

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
