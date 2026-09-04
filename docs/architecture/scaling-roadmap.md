# Scaling roadmap — to a make.com-class workflow engine

Tracking issue: #378. This note answers three questions with the evidence the
repo already holds, then lays out the order of work such that every runtime
change is justified by a measurement before it is built and proven by one
after. It is a maintainer document: the numbers it cites live in
[`benchmarks/BASELINES.md`](../../benchmarks/BASELINES.md) and are quoted here
with their section, never restated as facts of their own.

The three questions:

1. Do we have enough proof to build a massively scalable workflow engine on
   `@sigx/actors`?
2. What is the way forward to get numbers and Tier-3 tests until we know what
   we need?
3. What stops a host from using 8 cores, and what stops the fleet from scaling
   horizontally?

## Assumptions

These were set without the answers they deserve; each is stated so it can be
overturned by an issue comment rather than rediscovered.

| Assumption | Value | If wrong |
|---|---|---|
| Target | make.com-class: ~1,000 runs/s sustained cluster-wide, ~1M runs asleep at once, 100+ hosts, multi-tenant | A mid-scale target (100 runs/s, 20 hosts) needs only phases 0–2 below; phases 3–4 become optional |
| Language | Node-first. The 2026-09-02 "Go host PoC bar" (`BASELINES.md` §2026-09-02) stays a comparison bar | If a Go host is the plan, B3 (one host per core) is deprioritised and the ladder's job becomes defining that host's contract |
| Estate | The Azure rig may grow within reason: `POOL_MAX` ~16, one D8 pool, 1–2 h soak sessions | On the current shape only, phase 3's G1–G3 are modelled, not measured, and this note must say so wherever it quotes them |

## The honest answer today

### What is proven

Every figure below is recorded in `BASELINES.md` under the dated section named.

- **The engine runs and recovers.** On three 1 vCPU pods over HTTP with one
  Redis: 18.9 completed runs/s and 264 transitions/s at 50 offered, zero
  stuck runs, zero lost wakes, zero reminder CAS failures, a 0.2% error rate
  (§2026-09-02). The ceiling of that shape is the workload's own sha256 on
  the JS loop, not the runtime's bookkeeping — a 20 ms task measures
  28 → 36 → 58 ms of loop occupancy across the ladder.
- **Sleeping is cheap.** ~4,500 runs asleep at once on three hosts holding
  36 activations, every one woken within half a tick, no CAS lost at
  50 arms/s plus 50 fires/s (§2026-08-26).
- **A run's bookkeeping is bounded.** A job run costs 5 storage round trips
  (was 12); ~28k runs/s per host is the in-process floor (§2026-09-01,
  #309, #310).
- **Activation cost is flat in cluster size.** Two directory operations per
  cold activation, identical at N=1 and N=100; CI gates it as an `exact`
  metric (§2026-07-28).
- **Failure is exercised, not argued.** The in-process three-host suite
  kills the owner of a run while it sleeps on a durable reminder, on a
  volatile timer, mid fan-out, mid wait, and kills the aggregator
  (`perf/aks/__tests__/workflow-cluster.test.ts`). The fleet-level chaos
  suite (`INFRA_CHAOS=1`) covers a rolling restart, a hard pod kill and a
  Redis outage — for the HTTP chat workload only.

### What is not proven

- No Tier-3 run above 7 hosts, and only one above 3 (§2026-08-02, with the
  edge hash silently off). Every socket and workflow figure is
  `replicas=3 nodes=3`.
- No run has ever measured a host with more than one usable core, or a
  node packed with several hosts. The single 8-core data point is one host
  on a D8 at 1.07× (§2026-08-02 "Mailboxes are not cores").
- No soak: rungs are 20–60 s. Memory growth, activation counts and
  reminder-shard bytes over an hour are unknown.
- The 100 and 200 runs/s rungs were never re-run after #304 fixed the wedge
  they produced; they are excluded from the default ladder
  (`benchmarks/src/scenarios/workflow.ts`, `INFRA_WF_RATE_LADDER`).
- The rate at which `reminder_set_failure_ratio` leaves zero is unlocated
  (§2026-08-26 names `INFRA_WF_SLEEP_RATE_LADDER` as the way).
- Redis has never been identified as a limiter, and RUNBOOK scenario (c)'s
  "which saturates first, host CPU or Redis CPU" has no answer in
  `BASELINES.md`.
- Chaos and load have never run together on the workflow axis; no recorded
  workflow run has had a host killed mid-run.
- Nothing is scheduled (#60). Results live in Actions artifacts and in
  `BASELINES.md`.
- Engine test gaps, all closable without touching the pinned workload: the
  at-least-once task claim is a comment (`run.actor.ts` "activity
  scheduled" save); `reminderSetFailures` → `timer-fallback`,
  `publishFailures` / `completedUnreported`, `joinRepairs` via the watchdog
  and the stats-ring `dropped` path have no test; both suites run on
  `memoryStorage` only.

### What holds scaling back, in the order it bites

1. **Multi-core.** A host is one Node process on one event loop, and the
   codebase treats that as an invariant with a security consequence: a
   CPU-bound turn past the membership TTL fences the host fatally
   (`SECURITY.md` "A CPU-bound turn can fence its own host",
   [`clustering.md`](clustering.md)). `defineWorker` multiplies mailboxes,
   not cores — 1.0× against a single activation on a 20-core box.
   `worker_threads` was priced at a ~3.4× ceiling with a ~200 µs crossover
   and a 1 MB payload cliff, and deliberately kept out of the runtime
   (#119, §2026-08-06). **One host per core is the intended answer, and
   nothing ships to do it**: no supervisor, no k8s packing recipe, no docs
   page, no measurement.
2. **No admission control.** Turn queues are unbounded promise chains
   (`packages/actors/src/host/turns.ts`); `queued` is reported and nothing
   acts on it; the 30 s `callTimeoutMs` deadline is the only bound; a
   queued call whose deadline already passed still runs its body;
   `maxActivations` is a soft LRU sweep. The engine cannot refuse a start.
   This is why 100 runs/s wedged the fleet twice (#302).
3. **Reminders.** The default provider keeps 16 compat-frozen shards
   (`host/reminder-shards.ts`); every `set`/`clear` loads, stringifies
   twice and CAS-saves a 1/16 slice of the cluster's reminders with three
   attempts; the tick loads and scans the whole owned record; all of a
   host's mutations serialise on one chain (`host/reminders.ts`). At most
   16 hosts do reminder work. There is no Redis due-time provider —
   `pgReminders` has the indexed design, `redisStorage` deployments run the
   sharded one. This is the wall in front of 1M sleeping runs and 1M
   schedules.
4. **Singleton subscribers.** `ctx.publish` settles only when every
   subscriber's turn has (`host/topics.ts`), so one aggregator gates every
   publisher's completion path; publish rate was flat and fell with more
   hosts (§2026-08-02). #49 (one-way delivery) is open. More generally,
   any per-tenant singleton is the single-activation ceiling: 289 ops/s at
   3 hosts and at 7 (§2026-08-02).
5. **Membership at 100 hosts.** A join costs O(N²) Redis commands even
   after #26's coalescing (~10,400 at n=100, measured); a 100-host rolling
   restart is on the order of 2M commands in bursts that share the client
   with the heartbeat (§2026-07-28).
6. **The HTTP host-to-host fetch pool.** It was the identity cliff (#194);
   TCP (#203) multiplexes it away, but the workflow shape has only ever been
   measured over HTTP.

## The roadmap

Three tracks. **A** is measurements, **B** is runtime, **C** is the product
engine. Every B item names the A measurement that gates it and the one that
proves it; nothing in B ships before its A number exists. Phases are ordered
cheapest first. Sizes are S/M/L.

### Phase 0 — harness prep and test debt (laptop, free)

| Item | Issue | What |
|---|---|---|
| A0 `engine/unit-costs` | #379 | A Tier-1 `exact` gate for the workflow axis: saves, transitions and directory ops per run per template, child starts per fan-out, reminder sets and shard saves per durable delay, deliveries per completion. Under `selfPolicy` only. Joins `BENCH_GATE_SCENARIOS`. Not named `workflow/*` — `wf-bench` filters by substring and would run it in the paid job. |
| A1+A2 generator and rig | #380 | `completionMode: Indexed` and reset-once on the loadgen Job; `offeredRate = rate × pods`; `WF_MAX_INFLIGHT` (default 5,000 — past 50/s the sleep ladder measures the generator's own cap) becomes a ladder value; a `timeline` sampler (`kubectl top`, Redis `INFO`) that finally records host CPU beside Redis CPU; `chaos=owner-kill`; the tcp gate for `wf-load`; `cpu=` and `sku=` in the shape; an `env` input and a longer timeout on `cluster-test.yml`. |
| A3 `wf-fleet.mjs` | #381 | N `server.mjs` processes on one box over one local Redis, N generators at `rate/N`, rows merged with `mergeWfRows`; opt-in `wf-local/*` scenario with its own shape prefix. The first multi-core rig. |
| A4 `reminders-redis/arm-fire` | #382 | N in-process hosts on `redisStorage`, an arm-rate ladder to 1,000/s, `set_failure_ratio`, commands per arm, Redis CPU per 1k arms, shard bytes. |
| C0 engine test debt | #383 | `faultStorage` / `faultReminders` decorators; crash-mid-task at-least-once; CAS-loss → `timer-fallback`; `publishFailures`; `joinRepairs`; ring overflow; a `REDIS_URL`-gated run of the cluster suite. No file under `perf/aks/src/` changes. |

### Phase 1 — local numbers (laptop, ~1 h)

| Measurement | Rig | Decision it makes |
|---|---|---|
| L1 multi-core, hosts = 1, 2, 4, 8 | #381 | ≥ 6× at 8 hosts: one host per core is a recipe plus a chart pattern, not a runtime change. ≤ 3×: something shared (Redis on loopback, the singleton aggregator, the reminder writer chain) caps a box, and G1 must be designed to isolate it. |
| L2 reminder CAS ladder, N = 1, 3, 8, 16 tickers | #382 | The failure rung on loopback is the **upper bound** for any cloud figure. Under 1,000/s here justifies B2 before a Tier-3 minute goes to the sleep ladder. |
| L3 drown-vs-shed, 8 hosts, 50 → 500 runs/s, `WF_TASK_MS` 20 and 2 | #381 | The first rung where `queued` grows without bound while `start_deferred_ratio` stays 0 is B1's evidence; the first `publish_failures` / `completed_unreported` is B4's; generator CPU per run/s sizes the generator fleet for 1,000/s (~250/s per pod before `setTimeout` granularity makes Poisson bursty). |

### Phase 2 — mid-scale (100 runs/s, ≤ 20 hosts)

**B1. Admission control and back-pressure (M) — #384.** The one runtime
change needed at mid-scale; without it the 100 and 200 runs/s rungs are a
wedge, not a measurement.

- `'overloaded'` error kind, `ActorOverloadedError { scope, depth, limit }`,
  carried across the wire; never re-placed by routing.
- `defineActor({ maxQueued })` and `HostDefaults.maxQueuedPerActor`
  (0 = unlimited, the compat default), enforced in `Activation.enqueue`;
  system turns exempt. `HostDefaults.maxInflightTurns` at host level.
- Drop-on-dequeue: a call whose deadline expired in the queue is rejected
  without running its body.
- The #302 option-2 gauge: per-peer in-flight, refusals, and `saturatedMs`
  on the bounded fetch pool, through `ops()` and `prometheusOps`.
- Interactions: none with `retryQueuedOnConflict` (the cap keeps a queue
  short; the option keeps it alive); a refused reminder delivery is already
  re-armed one tick out (#306, #326); a refused topic delivery is a
  `failures[]` entry; one-way dispatch is refused synchronously at
  acceptance, which is what makes B4 safe. Sizing rule:
  `maxQueued ≈ callTimeoutMs / p50 turn ms`.
- Proof: Tier 1 `dispatch/overload-shed` and `dispatch/expired-skipped`
  (`exact`); Tier 3 T1 — "shed, not drown" means `stuck_ratio 0`, errors
  are refusals rather than `call-timeout`, completed/s at 200 offered is
  not below 50 offered, and per-host `queued` returns to ~0 within a drain.
- Also S: an `ops()` gauge and dev warning when a sharded reminder record
  passes N entries, so the reminder ceiling announces itself.

**Tier-3 sessions on the existing estate — #391.** T1 the throughput ladder
to 200 (before and after B1); T2 the sleep-rate ladder at a 1 s tick with the
generator cap raised; T5 Redis CPU versus host CPU riding both; T3 the
`TRANSPORT=tcp` arm; T4 chaos plus load with an owner kill mid-ladder.

### Phase 3 — make.com scale

**B2. `redisReminders()` and `remindersConformance` (M) — #385.** A
due-time ZSET plus Lua scripts for set, clear, claim-batch and re-arm-failed,
mirroring `pgReminders` through the unchanged seam; `ownsShard` ignored;
server time; arm O(log N), tick O(due), so `reminderTickMs` can drop to 5 s.
There is no reminder conformance suite today — the contract is re-pinned in
four test files — so one is added in `packages/actors/src/testing/` and run
by every provider. Gated on L2 and T2 locating the sharded ceiling; proven
by T2 re-run under a `REMINDERS=redis` knob in the shape.

**B4. One-way publish (S–M) — #49.** `PublishOptions.delivery:
'settled' | 'accepted'`; the wire already carries one-way end to end. Opts
out of deadlock detection because nobody waits. Ships after B1. The engine
pattern beside it: shard the aggregator by `key:` and persist its ring with
`ctx.append`.

**B3. One host per core (S now, M later) — #386.** The k8s packing recipe
first (D8 pool, `replicaCount = 7 × nodes`, `requests.cpu 1000m`, the rule
sum(requests) ≤ allocatable − 1 core, resources in the shape); the fence is
the watchdog and the packing hazard is oversubscription. Later a
`spawnHosts()` helper on `@sigx/actors/node` for non-k8s users. Gated on L1;
proven by G1.

**Tier-3 sessions on the grown estate — #391.** G1 the D8 shape, 7 × 1 vCPU
against 1 × 7 vCPU; G2 sixteen hosts (the ladder, the sleep ladder on
sixteen distinct tickers, a rolling restart mid-ladder with command counts);
G3 a 90 min soak at the knee with 20% durable sleeps.

### Phase 4 — 100+ hosts and the long tail

- **B5. Redis membership as one HASH (M) — #387.** Heartbeat one `HSET`;
  refresh three commands at any N; ~35× fewer commands per join at n=100;
  opt-in layout with a loud failure on mixed layouts. Measure
  `k8sMembership` at scale first — a Lease watch is O(1) per change and may
  already be the answer on Kubernetes. Gated on G2's churn measurement.
- **B6. Hot-key attribution (S) — #388.** Attribute the 289 ops/s (serial
  versus `reentrant: 'always'` versus a routed client) before touching the
  runtime; write the rule for track C — no per-tenant singleton on the hot
  path.
- **B7. Storage, measured not designed (S) — #389.** Commands, bytes and
  Redis CPU per run from the timeline sampler. Only if Redis CPU passes
  ~60% before hosts saturate does storage need design, and the first move
  is hash-tagged keys for Redis Cluster, not a batching seam. The engine
  lever worth ~3× fewer saves: `durability: 'eventual'` for intermediate
  transitions.
- **The 1M-sleeping attempt** on B2 plus G2's estate; 120k (200/s × 600 s)
  is the intermediate proof.
- **Weekly `wf-bench` — #60**, scoped to a weekly diff against the previous
  artifact posted on #378.

### Track C — from perf workload to product engine (#390)

The engine in `perf/aks/src/workflow` is a pinned Tier-3 workload: every
`WF_*` knob is `INFRA_SHAPE`, its counters are an `ops()` section the rig
sums, and `run.actor.ts` imports the rig-bound `defineActor`. A product
feature there is a re-baseline. The product engine is therefore a new
package, `@sigx/actors-workflow`, peer on `@sigx/actors` only, with its own
size budget; it copies the node vocabulary and keeps the four design rules
from `run.actor.ts` (event-driven with the zero-delay `advance` hop; wakes
token-fenced from `seq`; at-most-once wakes counted, with touch recovery; a
turn never awaits another host).

What a first product needs and today's runtime already supports: a per-run
event log on `ctx.append`; exponential backoff with jitter; a bounded inbox
and idempotent signals; a cancel cascade; a principal model (tenant in the
key, snapshot at start, `WorkflowRun` internal behind an authorised
façade); a per-tenant quota actor; a cron schedule actor with occurrence-
derived run ids; a webhook endpoint with an idempotency-key-derived run id;
a per-tenant/day index written by detached call, not by topic; one
retention reminder per tenant-day; a dead-letter queue.

Which product gaps are scale questions in disguise, so they are not
double-counted:

| Product gap | The scale question underneath | Owner |
|---|---|---|
| Per-tenant quotas holding under saturation | The host must be able to refuse (B1) | Runtime |
| Durable history throughput | `appendText` throughput per store (B7) | Runtime measures, product sets the write shape |
| Cron at 1M schedules, retention reminders | The reminder ceiling (B2) | Runtime; product keeps one entry per schedule and per tenant-day |
| Sub-minute triggers | Resident-actor liveness after host death | Runtime |
| Topic-triggered starts, `completedUnreported` | Durable topics, adjacent to #49 | Runtime |
| Tenant-day index and quota actors as hot keys | The single-activation ceiling (B6) | Product sidesteps by sharding keys |
| Cancel cascade, idempotent signal, backoff, DLQ, version migration, durable `parallel` | None | Product only |

## How to read a result against this note

- A Tier-3 number is never `exact`; `stuck_ratio` and
  `reminder_set_failure_ratio` gate hardest. A Tier-2 `wf-local` number is
  evidence in its counts and a decision in its **ratios**; its absolute
  timings are informational, as the tier legend in `BASELINES.md` says of
  every process-per-host rig.
- Every result lands as a dated section in `BASELINES.md` and a verdict
  comment on #378. When a decision number above comes in, edit the phase it
  gates here in the same PR — this note moves with the measurements the
  way the seam notes move with the code.
- A workstream whose gating measurement contradicts its premise is closed
  with the number attached, the way #119 was, rather than built anyway.
