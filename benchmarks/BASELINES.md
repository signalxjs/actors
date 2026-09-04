# Baselines

Reference figures, recorded by hand. `benchmarks/baselines/local.json` is your
machine's working baseline and is gitignored — absolute numbers from someone
else's laptop are not comparable. This file exists so the **relationships**
between layers are reviewable in a PR, since those hold across machines even
when the absolute figures do not.

Update it deliberately, when a change moves something here, and always record
the machine.

> **Renamed since these were recorded:** `Mailbox` is now
> `Turns`, and the scenario `dispatch/mailbox-raw` is now
> `dispatch/turns-raw`. Dated entries below are left exactly as they were
> measured — they are records of runs, not documentation — so read
> "mailbox" in them as "turns". A saved local baseline from before
> the rename will not match that one scenario; re-record it.

## Tiers — read this before quoting a number

### Tier 3 — a real deployment (`infra/*`)

Measured against a deployed cluster over its public endpoint: TLS, an
ingress, a placement policy, an edge hash, a directory in Redis. **Not
comparable to Tier 1 or Tier 2 numbers** — a Tier 1 dispatch figure counts
microseconds inside one process, a Tier 3 figure counts a request that
crossed the internet, a proxy and possibly two hosts.

**One Tier-3 section is not on that path at all.** The WebSocket
connection-scale numbers (#172) are driven from Jobs INSIDE the cluster
straight at the Service — no ingress, no TLS, no load VM — because the
question there is what the runtime can hold and deliver, not what the edge
can carry. They are not comparable with the HTTP sections either, and they
are run with `testenv.mjs ws-load` rather than through `bench:infra`. The
public socket path is deliberately unmeasured so far; when it is measured
it gets its own section, because it will be ingress-limited rather than
runtime-limited.

Since #184 they are also RECORDED rather than hand-run:
`testenv.mjs ws-bench [--save-baseline|--compare]` drives the `sockets/*`
scenarios through the ordinary baseline machinery, with `INFRA_SHAPE`
carrying the actors release's socket caps so a run under a different
`SOCKET_MAX_SUBSCRIPTIONS` is refused rather than compared. The figures in
the 2026-08-09 section below predate that and were read off a terminal;
treat them as the first measurement. The 2026-08-10 section is the first
one produced BY the harness, and is what a later run should be compared
against.

A THIRD axis joined in #297: `workflow/*`, the workflow engine (the
headless workload #85 asked for). Driven in-cluster like `sockets/*`, and
measuring an engine's nouns — runs started and completed per second,
end-to-end latency per template, the delay-node wake lag, what a fan-out
join and a lost wake cost — over real reminder shards, a real worker pool,
real cross-host child runs and one Redis CAS per node. `testenv.mjs
wf-bench [--save-baseline|--compare]`, shape prefixed `wf` and carrying
every `WF_*` host knob. Not comparable with either of the other two.

Opt-in (`BENCH_INFRA=1` plus an `INFRA_URL`, a signed-cookie secret, and a
load VM), and the load is driven FROM A VM IN THE CLUSTER'S REGION: the
same ladder run from a laptop across an ocean varies 50-80% run to run and
cannot detect a 30% regression. Comparisons are refused outright when the
deployment shape (replica count, how many distinct NODES those replicas
span, image tag, and the runtime knobs) differs from the baseline's — three
replicas packed on one node and three spread across three read identically
in every report and differ by more than 2x.

The knobs are in the shape because they change the curve WITHOUT changing
the image: `PLACEMENT`, `REBALANCE*`, `MAX_ACTIVATIONS` /
`SWEEP_INTERVAL_MS`, `DIGEST_*` and `MIGRATE_PERSIST`. `PLACEMENT` is the
sharpest case — `activation-count` steers at the least-loaded host while
the chart's ingress hashes to the owner, so under it `locality-ab`'s
`token_speedup` no longer measures what its name says. Two configurations
get compared deliberately, with `bench:diff --before --after`, never by
`--compare`.

Three of the six scenarios exist for the features only a real deployment
exercises: `worker-pool` (a `defineWorker` pool against the same body on
one activation — and the only place the pool's DEFAULT size, the node's
real `hardwareConcurrency`, is ever used; it turns out to be **1** inside a
container with a CPU limit, see #147), `cold-placement` (fresh keys, so
every call pays an activation; read `ops_per_sec` and `ownership_spread`
together, because a policy can buy latency by wrecking balance) and
`topics-fanout` (whether a singleton subscriber caps cluster-wide write
throughput, and at what concurrency).

No Tier-3 metric is `exact`, and none can be — every one rides a real
clock, a real network and a real scheduler. `error_rate` is the one that
gates hardest, at a 1% noise floor: a stale serverFn id 404s every write,
and a 404 is CHEAPER than a write, so without it a wholly broken run
reports as a throughput WIN. Not hypothetical — that is exactly how the
id pasted into `edge-ladder.mjs` went stale unnoticed.

Recorded with `node perf/aks/deploy/testenv.mjs baseline`;
compared by `… testenv.mjs test`. Absolute capacity is a different
question from regression detection — for that, `testenv.mjs load` runs the
generator directly with as many clients as you care to point at it.

Figures here come from two different kinds of measurement, and confusing them
is how a modelled number ends up cited as a fact — which is exactly what the
tier legend below exists to prevent. The scenario name says which tier it is:

| Tier | Scenarios | What it is |
|---|---|---|
| **Tier 1** | `dispatch/`, `state/`, `wire/`, `lifecycle/`, `memory/`, `cluster/` | One process, `pipeFetch`, **zero sockets**. Measures algorithmic shape and software cost. Anything about the network here is *modelled*, not measured. |
| **Tier 1, engine** | `engine/` | One process, one host, the workflow engine of `perf/aks` under `selfPolicy`, one run at a time: what a run costs the runtime as INVARIANTS — saves, transitions, directory calls, reminder-shard writes, completion deliveries. Every metric is `exact` and gates the merge queue; it is the Tier-1 face of the `workflow/*` axis, which cannot gate (#379). |
| **Tier 1, opt-in** | `compute/` | One process, but **real OS threads**. Opt-in via `BENCH_THREADS=1`, because the effect cannot appear on a 2-core runner: a number recorded there would be wrong rather than noisy. Never gates. |
| **Tier 2** | `cluster2/` | N hosts as **real OS processes over real loopback sockets**. Measures what the wire actually does. Opt-in: `pnpm bench:tier2`. |
| **Tier 2, with a store** | `wf-local/` | The workflow engine as N **real host processes on ONE box** over a local Redis, driven by the real generator through a round-robin proxy (#381). The multi-core rig: counts are evidence, every timing is `informational`, and the decision number is the **ratio** between fleet sizes measured back to back on one box. Shape prefix `wf-local`, never comparable with `wf`. Opt-in: `pnpm bench:wf-local` (needs `REDIS_URL`). |

Within Tier 2 there is a second split, and it is enforced in code rather than
by this paragraph — every timing metric is emitted `informational: true`, so
the comparer *cannot* gate on one:

- **Counted, and trustworthy on a busy machine**: sockets, bytes per call,
  requests per connection. These are counts of events; CPU contention does not
  change how many connections a transport opens.
- **Timed, and contended**: throughput, percentiles, RSS. N processes share
  the cores. Recorded for context; never evidence on its own.

Three Tier-1 scenarios ride a REAL store rather than `memoryStorage` and
are env-gated on `REDIS_URL` so the default suite still runs anywhere:
`cluster/redis-amplification` (commands per membership change),
`reminders-redis/arm-fire` (#382: the arm rate at which the sharded
reminder table's CAS starts failing) and `reminders-redis/table-size`
(#382: what a set and a tick cost with P entries already asleep in the
records). Tier 1 with an external store — one process, real round trips:
the counts and ratios are the findings, the timings depend on the box and
the store both.

---

## 2026-07-28 · initial baseline

| | |
|---|---|
| Machine | Apple M4, darwin/arm64 |
| Node | v24.11.1 |
| Build | `dist/*.prod.js` (`--conditions=production`) |
| Commit | `cf44a1a` |
| Settings | 5 rounds × 400 ms, interleaved |
| Conditions | **Contended** — the probe varied 87% across rounds. Absolute values are pessimistic and worth ±20%; the ratios below were measured within a single interleaved run and are the trustworthy part. |

### The dispatch ladder — uncontended (`c=1`, ops/sec)

| Layer | Throughput | Cost of this layer |
|---|---:|---|
| `dispatch/mailbox-raw` | 7.50 M | — (the floor) |
| `dispatch/warm-actor` | 1.95 M | **−74%** — placement, reentrancy check, directory lookup, turn bookkeeping |
| `dispatch/warm-actor-deadline` | 1.21 M | **−38%** — `raceDeadline`, i.e. the *default* `callTimeoutMs: 30_000` |
| `dispatch/via-proxy` | 1.07 M | **−12%** — client proxy + `mintCallId()` |
| `wire/endpoint-roundtrip` | 115 k | **−89%** — wire codec, JSON, endpoint (no socket) |

> **Annotated since (#23):** the `wire/endpoint-roundtrip` figure includes the
> benchmark's own `new Request(...)` construction inside the timed closure
> (see the 2026-07-31 profile note below), so the −89% overstates the
> endpoint. `wire/request-construction` now prices that setup as its own
> rung; the corrected attribution awaits the next recorded run.

Two findings stand out:

- **The default call deadline costs ~38% of dispatch throughput.** Every
  dispatch with a non-zero `callTimeoutMs` allocates a promise and a
  `setTimeout` in `raceDeadline`. The default is 30 s, so essentially every
  production call pays it. **Fixed since:** far deadlines now share
  one coarse registry tick (`host/deadlines.ts`) — see item 1 below.
- **A turn through the host costs ~4× a bare mailbox turn.** The mailbox itself
  (a promise chain, ~4 promises per turn) is not the dominant cost at this
  layer; what sits on top of it is. **Since 2026-07-31 we know what:** two
  `async` functions whose warm path never awaits — see "Where the time goes".

### Queueing (single actor vs many)

| Scenario | c=1 | c=64 | c=512 | c=64 p50 |
|---|---:|---:|---:|---:|
| `dispatch/warm-actor` (one actor) | 1.95 M | 1.86 M | 1.56 M | 36.5 µs |
| `dispatch/fan-out-actors` (1 000 actors) | 1.65 M | 1.66 M | 1.55 M | 40.1 µs |

Throughput is flat in concurrency while p50 grows linearly with queue depth —
the mailbox serializing turns, as designed. Fan-out does not currently beat the
single actor, because this is one process: the runtime is not the bottleneck,
the single JS thread is.

### Persistence and the change feed

| Metric | Value |
|---|---:|
| `state/explicit-save` tiny (`{count}`) | 591 k saves/s |
| `state/explicit-save` large (200 rows) | **3.8 k saves/s** |
| `state/write-behind` (c=1) | 952 k ops/s |
| `streams/changes-fanout` 0 subscribers | 1.62 M ops/s |
| `streams/changes-fanout` 1 subscriber | 611 k ops/s |
| `streams/changes-fanout` 16 subscribers | 136 k ops/s |

- **Saving 200 rows is ~155× more expensive than saving one field.** `ctx.save()`
  encodes the whole state and `memoryStorage` `structuredClone`s it on the way
  in — the cost tracks state size, not the size of the change.
- **The first change-feed subscriber costs 62% of throughput**, and it keeps
  costing after that (16 subscribers → another 4.5×). ~~`#snapshot()` is
  `revive(encode(raw))` — two full deep walks per mutating turn — plus per
  subscriber delivery.~~ **Corrected 2026-07-31 by profile:** the snapshot is
  computed once per turn and shared across subscribers (`activation.ts:705`),
  and `cloneState` does not appear in the profile at all. The first subscriber
  installs a `watch(…, {deep: true})` (`activation.ts:1322` → `:771`) that
  re-traverses state on every mutation; the 1 → 16 step is delivery cost. See
  "Where the time goes".

### Lifecycle and background jobs

| Metric | Value |
|---|---:|
| `activation/cold-cycle` | 152 k cycles/s (4.2 µs p50) |
| `sweeper/scan` 10 k activations | 269 µs |
| `sweeper/scan` 50 k activations | 1.12 ms |
| `reminders/tick` 2 k reminders | 2.42 ms |
| `wire/guard-chain` unguarded → 2 guards | 124.9 k → 124.2 k ops/s (~0.6%) |

Activation is cheap (4.2 µs including a storage load and teardown), so idle
collection is affordable. The sweeper is linear at ~22 ns per activation — 50 k
actors cost about a millisecond per 60 s tick, which is nothing. Guards are
essentially free per request.

### Memory

| Metric | Value |
|---|---:|
| `mem/per-actor-footprint` tiny | **4.1 KiB per actor** |
| `mem/per-actor-footprint` large (200 rows) | 46.3 KiB per actor |
| `mem/leak-activate-deactivate` | 3 B retained per actor per cycle |
| `mem/leak-streams` | 0 B retained per stream |
| `mem/leak-timers` | 3 B retained per timer |
| `mem/soak-steady-state` | 486 B/sample slope (≈ 0) |

**A live actor costs ~4 KiB**, so a gigabyte of heap holds roughly 250 k idle
activations of a trivial actor. No leaks detected on any path: activate/
deactivate, change-feed streams, and volatile timers all return their memory,
and a mixed steady-state workload holds a flat heap.

The detector was verified against a planted leak (an actor retaining its context
on activation): **626 B/actor** vs **3 B/actor** clean.

---

## 2026-07-28 · cluster scaling, N = 1 … 100

In-process (`benchmarks/src/cluster-harness.ts`), one CPU shared by every
host. **Absolute throughput here is meaningless** — 100 hosts contending for
one core. What is exact is the algorithmic shape: provider calls and
per-decision cost as a function of N. Node v24.11.1, Apple M4, prod dist.

| property | N=1 | N=100 | verdict |
|---|---:|---:|---|
| directory calls per cold activation | 2.00 | **2.00** | ✅ flat |
| membership `view()` per activation | 1.00 | **1.00** | ✅ flat |
| notifications per membership change | 1 | **100** | ❌ O(N) |
| hosts doing reminder work | 1 | **16** (84 idle) | ❌ hard ceiling |
| locality, random policy | 1.00 | **0.01** | ⚠️ 1/N, structural |
| locality, consistent-hash | 1.00 | 0.00 | ⚠️ same |
| locality, edge-hash × prefer-local | 1.00 | **1.00** | ✅ fixed by routing |
| `choose()` random | 14.8 M/s | 3.7 M/s | ⚠️ 4× slower |
| `choose()` consistent-hash | 3.9 M/s | **61.8 k/s** | ❌ 63× slower |

### ✅ The most important result is a positive one

**Activation cost does not grow with cluster size.** Two directory calls
(one `lookup`, one `claim`) and one cached `view()` per cold activation,
identical at N=1 and N=100. The directory is a shared service, but the
runtime's demand on it per activation is O(1) — so adding hosts adds
capacity rather than adding load per unit of work. That is the property
scaling depends on, and it holds.

### ❌ Membership change is O(N²) against a remote provider

One host joining an N-host cluster produces **exactly N notifications** —
measured, not modelled. That much is inherent: every host must learn.

The cost is in what a notification *becomes*. Measured against **Redis
8.8.1** (`CONFIG RESETSTAT`, add one host, `INFO commandstats`, with
heartbeat and poll pushed out so only the join's traffic is counted):

| n | commands for ONE join | of which `hget` | refreshes |
|---:|---:|---:|---:|
| 1 | 21 | 4 | 2 |
| 10 | 156 | 121 | 11 |
| 50 | 2 716 | 2 601 | 51 |
| **100** | **10 416** | **10 201** | 101 |

`hget` is exactly `(n + 1)²` at every point, and the total fits

```
commands = (n + 1)(n + 3) + 13
```

exactly across the whole sweep. **The quadratic is measured, not argued.**
Each of the `n + 1` members (the joiner included) runs a `refresh()`, and
each refresh is one `SMEMBERS`, one `GET` of the version key, and one `HGET`
per id that came back.

A rolling restart of 100 hosts is ~200 membership changes — on the order of
**2 M Redis commands**, in bursts.

> Reproduce with `REDIS_URL=... pnpm bench:run cluster/redis-amplification`.
> This figure was previously *modelled* from the provider source and was
> wrong twice — 10 100, then 10 200 — because the model missed that the
> joining host also refreshes (`n + 1` refreshers, not `n`) and that each
> refresh reads the version key as well as the member set (2 fixed
> commands, not 1). The `+ 13` is the joiner's own write path plus the
> `CLIENT SETINFO` handshake ioredis performs on each new connection. It is
> measured now.

Cheapest fix is a debounce/coalesce on the subscriber path: N changes
arriving together should cost one refresh, not N.

**Since fixed (#26):** `refreshCoalescer` (core, wired into redis/pg/surreal)
makes the subscriber single-flight with a version gate. An isolated join is
unchanged by design (one notification per subscriber = one refresh). The
burst is the case it exists for — measured on loopback (the WORST case for
single-flight, since a refresh completes between publishes), k=10 concurrent
joins into n=50: refreshes 510 → 292, commands per join 3 080 → 1 709; with
`coalesceMs: 25`, 213 and 1 264. A real network RTT widens the coalescing
window for free. The `burst` arms of `cluster/redis-amplification` carry
this measurement.

### ❌ Reminders stop scaling at 16 hosts

`REMINDER_SHARD_COUNT = 16`, pinned as storage identity ("never change
either"), with rendezvous ownership. Measured hosts owning ≥1 shard:

| N | 1 | 2 | 10 | 50 | 100 |
|---|---:|---:|---:|---:|---:|
| hosts with reminder work | 1 | 2 | 9 | 13 | **16** |
| idle hosts | 0 | 0 | 1 | 37 | **84** |

Note N=50 gives 13, not 16 — rendezvous collisions leave some shards
doubled up before the ceiling is even reached. Reminder throughput is capped
at 16 hosts' worth regardless of cluster size, and the shard count cannot be
raised without an explicit storage migration.

### ⚠️ Locality is 1/N, by design

Nothing routes a caller toward the owner of an actor, so a request landing on
an arbitrary host finds the owner ~1/N of the time — measured 1.00, 0.54,
0.10, 0.02, 0.01 for N = 1, 2, 10, 50, 100. Neither shipped policy improves
on that, because neither considers the caller; the small differences between
them in the table are sampling noise around the same ~1/N curve.

This is normal for a virtual-actor runtime and not a bug, but it fixes the performance model:
**at any real cluster size essentially every call takes a network hop**, so
cluster throughput is network-bound, and the shared state above is what
decides whether it scales.

### ✅ …and routing fixes it, if the edge and the policy agree to compose

`cluster/locality-routed` measures the steady state (ownership established
first, then measured — the probe above is cold-only, which makes it
structurally 1.00 under any caller-affinity policy and unable to evaluate
this at all). Local fraction:

| edge × placement | N=2 | N=10 | N=50 | N=100 |
|---|---:|---:|---:|---:|
| round-robin × random *(default)* | 0.50 | 0.12 | 0.02 | 0.01 |
| hash token × consistent-hash | 0.48 | 0.09 | 0.02 | 0.01 |
| **hash token × prefer-local** | **1.00** | **1.00** | **1.00** | **1.00** |

The edge hashes the routing token now in the request line; the third
row is the whole point — the load balancer *becomes* the placement, because
whoever receives the first call activates the actor locally and the same key
hashes back to that host forever. The two agree on nothing but stability.

The middle row is the result worth keeping: **deterministic placement is an
anti-pattern here, not a middle ground.** The edge's hash and the cluster's
rendezvous hash are different functions over different sets, so they
disagree on most keys — consistent-hash under edge hashing is no better than
random. Caller affinity composes; determinism does not.

These are exact ratios from deterministic bookkeeping, not timings, so the
noise warning in the runner does not apply to them; the spread on the low
rows is real run-to-run variance in random placement.

### ⚠️ …but caller affinity is not free, and is not a default

`cluster/locality-routed` above measures placement under a STABLE edge.
`cluster/locality-warm` asks the question a running cluster has — of the
calls I am making now, how many hop? — and adds the arm that decides whether
`preferLocalPolicy()` should be the default. N=100, 240 actors:

| edge × policy | local fraction | ownership spread |
|---|---:|---:|
| round-robin × random *(default)* | 0.02 | 2.92 |
| round-robin × prefer-local | **0.00** | 1.25 |
| hash the routing token × prefer-local | **1.00** | 2.50 |
| skewed LB × random | 0.00 | 2.92 |
| skewed LB × prefer-local | 0.80 | **80.4** |

Ownership spread = most-loaded host's actors ÷ mean; 1.0 is even, N is "one
host owns everything".

**Prefer-local buys nothing under a round-robin balancer** (0.00 vs 0.02,
both noise around 1/N) —
it pins each actor where its first call landed and the balancer sends the
next one elsewhere regardless. **And under an uneven balancer it concentrates
ownership 80×**, which is exactly the situation — a rolling deploy, a bad
health check — where you can least afford one hot host. Actors do not move
back, because placement applies only to new activations.

So the default stays random: it holds ~2.9 spread whatever the edge does.
Prefer-local is the right answer only when the edge hashes the routing token,
where it is worth the whole 69× cross-host cost.

Two measurement traps this scenario had to avoid, both of which silently
flatter prefer-local:

- **Do not derive locality from `routedLocal`.** `dispatcherFor` returns the
  local dispatcher *before* the routing path that increments it, so in the
  warm state a local hit counts nothing at all, and
  `routedLocal / (routedLocal + remoteDispatches)` reads near-zero for a
  perfectly local cluster. Count `remoteDispatches` against a known call
  count instead. (Since #52 the placement also exposes a per-request pair,
  `dispatchesLocal` / `dispatchesRemote`, that does count the warm path —
  that is what an operator reads; the scenario keeps its own denominator so
  the gates stay as baselined.)
- **A skewed edge needs its own counter for the traffic it does NOT skew.**
  Reusing one counter for both advances it by five per leftover call, so the
  "spread the rest" fifth only ever visits every fifth host — concentrating
  the very traffic the arm exists to spread.
- **Round-robin must be by arrival order, and the call order must not track
  actor order.** A counter iterated in actor sequence is a stable per-actor
  assignment wearing a costume, and scores 1.00 whenever `actors % N === 0`.
  The scenario visits actors on a coprime stride for that reason.

### Per-call cluster costs (N=2, so the numbers mean something)

| | ops/s | p99 |
|---|---:|---:|
| locally-owned dispatch | 1.40 M | 1.4 µs |
| cross-host, HMAC on | 20.3 k | 95.6 µs |
| cross-host, HMAC off | 68.0 k | 21.5 µs |

Cross-host costs ~69× a local call **in software alone** — no real network
is involved. HMAC accounts for ~35 µs of that per call (~17 µs per
sign/verify, against the ~9 µs the comment in `cluster/envelope.ts`
claims — worth correcting, though see below).

Keep this in proportion: a real LAN round trip is 200-1000 µs, which
dominates the ~50 µs of software. **HMAC and `choose()` are not what limits
a real deployment** — membership churn and the reminder ceiling are.

> ⚠️ **Tier 1 — no sockets.** These come from `createCluster` → `pipeFetch`,
> which routes in-process. They are the *software* cost of a hop. The
> `cluster2/` section below measures the same comparison over a real socket
> and lands somewhere quite different; prefer it when reasoning about
> deployment.

---

## 2026-07-28 · Tier 2 — real sockets, N processes

| | |
|---|---|
| Machine | Apple M4, darwin/arm64 |
| Node | v24.11.1 |
| Build | `dist/*.prod.js` (`--conditions=production`) |
| Rig | `cluster2/*` — `child_process.fork` per host, loopback TCP, store in the parent over IPC |
| Settings | 5 rounds × 400 ms, interleaved |
| Conditions | **Contended** — the probe varied 15%, so the suite printed `THE MACHINE WAS BUSY`. The *counts* below were nevertheless identical every round; that is the point of the tier split. |

### The connection pool sizes to concurrency, at 2× [counted]

Two hosts, one driving, every call crossing to the owner:

| concurrency | peak sockets | sockets / concurrency | requests / connection |
|---:|---:|---:|---:|
| 1 | 2 | **2.00** | 1 163 |
| 8 | 16 | **2.00** | 531 |
| 64 | 128 | **2.00** | 69 |

Two things are true at once:

- **Keep-alive works.** 69–1 163 requests per connection; connections are
  reused heavily rather than opened per request.
- **The pool still sizes to concurrency**, at *twice* the projected rate. The
  model predicted one connection per in-flight request; the measurement is a
  flat **2.00** at every concurrency tested.

Extrapolated to c=64 against 99 peers — that is **~12 600
sockets per host**, not 6 300. The extrapolation is still a model (this rig
has 2 hosts, not 100), but the per-peer coefficient it rests on is now
measured rather than assumed.

*Why 2× and not 1× is not yet explained.* It reproduces exactly across
concurrency and is confirmed independently by libuv's own TCP handle table on
both ends, so it is a real property of the current client and not a counting
artifact. The cause is established below: it is the unbounded pool growing,
and capping it removes the growth exactly.

### HMAC costs far less over a real socket than in-process [timed, contended]

| | c=1 ops/s | c=64 ops/s | bytes/call |
|---|---:|---:|---:|
| HMAC on | 4 223 | 15 111 | 637 |
| HMAC off | 5 882 | 17 940 | 534 |
| **ratio** | **1.39×** | **1.19×** | +103 B |

**Tier 1 put this ratio at 3.35×** (68.0 k vs 20.3 k). Over a real socket it
is 1.19× at c=64 — the socket latency hides most of the signing cost, exactly
as the "keep this in proportion" note above predicted, and now measured
instead of argued.

The practical reading: **removing per-call HMAC is worth far less than the
in-process figure suggests.** It remains worth doing — it is ~30 lines with
session tokens and 103 bytes/call on the wire — but it is not the
3× headline, and it is not on its own an argument for a new transport.

### Throughput plateaus at the wire, not at the runtime [timed, contended]

18–19 k ops/s at c=8 and above, against the ~17 k/s a bare Node HTTP echo
server reaches on this machine. The runtime is not the ceiling here; Node's
HTTP stack is.

### Bounding the pool fixes it; HTTP/2 does not [counted + contended]

Three candidates — HTTP/2, a bounded pool, and documenting the escape hatch —
at concurrency 64 against one peer, all driven through the existing `fetch`
seam so no runtime change was needed to test them:

Measured with the **undici 7.29** package against a Node whose bundled global
`fetch` was undici 7.16 — i.e. Node 24. **That is not the major every
supported Node bundles:** the package's `engines` range is
`^20.19.0 || >=22.12.0`, and Node 22.22.0 ships undici **6.23.0**. The CI
matrix includes Node 20, which is on undici 6 as well. That choice matters —
see the version note below.

| dispatcher | peak sockets | sockets / concurrency | ops/s | vs default |
|---|---:|---:|---:|---:|
| default (global fetch) | 128 | 2.00 | 15 600 – 16 095 | — |
| **`Agent({ connections: 64 })`** | **64** | **1.00** | **16 840 – 17 121** | **+6%** |
| `Agent({ connections: 8 })` | 8 | 0.13 | 5 407 – 5 434 | −66% |
| `Agent({ connections: 1 })` | 1 | 0.02 | 6 661 – 6 941 | −57% |
| `Agent({ connections: 8, allowH2 })` | 8 | 0.13 | 5 239 – 5 385 | −66% |
| `Agent({ connections: 1, allowH2 })` | 1 | 0.02 | 6 851 – 6 946 | −56% |

**Candidate 2 wins, but only at the right size.** Capping the pool *at* the
concurrency halves the socket count and is marginally **faster** than the
unbounded default — which also answers the "why 2×?" left open above: it is
the unbounded pool growing, and the cap removes the growth exactly. The extra
connection per in-flight request is waste, not headroom.

Going *below* the concurrency trades throughput steeply: `connections: 8` at
concurrency 64 costs about **3×**. Worth it only when file descriptors are the
actual constraint. (`connections: 8` being slower than `connections: 1` is
consistent across runs and unexplained; both are far off the pace, so it does
not change the recommendation.)

Extrapolated the same way — c=64 across 99 peers — matching the cap to
concurrency gives **~6 300 sockets per host instead of ~12 600**, at no cost.
Getting below that is a deliberate throughput trade.

> ⚠️ **These numbers are undici-major-specific, and the supported Node range
> spans majors.** An earlier pass of this table was recorded against undici
> 8.x and showed `connections: 8` costing only ~2% — a conclusion that would
> have shipped a bad default had it not been re-run. The table above is
> undici 7; Node 20 and 22 bundle undici 6. Re-measure on your own target
> before tuning, and do not quote these figures as if they held across the
> whole range.

**Candidate 1 is not reachable.** `allowH2: true` measures identical to plain
keep-alive at every pool size (5 844 vs 5 862 at one connection; 13 254 vs
13 548 at eight). `createAppHandler` serves over `node:http`, which is
HTTP/1.1 only, so the client negotiates nothing and silently falls back.
Multiplexing would require a `node:http2` server first — a much larger change
that buys the same socket reduction the pool cap already gives.

Candidate 3 — documenting the escape hatch — is therefore the whole shipped
change, now with a measured recommendation attached. See
[Host transports](https://sigx.dev/actors/docs/transports/). Shipping it as
code rather than prose is #118.

### Session tokens: measured, and declined

The plan carried an idea to replace the per-call HMAC with a short-lived
session token, on the strength of the Tier-1 3.35×. Over a real socket the
gap is **1.19× at c=64**, and a session token authorises *any* call for its
window where today's signature is bound to a specific symbol + callId.

**A 19% gain does not justify weakening that binding**, so this is declined
rather than deferred. Revisit only if a profile shows signing dominating on a
deployment where the network is not the cost — which is the opposite of what
the numbers above show.

### The transport decision

All three on one rig, back to back, against the **tuned** HTTP baseline
(pool bounded to the concurrency) rather than the shipped default —
comparing a new transport to an untuned incumbent would flatter it. Two hosts,
concurrency 64.

> The WebSocket rows below are **history**: the host-to-host WS transport was
> retired in #151 (an edge runtime should be a *client* of the deployment —
> #99 — not a cluster peer, and TCP is the one blessed socket transport for
> hosts). The numbers stay because they were measured and because they
> record what a `messageOriented` FrameLink costs relative to raw TCP; the
> scenario now runs tuned HTTP vs TCP only.

| | TCP handles | ops/s | p99 | bytes/call |
|---|---:|---:|---:|---:|
| tuned HTTP | 65 (1 listener + 64 conns) | 14 287 | 9.6 ms | 640 |
| **TCP** | **3** (2 listeners + 1 conn) | **69 768** | **1.58 ms** | not observable |
| **WebSocket** | **2** (1 listener + 1 conn) | **63 495** | **1.58 ms** | **236** |

Against the gate agreed *before any of these transports existed*:

| criterion | threshold | TCP | WS |
|---|---|---|---|
| throughput ratio | ≥ 1.30 | **4.88×** ✅ | **4.44×** ✅ |
| p99 ratio | ≤ 0.80 | **0.17** ✅ | **0.16** ✅ |
| bytes/call ratio | ≤ 0.70 | not measured | **0.37** ✅ |
| sockets vs peers | ≤ 1.2× | **1.0×** ✅ | **1.0×** ✅ |
| tie-break | ≥ 2.0 | **4.88×** ✅ | — |

Bytes are read off the HTTP listener's sockets, so they are real for HTTP and
for WebSocket (which upgrades on that same socket) and **not observable for
TCP**, which owns a separate listener. Recorded as not-measured rather than
scored; TCP's framing is strictly leaner than WebSocket's — no HTTP upgrade —
so it would clear it, but that is an inference and is labelled as one.

#### The result contradicts what the transports' own READMEs claimed

Both packages were written saying this was "not about latency, only socket
count", on the strength of the in-process HMAC measurement above. **That was
wrong, and
this rig is what caught it.** Per-call HMAC really is worth only 1.19× — but
Node's HTTP *stack* is a separate and much larger cost, and a framed protocol
on a persistent socket skips it entirely. Corrected in both READMEs.

#### But the ratio is software-only, and a real network absorbs most of it

Per call: tuned HTTP ≈ 70 µs, TCP ≈ 14 µs — a **56 µs** difference. On a LAN
with a 200–1000 µs round trip that is 570 µs vs 514 µs, or about **1.1×**. The
4.88× is a loopback number and belongs in the same bucket as everything else in
this tier: it measures software, and this rig systematically overstates
software's share of latency.

**The socket-count win is not absorbed that way.** 64 connections per peer
versus 1 stays true at any RTT, and that is the property to choose on.

#### The verdict: HTTP stays the default anyway

The gate is cleared, and the default does not change — because the gate was
missing a constraint that no measurement can express. **`@sigx/actors/cluster`
must stay zero-dep and WinterCG-clean**, which is the whole reason the host
wire is HTTP: a default requiring `node:net` would break Cloudflare
Workers outright, and HTTP is the only transport that runs everywhere.

So the recommendation is stronger than "reach for TCP when file descriptors
hurt", which is what the packages currently say:

- **On Node, prefer `@sigx/actors-tcp`.** It clears every measured
  criterion, most of them by a wide margin.
- **HTTP remains the default and the only portable option**, and with a bounded
  pool it is a perfectly reasonable one.

### What this rig cannot honestly measure

Stated so it is never quoted as though it could:

- **Absolute throughput at N ≥ 10.** N processes share one CPU. Needs N
  machines.
- **Anything RTT-shaped.** Loopback is ~30–60 µs against a LAN's 200–1000 µs,
  so this rig systematically *overstates* software's share of latency and
  *understates* connection setup. There is no TLS handshake here either.
- **Head-of-line blocking, congestion, loss, MTU.** Loopback has none — which
  is precisely the main risk of HTTP/2 multiplexing, so an h2 transport can
  look good here and behave worse on a real network.
- **Cluster-wide socket pressure at N=100.** The per-peer coefficient is
  measured; the 100-host total remains *modelled*.

The cheap thing that would fix most of this: **two machines at N=2**, giving a
real NIC and a real RTT.

---

## 2026-07-31 · Where the time goes — the first CPU profiles

Every figure above this line is a **subtraction between ladder rungs**: it says
what a layer costs, never which function spends it. These are the first stored
profiles, and they move three items on the list below — two of them because the
guess was wrong.

| | |
|---|---|
| Machine | 12th Gen Intel Core i9-12900HK, win32/x64, 20 threads |
| Node | v22.22.0 |
| Build | `dist/*.prod.js` (`--conditions=production`), source-mapped back to `src/` |
| Commit | `87f35d8` |
| Command | `pnpm bench:profile <scenario>` (`--cpu-prof`, `--runs=1`) |

Self-time percentages, resolved through the dist source maps. **The profiler
itself inflates absolute throughput ~2×** — read the shares, not the ops/sec.

### The finding that spans every profile: two async functions on a sync path

`#activationFor` (`host/local-host.ts:186`) is in the top six of **all four**
profiles — 11.6% of `dispatch/warm-actor`, 6.0% of `streams/changes-fanout`,
5.5% of `state/*`, 1.1% of `wire/endpoint-roundtrip`. `#checkReentrancy`
(`:145`) and `#dispatchInner` (`:121`) add 0.7–4.9% more.

Both are `async`, and **both return synchronously on the warm path** — the
overwhelmingly common one. `#checkReentrancy` returns `null` at `:147` when the
call chain is empty; `#activationFor` returns `slot.activation` at `:219` on a
`Map` hit. Neither awaits anything to get there, yet each allocates a promise
and burns a microtask tick per dispatch, and `actorId(ref)` (`types.ts:16`)
builds the same `` `${type}\u0000${key}` `` string twice (`:146` and `:187`).

That is the bulk of the **−74% between `mailbox-raw` and `warm-actor`** which
the ladder could only attribute to "placement, reentrancy check, activation
lookup, turn bookkeeping". It also explains the GC share: **4.3–8.9% of every
profile is garbage collection**, and the mailbox's own 4 promises per turn are
not the source — `host/mailbox.ts` is only 2.8% of the dispatch profile,
confirming item 4 below was correctly deprioritized.

A synchronous fast path in both, plus computing `actorId` once, is a contained
change with no API surface.

### `wire/endpoint-roundtrip`: the 89% is almost none of it ours

The rung the ladder calls "wire codec, JSON, endpoint" is dominated by request
plumbing, not by our serialization:

| what | self% |
|---|---:|
| `@sigx/server` `handleServerFnRequest` (`src/server/index.ts:433`) | 11.2% |
| undici `Request` / `Response` / `HeadersList` / `extractBody` | 14.2% |
| `node:internal/webstreams/readablestream` | 8.8% |
| `node:internal/async_hooks` (promise hooks) | 7.1% |
| native `JSON.parse` | 5.7% |
| `@sigx/server` `runInScope` — the ALS request scope (`:636`) | 2.2% |
| **all of `@sigx/actors`** | **~6%** |

Two consequences. **The WinterCG object model is the cost** — constructing a
`Request`, a `Response`, a `ReadableStream` and a `Headers` per call is ~23%,
and the AsyncLocalStorage scope adds ~7% in promise hooks. And **`@sigx/actors`
is a bit player on its own hottest rung**, so tuning our codec cannot move it
much.

Note also a measurement artifact: `actorRequest()` in
`benchmarks/src/scenarios/wire.ts` runs **inside** the timed closure, so part of the
undici share is the benchmark's own setup rather than the endpoint's work. The
rung overstates the endpoint. Worth splitting before anyone optimizes against
it. **Split since (#23):** `wire/request-construction` is the control —
subtract it from `endpoint-roundtrip` before quoting a wire cost.

### The change-feed cliff is the deep watch, not the snapshot

Item 2 below was wrong, in two ways.

`#snapshot()` is already computed **once per turn and shared** — `activation.ts:705`
does `const snap = this.#snapshot()` and pushes that same object to every
subscriber. So the 1 → 16 subscriber fall (611 k → 136 k) is delivery and
generator cost, **not** snapshot cost, and structural sharing would not touch it.

And the first-subscriber −62% is not `revive(encode())` either. In the
`streams/changes-fanout` profile, `cloneState` does not appear in the top 14 at
all. What does is **`@sigx/reactivity` `watch.ts:14` at 6.5%** — the deep watch
installed by `#ensureDeepWatch()` (`activation.ts:771`), which `#openChanges`
turns on at `:1322` immediately before the first `#subs.add`. A `watch(…, {deep:
true})` re-traverses state on every mutation just to bump `#version`. That is
the cliff.

**Since (#28):** the per-mutation walk is gone — a scheduler-deferred effect
flips a dirty bit synchronously at write time and the ONE deep walk happens
when the turn boundary folds it into `#version` (re-tracking whatever the
turn added). `#ensureDeepWatch()` above is now `#ensureChangeTracking()`,
and the `activation.ts` line anchors describe the code as profiled, not as
it is today. The floor that remains is one walk per dirty turn; removing
the walk entirely needs a write-hook primitive upstream
(signalxjs/core#546 tracks it, with these numbers). The profile figures
above predate the change.

### `state/*`: `structuredClone` is the cost, and it is the redundant one

| what | self% |
|---|---:|
| `@sigx/reactivity` `watch.ts:14` (deep watch) | 13.0% |
| `structuredClone` (all frames) | ~14.5% |
| `#activationFor` | 5.5% |
| `@sigx/reactivity` `signal.ts:333` `set` | 3.1% |
| `host/storage-memory.ts:18` `save` | 1.5% |

`cloneState`'s codec round-trip is again absent from the top. The clones that
show up are `memoryStorage`'s, on both save and load (`storage-memory.ts:18`,
`:14`) — and on the save side it clones a value the host has **already** encoded
into a fresh tree at `host.ts:230`, which nothing else aliases. Item 3 was
right about the redundancy and wrong about which copy dominates.

Caveat before acting: `ActorStorage` (`types.ts:300`) is a public seam, so the
defensive clone is part of its contract for third-party callers even though the
host's own path cannot need it. **Since fixed (#25):** the seam contract now
assigns `save` ownership of its argument and the save-side clone is gone; the
figures above predate that.

---

## Things worth investigating

Recorded here so the next person does not have to re-derive them. **None of
these are known problems** — they are measurements looking for a decision.

0. ~~**`#activationFor` and `#checkReentrancy` are `async` on a synchronous
   path** — the largest single cost in the runtime's own code, and present in
   every profile. See the section above.~~
   **Fixed (#24):** the warm path already bypassed both helpers; #24
   de-asynced the frames around it — `#dispatchInner` is a plain function
   (the awaited tail hoisted into `#dispatchSlow`) and `HostImpl.dispatch`
   branches on a thenable instead of `await`ing the default placement's
   synchronous `dispatcherFor`. Gated by the turn counts, all spread 0:
   `dispatch/warm-turns` 10 → 7, `dispatch/warm-turns-deadline` 11 → 8,
   `dispatch/always-warm-turns` 7 → 4. The helpers themselves stay `async` —
   they are cold-path only now.
1. ~~**`raceDeadline` costs 38% of dispatch throughput.** A shared timer wheel, or
   skipping the race when the deadline is far away, would recover most of it.
   (Profiled at 6.1% self in `local-host.ts:410` plus 0.9% in its inner closure
   at `:422`, across a run where only half the scenarios enable it.)~~
   **Fixed:** `CallDeadlines` (`host/deadlines.ts`) gives far
   deadlines (≥ 10 s remaining — the production default) one shared unref'd
   1 s registry tick over ceil'd buckets; near deadlines keep the exact
   per-call timer. `dispatch/warm-turns-deadline` gates the path exactly:
   12 → 11 microtask turns and 1000 → 0 host timers per 1000 dispatches
   (`dispatch/warm-turns` unchanged at 10). A far deadline now fires up to
   ~2 s late, never early; the `deadline` field crossing hops is untouched.
   Remaining per-call cost is the wrapper promise plus the two settle
   closures — the dispatch frames themselves are de-asynced since #24 (see
   item 0), which took the deadline rung from 11 to 8 turns.
2. ~~**The change feed's per-turn snapshot is two full deep walks.**~~
   **Superseded by the profile:** the snapshot is already shared across
   subscribers, and the 1-subscriber cliff is the `{deep: true}` watch, not the
   clone. See "The change-feed cliff is the deep watch".
3. ~~**`memoryStorage` `structuredClone`s on both save and load** — ~14.5% of the
   `state/*` profile, and the save-side clone duplicates a tree `host.ts:230`
   just built. Bounded by the `ActorStorage` seam's contract, not by the host.~~
   **Fixed (#25):** the seam's contract now says it — `save` takes ownership
   of its argument (the host always passes a codec-fresh `encodeWithHandlers`
   tree), so `memoryStorage` stores it by reference; the load-side clone
   stays, keeping "a stored value never aliases live activation state" true
   from both directions.
4. **The mailbox allocates ~4 promises per turn.** It is not the dominant cost
   today (see the ladder), so this is lower priority than it looks. **Profile
   confirms it:** `host/mailbox.ts` is 2.8% of the dispatch profile.
5. ~~**Debounce the membership subscriber** — the single highest-value cluster
   fix, and the only measured O(N²).~~
   **Fixed (#26):** `refreshCoalescer` on `@sigx/actors/cluster`, wired into
   all three store providers — single-flight with a version gate, `demand()`
   preserving `refresh()`'s started-at-or-after contract. Loopback burst
   (k=10 into n=50): refreshes 510 → 292 (default) → 213 (`coalesceMs: 25`).
   See the amplification section above.
6. **The 16-shard reminder ceiling** needs a decision, not a patch: raising
   the count is a storage migration.
7. ~~**`consistentHashPolicy()` costs ~16 µs per decision at N=100** (63×
   worse than at N=1) because it hashes `actorId|hostId` for every host.
   Only paid on a route-cache miss, and small next to a network hop.~~
   **Allocation half fixed (#27):** the per-call `filter` of active hosts is
   memoized per view object (WeakMap; the memory hub now returns a stable
   view per version so the memo hits everywhere). `randomPolicy` goes flat
   across N; the hash policy keeps rendezvous's inherent O(N) scoring —
   that part does not go away, and `rendezvous()` itself is untouched
   (reminder-shard winners are pinned storage identity, now guarded by a
   hard-coded expectation table in `placement-active-hosts.test.ts`).
8. **FIXED: every `JSON.parse` on the wire passed a reviver**
   (`wire-parse.ts`, used at `wire-shared.ts` `readNdjson`,
   `client/index.ts` ×2, `cluster/frames.ts` `decodeFrameBody`,
   `cluster/transport.ts` ×2). A reviver disables V8's fast parser. Measured
   standalone on the machine above:

   | payload | plain | + reviver | pre-scan then parse |
   |---|---:|---:|---:|
   | 17 B | 401 ns | 1 997 ns (5.0×) | 454 ns |
   | 90 B | 1 191 ns | 10 020 ns (8.4×) | 1 390 ns |
   | 9 KB (200 rows) | 72 µs | 773 µs (10.7×) | 90 µs |

   `parseWire` now pre-scans for `__proto__` / `constructor` / `prototype` /
   `\u` and skips the reviver when none can be present; a custom codec
   reviver (judged by identity against the default) always takes the full
   walk. The `frames/decode` scenario pins the effect in-tree: decode with
   the default reviver vs a custom one measured **4.6×** on a ~90 B reply
   and **7.6×** on a 9 KB 200-row payload (i9-12900HK, busy machine — the
   ratio is robust even when the absolute numbers are not).

   **Sized against the rung, as predicted:** `wire/endpoint-roundtrip` was
   unchanged within noise (native `JSON.parse` is only 5.7% of it — the rung
   is dominated by undici and webstreams, above). The win lives on
   `cluster/frames.ts`, where the binary transports carry no
   `Request`/`Response` overhead at all.

---

## What a native (Rust) engine could and could not do

Recorded so this is decided rather than re-argued. Measured on the machine
above, Node v22.22.0.

**It cannot win the engine core.** `dispatch/mailbox-raw` is 7.50 M ops/s =
**133 ns per turn**. A crossing out of JS and back costs **40–67 ns each way**
even for optimized built-in intrinsics (`process.hrtime.bigint()` 40 ns,
`Date.now()` 67 ns); a napi-rs call marshalling real arguments is 150–300 ns.
The mailbox, the activation table and placement would spend their whole budget
at the boundary — and every turn still ends in a JS call, because the actor
method is JavaScript. The profiles above point the same way: the runtime's own
hot spot is *promise allocation on a synchronous path*, which is removed by
deleting two `async` keywords, not by changing language.

**The measured ceilings are both outside such a core.** "Throughput plateaus at
the wire, not at the runtime" — 18–19 k ops/s against a bare Node HTTP echo
server at ~17 k/s on the same box. And fan-out across 1 000 actors does not beat
a single actor, because the limit is one JS thread. Neither moves because the
scheduler got faster.

**Where it could genuinely win, if the JS work proves insufficient:** the
transport plane. Terminating sockets, framing, HMAC and directory-cache
forwarding in native code means the ~(N−1)/N of calls a host does not own never
enter JS at all — the one place the boundary is not crossed rather than crossed
faster. Locality is 1/N by design, so at real cluster size that is most traffic.
Ships as a sidecar or a Node-only optional accelerator, not as a rewrite.

**The packaging cost is a project of its own, independent of the engine.** CI
has no macOS and no arm64 leg (`.github/workflows/ci.yml`), so prebuilt binaries
have no producer today; `pnpm-workspace.yaml`'s `onlyBuiltDependencies` allowlist
blocks postinstall binary linking; and `scripts/publish.js` is hardcoded to a
single package with no per-platform `optionalDependencies` fan-out. workerd
(`packages/actors-cloudflare`, CI-tested) has no N-API at all — only WASM, which
gives up threads and shared memory and would blow the 2–11 KB `.size-limit.json`
budgets.

---

## 2026-08-02 · Tier 3 — the first real deployment

The first numbers this tier has ever produced. Everything below was
measured against a live AKS cluster over its public HTTPS endpoint, with
the load driven from a VM in the same region.

> **2026-08-05 — the app under test moved, the measurement did not.** It was
> `examples/chat` when these were recorded and is now `perf/app`. Every
> measured body moved byte-identical: `Room#recent`/`#post`, the
> `Room → ActivityFeed` publish topology, `Digest`/`DigestActor` at
> `DIGEST_ITERS=2000`, the chart, and `edge-ladder.mjs`. **The figures below
> stand.**
>
> One derived value did change: the `postMessage` serverFn's `__sigxKey` is
> now `sigx-perf-app/src/chat.server.ts/postMessage`, so its hashed wire id
> is different. That is invisible — `post-fn.mjs` derives the id from the
> build, which is the entire reason it exists — but a `write-mix` re-run
> posts to a different URL path of the same length, and it is worth stating
> so nobody rediscovers it as a mystery.
>
> The move also **pins** the workload. An edit to an actor body in `perf/app`
> is now a deliberate baseline invalidation rather than a side effect of
> tidying an example, which is what it was while the two were the same file.

| | |
|---|---|
| Cluster | AKS 1.34, `Standard_D2ls_v6` (2 vCPU, **1900m allocatable**) |
| Hosts | one per node, `spread.whenUnsatisfiable=DoNotSchedule`, `limits.cpu 1000m` |
| App | `perf/app`, redisStorage + redisCluster, one Redis pod |
| Driver | `Standard_D4s_v5` in-region, `edge-ladder.mjs`, Node v22.22.0 |
| Settings | 3 runs × 20 000 ms per rung |
| Shapes | `replicas=3 nodes=3` and `replicas=7 nodes=7`, image `02dded3`, knobs identical (`PLACEMENT=prefer-local, MAX_ACTIVATIONS=0, SWEEP_INTERVAL_MS=60000, MIGRATE_PERSIST=lazy, DIGEST_ITERS=2000`) |

> ⚠️ **The edge hash was OFF for every number in this section.**
> ingress-nginx ≥ 1.12 silently drops `upstream-hash-by` under its default
> annotation risk level, so the routing token bought nothing and every actor
> call paid ~0.86 cross-host hops. **These figures are a floor, not a
> ceiling** — locality was never engaged. See the `edge hash actually
> pins a token` assertion, which now fails loudly instead of leaving this to
> be inferred from a `token_speedup` of ~1.0.
>
> **The hash has since been made to work** (#143 — it needed a controller
> ConfigMap key *and* a dedicated Service, because `upstream-hash-by` is
> backend-level and the un-hashed web Ingress was winning the backend
> merge). "With the edge hash ON" below is the same shape re-measured, and
> is the section to read for what locality actually buys.

### Throughput and latency, 3 hosts → 7 hosts

| scenario · rung | 3h ops/s | 3h p50 | 7h ops/s | 7h p50 | Δ ops/s |
|---|---|---|---|---|---|
| read-ladder c=32 | 1842 | 12.6 ms | 2719 | 8.9 ms | +48% |
| read-ladder c=64 | 2115 | 24.0 ms | 3421 | 11.6 ms | +62% |
| read-ladder c=128 | 2105 | 51.1 ms | 3682 | 23.7 ms | +75% |
| read-ladder c=256 | 2144 | 103.4 ms | 3551 | 32.3 ms | +66% |
| write-mix c=64 (20% writes) | 1382 | 30.3 ms | 1763 | 18.7 ms | +28% |
| cold-placement c=32 | 2050 | 10.7 ms | 2970 | 8.4 ms | +45% |
| cold-placement c=64 | 2434 | 19.9 ms | 3890 | 10.2 ms | +60% |
| cold-placement c=128 | 2362 | 40.8 ms | 4389 | 18.8 ms | +86% |

Read throughput knees at c=64 on three hosts and keeps climbing to c=128 on
seven. 2.33× the hosts bought +66…+86% — sub-linear, and with locality
disabled every call was paying a hop, so the gap is not the ceiling.

| ratio | 3 hosts | 7 hosts |
|---|---|---|
| `token_speedup` | 1.011 | 0.986 |
| `ownership_spread` | 1.014 | 1.043 |
| `pool_spread` | 3.028 | 6.514 |

`token_speedup` ≈ 1.0 at both sizes is the disabled edge hash, not a
statement about locality. `pool_spread` (named `worker_speedup` before the
rename — the old name claimed a worker win) tracks host count (3.03≈3,
6.51≈7) because a worker is always placed locally while a stateful actor is
pinned to one host. Per host the pool did 289 and 266 ops/s against the
single activation's 289 — flat: a host is one JS thread, and pool members
buy await-interleaving, not CPU throughput.

That spread is **conditional on the edge scattering the calls**, which was
only true because the hash was off. With it on, `pool_spread` is 1.12 — see
"With the edge hash ON" below, and #148.

### The single-activation ceiling

| | 3 hosts | 7 hosts |
|---|---|---|
| `defineWorker` pool, cluster total | 867 ops/s | 1863 ops/s |
| **single activation** | **289 ops/s** | **289 ops/s** |

289 at three hosts and 289 at seven. Per-key throughput is fixed by the
single-activation guarantee — 2.33× the hardware buys one actor nothing.
The number to design key spaces against.

### A singleton subscriber caps cluster-wide writes

Every room publishes to one `ActivityFeed('all')`.

| concurrency | publish 3h | publish 7h | read 3h | read 7h |
|---|---|---|---|---|
| 32 | 411 | 337 | 1677 | 2694 |
| 64 | 447 | 387 | 2028 | 3428 |
| 128 | 411 | 398 | 2153 | 3852 |

Publish is **flat across a 4× concurrency range and goes *down* as hosts are
added** (−18% at c=32), while reads scale +61…+79%. More hosts means more of
those deliveries are remote into the same subscriber. A singleton aggregator
is a cluster-wide write ceiling that gets worse with fleet size — a property
of the pattern, not of the runtime.

### With the edge hash ON — the same shape, locality actually engaged

Everything above was measured with `upstream-hash-by` silently stripped.
This is `replicas=3 nodes=3` re-measured after #143 made the hash program,
image `04e6598`, 3 runs, all other knobs identical
(`PLACEMENT=prefer-local, MAX_ACTIVATIONS=0, SWEEP_INTERVAL_MS=60000,
MIGRATE_PERSIST=lazy, DIGEST_ITERS=2000`). Confirmed engaged three ways
before the run: the controller's programmed backend carries
`upstream-hash-by: $http_x_sigx_actor_route`, nginx's access log put 12
same-token requests on one pod, and `token_speedup` moved off 1.0.

**The `c=32` rung is omitted from every comparison below.** It came back at
±65%, ±85%, ±53% and ±35% across the four scenarios that have one — it is
the warm-up rung, and nothing can be read from it.

| scenario · rung | hash OFF | hash ON | Δ | variance |
|---|---|---|---|---|
| read-ladder c=64 | 2115 | 2880 | **+36%** | — |
| read-ladder c=128 | 2105 | 3350 | **+59%** | ±15% |
| read-ladder c=256 | 2144 | 3023 | **+41%** | ±11% |
| cold-placement c=64 | 2434 | 2868 | **+18%** | ±10% |
| cold-placement c=128 | 2362 | 3295 | **+39%** | — |
| write-mix c=64 | 1382 | 980 | **−29%** | ±22% |
| topics publish c=64 | 447 | 708 | **+58%** | ±24% |
| topics publish c=128 | 411 | 835 | **+103%** | — |
| topics read c=64 | 2028 | 2933 | **+45%** | — |
| topics read c=128 | 2153 | 3240 | **+50%** | — |

| ratio | hash OFF | hash ON |
|---|---|---|
| `token_speedup` | 1.011 | **1.478** (±19%) |
| `ownership_spread` | 1.014 | 1.015 |
| `pool_spread` | 3.028 | **1.12** (±20%) |

Three things worth naming.

**Locality pays, and `token_speedup` finally means what it says.** 1.48× —
810 ops/s with the token against 570 without, p50 38.1 ms against 90.7 ms,
zero errors either arm. The read ladder gains +36…+59% on the identical
hardware; that is the hop the earlier figures were paying, recovered.

**The singleton-subscriber ceiling is not what it looked like.** Publish was
described above as *flat across a 4× concurrency range* (411 / 447 / 411).
With the hash on it climbs: 708 at c=64, 835 at c=128, and c=128 is **+103%**
over the same rung. The flatness was substantially the edge scattering each
room's publishes away from the room's owner, not the aggregator saturating.
A singleton subscriber is still a cluster-wide write ceiling — reads still
outrun publishes ~4× — but the earlier section overstated how low it sits.

**`pool_spread` collapsed, 3.03 → 1.12, and that is a regression the hash
caused.** A `defineWorker` pool won 3.03× precisely *because* the edge
scattered its calls: a worker is always placed locally, so round-robin gave
it three hosts' worth of pool. Hashing the routing token pins every call for
one worker key to one pod and hands that back. Filed as #148. Compounding
it, `maxLocal` defaults to `navigator.hardwareConcurrency`, which is **1**
inside a container with `limits.cpu: 1` — so the pool is also down to a
single member (#147). Together they put `Digest` (300 ops/s) within 12% of
the single-activation `DigestActor` (268 ops/s).

`write-mix` at −29% is the one number here not to trust yet: ±22% on three
runs, and it is the only scenario that regressed. Reproduce it before
treating it as real.

> **Tier 3 cannot see #139 at all.** The coalescing win is O(hosts) fan-out
> per *live watcher*, and there is no live-watcher load mode here —
> `topics-fanout` drives unary publishes and reads. A `$live`-subscriber
> ladder (N held connections to `ActivityFeed` from the load VM, emissions/s
> against N) is the missing scenario; nothing in this section is evidence
> for or against #139.

### Mailboxes are not cores (ad-hoc, one host, 8-core node)

`Standard_D8ls_v6`, 7000m limit, 8 pool members, 8 concurrent calls of
150 000 hash iterations, all pinned to one host:

| arm | mailboxes | compute | ops/s | vs single activation |
|---|---|---|---|---|
| single activation | 1 | on the loop | 4.6 | 1.00× |
| `defineWorker` pool | 8 | on the loop | 4.9 | 1.07× |

Seven idle cores and 7%: a pool multiplies mailboxes, which is concurrency
at `await` points, and a host is one Node process with one JS thread. On the
2-vCPU nodes the effect is invisible anyway — 1900m allocatable minus nine
kube-system daemonsets leaves ~1.1 usable cores, so one host already
saturates a node.

### Also observed — since fixed

A host whose event loop paused past the 15 s membership TTL was evicted, had
its directory claims released by `evictHost`, and **kept serving with its
activations live** — it never rejoined in 77 s, never fenced
(`selfFences=0`), stayed `Ready`. Single activation was not preserved across
that window. Measured, not inferred.

**This was #45, and it is closed.** A host now fences when it cannot prove its
own membership — not only when a beat *fails* past the TTL, but when one
merely *lands* past it, which is exactly what a stalled event loop does: the
write succeeds, so nothing errors, while peers have already released the
claims. Every membership provider implements the rule, and `k8sMembership`
extends it to a renewal that returns 404 (#69). The measurement above is left
as a record of the run that found it — do not quote it as current behaviour.

---

## 2026-08-06 · What state SIZE costs the dirty-tracking walk (#124)

| | |
|---|---|
| Machine | 12th Gen Intel i9-12900HK, win32/x64 |
| Node | v22.22.0 |
| Build | `dist/*.prod.js` (`--conditions=production`) |
| Commit | `c301533` |
| Settings | 5 rounds × 400 ms, interleaved |
| Conditions | **Contended** — the probe varied 29% across rounds, and the suite said so. Read the RATIOS, which span three orders of magnitude; do not quote an absolute. |

Two scenarios added here, because nothing in the suite measured this:
`state/write-behind` and `streams/changes-fanout` both pin state at
`{ count: 0 }`, and `Large` was only ever exercised through `save()` and heap
footprint — never through the tracking walk. The pathology was invisible.

### `state/dirty-size` — mutating turns, by state size

Both arms are the same write-behind actor whose debounce never fires, so the
only difference between them is whether a change feed is open.

| rows | subs=0 (walk only) | subs=1 (walk + snapshot) | what the subscriber adds |
|---:|---:|---:|---:|
| 0 | 165.9 k ops/s | 133.5 k ops/s | −20% |
| 200 | **818 ops/s** | **736 ops/s** | −10% |
| 2 000 | **71.7 ops/s** | 55.6 ops/s | −22% |

- **The tracking walk costs 200× an empty-state turn at 200 rows, and 2 300×
  at 2 000** — with no subscriber, no snapshot and no storage write anywhere
  near it. A turn on 200-row state is ~1.2 ms, of which essentially all is the
  walk. Cost is linear in the size of the state and independent of the size of
  the change, which is #124's central claim, reproduced.
- **Enumerating the Proxy is the dominant term.** The fixture proxies 201 plain
  objects (the state root plus 200 rows — the `rows` and `tags` arrays take the
  walker's `Array.isArray` index-loop branch, and reactivity refuses to proxy
  the `Date`). At the ~2 µs per proxied `Object.keys()` core measured
  (signalxjs/core#642: ~165× the raw cost, V8 validating the `ownKeys` trap
  result against the target's own property descriptors) that is ~0.4 ms, with
  ~1 600 per-key `get` reads on top of it. The right order of magnitude for a
  1.2 ms turn, and precisely the two terms core#642 and core#645 removed
  upstream — neither of which `trackDeep` inherits, because it is a private
  copy of the pre-#642 walk.
- **The snapshot is the smaller half — for now.** `#snapshot()`'s encode+revive
  adds 10–22%, against a walk that is the other 100×. That ordering is the
  reason to fix the walk first, and the reason to re-measure this table
  afterwards rather than assume the snapshot stays negligible.

### `state/dirty-growth` — a job actor accumulating a step per turn

The shape #124 was reported against, and the one `defineJob` produces: explicit
persistence, so nothing is tracked until a `job.watch()` subscriber attaches —
and then every boundary walks and clones a graph that only ever grows.

| Metric | Value |
|---|---:|
| `head/turn_us` (mean over the first 100 of 500 steps) | 350 µs |
| `tail/turn_us` (mean over the last 100) | **3 001 µs** |
| `growth_ratio` | **8.6×** |

- **Per-turn cost grows linearly with the run.** The head window averages ~50
  accumulated steps and the tail ~450 — 9× the state for 8.6× the cost. The
  reporter measured ~13 ms → ~33 ms across 50 steps of a much larger per-step
  payload; same curve.
- `growth_ratio` is **informational**: a quotient of two measurements inherits
  both their errors, and `head` carries most of it. The two absolute figures
  are what gate.

### Why no `exact` metric here

Every invariant worth gating in this area — walks per dirty boundary, clones
per turn — needs either a getter-marker fixture or a runtime counter that does
not exist, and `Metric.exact` demands determinism *by construction*. A gate
that cries wolf is worse than none, so these scenarios gate as timings only
and stay out of `BENCH_GATE_SCENARIOS`.

### Fixed the same day: `deepTrack` replaces the copied walk

Same machine, run back to back rather than against the table above — the
figures there were captured under heavier contention, and comparing across them
would flatter the change. The `after` side is a `pnpm pack`ed build of the
reactivity release candidate installed as an ordinary tarball dependency, not a
`link:` — a linked workspace package is inlined rather than externalised, which
is a different module graph from the one a user gets.

| scenario | before | after | |
|---|---:|---:|---:|
| `state/dirty-size` rows=0, subs=0 | 229.3 k ops/s | 487.2 k ops/s | +113% |
| `state/dirty-size` rows=0, subs=1 | 163.1 k ops/s | 269.6 k ops/s | +65% |
| `state/dirty-size` rows=200, subs=0 | 1.0 k ops/s | **10.1 k ops/s** | **+896%** |
| `state/dirty-size` rows=200, subs=1 | 741 ops/s | 3.0 k ops/s | +308% |
| `state/dirty-size` rows=2000, subs=0 | 88.9 ops/s | **714 ops/s** | **+703%** |
| `state/dirty-size` rows=2000, subs=1 | 72.1 ops/s | 214 ops/s | +196% |
| `state/dirty-growth` `head/turn_us` | 267 µs | 89 µs | −67% |
| `state/dirty-growth` `tail/turn_us` | 2 183 µs | 666 µs | **−69%** |

Every row moved far past the noise; the change is `trackDeep` deleted in favour
of `deepTrack` from `@sigx/reactivity/internals`, and nothing else.

**The ordering has now flipped, which is the finding to carry forward.** At 200
rows the subscriber cost 26% of a turn before (1.0 k → 741) and costs **70%**
after (10.1 k → 3.0 k). `#snapshot()`'s encode+revive was the smaller of the two
O(state-size) costs on a dirty boundary and is now the larger one. Re-read the
table above before quoting it: fixing the walk did not make the per-boundary
cost independent of state size, it just changed which term dominates.

`state/dirty-growth` also allocates measurably less — 26 → 14 GC collections
and 17.7 ms → 9.1 ms of pause over the same fixed 500 steps. (The
`state/dirty-size` GC counts went *up*, which is not a regression: those
scenarios are duration-bounded, so ~9× the throughput means ~9× the turns in
the same 400 ms.)

### And then: a `$live` watch was paying for a snapshot it never read

`streams/live-watch` (new) measures the path `useActorState(…, { live: true })`
and every wire watch take — `host.dispatchWatch`, which re-invokes a read
method on change. It is not `streams/changes-fanout`: `createSharedWatch`'s
pump does `const { done } = await iterator.next()` and **never touches the
value**, so the snapshot the boundary built for it was discarded, once per
mutating turn, over the whole state.

| rows | before | after | |
|---|---:|---:|---:|
| 0 | 176.0 k ops/s | 218.9 k ops/s | +24% |
| 200 | 3.0 k ops/s | **9.7 k ops/s** | **+224%** |
| 2 000 | 216 ops/s | **696 ops/s** | **+222%** |

**Read this next to the `state/dirty-size` table above**, which is the point:
at 200 rows a live watch now runs at 9.7 k against that scenario's 10.1 k with
no subscriber at all, and at 2 000 rows 696 against 714. *The marginal cost of
a `$live` subscriber is now approximately zero* — it was 3.3× before.

**On the evidence.** `bench:diff` **declined a verdict** on this pair: the
actor-free probe scored 6% apart between the two runs, so it downgraded every
row to "within run-to-run noise" and said so. That is the tool behaving
correctly and it is quoted here rather than hidden — but a 3.2× move is ~37×
the drift it detected. The claim does not rest on the timing anyway: a type
handler counting its own `serialize` calls proves **zero** snapshots are built
for a live watch, deterministically and with no clock involved
(`packages/actors/__tests__/change-throttle.test.ts`). The timings corroborate;
the counter is the proof.
---

## 2026-08-06 · Would `worker_threads` pay? (#119)

| | |
|---|---|
| Machine | 12th Gen Intel i9-12900HK, **20 logical cores**, win32/x64 |
| Node | v22.22.0 |
| Build | `dist/*.prod.js` (`--conditions=production`) |
| Settings | 7 rounds × 700 ms, interleaved, `BENCH_THREADS=1` |
| Conditions | **Contended** — probe varied 18%. Read `thread_speedup`, which is two arms measured back to back in one process; ignore the absolutes. |

#119 asks for "a benchmark first, not an implementation", and specifically for
one **on a multi-core box** — the Tier-3 arm reports 1.07× on a node with ~1.1
usable cores, which cannot show thread parallelism whatever the runtime does.
This is that measurement. It changes nothing in the runtime: the threaded arm
drives a `worker_threads` pool from inside an ordinary actor method, which is
enough to price the crossing.

### Compute size, at a 64-byte payload

| iterations | one activation | `defineWorker` pool (8) | `worker_threads` (8) | speedup vs pool |
|---:|---:|---:|---:|---:|
| 1 000 | 307.4 k ops/s | 293.2 k ops/s | 95.9 k ops/s | **0.33×** |
| 10 000 | 43.5 k ops/s | 44.2 k ops/s | 78.3 k ops/s | 1.77× |
| 150 000 | 3.3 k ops/s | 3.1 k ops/s | 8.2 k ops/s | **2.69×** |

### Payload size, at 150 000 iterations

| payload | `defineWorker` pool (8) | `worker_threads` (8) | speedup |
|---:|---:|---:|---:|
| 64 B | 2.3 k ops/s | 8.4 k ops/s | **3.46×** |
| 16 KB | 2.3 k ops/s | 8.1 k ops/s | 3.36× |
| 1 MB | 2.2 k ops/s | 2.4 k ops/s | **1.02×** |

Three independent runs (1 × 1-round quick, 5 × 400 ms, 7 × 700 ms) agree on all
three shapes below, which is why they are quotable despite the spreads.

- **The crossover is between 1 000 and 10 000 hash iterations** — call it ~10 k,
  or roughly 200 µs of compute. Below it, crossing costs more than the work:
  at 1 k iterations threads are **3× SLOWER**. A seam that made this easy to
  reach for would be a footgun at small sizes.
- **`defineWorker` buys nothing over a single activation at any size** (293 k
  vs 307 k, 44.2 k vs 43.5 k, 3.1 k vs 3.3 k). That is #119's premise
  reproduced locally, and now on a box with 20 cores rather than 1.1 — so it is
  the *mailboxes-are-not-threads* property, not the node size.
- **The ceiling is ~3.4×, not 8×.** Eight threads, three and a half times the
  throughput. Structured clone runs on the CALLING thread, so the main loop
  still serialises every argument in and every result out; that is the
  bottleneck, and it is also why **1 MB payloads erase the win entirely**
  (1.02×). Any real seam would need transferables (`ArrayBuffer` in a transfer
  list) to beat this, and would still be capped by whatever it cannot transfer.

### The case more hosts cannot fix

The two tables above compare threads against `defineWorker`, and on that
comparison **threads lose to simply running more hosts**. From the Tier-3
section above: a pool's cluster total is 867 ops/s on 3 hosts and 1863 on 7 —
linear in the fleet — where threads cap at 3.4× on one box. For stateless
CPU-bound work spread over many keys, the scaling answer already exists, ships
today, and beats this.

The exception is the row directly under it: **a single activation is 289 ops/s
on 3 hosts and 289 on 7.** Single-activation is a correctness guarantee, so a
hot key is pinned to one host and one thread however large the fleet. Hardware
buys it exactly nothing, and that is the one place a thread seam would not be
duplicating something the cluster already does.

`compute/single-activation-threads`, one `reentrant: 'always'` activation,
64 B payload (7 rounds × 700 ms; two runs agreeing at 2.74× and 2.80×):

| iterations | compute on the loop | on `worker_threads` | speedup |
|---:|---:|---:|---:|
| 10 000 | 28.2 k ops/s | 34.9 k ops/s | 1.32× *(inconclusive — ±40%)* |
| 150 000 | 2.9 k ops/s | 8.3 k ops/s | **2.80×** |

`reentrant: 'always'` is load-bearing, not incidental. A serial actor cannot
benefit at all: its mailbox will not begin turn N+1 until turn N returns, so
offloading only moves the same serialized wait. Interleaved turns park on an
`await`, which is the only shape where a thread pool gives ONE activation real
parallelism.

### The honest summary for #119

Threads *do* pay — the "seven idle cores and 7%" figure is a property of the
measuring node, not a law — but the case for building a seam is narrower than
the multipliers suggest, and it is not the case #119 frames:

- **For stateless CPU work over many keys, use more hosts.** Linear, already
  shipped, and better than the 3.4× ceiling threads can reach.
- **The niche threads uniquely serve is a single hot activation**, where the
  fleet is powerless (1.00×) and threads give 2.80×. That argues for offloading
  *inside* an activation rather than for anything attached to `defineWorker`.
- **And that niche is narrow**: it needs `reentrant: 'always'`, ≥ ~10 k
  iterations (~200 µs) of compute per turn, and payloads well under a megabyte.

A seam aimed at `defineWorker` would be solving the problem the cluster already
solves. A seam aimed at the hot single activation would be solving one nothing
else can — for actors that meet all three conditions above.

## 2026-08-09 · Tier 3 — WebSocket connection scale (#172)

The first connection-count figures the repo has ever had. The socket stack
shipped in v0.6.0 with nothing measuring it: no example or perf tree opened
a WebSocket, no scenario in any tier touched one, and `socketStats()` was
wired to nothing.

**This is a different axis from every other Tier-3 number here.** The HTTP
sections measure ops/s over the public HTTPS endpoint from a same-region
VM. These measure CONNECTIONS HELD and MESSAGES DELIVERED, driven from
inside the cluster straight at the Service — no ingress, no TLS, no load
VM. That makes them a measurement of the runtime rather than of the edge,
and it means **they are not comparable with the rows above**. The public
path is deliberately not measured yet; when it is, it goes in its own
section and is reported separately, because it will be ingress-limited
rather than runtime-limited.

| | |
|---|---|
| Cluster | AKS 1.34, `Standard_D2ls_v6` (2 vCPU, 1900m allocatable) |
| Hosts | 3 pods, `limits.cpu 1000m`, `redisStorage` + `redisCluster` |
| App | `perf/aks`, `Fanout` actor, `ENABLE_SOCKET=1`, socket caps at runtime defaults |
| Driver | in-cluster `ws-loadgen.mjs` Jobs (`Indexed`, pod 0 publishes), image `6bcb35a` |
| Path | `ws://sigx-host:7311/_sigx/socket`, `permessage-deflate` OFF both ends |

### Connections are not the constraint

| dialed | connected | connect failures | refusals | breaches | client RSS |
|---|---|---|---|---|---|
| 1 000 | 1 000 | 0 | 0 | 0 | 96 MB |
| 5 000 | 5 000 | 0 | 0 | 0 | 122 MB |
| 10 000 | 10 000 | 0 | 0 | 0 | 151 MB |
| 20 000 | 20 000 | 0 | 0 | 0 | 206 MB |
| **100 000** (4 pods × 25 000) | **100 000** | **0** | **0** | **0** | 418 MB/pod |

100 001 connections opened and 100 001 closed, with subscriptions matching
one-for-one. **`peakOpen` — the `open` gauge sampled off the hosts — read
83 774.** Quote that as the concurrency figure, not the 100 000: the
generator's pods are NOT barrier-synchronised, so a pod that finished
dialling early began its hold window early, and the sum of per-pod peaks
overstates how many were up at the same instant. The gauge is also sampled
every ~5 s, so it under-reports in the other direction. The honest
statement is **~84 000 concurrent subscribers observed on the hosts, with
100 000 dialled and none refused.**

A generator pod caps at ~28k: a 40 000 rung from one pod produced 28 232
connected and 11 768 failures with **zero host-side refusals** — the pod's
ephemeral port range, not a server limit. Add pods, keep each rung under
~25 000.

### Delivery throughput is the constraint, and it sets latency

One actor, anonymous subscribers, 10 publishes/s, 60 s per rung:

| subscribers | deliveries/s | p50 | p99 | publishes achieved (of 600) |
|---|---|---|---|---|
| 1 000 | 10 022 | 63 ms | 73 ms | 602 |
| 5 000 | 41 584 | 117 ms | 181 ms | 514 |
| 10 000 | 48 429 | 192 ms | 442 ms | 300 |
| 20 000 | 43 229 | 370 ms | 926 ms | 131 |

**~48 000 deliveries/s across 3 hosts — ~16k/host, ~62 µs per delivery on a
1000m CPU limit.** Fan-out is O(1) actor reads and **O(N) serializations**:
the read is shared across subscribers, but each has its own correlation id,
so the frame is encoded and `JSON.stringify`d once per subscriber.

> ⚠️ **The count is right; the attribution is wrong — retracted by #245.**
> Profiled, the serialization is **1.7% of busy time** and the socket write is
> **77%**: fan-out is O(N) `writev` syscalls, one per subscriber, and the
> encode rides along. **`~62 µs per delivery` is also a throughput reciprocal
> rather than a CPU cost** — the host under it was never shown to be
> saturated, and the one that was measured sat at ~0.65 of a core. See
> "Where a delivery's CPU actually goes (#245)" at the end of this file.

At 84k subscribers with one publish every 4 s: p50 **1 343 ms**, p90 1 906 ms,
p99 **2 235 ms**. 84 000 ÷ 48 000/s ≈ 1.75 s to walk the set, which is what
those bracket — so

> **worst-case delivery latency ≈ subscribers ÷ (16k/s × hosts)**

The publisher pays out of the same budget: the achieved publish rate fell
from 602 to 131 per minute across those rungs with zero failures, because
the publishing turn queues behind fan-out on a single-threaded actor.
**Write throughput to a hot actor degrades as its audience grows.** (#182)

Two figures that did NOT move: `maxBufferedBytes` stayed 0 at every rung and
`protocolBreaches` stayed 0 throughout. The absence of send-path backpressure
is therefore a *structural* gap here rather than an observed failure.

> ⚠️ **That `maxBufferedBytes` is the CLIENT's buffer, and an earlier version
> of this paragraph read it as "the hosts never outran the clients". It does
> not say that** (#208). It is `ws-loadgen` sampling the subscriber's own
> `WebSocket#bufferedAmount` — data queued to *send* — and a subscriber sends
> almost nothing, so it reads ~0 however much a host is holding. Nothing in
> the runtime instruments the host side, so these zeros are equally consistent
> with unbounded host buffering. #182 is left unrefuted here, not answered.
>
> The original paragraph went on to say that demonstrating it "needs a
> deliberately slow consumer, which the rig cannot do yet". The rig gained one
> in #205 and it ran — see the 2026-08-11 section. It does not close this,
> because a slow consumer proves nothing about a buffer nobody measures; what
> is still missing is the host-side counter (#208), not the workload.

### Identity is the cliff — and it is not a curve

The same fan-out with one distinct principal per subscriber, on a read that
consults `ctx.principal`:

| identities | rate | connected | failures | deliveries/s | p50 | publishes |
|---|---|---|---|---|---|---|
| 100 | 10/s | 100 | 0 | 1 003 | 53 ms | 452 of 450 — healthy |
| 250 | 10/s | 220 | **30** | 0.4 | — | **1** |
| 1 000 | 10/s | 484 | **516** | 2.0 | — | **1** |
| 250 | 1/s | 216 | **34** | 216 | **52 ms** | 46 of 45 |

**Anonymous scales to 20 000 on one actor; signed-in falls over between 100
and 250.** The last row separates the two costs: steady-state delivery to
216 distinct identities is *fine* at 52 ms p50, and every failure happened
while dialling. So the wall is **establishing** the per-principal watch
loops, not serving them — each is a turn on a single-threaded actor, and
seeding competes with fan-out for the same queue until new subscribers stop
completing. Positive feedback, hence a cliff. (#180)

The mechanism is #121: a read observed consulting `ctx.principal` gets one
watch loop per encoded principal. Locally, 200 subscribers on one actor:
**31 actor turns anonymous versus 6 200 per-user**, with the control (same
principal-reading method, one identity) collapsing back to 62 — so the
split is per principal, not per subscriber. Cross-host is worse and
unconditional: `#coalescedWatch` keys on `call.principal` whether or not
the read consults it (#138).

**Anonymous and signed-in fan-out are different products. Neither number
means anything without saying which one it is.**

### Two limits that bound every figure above

- **Client subscriptions cannot set `throttleMs`.** Neither the socket
  session nor `$live` passes it to `dispatchWatch`, so every live
  subscription runs at `DEFAULT_WATCH_THROTTLE_MS` (50 ms) — a hard ~20
  pushes/s per subscriber. The 53–63 ms p50 floor in the small rungs IS
  that trailing window.
  **Since #247 they can**, from a fixed ladder of windows, floored at the
  same 50 ms — so a subscriber may ask to be served more slowly, never
  faster. Every figure in this section predates it and none of the
  generators send one, so they remain no-`w` runs; a run that sets one is a
  different measurement rather than a faster one.
- **`deliveriesPerPublish` is a coalescing ratio, not a constant.** Above
  ~20 publishes/s per key it falls below the subscriber count by design,
  and reading it as loss is a misreading.

### What this section cannot say

Sockets are **host-affine**: the client is pinned to whichever pod the
Service gave it, and every call inside re-dispatches through placement, so
the edge hash buys nothing and cross-host rates read like `random`
placement whatever `PLACEMENT` says. Nothing here measures the public
ingress path, TLS cost, or behaviour across a rolling restart — and nothing
here was run with a slow consumer, which is the one shape most likely to
find the missing backpressure.

## 2026-08-10 · Tier 3 — the socket run, recorded (#184)

The same axis as the section above, this time through the harness rather
than read off a terminal: `testenv.mjs ws-bench` drove the `sockets/*`
scenarios and the result was kept as a JSON artifact. **This is the section
to compare against** — the 2026-08-09 figures were the first measurement
and were transcribed by hand.

| | |
|---|---|
| Shape | `ws replicas=3 nodes=3 image=db2b296 knobs=ENABLE_SESSIONS=1,ENABLE_SOCKET=1` |
| Cluster | AKS, `Standard_D2ls_v6` (2 vCPU, 1900m allocatable), `limits.cpu 1000m` |
| Driver | in-cluster `ws-loadgen.mjs` Jobs, ONE pod, 30 s per rung |
| Settings | `--runs=1` (each run is a real Job; the harness discards a warmup) |

Socket caps were the runtime defaults throughout — 256 in-flight calls, 256
subscriptions, 30 s keepalive, no revalidation, no lifetime cap. They are in
the shape string, so a run under different caps is refused rather than
compared.

### `sockets/hot-fanout` — N subscribers on one actor, 10 publishes/s

| subscribers | deliveries/s | p50 | p99 | connect failures |
|---|---|---|---|---|
| 1 000 | 10 042 | 63.0 ms | 72.6 ms | 0 |
| 5 000 | 36 369 | 138.5 ms | 246.1 ms | 0 |
| 10 000 | 38 302 | 225.7 ms | 539.4 ms | 0 |

`peak_open` 10 001 (the subscribers plus the publisher's own connection),
`peak_subscriptions` 10 000, `protocol_breaches` 0, `max_buffered_bytes` 0
at every rung.

**It reproduces.** Against the hand-run a day earlier at the same rungs:
10 022 → 10 042 msg/s and 63 → 63.0 ms p50 at 1 000 subscribers. The 5 000
and 10 000 rungs read lower here (36 369 vs 41 584; 38 302 vs 48 429)
because these ran a 30 s window against the earlier 60 s one — a shorter
window pays proportionally more of its time for the dial and the trailing
throttle flush. Same shape, and the same conclusion: throughput plateaus
while latency keeps climbing.

### `sockets/idle-capacity` — held connections, no traffic

1 000 / 5 000 / 10 000 all connected with a zero failure rate, `peak_open`
10 000, zero breaches. One generator pod, so this says nothing about the
ceiling — the 2026-08-09 section reached ~84 000 concurrent with four pods,
and the per-pod ephemeral port range (~28k) is what bounds a single one.

### `sockets/principal-cliff` — `max_healthy_identities` = **100**

The headline of this section, and the number to watch. It reproduces the
hand-run exactly: 100 distinct signed-in identities on one actor dial and
serve cleanly (1 005 msg/s, p50 53.5 ms, p99 60.2 ms, zero failures); the
next rung up does not.

Against 10 000 anonymous subscribers on the same actor in the same run,
that is the whole of #180 in one metric — and it is the metric that moves
if #138 lands.

### Two notes for whoever compares the next run

- **`gc/*` metrics ride along and mean nothing here.** The harness attaches
  them automatically and they describe the LOCAL Node process, which in
  this tier only orchestrates `kubectl`. Ignore them; they are not
  measuring the cluster.
- **A baseline FILE is per-machine, and the runner is disposable.** The
  durable artifact is `sockets.json` on the workflow run. Compare two of
  them with
  `pnpm bench:diff --before=sockets-old.json --after=sockets-new.json`
  rather than expecting `--compare` to find a baseline the runner threw
  away.
## 2026-08-10 · Tier 1 — the per-principal watch split, counted (#180)

| | |
|---|---|
| Machine | Apple M4, darwin/arm64 |
| Node | v24.11.1 |
| Build | `dist/*.prod.js` (`--conditions=production`) |
| Commit | `1153c94` (dirty — the scenario itself) |
| Settings | 1 run × 150 ms slices |
| Conditions | **Contended** (probe varied 50%) — which is exactly why the gating rows are counts: every `exact` value below is identical on a quiet machine. Treat the two timing rows as shape, not figures. |

`live/principal-fanout` is the laptop-priced counterpart of Tier 3's
`sockets/principal-cliff` (2026-08-09 above): the costs that collapsed the
AKS rig between 100 and 250 identities, as counts that hold by
construction. One actor; P distinct identities watch `mine()` (which reads
`ctx.principal`); the control arm has the same identities watch the
identity-blind `current()`. Throttle 0, establishment sequential and
awaited — see the scenario header for why that makes the counts exact.

### The split, per distinct identity — all `exact`, all gate

| metric | P=1 | P=25 | P=100 | anon control (P=100) |
|---|---:|---:|---:|---:|
| `watch_loops` | 1 | 25 | 100 | **1** |
| `seed_turns` | 1 | 25 | 100 | — |
| `read_turns_per_publish` | 1 | 25 | 100 | **1** |
| `read_turns_per_publish_spread` | 0 | 0 | 0 | — |

One variable changes between the arms — does the read touch
`ctx.principal` — and every per-identity count collapses to 1. This is
#121's split (correct, per-identity views) priced in turns on one serial
queue. The #180 establishment fix (the watch read pump) does NOT move
these counts — every read still runs, one per loop per publish, and
`watch_loops` stays P (identity remains an input to the read until
#138-class semantics exist). What the pump moves is the queue-slot
contribution and seed ordering, which the informational rows below are the
scenario's window on.

### The mechanism's shape, informational

| metric | P=1 | P=25 | P=100 |
|---|---:|---:|---:|
| `seed_latency_under_load_ms` | 0.098 | 0.149 | 0.426 |
| `publishes_per_sec` | 39.3 k | 5.4 k | 1.0 k |

A new identity's seed queues behind the P re-reads the last publish
requested — establishment latency scales with P even on one warm CPU with
a trivial read. On the cluster the same mechanism, at 10 publishes/s under
a 1000m CPU limit and with the client dialling on a timeout, is the cliff:
seeds starve, clients give up and retry, and the rung fails while
steady-state delivery to the already-established is still fine. The
publish/drain round-trip falling ~40× from P=1 to P=100 is the O(P) turns
per publish, paid in-process.

## 2026-08-10 · Tier 3 — the identity cliff was the fetch pool (#194)

| | |
|---|---|
| Cluster | AKS, 3 × `Standard_D2ls_v6`, `limits.cpu 1000m`, `ENABLE_SESSIONS=1,ENABLE_SOCKET=1` |
| Images | pre-fix `db2b296` (recorded 2026-08-10 morning, above) · post-#193 `d4fc459` |
| Driver | in-cluster `ws-loadgen.mjs`, one pod, `connectBatch=50` |
| Artifacts | Actions runs 31363879792 (pre-fix), 31376118653 (pump, pool 64), 31378307872 / 31379030048 (the A/B), 31379350809 (recorded, pool 1024) |

The #193 watch read pump merged with a Tier-1 proof (`live/principal-fanout`:
~6× publish/drain throughput at P=25/100, seeds never behind re-reads) — and
the recorded `sockets/principal-cliff` did not move: `max_healthy_identities`
read **100** on the pump image exactly as it had before it. What changed is
that the failure became visible: at 250 identities the rung now showed **128
per-subscription 504s** (the new establishment deadline) instead of silent
hangs, one publish landing in a 270 s window.

**The binding constraint was `FETCH_CONNECTIONS` — the rig's undici pool for
host-to-host fetch, default 64 per peer origin.** Every per-principal
cross-host watch is a held-open HTTP response that PINS one pooled connection
for the life of the subscription (#138 keys coalescing on the principal, so
distinct identities never share). Sockets are host-affine, so ~⅔ of
subscribers relay: identities × ⅔ held streams across (replicas−1) = 2 pools
of 64 = 128 slots. 100 identities → ~67 streams → fits. 250 → ~167 → the
pool pins forever and every later relay→owner request — seeds AND the
publisher's own dispatch — starves behind connections that never free.
Predicted ceiling ≈ 190: the measured shelf sat between 100 and 250, on
every run, from the first measurement on.

### The A/B — same image, one variable

`ws-up env.fetchConnections=1024`, hand ladder, 10 publishes/s, 30 s rungs:

| identities | pool | connected | failures | sub errors | deliveries/s | p50 | p99 | publishes |
|---|---|---|---|---|---|---|---|---|
| 250 | 64 | 225 | 25 | **128** | 0.4 | — | — | **1** (in 270 s) |
| 250 | 1024 | 250 | **0** | 0 | 2 512 | 55.3 ms | 59.8 ms | 302 of 300 |
| 500 | 1024 | 500 | **0** | 0 | 5 021 | 55.0 ms | 69.0 ms | 302 |
| 1 000 | 1024 | 1 000 | **0** | 0 | 7 772 | 61.0 ms | 104.3 ms | 302 |

One thousand distinct signed-in identities on one actor, zero failures, full
publish rate, p50 at the 50 ms throttle floor. Pre-fix this rung connected
484 of 1 000 and delivered 2/s. At 1 000 the coalescing ratio dips
(`deliveriesPerPublish` 773) and p99 stretches — the delivery path starting
to work, not establishment failing.

### The recorded run under the sized pool

`ws-bench` on `d4fc459`, `env.fetchConnections=1024`:
**`max_healthy_identities: 500`** — every rung clean, so the value is the
ladder's own ceiling, not a cliff (the ladder default now reaches 1000, and
the hand rows above measured it clean). p50 55.1 ms, p99 78.0 ms at 500.

### What to take from it

- **Attribution, honestly.** #180 read the cliff as the owner's turn queue.
  That serialization was real (Tier 1 counts it) and is fixed, but the
  cluster shelf was the pool arithmetic — the establishment 504s the fix
  added are what made the difference diagnosable. Both halves were needed:
  the pump for the queue, the pool for the cluster number.
- **`FETCH_CONNECTIONS` is now part of `INFRA_SHAPE`** (this section's
  change), so a run under a different pool refuses to compare — which also
  means every artifact recorded before this section refuses against new
  ones. Deliberate: their pool size is unknown to the guard.
- **Sizing rule** for per-user live fan-out over the HTTP host transport:
  each relay pod holds ~`watchers ÷ replicas` streams to the owner, so set
  the pool per peer to at least that — an expectation, not a bound, so add
  headroom for placement skew. Or stop paying a connection per identity:
  `@sigx/actors-tcp`
  multiplexes every stream over one connection per peer (this measurement is
  its "justified by socket count" case on the client-fan-out axis), and #138
  would collapse identity-independent reads to one stream outright.

## 2026-08-11 · Tier 3 — declaring the read removes the identity ceiling (#138/#210)

| | |
|---|---|
| Cluster | AKS, 3 × `Standard_D2ls_v6` (2 vCPU), `limits.cpu 1000m`, 1 Redis |
| Shape | `ws replicas=3 nodes=3 image=0d2d38d knobs=ENABLE_SESSIONS=1,ENABLE_SOCKET=1,FETCH_CONNECTIONS=64,TRANSPORT=http` |
| Pool | **`FETCH_CONNECTIONS` at the chart default 64 — untouched.** That is the whole point (below). |
| Runtime | undici 7.29.0, `ws` 8.21.1 (lockfile at `0d2d38d`) |
| Driver | in-cluster `ws-loadgen.mjs`, ONE pod, `connectBatch=50`, 30 s rungs, 10 publishes/s |
| Artifacts | Actions runs 31520378502 (smoke), 31522212324 (`mine`), 31522864082 (`current`), 31524388378 (`shared`), **31524781111 (`ws-bench`, `sockets.json`)** |

#194 escaped the identity cliff by *resizing the pool* — 64 → 1024 — which
answered "can this deployment carry 1 000 signed-in watchers?" but not "does
the per-identity stream have to exist at all?". #201 shipped
`watches: { m: { principalIndependent: true } }` to answer the second
question, and this is the measurement: **the same ladder, the same untouched
64-connection pool, one variable.**

`Fanout` carries three reads that differ in exactly one thing each — and the
middle one matters most, because it isolates the declaration from the read
body:

| arm | reads `ctx.principal`? | declared `principalIndependent`? |
|---|---|---|
| `mine()` | yes | n/a |
| `current()` | no | **no** |
| `shared()` | no — byte-for-byte `current()`'s body | **yes** |

### The ladder — `principal=per-user`, one distinct identity per subscriber

| n | arm | connected | connect failures | subscription errors | deliveries/s | deliveries/publish | p50 | p99 | publishes |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | `mine` | 100 | 0 | 0 | 1 002 | 100 | 55.7 ms | 60.7 ms | 301 |
| 250 | `mine` | 216 | 34 | **128** | 0.3 | 88 | — | — | **1** (in 271 s) |
| 100 | `current` | 100 | 0 | 0 | 1 002 | 100 | 56.9 ms | 61.6 ms | 301 |
| 250 | `current` | 203 | 47 | **89** | 0.4 | 114 | — | — | **1** (in 271 s) |
| 100 | **`shared`** | 100 | 0 | 0 | 1 003 | 100 | 55.5 ms | 62.5 ms | 301 |
| 250 | **`shared`** | **250** | **0** | **0** | 2 506 | 250 | 53.9 ms | 57.6 ms | 301 |
| 500 | **`shared`** | **500** | **0** | **0** | 5 020 | 500 | 61.2 ms | 68.1 ms | 302 |
| 1 000 | **`shared`** | **1 000** | **0** | **0** | 10 028 | 1 000 | 62.0 ms | 69.4 ms | 301 |

Both control arms shelve between 100 and 250, and the ladder stops there —
the rig refuses to climb past a rung that could not hold its connections,
since that measures nothing but the failure mode. The
declared arm walks to the top of the ladder with zero failures, full publish
rate, `deliveries_per_publish` exactly equal to the subscriber count at every
rung, and p50 pinned near the 50 ms watch throttle floor. `peak_open` on the
declared 1 000 rung was **1 001** (the subscribers plus the publisher's own
connection), `peak_subscriptions` 1 000, `protocol_breaches` 0.

**The `mine` 250 rung reproduces #194 to the digit** — 128 subscription errors
against #194's 128, one publish landing in a ~270 s window. Same pool
arithmetic (2 relay pods × 64 slots), same cliff, a day and four merges later.
That reproduction is what licenses reading the `shared` column as a change in
mechanism rather than in weather.

### The mechanism, counted — why it moved

`ops.cluster.counters`, summed across the three hosts, delta over each run:

| arm | rungs in the run | subscriptions opened | `remoteWatches` | `coalescedWatches` | `inboundWatches` |
|---|---|---:|---:|---:|---:|
| `mine` | 100 + a failed 250 | 350 | **233** | **0** | 199 |
| `current` | 100 + a failed 250 | 350 | **232** | **0** | 185 |
| **`shared`** | 100 + 250 + 500 + 1 000 | 1 850 | **8** | **1 225** | 8 |

This is the claim in two columns. Sockets are host-affine, so ~⅔ of
subscribers watch an actor another host owns; undeclared, each of those opens
its own cross-host stream, which is a held-open HTTP response pinning one
pooled connection for the life of the subscription. `coalescedWatches` is 0 in
both control arms because the coalescing key carries the principal and no two
subscribers share one.

Declared, the principal leaves the key: **8 remote streams served 1 850
subscriptions**, and 1 225 joins landed on a stream that already existed. The
pool never binds because the thing that exhausted it no longer exists. The
smoke rung shows the same shape in miniature — 100 identities, 3 remote
streams, 59 coalesced joins.

### The recorded run — both arms in one artifact, one shape

The hand-run above is the experiment; this is the number a later run compares
against. `ws-bench` drove the `sockets/*` scenarios on the same deployment, so
`sockets/principal-cliff` (`mine`) and `sockets/declared-fanout` (`shared`)
carry the identical `INFRA_SHAPE` — no cross-shape inference required:

| scenario | `max_healthy_identities` | deliveries/s | p50 | p99 | `remote_watch_streams` | `coalesced_watch_joins` |
|---|---:|---:|---:|---:|---:|---:|
| `sockets/principal-cliff` | **100** | 1 004 | 54.1 ms | 61.8 ms | 267 | **0** |
| `sockets/declared-fanout` | **1 000** | 10 018 | 62.9 ms | 70.5 ms | **10** | 1 247 |

`declared_read: 1` rides in the `declared-fanout` artifact, so the arm cannot
be quoted under the wrong name later. The `principal-cliff` value reproduces
#184's recorded 100 and #194's shelf exactly. **1 000 is the ladder's own top,
not a cliff** — every rung was clean, as the hand-run table shows.

Two other scenarios ran in the same sweep and belong here as context rather
than as findings:

- **`sockets/hot-fanout`** — 10 000 *anonymous* subscribers on one actor:
  34 795 msg/s, p50 264.7 ms, zero failures, `peak_open` 10 001. It runs the
  **undeclared** `current()` read, and its `remote_watch_streams` is still
  **6** for 10 609 coalesced joins — because anonymous subscribers all encode
  to the same principal, so the coalescing key already matched. Undeclared
  plus anonymous coalesces; undeclared plus per-user opens 232 streams. That
  gap is precisely the asymmetry #138 closes: what anonymous fan-out got for
  free, a declared read now gets for signed-in fan-out.
- **`sockets/idle-capacity`** — 1 000 / 5 000 / 10 000 all connected with a
  zero failure rate, `peak_open` 10 000. One generator pod, so this says
  nothing about the ceiling (see the 2026-08-09 section).

### The slow-consumer arm, and what it does not say

`sockets/slow-consumer` also ran (it is part of the sweep, not of this
question). At the 5 000 rung with `slow_fraction=0.1`: **480 slow connections**
genuinely established, `drops` **0**, healthy delivery 19 055 msg/s, p50
258.1 ms.

> ⚠️ **This does not confirm #182 and cannot.** `client_max_buffered_bytes` is
> 0, and it is the *client's* outbox — the host has no `bufferedAmount`
> instrumentation at all (#208). The arm measures consequences: healthy
> subscribers kept being served and nothing was dropped while a tenth of the
> population read slowly. Read that as "not refuted", never as "no host
> buffering occurred".

### What to take from it

- **#138 removes the ceiling; it does not merely raise it.**
  `max_healthy_identities` **100 → 1 000, one declaration apart, same shape**.
  The controls fail at the same rung they failed at before #201, on the same
  pool. The declared arm reaches the ladder's own top — not a cliff — and does
  it at `FETCH_CONNECTIONS=64`, where #194 needed 1 024. The sizing rule from
  #194 still holds for reads that genuinely depend on identity; for reads that
  do not, the rule is now "declare it".
- **The declaration is the variable, not the read body.** `current()` and
  `shared()` are the same code. Anyone tempted to conclude "identity-blind
  reads are fine" from the older anonymous-fan-out numbers should read the
  `current` row: identity-blind and undeclared is exactly as expensive as
  identity-dependent, because the runtime cannot know the difference.
- **Delivery scales linearly across the declared ladder** — 1 003 → 2 506 →
  5 020 → 10 028 msg/s at 10 publishes/s — with p50 moving only 55.5 → 62.0 ms.
  At these rungs the constraint is neither establishment nor the pool. The
  ~16k/s/host delivery plateau from the 2026-08-09 section is still out there;
  this ladder does not reach it.
- **What this does NOT measure.** `TRANSPORT=tcp` (#203) is the third answer
  to the same problem and is deliberately unmeasured here — with the ceiling
  removed by declaration, multiplexing is a nice-to-have on this axis rather
  than a fix, and measuring it first would have risked crediting it. #182 is
  untouched: every `max_buffered_bytes` above is the client's, and means
  nothing about a host's (#208).

  *Measured the next day — see the section below. The "nice-to-have" reading
  was wrong in one important way: TCP is the ONLY answer for a read that
  genuinely depends on identity, because such a read can never be declared.*

## 2026-08-13 · Tier 3 — TCP does not remove the streams, it makes them free (#203/#210)

| | |
|---|---|
| Cluster | AKS, 3 × `Standard_D2ls_v6` (2 vCPU), `limits.cpu 1000m`, 1 Redis |
| Image | `698a5bd` (v0.8.0) — **both arms, same image**, see "Why the control was re-run" |
| Shape (http) | `ws replicas=3 nodes=3 image=698a5bd knobs=ENABLE_SESSIONS=1,ENABLE_SOCKET=1,FETCH_CONNECTIONS=64,TRANSPORT=http` |
| Shape (tcp) | `ws replicas=3 nodes=3 image=698a5bd knobs=ENABLE_SESSIONS=1,ENABLE_SOCKET=1,FETCH_CONNECTIONS=64,TRANSPORT=tcp` |
| Pool | `FETCH_CONNECTIONS` at the chart default 64 throughout — untouched, as in the 2026-08-11 section |
| Runtime | undici 7.29.0, `ws` 8.21.1 |
| Driver | in-cluster `ws-loadgen.mjs`, ONE pod, `connectBatch=50`, 30 s rungs, 10 publishes/s |
| Artifacts | Actions runs 31679261946 (http `mine`), 31679996106 (tcp `mine`, **discarded**), 31680288289 (tcp `mine`), 31680580939 (`ws-bench` tcp), 31687209213 (`ws-bench` http control) |

The 2026-08-11 section answered the identity ceiling for reads that CAN be
declared `principalIndependent`. This one answers the case that cannot:
`mine()` consults `ctx.principal`, so its subscribers genuinely need
per-identity answers, declaring it is forbidden and enforced
(`ActorWatchDeclarationError`), and the per-identity cross-host stream is
not a defect to remove. The question is whether that stream still has to
cost a pooled connection.

### The ladder — `read=mine principal=per-user`, one variable

| n | transport | connected | connect failures | sub errors | deliveries/s | p50 | p99 |
|---:|---|---:|---:|---:|---:|---:|---:|
| 100 | http | 100 | 0 | 7 | 933 | 54.7 ms | 62.5 ms |
| 250 | http | 224 | 26 | **128** | 0.4 | — | — |
| 100 | **tcp** | 100 | 0 | 0 | 1 004 | 52.1 ms | 55.2 ms |
| 250 | **tcp** | **250** | **0** | **0** | 2 510 | 52.3 ms | 55.9 ms |
| 500 | **tcp** | **500** | **0** | **0** | 5 014 | 52.3 ms | 54.5 ms |
| 1 000 | **tcp** | **1 000** | **0** | **0** | 10 029 | 52.7 ms | 67.2 ms |

The HTTP arm shelves between 100 and 250 with **128 subscription errors** —
the third independent reproduction of that exact figure (2 relay pods × 64
pool slots), after 2026-08-10 (#194) and 2026-08-11. The TCP arm walks the
whole ladder clean.

### Why it moved — the opposite mechanism to #138

`ops.cluster.counters`, summed across the three hosts:

| arm | subscriptions | `remoteWatches` | `coalescedWatches` |
|---|---:|---:|---:|
| http `mine` | 350 | 219 | **0** |
| **tcp `mine`** | 1 850 | **1 246** | **0** |
| http `shared` (2026-08-11) | 1 850 | **8** | 1 225 |

**TCP did not reduce the stream count. It kept every one of them.** 1 246
against the ⅔-of-1 850 prediction of 1 233 — one cross-host stream per remote
subscriber, exactly as on HTTP, with zero coalescing, because identity really
is an input to this read and no declaration could honestly say otherwise.

What changed is what a stream costs. Over `httpTransport()` each held-open
response pins one slot in a 64-deep undici pool per peer; over
`tcpTransport()` all of them multiplex onto one connection per peer. So the
two fixes on this axis are not variants of each other:

- **Declaring removes the streams** (1 233 → 8). Available only to a read
  that ignores `ctx.principal`.
- **TCP keeps every stream and removes its per-stream cost.** Available to
  any read, including the ones that can never be declared.

They compose: `sockets/declared-fanout` on TCP recorded **10** streams for
1 239 coalesced joins.

### The recorded runs — the 2×2, both sweeps on one image

`ws-bench` ran on BOTH transports on `698a5bd`, so every cell below is the
same image and differs only in the transport.
**`hosts_with_tcp_transport` = 3 on every tcp scenario and 0 on every http
one**, which is the gate #210 sets — the transport is a CHAIN
(`[tcpTransport, httpLeg]`), so a peer advertising no tcp address falls back
per LINK and would yield a clean HTTP measurement wearing the tcp label.

`max_healthy_identities`, and the streams that explain it:

| scenario | http | tcp |
|---|---:|---:|
| `sockets/principal-cliff` (`mine`) | **100** — 260 streams, 0 coalesced | **1 000** — 1 273 streams, 0 coalesced |
| `sockets/declared-fanout` (`shared`) | **1 000** — 10 streams, 1 227 coalesced | **1 000** — 10 streams, 1 239 coalesced |

Read the row and the column separately and the whole axis is in one table.
**Declaring fixes the http column** (100 → 1 000 by deleting the streams).
**TCP fixes the `mine` row** (100 → 1 000 by keeping all 1 273 and making
them cheap). Either alone suffices; the declared read is unchanged by the
transport because it had already stopped opening streams.

A stronger gate than `hosts_with_tcp_transport` exists and was checked by
hand: **`transportFallbacks` was 0 on all three hosts after 1 324 remote
watch streams** — not one link fell back. Since #223 the `ws-load` hand-run
prints it as `cluster/transportFallbacks` in its `delta` block and fails a
`TRANSPORT=tcp` run on any; the `ws-bench` artifact still records only
`hosts_with_tcp_transport`.

### The delivery path — a smaller and messier effect than it first looked

The tcp sweep also moved the ANONYMOUS fan-out numbers, which have nothing
to do with identity. Against the http sweep on the same image, one run each:

| `sockets/hot-fanout` | rung | http | tcp |
|---|---:|---:|---:|
| deliveries/s | 1 000 | 10 018 | 10 057 |
| | 5 000 | **37 285** | 32 334 |
| | 10 000 | 38 992 | **45 011** |
| p50 | 1 000 | 63.8 ms | **54.5 ms** |
| | 5 000 | 129.2 ms | **59.0 ms** |
| | 10 000 | 192.0 ms | **146.9 ms** |
| `deliveries_per_publish` | 5 000 | 4 746 | **5 000** |
| | 10 000 | 8 069 | **10 000** |

What holds up:

- **`deliveries_per_publish` is exactly the subscriber count on tcp at every
  rung**, while http falls to 0.95 and 0.81 of it at 5 000 and 10 000. Every
  publish reached every subscriber. This is the count-like figure and it is
  consistent.
- **p50 is lower on tcp at every rung**, by margins (129 → 59 ms at 5 000)
  far larger than the run-to-run spread this tier usually shows.

What does NOT hold up:

- **Throughput disagrees in sign between rungs** — tcp is 13% *lower* at
  5 000 and 15% higher at 10 000. The 5 000 row is explained by the achieved
  publish rate rather than by delivery: tcp completed ~194 publishes in the
  window against http's ~236, delivering more per publish and fewer overall.
  One run each, no repetition, and **timings in this tier never gate**. Treat
  the throughput column as unresolved.

> ⚠️ `AGENTS.md` describes `@sigx/actors-tcp` as "justified by socket count,
> not latency". These figures are the first evidence that understates it at
> high fan-out — but a single pair of runs with a sign disagreement is not
> enough to rewrite that claim. It needs repetition before anyone edits it.

**An earlier draft of this section quoted +29% throughput and a halved p50**,
from comparing the tcp sweep against the 2026-08-11 http sweep. Those ran on
different images: `698a5bd` improved http hot-fanout on its own (34 795 →
38 992 msg/s and p50 264.7 → 192.0 ms at 10 000). Roughly half of the
apparent tcp win was v0.8.0. The http control sweep exists solely to catch
that, and it did.

### Why the control was re-run rather than taken from 2026-08-11

`main` moved from `0d2d38d` to `698a5bd` (v0.8.0) between the two sessions —
**~320 changed lines in `cluster/placement.ts`, landing in the remote-watch
region**, plus `cluster/counters.ts`. Comparing TCP-at-`698a5bd` against the
recorded HTTP-at-`0d2d38d` would have confounded the transport with the
runtime, so the HTTP `mine` ladder was re-run on the new image and that is
the control above. Nothing in the tooling would have objected — the hand-run
path has no `INFRA_SHAPE` guard (#224).

Two counters were added in v0.8.0 (`targetedDispatches`, plus a `?? 0` in
`addCounters` for mixed-version clusters); the three used for attribution
here are unchanged in meaning.

### One discarded run, and why it is named

The first TCP ladder (run 31679996106) hit **4 connect failures at n=100**
and aborted — `ws-loadgen` breaks the ladder on any connect failure — so it
never reached the rungs that matter. The cluster had rolled over ~90 s
earlier and was still converging (`unreachableRetries` non-zero, membership
still moving). Re-run after it settled: every rung clean.

It is recorded here because of what it would have done on the `ws-bench`
path: `max_healthy_identities` would have read as a failure at the first
rung and **TCP would have looked useless** — a plausible wrong number rather
than an error. That is #222. A transport flip needs a settle period before
measuring; pod readiness is not membership convergence.

## 2026-08-13 · The save-path serialize cost, priced (#227, the rest of #124)

| | |
|---|---|
| Machine | Dedicated bench VM — AMD EPYC 9V74, 4 vCPU, linux/x64 |
| Node | v24.18.1 |
| Build | `dist/*.prod.js` (`--conditions=production`) |
| Commit | `b78df80` |
| Settings | `bench.yml` dispatch, 5 rounds × 400 ms, interleaved, branch on BOTH sides |
| Conditions | Quiet. The identical-commit A/B called **no verdict across 525 metrics**, which is also the noise audit for the three new scenarios below: every new row read `no change`. |

#124's reporter re-profiled on 0.5.0 and `@sigx/serialize` moved only −7%
while `@sigx/actors` fell −88%: the codec cost that remains lives on the
SAVE path (`job.checkpoint()` re-encoding the whole growing state per step)
and on the job READ path — and nothing in the suite measured either.
`state/dirty-growth` deliberately never saves, `state/explicit-save` is
fixed-size, and no `defineJob` fixture existed in Tier 1 at all. Three
scenarios close that hole.

### `state/save-growth` — append + `ctx.save()` per turn, 500 steps

Three arms, identical except one variable each. `mem` is explicit
persistence with no subscriber, so no tracking is installed: the turn is
body + ONE `encodeWithHandlers` walk + a by-reference store.

| arm | head (µs/turn) | tail (µs/turn) | tail delta vs `mem` |
|---|---:|---:|---|
| `mem` | 36.2 | 190.3 | — (the host encode walk) |
| `stringify` | 47.7 | 287.8 | **+97 µs, +51%** — the adapter's own `JSON.stringify` walk |
| `mem+sub` | 80.6 | 595.5 | **+405 µs, +213%** — deepTrack + the boundary `#snapshot()` |

- **Every real storage adapter pays the `stringify` arm.** pg, redis,
  surreal and `fileStorage` all stringify the encoded tree, so the honest
  Tier-1 floor for a durable checkpoint is ~288 µs/turn at 500 rows, not
  190. The two walks measure the same tree twice; a single-walk
  encode-to-string in `@sigx/serialize` would collapse the delta.
- **A subscriber still triples the turn.** Cross-check: `state/dirty-growth`
  (walk + snapshot, NO save) tails at 416 µs on the same run, and
  595 ≈ 190 + (416 − dispatch floor). The arms agree with each other.
- Per-arm `settleGc()` matters: without it an arm's head window pays the
  previous arm's GC debt — measured at 3× on `mem/head_turn_us` locally.

### `jobs/status-read` — the `JobInfo` clone, by checkpoint size

A paused job holding a `rows`-sized checkpoint, closed-looped through
`status()`. `status()` returns 8 fields that deliberately EXCLUDE the
checkpoint — and builds them via `toInfo(ctx.snapshot())`, a full
encode+revive of everything.

| checkpoint | status()/s | p99 |
|---|---:|---:|
| rows=0 | 406.7 k | 3.4 µs |
| rows=200 | 6.1 k | 183.1 µs |
| rows=2000 | 597.1 | 2.03 ms |

- **681× from empty to 2 000 rows, linear in rows** (10× rows ≙ 10.2×
  slower). The read pays for state it does not return; a `$live` watch on
  `status()` re-pays it per tick, handing back the #130 ticksOnly win.
- This ladder is the acceptance metric for building `JobInfo` without the
  whole-state clone: after that fix it must go FLAT.

### `jobs/checkpoint-growth` — the reporter's composite, 300 steps

A real `defineJob` whose run body appends one step's output and
checkpoints, released one step at a time; release→checkpoint-ack timed.

| arm | head (µs/step) | tail (µs/step) |
|---|---:|---:|
| `watch=0` | 19.8 | 113.9 |
| `watch=1` | 44.2 | 290.2 |

- **A watcher multiplies a checkpoint by ~2.5×** (tail +176 µs/step): the
  boundary snapshot plus `toInfo` over the same growing state, on top of
  the save encode. `job.watch()` hard-codes `ctx.changes({ initial: true })`
  with no throttle knob, so a consumer cannot opt into #130's coalescing —
  that knob, and the `{version, encoded}` cache deferred from #129, compete
  to close this gap; this pair of rows is their before.

### Why no `exact` metric here

The deterministic counters exist (encodes per step are exact by
construction under serial dispatch) but belong in the fix PRs' unit tests,
probe-counter style: an `exact` bench metric pinned to today's counts would
put these scenarios in the merge-queue gate and then FAIL it on the very
improvement the fixes intend — the same split as the 2026-08-06 section.

## 2026-08-13 · The save-path fixes, measured (#229/#231/#233 — closing out #124)

| | |
|---|---|
| Machine | Dedicated bench VM — AMD EPYC 9V74, 4 vCPU, linux/x64 |
| Node | v24.18.1 |
| Build | `dist/*.prod.js` (`--conditions=production`) |
| Settings | `bench.yml` dispatch, 5 rounds × 400 ms, interleaved |
| Compared | `ad52a84` (the #227 scenarios, pre-fix) → `7100e61` (#230 + #232 + #234) |

The section above priced three serialize costs; three PRs later, the same
scenarios say what the fixes bought. Every row below was CALLED — all five
rounds agreed in sign.

| scenario · metric | before | after | |
|---|---:|---:|---:|
| `jobs/status-read` rows=0 | 408.5 k/s | 723.1 k/s | +77% |
| `jobs/status-read` rows=200 | 6.1 k/s | 734.0 k/s | **+11 920%** |
| `jobs/status-read` rows=2000 | 598.1 /s | 736.1 k/s | **+123 073%** |
| `jobs/checkpoint-growth` `watch=1` tail | 295.2 µs | 183.4 µs | **−38%** |
| `jobs/checkpoint-growth` `watch=0` tail | 117.3 µs | 114.5 µs | no change |
| `state/save-growth` `mem+sub` tail | 594.9 µs | 420.7 µs | **−29%** |
| `state/save-growth` `mem` tail | 192.5 µs | 192.3 µs | no change |
| `state/save-growth` `stringify` tail | 289.2 µs | 286.4 µs | no change |

- **The `jobs/status-read` ladder is FLAT** (~730 k/s at every rung): #229
  removed the whole-state clone from job reads, so a read no longer pays
  for the checkpoint it does not return. p99 at 2 000 rows: 2.05 ms → 1.9 µs.
- **The watched-checkpoint premium halved** (+178 → +69 µs/step): #233
  reuses the save's encode for the boundary snapshot, so a save+emit
  boundary costs one whole-state encode. What remains of the premium is
  the walk, the revive and delivery.
- **`watch=throttled` (head-only, #231)** records at the `watch=0` floor —
  a consumer that opts in drops the rest of the premium for a burst.
- **`mem` and `stringify` unchanged, as designed** — no fix targeted the
  storage encode itself or the adapter's second walk. Those are the
  remaining O(state) terms per durable save; the adapter walk's
  single-pass answer is upstream (signalxjs/core#657).
  **Superseded — see 2026-08-15 (#238).** That answer shipped, was wired in
  and MEASURED SLOWER (7/7 rounds), because the adapter's walk is native and
  the single-pass one is not. The gap is still real and still unfixed; what
  changed is that it is no longer waiting on core#657.

## 2026-08-14 · Where a delivery's CPU actually goes (#245)

| | |
|---|---|
| Machine | 12th Gen Intel Core i9-12900HK, win32/x64, 20 threads |
| Node | v22.22.0 |
| Build | `dist/*.prod.js` (`--conditions=production`) |
| Rig | `perf/aks/ws-dev.mjs` + `ws-loadgen.mjs`, BOTH ends on one laptop |
| Load | `MODE=hot CONNECTIONS=2000 PUBLISH_RATE=20 DURATION_S=50`, one actor |
| Profile | `--cpu-prof --cpu-prof-interval=200`, steady-state window 15–60 s |

The 2026-08-09 section attributed the socket fan-out ceiling to serialization —
"O(1) actor reads and **O(N) serializations**". The count is right. **The
attribution is wrong, and this section retracts it.**

### Self time, by bucket (% of BUSY samples)

| bucket | payload 0 B | payload 4 KB | 0 B, `SOCKET_PING_MS=0` |
|---|---:|---:|---:|
| socket write (`writev` + `node:net`/stream) | **77.4** | **73.2** | **83.1** |
| `ws` (sender/receiver) | 6.1 | 5.2 | 6.0 |
| **write path, combined** | **83.6** | **78.4** | **89.1** |
| `@sigx/actors` (incl. `reply`'s own `JSON.stringify`) | 7.8 | 16.5 | 7.2 |
| `@sigx/serialize` (`encodeWire`'s walk) | 1.7 | 1.6 | 2.5 |
| timers (`armPing`'s clear/set pair) | 2.6 | 1.2 | **0.02** |
| GC | 1.1 | 1.4 | 0.7 |
| idle (of ALL samples) | 26.7 | 22.6 | 22.4 |

One frame is the whole story: **`writev` (native) is 69–72% of busy time on
its own.** It is one socket write per subscriber per delivery.

### The host was never CPU-saturated

`ws-dev.mjs` samples its own load since this run. Under the 2 000-subscriber
fan-out it held **`cpu` 0.58–0.77 of one core** with 22–27% of samples idle,
and **`cpuSystem` ran ~2× `cpuUser`** throughout (0.37–0.52 vs 0.15–0.25) —
two independent instruments agreeing that the process spends most of its
on-CPU time in the kernel, in the write syscall.

That also settles a methodological point for the whole tier: the 2026-08-09
figure of **"~62 µs per delivery" is a throughput reciprocal, not a CPU cost**,
and it may not be read as one. The host under it was not saturated.

### Against the thresholds this run committed to in advance

| pre-registered threshold | measured | verdict |
|---|---|---|
| ws/net > 40% ⇒ the ceiling is the write path | 78–89% | **met, decisively** |
| serialize + `JSON.stringify` < 15% at 0 B ⇒ a per-value encode cache is not the headline fix | 1.7% (≈5% counting `reply`'s own stringify) | **met** |
| …unless the 4 KB arm clears ~30% | 14.6% | **not met** |
| timers > 10% ⇒ the `armPing` re-arm leads | 2.6% | **not met** |
| GC > 20% ⇒ allocation is the cost | 1.1–1.4% | **not met** |

**So the encode-once-per-value fan-out cache is cancelled.** At 2 000
subscribers the entire serialization term it would collapse is under 2% of
busy time, and even a 4 KB payload leaves it under 15%. It would have been a
correct change to a term that is not the constraint — and it carried a real
staleness hazard (a read returning a live reference to state) plus retained
strings behind a slow consumer. Not worth it against this profile.

### What the profile says the constraint IS

Every subscriber is its own socket, so a publish costs **N `writev`
syscalls**, serialized. Nothing in the serialization path divides that, and
neither does `cork()`/`uncork()` on this shape: with one subscription per
connection there is only ever one frame per socket per tick to coalesce.

Two levers follow, and only two:

1. **Deliver less.** Deliveries per subscriber is the multiplier on the
   syscall count, and today no client can influence it — every live
   subscription runs at `DEFAULT_WATCH_THROTTLE_MS` (`watch-core.ts`, 50 ms),
   because neither the socket session nor `$live` passes `throttleMs`
   through; see "Two limits that bound every figure above" in the 2026-08-09
   section. A subscriber that opts into a 1 s window costs 1/20 of the
   syscalls. This is now the headline fix rather than a secondary one.
2. **Carry more subscriptions per connection** — measured, below, and worth
   an order of magnitude.

### The same 2 000 subscriptions, on 250 sockets instead of 2 000

One variable: `CONNECTIONS=250 SUBS_PER_CONN=8` against `CONNECTIONS=2000
SUBS_PER_CONN=1`. Same actor, same publisher, same 0-byte payload, same 2 000
subscriptions.

**Two pairs, run on different builds a few minutes apart**, because the first
pair alone put a 10× claim on one run each:

| pair | | 2 000 sockets × 1 sub | 250 sockets × 8 subs |
|---|---|---:|---:|
| 1 | deliveries/s | 12 067 | **27 095** |
| 1 | host CPU (fraction of one core) | 0.58–0.77 | **0.13–0.19** |
| 1 | **host CPU µs per delivery** | **~54** | **~5.5** (≈10×) |
| 2 | deliveries/s | 15 461 | **25 994** |
| 2 | host CPU (fraction of one core) | 0.57–0.62 | **0.12–0.21** |
| 2 | **host CPU µs per delivery** | **~38** | **~6.3** (≈6×) |
| | p50 | 59.5 / 59.5 ms | 63.0 / 62.6 ms |

**6–10× less host CPU per delivery, for the same subscription count.** The
pairs disagree on the ratio — the 1-sub arm is the noisy one, 12 067 against
15 461 for the same shape — and agree on everything that matters: same sign,
same order, far outside the spread either arm shows on its own. Quote the
range, not either figure. The
frames are identical and `ws` still calls `send()` once per subscription —
what changes is that eight frames queued to ONE socket in one tick leave as
one `writev`, because Node's writable already coalesces its queue. The
syscall count, not the frame count, is the cost.

Two consequences worth carrying:

- **`sockets/hot-fanout` measures the worst case, not the typical one.** Its
  1-subscription-per-socket shape maximises the term that dominates. A real
  page multiplexes every live read onto one connection — which `$live` and
  the socket transport already do — so a deployment's own numbers should be
  better than this scenario's, and the scenario should not be quoted as "what
  a browser costs".
- **Explicit `cork()`/`uncork()` has less headroom than it first appears**,
  because the coalescing it would force is largely already happening. It is
  worth measuring against this baseline before it is worth building.

### Two smaller findings, kept

- **`armPing()` runs on every delivery frame** (`socket-session.ts:306` —
  `reply()` calls it unconditionally, and it is `clearTimeout` + `setTimeout`).
  Turning the ping off took the timers bucket from 2.60% to **0.02%** of busy.
  A real 2.6%, and a ten-line fix, but not the ceiling. Throughput moved
  8 895 → 9 492 deliveries/s across the two arms, which is inside this rig's
  run-to-run spread and should not be quoted as the win — the bucket share is
  the robust number.
  **Fixed (#250):** the deadline is now a timestamp plus one
  self-rescheduling timer instead of a clear/set per frame. Same rig, same
  arm, ping still ENABLED at the 30 s default: the timers bucket reads
  **0.04%** — the `SOCKET_PING_MS=0` control's 0.02%, with the keepalive
  still running. The whole cost is recovered rather than traded away.
- **Payload size barely registers up to 4 KB**: 8 895 → 8 760 deliveries/s,
  while `reply`'s own `JSON.stringify` share went 3.1% → 13.0%. The host
  absorbed the extra encode out of idle rather than out of throughput, which
  is what a write-bound process looks like.

### What this rig cannot say

Loopback, both ends on one box, Windows, and the profiler itself is in the
measurement. Absolute throughput here means nothing next to the AKS figures.
What transfers is the **shape**: one write syscall per subscriber per
delivery, dominating everything the runtime does per frame. On a real NIC
that term gets larger, not smaller.

## 2026-08-15 · Tier 3 — the host's own numbers, and #182 answered (#252/#258)

| | |
|---|---|
| Shape | `ws replicas=3 nodes=3 image=98cff96 knobs=ENABLE_SESSIONS=1,ENABLE_SOCKET=1,FETCH_CONNECTIONS=64,TRANSPORT=http` |
| Cluster | AKS, 3 × `Standard_D2ls_v6` (2 vCPU), `limits.cpu 1000m`, 1 Redis |
| Driver | in-cluster `ws-loadgen.mjs` Jobs, `testenv.mjs ws-bench`, `--runs=1` |
| Artifact | Actions run **31878735718** (`sockets.json`) |

The first socket run carrying the host-side instrumentation from #252. Two of
its numbers have never existed before, and one of them settles a question two
previous sessions had to leave open.

### `sockets/slow-consumer` — the hosts buffer, without bound

5 000 subscribers, 10% of them made to stop reading (480 connections):

| metric | value |
|---|---:|
| **`peak_host_buffered_bytes`** | **367 153 508** (~350 MB) |
| `client_max_buffered_bytes` | **0** |
| `drops` | 0 |
| `protocol_breaches` | 0 |
| `host_deliveries` | 2 036 000 |
| `host_delivery_bytes` | 8 449 376 000 |

**350 MB queued across three 1000m pods while every client-side signal reads
zero.** The 2026-08-09 section's "`maxBufferedBytes` stayed 0 at every rung"
was true and meant nothing: it was the CLIENT's buffer, as #208 pointed out.

Nothing dropped and nothing broke, which is the finding rather than a
mitigation of it — there is no backpressure to observe failing. The host
accepts everything the fan-out produces and holds it until the socket drains,
so **a slow consumer is funded out of the host's heap.** Filed as #258 for a
decision; this section is the number it rests on.

### The host's own delivery counts, first recorded

`sockets/hot-fanout`, summed across hosts over the run: `host_deliveries`
2 848 000 and `host_delivery_bytes` 163 584 000. Every delivery figure on this
tier before today was the GENERATOR's count. Both are now recorded, and a gap
between them is a finding rather than an inconsistency — on this run there was
none worth reporting.

`throttle_quantized` is 0 on every scenario, as it must be: `ws-loadgen.mjs`
sends no `w`, so every subscription ran at the default 50 ms window. A run
that sets one is a different measurement (#247).

### The identity numbers reproduce exactly

| scenario | 2026-08-11 | today |
|---|---:|---:|
| `sockets/principal-cliff` `max_healthy_identities` | 100 | **100** |
| `sockets/declared-fanout` `max_healthy_identities` | 1 000 | **1 000** |

Two images and five days apart, the cliff and the declared arm's ladder-top
land on the same integers. That is the strongest evidence yet that these two
are properties of the mechanism rather than of the weather.

### The throughput deltas, and why they are NOT attributed

Against run 31524781111 (2026-08-11, image `0d2d38d`):

| `sockets/hot-fanout` | before | after | |
|---|---:|---:|---|
| 1 000 deliveries/s | 10 028 | 10 033 | +0.0% |
| 5 000 deliveries/s | 39 831 | 39 631 | −0.5% |
| 10 000 deliveries/s | 34 795 | **44 646** | +28.3% |
| 1 000 p50 | 56.5 ms | 63.2 ms | +12.0% |
| 5 000 p50 | 137.9 ms | **106.3 ms** | −22.9% |
| 10 000 p50 | 264.7 ms | 291.8 ms | +10.2% |
| 5 000 `deliveries_per_publish` | 4 780 | **5 000** | +4.6% |
| 10 000 `deliveries_per_publish` | 8 468 | **10 000** | +18.1% |

> ⚠️ **None of this is attributable to any particular change.** `0d2d38d` to
> `98cff96` spans v0.8.0, v0.9.0, the whole #124 save-path arc, two new
> packages and the four PRs of the #245 investigation. The 2026-08-13 TCP
> section already paid for this lesson — half an apparent transport win turned
> out to be v0.8.0 — which is why it re-ran its control on the same image.
> Attributing any row here needs a same-image A/B, i.e. deploying a second
> image with the change reverted, i.e. another paid session.

What can be said without attribution: **`deliveries_per_publish` is now
exactly the subscriber count at every rung**, where the 2026-08-11 HTTP run
fell to 0.95 and 0.85 of it at 5 000 and 10 000. That is a count, not a
timing, and it means every publish reached every subscriber.

### One correction to the method

**`pnpm bench:diff` enforces `INFRA_SHAPE` too.** The `--compare` path is not
the only guarded one — `compare-files.ts` runs the same `fatalMismatch` check
and refused this pair outright, because the image tag is part of the shape:

```
refusing to compare — the deployments differ:
  deployment shape: … image=98cff96 … vs … image=0d2d38d …
```

That is the guard working. It also means "record with `bench:diff` across
images" is not a procedure that exists, and the table above was assembled by
hand from the two artifacts, precisely so it could be labelled unattributable
rather than printed as a verdict.

## 2026-08-15 · The single-walk save path, measured and REJECTED (#238)

| | |
|---|---|
| Machine | Dedicated bench VM — AMD EPYC 9V74, 4 vCPU, linux/x64 |
| Node | v24.18.1 |
| Build | `dist/*.prod.js` (`--conditions=production`) |
| Settings | `bench.yml` dispatch, 7 rounds × 400 ms, interleaved |
| Compared | `98cff96` (main) → `e748629` (the #238 implementation) |
| Conditions | Quiet. **No verdicts called across 527 metrics**, and the three pre-existing arms held (`mem` 191.2 → 194.3, `stringify` 288.7 → 293.5, `mem+sub` 416.2 → 420.1) — which is what makes the within-run pairings below readable. |

The 2026-08-13 section closed with the adapter's second walk as the one
remaining O(state) term per durable save, priced at **+51%**, and said the
answer was upstream. It arrived: signalxjs/core#663 shipped
`stringifyWithHandlers` in `@sigx/serialize` 0.15.5 — the codec's JSON in one
walk. #238 wired it in behind an optional `ActorStorage.saveText`.

**It made the save slower, and the change was reverted.** The new arms are
read WITHIN the head run against the arm each is meant to beat, because a
new arm has no "before" and cannot appear in an A/B table.

| pair (500-row tail) | two-walk | one-walk | delta | rounds agreeing |
|---|---:|---:|---:|---|
| `state/save-growth` dated rows | 293.5 µs | 315.5 µs | **+7.5%** | **7/7** |
| `state/save-growth` scalar rows | 152.2 µs | 155.7 µs | **+2.4%** | **7/7** |
| `jobs/checkpoint-growth` `watch=0` | 182.5 µs | 216.5 µs | **+20%** | **7/7** |

Per-round deltas, dated rows: +9.2 +6.7 +4.8 +5.9 +8.7 +11.5 +7.1 %.
Scalar rows: +1.3 +5.3 +2.8 +2.3 +2.4 +3.2 +0.8 %.

**These three rows are a hand sign-test, not `ab-report` verdicts**, and the
two are not in conflict. `ab-report` compares a metric on the base side
against the same metric on the head side; every arm in the table above
exists only on the head side, so it has nothing to compare and correctly
says nothing — which is also why the run as a whole reports no verdicts. The
judgement here is the other axis: two arms measured **within one round**,
differing in one variable, across seven rounds. Unanimous sign is what
carries the scalar pair despite its small median; a mixed sign claims
nothing, which is exactly what the head windows below do.

### Why — the +51% was never a walk that could be deleted

**The adapter's second walk is NATIVE.** `JSON.stringify` over the encoded
tree runs in C++; the host's `encodeWithHandlers` is the JS one. Replacing
*JS encode + native stringify* with *one JS emitter* only wins where the
fused walk can still reach the native serializer, and core's answer to that
is a pure-JSON fast path: a node whose own values are all JSON-native
scalars goes to `JSON.stringify` wholesale. `pureScalars1` **rejects a
`Date`**.

`GrowingSaver` carries `at: new Date(…)` in every row, so every row was
disqualified and emitted key by key in JS. `stringify-scalar` /
`text-scalar` exist to isolate exactly that: the same two storages over rows
with the `Date` removed and nothing else changed.

**Removing the `Date` halves the penalty — 7.5% → 2.4% — but does not
reverse it.** So the fast path is real and it is not the whole story. What
is left is granularity: the fast path fires **per node**, so 500 small rows
means ~500 separate native calls whose results are joined in JS, against
**one** native call over the whole tree in the two-walk version. Actor state
is a large collection of small nodes, which is the shape this loses on.

The head windows agree from the other side: at small state both pairs go
mixed-sign (dated 3/7, scalar 1/7 — no claim), which is what fewer nodes and
less joining should look like.

### What this means

- **`state/save-growth`'s `mem` / `stringify` gap is not addressable this
  way.** It stays the remaining O(state) term per durable save, and it stays
  unfixed — but it is no longer "waiting on upstream", it is waiting on a
  fused emitter that batches runs of pure-scalar nodes into one native call.
  Filed upstream with these numbers.
- **Do not re-attempt this from the issue text alone.** #238 reads like a
  +51% prize; it is not one. The `text*` arms and the `saveText` seam live on
  in the `238-storage-save-text` branch, so a re-measurement after an
  upstream change is a rebase, not a rewrite.
- **The measurement method is the reusable part**: a new arm has no "before",
  so pair it against its control inside one run and sign-test the rounds. The
  A/B table cannot judge an arm that only exists on one side.

## 2026-08-17 · The single-walk save path, re-measured and ACCEPTED (#265)

| | |
|---|---|
| Machine | Dedicated bench VM — AMD EPYC 9V74, 4 vCPU, linux/x64 |
| Node | v24.18.1 |
| Build | `dist/*.prod.js` (`--conditions=production`) |
| Settings | `bench.yml` dispatch, 7 rounds × 400 ms, interleaved |
| Compared | `73a2cbf` (main) → `00f7e06` (`266-save-text-v2` = `e748629` rebased onto main, `@sigx/serialize` catalog `^0.15.6`) |
| Conditions | Quiet. No `improved`/`regressed` verdicts across the shared metrics, and the three pre-existing arms held (`mem` 193.9 → 194.0, `stringify` 291.5 → 292.1, `mem+sub` 421.9 → 417.9) — same readability condition as the 2026-08-15 run. |

The section above ended with the gap "waiting on a fused emitter that batches
runs of pure-scalar nodes into one native call". It shipped:
signalxjs/core#667 (`@sigx/serialize` **0.15.6**, tracked as core#666) makes
the pure-JSON fast path fire per RUN — consecutive eligible rows go to the
native serializer in one call, and a row whose only codec hit is a built-in
scalar-payload leaf (`Date`, `BigInt`, `URL`, explicit `undefined`) joins the
run via a shallow encoded copy instead of disqualifying its node.

**Same branch, same arms, same hand sign-test as the 2026-08-15 section —
opposite sign, unanimously:**

| pair (500-row tail) | two-walk | one-walk | delta | rounds agreeing |
|---|---:|---:|---:|---|
| `state/save-growth` dated rows | 292.1 µs | 182.9 µs | **−37.7%** | **7/7** |
| `state/save-growth` scalar rows | 152.7 µs | 90.2 µs | **−44.4%** | **7/7** |
| `jobs/checkpoint-growth` `watch=0` | 181.9 µs | 113.1 µs | **−39.8%** | **7/7** |

Per-round deltas, dated rows: −37.8 −39.7 −37.5 −37.7 −35.4 −35.6 −37.9 %.
Scalar rows: −43.5 −45.0 −44.4 −44.9 −43.8 −20.2 −44.6 %. Checkpoint:
−39.8 −41.5 −38.6 −41.3 −37.8 −25.7 −40.0 %.

Head windows: scalar now reads −30.5% median (6/7 negative) — enough rows to
batch even at ~100 — while the dated head window stays mixed-sign and claims
nothing, as before.

### What this means

- **`text` at 182.9 µs undercuts `mem` at 194.0 µs.** A save that emits the
  wire string outright is now cheaper than storing the tree by REFERENCE,
  because the fused walk never builds the tree at all. The "remaining
  O(state) term per durable save" from 2026-08-13 is not merely fixed — the
  stringify-storage save is now the cheapest durable configuration measured.
- **Every closing item of the 2026-08-15 section is resolved**: the upstream
  filing (core#666) produced the batching emitter; the parked branch was the
  promised rebase-not-rewrite; and the within-run sign-test carried the
  verdict again, this time in the direction #238 originally hoped for.
- The scenario-level `gc/*` rows read large "regressions" against base in the
  raw table — an artifact of the head side running two extra arms in the same
  scenario, not of the save path allocating more; the shared arms above are
  the like-for-like comparison.

## 2026-08-26 · Tier 3 — the workflow engine, first measurement (#297, the #85 workload)

The third Tier-3 axis: an ENGINE on the cluster. Every run is an
event-driven `WorkflowRun` actor — one Redis CAS per node, a volatile timer
or a durable reminder per delay, a `defineWorker` pool per task, child
runs with a durable join, a signal with a timeout edge, and a completion
published to one singleton aggregator. Nothing else in the suite makes the
runtime do all of those in one request, which is why an engine is the
workload. Read the tier legend before comparing this with `infra/*` or
`sockets/*`: it is driven in-cluster and measures runs, not requests.

**Shape:** `wf replicas=3 nodes=3 image=fccf287 knobs=FETCH_CONNECTIONS=64,TRANSPORT=http,WF_REMINDER_TICK_MS=1000`
— three 1 vCPU host pods on three nodes, HTTP host-to-host, the reminder
tick lowered from the runtime's 30 s to 1 s so a durable wake's lag is a
number rather than a design constant. Every other engine knob at its
default: 30 s timer threshold, deactivate-on-sleep on, 20 ms tasks, 10%
failure rate, 8-wide fan-out to child runs. Generator: one pod, Poisson
arrivals, 60 s per rung, the default mix `order:50,approval:20,etl:20,saga:10`.

### The hand-run ladder (`wf-load sweep=10,25,50 WF_DELAY_MS=2000`)

Recorded from `testenv.mjs wf-load`, run 32957835456, before the recorded
`wf-bench` path existed for this axis — so treat it as the first
measurement, exactly as the 2026-08-09 socket section is treated.

| runs/s offered | started | stuck | start p50 / p90 | order p50 | etl p50 (8 children) | join p50 / p90 | approval p50 | saga p50 | wake lag p50 / p99 | lost wakes |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 587 | **0** | 12 ms / 96 ms | 2.08 s | 260 ms | 184 ms / 311 ms | 2.37 s | 74 ms | 0 / 57 ms | 0 |
| 25 | 1 576 | **0** | 169 ms / 3.2 s | 2.20 s | 5.9 s | 2.7 s / 16.8 s | 4.5 s | 169 ms | 1 / 117 ms | 0 |
| 50 | 2 115 (+973 starts 504'd) | **0** | 26 s / 107 s | 2.23 s | 204 s | 107 s / 240 s | 57 s | 234 ms | 1 / 117 ms | **131** |

(`order` carries a 2 s delay and `approval` a 2 s signal, so their floors
are 2 s; a fifth of approvals deliberately get no signal and take the 30 s
timeout edge, which is the p90 in that column.)

### ✅ At 10 runs/s the engine is invisible

Start p50 12 ms, every node's cost is its own work (task p50 20 ms — the
task IS 20 ms), a delay wakes with **0 ms median lag** on a volatile timer,
a child fan-out of eight completes in 260 ms, and nothing is left behind.
4 436 transitions and ~100 saves/s across the fleet at ~5 vCPU-seconds
per second of load.

### ❌ The knee is ~25 runs/s of this mix, and the JOIN is what bends first

At 25 runs/s children still finish in 217 ms (p50) but the parent resolves
the join **2.7 s** later (p90 16.8 s) — so `etl` goes from 260 ms to 5.9 s
while every other template barely moves. The join is a `childDone` call
from each child INTO the parent, cross-host 88% of the time
(`remoteDispatches` 35 134 vs `routedLocal` 4 931 over the ladder), and
those calls queue behind the parent's own turns and behind every other
inbound dispatch on the host. Start latency shows the same queueing from
the outside: p50 169 ms, p90 3.2 s. The CPU arithmetic says why the knee
is here — the mix costs ~1.45 vCPU-seconds of sha256 per second at 25/s on
a fleet of 3 vCPU, before a single save.

### ❌ At 50 runs/s the cluster does not shed load, it drowns — and recovers

973 of 3 088 starts hit the 30 s call deadline (504 at the Service), yet
`unknownEvents: 1018` says most of those runs RAN — the caller lost the
ack, the engine did the work. Start p50 26 s. The recovery machinery all
fired, and all of it was needed:

- **131 lost wakes** — a reminder tick delivered a wake whose dispatch then
  timed out; the shard entry was already gone (at-most-once), and the run
  was recovered by the sweep's `status()` touch. `wakesLost` is the
  runtime finding this axis was built to produce, and it appeared at the
  first overloaded rung.
- **194 child starts failed and 194 were repaired** by the parent's
  `join-check` reminder — the idempotent re-start did exactly its job.
- **91 publish failures / 45 completions the aggregator never saw** — the
  singleton `WorkflowStats` turn timing out under the completion storm.
- **0 stuck** after a 274 s drain. Slow, but nothing was lost.

### What to do about it (each its own issue)

1. The reminder path is at-most-once by construction: the entry is deleted
   before dispatch, so a dispatch that times out loses the wake. A
   deliver-then-delete (or a redelivery-on-failure) would turn `wakesLost`
   into a retry count.
2. Inbound cross-host calls and the local turn queue share one fate under
   overload; `childDone` from many children into one parent is the hot
   case. Coalescing child completions per parent, or one-way delivery for
   them (#49), takes the join off the critical path.
3. The singleton subscriber on the completion path: batching or a one-way
   publish (#49 again) — `WF_STATS_SAVE_EVERY` is the knob to price it.
4. A 30 s call deadline lets an overloaded host hold a request for 30 s
   before failing it; per-call deadlines (#75) would let the generator fail
   fast and the fleet shed instead of queue.

### The recorded run — after the engine stopped waiting on other hosts (#302, #303, #304)

The hand-run ladder above was followed by two recorded runs that never
produced a number: the 100 runs/s rung wedged the fleet both times — three
idle hosts on 50 000 queued turns — and every later scenario died at its
first call. The mechanism is #302: a turn that awaits a cross-host call
holds its actor's queue AND a pooled connection, and once the pool held
only calls whose targets were waiting on the pool, nothing drained and
nothing timed out. The engine was changed to never await another host
inside a turn (child starts, `childDone`, the completion publish, the
definition read, the watchdog's calls — all detached, with the join
watchdog and the wake protocol as the retry), compute tasks now yield to
the loop every 2 ms, and the harness refuses to start a Job on a
backlogged fleet. This is the first recorded run on that engine, image
`c6d1b15` (the #304 branch), same shape otherwise.

| runs/s offered | finished | stuck | errors | start p50 | order p50 / p99 | etl p50 (8 children) | saga p50 | approval p50 | task p50 | wake lag p50 | transitions/s |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 606 | **0** | 0 | 8 ms | 2.08 s / 2.28 s | 261 ms | 78 ms | 2.35 s | 28 ms | 0 | 53 |
| 25 | 1 469 | **0** | 0 | 43 ms | 2.13 s / 2.60 s | 490 ms | 116 ms | 2.25 s | 35 ms | 1 ms | 171 |
| 50 | 2 998 | **0** | 0 | 120 ms | 6.58 s / 14.3 s | 20.8 s | 188 ms | 35.0 s | 55 ms | 5 ms | 358 |

Over the whole ladder: 8 096 child starts, **0 join repairs, 0 lost
wakes, 0 publish failures, 0 reminder-set failures**; 83% of dispatches
crossed hosts; peak 1 237 activations.

### ✅ 50 runs/s is now a slow rung, not a collapse

The same rung that produced 973 deadline failures, 131 lost wakes and a
274 s drain on the first engine now finishes every run with no errors:
start p50 120 ms (was 26 s), 358 transitions/s sustained, and the fleet
idle within the drain. It is still past the knee — orders take 6.6 s for
2 s of delay and an eight-child etl 20 s — because 50 runs/s of this mix
is ~2.9 vCPU-seconds of sha256 per second on a 3 vCPU fleet before a
single save; but queueing is now a curve, not a cliff. `task_p50_ms`
tells the same story from inside: a 20 ms task measures 28 → 35 → 55 ms
as the loop fills.

### Two rungs that were not what they claimed, and why the harness changed

`def_reads` came out negative over this ladder: the rollout's surge pod
retired mid-run and took its counters with it. A pod set that changes
between the two snapshots now voids the counter delta (#304).

And every scenario after the first in this run seeded its definitions
under version 1 — `WorkflowDefinition.put` is idempotent by version, so
the sleeping-runs rung ran 2 s delays while its Job said 90 s, and every
fan-out width ran width 8. Only the throughput ladder above is valid from
that run; the seed version is now derived from the knob bag, and the
other scenarios were re-run under versions of their own (next section).

### The other six scenarios, re-run under versions of their own (image `c6d1b15`)

Every rung below: **0 stuck, 0 errors, 0 lost wakes, 0 join repairs,
0 publish failures, 0 reminder-set failures.** Same shape, 60 s of
arrivals per rung, one generator pod.

**`workflow/sleeping-runs`** — orders only, a 90 s shipping delay on a
DURABLE reminder, every run leaving memory and coming back on a tick:

| runs/s | finished | start p50 | order p50 / p99 | wake lag p50 | reminders set / fired | peak activations | completed/s over the window |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 25 | 1 530 | 6.5 ms | 90.54 s / 91.09 s | 507 ms | 4 095 / 4 095 (summed over both rungs) | 36 | 9.0 |
| 50 | 2 965 | 15.3 ms | 90.55 s / 91.09 s | 509 ms | (in the 4 095 above) | 36 | 17.5 |

The number to keep: **~4 500 runs asleep at once on three hosts holding
36 activations**, every one of them woken by the shard tick within
half a tick of its due time (the 507 ms is the tick's own quantisation —
the lag floor the shape names), and not one reminder mutation lost its
CAS at 50 arms/s + 50 fires/s. The 16-shard ceiling recorded in the
cluster-scaling section is real, but this rate is well under it; the
rate at which `reminder_set_failure_ratio` leaves zero is the next thing
to find, and `INFRA_WF_SLEEP_RATE_LADDER` is how. (`completed/s` is low
only because the window includes 90 s of sleep — 25/s in, 25/s out.)

**`workflow/fanout-width`** (child RUNS, cross-host) and
**`workflow/fanout-pool`** (the same width as pool tasks in ONE turn),
arrival rate scaled so child work stays ~32 units/s:

| width | children/run | runs/s | etl p50 (runs) | join p50 | task p50 (runs) | etl p50 (pool) | task p50 (pool) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 4 | 4 | 8 | 169 ms | 93 ms | 41 ms | 175 ms | 86 ms |
| 16 | 16 | 2 | 502 ms | 278 ms | 105 ms | 558 ms | 239 ms |
| 64 | 64 | 1 | 3.38 s | 1.81 s | 514 ms | 4.71 s | 1.13 s |

The unit rate is constant, so what grows with width is BURST: 64
children land on the fleet's worker pools at once, and a 20 ms task
measures 514 ms behind its siblings. The pool arm is worse at every
width — 64 × 20 ms of CPU on ONE 1 vCPU host is 1.3 s of wall before
anything else — which is the honest answer to "fan out locally or
across hosts?" on this shape: across, and the durable join costs
93 ms at width 4.

**`workflow/signals`** — approvals at 25 runs/s, the signal sent after
`d` ms (±50%), a fifth of runs deliberately never signalled:

| signal delay | finished | approval p50 / p99 | delivered | buffered | late | timed out (the 20%) |
|---:|---:|---:|---:|---:|---:|---:|
| 500 ms | 1 501 | 604 ms / 15.07 s | 1 184 | 0 | 0 | 317 |
| 2 000 ms | 1 527 | 2.28 s / 15.08 s | 1 220 | 0 | 0 | 307 |
| 10 000 ms | 1 488 | 11.23 s / 15.08 s | 1 190 | 0 | 0 | 298 |

p50 is the signal delay plus ~100 ms; p99 is the 15 s timeout edge, as
designed. No signal arrived late, and none had to be buffered (the wait
node is reached within 30 ms of start) — the buffered path is exercised
by the unit suite, not by this shape.

**`workflow/saga-failure`** — sagas at 25 runs/s:

| failure rate | finished | compensated | failed | task attempts / run | saga p50 / p99 |
|---:|---:|---:|---:|---:|---:|
| 5% | 1 532 | 8.2% | **0** | 3.18 | 63 ms / 1.61 s |
| 20% | 1 490 | 35.1% | **0** | 3.72 | 64 ms / 1.61 s |
| 50% | 1 512 | 74.6% | **0** | 4.50 | 118 ms / 1.61 s |

Compensation never failed to complete; the p99 is the retry backoff
(3 × 500 ms) of a hotel booking that fails twice.

**`workflow/definition-hotkey`** — the default mix at 25 runs/s: start
p50 39 ms, 3 definition reads for 8 587 cache hits across the fleet. The
"hot key" is not hot any more: definitions are immutable per version and
cached per host (#304), so the scenario now prices the first read per
host and nothing else. It stays as the arm that would notice a cache
that stopped working.

### What this rig has found so far, and where each went

| finding | evidence | where |
|---|---|---|
| Awaiting a cross-host call inside a turn can wedge a fleet through the fetch pool, with no deadline to break it | three idle hosts, 50 000 queued turns, 40 min, twice | **#302** (runtime); the engine no longer does it (#303, #304) |
| Reminder firing is at-most-once: a delivered wake whose dispatch times out is gone | 131 lost wakes at the collapsed 50/s rung, recovered by the sweep's touch | **#306** (runtime) |
| A singleton subscriber on the completion path is the first thing to time out under overload | 91 / 2 496 / 2 671 publish failures per host in the wedged runs | #49 (one-way delivery) is the fix; noted there |
| The knee of the default mix on 3 × 1 vCPU is ~25 runs/s, set by sha256 on the loop, not by the runtime | task p50 28 → 35 → 55 ms; `transitions_per_sec` 358 at 50/s | the shape; `WF_TASK_MS` moves it |

## 2026-09-01 · The job lifecycle, priced — 12 round trips per run and a shard rewrite per start (#307)

| | |
|---|---|
| Machine | Dedicated bench VM — AMD EPYC 9V74, 4 vCPU, linux/x64 |
| Node | v24.18.1 |
| Build | `dist/*.prod.js` (`--conditions=production`) |
| Commit | `7c6d4d4` (#308), head side of the PR's own `bench.yml` A/B against `d86b448` |
| Settings | 5 interleaved rounds × 400 ms |
| Conditions | Quiet. **No verdicts called across 537 metrics** on the shared rows, and every `exact` figure below is bit-identical across all five rounds. The new scenarios exist only on the head side, so they have no A/B row — these are their first recorded values, read straight off `rounds.json`. |

Everything the suite measured on the `defineJob` path was a STEP
(`jobs/checkpoint-growth`) or a READ (`jobs/status-read`). Nothing priced a
RUN — `start()` to terminal — which is the unit a workflow engine
multiplies. Two scenarios close that, aimed at the "many runs in flight"
shape rather than the long-run one.

### `jobs/lifecycle` — start() → terminal on a trivial job, fresh key per run

The body does nothing, so a run is the runtime's own bookkeeping. Counted
through `countingStorage` over 25 serial runs, all three `exact`:

| per run | value |
|---|---:|
| `storage_loads_per_run` | **6** |
| `storage_saves_per_run` | **5** |
| `storage_clears_per_run` | **1** |

**Twelve storage round trips for a job that does no work.** Traced: the
activation's state `load` and task-ledger `load`; `start()`'s state CAS,
ledger `load` + CAS and `reminders.set(TASK_REMINDER)` (a shard-table
`load` + CAS); the terminal state CAS; then `#forgetTask`'s ledger `load` +
`clear` and the shard-table `load` + CAS again. On a real store every one
of those is an RTT, and the four ledger operations are redundant for a job
— `defineJob` already persists `status`/`input`/`attempts` in the state
record (#309). The two shard writes are #310.

The timed half, closed-loop, one host, no network:

| arm | c=1 runs/s | c=1 p50 | c=16 runs/s | c=16 p99 | c=64 runs/s | c=64 p99 |
|---|---:|---:|---:|---:|---:|---:|
| `mem` | 16.3 k | 47.5 µs | 18.0 k | 6.54 ms | 14.2 k | 10.5 ms |
| `text` | 20.1 k | 39.0 µs | 19.4 k | 8.09 ms | 16.8 k | 11.6 ms |

- **~16–20 k runs/s per host is the in-process floor** — about 50 µs of
  one JS thread per run, of which the body is nothing. Throughput does not
  rise with concurrency (16.3 k → 18.0 k → 14.2 k): every start serializes
  on the reminder writer chain, and this is one thread. That flatness is the
  shape to expect; the absolute number is what a real store's twelve RTTs
  will divide.
- **`text` beats `mem` by 23%** at c=1, the #265 result again on a
  different workload: the single-walk save path costs less than storing a
  tree by reference. A real adapter is not paying a serialize penalty here.

### `jobs/many-running` — start() with N jobs already running

N parked jobs, then a fresh `start()` probed 40 times (only the second half
timed; each probe cancelled again, outside the timed section, so every
start sees exactly N running — see the scenario for the flush that makes
that deterministic). `shard_entries_per_start` is the size of the reminder
shard table the probe's start REWRITES — a pure function of the key
strings through FNV-1a, so it gates:

| N running | `start_us` | `shard_entries_per_start` (exact) | `shard_bytes_per_start` |
|---:|---:|---:|---:|
| 0 | 63.3 µs | **1** | 89 B |
| 1 000 | 134.8 µs | **64** | 5 754 B |
| 5 000 | **462.9 µs** | **317** | 28 777 B |

- **A start costs 7.3× more with 5 000 jobs running than with none — on
  memory storage, with no network in the path.** The extra ~400 µs is the
  shard table: `reminders.set` loads it, `JSON.stringify`s it twice (the
  no-op compare) and writes it back whole, and the table holds an entry
  for every running job in the CLUSTER ÷ 16. The cost is linear in running
  jobs (1 000 → 5 000 rows: 5.0× the entries, 4.6× the marginal cost).
- **On a real store this is also an O(running-jobs) blob per start and per
  finish**, CAS-contended by every host that shares the shard
  (`MUTATE_ATTEMPTS = 3`, then it throws) and serialized per host through
  one `#chain` — so job starts per second per host is bounded by
  ~1 / (2·RTT) before any of the above. This is the 16-shard ceiling of
  2026-07-28 ("hosts doing reminder work") showing up as a WRITE ceiling,
  and it is what #310 exists to remove. `pgReminders` and
  `surrealReminders` use a due-time table and do not have this shape;
  `redisStorage` deployments do.
- `shard_bytes_per_start` stays informational: every entry carries a
  wall-clock `nextDue`, so the byte count is deterministic only until the
  digit count changes.

### Why these DO carry `exact` metrics

The 2026-08-13 section kept `jobs/*` out of the gate because a count
pinned to "today" would fail on the fix. That was written before the
comparer's `exact` verdict was read closely: it is direction-aware — a
LOWER count on a `direction: 'lower'` metric reads `improved`, not
`regressed` — so #309 and #310 will each move a row here and pass. What
the gate refuses is the other direction: a change that adds a round trip
to a job run, which is exactly the class of regression a shared runner
can see and a timing table cannot. Both scenarios are in
`BENCH_GATE_SCENARIOS`; `benchmarks/__tests__` asserts the list and the
flags agree.

One runtime observation from writing the exact pass, filed as #313:
`host.stop()` does not wait for a completed run's detached ledger and
reminder clears, because `#release` removes the run from the task table
before `#forgetTask` runs. The scenario flushes one macrotask before
stopping for that reason; the totals came up exactly one clear short
without it.

### Since fixed (#309) — the ledger lives in the state record

| | |
|---|---|
| Compared | `5bb4167` (main) → `f0e85d7` (#314), `bench.yml`, **7 interleaved rounds** (re-run of the PR's own 5-round A/B, which called the same rows) |
| Conditions | Quiet — no `noisy` verdicts on the rows below; every `exact` row bit-identical across rounds |

`defineJob` derives its task ledger from its own state (`status`, `input`,
`attempts`) instead of keeping a `$sigx:tasks` record. All three exact rows
moved and read `improved`, as the section above said they would:

| `jobs/lifecycle` | before | after |
|---|---:|---:|
| `storage_loads_per_run` | 6 | **3** |
| `storage_saves_per_run` | 5 | **4** |
| `storage_clears_per_run` | 1 | **0** |
| `mem/c=1` runs/s · p50 | 16.3 k · 47.3 µs | **21.4 k · 35.1 µs** (+24–31%, 7/7) |
| `text/c=1` runs/s · p50 | 20.1 k · 39.4 µs | **24.6 k · 31.5 µs** (+20–23%, 7/7) |
| `text/c=16` runs/s | 19.4 k | **23.5 k** (+21%, 7/7) |

Twelve round trips per run became **seven**; the four that went were the
ledger's (its load on activation, load + CAS on start, load + clear on
finish). The five that remain are the state load, the state CAS on start
and on finish, and the two reminder-shard rewrites — #310's half.
`jobs/many-running` `n=0/start_us` 63.6 → 56.3 µs (+10.6%, 7/7) for the
same reason; the shard rewrite itself is untouched, so its ladder is not.

**One thing this run found that it cannot explain, recorded so it is not
rediscovered.** Two `text*` HEAD windows read `regressed`, unanimously, in
both the 5-round and the 7-round A/B:

| row | before | after | 7-round range |
|---|---:|---:|---|
| `jobs/checkpoint-growth` `watch=0,text/head_step_us` | 27.97 µs | 33.00 µs | −19.5% … −13.6% |
| `state/save-growth` `text-scalar/head_turn_us` | 21.43 µs | 24.37 µs | −15.5% … −10.1% |

(`state/save-growth` `text/head_turn_us` moved the same way, −19.8%, but
`noisy`.) What bounds it: every TAIL of those arms is inside the 3% floor
(`text-scalar/tail` 84.4 → 85.5 µs, `watch=0,text/tail` 109.2 → 109.2 µs);
the `mem` and `stringify` heads of the SAME scenarios did not move; GC
counts are bit-identical on both sides (24/24, 18/18, 16/16 collections);
and `state/save-growth` is a plain `defineActor` that never executes a line
this change touched. So it is ~3 µs per small-state `saveText` turn, on
arms this PR does not reach, in the warm-up window only. The hypothesis
that fits all of that: `taskLedger.mutate` was a hot caller of the same
`stringifyWithHandlers` the `saveText` path uses (`host/tasks.ts` `toJson`),
and jobs no longer call it — so between arms that function's optimization
state is different, and the `text` heads now pay warm-up they used to
inherit from the ledger traffic. That is a property of the benchmark
PROCESS, not a per-save cost; a per-save cost would have moved the tails by
the same ~3 µs and it did not. It is a hypothesis: settle it with a
per-turn head profile of `state/save-growth text-scalar` on both sides, or
by running that scenario ALONE (no `jobs/*` in the process) on both, before
anyone optimizes against it.

### Since fixed (#310) — task liveness is a per-host roster

| | |
|---|---|
| Compared | `a313fad` (main, with #309) → `28f502c` (#315), the PR's `bench.yml` A/B, 5 interleaved rounds |
| Conditions | Quiet — no `regressed` verdict anywhere in the table; the `checkpoint-growth` and `save-growth` rows all read `no change` this time, head windows included |

The `TASK_REMINDER` per running task is gone: a host keeps ONE roster
(`$sigx:tasks-roster/{hostId}/p0..p15` + a `$hosts` index) that only it
writes, so a start or finish is one CAS with no load, and a survivor adopts
a dead host's roster on the reminder tick. `reminderTaskLiveness()` keeps
the old mechanism where an alarm is the wake-up (Durable Objects).

| `jobs/lifecycle` | before | after |
|---|---:|---:|
| `storage_loads_per_run` | 3 | **1** |
| `storage_saves_per_run` | 4 | 4 (two state CAS, two roster CAS) |
| `storage_clears_per_run` | 0 | 0 |
| `mem/c=1` runs/s · p50 | 21.0 k · 35.2 µs | **28.8 k · 24.9 µs** (+36%, 5/5) |
| `mem/c=16` runs/s · p50 | 22.7 k · 620 µs | **36.7 k · 366 µs** (+61%, 5/5) |
| `text/c=1` runs/s | 24.7 k | **28.2 k** (+15%, 5/5) |
| `text/c=64` runs/s · p50 | 19.6 k · 2.77 ms | **31.8 k · 1.57 ms** (+63%, 5/5) |

| `jobs/many-running` `start_us` | before | after |
|---|---:|---:|
| n=0 | 55.9 µs | **45.2 µs** |
| n=1 000 | 127.7 µs | **33.1 µs** (−74%) |
| n=5 000 | 450.9 µs | **26.1 µs** (−94%, 5/5 at −93.9 … −94.3%) |

**Read the entry counts before the times.** `roster_entries_per_start`
is **1 → 64 → 317** — the SAME counts the reminder shard had, because this
is a one-host bench: every parked job is on the probe's own host, so its
roster sub-shard holds exactly what the cluster shard held. Bytes halved
(89 → 46, 5 754 → 3 002, 28 777 → 15 146 B — an entry is `"id": since`
rather than `{ name: { nextDue, period } }`), and the time still fell
17×, so the bytes were never the cost. What went: the shard LOAD before
every write, the two `JSON.stringify` passes of the no-op compare, and
the reload-and-retry a contended CAS can take — the roster is a cached
table the host alone writes, so a start is one stringify and one CAS.
What this rig cannot show is the division by hosts: in an H-host cluster
each roster holds 1/H of the running jobs, where the shard held all of
them. The gain at c=16 and c=64 over c=1 is the group commit: every start
that lands while a roster write is in flight rides the next write, so a
busy host pays one CAS per BATCH of starts rather than one per start.

Together with #309, a job run went from **12 storage round trips
(6 / 5 / 1)** on 2026-09-01 morning to **5 (1 / 4 / 0)** by the afternoon,
and the term that grew with the number of running jobs in the CLUSTER now
grows with the number on one host, from memory, uncontended. What remains
per run is the state load, two state CAS and two roster CAS — the roster
pair is the next thing to fold, if it ever matters (a `{ status:
'running' }` marker in the state CAS itself would do it, at the cost of a
scan on adoption), and the per-host roster's own O(n/16) write is the
one after that (a per-host bloom or an append log would make it O(1)).

Also: `jobs/lifecycle` `gc/collections` and `gc/pause_ms` read higher on
the head side (99 → 126, 1.04 → 1.28 s). Those scenarios are
duration-bounded, so ~1.4× the throughput is ~1.4× the runs and ~1.4× the
allocations in the same 400 ms — the same reading the 2026-08-06 section
gave the `state/dirty-size` counts.

## 2026-09-02 · Tier 3 — the workflow engine on the 5-round-trip runtime (#329, the Go host PoC bar)

| | |
|---|---|
| Shape | `wf replicas=3 nodes=3 image=00a9da4 knobs=FETCH_CONNECTIONS=64,TRANSPORT=http` |
| Cluster | AKS, 3 × 1 vCPU host pods on 3 nodes, one Redis, HTTP host-to-host |
| Driver | in-cluster `wf-bench` via `cluster-test.yml` dispatch, run **33621360065** (`workflow.json` artifact) |
| Why | The first recorded Tier-3 run on the runtime with #309 (state-record ledger), #310 (roster liveness) and #311 (auto-pipelined `url`-constructed clients) — the comparison bar for the planned Go host PoC. The 2026-08-26 figures were image `fccf287`/`c6d1b15`, twelve storage round trips per run ago. |
| Caveat | `main` moved past the measured image the same day (#322–#325, including two runtime fixes: #323 job revert-on-failed-start, #324 deadlines on timer/task turns). Neither touches a hot path measured here, but the shape pins `image=00a9da4` and a comparison against a later image is a different run. |
| The A/B that is NOT here | #311's before/after is still parked: the old-image leg cannot deploy through the dispatch workflow — the Azure federated credential matches the OIDC subject *including the ref*, so only `main` can log in (AADSTS700213; noted on #311 with two unblock options). |

**Zero anomalies anywhere in the suite**: every rung of every scenario read
`stuck_ratio 0`, `wakes_lost 0`, `reminder_set_failure_ratio 0`,
`completed_unreported 0`; the only non-zero error rate in the whole run is
0.2% at the 50 runs/s rung.

### `workflow/throughput-ladder` — the default mix

| offered runs/s | completed/s | transitions/s | start p50 | task p50 | order p50 | etl p50 (8 children) | approval p50 | saga p50 | wake lag p50 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 4.6 | 36.9 | 9.2 ms | 28 ms | 2.08 s | 260 ms | 2.10 s | 68 ms | 0 ms |
| 25 | 12.5 | 138.6 | 63.4 ms | 36 ms | 2.16 s | 668 ms | 2.40 s | 142 ms | 1 ms |
| 50 | 18.9 | 263.9 | 178.3 ms | 58 ms | 6.43 s | 34.2 s | 50.7 s | 262 ms | 5 ms |

The same shape as 2026-08-26 (`c6d1b15`): the knee is compute on the loop
(task p50 28 → 36 → 58 ms is a 20 ms sha256 task measuring loop occupancy,
not hash speed — the hash is native `node:crypto`), queueing is a curve
past it, and nothing breaks at 2× the knee. Not directly comparable rung
for rung — the recorded scenario runs 60 s arrivals per rung where the
2026-08-26 hand ladder used different drain accounting — but the
transitions/s at r=50 (263.9 vs 358 over that run's longer window) and the
error-free drain say the same thing: **the ceiling of this shape is
1 vCPU × 3 of native sha256 plus one JS loop per host, not the runtime's
bookkeeping** — which is now 5 round trips per run instead of 12.

### The rest of the axes, one line each

- **`sleeping-runs`** (90 s durable delay, every run leaving memory):
  15.5 runs/s completed at r=50 with start p50 **19 ms**, order p50 103.5 s
  (90 s of that is the sleep), wake lag p50 ~15 s — the shard tick's
  quantisation at the default 30 s `reminderTickMs` (the 2026-08-26 run
  set `WF_REMINDER_TICK_MS=1000`; this one records the DEFAULT, which is
  the honest bar for a stock deployment). Zero reminder CAS failures at
  50 arms/s + 50 fires/s on the roster-era runtime.
- **`fanout-width` vs `fanout-pool`**: child runs across hosts beat the
  local pool at every width again (w=64: task p50 611 ms vs 1 324 ms,
  etl 4.0 s vs 5.6 s) — burst compute wants the fleet, and the durable
  join costs 122 ms at w=4.
- **`signals`**: approval p50 = signal delay + ~90 ms at every d
  (593 ms / 2.29 s / 11.5 s for d=500/2000/10000), `signal_late_ratio` 0,
  nothing buffered.
- **`saga-failure`**: compensation never failed to complete; completed/s
  falls with the injected failure rate (21.6 → 14.6 → 6.2 at f=.05/.2/.5)
  while transitions/s HOLDS (~98–111) — retries are work, and the engine
  does it instead of erroring.
- **`definition-hotkey`**: 13.9 runs/s at r=25 with the definition cache
  doing its job (the scenario exists to notice if it stops).

### For the Go host PoC, the bar this run sets

A Go host on this exact shape must: hold **≥ 18.9 completed runs/s /
≥ 264 transitions/s** at the 50 runs/s rung with zero stuck runs and zero
lost wakes; wake durable sleeps within a tick; and reproduce the exact
invariants (directory ops, roster/reminder CAS discipline) this suite
holds at zero. Where it should win, per the measured shape: the task p50
inflation (28 → 58 ms of loop occupancy for a 20 ms task) and the
fan-out burst columns — cores per process — plus whatever the wire rows
below concede to Node's HTTP stack.

### The HTTP curves are NOT here, and why

The `bench` verb (the `infra/*` read/write ladders from the same-region
VM) failed before measuring anything: `loadVmUp()` got
`AuthorizationFailed` — the workflow's OIDC identity has no rights on the
load-VM resource group (the RUNBOOK §1c role assignment, "Contributor
scoped to the load-VM resource group, pre-created once out of band", is
not in place for it). Estate-side, not repo-side. Once the role is
granted, `cluster-test.yml` → `bench` on the same shape completes this
section; until then the 2026-08-02/2026-08-06 curves (image `02dded3` /
`04e6598`, twelve-round-trip runtime) remain the newest HTTP figures and
must not be quoted as current.

## 2026-09-04 · The sharded reminder table against a real Redis — the CAS holds, the record size does not (#382)

| | |
|---|---|
| Machine | Apple M4, 10 cores, macOS; Redis 7 on loopback (`redis-server`, no persistence) |
| Node | v24.11.1 |
| Build | `dist/*.prod.js` (`--conditions=production`) |
| Settings | `reminders-redis/*`, 2 rounds, `--no-warmup`; 15 s of arrivals per rung |
| Conditions | Quiet — the calibration probe read 1.93 M / 1.82 M (`table-size`) and 1.92 M / 1.91 M (`arm-fire`) across rounds, under 6 %. An earlier pass on the same day, taken while four sibling worktrees were building, read 1.62 M / 0.55 M and its set latencies were up to 12× these; that pass is not recorded, and is why the timings here come with the probe. |
| Why | The L2 rung of the scaling roadmap (`docs/architecture/scaling-roadmap.md`): the 2026-08-26 section measured `reminder_set_failure_ratio` at zero for 50 arms/s + 50 fires/s and named "the rate at which it leaves zero" as the next thing to find. This is that number on loopback — the UPPER bound for any cloud figure, since a longer round trip widens the load-to-save window in which another writer can land. |

The shape: N in-process hosts (`createCluster`, membership and directory in
memory, `selfPolicy`) over ONE `redisStorage`, `reminderTickMs` 1 s, and an
OPEN-LOOP arm ladder — R fresh actor keys per second each arming a one-shot
reminder due 2 s out, round-robined over the hosts, so N hosts is N writer
chains against the same 16 shard records. Open-loop on purpose: a slow `set`
must not throttle the arrivals, or the ceiling hides behind its backlog. A
failure is the branded `ActorStorageConflict` after the runtime's three
attempts and nothing else — any other rejection fails the scenario. The
expected due time is stamped when the dispatch resolves, the closest stamp
for the set the runtime recorded.

### `reminders-redis/arm-fire` — the CAS ceiling is above 1 000 arms/s at 16 writers

| N hosts | first rung with a failure | `set_failure_ratio` there (per round) | set p50 / p99 at r=1000 | fired within a tick at r=1000 | shard bytes at r=1000 |
|---:|---|---|---|---:|---:|
| 1 | none through 1 000/s | 0 | 2.8 ms / 8.4 ms | 1.00 | 196 KiB |
| 3 | none through 1 000/s | 0 | 2.6 ms / 17 ms | 0.99 | 196 KiB |
| 8 | none through 1 000/s | 0 | 3.4 ms / 24 ms | 0.99 | 196 KiB |
| 16 | r=500 (one round of two) | 0.00013 / 0 at 500; **0.00027 / 0.00053** at 1 000 | 4.6 ms / 25 ms | 0.98 | 200 KiB |

Every rung at N ≤ 8 completed its 15 000 arms with zero CAS failures. At
sixteen writers the third attempt loses two to eight arms in fifteen
thousand at 500–1 000 arms/s — under the gate's 0.001 floor, so the gate
never trips on this shape — and the tick keeps up: `fired_within_tick_ratio`
never drops below 0.98. Commands per arm settle at ~9 (one `HGET` and
one script per set, the tick's loads, the fire's delete); Redis CPU falls
from ~290 ms per thousand arms at 50/s to ~90 ms at 1 000/s as the
per-tick scans amortise. **On this shape the CAS rate is not the ceiling
below 1 000 arms/s** — an auto-pipelined loopback round trip is too short
for three attempts to lose. And the records never pass 200 KiB, because
everything armed here fires two seconds later.

### `reminders-redis/table-size` — the O(table) term, priced

That last clause is the caveat, and the second arm exists for it. A
million SLEEPING runs is a million entries the records carry through every
set and every tick. Three hosts, 200 arms/s, P far-future entries seeded
into the records first:

| P asleep | record bytes (16 shards) | set p50 | set p99 | fired within a tick | Redis CPU per 1 000 arms | `set_failure_ratio` |
|---:|---:|---:|---:|---:|---:|---:|
| 0 | 43 KiB | 1.3 ms | 7.9 ms | 1.00 | 141 ms | 0 |
| 10 000 | 772 KiB | 4.5 ms | 15 ms | 0.99 | 385 ms | 0 |
| **100 000** | **6.25 MiB** | **870 ms** | **2.85 s** | **0.27** | **1 156 ms** | 0 |

At 100 000 sleeping entries — 400 KiB per shard record — a `set` takes
most of a second at the median and nearly three at p99, and **only a
quarter of the reminders fire within a tick of their due time**: each
tick loads, scans and rewrites 400 KiB per owned shard, each set does the
same, and all of it serialises on one writer chain per host. Redis spends
1.2 CPU-seconds per thousand arms, eight times the empty-table figure, on
`HGET`/`HSET` of the same bytes. The CAS never failed. The sharded design
does not fail at this size — it slows by the size of the table it
carries: 10 000 entries cost 3.5× the empty-table set, 100 000 cost
670×, with the knee between them where a record stops fitting in one
round trip's worth of time.

### What this decides for #385

1. **The reminder ceiling is a table-size ceiling, not a CAS-rate
   ceiling.** Below ~10 000 sleeping entries cluster-wide the sharded
   provider is fine at any arm rate this rig can produce. The
   `1M sleeping` target is an order of magnitude past the rung that
   already misses three ticks in four. `redisReminders()` (#385) — arm
   O(log N), tick O(due), a due-time ZSET rather than sixteen JSON blobs —
   is justified on this number alone, before any Tier-3 minute is spent
   on the sleep-rate ladder; the ladder (#391, T2) now exists to confirm
   the knee with real RTT, not to find it.
2. **The "outgrown `shardedReminders()`" gauge (#384's S item) has a
   threshold**: warn when a shard record passes ~1 000 entries (~64 KiB),
   which is where the per-set cost has already tripled.
3. **What this rig cannot say**: the cloud figure. Loopback round trips
   are ~20× shorter than in-cluster, so the CAS window at N=16 — a few
   arms in ten thousand at 1 000 arms/s here — should be read as "the
   retry budget is thin", not as a rate.

> Reproduce with `REDIS_URL=… pnpm bench:run reminders-redis/ --runs=2 --no-warmup`.
> The `table-size` arm seeds its population straight through the storage
> as the runtime writes its own table (`saveText`, round-robin over the
> shards), so a run costs seconds, not the hours a million real arms would.
