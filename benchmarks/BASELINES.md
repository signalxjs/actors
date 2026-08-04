# Baselines

Reference figures, recorded by hand. `benchmarks/baselines/local.json` is your
machine's working baseline and is gitignored — absolute numbers from someone
else's laptop are not comparable. This file exists so the **relationships**
between layers are reviewable in a PR, since those hold across machines even
when the absolute figures do not.

Update it deliberately, when a change moves something here, and always record
the machine.

> **Renamed since these were recorded (#284):** `Mailbox` is now
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
real `hardwareConcurrency`, is ever used), `cold-placement` (fresh keys, so
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

Recorded with `node examples/aks-cluster/deploy/testenv.mjs baseline`;
compared by `… testenv.mjs test`. Absolute capacity is a different
question from regression detection — for that, `testenv.mjs load` runs the
generator directly with as many clients as you care to point at it.

Figures here come from two different kinds of measurement, and confusing them
is how a modelled number ends up cited as a fact (which is what #87 is fixing
for the Redis figures). The scenario name says which tier it is:

| Tier | Scenarios | What it is |
|---|---|---|
| **Tier 1** | `dispatch/`, `state/`, `wire/`, `lifecycle/`, `memory/`, `cluster/` | One process, `pipeFetch`, **zero sockets**. Measures algorithmic shape and software cost. Anything about the network here is *modelled*, not measured. |
| **Tier 2** | `cluster2/` | N hosts as **real OS processes over real loopback sockets**. Measures what the wire actually does. Opt-in: `pnpm bench:tier2`. |

Within Tier 2 there is a second split, and it is enforced in code rather than
by this paragraph — every timing metric is emitted `informational: true`, so
the comparer *cannot* gate on one:

- **Counted, and trustworthy on a busy machine**: sockets, bytes per call,
  requests per connection. These are counts of events; CPU contention does not
  change how many connections a transport opens.
- **Timed, and contended**: throughput, percentiles, RSS. N processes share
  the cores. Recorded for context; never evidence on its own.

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
  production call pays it. **Fixed since (#230):** far deadlines now share
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

The edge hashes the routing token now in the request line (#132); the third
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
  count instead.
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

Two things are true at once, and #89 called both:

- **Keep-alive works.** 69–1 163 requests per connection; connections are
  reused heavily rather than opened per request.
- **The pool still sizes to concurrency**, at *twice* the rate #89 projected.
  It predicted one connection per in-flight request; the measurement is a
  flat **2.00** at every concurrency tested.

Extrapolated the way #89 does it — c=64 against 99 peers — that is **~12 600
sockets per host**, not 6 300. The extrapolation is still a model (this rig
has 2 hosts, not 100), but the per-peer coefficient it rests on is now
measured rather than assumed.

*Why 2× and not 1× is not yet explained.* It reproduces exactly across
concurrency and is confirmed independently by libuv's own TCP handle table on
both ends, so it is a real property of the current client and not a counting
artifact. Establishing the cause belongs with the `undici.Agent` work in #89.

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
session tokens (#90 step D) and 103 bytes/call on the wire — but it is not the
3× headline, and it is not on its own an argument for a new transport.

### Throughput plateaus at the wire, not at the runtime [timed, contended]

18–19 k ops/s at c=8 and above, against the ~17 k/s a bare Node HTTP echo
server reaches on this machine. The runtime is not the ceiling here; Node's
HTTP stack is.

### Bounding the pool fixes it; HTTP/2 does not [counted + contended]

#89's three candidates, at concurrency 64 against one peer, all driven through
the existing `fetch` seam so no runtime change was needed to test them:

Measured with **undici 7.29**, which is the major Node currently bundles for
the global `fetch` (`process.versions.undici` = 7.16 here). That choice
matters — see the version note below.

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

Extrapolated the way #89 does it — c=64 across 99 peers — matching the cap to
concurrency gives **~6 300 sockets per host instead of ~12 600**, at no cost.
Getting below that is a deliberate throughput trade.

> ⚠️ **These numbers are undici-major-specific.** An earlier pass of this
> table was recorded against undici 8.x and showed `connections: 8` costing
> only ~2% — a conclusion that does not hold on the major Node actually
> ships, and which would have shipped a bad default had it not been re-run.
> Re-measure before tuning against a different major.

**Candidate 1 is not reachable.** `allowH2: true` measures identical to plain
keep-alive at every pool size (5 844 vs 5 862 at one connection; 13 254 vs
13 548 at eight). `createAppHandler` serves over `node:http`, which is
HTTP/1.1 only, so the client negotiates nothing and silently falls back.
Multiplexing would require a `node:http2` server first — a much larger change
that buys the same socket reduction the pool cap already gives.

Candidate 3 — documenting the escape hatch — is therefore the whole shipped
change, now with a measured recommendation attached. See the README.

### Session tokens: measured, and declined

The plan carried an idea to replace the per-call HMAC with a short-lived
session token, on the strength of the Tier-1 3.35×. Over a real socket the
gap is **1.19× at c=64**, and a session token authorises *any* call for its
window where today's signature is bound to a specific symbol + callId.

**A 19% gain does not justify weakening that binding**, so this is declined
rather than deferred. Revisit only if a profile shows signing dominating on a
deployment where the network is not the cost — which is the opposite of what
the numbers above show.

### The transport decision [#105]

All three on one rig, back to back, against the **tuned** HTTP baseline from
#98 (pool bounded to the concurrency) rather than the shipped default —
comparing a new transport to an untuned incumbent would flatter it. Two hosts,
concurrency 64.

| | TCP handles | ops/s | p99 | bytes/call |
|---|---:|---:|---:|---:|
| tuned HTTP | 65 (1 listener + 64 conns) | 14 287 | 9.6 ms | 640 |
| **TCP** | **3** (2 listeners + 1 conn) | **69 768** | **1.58 ms** | not observable |
| **WebSocket** | **2** (1 listener + 1 conn) | **63 495** | **1.58 ms** | **236** |

Against the gate written down in #95 *before any of these transports existed*:

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
count", on the strength of the HMAC measurement in #96. **That was wrong, and
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
must stay zero-dep and WinterCG-clean**, which is the whole reason RFC #20 put
the host wire on HTTP: a default requiring `node:net` would break Cloudflare
Workers outright, and HTTP is the only transport that runs everywhere.

So the recommendation is stronger than "reach for TCP when file descriptors
hurt", which is what the packages currently say:

- **On Node, prefer `@sigx/actors-tcp`** (or `@sigx/actors-ws` when one port,
  proxy traversal or a WinterCG client matters). It clears every measured
  criterion, most of them by a wide margin.
- **HTTP remains the default and the only portable option**, and with a bounded
  pool (#98) it is a perfectly reasonable one.

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
   **Fixed (#230):** `CallDeadlines` (`host/deadlines.ts`) gives far
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
8. **FIXED (#218): every `JSON.parse` on the wire passed a reviver**
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

| | |
|---|---|
| Cluster | AKS 1.34, `Standard_D2ls_v6` (2 vCPU, **1900m allocatable**) |
| Hosts | one per node, `spread.whenUnsatisfiable=DoNotSchedule`, `limits.cpu 1000m` |
| App | `examples/chat`, redisStorage + redisCluster, one Redis pod |
| Driver | `Standard_D4s_v5` in-region, `edge-ladder.mjs`, Node v22.22.0 |
| Settings | 3 runs × 20 000 ms per rung |
| Shapes | `replicas=3 nodes=3` and `replicas=7 nodes=7`, image `02dded3`, knobs identical (`PLACEMENT=prefer-local, MAX_ACTIVATIONS=0, SWEEP_INTERVAL_MS=60000, MIGRATE_PERSIST=lazy, DIGEST_ITERS=2000`) |

> ⚠️ **The edge hash was OFF for every number here.** ingress-nginx ≥ 1.12
> silently drops `upstream-hash-by` under its default
> `--annotations-risk-level=High`, so the routing token bought nothing and
> every actor call paid ~0.86 cross-host hops. **These figures are a floor,
> not a ceiling** — locality was never engaged. See the `edge hash actually
> pins a token` assertion, which now fails loudly instead of leaving this to
> be inferred from a `token_speedup` of ~1.0.

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
| `worker_speedup` | 3.028 | 6.514 |

`token_speedup` ≈ 1.0 at both sizes is the disabled edge hash, not a
statement about locality. **`worker_speedup` does not mean what its name
says** — it tracks host count (3.03≈3, 6.51≈7) because a worker is always
placed locally while a stateful actor is pinned to one host. Per host the
pool did 289 and 266 ops/s against the single activation's 289. See #264.

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

### Also observed

A host whose event loop pauses past the 15 s membership TTL is evicted, has
its directory claims released by `evictHost`, and **keeps serving with its
activations live** — it never rejoined in 77 s, never fenced
(`selfFences=0`), stayed `Ready`. Single activation is not preserved across
that window. Measured, not inferred; see #263.
