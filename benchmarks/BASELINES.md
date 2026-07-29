# Baselines

Reference figures, recorded by hand. `benchmarks/baselines/local.json` is your
machine's working baseline and is gitignored — absolute numbers from someone
else's laptop are not comparable. This file exists so the **relationships**
between layers are reviewable in a PR, since those hold across machines even
when the absolute figures do not.

Update it deliberately, when a change moves something here, and always record
the machine.

## Tiers — read this before quoting a number

Figures here come from two different kinds of measurement, and confusing them
is how a modelled number ends up cited as a fact (which is what #87 is fixing
for the Redis figures). The scenario name says which tier it is:

| Tier | Scenarios | What it is |
|---|---|---|
| **Tier 1** | `dispatch/`, `state/`, `wire/`, `lifecycle/`, `memory/`, `cluster/` | One process, `pipeFetch`, **zero sockets**. Measures algorithmic shape and software cost. Anything about the network here is *modelled*, not measured. |
| **Tier 2** | `cluster2/` | N silos as **real OS processes over real loopback sockets**. Measures what the wire actually does. Opt-in: `pnpm bench:tier2`. |

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
| `dispatch/warm-grain` | 1.95 M | **−74%** — placement, reentrancy check, directory lookup, turn bookkeeping |
| `dispatch/warm-grain-deadline` | 1.21 M | **−38%** — `raceDeadline`, i.e. the *default* `callTimeoutMs: 30_000` |
| `dispatch/via-proxy` | 1.07 M | **−12%** — client proxy + `mintCallId()` |
| `wire/endpoint-roundtrip` | 115 k | **−89%** — wire codec, JSON, endpoint (no socket) |

Two findings stand out:

- **The default call deadline costs ~38% of dispatch throughput.** Every
  dispatch with a non-zero `callTimeoutMs` allocates a promise and a
  `setTimeout` in `raceDeadline`. The default is 30 s, so essentially every
  production call pays it.
- **A turn through the silo costs ~4× a bare mailbox turn.** The mailbox itself
  (a promise chain, ~4 promises per turn) is not the dominant cost at this
  layer; what sits on top of it is.

### Queueing (single grain vs many)

| Scenario | c=1 | c=64 | c=512 | c=64 p50 |
|---|---:|---:|---:|---:|
| `dispatch/warm-grain` (one grain) | 1.95 M | 1.86 M | 1.56 M | 36.5 µs |
| `dispatch/fan-out-grains` (1 000 grains) | 1.65 M | 1.66 M | 1.55 M | 40.1 µs |

Throughput is flat in concurrency while p50 grows linearly with queue depth —
the mailbox serializing turns, as designed. Fan-out does not currently beat the
single grain, because this is one process: the runtime is not the bottleneck,
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
  costing after that (16 subscribers → another 4.5×). `#snapshot()` is
  `revive(encode(raw))` — two full deep walks per mutating turn — plus per
  subscriber delivery.

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
grains cost about a millisecond per 60 s tick, which is nothing. Guards are
essentially free per request.

### Memory

| Metric | Value |
|---|---:|
| `mem/per-grain-footprint` tiny | **4.1 KiB per grain** |
| `mem/per-grain-footprint` large (200 rows) | 46.3 KiB per grain |
| `mem/leak-activate-deactivate` | 3 B retained per grain per cycle |
| `mem/leak-streams` | 0 B retained per stream |
| `mem/leak-timers` | 3 B retained per timer |
| `mem/soak-steady-state` | 486 B/sample slope (≈ 0) |

**A live grain costs ~4 KiB**, so a gigabyte of heap holds roughly 250 k idle
activations of a trivial actor. No leaks detected on any path: activate/
deactivate, change-feed streams, and volatile timers all return their memory,
and a mixed steady-state workload holds a flat heap.

The detector was verified against a planted leak (an actor retaining its context
on activation): **626 B/grain** vs **3 B/grain** clean.

---

## 2026-07-28 · cluster scaling, N = 1 … 100

In-process (`benchmarks/src/cluster-harness.ts`), one CPU shared by every
silo. **Absolute throughput here is meaningless** — 100 silos contending for
one core. What is exact is the algorithmic shape: provider calls and
per-decision cost as a function of N. Node v24.11.1, Apple M4, prod dist.

| property | N=1 | N=100 | verdict |
|---|---:|---:|---|
| directory calls per cold activation | 2.00 | **2.00** | ✅ flat |
| membership `view()` per activation | 1.00 | **1.00** | ✅ flat |
| notifications per membership change | 1 | **100** | ❌ O(N) |
| silos doing reminder work | 1 | **16** (84 idle) | ❌ hard ceiling |
| locality, random policy | 1.00 | **0.01** | ⚠️ 1/N, structural |
| locality, consistent-hash | 1.00 | 0.00 | ⚠️ same |
| `choose()` random | 14.8 M/s | 3.7 M/s | ⚠️ 4× slower |
| `choose()` consistent-hash | 3.9 M/s | **61.8 k/s** | ❌ 63× slower |

### ✅ The most important result is a positive one

**Activation cost does not grow with cluster size.** Two directory calls
(one `lookup`, one `claim`) and one cached `view()` per cold activation,
identical at N=1 and N=100. The directory is a shared service, but the
runtime's demand on it per activation is O(1) — so adding silos adds
capacity rather than adding load per unit of work. That is the property
scaling depends on, and it holds.

### ❌ Membership change is O(N²) against a remote provider

One silo joining an N-silo cluster produces **exactly N notifications** —
measured, not modelled. That much is inherent: every silo must learn.

The cost is in what a notification *becomes*. Measured against **Redis
8.8.1** (`CONFIG RESETSTAT`, add one silo, `INFO commandstats`, with
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

A rolling restart of 100 silos is ~200 membership changes — on the order of
**2 M Redis commands**, in bursts.

> Reproduce with `REDIS_URL=... pnpm bench:run cluster/redis-amplification`.
> This figure was previously *modelled* from the provider source and was
> wrong twice — 10 100, then 10 200 — because the model missed that the
> joining silo also refreshes (`n + 1` refreshers, not `n`) and that each
> refresh reads the version key as well as the member set (2 fixed
> commands, not 1). The `+ 13` is the joiner's own write path plus the
> `CLIENT SETINFO` handshake ioredis performs on each new connection. It is
> measured now.

Cheapest fix is a debounce/coalesce on the subscriber path: N changes
arriving together should cost one refresh, not N.

### ❌ Reminders stop scaling at 16 silos

`REMINDER_SHARD_COUNT = 16`, pinned as storage identity ("never change
either"), with rendezvous ownership. Measured silos owning ≥1 shard:

| N | 1 | 2 | 10 | 50 | 100 |
|---|---:|---:|---:|---:|---:|
| silos with reminder work | 1 | 2 | 9 | 13 | **16** |
| idle silos | 0 | 0 | 1 | 37 | **84** |

Note N=50 gives 13, not 16 — rendezvous collisions leave some shards
doubled up before the ceiling is even reached. Reminder throughput is capped
at 16 silos' worth regardless of cluster size, and the shard count cannot be
raised without an explicit storage migration.

### ⚠️ Locality is 1/N, by design

Nothing routes a caller toward the owner of a grain, so a request landing on
an arbitrary silo finds the owner ~1/N of the time — measured 1.00, 0.54,
0.10, 0.02, 0.01 for N = 1, 2, 10, 50, 100. Neither shipped policy improves
on that, because neither considers the caller; the small differences between
them in the table are sampling noise around the same ~1/N curve.

This is Orleans-normal and not a bug, but it fixes the performance model:
**at any real cluster size essentially every call takes a network hop**, so
cluster throughput is network-bound, and the shared state above is what
decides whether it scales.

### Per-call cluster costs (N=2, so the numbers mean something)

| | ops/s | p99 |
|---|---:|---:|
| locally-owned dispatch | 1.40 M | 1.4 µs |
| cross-silo, HMAC on | 20.3 k | 95.6 µs |
| cross-silo, HMAC off | 68.0 k | 21.5 µs |

Cross-silo costs ~69× a local call **in software alone** — no real network
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
| Rig | `cluster2/*` — `child_process.fork` per silo, loopback TCP, store in the parent over IPC |
| Settings | 5 rounds × 400 ms, interleaved |
| Conditions | **Contended** — the probe varied 15%, so the suite printed `THE MACHINE WAS BUSY`. The *counts* below were nevertheless identical every round; that is the point of the tier split. |

### The connection pool sizes to concurrency, at 2× [counted]

Two silos, one driving, every call crossing to the owner:

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
sockets per silo**, not 6 300. The extrapolation is still a model (this rig
has 2 silos, not 100), but the per-peer coefficient it rests on is now
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
concurrency gives **~6 300 sockets per silo instead of ~12 600**, at no cost.
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
comparing a new transport to an untuned incumbent would flatter it. Two silos,
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
the silo wire on HTTP: a default requiring `node:net` would break Cloudflare
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
  measured; the 100-silo total remains *modelled*.

The cheap thing that would fix most of this: **two machines at N=2**, giving a
real NIC and a real RTT.

---

## Things worth investigating

Recorded here so the next person does not have to re-derive them. **None of
these are known problems** — they are measurements looking for a decision.

1. **`raceDeadline` costs 38% of dispatch throughput.** A shared timer wheel, or
   skipping the race when the deadline is far away, would recover most of it.
2. **The change feed's per-turn snapshot is two full deep walks.** Structural
   sharing, or deferring the snapshot until a subscriber actually pulls, would
   change the 1-subscriber cliff.
3. **`memoryStorage` `structuredClone`s on both save and load**, on top of the
   codec walk — three copies of the state per save.
4. **The mailbox allocates ~4 promises per turn.** It is not the dominant cost
   today (see the ladder), so this is lower priority than it looks.
5. **Debounce the membership subscriber** — the single highest-value cluster
   fix, and the only measured O(N²).
6. **The 16-shard reminder ceiling** needs a decision, not a patch: raising
   the count is a storage migration.
7. **`consistentHashPolicy()` costs ~16 µs per decision at N=100** (63×
   worse than at N=1) because it hashes `actorId|siloId` for every silo.
   Only paid on a route-cache miss, and small next to a network hop.
