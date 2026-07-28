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

The cost is in what a notification *becomes*. The Redis provider answers
each one with a `refresh()` — one `SMEMBERS`, then one `HGET` per id that
came back (`packages/actors-redis/src/index.ts:137-141,188`), with no
debounce. A join leaves `N + 1` silos in the set, so each refresh is `N + 2`
round trips:

| N | notifications | Redis ops for ONE join |
|---:|---:|---:|
| 10 | 10 | 120 |
| 50 | 50 | 2 600 |
| 100 | 100 | **10 200** |

A rolling restart of 100 silos is ~200 membership changes — on the order of
**2 M Redis operations**, in bursts. The `redis_ops_modelled` metric applies
the provider's own refresh shape to the measured notification count; it is
derived from the provider source, **not measured against Redis** (that needs
a Redis instance — Tier 2).

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
