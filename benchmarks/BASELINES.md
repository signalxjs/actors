# Baselines

Reference figures, recorded by hand. `benchmarks/baselines/local.json` is your
machine's working baseline and is gitignored — absolute numbers from someone
else's laptop are not comparable. This file exists so the **relationships**
between layers are reviewable in a PR, since those hold across machines even
when the absolute figures do not.

Update it deliberately, when a change moves something here, and always record
the machine.

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
