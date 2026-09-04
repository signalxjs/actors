# `@sigx/actors` benchmarks

Local performance baselines for the actor runtime: closed-loop throughput,
latency percentiles, heap footprint and leak detection — plus the profiling
recipes for working out *why* a number moved.

Not published, and never a merge gate. Primarily a tool you run by hand before
and after a change — CI also runs it on perf-sensitive PRs, but only to *report*
(see [In CI](#in-ci)).

```sh
pnpm bench                    # build, then run everything
pnpm bench:run dispatch       # only scenarios matching "dispatch"
pnpm bench:run --quick        # smaller working sets, 1 round — a smoke test
pnpm bench:run --list         # every scenario and what it isolates

pnpm bench:baseline           # record this machine's reference (5 rounds)
pnpm bench:compare            # run again and diff against that reference
pnpm bench:profile dispatch   # same, under --cpu-prof

pnpm bench:tier2              # real sockets, a process per host (opt-in)

# diff two saved result files (neither has to be the baseline)
pnpm bench:diff --before=a.json --after=b.json [--markdown=out.md]
```

Requires Node ≥ 22.18 (the sources are `.ts`, run through Node's native type
stripping — same requirement as `examples/counter/cluster-demo.mjs`).

## What it measures against

**The built prod dist, not the source.** `--conditions=production` picks
`dist/*.prod.js` out of the export map, so the numbers describe the code users
actually run, with `__DEV__` stripped by the minifier. `pnpm bench` builds
first; `pnpm bench:run` assumes you already did — **stale `dist/` means you are
benchmarking your last change, not this one.**

`pnpm bench:dev-dist` runs the same suite against the dev dist instead. The gap
between the two is what the `__DEV__` guards cost.

Sourcemaps ship with the dist, so `--cpu-prof` frames resolve back to
`packages/actors/src/*.ts` — which is what makes profiling worth doing here.

## Reading the output

Scenarios are a **ladder**. Each one adds a single layer to the one above, so
what you want is the gap, not the absolute number:

```
dispatch/turns-raw            promise-chain turn serialization, no actor
dispatch/warm-actor           + placement, reentrancy check, directory, turn
dispatch/warm-actor-deadline  + the call-deadline machinery (the production default)
dispatch/via-proxy            + client proxy and a minted call id
wire/endpoint-roundtrip       + wire codec, JSON, endpoint (no socket)
```

`c=1` is uncontended cost. `c=64` and `c=512` are the queueing story: against a
single actor, turns serialize, so throughput should flatten while p99
climbs. `dispatch/fan-out-actors` is the control — spread across 1 000 actors,
throughput should *not* collapse the same way.
`dispatch/always-warm-actor` is the same call over a `reentrant: 'always'`
actor — the delta against `dispatch/warm-actor` prices the interleaved lane
(per-turn AsyncLocalStorage + concurrent-lane bookkeeping), and
`dispatch/always-warm-turns` gates that path as a count, exactly as
`dispatch/warm-turns` gates the serial one.

`±NN%` after a value is that metric's own spread across rounds. Anything past
the comparison threshold means the metric cannot detect a regression of that
size, so don't read anything into it.

## Two tiers, and why the distinction is load-bearing

Almost everything here is **Tier 1**: one process, zero sockets. Cross-host
scenarios route through a `pipeFetch` that calls straight into the peer's
handlers. That is deliberate — it isolates the *software* cost and the
algorithmic shape, which is what scales to 100 hosts or does not — and it
means the entire socket story is invisible to it.

**Tier 2** (`cluster2/*`, `pnpm bench:tier2`) is the other half: N hosts as
real forked processes over real loopback TCP. It is opt-in because it forks a
process per host and binds ports, and it is slow.

```sh
pnpm bench:tier2                        # the Tier-2 scenarios
BENCH_TIER2=1 pnpm bench:run cluster2/  # same thing, spelled out

## Trusting the numbers

This is the part that took the most work, because the first honest attempt at a
self-check — comparing identical code against its own baseline — reported **24
regressions**. Everything below exists because of a specific way the suite lied:

- **Repetitions are interleaved** (round 1 of every scenario, then round 2, …)
  rather than finishing one scenario at a time. A laptop loses 15%+ of its speed
  to throttling and background load over a suite run; sequentially, all of that
  lands on whichever scenarios run last, which is exactly the ladder comparison
  the suite exists to make.

- **A machine probe runs every round.** It is an actor-free async
  allocation loop, so any change in *its* score is the machine, not the code.
  If it differs from the baseline's by more than half the regression threshold,
  every verdict is downgraded to inconclusive and the run reports **no verdict**
  — blaming your change for your laptop being busy is how a perf suite gets
  ignored. The probe must match the workload's *shape*: the first version was a
  tight integer loop and was useless, moving 4% while the scenarios moved 25%.

- **Noise is estimated by the full range, not the IQR.** Across five rounds an
  IQR comes from two order statistics and badly understates dispersion; it waved
  through 12–16% swings on unchanged code.

- **Metrics that can sit at or below zero carry an absolute `noiseFloor`** (heap
  slopes, retained bytes). A percentage against a baseline of ~0 is meaningless
  and against a negative one it *inverts*, which would have reported a brand-new
  leak as an improvement.

- **Sub-microsecond latencies carry a floor too.** An uncontended turn is ~0.2µs,
  where a 0.04µs wobble reads as "20% slower".

- **The threshold is 10%**, calibrated against measured noise rather than picked.
  GC metrics are single-sample and never gate; large moves surface separately
  under *Diagnostics*.

**Protocol for numbers you intend to act on:** plug the laptop in, quit the
browser and anything else busy, and record the baseline and the comparison run
back to back under the same conditions. Watch for the `THE MACHINE WAS BUSY`
banner — a suite run on a contended machine is not evidence. On a quiet machine
`--threshold=5 --runs=9` is reasonable for chasing something small.

## In CI

`.github/workflows/bench.yml` runs on any PR touching `packages/**`,
`benchmarks/**` or the lockfile, and posts one comment (updated in place on
each push) with a verdict table.

**It compares the PR's base ref against its head ref in interleaved,
counterbalanced rounds on one runner** (#98, ported from core#639) — not
against `BASELINES.md` and not against a committed baseline. A stored baseline
is worse than useless off-machine: the calibration probe would differ from
whatever machine recorded it, and every run would come back "no verdict"
(which is exactly what `pnpm bench:compare` correctly does if you hand it
someone else's baseline).

Why rounds rather than two halves: the sequential job measured base then head,
so drift, thermal state and cache warmth all accrued to the head — a measured
~+2% systematic bias, which no threshold can remove because it is bias, not
noise. `ab.ts` alternates the sides and counterbalances the order (even rounds
base-first, odd rounds head-first), so a linear drift cancels in the paired
deltas. `ab-report.ts` then says something honest per row: `improved` or
`regressed` only when **every round agrees in sign** (a sign test at
p = 2^-rounds) **and** the median delta clears both a 3% effect floor and the
row's own run-to-run spread; a side that swings more than 10% reads `noisy`
and claims nothing. Row matching and per-round deltas come from the same
`compare()` as the local flow.

The same machinery runs by hand between any two built checkouts
(`pnpm bench:ab` / `pnpm bench:ab:report`), and on the bench VM against any
two refs without opening a PR:

    gh workflow run bench.yml -f base=<ref> -f head=<ref> -f rounds=7

(`-f enforce=true` additionally fails the run on a unanimous timing
regression — the deliberate proof mode. The PR path never gates timings.)

### Two kinds of number, gated completely differently

This is the part worth internalising, and it is enforced in code
(`Metric.exact`) rather than by this paragraph:

- **Timings are informational on the PR path.** Throughput, percentiles, heap
  bytes. Even a `regressed` verdict — unanimous and above the floors — is a
  pointer, not a gate: re-litigate it with more rounds via the dispatch, or
  locally on a quiet machine, before acting on it.
- **Exact metrics gate, at zero tolerance.** `directory_ops` per activation,
  `microtask_turns` per dispatch, `notifications` per join, and the locality
  guarantees of the `prefer-local` placement arms. These are algorithmic
  *invariants*, not measurements: the same code yields the same value on any
  machine under any load. So they carry no threshold, no noise gate, and no
  machine-drift downgrade — **and an exact regression fails the Bench check.**
  An extra directory round-trip per activation moves `directory_ops` from 2 to
  3; a routing change that broke perfect locality moves
  `edgehash+prefer-local/hops_per_call` off 0. Neither is resolvable by any
  timing comparison on a shared runner, and both are invisible in review.

Mark a metric `exact: true` only where determinism holds **by construction**,
and note that "looked stable" is not the test:

- `preferLocalPolicy()` activates on the receiving host, and every edge
  strategy here is a pure function of an index or a key. Those arms qualify.
- `randomPlacementPolicy()` obviously does not.
- `consistentHashPolicy()` does not either, for a subtler reason worth
  remembering: it is deterministic *given the host ids*, and the ids are minted
  per run. Steady within a run, different across two — the worst possible shape
  for a gate.
- `cluster/reminder-shard-ownership` at n=2 was bit-identical in four runs and
  is still not flagged: 16 shards landing across 2 hosts is *probably* even, not
  deterministic, and it already varied at n=10.

### The merge queue runs a narrower set

`merge_group` supports no `paths` filter, so Bench fires on every queued merge
including docs-only ones. Re-measuring the whole suite there would tax each
merge to re-check timings that cannot fail anything, so the queue measures only
the scenarios carrying `exact` metrics — `BENCH_GATE_SCENARIOS` in the workflow.
That list and the flags are two halves of one decision living in two files, so
`benchmarks/__tests__` asserts they agree in both directions: a filter is a
substring match, and a renamed scenario would otherwise match nothing and shrink
the gate in silence.

The rest is only honesty about what the informational half is:

- A scenario that *throws* fails the step regardless — a benchmark that cannot
  prove it did its work must not report a number.
- **`noisy` and `no change` are normal outcomes.** A row whose own spread
  covers its delta has nothing to say, and the report says so instead of
  printing a number that looks like a finding.
- An `exact` metric that varies **between rounds of the same code** reads
  `nondeterministic` and fails the check outright — that breaks the
  `Metric.exact` contract, which matters more than any delta on top of it.
- Tier 2 stays out — it wants spare cores.

`workflow_dispatch` with no `base` input runs the suite once against the
current ref and uploads the result JSON, for when you want a number off a
Linux box without one to compare it to; with `base` (and optionally `head`,
`rounds`, `enforce`) it runs the interleaved A/B between those refs instead.

### What a shared runner actually does

The first A/B this workflow ran was the PR that added it — **no runtime change
whatsoever**, so every delta was by definition noise. It is the cleanest
calibration available, and worth recording:

- **1 `regressed`, 4 `improved`, 10 `inconclusive`** out of 363 metrics, at the
  local 10% threshold. The false verdicts spanned **10.1% to 18.7%**. That is
  why CI runs at 25%: the per-metric noise gate estimates noise from the spread
  across rounds *inside one process*, and two processes minutes apart on a
  shared 2-vCPU runner vary by more than that. A metric that happens to be
  steady within each half slips straight through it. A second identical-code
  run at 25% still produced two — a p99 that swung 53%, and a zero-baseline
  count — so **no threshold makes a timing trustworthy here**. That is the
  finding that motivated the exact gate rather than a bigger number.
- **148 of those 363 came back bit-identical**, across two independent
  processes on a noisy shared box. Those are the invariants — provider-call
  counts, microtask turns — and they are why the `exact` gate is worth more
  than the timing table it sits above. Not all 148 qualify: many are randomized
  ratios that merely happened to match, which is why the flag is applied by
  hand and only where the scenario has no randomness in it.
- **Order matters, mildly.** The head half is always measured second, and 31 of
  41 throughput metrics came back faster on it (median +1.76%). Small, but a
  real bias, not a coin flip — read a sub-5% "improvement" on your own PR as
  the running order, not the code.
- **The calibration probe did not see either effect**: it moved 1.8% between
  the halves, well inside the drift limit, so no banner fired. The probe
  catches a machine that is globally slower; it does not catch one scenario
  landing in a noisy window.
- The runner is 2 vCPU, and it is *not* slow — `dispatch/warm-actor c=1` came
  back at 814k ops/s there against 357k on a busy developer laptop. Absolute
  numbers from either are still meaningless; only the paired delta is not.

The suite takes ~4 minutes a half on that runner including install and build,
so the whole job is ~8–9 minutes.

`pnpm bench:diff` is the comparer the workflow calls, and it works on any two
result files — including two `benchmarks/results/<iso>.json` from your own
machine, which is the quickest way to diff two local runs without disturbing
the recorded baseline.

## Baselines

- `benchmarks/results/<iso>.json` — every run (gitignored)
- `benchmarks/baselines/local.json` — this machine's reference (gitignored)
- `benchmarks/BASELINES.md` — committed reference figures **with the machine
  they came from**

Committed absolute numbers from someone else's laptop are noise, which is why
only the curated markdown is in git. `bench:compare` refuses to compare silently
across a different node major, platform, arch or CPU.

## Working out what actually cost you

**CPU.** `pnpm bench:profile <scenario>` writes a `.cpuprofile` into
`benchmarks/profiles/`; open it in Chrome DevTools (Performance → Load profile)
or [speedscope](https://www.speedscope.app/). With sourcemaps on, frames land on
real `src/*.ts` lines.

```sh
pnpm bench:profile dispatch/warm-actor
```

**Allocation.** Sampled allocation-by-call-site — the direct way to confirm or
refute a suspicion about turn serialization or `CallDeadlines`:

```sh
node --conditions=production --expose-gc --enable-source-maps \
  --heap-prof --heap-prof-dir=benchmarks/profiles \
  benchmarks/src/main.ts dispatch/warm-actor --runs=1
```

**GC pressure.** Every scenario reports `gc/pause_ms` and `gc/collections` from a
dedicated pass. A change that doubles GC pause is a real regression even when
mean latency looks flat, and it is usually the explanation for a throughput drop.

This uses `v8.GCProfiler`, **not**
`PerformanceObserver({entryTypes:['gc']})`. The observer is delivered through the
event loop, and an in-process actor benchmark is a pure-microtask workload that
never turns it — the observer silently reports zero while `--trace-gc` shows
hundreds of scavenges. (`GCProfiler`'s `cost` field is microseconds, verified
against `--trace-gc`.)

**Heap composition.** When a leak scenario trips, get a snapshot and walk the
retainer paths in DevTools (Memory → Load profile):

```sh
BENCH_HEAP_SNAPSHOT=1 pnpm bench:run mem/leak-activate-deactivate
```

**Is the leak detector actually working?** It was verified by planting one: an
actor that retains its context on activation measured **626 B/actor retained**
against **3 B/actor** clean — a 150× signal. If you change the memory scenarios,
re-do that; a leak detector that has never caught a leak is not evidence.

## Cluster scaling

`cluster/*` answers a different question from the rest of the suite: not
"how fast" but **"does it still work at 100 hosts"**.

```sh
pnpm bench:run cluster/          # the whole sweep, N = 1, 2, 10, 50, 100
pnpm bench:run cluster/ --quick  # N = 1, 2, 10
```

Every host shares one CPU here, so **absolute throughput from an N=100 run is
meaningless** — it is 100 hosts contending for one core. What is exact, and
what these measure, is the *algorithmic* shape: how many provider calls the
runtime makes and how per-decision cost grows with N. An O(N²) shows up here
on a laptop instead of on a bill.

How to read it: a metric that stays **flat** from N=1 to N=100 scales. One
that grows with N is a shared bottleneck. One that grows with N *per host* is
an O(N²).

`benchmarks/src/cluster-harness.ts` wraps each host's cluster providers in a
counter, so `directory.claim`, `membership.refresh`, change notifications and
the rest are all attributable per host.

**What the in-process sweep cannot tell you:** what those provider calls cost
against real Redis. `memoryClusterHub` answers `refresh()` from a local map.
The `redis_ops_modelled` metric applies the Redis provider's refresh shape to
the measured notification count — *derived from the provider source, not
measured*, and it was wrong twice before anyone measured it.

### Measuring it for real

`cluster/redis-amplification` counts actual Redis commands per membership
change. It skips cleanly without `REDIS_URL`, so the default suite still runs
anywhere.

```sh
# macOS — no container runtime needed, Redis is a native bottle
brew install redis
redis-server --port 6399 --save '' --appendonly no --daemonize yes

REDIS_URL=redis://localhost:6399 pnpm bench:run cluster/redis-amplification
```

`keydb` and `valkey` are also native bottles if you want to compare a
multithreaded store — the amplification is algorithmic, so a faster server
raises the ceiling without changing the shape. Podman is *not* needed on
macOS and would cost a Linux VM for no benefit; use it only if you
specifically want parity with CI's `redis:7` service.

The same variable un-skips the provider tests:
`REDIS_URL=redis://localhost:6399 pnpm test actors-redis`. Measuring it
for real needs a Redis instance, and interpreting a real 100-host run needs
the cluster-wide stats `clusterStats()` now provides.

See `BASELINES.md` for what the sweep found.

The same variable un-gates the two `reminders-redis/*` scenarios (#382),
the ones the roadmap's reminder workstream is priced against:
`arm-fire` — N in-process hosts over one `redisStorage`, an open-loop arm
ladder, and the rate at which the sharded reminder table's three-attempt
CAS starts throwing; and `table-size` — one rung against P entries already
asleep in the shard records, so the cost a sleeping population adds to
every set and every tick is a number.

```sh
REDIS_URL=redis://localhost:6399 pnpm bench:run reminders-redis/ --runs=2 --no-warmup
```

`arm-fire` stops climbing a host count's ladder once a rung fails more than
a quarter of its arms, since every rung above would only measure the
backlog; `table-size` runs its three populations regardless.

## Adding a scenario

A scenario is `{ name, description, run(ctx) → Metric[] }`, registered in
`src/scenarios/index.ts`. It must create and tear down its own hosts — the runner
calls `run()` many times.

```ts
const myScenario: Scenario = {
    name: 'group/thing',
    description: 'what layer this isolates',
    async run(ctx) {
        const fixture = await createBenchHost({ actors: [Tiny] });
        try {
            return await sweepConcurrency({
                call: () => fixture.host.dispatch(ref, 'noop', [], benchCall()),
                concurrencies: [1, 64],
                durationMs: ctx.durationMs
            });
        } finally {
            await fixture.stop();
        }
    }
};
```

Rules worth knowing:

- Every metric declares a `direction` (`'higher'` = bigger is better). Getting it
  wrong inverts the verdict.
- Any metric that can be zero or negative **must** set `noiseFloor`.
- Is it an *invariant* — a count of work the runtime does, identical on every
  machine — rather than a measurement? Then set `exact: true` and it becomes a
  CI gate. This is the highest-value thing a scenario can contribute, because
  it is the only kind of number a shared runner can judge. Set it only where
  determinism holds by construction: no `randomPlacementPolicy()`, no wall
  clock, no heap.
- `createBenchHost` uses `manualScheduler()` and hour-long sweep/reminder
  intervals, so background jobs never land mid-measurement. Scenarios that
  measure those jobs drive `fixture.clock.advance(ms)` themselves.
- `callTimeoutMs` defaults to `0` (no deadline machinery on the measured path). Pass
  `PRODUCTION_CALL_TIMEOUT_MS` to measure the default configuration.
- Comparing two actor definitions? Make them **identical except for the variable**.
  The guard benchmark first compared against `Tiny`, which also carries a stream
  table and extra methods, and concluded that guards make things *faster*.
