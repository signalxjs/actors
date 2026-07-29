# `@sigx/actors` benchmarks

Local performance baselines for the actor runtime: closed-loop throughput,
latency percentiles, heap footprint and leak detection — plus the profiling
recipes for working out *why* a number moved.

Not published, not run in CI. This is a tool you run by hand before and after a
change.

```sh
pnpm bench                    # build, then run everything
pnpm bench:run dispatch       # only scenarios matching "dispatch"
pnpm bench:run --quick        # smaller working sets, 1 round — a smoke test
pnpm bench:run --list         # every scenario and what it isolates

pnpm bench:baseline           # record this machine's reference (5 rounds)
pnpm bench:compare            # run again and diff against that reference
pnpm bench:profile dispatch   # same, under --cpu-prof

pnpm bench:tier2              # real sockets, a process per silo (opt-in)
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
dispatch/mailbox-raw          the promise-chain turn queue, no actor
dispatch/warm-grain           + placement, reentrancy check, directory, turn
dispatch/warm-grain-deadline  + raceDeadline (the production default)
dispatch/via-proxy            + client proxy and a minted call id
wire/endpoint-roundtrip       + wire codec, JSON, endpoint (no socket)
```

`c=1` is uncontended cost. `c=64` and `c=512` are the queueing story: against a
single grain the mailbox serializes, so throughput should flatten while p99
climbs. `dispatch/fan-out-grains` is the control — spread across 1 000 grains,
throughput should *not* collapse the same way.

`±NN%` after a value is that metric's own spread across rounds. Anything past
the comparison threshold means the metric cannot detect a regression of that
size, so don't read anything into it.

## Two tiers, and why the distinction is load-bearing

Almost everything here is **Tier 1**: one process, zero sockets. Cross-silo
scenarios route through a `pipeFetch` that calls straight into the peer's
handlers. That is deliberate — it isolates the *software* cost and the
algorithmic shape, which is what scales to 100 silos or does not — and it
means the entire socket story is invisible to it.

**Tier 2** (`cluster2/*`, `pnpm bench:tier2`) is the other half: N silos as
real forked processes over real loopback TCP. It is opt-in because it forks a
process per silo and binds ports, and it is slow.

```sh
pnpm bench:tier2                        # the Tier-2 scenarios
BENCH_TIER2=1 pnpm bench:run cluster2/  # same thing, spelled out
```

Inside Tier 2 there is a second split, and **it is enforced in code rather
than by this paragraph**: every timing metric is emitted `informational: true`
so the comparer structurally cannot fail a run on it.

- **Counted** — sockets, bytes per call, requests per connection. Counts of
  events, invariant under CPU scheduling. These gate.
- **Timed** — throughput, percentiles, RSS. N processes share the cores, so
  these are context, not evidence.

That split is not theoretical: the run recorded in `BASELINES.md` printed
`THE MACHINE WAS BUSY` (the probe moved 15%) while the socket counts came back
identical every single round.

How the rig works, in one paragraph: `child_process.fork` per silo — fork
inherits `execArgv`, so `--conditions=production` propagates and children
measure the built prod dist, where a spawned `node` would silently benchmark
the dev dist. Each child binds port 0, reads back the port, and only then
constructs `cluster({ advertise })`, preserving the invariant that something
answers before a peer can learn the address. Membership and the directory live
in the *parent* over IPC, because the rig's job is isolating the transport and
a real Redis would inject its own latency and sockets into every comparison
(#87 owns that axis) — a guard metric fails the run if store traffic per call
rises above 0.05, which would mean the route cache stopped working and the
numbers describe the store instead. Sockets are counted by **accepts on the
receiving side**: if a silo accepted K connections from a peer, that peer
opened K, so no hook into undici is needed. The count is cross-checked against
libuv's own TCP handle table.

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
pnpm bench:profile dispatch/warm-grain
```

**Allocation.** Sampled allocation-by-call-site — the direct way to confirm or
refute a suspicion about the promise-chain mailbox or `raceDeadline`:

```sh
node --conditions=production --expose-gc --enable-source-maps \
  --heap-prof --heap-prof-dir=benchmarks/profiles \
  benchmarks/src/main.ts dispatch/warm-grain --runs=1
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
actor that retains its context on activation measured **626 B/grain retained**
against **3 B/grain** clean — a 150× signal. If you change the memory scenarios,
re-do that; a leak detector that has never caught a leak is not evidence.

## Cluster scaling

`cluster/*` answers a different question from the rest of the suite: not
"how fast" but **"does it still work at 100 silos"**.

```sh
pnpm bench:run cluster/          # the whole sweep, N = 1, 2, 10, 50, 100
pnpm bench:run cluster/ --quick  # N = 1, 2, 10
```

Every silo shares one CPU here, so **absolute throughput from an N=100 run is
meaningless** — it is 100 silos contending for one core. What is exact, and
what these measure, is the *algorithmic* shape: how many provider calls the
runtime makes and how per-decision cost grows with N. An O(N²) shows up here
on a laptop instead of on a bill.

How to read it: a metric that stays **flat** from N=1 to N=100 scales. One
that grows with N is a shared bottleneck. One that grows with N *per silo* is
an O(N²).

`benchmarks/src/cluster-harness.ts` wraps each silo's cluster providers in a
counter, so `directory.claim`, `membership.refresh`, change notifications and
the rest are all attributable per silo.

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
`REDIS_URL=redis://localhost:6399 pnpm test -- actors-redis`. Measuring it
for real needs a Redis instance, and interpreting a real 100-silo run needs
cluster-wide stats that do not exist yet (issue #38).

See `BASELINES.md` for what the sweep found.

## Adding a scenario

A scenario is `{ name, description, run(ctx) → Metric[] }`, registered in
`src/scenarios/index.ts`. It must create and tear down its own silos — the runner
calls `run()` many times.

```ts
const myScenario: Scenario = {
    name: 'group/thing',
    description: 'what layer this isolates',
    async run(ctx) {
        const fixture = await createBenchSilo({ actors: [Tiny] });
        try {
            return await sweepConcurrency({
                call: () => fixture.silo.dispatch(ref, 'noop', [], benchCall()),
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
- `createBenchSilo` uses `manualScheduler()` and hour-long sweep/reminder
  intervals, so background jobs never land mid-measurement. Scenarios that
  measure those jobs drive `fixture.clock.advance(ms)` themselves.
- `callTimeoutMs` defaults to `0` (no `raceDeadline` on the measured path). Pass
  `PRODUCTION_CALL_TIMEOUT_MS` to measure the default configuration.
- Comparing two actor definitions? Make them **identical except for the variable**.
  The guard benchmark first compared against `Tiny`, which also carries a stream
  table and extra methods, and concluded that guards make things *faster*.
