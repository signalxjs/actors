# Live reads and the watch machinery

How a `$live` subscription becomes re-invocations of a read method, when the
runtime shares one loop across subscribers and when it must not, and where the
measured sizing cliff is. For how to *use* live subscriptions, see
<https://sigx.dev/actors>; this note is the reasoning behind the machinery in
`packages/actors/src/host/watch.ts`, `host/watch-core.ts`, and the watch
sections of `host/activation.ts`.

## The shared-loop model

`ctx.changes()` yields state, but a subscriber asked for a **method result**
(`recent(20)`, `unreadCount()`). Only the actor can derive one from the other,
so a watch re-invokes the read after every mutating turn rather than pushing
state and hoping the client can (`watch.ts`'s header states this as the design
premise).

Two properties carry the scaling story, and neither is an optimisation:

- **One loop per `(method, throttleMs, encodedArgs)`** — the key grammar is
  `watchKey` over `canonicalKey`, injective by construction. Fifty subscribers
  to the same read cost one re-invocation per mutating turn, not fifty; a late
  joiner replays the loop's `last` value from the fan-out
  (`watch-core.ts`, `createFanOut`) and **enqueues no turn at all**. This is
  why anonymous fan-out scales to the tens of thousands (see the numbers
  below).
- **Trailing throttle.** The loop collapses the change feed to a dirty flag,
  waits the window out, then reads once — so a burst of mutations costs one
  re-invocation, and the value emitted is the final one, never a stale
  intermediate.

The loop's re-read runs with **a normal turn's isolation** — call context,
observer event, and change boundary per read — but serial-lane watch reads
do not each hold a queue slot: they run through the **watch read pump**
(`host/watch-pump.ts`, #180). The pump keeps at most ONE drain turn on the
queue, running pending reads back-to-back up to a 32-read slice per turn,
**seeds first**, with the remainder (and anything requested mid-drain)
re-enqueued at the tail. Three consequences carry #180's fix: the whole
watch population contributes O(1) queue slots however many loops exist; an
external call waits for at most one slice, never O(loops) read turns; and a
new subscriber's seed drains ahead of every pending re-read, so
establishment cannot starve behind steady-state fan-out. Failure stays per
read (one rejecting read reaches its own loop only; the drain turn never
rejects — the `Turns` tail must never carry a rejection). Interleaved
methods keep the direct `enqueue` path: they never contend on the serial
lane, and batching them would add serialization they opted out of. The seed
read is still requested synchronously when the first subscriber attaches —
subscribing costs a read *for the first subscriber on a key* and near-zero
for everyone after.

A watch subscriber's change feed rides the `ticksOnly` sentinel — it never
costs a state snapshot. That contract, and what breaks if the pump ever reads
the value, is documented in
[`runtime-internals.md`](runtime-internals.md) (#129); it is not restated here.

## The per-principal split (#121)

A read that derives its result from `ctx.principal` makes identity an **input
to the read**. Sharing a loop across identities then serves one subscriber
another's view — the first subscriber's context is what the loop invokes
under. That was a real defect (#121), and the fix is the split:

- The `ctx.principal` getter marks the invoking watch's base key (the
  `kWatchBase` marker on the loop's call context) into the activation's
  `#watchPrincipalDependent` set — **discovery, not declaration**. One touch
  anywhere in the read, even a discarded one, marks the key for the
  activation's lifetime.
- From then on, `#resolveWatch` qualifies the key with the subscriber's
  **encoded** principal (`qualifyWatchKey`, injective against bare base keys),
  so each distinct identity gets its own loop. The discovery sweep in the
  entry's invoke `finally` evicts mismatched subscribers *before* the
  discovering value is pushed — no subscriber ever receives a value computed
  under someone else's identity.
- Entry keys are **immutable**: a pre-discovery entry at the base key drains
  and is never re-keyed or rejoined, which is what keeps `onEmpty`'s
  delete-by-key exact. (Cost: the first same-principal re-subscriber after
  discovery builds one extra loop while the base entry drains.)
- Discovery is **per activation** and rediscovers from scratch after failover
  or idle collection — sticky state lives nowhere durable.

This is a correctness rule, not an optimisation miss. Any change that
re-merges loops across identities must prove the read cannot observe the
principal — which is exactly the declared-read design #138 records for the
cross-host half.

## The throttle is fixed at 50 ms for clients

`DEFAULT_WATCH_THROTTLE_MS` (`watch-core.ts`) is part of the watch identity,
but **no client can set it**: neither the `$live` endpoint nor the socket
session passes `throttleMs`, so every browser subscription runs at exactly
50 ms — a hard ~20 pushes/sec ceiling per subscriber, and one reason
`deliveries_per_publish` in the socket benchmarks is a coalescing ratio, not a
constant. Only in-process `dispatchWatch` callers choose a window (the Tier-1
benchmarks use 0 to make counts deterministic).

## Anonymous and signed-in fan-out are different products

Measured on the Tier-3 AKS rig (3 host pods, 1000m CPU limit, one `Fanout`
actor, 10 publishes/s — `perf/aks`, runbook scenario (q); recorded as
`sockets/principal-cliff` in `benchmarks/src/scenarios/sockets.ts`, figures in
`benchmarks/BASELINES.md`, analysis in #180):

Anonymous (`current()`, no principal): **20 000 subscribers**, zero failures,
p50 370 ms at the top rung.

Per-user (`mine()`, one distinct identity per connection):

| identities | connected | failures | deliveries/s | publishes in window |
|---|---|---|---|---|
| 100 | 100 | 0 | 1 003 | healthy |
| 250 | 220 | **30** | 0.4 | **1** |
| 1 000 | 484 | **516** | 2.0 | **1** |

It is a cliff, not a curve, and **establishment is what breaks**: re-running
250 identities at 1 publish/s connects 216 of them and serves them at full
rate with 52 ms p50 — every failure happened while dialling. The mechanism,
as measured (pre-pump):

- P distinct principals = P watch loops on one activation, each with its own
  change subscription. Every mutating turn dirties all P; after the 50 ms
  window each enqueued its own read turn — **O(P) turns per publish** on one
  serial queue.
- A new identity's seed was one more serialized turn, FIFO behind those — and
  the watch path armed **no deadline anywhere** (it bypasses `CallDeadlines`
  by design; the loop lives forever), so a starved seed waited as long as the
  client did, silently. Establishment and steady-state fed on each other past
  the threshold: the collapse of #180.

Two changes moved the local mechanism (#180). The **watch read pump**
(above) removed both FIFO costs: the watch population now holds one queue
slot, and seeds drain first — Tier 1's `live/principal-fanout` gates the
counts. And the **socket session arms the posture's `timeoutMs` on
establishment** (pipeline + authorization + dispatch + first value): a
seed that still cannot run in time answers a per-subscription 504 frame
and releases the loop and keep-alive it held, instead of hanging forever.
A seeded subscription is never timed out; the `$live` endpoint still has
the hang-forever gap (#192).

**On the cluster, the shelf turned out to be a different mechanism** — and
the 504s are what made it diagnosable (#194). Every per-principal
cross-host stream pins one pooled host-to-host connection for the life of
the subscription, and the measured 100–250 ceiling was the rig's fetch
pool arithmetic (64/peer × 2 relay pods = 128 streams ≈ 190 identities),
not the turn queue: the pump image alone left `max_healthy_identities` at
100, and sizing the pool on the same image took 1 000 identities to zero
failures at the throttle-floor latency. The remaining per-identity costs —
the reads themselves, P change subs, P settle timers, P socket frames, and
above all **one held connection per identity per host hop** until #138 —
are the steady-state model.

Local control measurement (200 subscribers, #172): 31 actor turns anonymous vs
6 200 per-user for the same publish load; one identity watching a
principal-reading method costs 62 — the split is per *principal*, not per
subscriber.

Never average the two arms. A read only has to touch `ctx.principal` once for
the split — and this entire section — to apply to it.

## Cross-host is worse, and unconditional (#138)

The cluster's relay coalesces cross-host watches per
`(actor, method, throttleMs, args, principal)` —
`#coalescedWatch` in `cluster/placement.ts` includes `call.principal` in the
key **whether or not the read consults it**, because the relay cannot observe
the owner's discovery. So 10 000 anonymous subscribers across 3 hosts cost 2
cross-host streams; 10 000 signed-in subscribers cost 10 000, even on an
identity-blind read. Restoring cross-principal sharing for declared
identity-independent reads is #138. The exact-count invariants for what
coalescing does guarantee are gated by the `cluster/live-fanout` benchmark.

## Sizing guidance, and the guardrails that pin it

The old ceiling was **~100 distinct principals** on one hot actor — and
it was the host-to-host fetch pool, not the runtime (#194): with the pool
sized to the identity population and the #193 pump in place, **1 000
distinct identities on one actor measured clean** (zero failures, p50 at
the 50 ms throttle floor; the recorded `max_healthy_identities: 500` was
the ceiling of the ladder as it stood that day — the default ladder now
reaches 1000). The durable sizing rule: per-principal cross-host
streams each hold a pooled connection until #138 lands, so size the
host-to-host pool for the signed-in watcher population, or use
`@sigx/actors-tcp` (one multiplexed connection per peer). Concretely:

- Keep `mine()`-shaped reads — anything consulting `ctx.principal` — off hot
  shared actors. Shard them: a per-user or per-cohort actor holds the
  identity-dependent projection, and the shared actor publishes to it (the
  `examples/chat` ActivityFeed shape), or the read takes the identity as an
  explicit *argument* on a caller-validated path so args, not principals, key
  the loops.
- Watch the **`watchLoops` gauge** (`HostStats.watchLoops`, in `metrics()`
  gauges / `ops()` / cluster `HostReport`) against subscriber counts, and
  `ActivationInfo.watchLoops`/`watchSubscribers` for the per-actor drill-down:
  many subscribers per loop is healthy sharing; a loop count tracking the
  subscriber count on a hot actor is this cliff building.
- What pins the numbers: `live/principal-fanout` (Tier 1, `exact` — the
  loop/seed/read counts, gated on every PR), `sockets/principal-cliff`
  (Tier 3, recorded — `max_healthy_identities` is the figure #180 exists to
  move) and the measured tables in `benchmarks/BASELINES.md`. Changes to
  the watch machinery must show those moving, not just unit tests passing.
