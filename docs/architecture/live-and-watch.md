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

The loop's re-read is a **normal serialized turn** (`Activation.enqueue` from
the entry's `invoke` closure) — the watch gets exactly the isolation every
other call has, and its reads queue behind (and ahead of) everything else on
the activation's single turn queue. The seed read fires synchronously when the
first subscriber attaches, so subscribing has a turn's worth of cost *for the
first subscriber on a key* and near-zero for everyone after.

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
rate with 52 ms p50 — every failure happened while dialling. The mechanism:

- P distinct principals = P watch loops on one activation, each with its own
  change subscription. Every mutating turn dirties all P; after the 50 ms
  window each enqueues its own read turn — **O(P) turns per publish** on one
  serial queue.
- A new identity's seed is one more serialized turn, FIFO behind those — and
  the watch path arms **no server-side deadline** (it bypasses
  `CallDeadlines` by design; the loop lives forever), so a starved seed waits
  as long as the client does. Establishment and steady-state feed on each
  other past the threshold: the collapse of #180.

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

Until #180's establishment fix and #138 land, treat identity-dependent live
reads on one hot actor as bounded at **~100 distinct principals** (the
measured cliff starts between 100 and 250 on 1-CPU pods). Concretely:

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
- What pins the numbers: `sockets/principal-cliff` (Tier 3, recorded —
  `max_healthy_identities` is the figure #180 exists to move) and the
  measured tables in `benchmarks/BASELINES.md`. Changes to the watch
  machinery must show those moving, not just unit tests passing.
