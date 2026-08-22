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
principal. `watches: { m: { principalIndependent: true } }` (#138, below)
discharges that obligation for the cross-host half — not by trusting the
author, but by making the observation FATAL: a declared read that consults
`ctx.principal` never returns a value to a merged population, because the
getter throws before the read completes and the invoke wrapper rethrows if
the body catches it. The owner-side machinery here is unchanged; a declared
method simply can never enter `#watchPrincipalDependent`.

## A client may ask to be served more slowly — from a fixed ladder

`DEFAULT_WATCH_THROTTLE_MS` (`watch-core.ts`) is part of the watch identity.
Until #247 **no client could set it**: neither the `$live` endpoint nor the
socket session passed `throttleMs`, so every browser subscription ran at
exactly 50 ms — a hard ~20 pushes/sec ceiling per subscriber, and one reason
`deliveries_per_publish` in the socket benchmarks is a coalescing ratio rather
than a constant.

That ceiling was also a FLOOR on cost, and #245 is why it had to go: profiled,
a delivery is 77% socket write, and every subscriber is its own socket, so a
publish costs one `writev` per subscriber. Deliveries per subscriber is the
multiplier on that, and it was not the subscriber's to choose. A tile happy to
update once a second was billed for twenty writes a second, forever.

A subscription may now carry `w` — a REQUESTED window in ms, on
`LiveSubscription`, so one field serves both mounts. The server does not
honour it verbatim:

```
absent            → no throttleMs is passed at all
n >= 0            → max(n, policy.min), then rounded UP to the nearest bucket
above the ladder  → the top bucket
anything else     → refused ($live per request, the socket per subscription)
```

**Rounding up to a ladder is the whole design, not a detail.** `throttleMs` is
in the watch key (`Activation.openWatch`) and in the cross-host coalescing key
(`ClusterPlacement.#coalescedWatch`), so honouring arbitrary numbers would
give every distinct value its own watch loop and its own remote stream — the
sharing #121, #138 and #139 exist to create, undone by clients asking for
1000, 1001 and 1002. A socket may hold 256 subscriptions, which makes that a
cheap way for one client to multiply an actor's work. The ladder is a hard
ceiling on fragmentation: at most `|buckets|` loops, whatever clients send.

Two properties are load-bearing and are pinned by tests:

- **Absent is not "the default value".** No `throttleMs` is passed at all, so
  the watch key is byte-for-byte the one a client that never heard of `w`
  produces. `Activation.openWatch` resolves an absent option to
  `DEFAULT_WATCH_THROTTLE_MS` and keys on the resolved number, so a request
  the policy answers with 50 shares that loop too — under
  `DEFAULT_THROTTLE_POLICY` that is any `w <= 50`, and old and new clients
  share. It is a property of the POLICY, not a guarantee of the field: a
  deployment configuring `{ min: 250, … }` puts every explicit request on a
  different loop from the silent clients, who keep the runtime default.
  That is the correct reading of `min` — it is a floor on what a client may
  ASK for, not a way to slow down clients that ask for nothing.
- **Never faster than asked.** The floor is `DEFAULT_WATCH_THROTTLE_MS`, so
  the default policy can only make a subscription slower. Sub-50 ms delivery
  is an operator decision (`{ min: 0, buckets: [0, 16, 50] }`), and
  `{ min: 50, buckets: [50] }` refuses the feature outright.

The client-side subscription identity (`app/live-shared.ts` `canonical()`)
carries the window too — otherwise two subscribers who asked for different
windows would coalesce onto one wire entry and one of them would silently get
the other's rate.

In-process `dispatchWatch` callers still choose any window they like; the
policy governs the wire, not the runtime (the Tier-1 benchmarks use 0 to make
counts deterministic).

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
counts. And **both wire mounts arm the posture's `timeoutMs` on
establishment** (pipeline + authorization + dispatch + first value — the
socket session since #180, the `$live` endpoint since #192): a seed that
still cannot run in time answers a per-subscription 504 frame and releases
the loop and keep-alive it held, instead of hanging forever. A seeded
subscription is never timed out — the deadline disarms on the first value,
and a deadline that already fired is stale once a value lands (the fan-out
replays its cached `last` even to a subscriber whose signal the deadline
aborted, and that replayed value IS the seed, never followed by a late
504). One coverage nuance on `$live`: core's endpoint arms the same
`timeoutMs` over the whole request up to the stream's FIRST chunk, so on a
connection where NO subscription seeds in time, the request-level 504
answers first and the client sees a connection failure plus backoff. The
per-subscription frame is what bites the multiplex case — a sibling
seeded, the response stayed alive, and only the starved index fails.

**On the cluster, the shelf turned out to be a different mechanism** — and
the 504s are what made it diagnosable (#194). Every per-principal
cross-host stream pins one pooled host-to-host connection for the life of
the subscription, and the measured 100–250 ceiling was the rig's fetch
pool arithmetic (64/peer × 2 relay pods = 128 streams ≈ 190 identities),
not the turn queue: the pump image alone left `max_healthy_identities` at
100, and sizing the pool on the same image took 1 000 identities to zero
failures at the throttle-floor latency. The remaining per-identity costs —
the reads themselves, P change subs, P settle timers, P socket frames, and
above all **one held connection per identity per host hop** unless the read
is declared `principalIndependent` (#138) —
are the steady-state model.

Local control measurement (200 subscribers, #172): 31 actor turns anonymous vs
6 200 per-user for the same publish load; one identity watching a
principal-reading method costs 62 — the split is per *principal*, not per
subscriber.

Never average the two arms. A read only has to touch `ctx.principal` once for
the split — and this entire section — to apply to it.

## Cross-host sharing is opt-in (#138)

The cluster's relay coalesces cross-host watches per
`(actor, method, throttleMs, args, principal)` — `#coalescedWatch` in
`cluster/placement.ts` includes `call.principal` **by default whether or not
the read consults it**, because the relay cannot observe the owner's
discovery. So 10 000 anonymous subscribers across 3 hosts cost 2 cross-host
streams, and 10 000 signed-in ones cost 10 000 — each pinning a pooled
host-to-host connection for the life of its subscription, which is what made
the pool arithmetic the real identity ceiling (#194).

**Unless the method says otherwise:**

```ts
defineActor({
    type: 'ActivityFeed',
    watches: { all: { principalIndependent: true } },
    methods: (ctx) => ({ all: () => [...ctx.state.entries] })
})
```

The relay then drops the principal from that method's key, and the whole
signed-in population shares one stream again — 2 rather than 10 000.

**Why a declaration is safe here when it would not be on `reads:`.** The
promise is policed where the read RUNS, not where it is believed. The
`ctx.principal` getter already knows it is inside a watch invoke (#121's
`kWatchBase` marker); on a declared method it raises
`ActorWatchDeclarationError` instead of recording discovery, and the invoke
wrapper rethrows it so a read body that catches the throw still cannot return
a value. It fails **in every build**, on the owner, whether or not any relay
coalesced — and it does not heal, because the declaration is source code
while the discovery it contradicts is per activation. So failing over does
not clear it; removing the flag or the read does.

Three things the declaration deliberately does not cover, all worth knowing
before using it:

- **`ctx.bag`** is already first-subscriber-only on any coalesced stream,
  declared or not (#137). This is a promise about *identity*, nothing else —
  which is why it is not spelled "caller-independent".
- **Identity reached through `ctx.actor()` into another actor** is invisible
  to the marker, exactly as it is to #121's discovery.
- **A touch that only authorizes** trips it too — #121 marks one touch
  anywhere, even a discarded one. That is deliberate: authorization belongs
  in `authorize`/`methodAuthorize`, which run per subscriber at the entry
  point, outside any turn. Coalescing shares a *read*, never a decision (the
  internal host-to-host mount never re-runs policies — see
  [`wire-and-frames.md`](wire-and-frames.md)).

Two operational notes. A relay that cannot resolve the definition — a
routing-only host, a lazily-registered type, a failed module load — keys
per principal, the conservative direction. And during a rolling deploy a
NEW relay may share a stream that an OLD owner does not yet police; the
shared stream carries no principal at all, so the worst case is everyone
seeing the anonymous view rather than one user seeing another's. Deploy the
declaration before relying on it.

**Dev builds hint at the missing declaration (#221).** The runtime already
holds both inputs — whether a shared read has been observed consulting
`ctx.principal` (#121's discovery) and whether the method is declared (#138)
— so after 25 completed watch reads of an undeclared method with no
consultation, the host warns once per (type, method) with the exact
`watches:` line to add. A hint, never an inference (a conditionally
identity-dependent read must keep its per-principal split, which is why one
consulting read silences the tally for good), tallied on the HOST
(`ActivationHost.onWatchRead`, installed only under `__DEV__`) so
re-activation does not restart the count.

The exact-count invariants are gated by the `cluster/live-fanout` benchmark:
`declared=P/*` (one stream for P identities) beside `undeclared=P/*` (P
streams), both `exact`, so a change that starts sharing an *undeclared* read
fails the check.

## Choosing the fix — the fan-out decision table (#225)

Two Tier-3 sessions (#210 and the TCP follow-on, #203) established that there
are **two independent fixes** for the signed-in live fan-out ceiling, working
by opposite mechanisms: **declaring removes the per-identity streams; TCP
keeps every stream and removes its per-stream cost.** They are not variants
of each other, and they compose. The measured 2×2 — same image, one variable
per axis — lives in `benchmarks/BASELINES.md`
§ "2026-08-13 · Tier 3 — TCP does not remove the streams, it makes them
free", which is the single source for the figures; this table carries the
shape of the result, not the numbers (quoting them here is how they drift).
All four rows were measured at the chart-default pool
(`FETCH_CONNECTIONS=64`) — sizing the pool moves row 1's shelf (#194, and
the sizing rule below), it does not change the shape.

| approach | remote streams | ladder | applies to |
|---|---|---|---|
| undeclared, HTTP | one per remote subscriber, each pinning a pooled connection | shelves at the pool arithmetic (#194) | — |
| **declared** `principalIndependent`, HTTP | a handful for the whole population | clean | identity-blind reads only |
| undeclared, **TCP** | one per remote subscriber, all multiplexed on one connection per peer | clean | any read, incl. identity-dependent |
| declared **+** TCP | a handful | clean | composes |

Which row you can reach depends on what the read does with `ctx.principal`,
and there are **three cases, not two**:

- **The read is identity-blind** — it never consults `ctx.principal` and the
  result is the same for every subscriber. Declare it
  (`watches: { m: { principalIndependent: true } }`, #138). Cheapest fix, no
  infrastructure change: the per-identity stream, and the connection it pins,
  stop existing rather than being budgeted for.
- **The read touches `ctx.principal` only to AUTHORIZE** — the check gates
  access but the value returned is the same for everyone. This is not an
  identity-dependent read; it is authorization in the wrong place. Move the
  check to `authorize`/`methodAuthorize`, which run per subscriber at the
  entry point outside any turn, then declare the read — provided the check
  needs only the principal and the request: those policies are entry-point
  `ServerPolicy`s and cannot see the actor's state, so a membership test
  against `ctx.state` cannot leave the turn and belongs in the third case
  below. Where it applies, it converts an identity-dependent cost into
  an identity-blind one. `ActorWatchDeclarationError`'s message already says
  this — but only to an author who declared and then tripped the check; this
  branch exists for the author who never declared and so never sees the
  error.
- **The read is genuinely identity-dependent** — the *result* differs per
  user. Declaring is forbidden and enforced (`ActorWatchDeclarationError`,
  above), and the per-identity cross-host stream is not a defect to remove.
  `@sigx/actors-tcp` (Node-only) is the structural answer: every stream still exists,
  multiplexed onto one connection per peer, so the pool arithmetic is moot.
  Sizing the host-to-host fetch pool to the watcher population (#194) is the
  HTTP fallback.

This also scopes #194's sizing rule, which otherwise reads as general:
**pool ≥ watchers per hop applies only to reads that genuinely depend on
identity.** A declared read opens no per-identity stream, so there is
nothing to budget for; an authorize-only read should become one.

## Sizing guidance, and the guardrails that pin it

The old ceiling was **~100 distinct principals** on one hot actor — and
it was the host-to-host fetch pool, not the runtime (#194): with the pool
sized to the identity population and the #193 pump in place, **1 000
distinct identities on one actor measured clean** (zero failures, p50 at
the 50 ms throttle floor; the recorded `max_healthy_identities: 500` was
the ceiling of the ladder as it stood that day — the default ladder now
reaches 1000). The durable sizing rule, cheapest option first:

1. **Declare the read** `principalIndependent` if it really is identity-blind,
   or make it so by moving an authorize-only `ctx.principal` check to
   `authorize`/`methodAuthorize` (#138 — the first two cases of the decision
   table above).
2. Otherwise — the read is genuinely identity-dependent and stays on HTTP —
   size the host-to-host pool for the signed-in watcher population:
   per-principal cross-host streams each hold a pooled connection for the
   life of the subscription. The shipped way to size it is
   `boundedFetch({ connections })` on `@sigx/actors/node` (#118), handed to
   `cluster({ fetch })` — scoped to that seam, never the process's global
   dispatcher.
3. Or use `@sigx/actors-tcp` (one multiplexed connection per peer), which
   makes the pool arithmetic moot whatever the reads do.

Concretely:

- Keep genuinely `mine()`-shaped reads — anything consulting `ctx.principal` — off hot
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
  loop/seed/read counts, gated on every PR), `cluster/live-fanout`'s
  `declared=P/*` and `undeclared=P/*` rows (Tier 1, `exact` — the #138
  split), `sockets/principal-cliff`
  (Tier 3, recorded — `max_healthy_identities` is the figure #180 exists to
  move) and the measured tables in `benchmarks/BASELINES.md`. Changes to
  the watch machinery must show those moving, not just unit tests passing.
