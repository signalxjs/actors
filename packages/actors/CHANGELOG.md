# Changelog

## [Unreleased]

### Added

- **Per-request locality counters: `dispatchesLocal` / `dispatchesRemote`**
  (#52). `routedLocal` counts placement decisions and `remoteDispatches`
  counts hop attempts, and neither sees the warm fast path — `dispatcherFor`
  hands back the local dispatcher before the routing loop whenever the host
  already holds the claim — so a fleet whose CPU per request had halved from
  locality routing still read ~0% from `routedLocal / (routedLocal +
  remoteDispatches)`. The new pair on `ClusterCounterTotals` counts once per
  call, stream or watch subscriber, on the warm path and the routed path
  alike (decided at the call's first resolved target), so `dispatchesLocal /
  (dispatchesLocal + dispatchesRemote)` is the locality fraction the docs'
  table promises. Stateless workers and `dispatchOn()` count in neither.
  Existing counters are unchanged; `addCounters` sums the pair with `?? 0`,
  so a mixed-version peer's report cannot NaN the `clusterStats` totals.

- **`onStateError` hook for write-behind save failures** (#54). A
  `write-behind` save has no caller to throw to: the debounced flush failed
  silently, and the final flush at deactivation was a `__DEV__`
  `console.error` at best — so an app learned it had lost a write only by
  polling for faults, or never. `onStateError(ctx, error, phase)` on the
  definition now receives both: `phase: 'flush'` for the debounced save
  (a transient error leaves the state dirty and schedules no retry of its
  own — the next write or the deactivation flush carries it; an etag
  conflict has faulted the activation), `'final-flush'` for the save at
  deactivation, where a failure IS lost data — dead-letter `ctx.snapshot()`
  from there. `ctx.save()` failures still reject the calling turn and never
  reach the hook. Without the hook, dev builds now log the debounced-flush
  failure too, instead of dropping it.

- **`__DEV__` warning for an activation retry storm** (#54). An actor whose
  `onActivate` (or state load, or `migrateState`) throws fails every parked
  caller and is forgotten, so the next caller activates it again — a hot
  loop throttled only by its callers, and nothing in the logs said so. Dev
  builds now warn once per streak when the same actor activates-and-fails
  three times in a row (`Type/key activated-and-failed 3 times in Nms`); a
  successful activation resets the streak.

- **`fetchTransport` retries a pre-response connection failure once** (#55).
  A rolling restart leaves an irreducible residue of connection-level errors
  at the edge: the client writes onto a pooled keep-alive socket in the same
  instant the exiting server retires it, and no server-side drain can win
  that race alone. When the `fetch()` await itself rejects — before any
  `Response` existed — with a connection-level failure (`UND_ERR_SOCKET`,
  `ECONNRESET`, `ECONNREFUSED`, `EPIPE`, or the opaque browser `TypeError`),
  the transport now re-sends the request exactly once. Pre-response is
  provably pre-dispatch, so the retry cannot double-execute a turn; a call
  is never retried after a status has arrived, on a mid-body NDJSON failure,
  or on an abort. ON by default; opt out with
  `configureActors({ retryConnectionErrors: false })`. Socket transports are
  unaffected — on a multiplexed connection a drop still fails in-flight
  calls un-retried (#99).

- **`membersMemo()` and `ClusterPlacement.onChange`** (#269). Every consumer
  that wanted "members, cheaply, on a hot path" hand-rolled a cache and had
  to guess the invalidation key — and the obvious guess, `view().version`,
  is wrong: a provider that expires a peer on the store's TTL clock
  re-allocates the view without necessarily bumping the version (#267), so a
  memo keyed on it latched a stale member count. `membersMemo(placement,
  filter?)` returns a thunk that answers `members(filter)` memoized per view
  OBJECT — the key placement's own derived-data caches have always used —
  recomputing exactly when `view()` hands back a new object. For anything
  else derived from the view, `placement.onChange(cb)` passes the provider's
  membership change stream straight through; it is optional on the
  interface, so a hand-rolled `ClusterPlacement` keeps compiling.

### Fixed

- **A `defineWorker` call no longer carries a routing token** (#148). The
  client minted a token for a worker call exactly as for an actor call, so
  an edge that hashes on it (`upstream-hash-by`, the k8s chart's Ingress)
  pinned every call for one worker key to ONE pod — and a worker is always
  placed locally, so the pool that had won 3.03× on three hosts by being
  spread across all of them collapsed to a single host (`pool_spread` 1.12
  on the same shape). The build now stamps a `worker` flag onto the client
  stub (`__actorRef(type, endpoint, streams, reads, true)`,
  `ExtractedActor.worker`), the proxy threads it through as
  `ActorCallInit.worker`, and `fetchTransport` emits no token — no path
  segment, no header — for such a call in any `route` mode; a
  `.with({ worker: true })` on a stateful actor is ignored, so a caller
  cannot drop an actor's token either. A custom transport that mints its
  own token should honour the same flag. **Caveat — the edge must fall back
  on an empty hash key.** nginx core `hash`, HAProxy `balance hdr(...)` and
  Envoy's ring hash round-robin (or pick at random) when the key is empty,
  so the spread comes back there; ingress-nginx's Lua consistent hash does
  NOT — it hashes the empty string, so an `upstream-hash-by` Ingress like
  the k8s chart's pins EVERY untokened worker call, for every key and type,
  onto one pod. On such an edge either give the hash an empty-key fallback
  (an nginx `map` to `$request_id`) or carve the worker paths out of the
  hashed Ingress by path, the way `$live` already is; #342 tracks the chart.

- **A shared watch read no longer runs under the creating subscriber's
  `ctx.bag` or abort signal** (#137). Watch loops are shared per `(method,
  args, throttleMs)` and re-invoke under the first subscriber's call context.
  #121 split that loop the moment a read consulted `ctx.principal`, but the
  context's per-REQUEST halves stayed the creator's for the life of the
  entry: `ctx.bag` handed a read serving every subscriber the creator's
  request bag (and `ctx.actor()` relayed it onward), and a cross-host hop
  the read made carried the creator's abort signal — so the first subscriber
  leaving failed the in-flight re-read for everyone still attached. Inside a
  shared watch turn `ctx.bag` is now the empty bag and the call's abort
  signal is the watch's own, fired only when the last subscriber leaves. A
  read that needs per-request context is not a shareable read — a plain
  method or a `streams:` entry is per caller by construction. Principal
  handling (#121, #138) is unchanged; this retires the limitation the 0.8.0
  notes recorded.

- **`host.stop()` now waits for a just-completed task's bookkeeping** (#313).
  A detached run left the activation's task table the moment its body
  returned, BEFORE its ledger clear and task-liveness `untrack` ran, and the
  deactivation grace only awaited what was still in the table — so a host
  stopping right after a job finished (a rolling deploy, at scale) left one
  stale task-roster entry per such job for adoption to find later. Launched
  runs are now tracked until their bookkeeping settles, separately from the
  reservation table, and deactivation awaits that set: a stop that lands in
  the window covers the clear, within the same `taskGraceMs`.

- **Timer ticks and task turns now carry the host's `callTimeoutMs`** (#302).
  A `ctx.timer` tick and a task's `ctx.turn` minted a call context with no
  `deadline`; `ctx.actor()` relays the deadline verbatim and the dispatcher
  stamps a default only on an EMPTY chain, so every call made from such a
  turn — cross-host ones included — was unbounded end to end. On the
  workflow rig that was the wedge nothing could break: a turn awaiting a
  cross-host call holds one pooled connection until the peer answers, the
  peer's turns were awaiting calls back through the same pool, and because
  the tick-originated legs had no deadline none of them ever timed out
  (three hosts idle, 50 000 turns queued, `FLUSHALL` + restart the only way
  out). Both contexts now carry `Date.now() + callTimeoutMs` the way an
  external call does, so a call from a tick or a task turn to a peer whose
  turn never answers rejects with `ActorCallTimeoutError`
  (`kind: 'call-timeout'`) at the deadline instead of hanging — the
  RECEIVING host races the remaining budget and answers `call-timeout`, so
  this covers a request that reaches the peer, not a socket that never
  delivers it. `callTimeoutMs: 0` still means no deadline, exactly as
  before.

- **A job's `start()` / `resume()` whose task launch fails no longer leaves
  it `running`** (#316). Both saved `status: 'running'` BEFORE
  `ctx.tasks.start`, so when the launch rejected — task liveness unable to
  reach storage, a host mid-shutdown — the caller saw the rejection while
  the durable record still said `running`, and since that record is the
  ledger (#309) the next activation resumed a run nobody was told had
  started. The transition is now taken back on a rejected launch: the job
  reads `pending` (or `paused`, with the rejected resume's data dropped)
  again, live and on disk, and a retried `start()` / `resume()` goes
  through. The revert is best-effort — if its own save fails, the previous
  at-least-once behaviour stands and the run resumes on the next
  activation. The original rejection is rethrown either way.

- **A reminder whose dispatch fails is retried next tick instead of being
  lost — in the default `shardedReminders()`** (#306). It advances or deletes
  a due entry *before* it dispatches — the at-most-once CAS design — and under
  overload the dispatch is exactly what fails: a call deadline, a host
  mid-restart, an `onReminder` that threw. The entry was already gone, so the
  actor slept past its `wake` for good (131 lost wakes on one overloaded rung
  of the workflow-engine workload). A rejected `deliver()` now re-arms its
  entry one tick out (`nextDue = now + tickMs`: a one-shot is re-inserted, a
  periodic one pulled forward), unless the actor set that reminder again in
  the meantime — a later decision wins. A failed dispatch therefore costs one
  tick rather than the wake, and a target that never answers costs one
  attempt per tick, never a hot loop; a shard's failures go back in one write,
  not one per failure. Two deliberate doubles: a one-shot the actor *cleared*
  while its dispatch was failing may still be retried once (the tick had
  already deleted it, so the clear left nothing for the re-arm to see), and a
  dispatch that timed out *after* `onReminder` had started is retried too —
  so `onReminder` should be idempotent, the assumption every at-least-once
  consumer already makes. Each failed attempt is counted in the new
  `HostStats.remindersUndelivered` (in `host.stats()`, the `ops()` snapshot,
  `metrics()` gauges and the cluster's per-host report), so a fleet that is
  missing wakes says so. **Scope:** the retry and the counter cover
  `shardedReminders()`; a custom `ActorReminders` feeds the counter only if it
  calls the new optional `ActorRemindersContext.undelivered(ref, name,
  error)`, and `pgReminders`, `surrealReminders` and `durableObjectReminders`
  do not yet — on those the wake is still lost and the counter reads `0`
  (#326 tracks them).
- **The `$live` endpoint's watch establishment now has a deadline** (#192).
  The socket session has armed the app posture's `timeoutMs` on watch
  establishment since #180 — pipeline + authorization + dispatch + the FIRST
  value — but the `$live` held-open POST never got the same fix: a
  subscription whose seeding read queued behind a busy actor held its watch
  loop, change subscription and keep-alive open silently, forever. The mount
  now arms the same bound per subscription and answers a per-subscription
  `{i, e}` frame (504, `…timed out before its first value`) when the seed
  starves. The timeout aborts only the starved subscription's own seed —
  sibling subscriptions on the same connection keep streaming, and a SEEDED
  subscription is never timed out (pushes are the watch loop's cadence, and
  the deadline disarms on the first value; a deadline that fired is stale
  once a value lands, so a value the abort races out of the fan-out's
  replay is still a seed, never a late 504 — the socket session's
  establishment deadline carried that stale-`timedOut` flaw since #180 and
  is fixed here too). The prelude and authorization run under the composed
  signal (the socket session's `callContext` shape), so app middleware and
  policies that honor `rq.abortSignal` observe the deadline instead of
  holding establishment open past it. One coverage nuance: core's endpoint
  arms the same `timeoutMs` over the request up to the stream's first
  chunk, so on a connection where NO subscription seeds in time the
  request-level 504 answers first — the per-subscription frame is what
  bites the multiplex case, where a sibling seeded and kept the response
  alive. No posture `timeoutMs` configured means no deadline, exactly as
  before.

- **A fenced host says it is fenced** (#272). Dispatch on a self-fenced host
  no longer degrades to a LOCAL activation attempt. Every branch of placement
  that resolves to self — the `hosts.length === 0` solo path a lost view
  produces, a policy answering self, a stale directory entry naming us — now
  throws the new branded **`ActorFencedError`** (`kind: 'fenced'`, 503 on both
  the public and the host-to-host wire) instead.

  Reported from a split web/engine deployment whose web tier registers only
  workers: after the pod fenced, every call came back as
  `ActorActivationError: unknown actor type "ClientRegistry" — is it
  registered with createHost({ actors })?`. The host registry throws before
  `beforeActivate` runs, so the claim-point fence guard was never reached and
  the caller was handed a registration bug's error for a membership event.
  The new error is also TERMINAL rather than retried: the old path spent
  three attempts, each with a directory lookup and a membership refresh,
  against the store that had just failed.

  A PEER calling into a fenced host still gets `unreachable`, which is what it
  should act on — go somewhere else.

### Added

- **`boundedFetch({ connections })` on `@sigx/actors/node`** (#118). The
  bounded-pool recipe is now code: a `fetch` that forwards every call through
  one undici `Agent({ connections })` as its `dispatcher`, built for the
  cluster's `fetch` seam —
  `cluster({ …, fetch: boundedFetch({ connections: 64 }) })`. Node's default
  pool is unbounded and measured at two sockets per in-flight request
  (`benchmarks/BASELINES.md`, Tier 2); capping it *at* the concurrency halves
  the socket count and is marginally faster (+6%). `connections` is required
  and deliberately has no default — match your host's outbound concurrency:
  the sweet spot moves across undici majors, and capping far below the
  concurrency collapses throughput. `undici` becomes an optional peer
  (`>=6`), loaded lazily on the first call, with install guidance when it is
  absent. Scoped on purpose: only the returned fetch is re-tuned, never the
  process's global dispatcher.

- **A host with nothing to fence rejoins instead** (#272). A host that
  registers no stateful actor type — a `defineWorker`-only tier — holds no
  directory claim by construction, so its membership lapse costs the cluster
  nothing and there is no single-activation invariant for a fence to defend.
  It now re-joins membership under the same identity, retrying with capped
  backoff (500 ms → 30 s) until the store takes it back, and keeps serving its
  pools throughout. Reusing the identity is sound precisely because no
  directory entry can name a host that never claims.

  Unchanged for everyone else: one stateful registration is enough to bring
  back the terminal fence, where a restart's fresh identity is the only safe
  way back. Two counters make the difference visible — `rejoinAttempts` (the
  decision) and `rejoins` (got back in) — beside the existing `selfFences`.

- **Dev builds hint at a missing `principalIndependent` declaration** (#221).
  An identity-blind watch read that is not declared costs exactly what a
  genuinely identity-dependent one costs — one cross-host stream per signed-in
  identity instead of one shared stream — and the only signal used to be the
  scaling wall. After 25 completed watch reads of an undeclared method that
  never consulted `ctx.principal`, the host now emits one `__DEV__`
  `console.warn` per (type, method) naming the exact
  `watches: { m: { principalIndependent: true } }` declaration to add, and the
  `authorize`/`methodAuthorize` escape hatch for reads that touch identity
  only to authorize. A hint, never an inference: the declaration stays yours
  to make, one consulting read silences the tally for good (conditional reads
  stay quiet), and the tally lives with the host so re-activation does not
  reset it. Production builds are untouched — the hook is absent, not inert.

### Changed

- **Task liveness is a per-host roster, not a reminder per running task**
  (#310). A new seam, `ActorTaskLiveness` (`CreateHostOptions.taskLiveness`),
  answers how a dead host's in-flight detached runs are found again. The
  default `rosterTaskLiveness()` keeps one `$sigx:tasks-roster` record per
  host (sub-sharded ×16) plus a small `$hosts` index; the host is its only
  writer, so a start or finish is **one CAS with no load**, group-committed,
  and nothing runs per running task per minute any more. A survivor adopts a
  dead host's roster on the reminder tick — one adopter per dead host, by
  reminder-shard ownership — touching each actor so it resumes wherever
  placement puts it. Detection latency (membership TTL + a tick) is
  unchanged. `reminderTaskLiveness()` is the previous mechanism, kept as an
  option and selected by `createHostDurableObject`, where an alarm IS the
  right liveness. Legacy `TASK_REMINDER` entries left in shard tables by a
  pre-upgrade deployment fire once more and clear themselves. With #309, a
  job run is **1 load, 4 saves, 0 clears** (was 6 / 5 / 1), and a start no
  longer rewrites a 16th of the cluster's running jobs (`jobs/many-running`).

- **A job's state record is its task ledger** (#309). `defineJob` no longer
  writes a `$sigx:tasks` record per run: the run to resume, its input and
  its restart count are derived from the job's own `status`/`input`/
  `attempts`, which it persists anyway. A run lifecycle drops from 6 loads,
  5 saves and 1 clear to **3 loads, 4 saves and 0 clears** (`jobs/lifecycle`,
  exact), and a crash between the `start()` save and the old ledger write —
  which used to leave a job `running` with nothing to resume it — now
  resumes like any other. Crash-resume semantics are unchanged: `attempts`
  still bumps per resume, `maxAttempts` still gives up, pause-resume is
  still free. Plain `defineActor` + `tasks:` keeps the stored ledger; the
  liveness reminder is unchanged for both (#310 is that half).

- **`defineWorker`'s default `maxLocal` now floors at 4** (#147). The default
  pool cap is `navigator.hardwareConcurrency` clamped to **[4, 16]** (was
  [1, 16]; the no-navigator fallback stays 4). Under a cgroup CPU quota
  `hardwareConcurrency` reports the quota, not the machine — inside a
  container with `resources.limits.cpu: 1` it is 1, which silently
  degenerated every default-capped pool to a single activation, making
  `defineWorker` a no-op in the normal production shape. Pool members are
  activations that overlap at `await` points, not threads, so they need no
  second core. An explicit `maxLocal` — including `maxLocal: 1` — is
  untouched.

## [0.9.2] - 2026-08-17

### Added

- **A durable save costs one walk, not two** (#238, #265). `ActorStorage`
  gains an optional `saveText(type, key, json, expectedEtag)`, and the host
  emits the record's JSON directly through `stringifyWithHandlers` from
  `@sigx/serialize/stringify` instead of encoding a tree the adapter then
  re-walks with `JSON.stringify`.

  That second walk was not incidental — it was every real adapter's, because
  what a store wants is a string, and it measured at **+51%** on top of the
  host's own encode (`state/save-growth`, #227). The first wiring (#238)
  measured as a **regression** and was parked: `@sigx/serialize` 0.15.5's
  fused emitter reached the native serializer per NODE, and actor state is a
  large collection of small rows (signalxjs/core#666). Core v0.15.6 batches
  runs of eligible rows into one native call, and the same arms re-measured
  on the bench VM read **−37.7%** (dated rows, 292.1 → 182.9 µs), **−44.4%**
  (scalar rows) and **−39.8%** (`jobs/checkpoint-growth`) against the
  two-walk pair, 7/7 rounds each — a one-walk save now undercuts even
  `memoryStorage`'s store-the-tree-by-reference turn. Needs
  `@sigx/serialize` ≥ 0.15.6; the catalog pins `^0.15.6`.

  **Implementing `saveText` is a promise of equivalence, not just validity**:
  it must be indistinguishable from `save(type, key, JSON.parse(json), etag)`
  — same CAS, same `ActorStorageConflict` brand, same record on the next
  `load()`. `pgStorage`, `redisStorage` and `surrealStorage` implement it and
  define `save` in terms of it, so the two cannot drift.

  **Omitting it is a supported answer and costs only the old path.**
  `memoryStorage` stores the tree by reference and `durableObjectStorage`
  hands a structured value to the platform; for both, a string would force a
  parse back on load. `fileStorage` also declines — its record is a
  pretty-printed `{ etag, state }` envelope, and splicing a pre-made string
  into that would put the whole state on one line, which is the one thing a
  `cat`-able dev store exists for.

  A **storage decorator must forward it** (`decorateStorage`), conditionally,
  so an inner storage without it does not appear to have it. Returning a
  fixed three-method literal silently drops the fast path: correct, quietly
  slower, and nothing says so. The built-in `metrics()` decorator forwards it
  and counts a save made through it identically.

  Two reserved records take the same path. The **task ledger** was the worst
  case in the repo — `JSON.stringify(encode(ledger))` for its no-op
  before-image, the same pair again for the after-image, then the adapter's
  own: five walks per mutation, now two. The **reminder shard table** is
  already JSON-native, so it simply reuses the string its dirty-compare just
  produced: three walks, now two.

  Load is unchanged — `JSON.parse` + `reviveWithHandlers`, off the hot path.

### Changed

- **`@sigx/serialize` peer floor is now `^0.15.6`** (#238, #265), up from
  `^0.15.0`. The save path imports `stringifyWithHandlers` from the
  `@sigx/serialize/stringify` subpath (added in 0.15.5 — a subpath absent
  from the exports map is a hard resolution failure rather than a graceful
  degradation), and 0.15.6 is where its emitter batches runs of small rows
  into one native call — on 0.15.5 this wiring measured *slower* than the
  two-walk pair it replaces (signalxjs/core#666). The floor names the version
  the feature is true on, same reasoning as the `@sigx/reactivity` floor.

## [0.9.1] - 2026-08-16

### Added

- **A live subscription may ask to be served more slowly** (#247).
  `ActorSubscription` gains `throttleMs`, carried on the wire as `w` on
  `LiveSubscription` — one field, so `$live` and the socket session cannot
  drift. The server floors it at `DEFAULT_WATCH_THROTTLE_MS` and rounds it
  **up** to one of a fixed ladder (`DEFAULT_THROTTLE_POLICY`: 50, 250, 1000,
  5000 ms), configurable per mount via `throttlePolicy`.

  Rounding to a ladder is the design, not a detail: `throttleMs` is part of
  the watch identity and of the cross-host coalescing key, so honouring
  arbitrary values would give every distinct number its own watch loop and
  its own remote stream — the sharing #121/#138/#139 exist to create, undone
  by clients asking for 1000, 1001 and 1002. The ladder caps fragmentation at
  `|buckets|` loops however many distinct numbers arrive.

  Why it matters: #245 profiled a delivery at **77% socket write**, and every
  subscriber is its own socket, so a publish costs one `writev` per
  subscriber. Deliveries per subscriber is the multiplier on that, and until
  now it was not the subscriber's to choose — a tile happy to update once a
  second was billed for twenty writes a second. It now costs a twentieth.

  **Nothing changes for a client that does not opt in.** An absent `w` passes
  no `throttleMs` at all, so the watch key is byte-for-byte what it was —
  and because the runtime keys on the resolved window, a request the policy
  answers with `DEFAULT_WATCH_THROTTLE_MS` shares that same loop (under the
  default policy, any `w <= 50`). That is a property of the policy rather
  than of the field: a deployment that raises `min` puts explicit requests on
  their own loop, while clients that ask for nothing keep the runtime
  default. The default policy's floor is
  the runtime's own throttle, so a client can only ever ask to go slower;
  sub-50 ms delivery is an operator decision (`{ min: 0, buckets: [0, 16,
  50] }`), and `{ min: 50, buckets: [50] }` declines the feature. A malformed
  window is refused — per subscription on the socket, per request on `$live`
  — never quietly defaulted.

- **Socket delivery counters, and the HOST's send buffer** (#252, answering
  #208). `socketStats()` gains `deliveries` (the `{i,v}` frames pushed for a
  subscription — not calls, not stream chunks), `deliveryBytes` (code units,
  documented as an approximation) and `throttleQuantized` (subscriptions
  whose requested `w` the policy moved).

  The gauge is the point: `bufferedBytes` reports the host's OWN send-buffer
  depth, summed across open sessions. The session cannot see it —
  `send(message: string)` is the whole contract — so it arrives through a new
  `ActorSocketSessionOptions.bufferedBytes` seam, which
  `@sigx/actors-ws/node` fills from `ws`'s `bufferedAmount`. Until now every
  buffered-bytes figure in the perf record was the CLIENT's, and reading its
  zeros as "the hosts never outran the clients" is exactly what left #182
  unresolved across two Tier-3 sessions.

  `bufferedBytes` is `number | null`, and **null means "no open session could
  answer", never zero** — the same rule `lifetimeMs` already follows.
  `ActorSocketSession.stats()` grows the field to match, which is a change to
  a public shape.

### Changed

- **A socket session no longer re-arms its keepalive on every frame** (#250).
  `reply()` called `clearTimeout` + `setTimeout` per outbound frame, which at
  a fan-out host's delivery rate is tens of thousands of timer-heap
  operations a second on a heap holding one `Timeout` per open connection —
  profiled at 2.60% of busy time, against 0.02% with the ping switched off.
  The keepalive is a deadline rather than a heartbeat, so it is now a
  timestamp plus one self-rescheduling timer: one `setTimeout` per ping
  instead of two heap operations per frame. Same rig with the ping still
  enabled now reads 0.04%. **No observable change** — a ping still goes out
  `pingMs` after the last outbound frame and only if nothing was sent since.

## [0.9.0] - 2026-08-13

### Added

- **`ctx.snapshot(value)`** (#229): deep, detached copy of an arbitrary
  value through the host codec — the same encode+revive a state snapshot
  uses, so custom `types:` handlers round-trip where a `structuredClone`
  would throw or strip them. Made for cloning a subtree of a large state.
- **`job.watch({ throttleMs })`** (#231): per-subscriber coalescing for
  the job feed, forwarding `ctx.changes`'s throttle — at most one
  `JobInfo` per window (leading + trailing, trailing taken fresh), and a
  window still owing an emit at deactivation is flushed, so a throttled
  watcher always sees the terminal info. Omitted = the old one-per-turn
  contract, byte for byte. An unthrottled watcher costs one whole-state
  snapshot per mutating turn; on the `jobs/checkpoint-growth` burst the
  throttled arm measures at the unwatched floor.

### Changed

- **A boundary that both saves and emits encodes the state once** (#233):
  the change-feed snapshot is revived from the same encode the save
  produced (before storage takes ownership of the tree, per the #25
  contract), instead of running a second full `encodeWithHandlers` at the
  same boundary. The default unthrottled `job.watch()` over a
  checkpoint-per-step job — the #124 workload — paid both on every step.
  Ticks-only (`$live`) and window-parked throttled subscribers prepare
  nothing, as before.
- **Job reads no longer clone the whole state** (#229): `status()`,
  `JobControl.info`, `onSettled` and the `start`/`cancel`/`resume` returns
  build `JobInfo` from live scalars instead of `ctx.snapshot()`, which was
  encoding and reviving the entire state — growing checkpoint and result
  included — per read (`jobs/status-read` measured it at 681× across a
  0→2000-row checkpoint ladder; the ladder is flat now). Detachment is
  unchanged: `progress`/`error` are fresh objects, and a declared `extra`
  is codec-cloned as a subtree (a definition without `state:` pays no
  codec work at all). One observable nuance: `progress` is now a shallow
  copy of the declared all-primitive shape rather than a codec clone, so
  undeclared non-JSON values smuggled into it no longer round-trip through
  type handlers on the METHOD path (the `watch()` feed is unchanged).

## [0.8.0] - 2026-08-12

### Added

- **`ClusterPlacement.members()` / `dispatchOn()` and `workerOn()`** (#213):
  enumerate the membership view (active by default, filterable to hosts
  registering a type — the queryable form of `view()`), and invoke a
  stateless worker method ON a chosen member — the `$sigx:host#stats`
  mechanism made public. One attempt by contract: no retry, no route cache,
  no directory; `unreachable` and `wrong-host` propagate branded as answers.
  Self-targets dispatch in-process. Stateful types are refused when
  resolvable — targeted delivery would fight placement over where the
  activation lives. New `targetedDispatches` counter.

### Fixed

- **`addCounters` no longer NaNs `clusterStats` totals against an older
  peer** (#213): fields missing from a mixed-version report now sum as 0.
  Latent since the counters shipped — any counter added in any release
  poisoned every total for the duration of a rolling deploy.

### Changed

- **Placement is registration-aware — default-deny** (#212): a cluster member
  is only ever CHOSEN to host an actor type its app registers, which is what
  makes heterogeneous clusters (different roles registering different actor
  apps) safe to run. Enforced at every decision point, not just the policy: a
  route-cache hint or a live directory entry naming a non-registering host is
  dropped/evicted and the actor re-placed, and the view handed to a
  `PlacementPolicy.choose()` is **narrowed** to the hosts registering
  `ref.type` — so custom policies are registration-safe without changes.
  Contract change for policy authors: the view is never empty, `self` may not
  be in it, and the answer must be a member of it or `self` (anything else
  fails the dispatch loudly, naming the policy; self is always accepted
  because it means 'local', where the fence, claim and registry guard
  authoritatively). `preferLocalPolicy()` on a host that
  does not register the type falls through to rendezvous over the eligible
  view instead of answering `self`. A descriptor without `types` (an older
  build) stays eligible for everything — no flag day in a mixed-version
  cluster. Homogeneous clusters hand policies the identical view object, so
  the fast path is unchanged, allocation and count alike.
- **An inbound cluster call for a type the host does not register answers
  `wrong-host`, not 404** (#212): `resolveClusterSymbol` resolves well-formed
  symbols for unknown types precisely so the refusal can carry the kind (with
  NO owner hint — the refusing host has no idea who owns it), and the caller
  evicts its route and re-places instead of failing on an unbranded 404 with
  a poisoned route cache — the old failure mode when a rolling deploy placed
  a brand-new type on a pod that predated it. The Durable Objects runtime
  (`hostEndpointRuntime`) keeps the 404: no cluster, nowhere to re-place.

### Added

- **`HostDescriptor.types`** (#212): the actor types a host registers —
  workers and lazy types included, gathered without loading lazy modules —
  published in the membership descriptor at join. A typed field rather than a
  `meta` key for the `publicAddress` reason: placement filters on it, so it
  must not be indistinguishable from a free-form label. Round-trips through
  every shipped provider unchanged. Also on `HostReport.types`, so
  `clusterStats()` shows a heterogeneous cluster's registration split.
- **`Host.registeredTypes()`**: every registered type name, sorted, stable
  for the host's lifetime. Optional on the interface so a hand-rolled `Host`
  keeps compiling; a placement treats its absence as a legacy descriptor.
- **`ActorUnplaceableError`, kind `'unplaceable'`** (#212): thrown when no
  ACTIVE host registers the requested type, instead of silently widening
  placement to the full view. Retried through the routing loop against a
  refreshed membership view (the one pod registering a type being mid-join
  IS a rolling deploy), then surfaced as the `cause` of the final
  `ActorActivationError`. Travels the host wire as a 503 with its kind; an
  older peer degrades it to an unbranded error, the safe direction.

- **`ActorErrorKind` gains `'watch-declaration'`** (#138), carried across the
  host-to-host wire like every other branded kind. A peer on an older build
  degrades it to an ordinary error rather than mis-branding it.
- **Watch reads are batched — the per-principal fan-out cliff moves**
  (#180): an activation's serial watch reads now run through a watch read
  pump — the whole watch population holds ONE turn-queue slot (a drain of
  up to 32 reads per turn, each still a full turn of its own for context,
  observability and change boundaries), and a NEW subscriber's seeding
  read drains ahead of every pending re-read. Before this, P distinct
  identities watching one principal-consulting read (#121) put O(P)
  serialized read turns on the queue per publish, and establishment
  starved behind them — measured on AKS as live fan-out collapsing between
  100 and 250 signed-in subscribers on one actor while 20 000 anonymous
  ones held. Steady-state semantics are unchanged: one loop per identity,
  the 50 ms trailing throttle, per-read isolation. The decoded-principal
  memo also became a bounded per-identity map (it thrashed into one decode
  per read turn under interleaving identities).
- **A socket subscription that cannot seed now fails visibly** (#180):
  `createActorSocketSession` arms the app posture's `timeoutMs` on the
  watch path, covering establishment — pipeline, authorization, dispatch
  and the FIRST value. On expiry the subscription answers a per-
  subscription 504 error frame and releases everything the starved seed
  held (fan-out subscriber, shared loop, keep-alive). Previously it hung
  silently forever. A seeded subscription is never timed out. The `$live`
  endpoint has the same gap, tracked as #192.

### Added

- **`watches: { m: { principalIndependent: true } }` — cross-principal
  sharing, restored where it is sound** (#138): the cluster relay keys its
  coalesced cross-host watch on the caller's principal whether or not the
  read consults it, because it cannot observe the owner's per-principal
  discovery (#121). So 10 000 signed-in subscribers across 3 hosts cost
  10 000 cross-host streams even on an identity-blind read — and each one
  pins a pooled host-to-host connection for the life of the subscription,
  which is what made the fetch pool the real identity ceiling (#194).
  Declaring a method `principalIndependent` drops the principal from that
  key, so the whole signed-in population shares one stream. `cluster/
  live-fanout` gates it: `declared=P/*` reads 1 stream and P−1 joins,
  `undeclared=P/*` reads P and 0, both `exact`.

  **The promise is policed, unlike `reads:`.** A declared read observed
  consulting `ctx.principal` fails the watch with the new branded
  `ActorWatchDeclarationError` — in every build, on the owner, whether or
  not any relay coalesced, and rethrown by the invoke wrapper so a read body
  that catches it still cannot return a value. It does not heal on
  re-subscribe or failover: the declaration is source code, while the
  discovery it contradicts is per activation. A relay that cannot resolve
  the definition keys per principal, the conservative direction.

  It is a promise about **identity only** — `ctx.bag` remains
  first-subscriber-only on any coalesced stream (#137), and identity reached
  through `ctx.actor()` into another actor is invisible to it, exactly as it
  is to #121. A touch that only *authorizes* trips it too; that belongs in
  `authorize`/`methodAuthorize`, which run per subscriber outside any turn.
- **Watch-loop observability** (#180): `HostStats.watchLoops` — a live
  gauge of shared watch loops across activations, flowing into `metrics()`
  gauges, `ops()` and the cluster `HostReport` with no new plumbing
  (optional on the wire, so mixed-version fleets keep parsing) — and
  `ActivationInfo.watchLoops`/`watchSubscribers`, so `activations()` (and
  anything built on it, like `sigx actors top`) can show which actor holds
  the loops. The per-principal watch split (#121) is one loop per distinct
  identity; a loop count tracking the subscriber count on a hot actor is
  the fan-out collapse of #180 building, and until now it was visible only
  from a collapsed deployment.

## [0.6.0] - 2026-08-09

### Added

- **`socketStats()` on `@sigx/actors/server`** (#166): socket observability
  as the repo's counter discipline prescribes — one flat totals object per
  host/listener (connections opened/closed/refused, calls started/failed,
  subscriptions opened/closed, protocol breaches, lifetime closes), live
  gauges summed from open sessions (`open`, `inFlight`, `subscriptions`),
  and a connection-lifetime histogram in the runtime's own log-linear
  layout so digests merge like every other histogram. Sessions record into
  it via the new `ActorSocketSessionOptions.stats`; the app publishes it as
  an ops section (`registry.reportOps('sockets', () => stats.snapshot())`),
  riding the ops endpoint's existing bearer posture with no endpoint
  changes. Prometheus/OTel/CLI rendering is the follow-up half of #166.
- **Socket sessions no longer outlive their credentials** (#159):
  `createActorSocketSession` gains `revalidateMs` — re-run authentication
  against the pinned upgrade request on a fresh context every interval, and
  close 1008 when it no longer stands (authenticate throws, an
  authenticated connection comes back anonymous, or the identity changes;
  presence is compared even with no principal codec configured) — and
  `maxConnectionMs`, a hard lifetime cap that doubles as the
  credential-refresh mechanism: the reconnect is a fresh upgrade carrying
  the browser's CURRENT cookies, subscriptions re-establish, in-flight
  calls fail un-retried as on any drop. Both default 0 = off and are
  validated at construction. Guards grew `actorPrincipal()` (the presence
  check, through the same single door as the rest of the pipeline).

- **Incremental live over the socket** (#99): the session speaks
  `{i,sub}`/`{i,uns}` — one small message per subscription-set change,
  ending the reopen-and-reseed storm that `$live`'s held-open POST cannot
  escape. Per subscription it runs the same sequence the `$live` endpoint
  runs (definition → full prelude + authorization on a per-message context →
  `dispatchWatch`), failure stays per subscription, the id namespace is
  shared with calls (replies carry only `i`), and the per-connection
  `maxSubscriptions` cap is live. The client machinery both channels share —
  identity coalescing, late-subscriber replay, `fingerprint()` re-seed
  suppression, and `createSocketLiveChannel` itself — moved to
  `app/live-shared.ts` and is published through `@sigx/actors/socket-wire`,
  so the two channels (and any out-of-repo transport) cannot fork it.
  `useActorState` and the `$live` channel's behaviour are unchanged — the
  existing live suites pin that.
- **`createActorSocketSession()` on `@sigx/actors/server`** (#99): the server
  half of the client-facing socket transport — one session per connection, a
  `Request` in and two callbacks out, so `ws`, socket.io, uWS, Bun, Deno and
  a Durable Object all drive the same core. Origin checked at upgrade
  (`posture.origin ?? 'same-origin'` — a browser opens a cross-origin
  WebSocket with cookies attached and no preflight), identity pinned once at
  upgrade (cookies-only v1), the FULL prelude re-run per message (middleware
  may be a rate limiter; authentication itself stays memoized on the
  connection), unary + stream + cancel with per-call deadlines from
  `posture.timeoutMs`, and the caps HTTP gave implicitly repaid explicitly:
  `maxMessageBytes` (close 1009), parse-or-close-1003, `maxConcurrent` (429
  per call — over one socket a page can hold thousands of in-flight calls,
  each able to force an activation), `maxSubscriptions` validated at
  construction. Live subscriptions answer 501 until the incremental-live PR
  lands. Guards grew `enterActorRequest()`/`actorPosture()` so the session
  reaches core's pipeline through the same single door as every transport.
- **`@sigx/actors/socket-wire`** (#99): the published vocabulary of the
  incoming client-facing socket transport — `SocketRequest`/`SocketReply`
  (whose `{i,v}`/`{i,e}` reply shapes are the `$live` `LiveFrame` shapes by
  construction), plus the codec (`encodeWire`/`reviveWire`), the
  pollution-safe `parseWire` and branded `wireFail`, so an out-of-repo
  adapter reaches the one true wire instead of copying it — the same
  standing `./cluster/frames` has. Deliberately NOT the cluster frames: the
  vocabulary has no field for a principal, no envelope and no inbound-call
  direction, which is the browser trust model expressed as a shape.

### Changed

- **Cross-host watches coalesce: live fan-out now scales with hosts, not
  subscribers** (#111). n local subscribers to a remote actor's read share
  ONE cross-host stream per `(actor, method, throttleMs, args, principal)`
  and fan out locally, so the owning host does one serialized write per
  emission instead of n — the singleton-subscriber write ceiling measured
  on AKS moves with fleet size instead of against it. Two consequences,
  both deliberate:
  - The shared stream is pulled at the fastest consumer's rate; a
    subscriber slower than the feed drops oldest values at a 16-value
    buffer. A stalled tab can no longer backpressure the stream — nor
    anyone else on it — and a live read's superseded values are worthless
    by definition.
  - A shared-stream failure fails every subscriber on it and drops the
    entry — exact parity with the per-subscriber path it replaces, whose
    retry only ever covered the first pull. The `$live` channel's
    reconnect-and-reseed is the recovery path, unchanged. Shared
    re-establishment is future work.

  The key carries the encoded principal (the owner splits
  identity-dependent reads per principal, #121, and the relay cannot see
  that discovery), so distinct authenticated users do not yet share a
  stream — #138 tracks restoring that for identity-independent reads.
  `remoteWatches` now counts remote watch STREAMS (per attempt) rather
  than subscriber attaches; the new `coalescedWatches` counter counts
  attaches that joined an existing stream. The `@sigx/actors/cluster`
  size budget rises 13.2 → 14.2 KB to carry the shared fan-out core
  (see #73 for the budget process).

### Fixed

- **`LiveChannel.close()` now releases the delegate channel it resolved**
  (#102). A transport that brings its own push channel (`transport.live()`)
  may build a real connection; `close()` nulled the reference without
  closing it, which becomes a leak the day a transport ships `live()` — the
  bare `configureActors(socketTransport(...))` path, where no plugin owns
  the transport. The release goes through the transport that produced the
  delegate, only when one was actually resolved, and
  `ActorTransport.close()` is now documented as idempotent by contract so
  the plugin-owned path cannot double-free.
- **A shared watch no longer serves every subscriber the FIRST subscriber's
  `ctx.principal`** (#121). Watch loops are shared per `(method, args,
  throttleMs)` and re-invoke under the first subscriber's call context, so a
  live read that filtered by `ctx.principal` handed every later subscriber
  the first subscriber's view — confidentiality between authorized users,
  single-host and across the cluster alike.

  The fix keeps the sharing property where it is sound: the runtime observes
  whether a read actually consults `ctx.principal` and only then splits that
  key's loop per encoded principal. A read that never touches identity still
  costs one re-invocation per turn however many subscribers it has;
  subscribers of an identity-dependent read each see their own view, and
  same-principal subscribers still share. Discovery evicts a mismatched
  subscriber *before* the discovering invocation's value is pushed — no
  subscriber ever receives a value (or error) computed under someone else's
  identity — and the evicted subscription transparently re-attaches under
  its own key; callers observe nothing. No API change.

## [0.5.0] - 2026-08-07

### Added

- **`ctx.changes({ throttleMs })`** (#129) — coalesce a burst of mutating
  turns into at most one snapshot per window. Leading edge plus trailing edge,
  and the trailing snapshot is taken fresh when the window closes, so a
  throttled consumer is never handed a state older than the window it waited
  out. A boundary that lands inside an open window builds **no snapshot at
  all**, which is the saving: a snapshot is a full encode+revive of the whole
  state.

  For an actor whose state grows through a run — a job appending a step's
  output per turn and reporting progress as it goes — this is the difference
  between cloning everything accumulated so far on every step and cloning it
  once per window.

  The final state is never lost: a window still owing an emit when the actor
  deactivates is flushed before the feed ends. A malformed `throttleMs` throws
  rather than silently reading as "unthrottled". Omit it and behaviour is
  exactly as before — a snapshot per mutating turn.

### Fixed

- **A `$live` watch no longer builds a snapshot it never reads** (#129).
  `ctx.changes()` yields state, but a watch (`useActorState(…, { live: true })`
  and every wire watch) re-invokes a read method instead — its pump reads only
  the iterator's `done` flag. So the runtime was running a full encode+revive
  of the entire actor state on every mutating turn and discarding the result.
  Such a subscriber now receives a value-free tick, and a boundary whose
  subscribers all want ticks builds nothing.

  | `streams/live-watch` | before | after |
  |---|---:|---:|
  | 200-row state | 3.0 k ops/s | **9.7 k ops/s** (+224%) |
  | 2 000-row state | 216 ops/s | **696 ops/s** (+222%) |

  At 200 rows that is 9.7 k against 10.1 k for the same actor with **no
  subscriber at all** — the marginal cost of a live subscriber is now
  approximately nothing, where it was 3.3×.

  No API change and nothing observable: the values a watch delivers are
  unchanged, because they never came from the feed.

## [0.4.0] - 2026-08-07

### Added

- **`onSettled` on `defineJob`** (#125) — a hook for every terminal transition,
  including the two a run body structurally cannot observe: the runtime's
  `maxAttempts` give-up (it refuses the restart, so no body turn happens) and a
  `cancel()` that lands while the job is paused (no task to abort). An app
  projecting job status outside the job — a status row in its own database, a
  metric, a notification — previously had no way to learn about either, so the
  projection asserted "still running" forever. Fires for `completed` too: a hook
  covering only some terminal transitions would leave the handler needing to know
  which ones it must also cover from the body. Runs inside the settling turn
  after the state save, so a throwing handler is caught and dev-warned rather
  than unwinding a transition the runtime has already committed.

### Changed

- **Change detection now calls reactivity's own deep traversal instead of a
  copy of it (#124), and `@sigx/reactivity` 0.15.3 is the new floor.** The
  host learns "state is dirty" by walking `ctx.state` inside a
  scheduler-deferred effect. That walk was a private `trackDeep` mirroring
  upstream `traverse`, under a comment warning that divergence here would be
  divergence in what counts as a change — and it diverged: upstream stopped
  enumerating the reactive proxy (signalxjs/core#642), then stopped reading
  keys back through it at all (core#645, one any-write dep per object), while
  the copy stayed on the original algorithm.

  It is now `deepTrack` from `@sigx/reactivity/internals`, exported for this
  caller in core#651. `watch(…, { deep: true })` remains unusable here —
  `WatchOptions` has no `scheduler`, and parking the re-run so the walk folds
  once per turn boundary is the whole design.

  **Nothing observable changes**: the same writes mark the same actor dirty at
  the same boundaries, and the pins in `dirty-tracking.test.ts` are untouched
  — including the one that matters most, an object added in turn N and
  mutated in turn N+1 still emitting.

  What changes is the cost, which was severe on anything but small state. One
  mutating turn over 200-row state, with no subscriber and no write involved,
  measured **~1.2 ms**. Same machine, back to back:

  | scenario | before | after |
  |---|---:|---:|
  | `state/dirty-size` 200 rows, no subscriber | 1.0 k ops/s | **10.1 k ops/s** (+896%) |
  | `state/dirty-size` 2 000 rows, no subscriber | 88.9 ops/s | **714 ops/s** (+703%) |
  | `state/dirty-size` 200 rows, 1 subscriber | 741 ops/s | 3.0 k ops/s (+308%) |
  | `state/dirty-growth` tail turn (job actor, 500 steps) | 2 183 µs | **666 µs** (−69%) |

  The cost still scales with total state size rather than with the size of the
  change, because a boundary also pays `#snapshot()` — a full encode+revive —
  whenever anyone is subscribed. With the walk fixed, that is now the larger
  of the two: at 200 rows a subscriber costs 70% of the turn, against 26%
  before. Tracked separately.

- **The README is now a pointer to https://sigx.dev/actors** rather than a
  second copy of the manual (#113). It had grown to 2,632 lines duplicating
  the docs site page for page, and had drifted: seven confirmed factual errors
  against the source, including a claim that guards and stream bodies can
  re-enter a turn via `ctx.turn()` (only task bodies can), a
  `clusterPlacement({ definition })` option that does not exist, and an
  entry-points table missing the published `./cluster/frames` subpath. Nothing
  was lost: the user-facing material is on the docs site, and the seams,
  invariants and rationale a maintainer needs now live in `docs/architecture/`.
  No code or API change.

## [0.3.0] - 2026-08-05

### Added

- **A schema-bootstrap conformance suite** (#78), for contributors — **not a
  published import**. `bootstrapConformance` is the set of cases a provider's
  `ensure…Schema` must pass, plus the harness interface the provider supplies.
  It lives at `packages/actors/src/testing/` and is reachable inside this
  workspace as `@sigx/actors/testing` via a tsconfig/vitest alias; the subpath
  is deliberately absent from `package.json` exports, exactly like
  `@sigx/actors/cluster/testing`, so it cannot be imported from outside the
  repo until it is promoted. Three cases — a bootstrap leaves storage usable,
  bootstrapping twice is a no-op, and N concurrent bootstraps from independent
  connections all converge — the last being #76/#78 as a runnable assertion.
  It asserts the OUTCOME rather than the mechanism, because the two providers
  that run it converge differently (an advisory lock in Postgres; jittered
  retry in SurrealDB, which has no lock primitive). The `ActorStorage`
  conformance suite of #65 belongs beside it and composes with it:
  `storage()`/`stop()` are the shared intersection and `bootstrap?()` is
  optional.


- `memoryClusterHub().expire(hostId)` — drop a member the way a TTL lapse
  does: no cleanup, and the victim is never told (no `onSelfSuspect`),
  unlike `kill()`. The test seam for #45-shaped scenarios.
- `heartbeatClock().lost()` — for a provider that holds PROOF its record is
  gone (a Kubernetes Lease deleted under it) rather than a suspicion
  inferred from elapsed time. Fires immediately, whatever the clocks say,
  since the next write would succeed promptly and look healthy (#69).
  Latched like the rest; cleared by the next `confirmed()`.

### Changed

- **BREAKING: actor URLs no longer spend percent-escapes** (#96), matching
  the grammar `@sigx/server` adopted in core 0.15. The wire symbol's
  separator is now a **real path separator** and encoding is per segment,
  so `@`, `$`, `:`, `.`, `-`, `_` and `~` survive literally:

  ```
  before  POST /_sigx/actor/r/1xadm0a/Cart%23addItem
          POST /_sigx/actor/%24live%23subscribe
          GET  /_sigx/actor/Product%23price?args=%5B%22p-9%22%2C%22EUR%22%5D
          POST /_sigx/host/Cart%23addItem

  after   POST /_sigx/actor/r/1xadm0a/Cart/addItem
          POST /_sigx/actor/$live/subscribe
          GET  /_sigx/actor/Product/price?a0=p-9&a1=EUR
          POST /_sigx/host/Cart/addItem
  ```

  A type may hold slashes of its own (`acme/greeter`), so the symbol can
  span more than two segments and the **last** one is always the method —
  the same `lastIndexOf` rule the runtime already used for `#`. Declared
  `reads:` now send all-scalar arguments as named params (arg 0 is the actor
  key); one richer argument and the whole call falls back to `?args=<JSON>`,
  all-or-nothing so the cache key stays a pure function of the arguments.
  That grammar is core's verbatim, deliberately: the same declared read must
  decode identically on `/_sigx/fn` and `/_sigx/actor`.

  **The in-memory symbol is unchanged.** It is still `` `${type}#${method}` ``
  everywhere it is not a URL — the per-call HMAC signs it, `ServerFnInfo.symbol`
  reports it to a policy, and the frame transports (`@sigx/actors-tcp`,
  `@sigx/actors-ws`) carry it with no URL involved. Only the URL spelling
  changed.

  **⚠️ Upgrade order matters.** Servers accept both spellings (they must: one
  resolver serves URL and frame transports alike), so a cached old client
  bundle keeps working against a new host. The reverse does not: a new client
  or a new cluster peer talking to an **old** host 404s, and over
  `/_sigx/host/` it 403s, neither of which is classified retryable. **Upgrade
  every host before any client or peer starts emitting** — during a rolling
  cluster upgrade, hosts still on the previous version will refuse calls from
  upgraded ones.

- **A method name containing `/` is now refused** (#96). Escaping it does not
  round-trip: `Type#a/b` encodes to `Type/a%2Fb`, the reading half decodes each
  segment before it splits on the last separator, and the symbol comes back as
  `Type/a#b` — a different actor, silently. Refused at the encoder, the one
  chokepoint every wire path goes through, in production as well as dev.

- **`defineActor` refuses a type with an empty, `.` or `..` path segment**
  (#96). The type's slashes are wire path separators now, and `new URL()`
  resolves dot segments away — so such a type would silently RETARGET its
  route rather than 404. Refused at definition time, the only place it can be
  loud. `acme/greeter` is unaffected.

- **BREAKING: actor proxies answer introspection props locally instead of
  dispatching them.** Every proxy (`actor()` in the browser and on the
  server, `host.actor(...)`, `ctx.actor(...)`) manufactured a live
  dispatcher for ANY string prop, so `String(ref)`, `JSON.stringify(ref)`
  or a library probing `.constructor` silently issued a real call. Now
  `toString` reads `[actor Type#key]` — refs interpolate usefully into
  logs — and every other `Object.prototype` name (plus `toJSON` and Node's
  legacy `inspect`) reads `undefined`, exactly like symbols and `then`.
  The breaking edge: an actor method NAMED like an `Object.prototype`
  member (`toString`, `valueOf`, `hasOwnProperty`, `toJSON`, …) is no
  longer reachable through a proxy — rename it. Wire dispatch of such
  names is refused by the server exactly as before.

### Fixed

- **A secured host-to-host call to an actor whose type contains a slash
  always 403'd** (#96). The internal mount's HMAC pre-check read the symbol
  as the LAST path segment, which is only the whole symbol when the type has
  no slash: for the packaged-actor convention this README recommends
  (`acme/greeter`) it recovered `greeter#greet` while the sender had signed
  `acme/greeter#greet`, so `verifyAuth` rejected every call under
  `cluster({ secret })`. It now computes the symbol exactly the way core's
  `decodeFnPath` does — per segment — which is what the resolver a moment
  later sees. A mount that worked against every test fixture and failed on
  the one naming convention the docs recommend.


- **The Node mount now strips the routing token from the path, so a routed
  call behaves exactly like a direct one (#93).** `route: 'hash'` is the
  client default, so every call through `@sigx/actors/client` arrives as
  `{base}/r/{token}/{Type}%23{method}`. `handleActorRequest` (the WinterCG
  mount) rewrote that back to `{base}/{Type}%23{method}` before core decoded
  the path, but `createActorHandler` — and therefore `createAppHandler` and
  the Vite dev middleware, i.e. every Node deployment — handed core's adapter
  the request untouched. Core then read `r/{token}/{Type}#{method}` as the
  SYMBOL, which resolves to the unknown-actor wrapper: a 404 for an
  authenticated caller, and a **401 for an `allowAnonymous` actor**, because
  that wrapper carries no `__sigxAnon` and core's identity gate runs before
  any wrapper is invoked. The same actor answered 200 on the direct path and
  401 on the routed one — the transport asymmetry the endpoint comment says
  v4 exists to prevent, and invisible in-process (`peekHost()` short-circuits
  before the wire). The strip is now one shared `stripRoutePath` both mounts
  call; on the Node side it rewrites `req.url`, whose query string (a
  declared `reads:` GET carries its arguments there) rides along untouched.
  The workaround — `configureActors({ route: 'none' })` — is no longer
  needed.

- **An in-chain stream open against a call-chain-reentrant target no longer
  hangs (#46).** `dispatchStream` and `dispatchWatch` ran the reentrancy check
  and discarded its result, so a `reentrant: true | 'call-chain'` target
  *admitted* the cycle and then queued the stream's setup turn on its serial
  lane — behind the very turn that was up-stack awaiting the first chunk.
  Neither could proceed. `A → B → A.someStream()` now re-enters: the setup
  resolves the generator inline, exactly as an awaited in-chain call does.
  Sound rather than merely expedient — the setup sets the call context,
  resolves the generator and restores it with no `await` in between (invoking
  an async generator function runs none of its body), so no other turn can
  interleave. Iteration was already detached from turns, and is unchanged.

  A **watch** has no inline form — it is a long-lived subscription whose reads
  are turns of their own, so only its first read could ever run inline — and an
  in-chain open now **throws** `kind: 'deadlock'` instead of hanging. No `ctx`
  API reaches `dispatchWatch`, so this is not reachable from an actor body
  today; the refusal is what keeps a future entry point carrying a call chain
  from hanging silently.

  `reentrant: 'always'` was never affected: its turns are not chained on the
  serial tail in either direction.

- **A host that stalled past the membership TTL kept serving actors a
  survivor had already taken over — single activation was violated (#45).**
  Block a host's event loop for longer than `ttlMs` (a 20 s GC pause, a
  suspended container) and its peers expire it, sweep its directory claims
  and re-activate its actors elsewhere. The stalled host, meanwhile, saw
  nothing fail: its heartbeat resumed, wrote late, and SUCCEEDED. Its
  activations stayed live and kept accepting writes, and
  `/_sigx/health/ready` kept answering 200.

  Self-fencing only ever triggered on a heartbeat write *rejection*, so
  there was no failure to observe. It now judges the GAP instead of the
  outcome, in two places:

  - **Providers** share a new `heartbeatClock()` (exported from
    `@sigx/actors/cluster`): presence is only assumed until
    `lastConfirmedWrite + ttlMs`, and a beat starting past that fires
    `onSelfSuspect` before it writes. A write that *started* in time but
    took longer than the TTL to come back fires too — the stall on the
    store's side rather than ours, and equally invisible, since a client
    cannot tell when its write actually landed. It watches the monotonic
    **and** the
    wall clock, because `CLOCK_MONOTONIC` does not advance across a VM
    suspend — and `setTimeout` rides that same clock, so a suspended host
    would otherwise think every beat was punctual. The window is stamped
    when the beat is armed, not at the join write, so a slow join cannot
    fence a host at startup.
  - **Placement** self-fences when it finds itself absent from its own
    membership view, confirmed against a fresh `refresh()` first. Guarded
    so it cannot misfire: an EMPTY view still means solo/not-started (a
    store failing over to a cold replica must not fence the whole cluster),
    and absence only counts once this host has been seen in a view.

  Fencing now also withdraws the host from membership, so peers stop routing
  to a host that refuses every activation instead of waiting out their own
  TTL. Fenced remains terminal and still fails liveness — the orchestrator's
  restart is the way back.

  `ClusterMembership` gains a documented requirement it always relied on: a
  live host must appear in its own `view()`.

## [0.2.0] - 2026-08-05

### Changed

- **BREAKING: the guard split — `use` / `methodUse` / `unguarded` become
  `authorize` / `methodAuthorize` / `allowAnonymous` (#17, rfc-server-v4
  §7).** Core 0.15 split its single guard primitive into **middleware**,
  **authentication** and **authorization** and made the runtime
  **fail-closed**; actors follows, pinned `@sigx/server@^0.15.0`. One
  conceptual change across two repos — the migration guide owns both
  tables: [`docs/migrations/0.15-guard-split.md`][guard-split] in
  `signalxjs/core`.

  Three things it buys, in order of how much they matter:

  - **Policies see the INSTANCE.** A policy is
    `(principal, rq, op) => boolean` and `op.resource` carries
    `{ kind, type, key, method }`, so the dominant real actor policy —
    per-instance, *"may this user read cart `u_123`?"* — is expressible for
    the first time. A pre-v4 guard received `(rq, { symbol, name })` and
    the actor key was peeled off the wire arguments only after it had run.
  - **Identity propagates by construction.** The authenticated principal
    rides a first-class slot on the call envelope, never a bag key: it
    cannot be forged through `.with({ bag })`, and it cannot be dropped by
    a guard author forgetting `stampCallBag`. Actors read it as
    `ctx.principal` — decoded lazily, memoized, and carried unchanged
    through `ctx.actor`/`ctx.publish` hops and host-to-host, so a
    downstream actor sees whoever entered the system rather than the actor
    that called it. Requires `codec` on `createServerApp`; without one it
    propagates nothing and dev-warns once, which is fail-closed at the
    reader. `stampCallBag` survives for app DATA.
  - **Jobs authorize at ENQUEUE, and the run reads a snapshot.** A job
    outlives the request that started it — a crash-resume can happen on
    another host hours later with nobody waiting — so `start` is the entry
    point that decides, and the detached run body reads `job.principal`
    (persisted with the run) rather than re-authorizing. `ctx.principal` is
    null inside a task body by design, which is exactly why this exists.
  - **App-wide auth is one line.** `sigxActors({ serverApp })` satisfies
    the build gate for actors that declare nothing, because the app's
    default policy is then the answer. `examples/chat` declares
    authorization on **zero** actors where it previously repeated
    `use: [requireUser]` on every one.

  Also in the split: `sigxActors({ requireGuards })` →
  `requireAuthorization`, and its question is sharper — `use: [logRequest]`
  used to satisfy the old gate, because one primitive meant logging and
  access alike. The registration warning flips polarity ("every call will
  be DENIED") and fires only when no server app is configured. The
  definition-time `unguarded` + `use` contradiction throw is **removed with
  no analog**: `allowAnonymous` + `authorize` is coherent — the identity
  gate is waived and the declared policy still decides, against a nullable
  principal. `info.transport` (`'wire'` | `'in-process'`) replaces core's
  old `symbol === ''` discriminator, which actors never honoured, while
  keeping `Type#method` identity on both transports.

### Fixed

- **Actor wire calls 404'd once core enforced the mount base
  (signalxjs/core#563).** `handleActorRequest` passed `base` to its resolver
  but never to `handleServerFnRequest`, so core matched the path against its
  own `/_sigx/fn` default. Latent until core made the prefix load-bearing,
  then fatal for **every** actor call. The internal cluster mount and the
  Cloudflare Durable Object mount had the same omission.
- **A routing token was read as part of the actor type.** Under "everything
  after the base IS the symbol" (signalxjs/core#543),
  `{base}/r/{token}/{Type}#{method}` resolved a type literally named
  `r/{token}/{Type}`. The hint is now stripped from the RAW path, before core
  decodes it: core decodes each path segment separately, and a `'key'`-mode
  or custom token may contain a slash — as may an actor type
  (`acme/greeter`) — so after the decode the two are indistinguishable.

[guard-split]: https://github.com/signalxjs/core/blob/main/docs/migrations/0.15-guard-split.md

- **State-change detection no longer walks the state on every mutation**
  (#28). The change feed and write-behind persistence used a
  `watch({ deep: true })` whose full deep traversal ran per mutation to
  learn one bit — 13% of the `state/*` CPU profile and the cause of the
  change-feed first-subscriber cliff. A scheduler-deferred effect now flips
  a dirty bit synchronously at write time, and the one deep walk happens at
  the turn boundary (re-subscribing anything the turn added). Observable
  behaviour is unchanged: one snapshot per mutating turn, `onActivate` and
  out-of-turn mutations still detected, deactivation flush still covers
  writes with no turn behind them.

- **`ActorStorage.save()` now takes ownership of its `state` argument**
  (#25) — the caller must not mutate the tree after the call, and an
  implementation may store it by reference. `load()`'s half of the contract
  is now explicit too: the returned record is the caller's to mutate.
  `memoryStorage` accordingly stops `structuredClone`-ing on save (the host
  always passes a codec-fresh tree; the load-side clone stays, so a stored
  value still never aliases live activation state). Only storage
  implementations that retain the argument by reference AND callers that
  mutate a tree after saving it are affected — no in-repo provider or caller
  was.

- **`refreshCoalescer()` on `@sigx/actors/cluster`** (#26): the
  notification→refresh coalescing primitive membership providers share —
  leading-edge immediate, single-flight, version-gated hint skipping, with
  `demand()` preserving `ClusterMembership.refresh()`'s
  started-at-or-after contract and `settled()` for teardown. Wired into
  `@sigx/actors-redis`, `@sigx/actors-pg` and `@sigx/actors-surreal`, whose
  subscriber paths were the measured O(N²) (a burst of N changes cost N
  full member-list re-reads per subscriber).

## [0.1.0] - 2026-08-03

### Changed

- **BREAKING: `Mailbox` is now `Turns`** (#284) — the class exported from
  `@sigx/actors/host`, its module (`host/mailbox.ts` → `host/turns.ts`), and
  the `Activation.mailbox` field (now `Activation.turns`). No alias; nothing
  has shipped.

  There was never a mailbox: nothing holds messages, turns chain onto a
  promise, and `depth` is a counter rather than a length. The name also had
  to be explained every time it appeared, which is the surest sign it was
  wrong.

  `Turns` needs no explanation — a turn is already the unit the docs use, so
  the prose says what it means directly: *turns serialize*, *its own turns*,
  *outside a turn*, *holding the activation*. Most sites lost the noun
  entirely rather than swapping it, and three paragraphs of disambiguation
  went with it.

  The docs moved with the code — README, `types.ts`, `ops()` descriptions,
  `SECURITY.md` and the OTel metric HELP strings — and the README gains a
  short **Turns** section, since one-turn-at-a-time is the single thing worth
  understanding about why `ctx.state` needs no lock.

  Wire format, storage and the `queued` ops field are unchanged. In the
  benchmarks the scenario `dispatch/mailbox-raw` is now `dispatch/turns-raw`,
  so a saved local baseline will not match that one scenario until
  re-recorded.

- **The unguarded-actor warning is no longer `__DEV__`-gated** (#277). The
  Vite `requireGuards` build error is the real safety net, but it only sees
  first-party source and only exists if you use the plugin: a plain
  `createHost` + `createActorHandler` deployment, `requireGuards: false`,
  and any actor arriving from a third-party package all bypass it. Gating
  the runtime warning on `__DEV__` stripped it from exactly the build
  where those deployments would need it. It costs one string per
  registered type, once, at startup.

- **Security: `$live` now caps the subscription array**, at 256 by default
  (#277). The mount fans out one `dispatchWatch` per entry, started at
  once, and each can force a distinct activation pinned for `idleAfterMs`;
  the per-subscription guards run *inside* that fan-out and so never
  bounded it. A minimal entry is ~25 bytes, so the 1 MiB default body cap
  bought tens of thousands of activations from one unauthenticated
  request. An over-cap array is now a `400` for the whole request, checked
  before the per-entry walk — answering per index would mean doing the
  work first. Tune with `handleActorRequest({ maxLiveSubscriptions })`,
  which also flows through `createFetchHandler` and `createAppHandler`;
  `0` disables it. The option joins the resolver cache key, so two mounts
  on one host with different caps no longer share whichever resolver was
  built first. Anything that is not a non-negative integer throws: under a
  plain `> 0` test a typo like `-1` silently turned the cap off, which is
  the exact state the option exists to prevent.

- **`@sigx/actors/cluster`'s size budget moves 12 → 12.5 kB** (#277). The
  `cluster()` secret check throws outside `__DEV__`, so unlike a
  `__DEV__`-gated warning its message survives into the production bundle —
  about 200 B of English. That is deliberate: it is the one place a
  misconfigured deployment is told its internal mount would be
  unauthenticated, and `./cluster` is a server entry, not a browser payload.
  Shorten the message before raising the budget again.

- **BREAKING (security): `cluster()` now requires a `secret` outside
  development** (#277). The internal host-to-host mount runs no guards —
  the public edge is supposed to have run them — and it is contributed as
  an ordinary route, so it rides the same listener as the public actor
  endpoint. Omitting the secret did not mean "clustering without auth": it
  left every registered type, key and method reachable unauthenticated,
  with the `use:`/`methodUse:` chains bypassed and no same-origin
  backstop, including the `$sigx:*` symbols the public mount deliberately
  refuses. `ops()` has thrown on the same omission since it shipped, and
  this exposes strictly more. `cluster()` now matches that posture: it
  throws at construction unless `__DEV__`, where it serves open and warns
  once. Pass `secret: null` to opt out deliberately, for a mount that is
  genuinely unreachable by an untrusted caller (mTLS-terminated mesh,
  private network) — spelled explicitly so the choice appears in the diff
  rather than in the absence of a line. An empty or whitespace-only secret
  now throws in every build, development included: it is never deliberate,
  only ever `process.env.CLUSTER_SECRET ?? ''` reading as configured while
  signing and verifying with an empty key. `hostEndpointRuntime()` is
  unaffected — a Durable Object holds one actor behind a platform
  single-instance guarantee and is not network-reachable, so the
  Cloudflare path needs no secret and keeps working.

- **sigx core peer range moves to `^0.14.0`** (#267). The
  `@sigx/reactivity` / `@sigx/runtime-core` / `@sigx/serialize` /
  `@sigx/server` / `@sigx/vite` peers now require core 0.14. Notable for
  consumers: core 0.14 turns on `sigxServer({ requireGuards })` by
  default, so a bare `serverFn` beside your actors becomes a build error
  — declare `use:`, derive from a preset, or mark it `unguarded: true`
  (the actors-side `requireGuards` gate in `sigxActors()` already worked
  this way). The live `useActorState` view also gains core 0.14's
  `AsyncState.hasValue`, and an ambient request scope's `locals` (the
  per-request store core 0.14 introduces) now flows through in-process
  actor guard chains by reference.

- **Wire JSON parsing keeps V8's fast parser** (#218). Every wire
  `JSON.parse` passed the prototype-pollution reviver, which forces a
  JS-level walk of every parsed node (measured 5–10.7× slower than a
  plain parse). The new guarded `parseWire` pre-scans the text and skips
  the reviver when no dangerous key (`__proto__` / `constructor` /
  `prototype`, literal or `\u`-escaped) can be present — same protection,
  measured 4.6–7.6× faster frame decode on the binary transports
  (`frames/decode`, new benchmark scenario). A custom codec reviver on
  the cluster seam is judged by identity and always takes the full walk —
  never silently downgraded to the fast path. `wire/endpoint-roundtrip`
  is unchanged within noise, as expected: parse is ~6% of that rung.

### Added

- **The request-context bag — `stampCallBag` / `ctx.bag` /
  `.with({ bag })`** (#246, second half, closing it). A small, string-only,
  size-capped (8 entries / 64 B keys / 256 B values / 1 KiB total, the new
  `CALL_BAG_MAX_*` exports) key/value bag on `ActorCallContext`. A guard
  stamps it at the edge with the new root export `stampCallBag(rq,
  entries)`; the public and `$live` endpoints lift it into the call context
  after the guard chain (never from a request header — client-settable
  identity would be an authorization bypass); the in-process `actor()`
  entry lifts the same locals store; methods read it as `ctx.bag` (frozen,
  per-read, empty outside a turn); `ctx.actor`/`ctx.publish` hops inherit
  it (guards do not re-run there); and it crosses hosts as an additive
  `bag` envelope field (no `HOST_PROTO` bump). Detached task bodies and
  volatile timer ticks deliberately read it empty, like `traceparent`. En
  route, a malformed or over-cap bag is dropped WHOLE and silently (never
  a 400) — treat a missing entry as unauthenticated. The envelope decode
  rebuilds the bag into a fresh null-prototype object (prototype-pollution
  guard), and its integrity between hosts is the envelope's existing
  perimeter posture (the HMAC signs the call identity, not the body — run
  mTLS/VPC between hosts). New transport conformance case: the bag
  survives a host-to-host hop intact, and only when valid. The chat
  example's `Room.post` now reads `ctx.bag.user` stamped by `requireUser`
  instead of threading `from` through the serverFn by hand.

- **One-way calls — `.with({ oneWay: true })`** (#246, first half). The
  dispatch resolves as `Promise<void>` at ACCEPTANCE — locally when the
  turn is enqueued into the target mailbox, remotely when the transport
  reply comes back from the receiving host's enqueue — never at turn
  settlement. Pre-acceptance failures (guard veto, unknown type,
  activation failure, shutdown, unreachable peer) still reject;
  post-acceptance failures are dropped-with-counter — the new
  `oneWayFailures` in `metrics()` (snapshot, digest and the cluster
  accumulator, additive within digest `v: 1`) — plus a dev-mode console
  warning. Works on all three proxies (browser client via a new
  `x-sigx-one-way` header, server entry, `ctx.actor`), types narrow to
  `Promise<void>`, streams refuse it, a declared read forces POST, a
  one-way self-call queues instead of deadlocking, and host-to-host the
  flag rides the envelope as an additive `ow` field (no `HOST_PROTO`
  bump; an older peer degrades the call to a normal awaited delivery).
  New transport conformance case: one-way acks on acceptance, delivers
  exactly once, drops the post-acceptance failure. Note: `metrics()` now
  attaches a minimal turn observer even under `histograms: false` so the
  counter stays correct — that mode newly pays the activation's per-turn
  clock reads.

- **Trace-context plumbing for observability exporters** (#245, the core
  half). `ActorCallContext` gains an optional `traceparent` — captured
  from the W3C `traceparent` header at the public and live endpoints,
  inherited by `ctx.actor`/`ctx.publish` hops, and carried host-to-host
  as an additive `tp` envelope field (no `HOST_PROTO` bump; malformed
  values cost the trace, never the call). `ActorTurnObserver` gains a
  trailing optional `call` parameter so an observer can correlate a turn
  with the dispatch that caused it; five-parameter observers are
  untouched. `@sigx/actors/host` newly exports `bucketUpperBoundsUs()`
  (histogram bucket bounds by layout, for Prometheus-style renderers)
  and `timingSafeEquals` (so a separately-installed package mounting a
  bearer-guarded endpoint does not carry its own copy).

- **Full interleaving — `reentrant: 'always'` and per-method
  `methodReentrancy`** (#15, closing it). The Orleans `[Reentrant]` /
  `[AlwaysInterleave]` pair: `reentrant` widens to
  `boolean | 'call-chain' | 'always'` (`true` stays the exact v1
  call-chain behavior), and `methodReentrancy: { method: 'always' }`
  exempts individual methods on an otherwise serial actor — the
  read-that-must-not-queue-behind-a-slow-write case. Interleavable turns
  launch immediately on a concurrent mailbox lane: they never wait for
  in-flight turns and are never waited for, serial turns keep their
  mutual exclusion, and in-chain calls to an interleavable target
  complete as concurrent turns instead of running inline — so a
  self-cycle cannot deadlock by construction, single-node and cross-host
  alike (the wire envelope is unchanged; the owning host reads its own
  definition). Each turn keeps its own call context
  (chain/callId/deadline) across awaits via a per-activation
  `AsyncLocalStorage`, loaded lazily only for interleaving types — the
  serial path never imports `node:async_hooks` and its microtask count
  is bit-identical (CI-gated). Saves are now single-flighted per
  activation with trailing coalescing, so concurrent `ctx.save()`s from
  interleaved turns merge (last-writer-wins, whole-state) instead of
  CAS-faulting the activation on its own sibling write;
  `ctx.clearState()` serializes through the same gate. Deactivation
  drains both lanes before `onDeactivate` and the final write-behind
  flush; the sweeper, `ctx.deactivate()` and `stats().queued` treat
  in-flight interleaved turns as work. Declarations are validated at the
  type's first activation (loudly, every build); `defineWorker` keeps
  both options structurally absent. New gated benchmark
  `dispatch/always-warm-turns` (7 microtask turns per warm interleaved
  dispatch, deterministic) plus informational
  `dispatch/always-warm-actor`.

- **Stateless workers — `defineWorker()`** (#243, closing it).
  Multi-activation pure-compute actors: the host pools up to `maxLocal`
  (default `navigator.hardwareConcurrency`, clamped to 16) interchangeable
  activations per (type, key), spun up on mailbox pressure, dispatched to
  the shallowest mailbox — so two calls to the same key run concurrently,
  which is the declared contract. Always placed locally: no directory
  claim, lookup or release (gated exactly in CI at `directory_ops == 0`,
  alongside a pool ≤ `maxLocal` invariant), invisible to fencing,
  migration and rebalancing, and on Cloudflare a worker runs in the
  calling isolate rather than a Durable Object. The identity-bound
  surface (`state`, `persistence`, `reminders`, `tasks:`,
  `subscriptions:`, `placement`, `reentrant`, `migrateState`) is
  structurally absent from
  `WorkerOptions` and typed away on the new `WorkerContext`
  (with every-build throwing guards behind casts); guards, `reads:`,
  `streams:`, `onActivate`/`onDeactivate`, `ctx.timer`/`actor`/`publish`
  all work as on `defineActor`. The Vite extractor recognizes
  `defineWorker` in `*.actor.ts` modules (same `__actorRef` client swap,
  same `requireGuards` gate), and `app.defineWorker` is the plugin-typed
  twin. Watches are refused for worker types; a same-key self-call is a
  deterministic `ActorDeadlockError`.

- **`migrateState` — evolve long-lived state across deploys** (#244,
  closing it). `defineActor({ migrateState: (stored, { raw, key }) => State })`
  runs between the storage read and activation, and only on a load that
  FOUND a record — never on the `state(key)` fresh path, never on
  `ctx.clearState()`, and always before `onActivate`. `stored` is already
  codec-revived, so `Date`/`Map`/`Set` are real objects and `unknown` means
  unknown SHAPE, not raw JSON; `raw` is the encoded record for the cases
  where the revived view cannot tell two versions apart. Returning the input
  unchanged is the fast path, and identity is how that is detected — so to
  migrate, return a NEW object.

  `S` is still inferred from `state:` alone: the hook's return is a check
  site, not a second inference site, so a migration written over `any` —
  which casting a `stored: unknown` naturally produces — cannot silently
  widen your state type. It also makes an `async` hook a type error, which
  is the sync-only rule enforcing itself.

  The migrated shape is written back LAZILY: it rides the next save the actor
  would have made anyway, in BOTH persistence modes. `migrateState` never
  causes a write by itself, so read paths stay read-only and a rolling deploy
  adds no write amplification — which does mean a write-behind actor that is
  only ever read after a migration never persists it. For a record that would
  otherwise never be saved at all, `{ persist: 'eager', migrate }` opts into
  one CAS write-back at activation; when a peer migrates the same record
  first, the loser adopts the winner instead of failing its callers.

  The consequence is stated rather than hidden: a fleet mid-deploy can
  migrate the same record more than once, and the etag CAS is what makes that
  safe — the first save wins, the loser either adopts the winner (eager) or
  gets `ActorStateConflictError` and re-activates against it (lazy).

  A throw fails activation with `ActorActivationError`, the same posture as a
  throwing `onActivate`: every parked caller sees it, nothing is remembered,
  and the stored record is never silently reset. Deliberately a plain
  function over `unknown` — version-field bookkeeping is the app's
  convention, and versioning an actor's INTERFACE across a mixed-version
  fleet is a different problem this does not attempt.

- **`defaults.maxActivations` — a soft LRU cap on live activations**
  (#16, closing it). When the sweep finds more than this many active
  (default 0 = unlimited), it deactivates the least-recently-used idle,
  unheld ones with the new `DeactivationReason` `'capacity'` — pressure
  relief before the heap applies its own. Deliberately soft: busy,
  queued, or kept-alive activations are never shed, so a loaded host may
  sit over the cap until it quiets, and correctness never depends on the
  number — a shed actor re-activates on its next call with state intact.
  Rides the existing sweeper (`sweepIntervalMs > 0`); shedding shows up
  in `metrics().activations.byReason.capacity`.

- **Automatic rebalancing — `cluster({ rebalance })` and
  `placement.rebalance()`** (#241, closing it). Off unless configured.
  Each host runs one round per `intervalMs` (default 60 s): probe peer
  loads over the ops channel, and when over `threshold × mean` (default
  1.2), `migrate()` a bounded batch (`maxMoves`, default 10) of its
  idlest activations — skipping kept-alive, queued, and recently-active
  (`minIdleMs`, default 60 s) ones. A host sheds only its own actors,
  only down to the mean (plus an `own - mean ≥ 1` floor, so a two-host
  cluster cannot trade one actor forever), and never acts on missing
  data: unreachable peers are excluded from the mean, and no answering
  peer means no action. One round is total — it resolves to
  `{ own, peers, mean, moved, reason? }` rather than throwing — and
  callable directly for ops tooling and tests. New counters:
  `rebalanceRounds`, `rebalanceMigrations`.

- **`activationCountPolicy()` — load-aware placement** (#241). Steers NEW
  activations toward the least-loaded host: a load view refreshed out of
  band over the authenticated host-to-host ops channel (`refreshMs`
  default 5 s, per-peer `timeoutMs` 1 s, probe `concurrency` 8), consumed
  by a sync `choose()` that samples two random active hosts and takes the
  less loaded (power-of-two-choices), plus a local pending delta so a
  burst inside one refresh window sees its own effect. A failed probe
  keeps the stale value; a host with no data reads as cold, so a freshly
  joined host attracts work immediately; un-attached it keeps no state
  and is behaviorally `randomPlacementPolicy()`. Never load-bearing for
  correctness — the directory stays the sole arbiter of
  single-activation.

- **The `attach` seam for stateful policies** (#241). `PlacementPolicy`
  grows an optional `attach(runtime: PolicyRuntime): void | (() => void)`
  — called once per policy object when the placement starts (or on first
  resolution, for a `defineActor({ placement })` declaration), torn down
  at placement stop. `PolicyRuntime` is deliberately narrow — `{ hostId,
  view(), selfLoad(), peerLoad(target, timeoutMs, signal?) }` — the load
  numbers being the one thing a policy could not legally obtain before. A
  throwing `attach` is dev-warned and contained: a policy can cost
  throughput, never the placement.

### Added

- **Topics — actor-to-actor pub/sub** (#239). `topic(name, key?)` declares a
  topic; a `subscriptions:` table on `defineActor` receives its events; and
  `ctx.publish` / `host.publish()` / `publishTopic()` fan one event out to
  every subscribing type, resolving to a settlement report
  (`{ subscribers, delivered, failures }`).

  Subscriptions are **implicit and declarative** — the subscriber set is a
  pure function of the deploy, derived from the host's registry, and a
  publish activates idle subscribers exactly the way a reminder delivery
  does. Each delivery is an ordinary dispatch of the reserved method
  `$sigx:topic` through placement, so a remotely-owned subscriber rides the
  existing internal transport (HMAC envelope, deadline propagation, branded
  errors) with **zero topic-specific wire machinery**; the cost model is one
  dispatch per subscribing type.

  **Delivery is best-effort, at-most-once, and settled.** The publisher
  awaits every handler turn (intrinsic backpressure, bounded by the call
  deadline) and never throws for a subscriber: a throwing handler, an
  unreachable host, or a detected cycle each land as one `failures` entry.
  `ctx.publish` carries the publishing turn's call chain, so a subscription
  that dispatches back into a non-reentrant publisher is a `deadlock`
  failure rather than a hang. Nothing is persisted or retried — a durable
  mode is an explicit non-goal for v1, with API room reserved
  (`PublishOptions`), and `ctx.topics.*` stays free for a future explicit
  subscribe.

  An entry may map the subscriber key — `{ key: () => 'aggregate', handle }`
  makes one singleton receive every topic key's events. Handlers are
  ordinary turns (mutate state, `ctx.save()`), are not wire-callable, and
  never appear on the client. Topic names are validated like actor types
  (definition-time throws in every build): no `#`/NUL, no leading `$`/`@`.

### Fixed

- **`metrics()` no longer breaks every watch — and with it `$live`**
  (#259). Its dispatch middleware forwarded `dispatch` and
  `dispatchStream` but not `dispatchWatch`, and all three are optional on
  `ActorDispatcher`, so the composed dispatcher simply lost the method:
  any host with `metrics()` attached answered every watch with
  `the placement for <Type> cannot watch`. That is the whole of
  `useActorState(…, { live: true })` and the `$live` mount, and `metrics()`
  is attached on essentially every production host — so the feature was
  broken everywhere it shipped and nowhere it was tested, because the
  in-process suites build hosts without the plugin. The middleware now
  forwards it (uncounted: a watch is one long-lived subscription that
  re-invokes a read per mutating turn, so folding it into `calls` would
  make that total stop matching what callers issued).

  The `__DEV__` guard that exists to catch exactly this only checked
  `dispatchStream`, which is how it went unnoticed; it now checks both, so
  third-party middleware gets told too. Found by the Tier-3 `$live`
  assertion added in the same change.

- **The public actor endpoint no longer dispatches runtime-reserved
  methods** (#240). `createActorResolver` synthesized a wrapper for any
  method name, so a caller could POST `Type#$sigx:reminder` and invoke
  `onReminder` under a reminder name of its choosing — a reminder firing
  the reminder table never scheduled. A `$sigx:`-prefixed method now
  answers exactly like a method that does not exist (404,
  `method-not-found`, same message shape), so probing cannot even learn
  which names are special. The HMAC-authenticated host-to-host mount is
  unchanged — `resolveHostSymbol` keeps resolving reserved symbols, which
  is how remote reminder deliveries travel.

### Changed

- **Orleans-derived naming is gone: silo → host, grain → actor** (#233).
  A full break across API and wire — nothing has shipped, so there are no
  aliases and old and new processes do not interoperate. The
  `@sigx/actors/silo` entry is now `@sigx/actors/host`; `createSilo` →
  `createHost`, `Silo`/`SiloStats`/`CreateSiloOptions` →
  `Host`/`HostStats`/`CreateHostOptions`, `currentSilo`/`peekSilo`/
  `requireSilo` → `currentHost`/`peekHost`/`requireHost`,
  `SiloShutdownError` → `HostShutdownError`, and every `Silo*` cluster
  export is now `Host*`. On the wire: the internal mount moved from
  `/_sigx/silo` to `/_sigx/host`, the call header from `x-sigx-silo-call`
  to `x-sigx-host-call`, the stats symbol from `$sigx:silo#stats` to
  `$sigx:host#stats`, JSON fields `siloId`/`silos` are now
  `hostId`/`hosts`, the ops query `?silo=` is `?host=`, and the error
  kind `silo-shutdown` is `host-shutdown`. The seam global is
  `__SIGX_ACTOR_HOST__`. Storage identity is untouched: reminder shards,
  the reserved `$sigx:*` record types and the directory key format are
  byte-identical, so persisted state survives.

- **Far call deadlines are enforced by a shared registry, not a timer per
  call** (#230). With a non-zero `callTimeoutMs`, every dispatch used to
  allocate a `setTimeout`/`clearTimeout` pair, a `Promise.race` and an
  `async` wrapper to race the caller's deadline — measured at 38% of
  dispatch throughput, and paid by every production call since the default
  is 30 s. Deadlines ≥ 10 s away now share one recurring unref'd 1 s tick
  (`CallDeadlines`); short budgets (e.g. a wire hop arriving nearly spent)
  keep an exact per-call timer. Observable change: a far deadline may fire
  up to ~2 s **late**, never early — the `deadline` value crossing hops is
  unchanged, so cross-host budgets are unaffected. Gated by the new
  `dispatch/warm-turns-deadline` exact benchmark (12 → 11 microtask turns,
  1000 → 0 host timers per 1000 dispatches).

### Fixed

- A call arriving with an already-expired deadline no longer leaks an
  unhandled rejection when its (still-enqueued, never killed) turn itself
  rejects — the turn promise now gets a rejection handler on every branch
  (#230).

### Added

- **`clusterStats()` aggregates behaviour, not just topology** (#121).
  `HostReport` grows three optional fields — a mergeable `metrics` digest,
  the host's `health`, and (under `detail`) its live `activations` — so ONE
  call answers for the whole fleet. One reachable endpoint, one secret, and
  it works behind an ingress where the peers are not individually reachable.

  `totals` gains `metrics` (calls, failures, streams, `errors.byKind`,
  storage operations, activation churn, per-type and per-method call counts,
  and merged latency/queue/turn) plus a `health` tally.

  **Latency is merged, not averaged.** `HistogramSnapshot` is p50/p90/p99
  with no buckets, and the mean of two hosts' p99s is not the p99 of
  anything — so the digest carries bucket COUNTS and the cluster's
  percentiles are re-derived from the summed distribution. Buckets travel
  sparse (of 384, a real host occupies a few dozen), and a peer whose bucket
  layout differs contributes its counters while its distribution is dropped
  rather than mixed into a different axis.

  **`totals.metrics.hosts` is the denominator.** A host with no `metrics()`,
  or one mid-rolling-deploy on an older build, contributes nothing — and
  totals that quietly cover two thirds of the fleet look exactly like totals
  that cover all of it. `totals.metrics` is `null` rather than zeroes when
  nothing anywhere is instrumented, because "no instrumentation" and "no
  traffic" are very different findings.

- **`registry.reportDigest()` / `registry.digest(name)`** (#121) — the seam
  the above rides on, mirroring `reportHealth`/`health()` and
  `reportOps`/`ops()`. It is a SECOND seam rather than a read of the ops
  section for two reasons: an ops section carries derived percentiles, which
  is exactly the un-mergeable shape, and reading one from
  `placement.report()` would re-enter that report — `cluster()` publishes it
  AS an ops section — and recurse until the stack gave out. `digest()` walks
  digest providers only, so that is structurally impossible.

- **`Histogram` digests** (#121): `HISTOGRAM_LAYOUT`, `HistogramDigest`,
  `mergeHistogramDigests()` and `digestSnapshot()`, exported from
  `@sigx/actors/host` alongside `createMetricsAccumulator()`. A client
  merging a user-selected subset of hosts needs the same arithmetic
  `clusterStats()` uses; without these it would have to reimplement the
  log-linear bucket layout, which is the mistake the layout tag exists to
  catch. Foreign bucket indices are bounds-checked rather than used as
  offsets — a digest arrives over a wire.

- **`detail` on the fan-out, and `?detail` on the ops route** (#121). The
  actor list and recent failures are opt-in, and `detail.hosts` targets one
  host: the walk is O(activations) on every host at once, and actor keys are
  the one field on this wire that can be personal data. Requested limits are
  clamped by the RESPONDER — HMAC proves who is asking, not that
  `activations: 1e9` is a sane thing to ask for.

### Changed

- `ops({ cluster })` takes an optional second argument (the parsed
  `?detail` query). Appended, so every existing
  `(signal) => clusterStats(placement)` keeps working unchanged.
- `HostReport.v` stays `1`. Every new field is optional, so a peer on an
  older build simply answers without them — a version bump would have made a
  new collector classify every not-yet-deployed peer as `unsupported`,
  blanking the report during exactly the deploy it exists to explain.

### Added

- **`reads:` — GET-cacheable actor reads** (#195, closing #11). A method
  listed there is served over `GET
  {base}/r/{token}/{Type}%23{method}?args=[key,…]` with the `Cache-Control`
  the declaration describes, so browsers, CDNs and reverse proxies absorb
  read traffic that would otherwise reach an actor. The vocabulary is core's
  `ServerFnReadCache` by alias — `maxAge`, `staleWhileRevalidate`, `public`,
  `sMaxAge` — because a second spelling of the same HTTP headers would drift
  from the one an app already learned for serverFns.

  Every moving part is core's: GET admission (only for a wrapper carrying
  both `__sigxGet` and `__sigxCacheControl`), `?args=` decoding through the
  same codec and pollution-safe reviver as a body, the query-length cap, the
  header emission, `no-store` on a non-2xx, and `Vary: Cookie` on anything
  not `public`. What is new here is the declaration, its validation, and the
  build stamping the names onto the client ref so the proxy issues GET with
  no call site changing — the same mechanism that already carries stream
  names.

  **The trade is explicit: a cached read bypasses mailbox ordering.** For
  `maxAge` seconds the response an intermediary serves may be older than the
  actor's state, and nothing on the server can pull it back — not
  `ctx.save()`, not `useActorAction`, not `cells.invalidate()`, which reach
  this page's cells and never a CDN's copy.

  Four things are refused at definition time rather than left to be
  discovered from a production cache hit: a non-seconds `maxAge` /
  `sMaxAge` / `staleWhileRevalidate`; a `streams:` method (a stream is not a
  cacheable representation, and the endpoint would 405 it anyway); and
  `public: true` on a read the actor guards. That last one is the security
  gate: `public` puts one caller's copy in a SHARED cache for the next
  caller, so core's contract is args-only, and a guard is the one thing here
  that provably reads the request — with no way to inspect *what* it reads,
  the safe reading of "this actor has a guard" is "this response is per
  caller". Without `public` the read is still cached, per client, with `Vary:
  Cookie`.

  Guards run on GET exactly as on POST, a rejection answers with
  `no-store`, POST keeps working for declared reads (the declaration is on
  the definition, not the wire), and the routing token travels in both
  carriers so an edge routes a cacheable read like any other call. The GET
  sends no `content-type` — it would describe a body that does not exist, and
  it is one fewer non-safelisted header, though not a promise of no CORS
  preflight: the routing token header ships by default and triggers one on
  its own.

  Two consequences of the carrier, documented rather than discovered: a GET
  puts the actor key and every argument in the URL — the log-hygiene concern
  the hashed routing token exists for, now applying to arguments as well —
  and a long enough query is a 414. `actor(Def, key).with({ get: false })`
  opts one call back onto POST for either reason; declared reads default to
  GET and an explicit `get` wins.

- **`useActorState(…, { live: true })` — the client half of the live layer**
  (#192, closing #10 and #14). The `$live` mount shipped in #68 and nothing
  in a browser could reach it: `ActorTransport.live?()` was declared with no
  implementation, and `examples/chat` hand-rolled the whole thing — a
  per-actor `watch()` stream per room, a reconnect loop, and a
  `cells.invalidate()` per frame.

  What it adds is *other people's* writes. Everything else in the read path
  refreshes only the tab that wrote: invalidation is local bookkeeping, and
  no request/response call tells a second browser anything happened. The
  first paint is untouched — the ordinary read still seeds the cell, SSR
  still serializes it, hydration still costs no request.

  The channel (`createLiveChannel`, `@sigx/actors/app`) holds **one**
  connection for the page's whole subscription set, refcounted so twelve
  components reading one actor share one watch. A set change coalesces for
  ~20 ms, aborts, and reopens with the new set: a `fetch` POST body is not
  duplex outside Chromium, so a late subscription cannot be pushed onto an
  open stream, and the obvious alternative — a control POST against a
  session — is rejected because on Workers, at an edge, or in a multi-host
  cluster it cannot be routed to the instance holding the open stream.
  Reopening is safe precisely because every subscription re-seeds, which is
  also why a reconnect needs no resume token and no server-side
  cross-request state.

  An unchanged value is dropped rather than delivered, by structural
  equality. Two things produce one routinely: that re-seed, and the fact that
  a mutating turn re-runs EVERY subscription on the actor — change a room's
  topic and its `recent(20)` watch re-runs too and returns an identical list.
  (Found by running the example, not by reading the code: the wire shows
  three frames per topic change, the page sees one.) Safe because these are
  views of current state and not an event log; a value that cannot be
  fingerprinted is never suppressed.

  It lives in `./app`, not `./client`, deliberately: that entry's bytes ride
  every bundle that touches an actor and its tree-shaking guard exists to
  keep policy out of it (`./client` is unchanged at 2.86 KB / 1.57 KB;
  `./app` grew 3.2 → 4.7 KB). A transport that brings its own `live()` — a
  WebSocket transport — wins over the default, with no call site changing.

  The hook wraps the cell rather than writing into it, because core has no
  way to write an `AsyncState` from outside (`writeBack` + `refresh()`
  invalidates first and would cost a round trip per frame; the upstream ask
  is `AsyncReadHandle.setValue`). Two rules keep that honest: **a cell
  settle wins over an older push**, so a writer sees their own write
  immediately rather than waiting out the watch throttle; and **a dead feed
  degrades to "not live", never to "broken"** — one subscription's failure
  reaches that read alone, and a read whose feed cannot be established keeps
  working. Pushes also `writeBack` under the canonical key, so a remount
  restores the pushed value instead of the last fetched one.

  `examples/chat` now uses it, which is what the hand-written stream, the
  reconnect loop and its `streams:` body were deleted for.

### Fixed

- **Only DECLARED methods are callable — inherited `Object.prototype`
  members no longer dispatch** (#198). A `methods:` factory returns an object
  literal, and the activation resolved an incoming name by *indexing* it, so
  the name walked up the prototype chain: `POST {Type}%23toString` answered
  `200 {"data":"[object Undefined]"}`, `constructor` answered `200 {}`, and
  `valueOf` / `hasOwnProperty` / `__proto__` threw a raw `TypeError` that
  surfaced as a masked 500. On an actor carrying a `use` chain it failed even
  earlier: the guard lookup inherited a function, whose `.length` is its
  arity, so the empty-chain early return did not fire and the guard loop
  iterated a function — a 500 before dispatch could answer at all.

  The asymmetry was the bug. `streamNames`, the `reads:` validator and the
  Vite extractor all work from OWN keys, while dispatch indexed — so anything
  on the prototype was invisible to every check and visible to every call.
  Every name lookup into a user-supplied table now agrees with them
  (own key **and** callable): the unary and stream dispatch paths, the guard
  chain lookup, and the `public: true` read validator. All of them are now
  a clean 404 `method-not-found`, which is what the wire contract and its
  build-skew hint always claimed. `tasks:` and the `reads:` lookup already
  did this; the idiom is shared rather than copied a fourth time.

  A method that *deliberately* shadows a prototype name (`methods: () => ({
  async toString() {…} })`) is unaffected — it is an own key. The one shape
  that stops working is a factory returning a **class instance**, whose
  methods live on a prototype; that never fully worked (`streamNames` and the
  build already read own keys), and `__DEV__` now warns rather than leaving
  you with a 404 for a method you can see in the source.

- **A quiet per-actor stream is now kept alive** (#178). `$live` has pinged
  since it shipped; `actor(X, k).watch()` sent nothing between yields, so a
  quiet room's stream was bytes-silent and every intermediary with an idle
  timeout reaped it — ingress at 60 s, cloud load balancers at ~4 min,
  mobile NATs — leaving the client with a stream that "ended without a
  done/error terminator" and a full catch-up read per reconnect.

  `handleActorRequest({ streamPingMs })` (default 30 s, `0` disables) emits
  a `{"ping":1}` line after that much silence, and the client's reader skips
  it. Injected at the BYTE layer, wrapping the NDJSON body: core owns the
  envelope and the actor's generator owns the values, so neither can emit a
  line the other does not know about — and a wrapper is the one place that
  sees "nothing has been written for a while" for every streaming method at
  once, including a `streams:` body parked on `ctx.changes()`, which is the
  quiet case that breaks. A `ping` line rather than a sentinel `chunk`,
  because a chunk is user data on every stream method and any sentinel there
  could collide with something an actor legitimately yields. Non-streaming
  responses are returned untouched, object identity included.

- **A disconnected stream or watch consumer now releases the activation**
  (#184). #120 fixed this inside the activation; the same trap was sitting in
  three layers ABOVE it, so on the wire nothing ever pulled the lever.

  Each layer wrapped the stream in an async generator (`async function* pump()
  { yield* iterable }`). A generator parked at `yield*` is suspended at an
  internal await, and the spec QUEUES `return()` there instead of running it —
  so the generator is never resumed, never forwards `return()` inward, and the
  activation's teardown is never reached. The affected layers were the public
  actor endpoint, the internal host-to-host endpoint, and `httpTransport`'s
  sending side (where the victim was `finally { controller.abort() }`, meaning
  a hung-up caller never even aborted its fetch).

  The cost was quiet and unbounded. `keptAlive` does not delay idle
  collection, it EXEMPTS an activation from it (`if (a.idle && !a.keptAlive &&
  …)`), so a departed consumer left the actor permanently ineligible — not
  "collected late", never collected. A control test pins the distinction: a
  plain idle activation is collected, a watched one was not, after five sweep
  periods at `idleAfterMs: 0`.

  All three now share `relayStream()`: a hand-written async iterator whose
  `return()` marks itself done and forwards inward BEFORE awaiting anything
  that could park it, plus a synchronous `onClose` hook for whatever wakes a
  parked reader (the transport aborts its fetch there). Both triggers are
  wired — a body cancel and a request abort — because hosts differ in which
  one they raise.

  **Not fixed on Cloudflare** (#187): `stub.fetch(url, { signal })` does not
  propagate cancellation into a Durable Object, so the chain is correct right
  up to that boundary and the boundary swallows it. Measured directly; the
  reproduction ships skipped.

### Changed

- **The default placement policy stays `randomPlacementPolicy()`, decided on
  a measurement** (#135, stage 4 of #84). RFC #84 asked whether it should
  change now that its locality cost is quantifiable. It should not, and the
  new `cluster/locality-warm` benchmark says why — N=100, warm steady state:

  | edge × policy | local fraction | ownership spread |
  |---|---:|---:|
  | round-robin × random *(default)* | 0.02 | 2.92 |
  | round-robin × `preferLocalPolicy()` | **0.00** | 1.25 |
  | hash the routing token × `preferLocalPolicy()` | **1.00** | 2.50 |
  | skewed LB × random | 0.00 | 2.92 |
  | skewed LB × `preferLocalPolicy()` | 0.80 | **80.4** |

  Caller affinity buys **nothing** under a plain round-robin balancer — it
  pins each actor where its first call landed and the balancer sends the
  next one elsewhere anyway. And under an uneven balancer (a rolling deploy,
  a bad health check) it concentrates ownership **80×**: one host holding
  80% of the actors, which do not move back, because placement applies only
  to new activations. Random holds ~2.9 spread whatever the edge does, which
  is the property that matters precisely when things are going wrong.

  `preferLocalPolicy()` remains the right answer when the edge hashes the
  routing token from #132 — that row wins outright, and is documented under
  "Which placement policy should you use?" in the README. It is a
  configuration, not a default.

  Recording the rationale even though nothing changed, so the question is
  not re-litigated a third time.

### Added

- **`@sigx/actors/job` — `defineJob`, durable long-running operations**
  (#164). One job = one actor (key = your run id), as convention over
  `defineActor` + the `tasks:` primitive: a standard state machine
  (`pending → running → (paused ⇄ running) → completed | failed |
  cancelled`), idempotent `start` (safe under HTTP retry), immediate
  `cancel` with guarded terminal writes, `resume(data)` for paused jobs,
  `result()` with typed errors (`JobNotDoneError` / `JobFailedError` /
  `JobCancelledError`), a live `watch()` stream of `JobInfo`, and a
  `JobHandle` for the run body — `progress()` (change-feed-visible; no
  write of its own, persisted only as part of the next checkpoint or
  terminal save), `checkpoint()` (durable resume point), `update()` (public
  `extra` fields), `pause()` (park durably with no live task), and
  `reminders` (HITL timeouts via the `onReminder(control, name)`
  passthrough, whose control resumes/cancels internally — no
  self-dispatch). Crash-resume bumps `attempts` and delivers
  `resumedFrom`; past `maxAttempts` (default 3) the job fails.
  `retainMs` clears terminal records via a one-shot reminder;
  `discard()` does it on demand.

- **`ActorRouter` — the pluggable client routing seam** (#135, stage 3 of
  #84): `routedTransport()`, `learningRouter()`, `staticRouter()`,
  `chainRouters()`, `routedFetchTransport()`, and `actorRedirect()`.

  `ActorTransport` decides *how* a call travels; `ActorRouter` decides
  *where* it goes. They sit beside each other and compose, so a router and a
  custom transport are one wrapper apart instead of a new class each.

  The actor is now threaded to the transport as `ActorCallInit.ref`, built
  once per `actor(def, key)` rather than re-derived from the symbol and
  `args[0]` — that convention belongs to `__actorRef`, and a transport that
  re-implemented it would break silently the day it changes. `$live` carries
  no ref (it multiplexes many actors onto one request), so it is never
  routed.

  **A router can never fail a call.** `resolve` throwing falls back to the
  default endpoint; `learn` and `invalidate` throwing are ignored, because a
  cache that breaks while *remembering* an answer must not destroy the
  answer the caller is about to receive. That is what makes it safe to put
  arbitrary user code on the dispatch path, and it holds because routing is
  an optimization — a wrong endpoint costs a hop, never a wrong answer.

  The redirect-following loop moved out of `fetchTransport` into
  `routedTransport`, so a custom transport gets it for free. `learningRouter()`
  is stage 2's route memo lifted into the public seam.

### Changed

- **`follow` and `router` moved off `ActorTransportConfig`** (#135) onto
  `routedFetchTransport({ ... })`. `configureActors({ endpoint, follow: true })`
  becomes `configureActors(routedFetchTransport({ endpoint, follow: true }))`.

  Routing is now opt-in **by import**, and that is worth ~960 bytes: with the
  sugar on the always-loaded config, `@sigx/actors/app` measured 4.15 kB;
  without it, 3.19 kB. The client entry rides every bundle that touches an
  actor, and the majority consumer — a single-origin browser app — cannot use
  redirects at all, since the retry would be cross-origin and preflight-
  refused. The `/app` budget goes back to 3.5 kB (it had been raised to 4 kB
  for exactly this coupling), and a new size-limit entry guards the claim: a
  plain `{ __actorRef, configureActors, fetchTransport }` import measures
  1.56 kB against the 2.85 kB whole module.

- **`endpointOf` precedence now puts a router's answer first** (#135):
  `init.route ?? config.endpoint ?? init.endpoint`. Previously the configured
  endpoint won, so any app that called `configureActors({ endpoint })` would
  have silently defeated its own router.

- **`@sigx/actors/app` uses the shared `resolveTransport`** (#135) instead of
  its own copy of the `isTransport` duck-check plus `fetchTransport`. The
  duplicate had already diverged once.

### Added

- **`ActivationInfo.tasks` — running detached-task count per activation**
  (#162). Flows through `host.activations()` and the `ops()` snapshot, so
  operators can see which actors hold long-running work (and why the idle
  sweeper is skipping them); `sigx actors top` shows it as a TASKS column
  in the actors table.

- **Task durability: ledger + liveness reminder + crash-resume** (#151).
  `ctx.tasks.start()` now resolves only after the run is durably recorded —
  an entry in the reserved `$sigx:tasks` storage record (etag-CAS,
  reload-and-reapply) plus a liveness reminder armed under the same
  reserved name. A run interrupted by deactivation (any reason but cancel)
  keeps its entry and restarts on the actor's next activation with
  `TaskInfo.restarts` bumped and its `input` replayed through the state
  codec; completion, throw, and cancel remove the entry, and an empty
  ledger disarms the reminder (and deletes the record — no tombstones).
  The reminder is the crash driver: a dead host's reminder shards are
  re-owned by survivors, the tick delivers through placement, and the
  actor re-activates — tasks and all — within ~60–90s, no client call
  needed. The reminder name never reaches `onReminder`; its handler
  self-heals (restarts a ledgered run that somehow is not running, disarms
  a stale reminder whose ledger is gone). At-least-once by contract: the
  runtime resumes the function, user code resumes the work from its own
  checkpointed state. New in `@sigx/actors/host`: `TASKS_TYPE`,
  `TASK_REMINDER`, `TASK_REMINDER_MS`, `TaskLedger`, `TaskLedgerEntry`.

- **`tasks:` — detached long-running work on an actor** (#147). Declare
  long-running operations in a `tasks:` factory on `defineActor` and start
  them with `ctx.tasks.start(name, input?)`: the body runs detached from
  the mailbox (reads, streams and watches keep answering while it works),
  touches state only through the new `ctx.turn(fn)` — one ordinary
  serialized mailbox turn — and holds a keep-alive ref so the idle sweeper
  skips the actor. Each run gets its own `abortSignal`, fired on
  `ctx.tasks.cancel(name)` (reason `'cancelled'`) or deactivation (reason:
  the `DeactivationReason`); deactivation waits a bounded grace
  (`HostDefaults.taskGraceMs`, default 10s) with the mailbox still open so
  a winding-down task can run a final checkpoint turn. `start` is
  single-flight per name; a thrown task is terminal; `cancel` is a request,
  not a join. New types: `TaskApi`, `TaskInfo`, `ActorTaskContext`,
  `ActorTask`, `ActorTaskTable`. Durable crash-resume ships separately.

### Changed

- **`ctx.abortSignal` now fires at the START of deactivation, before the
  mailbox drain** (#147) — previously it fired after, which made its
  documented contract ("long-running work should observe it")
  unsatisfiable: work awaiting the signal was exactly what the drain was
  waiting on, so a parked turn held `host.stop()` to its deadline and was
  then force-dropped. A turn parked on the signal now unwinds inside the
  drain window.

- **`attachSignalHandlers(host, { server, onStopBegin, onError })` drains the
  HTTP edge** (#157). Stopping the actors was only half a graceful shutdown:
  an orchestrator's preStop sleep and readiness-503 steer *new* connections
  away, but connections already established survive endpoint removal
  (conntrack) and ride into the exiting pod, where they are reset at
  `process.exit`. #142 measured that as 122 lost calls out of ~1.7M on a
  rolling restart — all connection-level, none visible from the actor layer,
  which reported a clean hand-off.

  That fix previously lived only in `examples/aks-cluster`, so the
  **documented** Node recipe still had the bug. The sequence now belongs to
  the seam, and both examples use it, so there is one shutdown story instead
  of two.

  The order is deliberately not the obvious one: `onStopBegin()` (the caller
  starts answering `connection: close`, which is what actually drains client
  pools — one response at a time, interrupting nothing) → `host.stop()` →
  `server.close()` + `closeAllConnections()` **last**. Closing the listener
  first looks more decisive and is worse: on Node ≥ 19 `close()` also
  destroys idle connections, and "idle" from the server's side includes a
  socket the client is at that instant writing its next request onto — which
  manufactures the very reset the sequence prevents. There is a test pinning
  that, because the naive order is an easy "improvement" to reintroduce.

  `server` is typed structurally, so `https`, `http2` and a test double all
  satisfy it, and `closeAllConnections` is optional-called for Node < 18.2.
  Omitting `server` behaves exactly as before — the options are additive.

### Changed

- **A failed drain now exits non-zero** (#157). `attachSignalHandlers`
  swallowed a `host.stop()` rejection and exited 0, so a pod that failed to
  flush looked like a clean stop — and on a terminated pod the exit code is
  often the only diagnostic left. It now exits 1 and reports through the new
  `onError` hook.

### Fixed

- **A fenced host now fails LIVENESS, not just readiness** (#141).
  `HealthCheck` gains `fatal?: boolean` (implies not-ready); `health()`
  answers `503 { status: 'fatal' }` on the liveness route when any check
  reports it, and `cluster()` marks the `fenced` state fatal. Fencing is
  terminal for a host identity, so before this a membership-store outage
  longer than `ttlMs` left every pod `200 live / 503 not-ready` with zero
  restarts — a dead cluster only a manual rollout restart could revive
  (observed on AKS). `leaving` is unchanged: draining stays live.

- **`mintCallId()` no longer draws its per-process seed at module scope**
  (#136). workerd forbids generating random values in global scope, so the
  seed was either refused or served from a deterministic startup seed — and
  in the latter case every isolate of one bundle shares a seed while its
  counter restarts at zero, so two Durable Objects mint *identical* call
  ids. That id is what deadlock detection and cross-hop correlation are
  built on. Now seeded lazily on first use, the same idiom `context.ts`
  already uses for the AbortController it cannot construct at import.

### Added

- **`HostEndpointRuntime` — the inbound half of `HostTransportRuntime`**
  (#136), plus `hostEndpointRuntime(host)` in `@sigx/actors/cluster`. The
  internal host endpoint never calls `descriptor()` or `view()`; those
  belong to the sending side, which has to pick a peer. Splitting them lets
  a single-host runtime with no membership — a Cloudflare Durable Object,
  where the platform guarantees the single instance — serve the internal
  mount without fabricating a membership view. `HostTransportRuntime` now
  extends the narrower interface, so every existing transport is unchanged
  and the whole cluster suite passes untouched.

  `$sigx:host#stats` resolves to `null` on such a runtime rather than
  synthesizing a `HostReport`: that payload is a *cluster* identity (hostId,
  epoch, address, owned reminder shards) and inventing one would put fiction
  on the ops channel. The endpoint's resolver now asks the runtime whether
  it offers the ops channel instead of assuming every runtime does.

- **`onMiss: 'proxy' | 'redirect' | 'auto'` on the public actor mount**
  (#134, stage 2 of #84), plus `cluster({ publicAddress })`, the
  `ActorPlacement.locate()` seam, and `configureActors({ follow })`.

  A call for an actor this host does not own has always been proxied: one
  client round trip, one internal hop, every time. `'redirect'` answers
  `421` naming the owner instead, and `'auto'` redirects only callers that
  advertise they can follow (`x-sigx-actor-follow`), so one cluster can
  serve a browser origin that proxies and a service origin that redirects.
  The option sits on the **mount** rather than the host for exactly that
  reason. Default stays `'proxy'`, byte for byte.

  **The client memo ships with it, not after it.** A redirect without
  client-side memory costs *two* client round trips versus one trip plus an
  internal hop — strictly worse than proxying. `configureActors({ follow:
  true })` remembers where each actor lives, so the cost is 2 requests once
  and 1 forever after, straight to the owner. A learned endpoint is dropped
  the moment it proves wrong (connection failure or 5xx) and the call falls
  back to the configured endpoint, because otherwise one dead host would
  strand every client that had learned it.

  **The endpoint resolves the owner BEFORE dispatching**, via the new
  optional `locate()` — it does not let a `wrong-host` escape the routing
  loop. 421 is a status a user agent may retry on its own (RFC 7540
  §9.1.2) and actor calls are not idempotent, so the answer has to be
  provably free of side effects; "we have not dispatched anything yet" is,
  and "the forward failed partway through" is not. A test asserts a
  redirect moves no activation and increments no `remoteDispatches`.

  **`publicAddress` is required for a redirect, and never defaulted from
  `advertise`.** The latter is the internal peer origin — typically a pod
  IP — so redirecting a client there would hang, and disclosing it would
  hand internal topology to anyone who can reach the public mount. Without
  `publicAddress` the mount proxies anyway and dev-warns once; the 421 body
  carries `owner.endpoint` and never `address`.

  Also fixed on the way: **`compositePlacement` silently dropped `locate()`**,
  which would have made every app-built host — i.e. every real deployment,
  since `defineActorApp` is the setup path — proxy forever while looking
  correctly configured. It forwards a fixed set of methods, so a new one is
  invisible until something asks for it.

  Known limits, documented rather than papered over: `$live` is never
  redirected (one held-open response fans out to many actors, so there is no
  single owner to name); a redirected watch is bound to its owner only until
  the actor migrates; and cross-origin redirects need an explicit `origin`
  allowlist and CORS, which is why `'proxy'` remains the browser answer.

  New counters `locates` / `locateRemote` — their ratio is the miss rate the
  edge is producing, and the number to watch after wiring up routing-token
  hashing.

### Changed

- **`HostDefaults.sweepIntervalMs: 0` disables idle collection** (#136),
  matching the existing `callTimeoutMs: 0` precedent. For a runtime that
  evicts the whole host when it goes idle the sweeper can never usefully
  fire — eviction destroys the isolate, the host and the activation
  together, and nothing keeps the object resident except in-flight work,
  which is exactly what the sweeper skips. It is also not free there: a
  pending timer holds a Durable Object in memory and billable, so a
  permanent interval would be one billing pin per actor.

- **The actor wire now carries a per-actor routing token** (#132, stage 1 of
  #84): `POST {base}/r/{token}/{Type}%23{method}`, mirrored into an
  `x-sigx-actor-route` header. No compatibility shim — nothing has shipped,
  and a wire that only *sometimes* carries the token means a load-balancer
  config that only sometimes works.

  The problem it solves: the actor **key** rides in the JSON body, so no
  load balancer can see which actor a request is for, and locality decays as
  1/N — measured 1.00, 0.50, 0.12, 0.02, 0.01 for N = 1, 2, 10, 50, 100. At
  N=100 that is ~99% of calls paying a cross-host hop, which #80 measured at
  20× a local dispatch with HMAC off and **69× with it on**.

  The token is a **middle** segment, which is what makes this cheap: the
  symbol is decoded as the *last* path segment, so the token slots in ahead
  of it and the endpoint neither parses nor validates it. Validating would
  make a hint load-bearing, and routing is an optimization that must never
  be — a stale, wrong or absent token costs a hop, never a wrong answer.
  (`actorRouteToken()` is exported from `./server` for adapters that want to
  log or shard on it; the endpoint itself does not call it.)

  Two carriers because neither alone covers every edge: a path segment
  cannot be silently stripped by a mesh, and a mangled one 404s loudly
  rather than degrading to zero locality in silence — while Envoy has no
  path-substring hash policy at all and can only hash a header.

  `configureActors({ route })` selects `'hash'` (default — an opaque hash of
  the actor id), `'key'` (raw, for debuggable routing), `'none'`, or a
  function. The default is a hash because actor keys are frequently user ids
  or emails; note this is **log hygiene, not privacy**, since an unkeyed
  hash of an email is one dictionary lookup from plaintext at any width.
  Because the composition needs only *stability* and not agreement, a hash
  routes exactly as well as the key.

  **The token alone changes nothing — it must be paired with
  `preferLocalPolicy()`.** The new `cluster/locality-routed` benchmark
  measures the composition in the steady state:

  | edge × placement | N=10 | N=100 |
  |---|---:|---:|
  | round-robin × `randomPlacementPolicy()` (default) | 0.12 | 0.01 |
  | hash token × `consistentHashPolicy()` | 0.09 | 0.01 |
  | hash token × `preferLocalPolicy()` | **1.00** | **1.00** |

  The middle row is an anti-pattern rather than a middle ground, and it
  settles an open question on #84: deterministic placement does not help
  here. The edge's hash and the cluster's rendezvous hash are different
  functions over different sets, so they disagree on most keys and guarantee
  a hop for every actor. Caller affinity is what composes; determinism is
  not a precondition for it. The shipped default stays
  `randomPlacementPolicy()` pending the warm-locality arms in stage 4.

  Known limits, both documented rather than papered over: `$live#subscribe`
  carries no token (one held-open response fans out to many actors, so there
  is no single owner to route to — sharding it would defeat the reason it
  exists), and an actor that is already hot does not follow a changed LB pool
  until it deactivates, because `preferLocalPolicy()` only applies to new
  activations.

- **`@sigx/actors` no longer depends on `sigx`** (#122). `./app` imported
  the umbrella for six symbols and four types; all of them come from
  `@sigx/runtime-core` (and `@sigx/runtime-core/internals`), which was
  already an optional peer. `sigx` re-exports exactly those, so the types
  are identical and **no consumer sees a difference**.

  The umbrella's first line is `import '@sigx/runtime-dom/platform'`, so
  depending on it dragged the DOM runtime in behind it. That is wrong for
  every non-browser consumer this runtime is meant to serve — a terminal
  app, a Lynx app, a headless host — none of which have a DOM, and none of
  which used a single DOM API through this package.

  `sigx` is gone from `peerDependencies`; it stays a devDependency, because
  the tests that mount a real sigx app and assert the plugin works there
  are precisely the integration worth keeping, and tests are not shipped.
  The build keeps `sigx` external as a guard, so a reappearing import stays
  unbundled rather than silently shipping.

  Incidentally smaller: the `@sigx/actors/app` bundle measures 2.92 kB
  against its 3.5 kB budget. Its size-limit `ignore` now names
  `@sigx/runtime-core`, which is what it actually links against.

### Fixed

- **A `streams:` consumer that disconnects mid-pull can now tear the body
  down** (#71). A body parked inside `ctx.changes()` is suspended at an
  INTERNAL await, and the spec *queues* `gen.return()` there: the generator
  is never resumed, so the feed's own `return()` — the one thing that would
  wake it — was never called. `iterator.return()` therefore never settled,
  the stream's keep-alive ref was never released, and idle sweeping skipped
  that activation for the life of the process. A quiet actor never escaped
  it, because only a mutation could have unparked the body.

  The subscription now has to be closable from OUTSIDE the generator, which
  needs to know which feeds that body opened. So the `streams:` table is
  built **per subscription** rather than per activation, each with a context
  of its own: a shared context cannot tell two concurrent bodies apart,
  since a body resumes from its internal await in a microtask of its own and
  no "currently running stream" flag can be trusted. The factory is a pure
  table constructor by contract — it must not touch `ctx` while constructing,
  and `defineActor` already calls it a second time to read `streamNames` —
  so this costs a handful of closures per subscription and changes nothing
  an actor author writes.

  Disconnect still runs the body's `finally`: closing the feed unwinds the
  body rather than abandoning it. A body is lazy, so it can reach its first
  `ctx.changes()` *after* the disconnect has run — a feed opened then is born
  closed, which makes teardown independent of the order the two land in.
- **`metrics()` caps no longer fail open on a non-finite value** (#109).
  `Math.floor(NaN)` is `NaN` and every comparison against it is false, so
  `maxTypes: NaN` meant the `'(other)'` fold never fired and the per-type map
  grew one bucket per distinct type — three histograms each — for the life of
  the process. `maxMethods` was the same and worse, since methods multiply
  types, and `recentErrors: NaN` left the ring untrimmed.

  Exactly the unbounded growth the caps exist to prevent, and it looks like
  healthy behaviour right up until the heap goes. All three now go through
  `resolveLimit()`, which #108 added for the same bug in `host.activations()`.
- **A misdeclared `placement` is now refused instead of silently ignored**
  (#86). `ActorOptions.placement` is typed `ActorPlacementStrategy`, which
  requires only an optional `name` — so a strategy with no `choose()`
  type-checked cleanly, was dropped with a `__DEV__`-only warning, and in
  production placed actors somewhere other than where their author declared,
  with nothing pointing at the cause.

  The runtime genuinely could not do better: the type is deliberately opaque
  (choosing a host needs a membership view, and core must not depend on
  `./cluster`), so it could not tell a strategy meant for ANOTHER backend from
  one meant for it but broken.

  `ActorPlacementStrategy` gains an optional **`backend`** tag, which the
  cluster's own factories set. That turns one case into three: mine and valid;
  tagged for someone else (ignored **silently, on the tag alone** — a foreign strategy that happens to expose a `choose()` is still not run here, which is what the opacity exists
  for); and mine, or untagged and unrecognised — now a **thrown error, in
  production too**.

  **Behaviour change:** an untagged strategy without `choose()` used to fall
  back to the configured policy and now throws. A strategy for another backend
  should set `backend` to keep being ignored.

### Documentation

- **Bounding the host-to-host connection pool** (#97, closes #89). Node's
  global `fetch` uses an unbounded undici pool, measured at **two connections
  per in-flight request** — ~12 600 sockets per host at concurrency 64 across
  99 peers. A four-line recipe through the existing `fetch` seam caps it.

  **Size the cap to your per-peer concurrency.** At `connections: 64` under
  concurrency 64 the socket count halves *and* throughput improves ~6%, which
  shows the default's extra connection per in-flight request is waste rather
  than headroom. Going lower trades throughput steeply — `connections: 8` at
  the same concurrency costs about 3× — and is only worth it when file
  descriptors are the real constraint.

  No API and no dependency: `undici` stays out of this package, and the seam
  that makes the recipe possible already shipped in #92.

  **HTTP/2 does not help today** — `allowH2` measures identical to plain
  keep-alive at every pool size, because `createAppHandler` serves over
  `node:http`, which is HTTP/1.1 only. Multiplexing would need a `node:http2`
  server first, for the same socket reduction the pool cap already delivers.

  **Session tokens were measured and declined.** Replacing the per-call HMAC
  looked worth 3.35× in-process; over a real socket it is 1.19×, and a session
  token authorises any call for its window where the current signature is bound
  to a specific symbol and callId. 19% does not buy that trade.

### Added

- **`dispatchWatch` and the `$live` mount — the server half of the live
  layer** (#68, half of #10). A subscription asks for a METHOD RESULT
  (`total()`, `recent(20)`), but `ctx.changes()` yields STATE, and only the
  actor can derive one from the other. So a watch re-invokes the read after
  every mutating turn rather than pushing state the client would have to
  re-derive.

  Two properties are the design, not optimisations. **One loop per
  `(method, args)`**: fifty subscribers to the same read cost one
  re-invocation per turn, not fifty — and since the mailbox is
  single-threaded, the naive version would serialise those turns and turn a
  popular actor into a self-inflicted load problem. **Keep-alive**: an open
  watch counts as activity, so idle collection cannot deactivate an actor
  out from under a live subscriber. Both are asserted on observed turn
  counts.

  Reads are trailing-throttled (default 50 ms) through the `ActorScheduler`
  seam, so a burst of mutations coalesces into one read and a runtime
  without background timers can still drive it.

  `$live#subscribe` carries many subscriptions on one held-open NDJSON
  response — a page with twelve live components holds one connection, not
  twelve. It is a synthesized serverFn like any actor method, so it inherits
  origin policy, the wire codec, `ServerFnError` masking, body caps and the
  request scope. Failure is **per subscription**: a guard rejecting one
  widget yields an error frame at that index and the rest keep streaming.
  An error frame carries the status the same call would have got as a unary
  request — an unknown method is a 404 either way, not a 500 on one path and
  a 404 on the other. An otherwise-silent connection emits a `{"p":1}` ping
  every 30 s, so the proxies and mobile NATs between a browser and the host
  do not reap it between mutations.

  Guards run at subscribe time, the same chain a unary call runs, so a watch
  exposes nothing a polling client could not already read — which is why
  there is deliberately no per-actor `live:` opt-in to add.

  A disconnect releases everything it held, on a quiet actor as much as a
  busy one. That is not free: the frame loop parks on a promise, and an
  async generator suspended at an `await` cannot see `return()` until it
  next reaches a `yield`, so a subscription to an actor that never changes
  would otherwise pin its activation for the life of the process — and tabs
  close far more often than actors mutate.

  `defineActor` now refuses types starting with `$` or `@`, reserving the
  runtime's wire (`$live`) and data-key (`@actor`) namespaces while nothing
  has shipped.

- **A watch crosses a cluster hop** (#69). A subscription now works for an
  actor placed on ANY host, over all three transports, rather than only for
  the ones that happened to land on the host serving the connection.

  A watch is an ordinary read dispatched in watch mode, so the receiving
  host cannot tell what is being asked of it from `Counter#read` alone —
  `streamNames` separates streams from calls, and a watch is neither. The
  intent therefore travels, as a reserved symbol prefix:
  `$watch:Counter#read`.

  **On the symbol rather than the call envelope, deliberately.** The
  per-call HMAC signs `proto\nsymbol\ncallId\ntimestamp` — the envelope is
  not part of it. Carried there, anyone who could reach the internal mount
  could turn a plain read into a keep-alive-pinning subscription without
  disturbing the signature. Carried on the symbol, they cannot. The prefix
  is free to reserve because `defineActor` already refuses a `$`-prefixed
  type, and the mount already answers reserved symbols before any
  definition lookup — the same shape `$sigx:host#stats` uses.

  It also keeps both wire paths honest about the same thing. On the socket
  transports `FLAG_STREAM` goes on meaning "the reply is a chunk stream",
  which a watch's reply genuinely is, so credit, `CANCEL` and the rest
  apply unchanged. No new frame type and no protocol bump.

  `throttleMs` rides the payload beside the actor key, since it is per
  SUBSCRIPTION rather than per actor — and is validated on arrival rather
  than trusted: a non-finite value compares false against every bound and
  would disable throttling outright, the same fail-open shape as the
  `metrics()` caps.

  **A disconnect releases the owner's keep-alive**, which is the part worth
  doing deliberately: the owner has no other way to learn the subscriber
  has gone. The serving generator spends its life parked at an `await`,
  where an async generator cannot observe `return()` at all, so the watch
  loop honours the CALLER's abort signal directly. Without that, a dropped
  connection pinned an activation on a host with no idea anyone had left —
  forever, for a quiet actor.

  Both properties are in the shared transport conformance suite, so HTTP,
  TCP and WebSocket are held to them together rather than one at a time.
  New counters `remoteWatches` and `inboundWatches` report the two sides.

- **`Host.activations()` — the live actor list** (#101). Bounded and
  sorted: `{ type, key, queued, ageMs, idleMs, keptAlive }`, with
  `sortBy: 'queued' | 'age' | 'idle'`, a `type` filter and a `limit`
  defaulting to 100.

  `stats()` could report that a host held 12,000 activations with 400
  queued turns and could not say WHERE, which is the only question worth
  asking at that point. The activation directory was private and nothing
  else exposed a single actor key, so "top actors by queue depth" was not a
  panel anyone could build.

  Ties break on the actor id, so the order is **stable between polls** — a
  table that reshuffles equal rows at 1 Hz is unreadable. The walk is
  O(activations) and allocates per candidate, hence the low default limit:
  this is a top-N view, not an export.

  `ageMs` is monotonic while `idleMs` is wall-clock, and the difference is
  deliberate: age is a duration and must survive an NTP step, whereas idle
  is compared against `idleAfterMs` by the sweeper, which genuinely wants
  wall time. Both clamp at 0, so a clock stepped backwards cannot report a
  actor last used in the future.

- **`HostStats.transitional`** (#101) — `{ activating, deactivating }`.
  Slots mid-transition carry no activation, so `stats()` skipped them
  entirely; a host in the middle of an activation storm therefore read as
  **idle**, at precisely the moment an operator was looking at it. Counted
  separately rather than folded into `activations`, which still means
  "settled".

- **`OpsSnapshot.activations`** (#101) — the hottest actors on the ops
  endpoint, deepest mailbox first, bounded by `ops({ activations })`
  (default 20, `0` omits the list). Small by default because the walk is
  O(activations) and this endpoint gets polled — and because actor keys are
  the one field in the snapshot that can be personal data, so it is worth
  being able to drop them without giving up the rest.

- **`metrics()` breaks down per METHOD** (#101) — `snapshot().byMethod`,
  keyed `Type#method`, carrying the same five numbers as `byType`.

  The data was always there and always discarded: both `useDispatch` and
  `observeTurns` receive `method` and neither kept it. `byType` tells you a
  type is slow and never which of its methods is, and the queue/turn split
  is most useful exactly here — within one type, a hot actor and one slow
  method look identical until the methods are separated.

  Its own cap (`maxMethods`, default 256) rather than sharing `maxTypes`,
  because methods MULTIPLY types: 64 types under the type cap would leave
  under one method each and the breakdown would be almost entirely
  `'(other)'` on an ordinary app. `maxMethods: 0` disables it,
  `histograms: false` nulls its distributions, and `reset()` drops the map
  like `byType`.

  **It costs ~3.5% per dispatch**, which takes `metrics()`'s own overhead
  from the −28% this changelog recorded under #79 to **−30%** on a `noop`
  dispatch; the README table is updated rather than left flattering.
  `maxMethods: 0` gets the old figure back.

  The map is NESTED (type → method → bucket) rather than one map keyed
  `'Type#method'`, which is why it is only 3.5%. The flat form reads better
  but composes a string on every dispatch AND every turn; measured, it cost
  roughly three times as much. Two map lookups beat one allocation, and the
  key is now composed once per snapshot instead.

  The figure comes from a PAIRED measurement — both builds loaded into ONE
  process with their rounds interleaved (main 1425.7ns/call vs 1476.0ns on
  this branch, inert-plugin controls agreeing to 0.2%). The usual
  separate-process comparison could not resolve it at all: the machine
  drifts ~10% run to run, comfortably larger than the effect, and its
  controls disagreed by more than the thing being measured. The absolute
  ops/s in the README table are the #79 machine's, rescaled by this ratio
  rather than re-measured — the percentages are what was actually observed.

- **`metrics()` counts error KINDS** (#101) — `snapshot().errors.byKind`
  over `ActorErrorKind` (`'call-timeout'`, `'wrong-host'`,
  `'state-conflict'`, `'unreachable'`, …) plus `'(unknown)'` for anything an
  actor method threw itself, classified with the existing `isActorError()`
  brand check so wire-recreated errors count correctly.

  `calls.failed` was a single scalar: it said a host was failing and nothing
  about what was wrong with it. Those are different questions with different
  answers — a rising `'unreachable'` is network or membership, a rising
  `'(unknown)'` is your code.

  `errors.recent` keeps the last few as
  `{ at, type, method, kind, message }` (default 32, `recentErrors: 0`
  disables). **Message only** — no args and no state, because this travels
  over `/_sigx/ops` and a failing call's arguments are exactly where the
  secrets are. The counts are unbounded; only the samples are capped. The
  snapshot hands out a copy, so a held snapshot cannot watch the ring
  mutate.

- **`ops()` — the authenticated ops endpoint** (#101). `GET /_sigx/ops`
  serves an `OpsSnapshot`
  (`{ v, at, uptimeMs, stats, activations, health, ops }`);
  `GET /_sigx/ops/cluster` serves a `clusterStats()` fan-out. Contributed as
  ordinary plugin routes, so every mount picks them up.

  It exists because everything M7 and `metrics()` collect was unreachable
  from outside the process. `metrics()` contributes no route,
  `clusterStats()` takes a placement OBJECT rather than a URL, and the one
  remote surface that did exist — the `$sigx:host#stats` symbol — is
  cluster-only, carries no latency distributions, and disappears entirely in
  a cluster of socket-only transports. A single-node host was completely
  unobservable from outside.

  Why a new route rather than more of `/_sigx/health`: health is probed by a
  load balancer that HAS no cluster secret, so it cannot be authenticated,
  so it must not name your actor types. Ops is read by an operator who does,
  so it can carry `perType`, the whole check map and the cluster topology.
  Extending health would have meant either withholding the useful half or
  publishing the deployment to anyone who can reach the port.

  **The secret is mandatory** — `ops()` throws at construction without one
  unless `__DEV__`, rather than defaulting to open, because an ops endpoint
  that is unauthenticated by omission is worse than none: nothing in the
  response tells you it is happening. `401` covers a missing and a wrong
  token identically, and auth runs *before* the path split so an
  unauthenticated caller cannot enumerate paths. Comparison is constant-time.
  In dev, omitting the secret serves open and warns once.

  The cluster fan-out is wired by the caller as a thunk
  (`cluster: (signal) => clusterStats(placement, { signal })`) rather than
  discovered: `ops()` lives in `@sigx/actors/host` and `clusterStats` in
  `@sigx/actors/cluster`, and a single-node host must not pay for the cluster
  bundle to have an ops endpoint. It also leaves `timeoutMs`/`concurrency`
  where they already are. Unwired, `{base}/cluster` answers 404. A collector
  that fails answers 503 carrying the reason rather than an empty report,
  which would read as a healthy cluster of zero hosts.

- **`PluginRegistry.reportOps(name, provider)` / `registry.ops()`** (#101) —
  the contribution seam, the counterpart to `reportHealth` / `health()` and
  deliberately identical in shape: unique names (a clash throws at setup
  naming both plugins), and a LIVE aggregate read at call time, so `.use()`
  order does not matter.

  Providers run per read and must stay sync — the endpoint you reach for when
  a host is already unwell must not be able to hang. A throwing provider is
  caught and its section replaced with `{ error }` rather than failing the
  snapshot; the section stays PRESENT, because an absent key would read as
  "this plugin contributes nothing", which is the opposite of the truth.

  `metrics()` contributes under `'metrics'` and `cluster()` contributes
  `placement.report()` under `'cluster'`, so `.use(metrics()).use(ops(…))`
  needs no wiring between them. Registration costs only a closure: an app
  with no `ops()` never calls a provider.

- **A transport conformance suite** (#93), for contributors — **not a
  published import**. `transportConformance` is the set of cases every
  `HostTransport` must pass, plus the harness interface a transport supplies.
  It lives at `packages/actors/src/cluster/testing.ts` and is reachable inside
  this workspace as `@sigx/actors/cluster/testing` via a tsconfig/vitest
  alias; the subpath is deliberately absent from `package.json` exports, so
  it cannot be imported from outside the repo until it is promoted.
  Sixteen cases covering the
  codec round-trip, single activation, streams and their cancellation,
  deadline propagation, wrong-host convergence, unreachable-vs-crash, error
  re-branding, the ops channel, auth rejection, the cross-host call chain,
  graceful handoff, and link hygiene.

  It ships green against `httpTransport()` **before a second transport
  exists**, which is the point: a suite written alongside a new transport
  describes that transport's habits, while one the shipped default already
  satisfies describes the contract.

  The governing rule is **assert on `kind`, never on an HTTP status** — a
  status is one encoding of a kind, which is what makes "what is 421 over
  TCP?" answerable. The one number that still matters is the ops surface,
  where `clusterStats` classifies peer failures by status code, and that has
  its own case.

  It imports no test framework: cases are descriptors whose `run()` throws,
  so a transport package can drive them from any runner. Cases a transport
  cannot express (link hygiene, for a connectionless one) report as
  **skipped with a reason** rather than passing vacuously.

  Being unpublished — no `package.json` export, no Vite entry — the shipped
  surface and `pnpm size` are untouched. An out-of-repo transport package
  cannot import it yet; promoting the subpath is a separate, deliberate step.

- **The host-to-host transport is a seam** (#91). `cluster({ transport })`
  takes a `HostTransportFactory`, or a LIST of them as a fallback chain.
  `httpTransport()` is the default and nothing about a default-configured
  cluster changes.

  A `HostTransport` owns **both halves** of a link — the outbound dispatcher
  for a peer, and the listener that answers peers dispatching at us. That is
  why it is a lifecycle object rather than one method: a connection-oriented
  transport has sockets to open, departed peers to forget, and a port to
  advertise. It starts inside `placement.start()`, *before* the membership
  join, so no peer can learn an address before something answers on it —
  which is also why this is an option on `cluster()` rather than a plugin of
  its own: no registry hook runs early enough.

  `dispatcherFor(target)` may return **`null`**, meaning "I publish no
  address for that peer". That is a routing answer, not a failure, and it is
  what makes a mixed-transport cluster expressible — which is the only way a
  new transport ever gets deployed. During the rolling deploy that
  introduces one, half the cluster advertises it and the other half does
  not, and both halves stay reachable.

  A **single** transport is strict: a peer advertising no address for it is
  unreachable, loudly. There is no implicit HTTP fallback, because a silent
  one means you deploy a transport, benchmark it, and measure the old one
  without ever knowing. Fallbacks that do happen are counted
  (`transportFallbacks`), reported (`HostReport.transports`) and dev-warned
  once per peer.

  Because the internal mount is now just a route a transport declares, a
  cluster of only socket transports has **no internal HTTP endpoint at all**.
  The public actor wire is unaffected.

- **`HostDescriptor.addresses`** (#91) — peer-reachable address per
  transport, keyed by transport name. Optional, and absent reads as
  "HTTP only", so a host from a build predating the field stays reachable.
  It round-trips through the existing providers untouched: both the Redis
  and memory providers store the descriptor whole.

- **`resolveHostSymbol`, `hostRuntime`, `toHostWireError`,
  `fromHostWireError`, `hostWireCodec`** (#91) — the compat-critical
  plumbing a transport needs, exported so an out-of-package transport uses
  the same codec and the same error mapping instead of re-deriving them. A
  transport that `JSON.stringify`s raw values would silently drop every
  registered codec handler and re-open `__proto__`; "the error is the same
  error" is likewise a contract, not a nicety, since a caller must not be
  able to tell a remote hop from a local dispatch.

### Changed

- **`createHostTransport()` / `HostTransportOptions` are removed** (#91),
  replaced by `httpTransport()`. They were exported but documented nowhere
  and had a single call site.
- **`@sigx/actors/cluster` size limit raised 8 KB → 9 KB** (#91). The seam
  costs ~0.9 KB: the chain walk, per-transport addressing, and the shared
  error mapping. Recorded rather than shaved.

- **`health()` — liveness and readiness endpoints** (#38). `GET
  /_sigx/health` and `GET /_sigx/health/ready`, contributed as ordinary
  plugin routes so every mount picks them up.

  The point is that the two answers are allowed to **disagree**. A graceful
  `host.stop()` announces `leaving` before activations hand off, so for the
  whole handoff window the host is `200 live` and `503 not-ready`: drain it,
  do not restart it. Restarting is what turns a rolling deploy into an
  outage, and until now nothing exposed that window to a load balancer.

  The check that would otherwise be invisible is **`fenced`**. A host that
  loses its membership heartbeat self-fences and refuses every activation,
  but `#fence()` deliberately leaves the *published* status at `active` — so
  a balancer would keep feeding a black hole. Readiness reads the fenced
  state, not the descriptor.

  Before `start()` the whole mount answers 503, including liveness: a route
  only runs on a started host, so serving at all is the liveness signal. The
  README carries the Kubernetes manifest (`startupProbe` covers a slow boot
  and suppresses liveness until it passes). The routes cannot be
  authenticated — a kubelet cannot sign the cluster HMAC — so they are
  documented as internal-only. The body never includes a `perType` breakdown
  or anything key-shaped, so it cannot name your actor types;
  `health({ detail: false })` drops the `checks` map and the gauges too,
  leaving the status code and a bare `{ status, uptimeMs }`.

- **`PluginRegistry.reportHealth(name, check)` / `registry.health()`** (#38)
  — the readiness seam. Every contributed check must pass; all of them are
  evaluated, so a failing probe names every reason rather than the first,
  and a check that throws is reported not-ready with its message rather than
  500ing the endpoint meant to diagnose it. `cluster()` registers its own,
  which is why `health()` needs no cluster wiring and works single-node
  unchanged. `registry.health()` reads the aggregate at call time, so
  `.use()` order does not matter.

- **`clusterStats()` — one read of the whole cluster** (#38). Fans out
  across the membership view and returns per-host activations, queue depths,
  per-type counts, the reminder-shard ownership map and the counters below,
  plus cluster-wide totals.

  It travels as a reserved symbol (`$sigx:host#stats`) on the **existing**
  internal mount rather than as a new route, which is the security-relevant
  choice: it inherits the per-request HMAC, the envelope, the codec and the
  body cap, so there is no second and eventually-unauthenticated way to read
  your topology. Answered before any definition lookup, so a user actor
  cannot shadow it.

  It **never throws because a peer is sick** — a host that times out,
  refuses the secret or predates this build lands in `unreachable` with a
  classified `reason`, and `partial: true` marks the totals as a lower
  bound. Peers are queried with bounded concurrency (default 16) so a
  100-host fan-out is waves, not 100 simultaneous connections, and the
  collector answers for itself in process rather than looping back through
  an address it may not be able to reach.

  `reminderShards` maps each shard to the hosts *claiming* it, built from
  the reports rather than recomputed centrally — two claimants means views
  diverged, an empty list means nothing is ticking that shard, and the
  distinct host count is how many hosts do reminder work at all.

- **`ClusterPlacement.counters()`, `view()`, `report()`** (#38) — pull-based
  routing, directory, membership and auth counters. Always on: these are
  integer increments on paths already doing network and directory work, and
  the local fast path (`dispatcherFor` for a claimed actor) is not
  instrumented at all, so it stays byte for byte what it was.

  The rule that makes them safe to aggregate: every counter moves on the one
  host where its event happened, and the two sides of a cross-host call get
  **different names** — `remoteDispatches` on the caller, `inboundDispatches`
  on the owner. They are reported side by side and never summed; the gap
  between them is itself the signal. `authFailures` is the one with no prior
  visibility at all: a 403 on the internal mount was completely silent, and
  during a secret rotation it is the only sign that half the cluster has not
  rotated yet.

- **`metrics()` in a cluster is split, not doubled** (#38) — documented, and
  now pinned by a test. It had been assumed that a cross-host call was
  counted once on the caller and again on the owner. It is not, and the
  reason is structural: `compositePlacement.bind()` hands the placement the
  **raw** local dispatcher and `dispatchInbound` calls it directly, so an
  inbound hop never passes through `useDispatch`. A cross-host call is
  counted once, on the host that originated it, while `queueMs`/`turnMs` are
  recorded on the host that executed it — so summing `calls.total` across
  hosts gives the true cluster-wide number. The test exists so a refactor
  cannot quietly turn the split into a real double count.

- **`metrics()` — pull-based observability** (#79). A plugin that counts
  calls, failures, activation churn, storage operations and etag conflicts,
  and reports latency distributions, read via `snapshot()`. No exporter, no
  push pipeline, no metrics-library dependency.

  The number it exists for is the split between **`queueMs`** (waiting for
  the mailbox) and **`turnMs`** (holding it). They are the two halves of a
  call's latency and mean opposite things — a slow method versus a hot
  actor — and a dispatch middleware, which only sees the sum, cannot tell
  them apart.

  Collection is switchable at runtime — `enable()`, `disable()`, `enabled`,
  and `metrics({ enabled: false })` to attach without collecting. `disable()`
  drops the turn subscription rather than returning early inside it, which
  is what makes off actually cheap: the runtime only times turns while an
  observer is attached. Counters freeze rather than clearing.

  Costs, on a `noop` dispatch (the cheapest call there is), each config in
  its own process against an inert-plugin control: `enabled: false` and the
  inert control are both indistinguishable from no plugin,
  `metrics({ histograms: false })` −8%, `metrics()` −28%. Not attaching it
  at all is free — the dispatch path is unchanged with no observer, verified
  against the benchmark suite. The −28% is ~190ns per call and looks large
  only because the measured call does nothing: for an actor whose turn takes
  100µs it is under 0.2%.

  Durations come from `performance.now()`, not `Date.now()`: a wall clock
  stepped backwards by NTP or a VM host would otherwise hand observers
  negative queue waits.

- **`PluginRegistry.observeTurns(observer)`** (#79) — the seam behind that
  split, available to any plugin:
  `(ref, method, queuedMs, elapsedMs, failed) => void`. Fires for
  dispatched turns only, including reminder delivery; volatile `ctx.timer`
  ticks, write-behind flushes and reentrant inline calls are excluded,
  since none of them has a caller waiting. A throwing observer is swallowed
  and dev-logged rather than failing the turn. `createHost({ onTurn })`
  exposes the same thing to hand-rolled hosts.

- **`Host.observeTurns(observer)`** (#79) — subscribe imperatively; returns
  an unsubscribe. When the last observer leaves, the runtime stops taking
  per-turn timestamps entirely, so observation can be switched on for an
  investigation and off again without leaving a cost behind.
  `PluginRegistry.observeTurns` returns the same unsubscribe.

- **Actors can ship from npm packages** (#73): the runtime already allowed
  it, but the build did not — `sigxActors()` only transforms first-party
  source, so a packaged actor was never client-swapped and its
  implementation would have reached the browser. A package now performs
  the swap itself through its exports map (`browser` → `__actorRef(...)`,
  server conditions → the definition), which is the same swap the Vite
  plugin does for `*.actor.ts`, by static resolution instead — so it works
  with any bundler.

  Two safety nets for what the consuming build cannot see: two different
  actors claiming one `type` now throw at startup (it is the wire,
  directory and storage key, so the loser's callers would silently reach
  the winner's state), and a registered actor declaring neither `use` nor
  `unguarded` dev-warns, since `requireGuards` cannot inspect a package.
  Registering the same definition twice stays fine.

- **The Vite plugin supplies the actor registry** (#61):
  `defineActorApp({ actors })` is now optional, and `sigxActors({ app })`
  hands over the registry it already builds. An app module therefore
  imports nothing Vite-specific and loads under plain Node too — so a
  production entry can share the very module the dev server runs
  (`app.withActors([Counter]).start()`), and actor modules can safely
  import the bound `defineActor` from it, which previously made them
  Vite-only.

  Dev uses its module-runner loaders rather than `virtual:sigx-actors`,
  which is what keeps HMR working. `withActors` throws if the app already
  declared `actors`, so a host cannot silently replace the author's
  configuration, and `start()` without a registry says so plainly.

- **`examples/chat` — actors in a real SignalX app** (#60). It runs the
  composition this repo had never actually run before: `sigx()` +
  `sigxServer()` + `sigxActors()` in one Vite build, a guarded actor read
  resolving during SSR, a serverFn beside the actor endpoint calling an
  actor in-process, and a first paint that costs **one request — the
  document**. Dev and production start the same app module.
  `examples/counter` stays as the framework-free demo.

- **Invalidation: a write refreshes the reads it staled** (#66).
  `useActorAction` now invalidates on success, so the manual
  `await read.refresh()` after every mutation is gone:

  ```ts
  const messages = useActorState(RoomActor, room, 'recent', 20);
  const post = useActorAction(RoomActor, room, 'post');
  await post.run([text]);   // `messages` refreshes itself
  ```

  The default pattern is the WHOLE-ACTOR prefix `actorKey(def, key)`, not a
  guess at which methods a write touched — `post()` changing what `recent()`
  returns is the normal case, and under-invalidating leaves stale data on
  screen, which is worse than one redundant refetch. Override with
  `invalidates` (a pattern list, a function of the result and key, or
  `false`).

  Reads register a key GETTER, not a key, so a reactive key is matched at
  its current value — a cell registered by value would silently stop
  matching the moment its key changed, exactly where a stale row is most
  visible. Invalidation also drops the page-payload entry, so a component
  that unmounts and remounts cannot restore what SSR wrote before the write.

  The registry is per app, like the rest of `actorsPlugin()`'s state: two
  concurrent SSR renders must not share one.

  `keyMatches` is reimplemented here (`@sigx/actors/app`) rather than
  imported — core's lives at `@sigx/server/dist/server/key-match.ts`, is
  exported from no subpath, and already carries a note that it is duplicated
  from `@sigx/cache`. `key-match.test.ts` pins the semantics against the same
  table so the three copies cannot drift silently.

  Server-declared cross-actor invalidation (`invalidates:` on `defineActor`,
  stamped as `__sigxInvalidates`) is deliberately NOT in this change: core
  calls that hook with `rq._input`, which only the serverFn validation
  pipeline sets and actors never runs, so it needs a decision about depending
  on a core internal — worth making on its own rather than behind a feature.

- **`actorKey()`, `useActorState()`, `useActorAction()`** (#57): actor reads
  and writes as component data.

  ```ts
  const total = useActorState(CartActor, id, 'total');
  const add = useActorAction(CartActor, id, 'add');
  return () => total.match({ ready: (n) => `${n} items`, pending: () => '…' });
  ```

  Built **on `useData`** rather than on core's `AsyncEngine` seam. That seam
  is app-exclusive and `defaultAsyncEngine` exists so a pack can delegate
  reads it has no policy for — that pack is `@sigx/cache`, and actors taking
  the token would make `app.use(cachePlugin()).use(actorsPlugin())`
  unresolvable. Building on `useData` also inherits the four things that
  matter, at no cost: canonical tuple keys, the SSR provider, page-payload
  restore, and **in-flight dedupe by key** — so ten components reading one
  actor make one dispatch, not ten.

  The payoff is SSR seeding with no new code: the read resolves during the
  server render (in-process through the host seam, guards and all),
  serializes into the page under its canonical actor key, and the browser
  restores it **without refetching**. There is an end-to-end test for exactly
  that — render a document over a real host, then mount over the emitted
  payload with `fetch` spied, asserting zero dispatches and zero requests.

  `actorKey()` is isomorphic and lives in the root entry, so server-side
  declarations use the same function the browser does; a test pins that a
  definition and a build-swapped client ref produce byte-identical tuples,
  since SSR seeding, hydration, invalidation and live-subscription identity
  all rest on it. It returns a **tuple**, not core's `__sigxKey` string:
  `['@actor','Cart','c1']` prefix-matches every read of that cart, which is
  the granularity invalidation needs and which a single-element key cannot
  express.

  Key arguments are constrained to JSON primitives **at compile time**, not
  just checked at runtime: a method parameter that cannot canonicalize makes
  that argument position a type error naming the rule. Optional primitives
  stay usable, and `useActorAction` is deliberately unconstrained — a
  mutation's arguments are not key material.

- **Pluggable client transports** (#55): the client proxy no longer speaks
  HTTP itself — it delegates to an `ActorTransport` (`call` / `stream`, plus
  an optional `live()` push channel), so batching, a different auth scheme,
  or a protocol other than fetch drops in without any call site changing.
  `fetchTransport()` is the default and implements exactly the wire contract
  that shipped before; `configureActors()` now takes either a full transport
  or — unchanged, the common case — a config object as sugar for
  `fetchTransport(config)`. Per-call `init.endpoint` carries the endpoint the
  build baked into the ref, so `configureActors({ headers })` still overrides
  headers alone without restating where the server is.

  Server-side, transports were already pluggable: a plugin contributes a
  mount through `PluginRegistry.route()`. One limit worth naming:
  `ActorRoute.handle` returns a `Response` and so cannot express a Node
  WebSocket upgrade, which needs the raw socket (Workers can express it).

- **`@sigx/actors/app` and `actorsPlugin()`** (#55): the sigx app
  integration, and the only entry that imports `sigx` — the isomorphic root,
  `./client`, `./host`, `./server`, `./node` and `./cluster` stay free of the
  framework, so a headless Worker deployment never drags the runtime in.
  `sigx` and `@sigx/runtime-core` are therefore **optional** peer
  dependencies.

  The plugin exists for something `configureActors()` cannot cover: a server
  app installs per request, and the transport seam is page-global, so a
  server writing to it would let one request's config bleed into another's
  concurrent render. It installs a transport on live clients only (matching
  `serverPlugin`'s posture exactly), tears it down on `app.unmount()` without
  stripping a transport another app has since installed, and provides the
  per-app context the coming hooks resolve through. It is optional — `actor()`
  works without it. It deliberately does not register codec type handlers:
  the actor wire reads `__SIGX_SERVERFN_CODEC__` directly, so one
  `serverPlugin({ types })` already covers both wires.

- **Pluggable durable reminders** (#50): `ReminderService` was constructed
  directly by `createHost`, so its sharded design was the only one
  possible. Reminders are now an `ActorReminders` seam
  (`createHost`/`defineActorApp`'s `reminders` option), with
  `shardedReminders()` as the unchanged default. An implementation
  receives `bind({ storage, scheduler, tickMs, ownsShard, deliver })` once
  before `start()`, mirroring `ActorPlacement.bind()` — so it is handed the
  host's real (plugin-decorated) storage and clock rather than being told
  them twice.

  The shard table assumes many actors per host. Under Cloudflare's
  one-Durable-Object-per-actor model each actor's reminders belong in its
  own DO storage, fired by its own alarm — nothing to shard, nothing to
  poll. The whole host-level reminder suite passes unchanged.

- **`ctx.changes({ initial: true })`** (#53): queues the current snapshot as
  the feed's first value, synchronously, in the same call that registers the
  subscription — so `yield* ctx.changes({ initial: true })` replaces the
  `yield ctx.snapshot()` prologue with no gap. The prologue subscribed only
  after the consumer resumed past that first yield, silently losing every
  mutation in between and yielding an already-stale snapshot; the wire suite
  had to paper over it with a settle delay. Plain `changes()` is unchanged.

- **Dev warning when a `streams:` body reads live `ctx.state`** (#53). Stream
  bodies run detached from the mailbox, so a turn can mutate underneath the
  read. The warning names the actor and points at `ctx.snapshot()` /
  `ctx.changes({ initial: true })`.

- **The clock seam, and `createFetchHandler`** (#48): every recurring or
  delayed job — the idle sweeper, the reminder tick, `ctx.timer`, and
  write-behind flushes — now runs through an `ActorScheduler`
  (`createHost`/`defineActorApp`'s `scheduler` option) instead of calling
  `setInterval` directly. `timerScheduler()` is the default and behaves
  exactly as before; `manualScheduler()` drives time by hand in tests
  (`advance(ms)`, `now`, `pending`), replacing the suite-wide
  `{ sweepIntervalMs: 60_000 }` trick that merely pushed background work
  past the end of each test. The seam is what makes a runtime with no
  background execution possible: a Cloudflare Worker only runs while
  handling a request, so an interval registered at startup never fires.

  `createFetchHandler(app)` in `@sigx/actors/server` is the portable
  entry — the public endpoint plus every plugin route as one
  `(Request) => Response`, so Deno and Bun need no adapter at all.
  `createAppHandler` remains the Node mount and deliberately keeps core's
  connect adapter for the public endpoint rather than routing everything
  through the generic bridge.

  The seam is named `ActorScheduler`, not `Scheduler`: the latter is a DOM
  global (the Prioritized Task Scheduling API), and a file that forgot the
  import silently resolved to it.

- **Vite unification — `sigxActors({ app })`** (#46): dev and prod used to
  configure the same host twice in different languages. The plugin took
  `storage` as a root-relative PATH STRING whose module needed a magic
  named export, while the entry took real values — and the dev host was
  built as `createHost({ actors, storage })`, so **dev could not receive
  `placement`, `types`, `defaults`, or any plugin at all**: clustering,
  metrics and ctx extensions simply did not exist in dev.

  `sigxActors({ app: '/src/actors.app.ts' })` loads the very app module the
  production entry imports and starts it, so every setting and plugin is
  identical across the two. Plugin-contributed routes are mounted in dev as
  well, so a cluster's internal mount answers there exactly as it does under
  `createAppHandler`.

  The extractor now accepts `defineActor` imported from the app module, not
  only from `@sigx/actors` — required for M1's app-bound `defineActor`, and
  load-bearing for safety: an unextracted actor module is never
  client-swapped, so its implementation would ship to the browser.
  `mayDefineActors` takes hints for the same reason.

  New `@sigx/actors/vite-client` export carries ambient types for
  `virtual:sigx-actors` (reference it from your `env.d.ts`, as with
  `vite/client`).

- **`cluster()` as an app plugin, and a Node adapter for every mount**
  (#44): a clustered host used to repeat itself — `secret` went to both
  `clusterPlacement` and `handleHostRequest`, `internalBase` had to agree
  with `matchesHostRequest`'s default, and routing the internal mount was
  left to the entry, with nothing type-checking any of it.
  `defineActorApp({ actors, storage }).use(cluster({ providers, advertise,
  secret }))` states each value once and contributes the host-to-host mount
  as a route. `providers` is a named field rather than a spread, so the
  provider/config boundary is visible. `cluster()` exposes `.placement` for
  the operational primitives (`identity`, `descriptor()`, `migrate(ref)`).

  `createAppHandler(app)` in `@sigx/actors/node` mounts the public actor
  endpoint **and** every plugin route on one connect-style handler. It
  carries the `IncomingMessage`→`Request` bridge — header copying, body
  buffering, streamed responses with backpressure, and disconnect→abort —
  that every clustered deployment previously hand-wrote, because core's
  adapter resolves symbols and nothing in sigx exposed a general
  fetch-handler→Node bridge. Responses stream (NDJSON cross-host `watch()`
  depends on it); request bodies are buffered under the endpoint's existing
  byte cap.

  `clusterPlacement`, `handleHostRequest` and `matchesHostRequest` remain
  exported for hand-rolled mounts, and the entire cluster test suite passes
  unchanged against the plugin.

- **`defineActorApp` — the composition root and typed plugin model**
  (#41): `createHost` takes exactly one `placement` and one `storage`, and
  `ActorPlacement.bind()` → `PlacementBindings` was the only lifecycle-hook
  shape in the package — so two things that both wanted `beforeActivate`
  (a cluster directory and an audit log) could not coexist.
  `defineActorApp({ actors, storage, types, defaults }).use(plugin)` folds
  every plugin's contributions into the single placement, storage and
  context `createHost` already understands, so the layer is purely additive
  and `createHost` stays the documented primitive.

  A plugin's `setup(registry)` may `addTypeHandlers`, `decorateStorage`
  (chained, last registered outermost), `setPlacement` (**exclusive** — a
  second claim throws naming both plugins; takes a FACTORY run once the
  host exists, so a custom placement backend can resolve the per-type
  strategies actors declared), `onBeforeActivate` (in order;
  throwing still refuses the activation), `onAfterDeactivate` (reverse
  order; errors caught per hook), `useDispatch` (outside-in middleware over
  the resolved dispatcher), `onStart`/`onStop` (`onStop` runs *after* the
  drain, in reverse), `route` (collected on `app.routes` for adapters), and
  `extendContext`. A placement's own hooks **bracket** the plugins' — its
  claim runs first and its release last.

  `ActorPlugin<Ext>` carries the shape a plugin adds to `ctx`; `.use()`
  accumulates it, and the app-bound `defineActor`
  (`export const { defineActor } = app`) types those additions inside every
  actor — no global declaration merging, so they stay per-app. `ActorContext`
  gains an optional second type parameter (`ActorContext<S, Ext>`,
  defaulting to no additions, built on the new `ActorContextBase<S>`) and
  `createHost` gains an `extendContext` option for hand-rolled hosts.

- **Per-actor placement strategies** (#41): `defineActor({ placement })`
  declares where new activations of a type go — a placement attribute on
  the actor instead of central configuration. The
  new core marker `ActorPlacementStrategy` carries the declaration (core
  cannot name `PlacementPolicy`, which needs a membership view); cluster's
  `PlacementPolicy` now extends it and the built-in policies carry a `name`
  (`'random'`, `'consistent-hash'`, `'prefer-local'`). Precedence for a new
  activation: the actor's own `placement` → `typePolicies` → `policy` →
  uniform random. The single-node host has one host and ignores it.

- **Clustering milestone 5 — per-request HMAC auth** (#20): the internal
  host-to-host mount now authenticates each request with an HMAC-SHA-256
  signature (WebCrypto, key cached per secret — ~9µs per operation) over
  the protocol version, symbol, call id, and a timestamp, replacing the
  static bearer header outright. A captured header cannot authorize a
  different call, and signatures expire after a 5-minute freshness
  window. Identical-request replay within the window is out of scope
  without a nonce store — mTLS/VPC between hosts remains the documented
  transport posture. `signAuth`/`verifyAuth` are exported for hand-rolled
  mounts.

- **Clustering milestone 4 — rebalancing & graceful handoff** (#20):
  `host.stop()` on a cluster host is now a HANDOFF — it announces
  `leaving` before draining (peers immediately stop placing new actors
  there), deactivates with the `'migrated'` reason, and releases each
  directory claim as its activation drains; callers hitting the leaver
  retry through routing (a remote `host-shutdown` is retryable with
  backoff) so rolling deploys drop zero calls. New placement policies
  `consistentHashPolicy()` (all hosts deterministically agree on a new
  key's target) and `preferLocalPolicy()`, a `typePolicies` per-actor-type
  override map on `clusterPlacement`, `Host.deactivate(ref, reason?)`, and
  the explicit rebalance primitive `ClusterPlacement.migrate(ref)`.

- **Clustering milestone 3 — sharded reminders** (#20): the reminder table
  is split into 16 fixed hash shards (`$sigx:reminders/p0..p15`, FNV-1a of
  the actorId — pinned forever); a host ticks only the shards it owns.
  Cluster ownership is rendezvous hashing over the membership view —
  deterministic, no stored assignment, and the M1 leader lease is REMOVED
  (`ReminderLease`, `ClusterProviders.reminderLease`): the per-shard etag
  CAS already keeps firing at-most-once through view divergence.

- **Clustering milestone 2 — failover & directory hygiene** (#20):
  `ActorDirectory` gains optional `evictHost(hostId)`; `clusterPlacement`
  now sweeps a departed host's directory entries proactively on membership
  change (store-confirmed dead; lazy eviction on lookup remains the
  backstop) and spaces unreachable retries with a linear backoff
  (`retryBackoffMs`, default 100 — wrong-host redirects still retry
  immediately).

- **`@sigx/actors/cluster` — multi-host clustering** (#20): `clusterPlacement()`
  plugs into `createHost({ placement })` for hosts on many hosts forming one
  actor system. Claim-on-activate distributed directory (one activation per
  key cluster-wide), TTL-heartbeat membership with self-fencing, internal
  host-to-host endpoint (`handleHostRequest`/`matchesHostRequest`, default
  mount `/_sigx/host`) reusing the serverFn wire with a versioned envelope
  header (call chain, call id, remaining-ms deadline), shared-secret auth,
  wrong-host 421 redirect-not-proxy routing with bounded retry, cross-host
  NDJSON streams with cancellation/keep-alive release, and sharded
  reminders (below) firing exactly once cluster-wide. Provider seams
  (`ClusterMembership`, `ActorDirectory`, `ReminderLease`) with
  `memoryClusterHub()` in-process implementations for tests; Redis
  providers ship separately as `@sigx/actors-redis`.

- Cluster seams on the placement contract (groundwork for multi-host
  clustering, #20): `ActorPlacement.bind?(local, host)`
  returning `PlacementBindings` — activation claim/release hooks
  (`beforeActivate` / `afterDeactivate`), `strictChainPresence`, and a
  `ownsReminderShard` gate for reminder-shard ownership. All optional;
  single-node behavior unchanged.
- New `ActorErrorKind`s `'wrong-host'` (`ActorWrongHostError`, carries an
  owner hint) and `'unreachable'` (`ActorUnreachableError`); both map to a
  retryable 503 if they ever reach the public wire. Exhaustive switches on
  `ActorErrorKind` gain two cases.
- `DeactivationReason` gains `'migrated'` — reserved for cluster
  rebalancing, not yet emitted.

- Initial release of `@sigx/actors` — virtual actors for SignalX.
- `defineActor` with options-object + `methods`/`streams` factories; state
  as a deep `@sigx/reactivity` signal; explicit `ctx.save()` (default) or
  opt-in write-behind persistence.
- Single-node host (`createHost`): lazy activation directory, per-activation
  mailbox with turn-based concurrency, idle sweep, graceful deactivation and
  shutdown, `deactivateType` for dev/HMR.
- Call-chain reentrancy metadata with immediate `ActorDeadlockError` on
  cycles; `reentrant: true` for call-chain re-entry.
- `ActorStorage` seam with etag optimistic concurrency; `memoryStorage()`
  and `fileStorage({ dir })` providers; conflict = fault-and-reload
  semantics.
- Volatile timers (mailbox-serialized, coalesced) and durable reminders
  (storage-backed, re-activate idle actors, 60s floor).
- Wire layer riding the serverFn endpoint: `handleActorRequest` /
  `matchesActorRequest` / `createActorResolver` (`./server`), generic typed
  client proxy + `configureActors` (`./client`), `createActorHandler` +
  `attachSignalHandlers` (`./node`). NDJSON streams end-to-end.
- Transport-independent guard chains (`use` / `methodUse`) shared by wire
  and in-process calls; `requireGuards` build gate (default on).
- `sigxActors()` Vite plugin: `*.actor.ts` client-module swap, prod registry
  (`virtual:sigx-actors`), dev host through the SSR module runner, HMR
  deactivate-through-storage.

### Changed

- `ActorTransport` is now the transport **interface**; the old
  `{ endpoint, headers, fetch }` config type is `ActorTransportConfig`, and
  `ActorClientCallOptions` is `ActorCallInit`. `configureActors({ … })` call
  sites are unaffected.

- Reminder-table mutations now reload-and-reapply on a storage etag
  conflict (up to 3 attempts) instead of failing — the table legitimately
  has concurrent writers once hosts share storage.
- `mintCallId()` ids carry a per-process random component so ids from
  different hosts and restarts are distinguishable. The format remains
  opaque.

### Fixed

- **The reminder tick no longer rewrites shards it did not change** (#74).
  `#tickShard` runs for every owned shard on every tick, and the mutation
  path saved unconditionally — so a host with **no reminders at all** wrote
  all 16 shard records every `reminderTickMs` (30s by default), forever,
  bumping etags no reader could distinguish from a real change. A no-op
  edit now skips the save, and an idle app never creates the
  `$sigx:reminders` records in the first place.

  Under `fileStorage` that write amplification was visible as well as
  wasteful: each save is a temp file plus a rename inside the project tree,
  which Vite's dev watcher raced — the `ENOENT … p4.json.<pid>.tmp` error
  overlay that reported it. A storage `dir` inside a Vite root also belongs
  in `server.watch.ignored` (now documented, and set in both examples):
  actor state is not source and should reload nothing.

- **`createAppHandler` no longer 500s on an unparseable request target**
  (#60). `req.url` is the RAW target — Node neither normalizes nor
  validates it — so `GET //` or an absolute form like `GET http://[` made
  `new URL()` throw, and the handler answered 500. Such a target cannot
  address an actor, so it is now a fallthrough: whatever is mounted after
  the actor handler (a document handler, typically) gets to answer, exactly
  as for any other path it does not own. Found by pointing a raw socket at
  `examples/chat`.

- **The "defineActor outside `*.actor.ts`" warning no longer fires for
  excluded files** (#60). `filter()` answers `false` for two different
  things — "not an actor module" and "explicitly excluded" — and the warning
  treated them alike, so `@sigx/actors`' own bundled dist (which mentions
  `defineActor`, and is excluded by `**/dist/**`) warned on every client
  build of an app that links the package from a workspace, telling the
  developer to go move code they do not own.

- Subpath `types` conditions in the exports map now point at the emitted
  declaration files (`./dist/<entry>/index.d.ts`) — previously they named
  `./dist/<entry>.d.ts`, which the build never produced, so TypeScript
  consumers of `@sigx/actors/host` etc. got an implicit-`any` module.
- External wire calls now inherit the host's `callTimeoutMs` deadline, as
  documented. Previously only in-process external calls got a deadline;
  wire calls could hang past the configured timeout without a 504.

### Fixed

- **An unknown actor now 404s as an actor** (#53): the endpoint rides core's
  serverFn resolver, so a missing type answered `Unknown server function` —
  the wrong noun on the actor mount, and the first thing a developer sees
  when client and server builds fall out of sync. The message now names the
  actor and asks whether the builds are from the same deploy. Status and
  envelope shape are unchanged.

### Removed

- **`sigxActors({ storage })` and `sigxActors({ guard })`** (#46): the
  path-string options are replaced by `app`, which supplies both (and
  everything else) as real values. Nothing is published, so there is no
  shim.
