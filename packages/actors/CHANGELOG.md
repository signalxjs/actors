# Changelog

## [Unreleased]

### Changed

- **Far call deadlines are enforced by a shared registry, not a timer per
  call** (#230). With a non-zero `callTimeoutMs`, every dispatch used to
  allocate a `setTimeout`/`clearTimeout` pair, a `Promise.race` and an
  `async` wrapper to race the caller's deadline — measured at 38% of
  dispatch throughput, and paid by every production call since the default
  is 30 s. Deadlines ≥ 10 s away now share one recurring unref'd 1 s tick
  (`CallDeadlines`); short budgets (e.g. a wire hop arriving nearly spent)
  keep an exact per-call timer. Observable change: a far deadline may fire
  up to ~2 s **late**, never early — the `deadline` value crossing hops is
  unchanged, so cross-silo budgets are unaffected. Gated by the new
  `dispatch/warm-turns-deadline` exact benchmark (12 → 11 microtask turns,
  1000 → 0 host timers per 1000 dispatches).

### Fixed

- A call arriving with an already-expired deadline no longer leaks an
  unhandled rejection when its (still-enqueued, never killed) turn itself
  rejects — the turn promise now gets a rejection handler on every branch
  (#230).

### Added

- **`clusterStats()` aggregates behaviour, not just topology** (#121).
  `SiloReport` grows three optional fields — a mergeable `metrics` digest,
  the silo's `health`, and (under `detail`) its live `activations` — so ONE
  call answers for the whole fleet. One reachable endpoint, one secret, and
  it works behind an ingress where the peers are not individually reachable.

  `totals` gains `metrics` (calls, failures, streams, `errors.byKind`,
  storage operations, activation churn, per-type and per-method call counts,
  and merged latency/queue/turn) plus a `health` tally.

  **Latency is merged, not averaged.** `HistogramSnapshot` is p50/p90/p99
  with no buckets, and the mean of two silos' p99s is not the p99 of
  anything — so the digest carries bucket COUNTS and the cluster's
  percentiles are re-derived from the summed distribution. Buckets travel
  sparse (of 384, a real silo occupies a few dozen), and a peer whose bucket
  layout differs contributes its counters while its distribution is dropped
  rather than mixed into a different axis.

  **`totals.metrics.silos` is the denominator.** A silo with no `metrics()`,
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
  `@sigx/actors/silo` alongside `createMetricsAccumulator()`. A client
  merging a user-selected subset of silos needs the same arithmetic
  `clusterStats()` uses; without these it would have to reimplement the
  log-linear bucket layout, which is the mistake the layout tag exists to
  catch. Foreign bucket indices are bounds-checked rather than used as
  offsets — a digest arrives over a wire.

- **`detail` on the fan-out, and `?detail` on the ops route** (#121). The
  grain list and recent failures are opt-in, and `detail.silos` targets one
  silo: the walk is O(activations) on every silo at once, and grain keys are
  the one field on this wire that can be personal data. Requested limits are
  clamped by the RESPONDER — HMAC proves who is asking, not that
  `activations: 1e9` is a sane thing to ask for.

### Changed

- `ops({ cluster })` takes an optional second argument (the parsed
  `?detail` query). Appended, so every existing
  `(signal) => clusterStats(placement)` keeps working unchanged.
- `SiloReport.v` stays `1`. Every new field is optional, so a peer on an
  older build simply answers without them — a version bump would have made a
  new collector classify every not-yet-deployed peer as `unsupported`,
  blanking the report during exactly the deploy it exists to explain.

### Added

- **`reads:` — GET-cacheable actor reads** (#195, closing #11). A method
  listed there is served over `GET
  {base}/r/{token}/{Type}%23{method}?args=[key,…]` with the `Cache-Control`
  the declaration describes, so browsers, CDNs and reverse proxies absorb
  read traffic that would otherwise reach a grain. The vocabulary is core's
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
  session — is rejected because on Workers, at an edge, or in a multi-silo
  cluster it cannot be routed to the instance holding the open stream.
  Reopening is safe precisely because every subscription re-seeds, which is
  also why a reconnect needs no resume token and no server-side
  cross-request state.

  An unchanged value is dropped rather than delivered, by structural
  equality. Two things produce one routinely: that re-seed, and the fact that
  a mutating turn re-runs EVERY subscription on the grain — change a room's
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
  actor endpoint, the internal silo-to-silo endpoint, and `httpTransport`'s
  sending side (where the victim was `finally { controller.abort() }`, meaning
  a hung-up caller never even aborted its fetch).

  The cost was quiet and unbounded. `keptAlive` does not delay idle
  collection, it EXEMPTS an activation from it (`if (a.idle && !a.keptAlive &&
  …)`), so a departed consumer left the grain permanently ineligible — not
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
  pins each grain where its first call landed and the balancer sends the
  next one elsewhere anyway. And under an uneven balancer (a rolling deploy,
  a bad health check) it concentrates ownership **80×**: one silo holding
  80% of the grains, which do not move back, because placement applies only
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
  (#164). One job = one grain (key = your run id), as convention over
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

  The grain is now threaded to the transport as `ActorCallInit.ref`, built
  once per `actor(def, key)` rather than re-derived from the symbol and
  `args[0]` — that convention belongs to `__actorRef`, and a transport that
  re-implemented it would break silently the day it changes. `$live` carries
  no ref (it multiplexes many grains onto one request), so it is never
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
  (#162). Flows through `silo.activations()` and the `ops()` snapshot, so
  operators can see which grains hold long-running work (and why the idle
  sweeper is skipping them); `sigx actors top` shows it as a TASKS column
  in the grains table.

- **Task durability: ledger + liveness reminder + crash-resume** (#151).
  `ctx.tasks.start()` now resolves only after the run is durably recorded —
  an entry in the reserved `$sigx:tasks` storage record (etag-CAS,
  reload-and-reapply) plus a liveness reminder armed under the same
  reserved name. A run interrupted by deactivation (any reason but cancel)
  keeps its entry and restarts on the grain's next activation with
  `TaskInfo.restarts` bumped and its `input` replayed through the state
  codec; completion, throw, and cancel remove the entry, and an empty
  ledger disarms the reminder (and deletes the record — no tombstones).
  The reminder is the crash driver: a dead silo's reminder shards are
  re-owned by survivors, the tick delivers through placement, and the
  grain re-activates — tasks and all — within ~60–90s, no client call
  needed. The reminder name never reaches `onReminder`; its handler
  self-heals (restarts a ledgered run that somehow is not running, disarms
  a stale reminder whose ledger is gone). At-least-once by contract: the
  runtime resumes the function, user code resumes the work from its own
  checkpointed state. New in `@sigx/actors/silo`: `TASKS_TYPE`,
  `TASK_REMINDER`, `TASK_REMINDER_MS`, `TaskLedger`, `TaskLedgerEntry`.

- **`tasks:` — detached long-running work on an actor** (#147). Declare
  long-running operations in a `tasks:` factory on `defineActor` and start
  them with `ctx.tasks.start(name, input?)`: the body runs detached from
  the mailbox (reads, streams and watches keep answering while it works),
  touches state only through the new `ctx.turn(fn)` — one ordinary
  serialized mailbox turn — and holds a keep-alive ref so the idle sweeper
  skips the grain. Each run gets its own `abortSignal`, fired on
  `ctx.tasks.cancel(name)` (reason `'cancelled'`) or deactivation (reason:
  the `DeactivationReason`); deactivation waits a bounded grace
  (`SiloDefaults.taskGraceMs`, default 10s) with the mailbox still open so
  a winding-down task can run a final checkpoint turn. `start` is
  single-flight per name; a thrown task is terminal; `cancel` is a request,
  not a join. New types: `TaskApi`, `TaskInfo`, `ActorTaskContext`,
  `ActorTask`, `ActorTaskTable`. Durable crash-resume ships separately.

### Changed

- **`ctx.abortSignal` now fires at the START of deactivation, before the
  mailbox drain** (#147) — previously it fired after, which made its
  documented contract ("long-running work should observe it")
  unsatisfiable: work awaiting the signal was exactly what the drain was
  waiting on, so a parked turn held `silo.stop()` to its deadline and was
  then force-dropped. A turn parked on the signal now unwinds inside the
  drain window.

- **`attachSignalHandlers(silo, { server, onStopBegin, onError })` drains the
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
  pools — one response at a time, interrupting nothing) → `silo.stop()` →
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
  swallowed a `silo.stop()` rejection and exited 0, so a pod that failed to
  flush looked like a clean stop — and on a terminated pod the exit code is
  often the only diagnostic left. It now exits 1 and reports through the new
  `onError` hook.

### Fixed

- **A fenced silo now fails LIVENESS, not just readiness** (#141).
  `HealthCheck` gains `fatal?: boolean` (implies not-ready); `health()`
  answers `503 { status: 'fatal' }` on the liveness route when any check
  reports it, and `cluster()` marks the `fenced` state fatal. Fencing is
  terminal for a silo identity, so before this a membership-store outage
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

- **`SiloEndpointRuntime` — the inbound half of `SiloTransportRuntime`**
  (#136), plus `siloEndpointRuntime(silo)` in `@sigx/actors/cluster`. The
  internal silo endpoint never calls `descriptor()` or `view()`; those
  belong to the sending side, which has to pick a peer. Splitting them lets
  a single-host runtime with no membership — a Cloudflare Durable Object,
  where the platform guarantees the single instance — serve the internal
  mount without fabricating a membership view. `SiloTransportRuntime` now
  extends the narrower interface, so every existing transport is unchanged
  and the whole cluster suite passes untouched.

  `$sigx:silo#stats` resolves to `null` on such a runtime rather than
  synthesizing a `SiloReport`: that payload is a *cluster* identity (siloId,
  epoch, address, owned reminder shards) and inventing one would put fiction
  on the ops channel. The endpoint's resolver now asks the runtime whether
  it offers the ops channel instead of assuming every runtime does.

- **`onMiss: 'proxy' | 'redirect' | 'auto'` on the public actor mount**
  (#134, stage 2 of #84), plus `cluster({ publicAddress })`, the
  `ActorPlacement.locate()` seam, and `configureActors({ follow })`.

  A call for a grain this silo does not own has always been proxied: one
  client round trip, one internal hop, every time. `'redirect'` answers
  `421` naming the owner instead, and `'auto'` redirects only callers that
  advertise they can follow (`x-sigx-actor-follow`), so one cluster can
  serve a browser origin that proxies and a service origin that redirects.
  The option sits on the **mount** rather than the silo for exactly that
  reason. Default stays `'proxy'`, byte for byte.

  **The client memo ships with it, not after it.** A redirect without
  client-side memory costs *two* client round trips versus one trip plus an
  internal hop — strictly worse than proxying. `configureActors({ follow:
  true })` remembers where each grain lives, so the cost is 2 requests once
  and 1 forever after, straight to the owner. A learned endpoint is dropped
  the moment it proves wrong (connection failure or 5xx) and the call falls
  back to the configured endpoint, because otherwise one dead silo would
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
  which would have made every app-built silo — i.e. every real deployment,
  since `defineActorApp` is the setup path — proxy forever while looking
  correctly configured. It forwards a fixed set of methods, so a new one is
  invisible until something asks for it.

  Known limits, documented rather than papered over: `$live` is never
  redirected (one held-open response fans out to many grains, so there is no
  single owner to name); a redirected watch is bound to its owner only until
  the grain migrates; and cross-origin redirects need an explicit `origin`
  allowlist and CORS, which is why `'proxy'` remains the browser answer.

  New counters `locates` / `locateRemote` — their ratio is the miss rate the
  edge is producing, and the number to watch after wiring up routing-token
  hashing.

### Changed

- **`SiloDefaults.sweepIntervalMs: 0` disables idle collection** (#136),
  matching the existing `callTimeoutMs: 0` precedent. For a runtime that
  evicts the whole silo when it goes idle the sweeper can never usefully
  fire — eviction destroys the isolate, the silo and the activation
  together, and nothing keeps the object resident except in-flight work,
  which is exactly what the sweeper skips. It is also not free there: a
  pending timer holds a Durable Object in memory and billable, so a
  permanent interval would be one billing pin per actor.

- **The actor wire now carries a per-grain routing token** (#132, stage 1 of
  #84): `POST {base}/r/{token}/{Type}%23{method}`, mirrored into an
  `x-sigx-actor-route` header. No compatibility shim — nothing has shipped,
  and a wire that only *sometimes* carries the token means a load-balancer
  config that only sometimes works.

  The problem it solves: the actor **key** rides in the JSON body, so no
  load balancer can see which grain a request is for, and locality decays as
  1/N — measured 1.00, 0.50, 0.12, 0.02, 0.01 for N = 1, 2, 10, 50, 100. At
  N=100 that is ~99% of calls paying a cross-silo hop, which #80 measured at
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
  a hop for every grain. Caller affinity is what composes; determinism is
  not a precondition for it. The shipped default stays
  `randomPlacementPolicy()` pending the warm-locality arms in stage 4.

  Known limits, both documented rather than papered over: `$live#subscribe`
  carries no token (one held-open response fans out to many grains, so there
  is no single owner to route to — sharding it would defeat the reason it
  exists), and a grain that is already hot does not follow a changed LB pool
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
  app, a Lynx app, a headless silo — none of which have a DOM, and none of
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
  `resolveLimit()`, which #108 added for the same bug in `silo.activations()`.
- **A misdeclared `placement` is now refused instead of silently ignored**
  (#86). `ActorOptions.placement` is typed `ActorPlacementStrategy`, which
  requires only an optional `name` — so a strategy with no `choose()`
  type-checked cleanly, was dropped with a `__DEV__`-only warning, and in
  production placed grains somewhere other than where their author declared,
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

- **Bounding the silo-to-silo connection pool** (#97, closes #89). Node's
  global `fetch` uses an unbounded undici pool, measured at **two connections
  per in-flight request** — ~12 600 sockets per silo at concurrency 64 across
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
  every 30 s, so the proxies and mobile NATs between a browser and the silo
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
  actor placed on ANY silo, over all three transports, rather than only for
  the ones that happened to land on the host serving the connection.

  A watch is an ordinary read dispatched in watch mode, so the receiving
  silo cannot tell what is being asked of it from `Counter#read` alone —
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
  definition lookup — the same shape `$sigx:silo#stats` uses.

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

- **`Silo.activations()` — the live grain list** (#101). Bounded and
  sorted: `{ type, key, queued, ageMs, idleMs, keptAlive }`, with
  `sortBy: 'queued' | 'age' | 'idle'`, a `type` filter and a `limit`
  defaulting to 100.

  `stats()` could report that a silo held 12,000 activations with 400
  queued turns and could not say WHERE, which is the only question worth
  asking at that point. The activation directory was private and nothing
  else exposed a single grain key, so "top grains by queue depth" was not a
  panel anyone could build.

  Ties break on the actor id, so the order is **stable between polls** — a
  table that reshuffles equal rows at 1 Hz is unreadable. The walk is
  O(activations) and allocates per candidate, hence the low default limit:
  this is a top-N view, not an export.

  `ageMs` is monotonic while `idleMs` is wall-clock, and the difference is
  deliberate: age is a duration and must survive an NTP step, whereas idle
  is compared against `idleAfterMs` by the sweeper, which genuinely wants
  wall time. Both clamp at 0, so a clock stepped backwards cannot report a
  grain last used in the future.

- **`SiloStats.transitional`** (#101) — `{ activating, deactivating }`.
  Slots mid-transition carry no activation, so `stats()` skipped them
  entirely; a silo in the middle of an activation storm therefore read as
  **idle**, at precisely the moment an operator was looking at it. Counted
  separately rather than folded into `activations`, which still means
  "settled".

- **`OpsSnapshot.activations`** (#101) — the hottest grains on the ops
  endpoint, deepest mailbox first, bounded by `ops({ activations })`
  (default 20, `0` omits the list). Small by default because the walk is
  O(activations) and this endpoint gets polled — and because grain keys are
  the one field in the snapshot that can be personal data, so it is worth
  being able to drop them without giving up the rest.

- **`metrics()` breaks down per METHOD** (#101) — `snapshot().byMethod`,
  keyed `Type#method`, carrying the same five numbers as `byType`.

  The data was always there and always discarded: both `useDispatch` and
  `observeTurns` receive `method` and neither kept it. `byType` tells you a
  type is slow and never which of its methods is, and the queue/turn split
  is most useful exactly here — within one type, a hot grain and one slow
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

  `calls.failed` was a single scalar: it said a silo was failing and nothing
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
  remote surface that did exist — the `$sigx:silo#stats` symbol — is
  cluster-only, carries no latency distributions, and disappears entirely in
  a cluster of socket-only transports. A single-node silo was completely
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
  discovered: `ops()` lives in `@sigx/actors/silo` and `clusterStats` in
  `@sigx/actors/cluster`, and a single-node silo must not pay for the cluster
  bundle to have an ops endpoint. It also leaves `timeoutMs`/`concurrency`
  where they already are. Unwired, `{base}/cluster` answers 404. A collector
  that fails answers 503 carrying the reason rather than an empty report,
  which would read as a healthy cluster of zero silos.

- **`PluginRegistry.reportOps(name, provider)` / `registry.ops()`** (#101) —
  the contribution seam, the counterpart to `reportHealth` / `health()` and
  deliberately identical in shape: unique names (a clash throws at setup
  naming both plugins), and a LIVE aggregate read at call time, so `.use()`
  order does not matter.

  Providers run per read and must stay sync — the endpoint you reach for when
  a silo is already unwell must not be able to hang. A throwing provider is
  caught and its section replaced with `{ error }` rather than failing the
  snapshot; the section stays PRESENT, because an absent key would read as
  "this plugin contributes nothing", which is the opposite of the truth.

  `metrics()` contributes under `'metrics'` and `cluster()` contributes
  `placement.report()` under `'cluster'`, so `.use(metrics()).use(ops(…))`
  needs no wiring between them. Registration costs only a closure: an app
  with no `ops()` never calls a provider.

- **A transport conformance suite** (#93), for contributors — **not a
  published import**. `transportConformance` is the set of cases every
  `SiloTransport` must pass, plus the harness interface a transport supplies.
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

- **The silo-to-silo transport is a seam** (#91). `cluster({ transport })`
  takes a `SiloTransportFactory`, or a LIST of them as a fallback chain.
  `httpTransport()` is the default and nothing about a default-configured
  cluster changes.

  A `SiloTransport` owns **both halves** of a link — the outbound dispatcher
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
  (`transportFallbacks`), reported (`SiloReport.transports`) and dev-warned
  once per peer.

  Because the internal mount is now just a route a transport declares, a
  cluster of only socket transports has **no internal HTTP endpoint at all**.
  The public actor wire is unaffected.

- **`SiloDescriptor.addresses`** (#91) — peer-reachable address per
  transport, keyed by transport name. Optional, and absent reads as
  "HTTP only", so a silo from a build predating the field stays reachable.
  It round-trips through the existing providers untouched: both the Redis
  and memory providers store the descriptor whole.

- **`resolveSiloSymbol`, `siloRuntime`, `toSiloWireError`,
  `fromSiloWireError`, `siloWireCodec`** (#91) — the compat-critical
  plumbing a transport needs, exported so an out-of-package transport uses
  the same codec and the same error mapping instead of re-deriving them. A
  transport that `JSON.stringify`s raw values would silently drop every
  registered codec handler and re-open `__proto__`; "the error is the same
  error" is likewise a contract, not a nicety, since a caller must not be
  able to tell a remote hop from a local dispatch.

### Changed

- **`createSiloTransport()` / `SiloTransportOptions` are removed** (#91),
  replaced by `httpTransport()`. They were exported but documented nowhere
  and had a single call site.
- **`@sigx/actors/cluster` size limit raised 8 KB → 9 KB** (#91). The seam
  costs ~0.9 KB: the chain walk, per-transport addressing, and the shared
  error mapping. Recorded rather than shaved.

- **`health()` — liveness and readiness endpoints** (#38). `GET
  /_sigx/health` and `GET /_sigx/health/ready`, contributed as ordinary
  plugin routes so every mount picks them up.

  The point is that the two answers are allowed to **disagree**. A graceful
  `silo.stop()` announces `leaving` before activations hand off, so for the
  whole handoff window the silo is `200 live` and `503 not-ready`: drain it,
  do not restart it. Restarting is what turns a rolling deploy into an
  outage, and until now nothing exposed that window to a load balancer.

  The check that would otherwise be invisible is **`fenced`**. A silo that
  loses its membership heartbeat self-fences and refuses every activation,
  but `#fence()` deliberately leaves the *published* status at `active` — so
  a balancer would keep feeding a black hole. Readiness reads the fenced
  state, not the descriptor.

  Before `start()` the whole mount answers 503, including liveness: a route
  only runs on a started silo, so serving at all is the liveness signal. The
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
  across the membership view and returns per-silo activations, queue depths,
  per-type counts, the reminder-shard ownership map and the counters below,
  plus cluster-wide totals.

  It travels as a reserved symbol (`$sigx:silo#stats`) on the **existing**
  internal mount rather than as a new route, which is the security-relevant
  choice: it inherits the per-request HMAC, the envelope, the codec and the
  body cap, so there is no second and eventually-unauthenticated way to read
  your topology. Answered before any definition lookup, so a user actor
  cannot shadow it.

  It **never throws because a peer is sick** — a silo that times out,
  refuses the secret or predates this build lands in `unreachable` with a
  classified `reason`, and `partial: true` marks the totals as a lower
  bound. Peers are queried with bounded concurrency (default 16) so a
  100-silo fan-out is waves, not 100 simultaneous connections, and the
  collector answers for itself in process rather than looping back through
  an address it may not be able to reach.

  `reminderShards` maps each shard to the silos *claiming* it, built from
  the reports rather than recomputed centrally — two claimants means views
  diverged, an empty list means nothing is ticking that shard, and the
  distinct silo count is how many silos do reminder work at all.

- **`ClusterPlacement.counters()`, `view()`, `report()`** (#38) — pull-based
  routing, directory, membership and auth counters. Always on: these are
  integer increments on paths already doing network and directory work, and
  the local fast path (`dispatcherFor` for a claimed actor) is not
  instrumented at all, so it stays byte for byte what it was.

  The rule that makes them safe to aggregate: every counter moves on the one
  silo where its event happened, and the two sides of a cross-silo call get
  **different names** — `remoteDispatches` on the caller, `inboundDispatches`
  on the owner. They are reported side by side and never summed; the gap
  between them is itself the signal. `authFailures` is the one with no prior
  visibility at all: a 403 on the internal mount was completely silent, and
  during a secret rotation it is the only sign that half the cluster has not
  rotated yet.

- **`metrics()` in a cluster is split, not doubled** (#38) — documented, and
  now pinned by a test. It had been assumed that a cross-silo call was
  counted once on the caller and again on the owner. It is not, and the
  reason is structural: `compositePlacement.bind()` hands the placement the
  **raw** local dispatcher and `dispatchInbound` calls it directly, so an
  inbound hop never passes through `useDispatch`. A cross-silo call is
  counted once, on the silo that originated it, while `queueMs`/`turnMs` are
  recorded on the silo that executed it — so summing `calls.total` across
  silos gives the true cluster-wide number. The test exists so a refactor
  cannot quietly turn the split into a real double count.

- **`metrics()` — pull-based observability** (#79). A plugin that counts
  calls, failures, activation churn, storage operations and etag conflicts,
  and reports latency distributions, read via `snapshot()`. No exporter, no
  push pipeline, no metrics-library dependency.

  The number it exists for is the split between **`queueMs`** (waiting for
  the mailbox) and **`turnMs`** (holding it). They are the two halves of a
  call's latency and mean opposite things — a slow method versus a hot
  grain — and a dispatch middleware, which only sees the sum, cannot tell
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
  and dev-logged rather than failing the turn. `createSilo({ onTurn })`
  exposes the same thing to hand-rolled silos.

- **`Silo.observeTurns(observer)`** (#79) — subscribe imperatively; returns
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
  server render (in-process through the silo seam, guards and all),
  serializes into the page under its canonical actor key, and the browser
  restores it **without refetching**. There is an end-to-end test for exactly
  that — render a document over a real silo, then mount over the emitted
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
  `./client`, `./silo`, `./server`, `./node` and `./cluster` stay free of the
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
  directly by `createSilo`, so its sharded design was the only one
  possible. Reminders are now an `ActorReminders` seam
  (`createSilo`/`defineActorApp`'s `reminders` option), with
  `shardedReminders()` as the unchanged default. An implementation
  receives `bind({ storage, scheduler, tickMs, ownsShard, deliver })` once
  before `start()`, mirroring `ActorPlacement.bind()` — so it is handed the
  silo's real (plugin-decorated) storage and clock rather than being told
  them twice.

  The shard table assumes many actors per silo. Under Cloudflare's
  one-Durable-Object-per-actor model each actor's reminders belong in its
  own DO storage, fired by its own alarm — nothing to shard, nothing to
  poll. The whole silo-level reminder suite passes unchanged.

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
  (`createSilo`/`defineActorApp`'s `scheduler` option) instead of calling
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
  configure the same silo twice in different languages. The plugin took
  `storage` as a root-relative PATH STRING whose module needed a magic
  named export, while the entry took real values — and the dev silo was
  built as `createSilo({ actors, storage })`, so **dev could not receive
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
  (#44): a clustered silo used to repeat itself — `secret` went to both
  `clusterPlacement` and `handleSiloRequest`, `internalBase` had to agree
  with `matchesSiloRequest`'s default, and routing the internal mount was
  left to the entry, with nothing type-checking any of it.
  `defineActorApp({ actors, storage }).use(cluster({ providers, advertise,
  secret }))` states each value once and contributes the silo-to-silo mount
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

  `clusterPlacement`, `handleSiloRequest` and `matchesSiloRequest` remain
  exported for hand-rolled mounts, and the entire cluster test suite passes
  unchanged against the plugin.

- **`defineActorApp` — the composition root and typed plugin model**
  (#41): `createSilo` takes exactly one `placement` and one `storage`, and
  `ActorPlacement.bind()` → `PlacementBindings` was the only lifecycle-hook
  shape in the package — so two things that both wanted `beforeActivate`
  (a cluster directory and an audit log) could not coexist.
  `defineActorApp({ actors, storage, types, defaults }).use(plugin)` folds
  every plugin's contributions into the single placement, storage and
  context `createSilo` already understands, so the layer is purely additive
  and `createSilo` stays the documented primitive.

  A plugin's `setup(registry)` may `addTypeHandlers`, `decorateStorage`
  (chained, last registered outermost), `setPlacement` (**exclusive** — a
  second claim throws naming both plugins; takes a FACTORY run once the
  silo exists, so a custom placement backend can resolve the per-type
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
  `createSilo` gains an `extendContext` option for hand-rolled silos.

- **Per-actor placement strategies** (#41): `defineActor({ placement })`
  declares where new activations of a type go, the way Orleans puts a
  placement attribute on the grain instead of configuring it centrally. The
  new core marker `ActorPlacementStrategy` carries the declaration (core
  cannot name `PlacementPolicy`, which needs a membership view); cluster's
  `PlacementPolicy` now extends it and the built-in policies carry a `name`
  (`'random'`, `'consistent-hash'`, `'prefer-local'`). Precedence for a new
  activation: the actor's own `placement` → `typePolicies` → `policy` →
  uniform random. The single-node host has one silo and ignores it.

- **Clustering milestone 5 — per-request HMAC auth** (#20): the internal
  silo-to-silo mount now authenticates each request with an HMAC-SHA-256
  signature (WebCrypto, key cached per secret — ~9µs per operation) over
  the protocol version, symbol, call id, and a timestamp, replacing the
  static bearer header outright. A captured header cannot authorize a
  different call, and signatures expire after a 5-minute freshness
  window. Identical-request replay within the window is out of scope
  without a nonce store — mTLS/VPC between silos remains the documented
  transport posture. `signAuth`/`verifyAuth` are exported for hand-rolled
  mounts.

- **Clustering milestone 4 — rebalancing & graceful handoff** (#20):
  `silo.stop()` on a cluster silo is now a HANDOFF — it announces
  `leaving` before draining (peers immediately stop placing new actors
  there), deactivates with the `'migrated'` reason, and releases each
  directory claim as its activation drains; callers hitting the leaver
  retry through routing (a remote `silo-shutdown` is retryable with
  backoff) so rolling deploys drop zero calls. New placement policies
  `consistentHashPolicy()` (all silos deterministically agree on a new
  key's target) and `preferLocalPolicy()`, a `typePolicies` per-actor-type
  override map on `clusterPlacement`, `Silo.deactivate(ref, reason?)`, and
  the explicit rebalance primitive `ClusterPlacement.migrate(ref)`.

- **Clustering milestone 3 — sharded reminders** (#20): the reminder table
  is split into 16 fixed hash shards (`$sigx:reminders/p0..p15`, FNV-1a of
  the actorId — pinned forever); a silo ticks only the shards it owns.
  Cluster ownership is rendezvous hashing over the membership view —
  deterministic, no stored assignment, and the M1 leader lease is REMOVED
  (`ReminderLease`, `ClusterProviders.reminderLease`): the per-shard etag
  CAS already keeps firing at-most-once through view divergence.

- **Clustering milestone 2 — failover & directory hygiene** (#20):
  `ActorDirectory` gains optional `evictSilo(siloId)`; `clusterPlacement`
  now sweeps a departed silo's directory entries proactively on membership
  change (store-confirmed dead; lazy eviction on lookup remains the
  backstop) and spaces unreachable retries with a linear backoff
  (`retryBackoffMs`, default 100 — wrong-host redirects still retry
  immediately).

- **`@sigx/actors/cluster` — multi-host clustering** (#20): `clusterPlacement()`
  plugs into `createSilo({ placement })` for silos on many hosts forming one
  actor system. Claim-on-activate distributed directory (one activation per
  key cluster-wide), TTL-heartbeat membership with self-fencing, internal
  silo-to-silo endpoint (`handleSiloRequest`/`matchesSiloRequest`, default
  mount `/_sigx/silo`) reusing the serverFn wire with a versioned envelope
  header (call chain, call id, remaining-ms deadline), shared-secret auth,
  wrong-host 421 redirect-not-proxy routing with bounded retry, cross-host
  NDJSON streams with cancellation/keep-alive release, and sharded
  reminders (below) firing exactly once cluster-wide. Provider seams
  (`ClusterMembership`, `ActorDirectory`, `ReminderLease`) with
  `memoryClusterHub()` in-process implementations for tests; Redis
  providers ship separately as `@sigx/actors-redis`.

- Cluster seams on the placement contract (groundwork for multi-host
  clustering, #20): `ActorPlacement.bind?(local, silo)`
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

- Initial release of `@sigx/actors` — Orleans-style virtual actors for
  SignalX.
- `defineActor` with options-object + `methods`/`streams` factories; state
  as a deep `@sigx/reactivity` signal; explicit `ctx.save()` (default) or
  opt-in write-behind persistence.
- Single-node silo (`createSilo`): lazy activation directory, per-activation
  mailbox with turn-based concurrency, idle sweep, graceful deactivation and
  shutdown, `deactivateType` for dev/HMR.
- Call-chain reentrancy metadata with immediate `ActorDeadlockError` on
  cycles; `reentrant: true` for call-chain re-entry.
- `ActorStorage` seam with etag optimistic concurrency; `memoryStorage()`
  and `fileStorage({ dir })` providers; conflict = fault-and-reload
  (Orleans `InconsistentStateException` semantics).
- Volatile timers (mailbox-serialized, coalesced) and durable reminders
  (storage-backed, re-activate idle actors, 60s floor).
- Wire layer riding the serverFn endpoint: `handleActorRequest` /
  `matchesActorRequest` / `createActorResolver` (`./server`), generic typed
  client proxy + `configureActors` (`./client`), `createActorHandler` +
  `attachSignalHandlers` (`./node`). NDJSON streams end-to-end.
- Transport-independent guard chains (`use` / `methodUse`) shared by wire
  and in-process calls; `requireGuards` build gate (default on).
- `sigxActors()` Vite plugin: `*.actor.ts` client-module swap, prod registry
  (`virtual:sigx-actors`), dev silo through the SSR module runner, HMR
  deactivate-through-storage.

### Changed

- `ActorTransport` is now the transport **interface**; the old
  `{ endpoint, headers, fetch }` config type is `ActorTransportConfig`, and
  `ActorClientCallOptions` is `ActorCallInit`. `configureActors({ … })` call
  sites are unaffected.

- Reminder-table mutations now reload-and-reapply on a storage etag
  conflict (up to 3 attempts) instead of failing — the table legitimately
  has concurrent writers once silos share storage.
- `mintCallId()` ids carry a per-process random component so ids from
  different hosts and restarts are distinguishable. The format remains
  opaque.

### Fixed

- **The reminder tick no longer rewrites shards it did not change** (#74).
  `#tickShard` runs for every owned shard on every tick, and the mutation
  path saved unconditionally — so a silo with **no reminders at all** wrote
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
  consumers of `@sigx/actors/silo` etc. got an implicit-`any` module.
- External wire calls now inherit the silo's `callTimeoutMs` deadline, as
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
