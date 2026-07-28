# Changelog

## [Unreleased]

### Added

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
