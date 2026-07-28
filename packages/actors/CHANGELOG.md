# Changelog

## [Unreleased]

### Added

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
