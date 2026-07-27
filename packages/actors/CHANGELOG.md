# Changelog

## [Unreleased]

### Added

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

- Reminder-table mutations now reload-and-reapply on a storage etag
  conflict (up to 3 attempts) instead of failing — the table legitimately
  has concurrent writers once silos share storage.
- `mintCallId()` ids carry a per-process random component so ids from
  different hosts and restarts are distinguishable. The format remains
  opaque.

### Fixed

- Subpath `types` conditions in the exports map now point at the emitted
  declaration files (`./dist/<entry>/index.d.ts`) — previously they named
  `./dist/<entry>.d.ts`, which the build never produced, so TypeScript
  consumers of `@sigx/actors/silo` etc. got an implicit-`any` module.
- External wire calls now inherit the silo's `callTimeoutMs` deadline, as
  documented. Previously only in-process external calls got a deadline;
  wire calls could hang past the configured timeout without a 504.
