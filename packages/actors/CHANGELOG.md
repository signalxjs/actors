# Changelog

## [Unreleased]

### Added

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
