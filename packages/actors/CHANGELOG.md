# Changelog

## [Unreleased]

### Added

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
