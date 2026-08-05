# Changelog

## [Unreleased]

### Fixed

- **A stalled host kept serving actors a survivor had taken over (#45).**
  Self-suspicion fired only from a heartbeat write *rejection*, so a host
  whose event loop stalled past `ttlMs` resumed, upserted late and
  succeeded — and because the upsert re-creates the row, it looked
  perfectly healthy again while peers had already expired it and released
  its directory claims. The beat now runs on the shared `heartbeatClock()`:
  a beat starting more than `ttlMs` after the last confirmed write fires
  `onSelfSuspect` before it writes.

## [0.2.0] - 2026-08-05

### Changed

- **Peers `@sigx/actors@^0.2.0`.** The guard split is breaking, so the
  whole family moves together — see the `@sigx/actors` changelog and core's
  [0.15 migration guide](https://github.com/signalxjs/core/blob/main/docs/migrations/0.15-guard-split.md).
  Actors, workers and jobs defined against this package declare access with
  `authorize` / `methodAuthorize` / `allowAnonymous` now, and the runtime is
  fail-closed: one that declares nothing, in a process with no server app,
  denies with 401.

### Changed

- **Membership pushes are coalesced** (#26): the LISTEN handler is now
  single-flight — a burst of N membership changes costs one refresh plus at
  most one trailing catch-up instead of N full re-reads, and a NOTIFY whose
  payload version the view has already caught up past costs nothing. New
  `coalesceMs` option (default 0); `refresh()` keeps its contract (resolves
  with a refresh that started at-or-after the call).

## [0.1.0] - 2026-08-03

### Added

- **`pgReminders()`** (#242, closing it): durable reminders on a
  due-time-indexed table — the reminder-scan answer for deployments with
  many reminders, through the existing `ActorReminders` seam rather than a
  new storage capability. One atomic claim statement per tick
  (`FOR UPDATE SKIP LOCKED` + CTE arms advancing periodic rows and
  deleting one-shots): the advance commits BEFORE delivery (at-most-once;
  a crash between commit and dispatch skips one firing), `GREATEST(next_due,
  now()) + period` resumes after downtime with one firing and no catch-up
  burst, row locks replace shard ownership so every host ticks the same
  table safely, and the database clock rules throughout. 60 s period floor,
  same as the default provider. Plus `pgTextDecode()` — the exact inverse
  of `pgText`, needed because a claimed row must become a deliverable ref
  again.

- **The package** (#242): Postgres providers for `@sigx/actors`.
  `pgStorage()` — etag-CAS `ActorStorage` (single-statement CAS, row count
  as verdict, `jsonb` state bound as an explicit JSON string so array
  states survive); `pgMembership()` — TTL-heartbeat membership judged on
  the database clock, LISTEN/NOTIFY push with poll fallback,
  signature-based change detection so silent deaths converge without a
  version bump, self-suspect fencing; `pgDirectory()` — create-if-absent
  claim, compare-and-delete release/evict, `evictHost` sweep; `pgCluster()`
  bundling membership + directory; `pgSchemaSql()` / `ensurePgSchema()` —
  explicit, idempotent DDL for one validated schema (the providers never
  issue DDL themselves). `pg` ≥ 8 as a peer dependency; provider tests
  env-gated on `PG_URL` with a dedicated CI job.
