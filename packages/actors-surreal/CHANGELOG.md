# Changelog

## [Unreleased]

### Fixed

- **A stalled host kept serving actors a survivor had taken over (#45).**
  Self-suspicion fired only from a heartbeat write *rejection*, so a host
  whose event loop stalled past `ttlMs` resumed, upserted late and
  succeeded — and because the UPSERT re-creates the record, it looked
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

- **Membership pushes are coalesced** (#26): the live-query subscriber is
  now single-flight — a burst of N membership changes costs one refresh
  plus at most one trailing catch-up instead of N full re-reads (events are
  unordered and at-most-once, so none are version-skippable). New
  `coalesceMs` option (default 0); `refresh()` keeps its contract (resolves
  with a refresh that started at-or-after the call).

### Added

- **The package** (#18): SurrealDB 3 providers for `@sigx/actors`.
  `surrealStorage()` — etag-CAS `ActorStorage` over a COMPOSITE record id
  (`{prefix}state:[type, key]`, so every load and save is a primary-index
  hit and never a scan), with state held as a JSON string so top-level
  arrays, scalars, `null` and NUL all round-trip exactly;
  `surrealMembership()` — TTL-heartbeat membership judged on the database
  clock, live-query push with poll fallback, signature-based change
  detection so silent deaths converge without a version bump, self-suspect
  fencing; `surrealDirectory()` — create-if-absent claim returning the
  winner, compare-and-delete release/evict, `evictHost` sweep;
  `surrealCluster()` bundling membership + directory; `surrealReminders()`
  — durable reminders on a due-time-indexed table, one indexed query per
  tick, advance-before-delivery (at-most-once), `time::now() + period` so
  downtime costs one firing rather than a catch-up burst, and a 60 s period
  floor matching the default provider; `surrealSchemaSql()` /
  `ensureSurrealSchema()` — explicit, idempotent DDL for one validated
  table prefix (the providers never issue DDL themselves); and
  `surrealRetryable()` — the conflict predicate a shared connection must
  install. `surrealdb` ≥ 2.0.8 as a peer dependency, SurrealDB ≥ 3.0 (3.2.4+
  recommended); provider tests env-gated on `SURREAL_URL` with a dedicated
  CI job.

  Four v3 behaviours shape the implementation, each verified against a live
  3.2.4 server rather than taken from the docs:

  - **`UPSERT … WHERE` does not gate its create path**, so it would
    resurrect a record a concurrent writer had deleted; state CAS therefore
    uses `UPDATE … WHERE` plus an explicit create.
  - **There is no `SELECT … FOR UPDATE`, `SKIP LOCKED` or advisory lock.**
    The commit-time write–write conflict check is the only mutual
    exclusion, which makes client retry part of the contract rather than an
    optimisation — hence `surrealRetryable`, and hence reminders partition
    by `ownsShard` (the opposite of `pgReminders`, which ignores it because
    row locks replace rendezvous hashing).
  - **`UPDATE`/`DELETE` do not use indexes**, so every predicate-driven
    write pushes its predicate through a `SELECT` subquery.
  - **Reading an undefined table is an error** (2.x returned `[]`), so the
    DDL step is mandatory rather than a convenience.
