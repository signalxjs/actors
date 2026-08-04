# Changelog

## [Unreleased]

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
