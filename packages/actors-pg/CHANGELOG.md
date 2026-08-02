# Changelog

## [Unreleased]

### Added

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
