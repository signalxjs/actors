# Changelog

## [Unreleased]

### Added

- **`pgStorage` implements `appendText`** (#312): a `log text[] NOT NULL
  DEFAULT '{}'` column on `{schema}.state`, appended with `array_append`
  under the same row-count CAS as a save; a full save sets `log = '{}'` in
  the statement that writes the snapshot, and `load` selects it. **Schema
  change**: `pgSchemaSql()` declares the column in the `CREATE TABLE` and
  follows it with `ALTER TABLE … ADD COLUMN IF NOT EXISTS`, so a schema
  created by an earlier version is upgraded by `ensurePgSchema()` at the
  next boot — or by carrying the new `pgSchemaSql()` through whatever tool
  owns the migrations. Rows written before the column read back with an
  empty log.

### Fixed

- **A reminder whose dispatch fails is retried next tick instead of being
  lost, and counted** (#326). `pgReminders` claims a due row — advancing a
  periodic one, deleting a one-shot — *before* it dispatches, and a rejected
  `deliver()` (a call deadline, a host mid-restart, an `onReminder` that
  threw) was at most logged: the wake was gone, and
  `HostStats.remindersUndelivered` read `0`. The default `shardedReminders()`
  got the retry in #306; this is the same contract here. A rejected (or
  synchronously throwing) `deliver()` now re-arms its row one tick out on
  the database clock (`next_due = now() + tickMs`: a one-shot re-inserted, a
  periodic one pulled forward) in ONE statement per claimed batch, and each
  failed attempt is reported through `ActorRemindersContext.undelivered`, so
  the host's counter, `ops()`, `metrics()` and the cluster's per-host report
  now say so on Postgres too. Same rules as the sharded table: a row the
  actor set again meanwhile is left as the actor set it (the re-arm compares
  against the `next_due` the claim wrote — a later decision wins), a periodic
  one it cleared stays cleared, and a one-shot it cleared while its dispatch
  was failing may be retried once — so `onReminder` should be idempotent, the
  assumption every at-least-once consumer already makes.
- **`leave()` no longer races its own heartbeat** (#209). The beat wrote
  the host row with an untracked `void writeSelf()` every `heartbeatMs`;
  `leave()` cleared the interval and issued its `DELETE`, but an upsert
  the pool already held was neither awaited nor ordered against it — on
  another pool connection it committed *after* the `DELETE`, resurrecting
  the row until its TTL lapsed. Peers saw a host that had left cleanly for
  up to `ttlMs`; on CI it surfaced as a stale host leaking into the next
  test case. `pgMembership` now tracks in-flight heartbeat writes,
  `leave()` drains them before the `DELETE`, and a beat that completes
  after `leave()` began no longer confirms the liveness clock.
- **`MembershipView.version` moves when a peer expires** (#267).
  `membership_version` is bumped only by a host that WRITES (join,
  `setStatus`, leave); a peer dying silently has no writer, so its TTL
  expiry changed `hosts` while `version` stood still, and a consumer
  memoizing on `version` latched the stale member count. `pgMembership`
  now advances the exposed version locally (`max(stored, cached + 1)`)
  whenever the host signature changes without a counter bump, and
  re-aligns with the counter only once written bumps carry it past the
  advanced value. The counter itself
  is untouched and remains the LISTEN/NOTIFY skip gate — a bump whose
  payload merely catches up to a locally advanced view is still refreshed.
- **A failing membership prune is no longer silent** (#268). The lazy
  expiry prune in `pgMembership` swallowed every failure — a permanently
  failing `DELETE` (permissions, schema drift) accumulated expired
  `hosts` rows forever with no signal. Under dev it now warns once per
  membership, matching the reminders tick's posture. The prune stays
  best-effort: a failure never affects the refresh itself.

## [0.9.2] - 2026-08-17

### Added

- **`pgStorage` implements `ActorStorage.saveText`** (#238), so a durable
  save no longer walks the same state twice. The host emits the record's JSON
  in one pass and it lands in the `state` `text` column directly, instead of
  the host building a tree that `JSON.stringify` here immediately re-walked —
  measured at +51% on top of the host's own encode. `save` is now defined in
  terms of the same CAS body, so the two entry points cannot drift. No schema
  change, no wire change, and a record written either way is byte-identical.

## [0.5.0] - 2026-08-07

### Changed

- **Peers `@sigx/actors@^0.5.0`.** The family versions in lockstep, so the
  range moves with the release. 0.5.0 only ADDS `ctx.changes({ throttleMs })`
  and removes a snapshot a `$live` watch never read (#129) — no wire or API
  break, so a 0.4.x host interoperates fine.

## [0.4.0] - 2026-08-07

### Changed

- **Peers `@sigx/actors@^0.4.0`.** The family versions in lockstep, so the
  range moves with the release. Nothing else to do: 0.4.0 only ADDS
  `onSettled` to `defineJob` (#125), so unlike the 0.2.0 and 0.3.0 moves
  there is no wire or API break and a 0.3.x host interoperates fine.

- README trimmed to a pointer at https://sigx.dev/actors (#113): thesis,
  install, peer-dependency and minimum-version requirements, and links. The
  reference material is on the docs site; relative links (which npm does not
  resolve) are gone. No code or API change.

## [0.3.0] - 2026-08-05

### Fixed

- **Two replicas booting at once crashed all but one of them (#78).** The
  Postgres half of #76, found by sweeping the providers for that bug class.
  `ensurePgSchema()` ran its DDL once, unguarded — and `CREATE SCHEMA/TABLE/
  INDEX IF NOT EXISTS` is check-then-create, *not* atomic against a concurrent
  creator, so racing boots collide in the catalog (`23505` on
  `pg_namespace_nspname_index` and friends, `42P07`, `42710`, `40P01`). Because
  node-postgres sends the values-free multi-statement string as a simple query,
  Postgres runs it as one implicit transaction and the whole bootstrap rolls
  back: the boot fails.
  It now takes `pg_advisory_xact_lock`, keyed on the schema name, as the FIRST
  statement of that same string — so the lock spans the whole DDL and is
  released at its commit, before the pooled client is recycled — with the same
  bounded jittered retry as the surreal sibling underneath, as a backstop for
  racers that take no lock (a migration tool, psql, an older release).
  `pgSchemaSql()` is unchanged and still pure DDL. Measured against a live
  Postgres 16: eight hosts racing the bootstrap produced seven failed boots
  before, and none after.

### Documentation

- **Corrected: state is stored as `text`, not `jsonb`.** The README claimed
  `jsonb` while the DDL and `pgStorage` have always used `text`, deliberately —
  actor state may contain NUL, which `jsonb` rejects.

- **A stalled host kept serving actors a survivor had taken over (#45).**
  Self-suspicion fired only from a heartbeat write *rejection*, so a host
  whose event loop stalled past `ttlMs` resumed, upserted late and
  succeeded — and because the upsert re-creates the row, it looked
  perfectly healthy again while peers had already expired it and released
  its directory claims. The beat now runs on the shared `heartbeatClock()`:
  a beat starting more than `ttlMs` after the last confirmed write fires
  `onSelfSuspect` before it writes.

### Changed

- **Peers `@sigx/actors@^0.3.0`.** The actor URL grammar is breaking
  (#96), so the whole family moves together — see the `@sigx/actors`
  changelog. **Upgrade every host before any client or peer starts
  emitting**: a host still on 0.2.x refuses calls from an upgraded one.

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
