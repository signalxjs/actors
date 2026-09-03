# Changelog

## [Unreleased]

### Added

- **`surrealStorage` implements `appendText`** (#312): an `l` array field
  on the state record (`DEFINE FIELD IF NOT EXISTS l … TYPE array<string>
  DEFAULT []` in `surrealSchemaSql()`, so `ensureSurrealSchema()` adds it to
  an existing schema), appended with `l += $entry` under the same etag
  `WHERE` as a save; the CAS save sets `l = []` in the statement that
  writes the snapshot, `CREATE` starts it empty, and `LOAD` selects it — a
  record written before the field existed reads back with an empty log.

### Fixed

- **A reminder whose dispatch fails is retried next tick instead of being
  lost, and counted** (#326). `surrealReminders` claims a due row — advancing
  a periodic one, deleting a one-shot — *before* it dispatches, and a
  rejected `deliver()` (a call deadline, a host mid-restart, an `onReminder`
  that threw) was at most logged: the wake was gone, and
  `HostStats.remindersUndelivered` read `0`. The default `shardedReminders()`
  got the retry in #306; this is the same contract here. A rejected (or
  synchronously throwing) `deliver()` now re-arms its row one tick out on
  the database clock (`d = time::now() + tickMs`: a one-shot re-created, a
  periodic one pulled forward) in ONE transaction per claimed batch, and each
  failed attempt is reported through `ActorRemindersContext.undelivered`, so
  the host's counter, `ops()`, `metrics()` and the cluster's per-host report
  now say so on SurrealDB too. Same rules as the sharded table: a row the
  actor set again meanwhile is left as the actor set it (the re-arm compares
  against the due time the claim wrote — a later decision wins), a periodic
  one it cleared stays cleared, and a one-shot it cleared while its dispatch
  was failing may be retried once — so `onReminder` should be idempotent, the
  assumption every at-least-once consumer already makes. The claim now takes
  `time::now()` once per transaction, so what it advances a periodic row to
  is exactly what the re-arm compares against.
- **`leave()` no longer races its own heartbeat** (#209). The beat wrote
  the host record with an untracked `void writeSelf()` every
  `heartbeatMs`; `leave()` cleared the interval and issued its `DELETE`,
  but an `UPSERT` already on the wire was neither awaited nor ordered
  against it — it could commit *after* the `DELETE` (an `UPSERT` never
  notices a concurrent delete, rule 1 of this package), resurrecting the
  row until its TTL lapsed. Peers saw a host that had left cleanly for up
  to `ttlMs`. `surrealMembership` now tracks in-flight heartbeat writes,
  `leave()` drains them before the `DELETE`, and a beat that completes
  after `leave()` began no longer confirms the liveness clock.
- **`MembershipView.version` moves when a peer expires** (#267). `sigx_mver`
  is bumped only by a host that WRITES (join, `setStatus`, leave); a peer
  dying silently has no writer, so its TTL expiry changed `hosts` while
  `version` stood still — and a consumer memoizing a member count on
  `version` latched the corpses into a concurrency cap for minutes, the
  production finding behind the issue. `surrealMembership` now advances the
  exposed version locally (`max(stored, cached + 1)`) whenever the host
  signature changes without a counter bump, and re-aligns with the counter
  only once written bumps carry it past the advanced value. The counter
  itself is untouched.
- **A failing membership prune is no longer silent** (#268). The lazy
  expiry prune in `surrealMembership` swallowed every failure — a
  permanently failing `DELETE` (permissions, schema drift) accumulated
  expired `sigx_host` rows forever with no signal. Under dev it now warns
  once per membership, matching the reminders tick's posture. The prune
  stays best-effort: a failure never affects the refresh itself.
- **The connection this package owns reconnects without limit** (#272).
  `surrealCluster({ url })` and friends now connect with
  `reconnect: { enabled: true, attempts: -1 }`. The SDK's default is five
  attempts — about 31 s of backoff — after which the socket is dead for the
  life of the process; the membership heartbeat then beats into it silently
  until the host's TTL lapses and it fences. A membership heartbeat is a
  process-lifetime obligation, so its transport may not have a finite one.

### Changed

- **A shared `db` must be connected the same way.** Documented on
  `SurrealConnectionOptions.db` and in the README, next to the existing
  `surrealRetryable` requirement: this package does not own that connection's
  lifecycle and cannot install the option for you.

## [0.9.2] - 2026-08-17

### Added

- **`surrealStorage` implements `ActorStorage.saveText`** (#238), so a
  durable save no longer walks the same state twice. State is held as a JSON
  STRING here by design, so the host's single-pass output is exactly what the
  field wanted — one walk earlier than the `JSON.stringify` this used to run
  over the host's tree, measured at +51% on top of the host's own encode.
  `save` is now defined in terms of the same CAS body, so the two entry points
  cannot drift. No schema change; a record written either way is byte-identical.

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

- **Two replicas booting at once crashed one of them (#76).**
  `ensureSurrealSchema()` issued its DDL exactly once, with no retry of its
  own, so a simultaneous bootstrap from a second host lost the commit-time
  write–write check and the loser's boot failed. Nothing caught it: the only
  retry in play was the SDK's connection-wide one, which ships **disabled**, so
  a caller-owned `db` — the documented setup — had none at all, and the
  reported conflict (`Multiple key errors`) was not a wording
  `surrealRetryable` matched even when it was installed.
  The bootstrap now carries its own bounded, jittered retry, independent of the
  connection's retry posture, and — because a racer may have completed the
  schema while every attempt lost — verifies the tables exist before giving up,
  rethrowing the ORIGINAL error when they do not. The retry is deliberately
  **blind to the error's shape**: the DDL is idempotent and every statement is
  its own transaction, so a bounded retry can only delay a permanent failure,
  never mask one — and matching on wording is what failed here (this server
  family has now produced three distinct conflict wordings).
  `surrealRetryable` is **not** widened: it is the connection-wide predicate for
  a shared `db`, where the same broad rule would turn a caller's genuine
  unique-index violation into a backoff loop that fails anyway.
  Measured against a live 3.2.4 server: eight hosts racing the bootstrap
  produced eight failed boots before, and none after.

- **A stalled host kept serving actors a survivor had taken over (#45).**
  Self-suspicion fired only from a heartbeat write *rejection*, so a host
  whose event loop stalled past `ttlMs` resumed, upserted late and
  succeeded — and because the UPSERT re-creates the record, it looked
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
