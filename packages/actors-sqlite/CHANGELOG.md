# Changelog

## [Unreleased]

### Added

- **`@sigx/actors-sqlite`** (#65): `sqliteStorage()`, an etag-CAS
  `ActorStorage` on `node:sqlite` — one row per actor in a single table
  keyed by `(type, key)`, the etag a row version the database mints, one
  statement (one SQLite write transaction) per save or clear, and
  `saveText` implemented so a durable save walks the state once (#238).
  The table is created on open; pass a `path` (the package opens the file
  with a WAL journal and a busy timeout) or an open `DatabaseSync`, and
  `close()` when the host stops. `type` and `key` go through the same
  injective NUL escape `@sigx/actors-pg` uses, because SQLite stores a
  NUL-bearing string whole and then truncates it on every read. Runs the
  shared `storageConformance` suite. Requires **Node >= 22.13** — the
  package's `engines` says so, and its tests skip where `node:sqlite` is
  unavailable.
