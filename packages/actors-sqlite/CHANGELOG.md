# Changelog

## [Unreleased]

### Added

- **`sqliteStorage` implements `appendText`** (#312): a sibling table
  `{table}_log(seq INTEGER PRIMARY KEY AUTOINCREMENT, type, key, entry)`,
  created on open beside the state table; an append is one `BEGIN
  IMMEDIATE` transaction — the version bump `WHERE version = ?` is the CAS,
  its row count the verdict, and the entry row lands only when it matched.
  A full save or clear deletes the record's log rows in the same
  transaction as the snapshot (a create stays the single `INSERT`: a record
  that does not exist has no log rows), and `load` reads the state row plus
  the log rows in `seq` order. `type` and `key` go through the same
  injective NUL escape in both tables.

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
  unavailable. `node:sqlite` is unflagged there but still experimental, so
  the host prints one `ExperimentalWarning: SQLite is an experimental
  feature` line on first import; `node --disable-warning=ExperimentalWarning`
  silences it. Passing both `path` and `database` throws (neither silently
  wins), and `close()` is idempotent.
