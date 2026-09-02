# @sigx/actors-sqlite

SQLite actor storage for [`@sigx/actors`](https://sigx.dev/actors) — durable
state in one file on Node's built-in `node:sqlite`, with nothing to install and
nothing to operate. The step up from `fileStorage` for a single-node host:
a real database, one B-tree probe per load, one statement per save.

- **`sqliteStorage()`** — etag-CAS `ActorStorage`, one row per actor in a
  table keyed by `(type, key)`; the etag is the row's version. Implements
  `saveText`, so a durable save walks the state once.

The table is created on open (`IF NOT EXISTS`) — there is no DDL step. Pass a
`path` and the package opens the file (WAL journal, 5 s busy timeout), or pass
your own open `DatabaseSync`. `close()` closes it.

```sh
pnpm add @sigx/actors-sqlite
```

Node-only and **Node ≥ 22.13** (`node:sqlite` is unflagged from there, but
still marked experimental: the process prints one `ExperimentalWarning: SQLite
is an experimental feature` line to stderr on first import — harmless, and
`node --disable-warning=ExperimentalWarning` silences it); no runtime
dependencies beyond the `@sigx/actors` peer. Single-node by design: two
hosts sharing one file serialize on SQLite's write lock — for a cluster, use
[`@sigx/actors-redis`](https://www.npmjs.com/package/@sigx/actors-redis) or
[`@sigx/actors-pg`](https://www.npmjs.com/package/@sigx/actors-pg).

## Documentation

**https://sigx.dev/actors/packages/actors-sqlite/overview/**

Storage seam: https://sigx.dev/actors/docs/storage/

Source, examples and architecture notes:
https://github.com/signalxjs/actors

MIT © Andreas Ekdahl
