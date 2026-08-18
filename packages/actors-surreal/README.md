# @sigx/actors-surreal

SurrealDB providers for [`@sigx/actors`](https://sigx.dev/actors) — for the team
whose one durable store is SurrealDB. Four providers, one connection.

- **`surrealStorage()`** — etag-CAS `ActorStorage`.
- **`surrealMembership()`** — TTL heartbeats judged on the **database** clock,
  with live-query push and a poll fallback.
- **`surrealDirectory()`** — the single-activation claim directory.
- **`surrealReminders()`** — durable reminders on a due-time-indexed table.
- **`surrealCluster()`** bundles membership + directory;
  **`surrealSchemaSql()`** / **`ensureSurrealSchema()`** provide the DDL.

Three things to know before you wire it up:

- **The DDL step is mandatory.** Reading an undefined table is an error in
  SurrealDB 3 (2.x returned `[]`).
- **Retry is part of the contract.** SurrealDB has no `SELECT … FOR UPDATE`,
  no `SKIP LOCKED` and no advisory lock, so the commit-time write–write
  conflict is the only mutual exclusion — and the SDK ships retry disabled. If
  you pass your own `db`, install **`surrealRetryable`** on it.
- **A shared `db` needs unlimited reconnect.** The SDK gives up after five
  reconnect attempts (~31 s), after which the socket is dead for the life of
  the process — and a membership heartbeat then beats into it silently until
  the host's TTL lapses. Connect with
  `reconnect: { enabled: true, attempts: -1 }`. Passing `url` instead of `db`
  does this for you.

```ts
await db.connect(url, {
    reconnect: { enabled: true, attempts: -1 },
    retry: { enabled: true, attempts: 5, retryable: surrealRetryable }
});
```

```sh
pnpm add @sigx/actors-surreal surrealdb
```

Requires **SurrealDB ≥ 3.0** (3.2.4 or newer recommended); prefer a
`ws://`/`wss://` endpoint. [`surrealdb`](https://surrealdb.com/docs/sdk/javascript)
(≥ 2.0.8) is a peer dependency, as is `@sigx/actors` itself.

## Documentation

**https://sigx.dev/actors/packages/actors-surreal/overview/**

Clustering guide: https://sigx.dev/actors/docs/clustering/ ·
Storage seam: https://sigx.dev/actors/docs/storage/

Source, examples and architecture notes:
https://github.com/signalxjs/actors

MIT © Andreas Ekdahl
