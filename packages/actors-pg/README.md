# @sigx/actors-pg

Postgres providers for [`@sigx/actors`](https://sigx.dev/actors) — for the team
whose one durable store is SQL. Four providers, one pool.

- **`pgStorage()`** — etag-CAS `ActorStorage`, one row per actor.
- **`pgMembership()`** — TTL heartbeats judged on the **database** clock, with
  LISTEN/NOTIFY push and a poll fallback.
- **`pgDirectory()`** — the single-activation claim directory.
- **`pgReminders()`** — durable reminders on an indexed table; one
  `FOR UPDATE SKIP LOCKED` claim statement per tick, so every host may tick.
- **`pgCluster()`** bundles membership + directory;
  **`pgSchemaSql()`** / **`ensurePgSchema()`** provide the DDL.

DDL is explicit — the providers never issue it, so a production role needs only
DML grants. `ensurePgSchema()` is safe to call from every replica at boot,
concurrently.

```sh
pnpm add @sigx/actors-pg pg
```

Requires **Postgres ≥ 13**. [`pg`](https://node-postgres.com/) (≥ 8) is a peer
dependency, as is `@sigx/actors` itself.

## Documentation

**https://sigx.dev/actors/packages/actors-pg/overview/**

Clustering guide: https://sigx.dev/actors/docs/clustering/ ·
Storage seam: https://sigx.dev/actors/docs/storage/

Source, examples and architecture notes:
https://github.com/signalxjs/actors

MIT © Andreas Ekdahl
