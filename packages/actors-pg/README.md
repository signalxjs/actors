# @sigx/actors-pg

Postgres providers for [`@sigx/actors`](../actors) — for the team whose one
durable store is SQL. Three providers, one pool:

- **`pgStorage()`** — etag-CAS `ActorStorage`: one row per actor, no TTL,
  a conflict throws the branded `ActorStorageConflict` the runtime turns
  into fault-and-reload. The cluster-safe persistence option.
- **`pgMembership()`** — TTL-heartbeat membership with the expiry judged on
  the **database clock** (host clock skew cannot fake a death or a
  survival), LISTEN/NOTIFY push with a poll fallback, and self-suspect
  fencing when this host cannot prove its own membership.
- **`pgReminders()`** — durable reminders on an indexed table: a tick is
  one `FOR UPDATE SKIP LOCKED` claim statement for exactly the due rows
  (advance/delete commits BEFORE delivery — at-most-once, no catch-up
  bursts, database clock throughout), and every host may tick the same
  table because row locks replace shard ownership. The reminder-scan
  answer for large tables.
- **`pgDirectory()`** — the single-activation claim directory:
  create-if-absent claim, compare-and-delete release/evict, and an
  `evictHost` sweep for departed hosts.

`pg` (node-postgres) ≥ 8 is a peer dependency; Postgres ≥ 13.

## Setup

Tables live in one Postgres schema (default `sigx`). DDL is explicit — the
providers never issue it, so a production role needs only DML grants:

```ts
import pg from 'pg';
import { ensurePgSchema, pgCluster, pgReminders, pgStorage } from '@sigx/actors-pg';

const pool = new pg.Pool({ connectionString: process.env.PG_URL });
await ensurePgSchema(pool);            // dev/tests; prod: pgSchemaSql() via your migration tool
```

Clustered host, everything on the one pool:

```ts
import { defineActorApp } from '@sigx/actors/host';
import { cluster } from '@sigx/actors/cluster';

const app = defineActorApp({
    actors,
    storage: pgStorage({ pool }),
    // Without this the runtime keeps the default sharded reminders (which
    // also work over pgStorage) — pass the provider to get the indexed table.
    reminders: pgReminders({ pool })
}).use(
    cluster({
        providers: pgCluster({ pool }),
        advertise: process.env.ADVERTISE!,
        secret: process.env.CLUSTER_SECRET!
    })
);
```

Options (all providers): `pool` **or** `url` (the package then constructs
its own `pg.Pool`), `schema` (default `sigx`, validated as an SQL
identifier). Membership adds `heartbeatMs` (5 s), `ttlMs` (15 s), `pollMs`
(5 s) and `coalesceMs` (0 — trailing quiet window for coalescing NOTIFY
pushes; the listener is single-flight either way, so a burst of N changes
costs one refresh plus at most one catch-up, not N).

## Semantics worth knowing

- **Etags are client-minted UUIDs**, equality-compared only. Every CAS is
  a single statement whose row count is the verdict — no transactions, no
  advisory locks.
- **State is `jsonb`**, always bound as a JSON string with an explicit
  cast — a top-level array state cannot be silently coerced into a
  Postgres ARRAY.
- **Directory entries carry no TTL** — one heartbeat per host, not per
  activation; entry validity is the owner's liveness in the membership
  view, and the storage etag CAS remains the integrity floor underneath.
- **Membership change detection compares host signatures, not just the
  version counter** — a host that dies silently expires on the database
  clock without anyone bumping a version, and views still converge.
- **Push is best-effort.** LISTEN rides a connection checked out of the
  pool; if the pool cannot dedicate one (or the connection drops), the
  poll is the guarantee and `pollMs` is the propagation bound.

## Tests

Env-gated on `PG_URL` (CI provides a `postgres:16` service container; the
rest of the matrix skips cleanly):

```sh
PG_URL=postgres://postgres:postgres@localhost:5432/postgres pnpm test actors-pg
```
