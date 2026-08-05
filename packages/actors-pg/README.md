# @sigx/actors-pg

Postgres providers for [`@sigx/actors`](../actors) — for the team whose one
durable store is SQL. Three providers, one pool:

- **`pgStorage()`** — etag-CAS `ActorStorage`: one row per actor, no TTL,
  a conflict throws the branded `ActorStorageConflict` the runtime turns
  into fault-and-reload. The cluster-safe persistence option.
- **`pgMembership()`** — TTL-heartbeat membership with the expiry judged on
  the **database clock** (host clock skew cannot fake a death or a
  survival), LISTEN/NOTIFY push with a poll fallback, and self-suspect
  fencing when this host cannot prove its own membership — whether the beat
  *failed* past the TTL or merely *landed* past it, which is what a stalled
  event loop does (#45): the upsert succeeds and silently re-creates the
  row, so without the gap check the host would look healthy again while a
  survivor already held its actors.
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

`ensurePgSchema()` is safe to call from **every replica at boot,
concurrently**. `CREATE … IF NOT EXISTS` is check-then-create and is *not*
atomic against a concurrent creator, so it takes a transaction-scoped advisory
lock (keyed on the schema name) as the first statement of the same implicit
transaction as the DDL: concurrent boots queue for milliseconds instead of one
of them crashing on a `23505` from the catalog. A bounded, jittered retry sits
underneath as a backstop for racers that take no lock. Two consequences worth
knowing:

- **`pgSchemaSql()` is pure DDL** and takes no lock — it is the string you hand
  a migration tool, which brings its own.
- **`ensurePgSchema()` expects to own its transaction.** Called with a queryable
  already inside your open transaction, a failure poisons that transaction and
  the retry cannot recover.

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
  advisory locks. (The one advisory lock in this package guards the schema
  bootstrap, not any runtime path.)
- **State is JSON in a `text` column, deliberately not `jsonb`.** Actor state
  may legitimately contain NUL — a reminder shard record keys entries by
  `type<NUL>key` — and `jsonb` parses the JSON escape for NUL into a real NUL byte
  and rejects it, while the serialized form is plain ASCII and always storable.
  It is always bound as a JSON string, so a top-level array state cannot be
  silently coerced into a Postgres ARRAY either.
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
