# @sigx/actors-surreal

SurrealDB providers for [`@sigx/actors`](../actors), for the team whose one
durable store is SurrealDB. Four providers, one connection:

- **`surrealStorage`** — the `ActorStorage` seam with etag CAS, so two hosts
  can never both persist an activation's state.
- **`surrealMembership`** — TTL heartbeats judged on the **database** clock, so
  a skewed host cannot fake a death or a survival.
- **`surrealDirectory`** — the single-activation claim: create-if-absent that
  returns the *winner*, plus compare-and-delete release/evict.
- **`surrealReminders`** — durable reminders on a due-time-indexed table, one
  indexed query per tick instead of scanning the default shard records.

`surrealdb` (the JS SDK) ≥ 2.0.8 is a peer dependency; SurrealDB **≥ 3.0**,
**3.2.4 or newer recommended**. Prefer a `ws://`/`wss://` endpoint: the HTTP
engine re-authenticates on every request and cannot serve live queries.

## Setup

DDL is explicit — the providers never issue it, so a production role needs only
DML grants. Unlike Postgres this is **not optional**: reading an undefined table
is an error in SurrealDB 3 (it returned `[]` in 2.x).

```ts
import { Surreal } from 'surrealdb';
import { ensureSurrealSchema, surrealRetryable } from '@sigx/actors-surreal';

const db = new Surreal();
await db.connect('ws://127.0.0.1:8000', {
    namespace: 'app',
    database: 'main',
    authentication: { username: 'root', password: 'root' },
    // REQUIRED — see "Retry is part of the contract" below.
    retry: { enabled: true, attempts: 5, retryable: surrealRetryable }
});
await ensureSurrealSchema(db);
```

Then wire a clustered host:

```ts
import { defineActorApp } from '@sigx/actors/host';
import { cluster } from '@sigx/actors/cluster';
import { surrealCluster, surrealReminders, surrealStorage } from '@sigx/actors-surreal';

const app = defineActorApp({
    actors,
    storage: surrealStorage({ db }),
    // Without this the runtime keeps the default sharded reminders (which
    // also work over surrealStorage) — pass the provider to get the indexed
    // table.
    reminders: surrealReminders({ db })
}).use(
    cluster({
        providers: surrealCluster({ db }),
        advertise: process.env.ADVERTISE!,
        secret: process.env.CLUSTER_SECRET!
    })
);
```

Every provider takes either `db` (a connected `Surreal`, shared with your app —
one socket multiplexes everything) or `url` plus `namespace`/`database`/`auth`,
in which case the package connects lazily and owns the socket. `prefix`
(default `sigx_`) names the tables; `heartbeatMs` (5 s), `ttlMs` (15 s),
`pollMs` (5 s) and `push` (true) tune membership.

## Retry is part of the contract

**If you pass your own `db`, you must install `surrealRetryable` on it.**

SurrealDB has no `SELECT … FOR UPDATE`, no `SKIP LOCKED` and no advisory lock.
Snapshot isolation with a commit-time write–write conflict check is the only
mutual exclusion available, so `claim()` and the create arm of `save()` are
correct *because* two racers collide and the loser re-runs to observe the
winner. Without a retry the loser raises a raw conflict error instead of
returning the winning entry.

The SDK ships retry **disabled by default**, and its own `isRetryableConflict`
matches only the structured `TransactionConflict` detail (wire code `-32009`),
which in practice never arrives — a conflicting statement surfaces
`Transaction conflict: Write conflict, retry the transaction. This transaction
can be retried` through the `NotExecuted` path instead. `surrealRetryable`
matches that, as SurrealDB's own tests do.

## Semantics worth knowing

- **Etags are opaque and client-minted** (`crypto.randomUUID()`); the runtime
  only ever compares them.
- **State is stored as a JSON string, deliberately.** Actor state is whatever
  the codec produced: it may be a top-level array or scalar, may contain NUL,
  and distinguishes `null` from absent. Round-tripping that through SurrealDB's
  value model would risk `none`/`null` conflation, datetime and record-id
  reinterpretation, and v3's collapsing of differently-typed numeric ids. The
  cost is that state is opaque in Surrealist; the benefit is that it round
  -trips exactly, in one `JSON.stringify` rather than a per-field CBOR walk.
- **`UPDATE … WHERE`, never `UPSERT … WHERE`.** `UPSERT`'s create arm carries
  no condition check, so a writer holding a stale etag whose record had since
  been deleted would *resurrect* it.
- **No NUL escaping layer.** An actor id is `type<NUL>key`, and SurrealDB
  carries a NUL verbatim and injectively inside a record id. (Postgres `text`
  cannot, which is the only reason `@sigx/actors-pg` has `pgText`.)
- **Every hot-path record is addressed by its record id** — the primary index.
  `{prefix}state:[type, key]` is a composite id, not a secondary index, so a
  load or save never scans. The three secondary indexes serve only cold sweeps
  (`evictHost`, the host-expiry prune, the due-reminder claim).
- **Directory entries carry no TTL**: an entry is valid iff its host is live in
  the membership view — one heartbeat per host, not per activation.
- **Membership change detection compares host signatures, not just the version
  counter**, because a silent expiry changes the view without bumping anything.
- **Membership push is best-effort.** It is a live query on the version table
  (record-scoped live queries fail to listen on 3.2.4; table-scoped work, and
  that table holds one record). Live queries are documented single-node-only,
  unordered and at-most-once, and a silent expiry produces no write to notify
  on — so the **poll is the guarantee**. Set `push: false` to disable it.
- **Reminders honour shard ownership**, the opposite of `pgReminders`, which
  ignores it because `SKIP LOCKED` lets every host claim disjoint rows from one
  scan. With no lock available, partitioning is what removes contention: each
  row carries a shard, a host claims only shards it owns, and the compound
  `(sh, d)` index serves the query. Transient divergence stays safe — the claim
  is one transaction, so a loser aborts and re-selects an already-advanced set.
- Reminder firing is **at-most-once**: the advance/delete commits before any
  delivery, and a periodic reminder advances to `time::now() + period`, so
  downtime costs one firing rather than replaying the gap.

## Tests

The suite is env-gated: without `SURREAL_URL` it skips, and CI runs it against
a real SurrealDB 3 container on one Linux job.

```sh
docker run -d --name surrealdb -p 8000:8000 surrealdb/surrealdb:v3.2.4 \
  start --bind 0.0.0.0:8000 --username root --password root memory

SURREAL_URL=ws://127.0.0.1:8000 SURREAL_USER=root SURREAL_PASS=root \
  pnpm test actors-surreal
```
