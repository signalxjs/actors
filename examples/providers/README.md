# providers example

The provider packages, each as a **seam swap on one three-host cluster**:
Postgres and SurrealDB for storage + membership, the TCP host-to-host
transport, and the Prometheus / OpenTelemetry exporters. No UI — one script
per package, and every step asserts its way to `COMPLETE`.

```sh
pnpm install && pnpm build
pnpm --filter providers-example tcp        # runs with nothing installed
pnpm --filter providers-example otel       # runs with nothing installed
pnpm --filter providers-example pg         # skips unless PG_URL is set
pnpm --filter providers-example surreal    # skips unless SURREAL_URL is set
```

```
host s.uv6pnexb listening on :5591  (tcp=tcp://127.0.0.1:57656)
[sigx actors] a second host was started while one is already running; the new one wins. Stop the old host first unless this is a dev-server restart.
host s.2epxqo0h listening on :5592  (tcp=tcp://127.0.0.1:57657)
[sigx actors] a second host was started while one is already running; the new one wins. Stop the old host first unless this is a dev-server restart.
host s.fcfu6ryx listening on :5593  (tcp=tcp://127.0.0.1:57658)

=== 1. Placement spread — 9 counters created via host 0 alone ===
activations per host: s.uv6pnexb=3  s.2epxqo0h=4  s.fcfu6ryx=2
(host 0 took the calls; the placement policy spread the actors)

=== 2. Single activation — the same key hammered through ALL hosts ===
6 concurrent increments via 3 hosts → [ 1, 2, 3, 4, 5, 6 ]
'cart-mtk87zvc' has exactly one owner: s.uv6pnexb (:5591)

=== 3. Cross-host calls — through a host that does NOT own the actor ===
increment(10), increment(100) via s.2epxqo0h → count=116
(2 remote dispatches left s.2epxqo0h; the turns ran on s.uv6pnexb)

=== 4. Socket count — 32 concurrent calls into the owner from ONE peer ===
owner s.uv6pnexb's HTTP listener holds 0 connection(s) after the burst
(TCP: the calls rode s.2epxqo0h's single connection to tcp://127.0.0.1:57656; fallbacks to http: 0)

=== 5. Owner s.uv6pnexb leaves — a survivor re-loads from memoryStorage() ===
survivors' view: s.2epxqo0h sees 2 host(s), s.fcfu6ryx sees 2 host(s)
survivor s.2epxqo0h serves 'cart-mtk87zvc' → count=148
directory re-claimed by: s.fcfu6ryx
(the activation died with its host; the state came back from memoryStorage())

=== 6. Ops surface — clusterStats() from a survivor ===
view v5: 2 members, 2 active
  host          status  activations  out  in  fallbacks  transports
  s.2epxqo0h  active            4   38   5          0  tcp,http
  s.fcfu6ryx  active            3    3   5          0  tcp,http
totals: 7 activations { Counter: 7 }
partial: false

TCP DEMO COMPLETE — the same cluster over one TCP connection per peer.
```

The "second host" warning is the dev dist noticing three hosts in one
process. It is the demo shape, not a deployment one: `actor()` resolves the
process's host through a global, and the demos never use that path — every
call goes through `member.host.actor(...)` explicitly.

Steps 1–3, the failover and the report are adapted from `examples/counter`'s
cluster demo walk (step 3 there is a cross-host `watch()` stream, and its
failover is a crash rather than a graceful leave — see Non-goals), lifted
into `src/cluster.ts` with its three seams as options:

| seam | default | swapped for |
|---|---|---|
| `storage` | `memoryStorage()` | `pgStorage`, `surrealStorage` |
| `providers` (membership + directory) | `memoryClusterHub()` | `pgCluster`, `surrealCluster` |
| `transport` (host-to-host) | `httpTransport()` | `[tcpTransport(), httpTransport()]` |

plus `plugins`, for the exporters. Nothing else differs between the four
scripts, which is the point: a provider is a handful of lines at the seam.

## What to look at when you open it

**`pg-demo.mjs` — the DDL is yours** (`ensurePgSchema`). Neither provider
ever issues DDL, so a production role needs DML grants only; the schema step
is explicit at boot, idempotent, serialised under an advisory lock, and safe
from every replica at once. The demo prints how many statements ran, then
after the walk reads the rows back with plain SQL — `state` is JSON in a
`text` column, one row per actor, and the departed host's row in `hosts` is
*gone* because it left gracefully (a crash leaves it until `expires_at`,
judged on the database clock, never a host's). Without `PG_URL`:

```
$ pnpm --filter providers-example pg
[providers] pg demo SKIPPED: PG_URL is not set.
  It needs a Postgres >= 13. One way to get one:
    docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16
    PG_URL=postgres://postgres:postgres@localhost:5432/postgres pnpm --filter providers-example pg
  (The @sigx/actors-pg test suite gates on the same variable; CI runs it in a dedicated job.)
```

**`surreal-demo.mjs` — retry is the contract, not an optimisation.**
SurrealDB has no `SELECT … FOR UPDATE`, no advisory lock: a commit-time
write–write conflict is the only mutual exclusion there is, and the loser
must re-run to observe the winner. The SDK ships retry *disabled*, so the
demo builds its own connection to show what `surrealRetryable` and an
unlimited `reconnect` do (pass `url` instead of `db` and the package does
this for you). The DDL step is *mandatory* on v3 — reading an undefined table
errors — and `connect()` selects a namespace/database without creating them,
so the demo does that too, as root, against a throwaway. Without
`SURREAL_URL`:

```
$ pnpm --filter providers-example surreal
[providers] surreal demo SKIPPED: SURREAL_URL is not set.
  It needs a SurrealDB >= 3.0 (3.2.4 or newer recommended) on a ws:// endpoint. One way to get one:
    docker run --rm -p 8000:8000 surrealdb/surrealdb:latest start --user root --pass root memory
    SURREAL_URL=ws://127.0.0.1:8000 pnpm --filter providers-example surreal
  (The @sigx/actors-surreal test suite gates on the same variable; CI runs it in a dedicated job.)
```

**`tcp-demo.mjs` — one connection per peer, measured.** Step 4 fires 32
concurrent calls into the owner from one peer, then asks the owner's HTTP
listener how many connections it holds: `0` over TCP, because every call
rode the one connection the peer already had open. Run the same script with
`--http` and the answer is the pool:

```
=== 4. Socket count — 32 concurrent calls into the owner from ONE peer ===
owner s.w2phapms's HTTP listener holds 32 connection(s) after the burst
(HTTP: one pooled connection per concurrent call, kept alive for the next one)
```

The `transports` column of the report — `tcp,http` against `http` — is
`clusterStats()` seeing the same swap from the outside.

**`otel-demo.mjs` — one digest, two exporters.** `prometheusOps()` mounts
`/_sigx/metrics` beside `ops()` with the same bearer posture; the demo
scrapes it unauthenticated (401) and then per host, parses the exposition
(`src/prometheus.ts`) and asserts on the samples rather than merely printing
them. `otelMetricsBridge()` registers observables on a `MeterProvider` and
reads the same digest once per collection — the demo's provider is
in-memory so it can read the collection back and check the two exporters
agree to the number:

```
=== 4. Prometheus — one scrape per host ===
GET /_sigx/metrics without a bearer → 401
s.payolfgm (:5591) → 200 text/plain; version=0.0.4; charset=utf-8: 139 samples; calls_total{type="Counter"}=11 method_calls_total{method="increment"}=11 call_duration_seconds_count=11 activations{type="Counter"}=5
  the exposition, excerpted:
    sigx_actors_calls_total{type="Counter"} 11
    # TYPE sigx_actors_call_duration_seconds histogram
    sigx_actors_call_duration_seconds_bucket{le="0.001024"} 6
    sigx_actors_call_duration_seconds_bucket{le="+Inf"} 11
    sigx_actors_call_duration_seconds_sum 0.023508
    sigx_actors_call_duration_seconds_count 11
  curl -H 'Authorization: Bearer demo-ops-secret' http://127.0.0.1:5591/_sigx/metrics
s.lyde53g5 (:5592) → 200 text/plain; version=0.0.4; charset=utf-8: 139 samples; calls_total{type="Counter"}=4 method_calls_total{method="increment"}=4 call_duration_seconds_count=4 activations{type="Counter"}=3
s.20bszqhj (:5593) → 200 text/plain; version=0.0.4; charset=utf-8: 139 samples; calls_total{type="Counter"}=2 method_calls_total{method="increment"}=2 call_duration_seconds_count=2 activations{type="Counter"}=2
sum over hosts: 17 calls — a cross-host call is metered where it ENTERED and where it RAN, so this is the out+in rule of clusterStats, not double counting

=== 5. OpenTelemetry — one collection per host, same digest ===
s.payolfgm: sigx.actors.calls{type="Counter"}=11  (Prometheus said 11; scope "@sigx/actors-otel", 22 instruments)
s.lyde53g5: sigx.actors.calls{type="Counter"}=4  (Prometheus said 4; scope "@sigx/actors-otel", 22 instruments)
s.20bszqhj: sigx.actors.calls{type="Counter"}=2  (Prometheus said 2; scope "@sigx/actors-otel", 22 instruments)
(distributions stay with Prometheus: the OTel metrics API cannot ingest pre-bucketed histograms)
```

A deployment hands the bridge an OTLP exporter's reader instead of the
in-memory one and changes nothing else. `otelTraces()` is not here — it
needs a trace backend to be worth looking at, and the exporter tests in
`packages/actors-otel` already run it against the SDK's in-memory span
exporter.

### Non-goals

- **Not an app.** The actor is `examples/counter`'s `Counter`, unchanged;
  anything interesting about it would get in the way of what is *around*
  it. The client swap, streams and the page are in `examples/counter`;
  actors in a real sigx app are `examples/chat`.
- **Not a crash drill.** The owner *leaves* (`app.stop()`), because that is
  what makes the step deterministic over a TTL-judged membership. The crash
  variant — `SIGKILL` the owner, wait for the TTL — is
  `examples/counter`'s cluster demo, and the durable-job version is its
  `job-demo.mjs`.
- **Not a measurement.** Step 4 of the tcp demo counts sockets, not
  milliseconds. Throughput over TCP against HTTP is `perf/aks`
  (`TRANSPORT=http|tcp`), and the reason it lives there and not here is in
  `AGENTS.md`.
- **Not Redis or Kubernetes.** `redisCluster` / `redisStorage` are what
  `perf/aks` deploys; `k8sMembership` needs a cluster to be in.

## The lesson worth copying

**Chain the transport, never replace it.** The tcp demo passes
`[tcpTransport(), httpTransport()]`, not `tcpTransport()` alone, and the
report's `fallbacks` column proves nothing fell through to HTTP. A single
transport is *strict*: a peer that publishes no `tcp` address is unreachable
rather than reached over HTTP — and during the rolling deploy that
introduces the transport, that peer is half the cluster. The chain is what
lets the two halves talk while the deploy is in flight, and the counter is
how you know when it is done.

## Things that will bite you

**The pg and surreal scripts exit 0 when they skip.** Deliberately — the same
posture as the packages' own suites, which `describe.skipIf(!PG_URL)`. A CI
step that runs them proves nothing unless the variable is set in that job.

**`ensurePgSchema` / `ensureSurrealSchema` are not optional.** The providers
never issue DDL. Skip the step and Postgres fails on the first `INSERT`;
SurrealDB 3 fails on the first *read*, because an undefined table is an
error there, not an empty result.

**A SurrealDB connection you build yourself has retry OFF.** The SDK's
default predicate never matches, so the loser of a write–write conflict gets
the error instead of a re-run. Either pass `url` and let the package build
the connection, or set `retry: { enabled: true, retryable: surrealRetryable }`
and `reconnect: { attempts: -1 }` as the demo does.

**Three hosts in one process share `PROVIDERS_DEMO_PORTS`.** Default
`5591,5592,5593`, so this runs beside `examples/counter`'s cluster
(5391–5393). Two provider demos at once need two settings; the tests use
port `0` and read the bound port back.

**A persistent store remembers the last run.** Every key carries a per-run
suffix (`cart-mtk87zvc`) so Postgres and SurrealDB start each run fresh
without a `TRUNCATE`; the `hosts` table still shows earlier runs' rows until
their TTL lapses.

**One `MeterProvider` per host.** A meter's instruments are keyed by name, so
three hosts registering `sigx.actors.calls` on one meter would be three
callbacks fighting over one time series. The demo builds a provider inside
the per-host `plugins` factory for that reason.

## Files

| File | |
|---|---|
| `src/cluster.ts` | the shared walk with the three seams as options — the thing to read first |
| `src/counter.actor.ts` | `examples/counter`'s `Counter`, one storage write per call |
| `src/gate.ts` | the env gate and its skip message, pure so the test can pin the text |
| `src/prometheus.ts` | a reader for the Prometheus exposition format, so the otel demo asserts on samples |
| `pg-demo.mjs` | `pgStorage` + `pgCluster`, `ensurePgSchema` first, the tables read back after |
| `surreal-demo.mjs` | `surrealStorage` + `surrealCluster`, a hand-built connection with `surrealRetryable` |
| `tcp-demo.mjs` | `[tcpTransport(), httpTransport()]`, and the socket count that shows why; `--http` to compare |
| `otel-demo.mjs` | `prometheusOps()` scraped and `otelMetricsBridge()` collected, per host, off one digest |
| `__tests__/cluster.test.ts` | the walk on memory seams and ephemeral ports, and the exposition reader against the real exporter |
| `__tests__/gate.test.ts` | the skip message names the variable, the requirement and every how-to line |
| `__tests__/prometheus.test.ts` | label unescaping, `+Inf`, timestamps, and null for a missing sample |
| `tsconfig.json` | `paths` to the packages' *source*, so a clean checkout typechecks before it builds |
| `package.json` | one script per provider; `pg`, `surrealdb` and the two `@opentelemetry/*` packages are the only non-workspace deps |
