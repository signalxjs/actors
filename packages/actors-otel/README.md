# `@sigx/actors-otel`

**Observability exporters for [`@sigx/actors`](../actors): Prometheus text exposition, OpenTelemetry traces, and an OpenTelemetry metrics bridge.**

The runtime's own surfaces (`metrics()`, `ops()`, `clusterStats()`) are pull-based JSON in the runtime's dialect. Every real deployment already runs a Prometheus scraper and/or an OTLP collector — this package is the adapter each team was otherwise writing by hand, kept out of core so core keeps zero runtime dependencies.

```sh
pnpm add @sigx/actors-otel @opentelemetry/api   # traces + metrics bridge
pnpm add @sigx/actors-otel                      # Prometheus only — no peer needed
```

`@opentelemetry/api` is an **optional** peer: the Prometheus surface lives on its own entry, `@sigx/actors-otel/prometheus`, which imports no OpenTelemetry at all.

## Prometheus: `prometheusOps()`

```ts
import { defineActorApp, metrics } from '@sigx/actors/host';
import { prometheusOps } from '@sigx/actors-otel/prometheus';

const app = defineActorApp({ actors: [Room] })
    .use(metrics())
    .use(prometheusOps({ secret: process.env.OPS_SECRET! }));
```

Mounts `GET /_sigx/metrics` beside the JSON `ops()` endpoint with the same bearer posture: the secret is **mandatory outside development** (construction throws), auth runs before any path logic, and one 401 covers missing and wrong tokens alike. Scrape with `authorization: Bearer <secret>`.

The renderer maps `metrics().digest()` onto the exposition format:

- Counters by actor `type` and `type`+`method` (both bounded by the digest and folded into `'(other)'` — actor **keys never appear**; they can be personal data).
- The four runtime distributions (`call`, `turn_queue`, `turn`, `storage_operation` durations) as real Prometheus histograms, in seconds, from the digest's raw log-linear buckets. The default grid is one bound per power of two, 1µs → ~134s (~28 bounds); cumulative counts at native bounds are **exact**, so downsampling loses resolution, never correctness.
- Gauges for live activations per type, queued messages, and `sigx_actors_metrics_window_seconds`.

`renderPrometheus(digest, stats, options)` is exported separately — a pure function, if you want the text without the route (e.g. a merged cluster-wide view fed by `clusterStats()`).

### Options

| option | default | |
|---|---|---|
| `path` | `'/_sigx/metrics'` | Route path. Must start with `/`. |
| `secret` | — | Bearer token. Mandatory outside development. |
| `prefix` | `'sigx_actors_'` | Metric-name prefix. |
| `bucketsSeconds` | per-octave grid | Requested histogram bounds (seconds); each snaps DOWN to the nearest native bound so counts stay exact. |

### The `reset()` caveat

`metrics().reset()` zeroes the counters. Prometheus `rate()`/`increase()` recover from a counter reset, but the increments between the reset and the next scrape are lost — don't combine `reset()`-based polling with scraping. A drop in `sigx_actors_metrics_window_seconds` is how you notice a reset happened.

## Traces: `otelTraces()`

```ts
import { otelTraces } from '@sigx/actors-otel';

const app = defineActorApp({ actors: [Room] }).use(otelTraces());
```

Two spans per call, because the runtime has two sides that never share a process by guarantee:

- **CLIENT span per dispatch** (a `useDispatch` middleware), on the host where the call is *made*. Attributes: `sigx.actor.type`, `sigx.actor.method`, `sigx.actor.key_hash`, `sigx.call.id`, `sigx.call.depth`. Errors are recorded and re-thrown.
- **SERVER span per turn** (the turn observer), on the host where the actor *lives* — with the split a middleware cannot see: `sigx.turn.queued_ms` (waited for its turn) as an attribute and the turn itself as the span's duration.

They join into one trace through `ActorCallContext.traceparent`, which the runtime itself propagates: captured from the W3C `traceparent` header at the public endpoint, inherited by `ctx.actor`/`ctx.publish` hops, and carried host-to-host on the cluster envelope — so a hop through an *uninstrumented* host still relays the context, and a browser- or gateway-started trace flows straight through.

The span attribute is `hashRouteToken(type, key)`, never the raw key — the same hash the `route` option mints, so spans join to routing tokens in access logs. That is log hygiene, not privacy: an unkeyed hash of an email is one dictionary lookup from plaintext.

**Cost when off.** With no tracer provider registered anywhere, the middleware is one branch per dispatch (plus one probe span every few seconds, so a provider registered late is still found), and the runtime's per-turn timestamps stay off — the turn observer is only subscribed once a provider is detected.

### Options

| option | default | |
|---|---|---|
| `tracerProvider` | the global registration | Where spans go. The global default is detected lazily. |
| `turnSpans` | `true` | The callee-side SERVER spans. Off also removes the per-turn timestamp cost. |

Deferred, deliberately: `tracestate`, span links for timers/reminders/tasks, stream/watch spans (long-lived — "one span per dispatch" is the wrong shape), baggage.

## Metrics bridge: `otelMetricsBridge()`

```ts
import { otelMetricsBridge } from '@sigx/actors-otel';

const app = defineActorApp({ actors: [Room] })
    .use(metrics())
    .use(otelMetricsBridge());
```

Observable instruments over `metrics().digest()` — no timer of its own; your `MeterProvider`'s reader owns the period, and one batch callback reads the digest once per collection so every instrument observes the same snapshot. Counters mirror the Prometheus set (`sigx.actors.calls` by type, errors by kind, activations, storage); gauges cover live activations and queued messages.

**Distributions**: the OTel metrics API cannot ingest pre-bucketed histograms, so the digest's percentiles export as gauges (`sigx.actors.call_duration.p50/.p90/.p99`, seconds) — plainly **non-aggregatable across hosts**. For real distribution export, scrape the Prometheus endpoint, where the raw buckets survive.

Register the `MeterProvider` (globally or via the option) **before `app.start()`** — unlike traces, the OTel api has no upgrading proxy for meters.

### Options

| option | default | |
|---|---|---|
| `meterProvider` | the global registration | Where metrics go. |
| `percentileGauges` | `true` | The per-host p50/p90/p99 gauges. |

## Notes

- The cluster envelope's `tp` field is additive: hosts that predate it ignore it, and a malformed traceparent costs the trace, never the call.
- Everything here follows the cardinality rule the runtime's own surfaces follow: dimensions are actor `type` and `method`, bounded and folded — never keys.
