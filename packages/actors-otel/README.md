# @sigx/actors-otel

Observability exporters for [`@sigx/actors`](https://sigx.dev/actors):
Prometheus text exposition, OpenTelemetry traces, and an OpenTelemetry metrics
bridge.

- **`prometheusOps()`** — mounts `GET /_sigx/metrics` beside the JSON `ops()`
  endpoint, same bearer posture (secret mandatory outside development). Real
  histograms from the digest's raw log-linear buckets.
  **`renderPrometheus()`** is the pure function behind it.
- **`otelTraces()`** — a CLIENT span per dispatch and a SERVER span per turn,
  joined across hosts by the runtime-propagated `traceparent`.
- **`otelMetricsBridge()`** — observable instruments over `metrics().digest()`.

Labels are actor `type` and `method` only — **keys never appear**, since they
can be personal data.

```sh
pnpm add @sigx/actors-otel @opentelemetry/api   # traces + metrics bridge
pnpm add @sigx/actors-otel                      # Prometheus only — no peer needed
```

`@opentelemetry/api` (≥ 1.9) is an **optional** peer: the Prometheus surface
lives on its own entry, **`@sigx/actors-otel/prometheus`**, which imports no
OpenTelemetry at all. `@sigx/actors` is a peer dependency.

## Documentation

**https://sigx.dev/actors/packages/actors-otel/overview/**

Observability guide: https://sigx.dev/actors/docs/observability/ ·
Metrics: https://sigx.dev/actors/docs/metrics/

Source, examples and architecture notes:
https://github.com/signalxjs/actors

MIT © Andreas Ekdahl
