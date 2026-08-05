# Changelog

## [Unreleased]

## [0.2.0] - 2026-08-05

### Changed

- **Peers `@sigx/actors@^0.2.0`.** The guard split is breaking, so the
  whole family moves together — see the `@sigx/actors` changelog and core's
  [0.15 migration guide](https://github.com/signalxjs/core/blob/main/docs/migrations/0.15-guard-split.md).
  Actors, workers and jobs defined against this package declare access with
  `authorize` / `methodAuthorize` / `allowAnonymous` now, and the runtime is
  fail-closed: one that declares nothing, in a process with no server app,
  denies with 401.

## [0.1.0] - 2026-08-03

### Added

- Initial release (#245): `prometheusOps()` + `renderPrometheus()` on the
  OTel-free `./prometheus` entry — Prometheus text exposition from the
  metrics digest, real histograms from the raw log-linear buckets, mounted
  with the `ops()` bearer posture; `otelTraces()` — a CLIENT span per
  dispatch and a SERVER span per turn, joined across hosts by the
  runtime-propagated `traceparent`, one-branch cost when no provider is
  registered; `otelMetricsBridge()` — observable instruments over
  `metrics().digest()`, percentile gauges per host, distributions pointed
  at the Prometheus endpoint. `@opentelemetry/api` is an optional peer.
