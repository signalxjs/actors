# Changelog

## [Unreleased]

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
