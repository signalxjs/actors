# Changelog

## [Unreleased]

### Added

- **The per-request locality fraction reaches both exporters (#346).**
  `prometheusOps({ cluster })` and `otelMetricsBridge({ cluster })` take a
  thunk over this host's cluster counters — typically
  `() => plugin.placement.counters()` from the `cluster()` plugin — read on
  every scrape / collection. `renderPrometheus()` takes the totals as
  `options.cluster` and emits `{prefix}cluster_dispatches_total` as two
  counter series, `locality="local"` and `locality="remote"`
  (`dispatchesLocal` / `dispatchesRemote` from #52); `otelMetricsBridge()`
  observes the same pair as `sigx.actors.cluster.dispatches` with a
  `locality` attribute. No precomputed ratio gauge: the fraction is
  `rate(local) / (rate(local) + rate(remote))` in PromQL, which is what
  makes it sum across a fleet. Without the option, or with a thunk answering
  `undefined` (not clustered yet), no cluster family is emitted — a single
  host is not a cluster with zero dispatches. Totals from a build predating
  the pair render `0`, not `NaN`.

- **Socket sessions reach both exporters (#166).** When the app publishes
  `socketStats().digest()` as the `'sockets'` digest
  (`registry.reportDigest('sockets', () => stats.digest())`), `prometheusOps`
  reads it beside `'metrics'` and `renderPrometheus()` emits the
  `{prefix}socket_*` families: one counter per total
  (`socket_connections_opened_total` / `_closed_total` / `_refused_total`,
  `socket_calls_total` / `_failed_total`, `socket_subscriptions_opened_total`
  / `_closed_total`, `socket_protocol_breaches_total`,
  `socket_lifetime_closes_total`, `socket_deliveries_total`,
  `socket_delivery_bytes_total`, `socket_throttle_quantized_total`), two
  derived gauges (`socket_sessions` and `socket_subscriptions` — `opened −
  closed`, which is exactly the live count because the recorder closes only
  what it opened), and the `socket_connection_duration_seconds` histogram on
  the same exact-at-native-bounds grid as the call histograms.
  `otelMetricsBridge()` observes the same set as `sigx.actors.socket.*`
  (plus `socket.connection_duration.p50/p90/p99` under `percentileGauges`).
  `renderPrometheus()` takes the digest as `options.sockets`; without one —
  a host with no socket mount — no socket family is emitted at all, because
  "no sockets here" and "zero connections" are different facts.
  `socket_delivery_bytes_total` is UTF-16 code units, exact for ASCII and an
  under-count otherwise, and its HELP text says so. The digest type is
  exported as `SocketStatsDigest`.

## [0.5.0] - 2026-08-07

### Changed

- **Peers `@sigx/actors@^0.5.0`.** The family versions in lockstep, so the
  range moves with the release. 0.5.0 only ADDS `ctx.changes({ throttleMs })`
  and removes a snapshot a `$live` watch never read (#129) — no wire or API
  break, so a 0.4.x host interoperates fine.

## [0.4.0] - 2026-08-07

### Changed

- **Peers `@sigx/actors@^0.4.0`.** The family versions in lockstep, so the
  range moves with the release. Nothing else to do: 0.4.0 only ADDS
  `onSettled` to `defineJob` (#125), so unlike the 0.2.0 and 0.3.0 moves
  there is no wire or API break and a 0.3.x host interoperates fine.

- README trimmed to a pointer at https://sigx.dev/actors (#113): thesis,
  install, peer-dependency and minimum-version requirements, and links. The
  reference material is on the docs site; relative links (which npm does not
  resolve) are gone. No code or API change.

## [0.3.0] - 2026-08-05

### Added

- **`oneWayFailures` reaches both exporters.** `renderPrometheus()` emits
  the `{prefix}one_way_failures_total` counter and `otelMetricsBridge()`
  observes `sigx.actors.calls.one_way_failures` — a one-way call that fails
  after acceptance has no caller to throw to, and this counter was the only
  place the failure existed while no exporter read it. Legacy digests
  without the field read 0.

### Changed

- **Peers `@sigx/actors@^0.3.0`.** The actor URL grammar is breaking
  (#96), so the whole family moves together — see the `@sigx/actors`
  changelog. **Upgrade every host before any client or peer starts
  emitting**: a host still on 0.2.x refuses calls from an upgraded one.

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
