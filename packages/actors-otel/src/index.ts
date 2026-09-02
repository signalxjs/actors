/**
 * `@sigx/actors-otel` — the OpenTelemetry half: trace middleware and the
 * metrics bridge. Requires the `@opentelemetry/api` peer.
 *
 * The Prometheus exposition lives on its own entry,
 * `@sigx/actors-otel/prometheus`, which imports no OpenTelemetry at all —
 * install without the peer if that is all you need.
 */
export { otelTraces, type OtelTracesOptions } from './traces';
export { otelMetricsBridge, type OtelMetricsBridgeOptions } from './metrics-bridge';
export type { SocketStatsDigest } from './prometheus';
export { formatTraceparent, parseTraceparent, type TraceparentFields } from './traceparent';
