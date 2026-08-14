/**
 * `@sigx/actors-monitor` — the renderer-free data layer every actors
 * dashboard is built on.
 *
 * It exists because the awkward parts are not the drawing. Core reports
 * cumulative counters with no windowing, splits a cross-host call across two
 * hosts on purpose, marks an incomplete fan-out rather than failing it, and
 * reports "this host said nothing" and "this host did nothing" as different
 * facts. A second implementation of any of those is a second chance to get it
 * wrong, and the ones here fail SILENTLY when they are wrong — a counter
 * misread across a reset draws a spike, not an error.
 *
 * So there is one implementation, with no renderer in it at all.
 * `@sigx/actors-cli` draws it in a terminal, `@sigx/actors-dashboard` draws
 * it in a browser, and neither is allowed to re-derive any of this.
 *
 * Nothing here imports `node:` anything, and nothing here touches a DOM. The
 * one runtime dependency is `@sigx/reactivity`, for the signal the poll loop
 * publishes its view through.
 */
export {
    hostViewFromReport,
    type ClusterView,
    type HostView,
    type MonitorSnapshot,
    type MonitorSource,
    type SnapshotOptions
} from './types';
export { httpSource, OpsRequestError, type HttpSourceOptions } from './http';
export {
    DashboardState,
    clampInterval,
    DEFAULT_INTERVAL_MS,
    MAX_INTERVAL_MS,
    MIN_INTERVAL_MS,
    type DashboardOptions,
    type DashboardView
} from './state';
export { rateBetween, RateTracker, Series, type Rate, type RateSample } from './rates';
export {
    alertLines,
    coverageNote,
    hostTone,
    polledLabel,
    scopeOf,
    type Alert,
    type AlertTone
} from './alerts';
export {
    shardStates,
    splitShards,
    unclaimedShards,
    type ShardState,
    type ShardStatus
} from './shards';
export { percentileCeiling, percentilePoints, type PercentilePoint } from './histograms';
export * as format from './format';
