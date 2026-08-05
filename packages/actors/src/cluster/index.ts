/**
 * `@sigx/actors/cluster` — multi-host clustering for the actor runtime.
 * WinterCG-clean and dependency-free: `clusterPlacement()` plugs into
 * `createHost({ placement })`, provider interfaces define the cluster store
 * seam (in-memory here for tests; Redis in `@sigx/actors-redis`), and
 * `handleHostRequest` is the internal host-to-host endpoint mounted beside
 * the public one.
 */
export { cluster, type ClusterPlugin, type ClusterPluginOptions } from './plugin';
export {
    clusterPlacement,
    consistentHashPolicy,
    preferLocalPolicy,
    randomPlacementPolicy,
    type ClusterPlacement,
    type ClusterPlacementOptions,
    type RebalanceOptions,
    type RebalanceReport
} from './placement';
export { activationCountPolicy, type ActivationCountPolicyOptions } from './load-policy';
export {
    handleHostRequest,
    handleHostRequestForRuntime,
    matchesHostRequest,
    createHostResolver,
    resolveHostSymbol,
    hostEndpointRuntime,
    hostRuntime,
    type HostEndpointOptions,
    type HostRequestOptions,
    type HostRuntimeRequestOptions
} from './host-endpoint';
export {
    clusterStats,
    HOST_STATS_SYMBOL,
    type ClusterStatsFailure,
    type ClusterStatsFailureReason,
    type ClusterMetricsTotals,
    type ClusterStatsDetail,
    type ClusterStatsOptions,
    type ClusterStatsReport,
    type HostReport,
    type HostReportOptions
} from './stats';
export type { ClusterCounters, ClusterCounterTotals } from './counters';
export { memoryClusterHub, type MemoryClusterHub } from './memory';
export {
    refreshCoalescer,
    type RefreshCoalescer,
    type RefreshCoalescerOptions
} from './coalesce';
export {
    heartbeatClock,
    type HeartbeatClock,
    type HeartbeatClockOptions
} from './heartbeat-clock';
export { httpTransport, HTTP_TRANSPORT_NAME, type HttpTransportOptions } from './transport';
export type {
    HostCallMode,
    HostCallTarget,
    HostEndpointRuntime,
    HostTransport,
    HostTransportConfig,
    HostTransportFactory,
    HostTransportRuntime,
    HostWireCodec,
    HostWireError
} from './seam';
export { parseWatchOptions, watchSymbol, WATCH_SYMBOL_PREFIX } from './watch-symbol';
export { fromHostWireError, hostWireCodec, toHostWireError } from './wire-errors';
export {
    HOST_AUTH_HEADER,
    HOST_CALL_HEADER,
    HOST_PROTO,
    decodeEnvelope,
    encodeEnvelope,
    signAuth,
    verifyAuth
} from './envelope';
export type {
    ActorDirectory,
    ClusterMembership,
    ClusterProviders,
    DirectoryEntry,
    MembershipView,
    PlacementPolicy,
    PolicyRuntime,
    HostDescriptor,
    HostIdentity,
    HostStatus
} from './types';
