/**
 * `@sigx/actors/cluster` — multi-host clustering for the actor runtime.
 * WinterCG-clean and dependency-free: `clusterPlacement()` plugs into
 * `createSilo({ placement })`, provider interfaces define the cluster store
 * seam (in-memory here for tests; Redis in `@sigx/actors-redis`), and
 * `handleSiloRequest` is the internal silo-to-silo endpoint mounted beside
 * the public one.
 */
export { cluster, type ClusterPlugin, type ClusterPluginOptions } from './plugin';
export {
    clusterPlacement,
    consistentHashPolicy,
    preferLocalPolicy,
    randomPlacementPolicy,
    type ClusterPlacement,
    type ClusterPlacementOptions
} from './placement';
export {
    handleSiloRequest,
    handleSiloRequestForRuntime,
    matchesSiloRequest,
    createSiloResolver,
    resolveSiloSymbol,
    siloRuntime,
    type SiloEndpointOptions,
    type SiloRequestOptions,
    type SiloRuntimeRequestOptions
} from './silo-endpoint';
export {
    clusterStats,
    SILO_STATS_SYMBOL,
    type ClusterStatsFailure,
    type ClusterStatsFailureReason,
    type ClusterStatsOptions,
    type ClusterStatsReport,
    type SiloReport
} from './stats';
export type { ClusterCounters, ClusterCounterTotals } from './counters';
export { memoryClusterHub, type MemoryClusterHub } from './memory';
export { httpTransport, HTTP_TRANSPORT_NAME, type HttpTransportOptions } from './transport';
export type {
    SiloCallTarget,
    SiloTransport,
    SiloTransportConfig,
    SiloTransportFactory,
    SiloTransportRuntime,
    SiloWireCodec,
    SiloWireError
} from './seam';
export { fromSiloWireError, siloWireCodec, toSiloWireError } from './wire-errors';
export {
    SILO_AUTH_HEADER,
    SILO_CALL_HEADER,
    SILO_PROTO,
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
    SiloDescriptor,
    SiloIdentity,
    SiloStatus
} from './types';
