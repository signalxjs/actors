/**
 * `@sigx/actors/cluster` — multi-host clustering for the actor runtime.
 * WinterCG-clean and dependency-free: `clusterPlacement()` plugs into
 * `createSilo({ placement })`, provider interfaces define the cluster store
 * seam (in-memory here for tests; Redis in `@sigx/actors-redis`), and
 * `handleSiloRequest` is the internal silo-to-silo endpoint mounted beside
 * the public one.
 */
export {
    clusterPlacement,
    type ClusterPlacement,
    type ClusterPlacementOptions
} from './placement';
export {
    handleSiloRequest,
    matchesSiloRequest,
    createSiloResolver,
    type SiloRequestOptions
} from './silo-endpoint';
export { memoryClusterHub, type MemoryClusterHub } from './memory';
export { createSiloTransport, type SiloTransport, type SiloTransportOptions } from './transport';
export {
    SILO_AUTH_HEADER,
    SILO_CALL_HEADER,
    SILO_PROTO,
    decodeEnvelope,
    encodeEnvelope
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
