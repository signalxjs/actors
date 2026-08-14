/**
 * `@sigx/actors/server` — the WinterCG surface. A pure barrel, so that
 * `fetch.ts` can build on the actor endpoint without importing the module
 * that re-exports it (a cycle that works under Node's loader but is exactly
 * the kind of initialization-order trap bundlers surface later).
 *
 * Same layout as `./host` and `./cluster`: implementation in siblings, this
 * file only re-exports.
 */
export {
    createActorResolver,
    handleActorRequest,
    matchesActorRequest,
    type ActorMissPolicy,
    type ActorRequestOptions,
    type ActorResolverOptions
} from './actor-endpoint';
export {
    createFetchHandler,
    requireHost,
    type FetchHandlerOptions
} from './fetch';
export {
    createActorSocketSession,
    type ActorSocketSession,
    type ActorSocketSessionOptions
} from './socket-session';
/**
 * The delivery-window policy (#247). Only the TYPE and the default are
 * public: they are named by `ActorSocketSessionOptions.throttlePolicy`, so a
 * consumer cannot configure one without them. `resolveClientThrottle` and
 * `resolveThrottlePolicy` stay off the barrel for the same reason
 * `resolveMaxSubscriptions` does — they are the mounts' shared internals, not
 * a surface anyone outside should call.
 */
export { DEFAULT_THROTTLE_POLICY, type LiveThrottlePolicy } from './live-endpoint';
export {
    socketStats,
    type SocketSessionRecorder,
    type SocketStats,
    type SocketStatsSnapshot,
    type SocketStatsTotals
} from './socket-stats';
/** The routing token an inbound call carries. READ-ONLY — the endpoint does
 *  not consult it, and deliberately never validates it. */
export {
    ACTOR_ONEWAY_HEADER,
    ACTOR_ROUTE_HEADER,
    ACTOR_ROUTE_SEGMENT,
    actorRouteToken
} from '../route';
