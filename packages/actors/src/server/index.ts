/**
 * `@sigx/actors/server` — the WinterCG surface. A pure barrel, so that
 * `fetch.ts` can build on the actor endpoint without importing the module
 * that re-exports it (a cycle that works under Node's loader but is exactly
 * the kind of initialization-order trap bundlers surface later).
 *
 * Same layout as `./silo` and `./cluster`: implementation in siblings, this
 * file only re-exports.
 */
export {
    createActorResolver,
    handleActorRequest,
    matchesActorRequest,
    type ActorRequestOptions
} from './actor-endpoint';
export {
    createFetchHandler,
    requireSilo,
    type FetchHandlerOptions
} from './fetch';
