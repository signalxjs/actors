/**
 * `@sigx/actors/app` — the sigx app integration.
 *
 * The ONLY actors entry that imports `sigx`, which is why it is a separate
 * export: the isomorphic root, `./client`, `./host`, `./server`, `./node`
 * and `./cluster` stay free of the framework, so a headless Worker
 * deployment never drags the runtime in.
 *
 * Client-side POLICY lives here rather than in `./client` — that entry's
 * byte budget rides every bundle that touches an actor, whether or not the
 * app uses any of this.
 */
export { actorsPlugin, type ActorsPluginOptions } from './plugin';
export {
    useActorState,
    type ActorCall,
    type UseActorStateOptions
} from './use-actor-state';
export { useActorAction, type UseActorActionOptions } from './use-actor-action';
export { ACTORS_TOKEN, useActorsContext, type ActorsContext } from './context';
export {
    createLiveChannel,
    type LiveChannel,
    type LiveChannelOptions,
    type LiveChannelStats
} from './live';
export { keyMatches } from './key-match';
export type { ActorCellRegistry, InvalidationPattern } from './cell-registry';
export type {
    ActorCallInit,
    ActorLiveChannel,
    ActorSubscription,
    ActorTransport,
    ActorTransportConfig
} from '../client';
export { fetchTransport, configureActors } from '../client';
