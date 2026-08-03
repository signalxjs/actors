/**
 * `@sigx/actors/node` — the only `node:`-touching entry: the connect-style
 * endpoint bridge (over core's `@sigx/server/node`), process signal wiring,
 * and the file dev-storage provider.
 */
export { fileStorage } from './file-storage';
export {
    createActorHandler,
    attachSignalHandlers,
    type ActorHandlerOptions,
    type DrainableServer,
    type SignalHandlerOptions
} from './handler';
export { createAppHandler, type AppHandlerOptions } from './app-handler';
