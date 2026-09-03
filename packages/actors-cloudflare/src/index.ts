/**
 * `@sigx/actors-cloudflare` — Durable Objects as the actor backend.
 *
 * The model is **one Durable Object per actor**. Cloudflare already
 * guarantees a single instance of a DO globally and serializes its
 * requests, which is exactly the virtual-actor contract — so this package
 * needs no membership, no directory, and no authenticated host-to-host
 * mount. Those exist in `@sigx/actors/cluster` to rebuild a guarantee the
 * platform hands us here for free.
 *
 * What is left is two seams: storage over the DO's own storage, and
 * reminders over its alarm.
 */
export {
    durableObjectStorage,
    type BlockConcurrencyWhile,
    type DurableObjectStorageOptions,
    type DurableStorage
} from './storage';
export {
    durableObjectReminders,
    type DurableAlarms,
    type DurableObjectReminders,
    type DurableObjectRemindersOptions
} from './reminders';
export {
    durableObjectName,
    durableObjectPlacement,
    durableObjects,
    durableObjectsHosted,
    durableObjectStubResolver,
    type DurableObjectPlacementOptions,
    type DurableObjectStubResolver
} from './placement';
export {
    createHostDurableObject,
    type DurableAppOptions,
    type DurableObjectStateLike,
    type DurableWebSocketLike,
    type HostDurableObjectInstance,
    type HostDurableObjectOptions
} from './host';
export {
    createWorkerHandler,
    unhostedStorage,
    type WorkerHandler,
    type WorkerHandlerOptions
} from './worker';
export {
    DEFAULT_SOCKET_PATH,
    objectSocketRoute,
    parseSocketActorPath,
    workerSocket,
    type CloudflareWebSocketLike,
    type ObjectSocketRouteOptions,
    type WorkerSocketOptions
} from './socket';
export type {
    DurableObjectIdLike,
    DurableObjectNamespaceLike,
    DurableObjectStubLike
} from './types';
