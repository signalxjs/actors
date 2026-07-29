/**
 * `@sigx/actors-cloudflare` — Durable Objects as the actor backend.
 *
 * The model is **one Durable Object per actor**. Cloudflare already
 * guarantees a single instance of a DO globally and serializes its
 * requests, which is exactly the virtual-actor contract — so this package
 * needs no membership, no directory, and no authenticated silo-to-silo
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
    type DurableObjectPlacementOptions
} from './placement';
export {
    createSiloDurableObject,
    type DurableAppOptions,
    type DurableObjectStateLike,
    type SiloDurableObjectInstance,
    type SiloDurableObjectOptions
} from './host';
export {
    createWorkerHandler,
    unhostedStorage,
    type WorkerHandler,
    type WorkerHandlerOptions
} from './worker';
export type {
    DurableObjectIdLike,
    DurableObjectNamespaceLike,
    DurableObjectStubLike
} from './types';
