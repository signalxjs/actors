/**
 * `@sigx/actors/silo` — the server-only actor runtime: `createSilo`, the
 * storage/placement seams, and the in-memory dev provider. Heavy by design;
 * the root entry never imports it.
 */
export { createSilo, type CreateSiloOptions, type SiloDefaults } from './silo';
export { memoryStorage } from './storage-memory';
export { Mailbox } from './mailbox';
export { REMINDER_METHOD } from './activation';
export { REMINDER_TYPE } from './reminders';
export type {
    ActorCallContext,
    ActorDispatcher,
    ActorPlacement,
    ActorRef,
    ActorStorage,
    ActorStorageRecord,
    PlacementBindings,
    Silo,
    SiloStats
} from '../types';
