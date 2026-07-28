/**
 * `@sigx/actors/silo` — the server-only actor runtime: `createSilo`, the
 * storage/placement seams, and the in-memory dev provider. Heavy by design;
 * the root entry never imports it.
 */
export { createSilo, type CreateSiloOptions, type SiloDefaults } from './silo';
export {
    defineActorApp,
    type ActorApp,
    type ActorAppOptions,
    type ActorPlugin,
    type ActorRoute,
    type DispatchMiddleware,
    type HealthCheck,
    type HealthReport,
    type OpsProviderError,
    type OpsReport,
    type PluginRegistry
} from './app';
export {
    health,
    type HealthOptions,
    type HealthPlugin,
    type HealthStatus
} from './health';
export { ops, type OpsOptions, type OpsPlugin, type OpsSnapshot } from './ops';
export {
    metrics,
    type ActorMetricsSnapshot,
    type MetricsOptions,
    type MetricsPlugin,
    type TypeMetrics
} from './metrics';
export type { HistogramSnapshot } from './histogram';
export { memoryStorage } from './storage-memory';
export { timerScheduler, manualScheduler, type ManualScheduler } from './scheduler';
export { Mailbox } from './mailbox';
export { REMINDER_METHOD } from './activation';
export { REMINDER_TYPE, shardedReminders } from './reminders';
export type {
    ActorCallContext,
    ActorDispatcher,
    ActorPlacement,
    ActorRef,
    ActorReminders,
    ActorRemindersContext,
    ActorStorage,
    ActorStorageRecord,
    ActorTurnObserver,
    PlacementBindings,
    ActorScheduler,
    Silo,
    SiloStats
} from '../types';
