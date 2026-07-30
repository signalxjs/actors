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
    type MetricsDigestOptions,
    type MetricsErrorKind,
    type MetricsOptions,
    type MetricsPlugin,
    type RecentActorError,
    type TypeMetrics
} from './metrics';
export {
    HISTOGRAM_LAYOUT,
    digestSnapshot,
    emptyHistogramDigest,
    mergeHistogramDigests,
    type HistogramDigest,
    type HistogramSnapshot
} from './histogram';
/**
 * The mergeable metrics shape and the folding over it.
 *
 * Exported because a dashboard merging a user-selected SUBSET of silos
 * needs the same arithmetic `clusterStats()` uses. Without these it would
 * have to reimplement the log-linear bucket layout, which is exactly the
 * mistake `HISTOGRAM_LAYOUT` exists to catch.
 */
export {
    createMetricsAccumulator,
    foldCallDigests,
    type CallDigest,
    type MergedMetrics,
    type MetricsAccumulator,
    type MetricsDigest,
    type MetricsFoldResult
} from './digest';
export { memoryStorage } from './storage-memory';
export { timerScheduler, manualScheduler, type ManualScheduler } from './scheduler';
export { Mailbox } from './mailbox';
export { REMINDER_METHOD } from './activation';
export { REMINDER_TYPE, shardedReminders } from './reminders';
export {
    TASKS_TYPE,
    TASK_REMINDER,
    TASK_REMINDER_MS,
    type TaskLedger,
    type TaskLedgerEntry
} from './tasks';
export type {
    ActivationInfo,
    ActivationsOptions,
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
