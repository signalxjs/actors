/**
 * `@sigx/actors/host` — the server-only actor runtime: `createHost`, the
 * storage/placement seams, and the in-memory dev provider. Heavy by design;
 * the root entry never imports it.
 */
export { createHost, type CreateHostOptions, type HostDefaults } from './host';
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
    bucketUpperBoundsUs,
    digestSnapshot,
    emptyHistogramDigest,
    mergeHistogramDigests,
    type HistogramDigest,
    type HistogramSnapshot
} from './histogram';
/**
 * The mergeable metrics shape and the folding over it.
 *
 * Exported because a dashboard merging a user-selected SUBSET of hosts
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
export { timingSafeEquals } from '../timing-safe';
export { timerScheduler, manualScheduler, type ManualScheduler } from './scheduler';
export { Turns, type TurnLoad } from './turns';
export { REMINDER_METHOD } from './activation';
export { TOPIC_METHOD } from './topics';
export { REMINDER_TYPE, shardedReminders } from './reminders';
export {
    ROSTER_INDEX_KEY,
    ROSTER_TYPE,
    reminderTaskLiveness,
    rosterTaskLiveness
} from './task-liveness';
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
    ActorTaskLiveness,
    ActorTaskLivenessContext,
    ActorStorage,
    ActorStorageRecord,
    ActorTurnObserver,
    PlacementBindings,
    ActorScheduler,
    Host,
    HostStats
} from '../types';
