/** The workflow engine (#297) — the actors `server.mjs` registers. */
export { WorkflowRun, workflowEvents } from './run.actor.ts';
export { WorkflowDefinition } from './definition.actor.ts';
export { WorkflowStats } from './stats.actor.ts';
export { ComputeWorker, IoWorker, WORKER_KEY } from './workers.ts';
export { workflowCounters, snapshotCounters, resetCounters } from './counters.ts';
export { config as workflowConfig } from './config.ts';
export * from './templates.ts';
export * from './types.ts';

import { WorkflowRun } from './run.actor.ts';
import { WorkflowDefinition } from './definition.actor.ts';
import { WorkflowStats } from './stats.actor.ts';
import { ComputeWorker, IoWorker } from './workers.ts';

/** Every actor the engine needs, in one list. */
export const workflowActors = [
    WorkflowRun,
    WorkflowDefinition,
    WorkflowStats,
    ComputeWorker,
    IoWorker
] as const;
