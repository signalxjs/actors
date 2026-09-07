/**
 * `@sigx/actors-workflow` — durable workflows on `@sigx/actors`.
 *
 * One actor per run. A definition is DATA, versioned and pinned at start,
 * so editing a workflow never changes a run already in flight. A run's
 * state is a durable event log folded through `applyEntry`, so a step is
 * O(entry) rather than O(state) — the shape a workflow punishes hardest,
 * since its variables only grow and it takes a step per node.
 *
 * What this deliberately does NOT do yet: fan-out to child runs, external
 * signals, saga compensation, triggers, tenancy. Each of those is a state
 * that has to survive a host dying mid-node, and each is earning its way
 * in behind a measurement rather than arriving together.
 */
export { defineWorkflow, type WorkflowOptions } from './run';
export { applyRunEntry, emptyRun, type RunEntry, type RunState } from './log';
export type {
    BranchNode,
    DelayNode,
    EndNode,
    Json,
    RetryPolicy,
    RunInfo,
    RunStatus,
    TaskContext,
    TaskHandler,
    TaskNode,
    WorkflowDefinition,
    WorkflowNode
} from './types';
