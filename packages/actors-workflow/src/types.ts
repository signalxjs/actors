/**
 * The workflow vocabulary. A definition is DATA, never code.
 *
 * That is the constraint everything else follows from: a definition is
 * stored, versioned, sent over a wire and read by a run that may start
 * days later on a host that has never seen the process that wrote it. A
 * function in a node could satisfy none of those, so a `branch` is a
 * `{ var, op, value }` triple rather than a predicate, and a `task` names
 * a handler the deployment registered rather than carrying one.
 *
 * The vocabulary is deliberately smaller than the perf rig's (#390): this
 * is the set a run needs to be useful and durable, and every addition to
 * it is a new state that has to survive a host dying mid-node.
 */

/** A JSON value — what a definition, a run's variables and a task's
 *  input and output are made of. Nothing here is ever a function. */
export type Json = null | boolean | number | string | Json[] | { [k: string]: Json };

/** How a failed task is retried. Exponential with full jitter, capped —
 *  the perf rig's linear-no-jitter policy synchronised its retries across
 *  a fan-out, which is the thundering herd the jitter exists to break. */
export interface RetryPolicy {
    /** Total attempts including the first. 1 = no retry. */
    maxAttempts: number;
    /** Base delay; attempt N waits a random point in [0, base × 2^(N-1)]. */
    backoffMs: number;
    /** Ceiling on that window, so a long policy does not sleep for hours. */
    maxBackoffMs?: number;
}

/** Run one registered task handler. */
export interface TaskNode {
    type: 'task';
    /** The handler name, resolved against the registry at run time. */
    handler: string;
    /** Arguments, after `${var}` substitution from the run's variables. */
    input?: Json;
    /** Where the result is stored in the run's variables. */
    assignTo?: string;
    retry?: RetryPolicy;
    next?: string;
}

/** Sleep. Below the host's timer threshold this rides a volatile timer;
 *  at or above it a durable reminder, so the run leaves memory. */
export interface DelayNode {
    type: 'delay';
    ms: number;
    next?: string;
}

/** A two-way branch on one variable. */
export interface BranchNode {
    type: 'branch';
    /** Variable name to read from the run's variables. */
    var: string;
    op: 'eq' | 'ne' | 'lt' | 'lte' | 'gt' | 'gte' | 'exists';
    /** Compared against; ignored by `exists`. */
    value?: Json;
    then: string;
    else?: string;
}

/** Stop. `status` decides whether the run reads as done or failed. */
export interface EndNode {
    type: 'end';
    status?: 'completed' | 'failed';
    /** Recorded on the run when `status` is `failed`. */
    reason?: string;
}

export type WorkflowNode = TaskNode | DelayNode | BranchNode | EndNode;

/**
 * A stored, versioned definition. Immutable per version: a run pins the
 * version it started on, so editing a workflow never changes a run that is
 * already in flight, and a host may cache a version forever.
 */
export interface WorkflowDefinition {
    name: string;
    version: number;
    start: string;
    nodes: Record<string, WorkflowNode>;
}

/** What a run is doing. Only `completed` and `failed` are terminal. */
export type RunStatus = 'running' | 'sleeping' | 'completed' | 'failed';

/** A task handler: ordinary async code, registered by the deployment.
 *
 *  It is called with the node's input and a context identifying the
 *  attempt. Because a host can die between the "attempt starting" record
 *  and the call, a handler MAY run twice for one node — `idempotencyKey`
 *  is stable across attempts of the same node of the same run, and is
 *  what a handler with side effects should deduplicate on. */
export type TaskHandler = (input: Json, ctx: TaskContext) => Promise<Json> | Json;

export interface TaskContext {
    runId: string;
    nodeId: string;
    /** 1 for the first try. */
    attempt: number;
    /** Stable per (run, node) across attempts. */
    idempotencyKey: string;
}

/** What `status()` answers. */
export interface RunInfo {
    id: string;
    definition: string;
    version: number;
    status: RunStatus;
    /** The node the run is on, or the one it finished at. */
    cursor: string;
    vars: Record<string, Json>;
    startedAt: number;
    endedAt?: number;
    error?: string;
    /** Node transitions so far — the run's own progress counter. */
    transitions: number;
}
