/**
 * The workflow engine's vocabulary (#297).
 *
 * A definition is DATA, not code: it is stored in a `WorkflowDefinition`
 * actor and read by every run, so a node cannot carry a function. Branch
 * conditions are therefore a `{ var, op, value }` triple over the run's
 * variables, and a task's behaviour is a worker kind plus a duration.
 */

export type NodeId = string;

/** Which `defineWorker` pool a task runs on. `compute` burns CPU for ~ms
 *  (sha256 chain); `io` awaits a timer for ms (holds a pool slot, no CPU). */
export type WorkerKind = 'compute' | 'io';

export interface RetryPolicy {
    /** Total attempts including the first. */
    maxAttempts: number;
    /** Base backoff; attempt N waits N × this. */
    backoffMs: number;
}

export interface TaskSpec {
    worker: WorkerKind;
    ms: number;
    /** Probability in [0, 1] that an attempt throws. Deterministic per
     *  (run, node, attempt), so a retry can succeed. */
    failureRate?: number;
}

export type NodeDef =
    | (TaskSpec & {
          type: 'task';
          retry?: RetryPolicy;
          /** The task node that undoes this one, walked in reverse
           *  completion order when the run compensates. */
          compensate?: NodeId;
          next: NodeId;
      })
    | { type: 'delay'; ms: number; next: NodeId }
    | {
          type: 'branch';
          var: string;
          op: 'gt' | 'lt' | 'eq';
          value: number | string;
          then: NodeId;
          else: NodeId;
      }
    | {
          type: 'parallel';
          /** Each branch is a sequence of TASK node ids, run concurrently
           *  inside one turn and joined there — no durable join needed. */
          branches: NodeId[][];
          next: NodeId;
      }
    | {
          type: 'fanout';
          width: number;
          /** `children`: one child RUN per unit (cross-host, durable join).
           *  `tasks`: `width` concurrent worker calls inside one turn. */
          mode: 'children' | 'tasks';
          child?: { workflow: string; version?: number };
          task?: TaskSpec;
          next: NodeId;
      }
    | {
          type: 'wait';
          signal: string;
          timeoutMs: number;
          onTimeout: NodeId;
          next: NodeId;
      }
    | { type: 'subworkflow'; workflow: string; version?: number; next: NodeId }
    | { type: 'end' };

export interface WorkflowDef {
    name: string;
    version: number;
    start: NodeId;
    nodes: Record<NodeId, NodeDef>;
    /** What a task failure after its retries does to the run. Default
     *  `fail`. `compensate` walks the `compensate` targets of every done
     *  node in reverse order, then ends `compensated`. */
    onFailure?: 'fail' | 'compensate';
}

export type RunStatus =
    | 'pending'
    | 'running'
    | 'sleeping'
    | 'waiting'
    | 'blocked'
    | 'compensating'
    | 'completed'
    | 'failed'
    | 'compensated'
    | 'cancelled';

export const TERMINAL: ReadonlySet<RunStatus> = new Set([
    'completed',
    'failed',
    'compensated',
    'cancelled'
]);

export type WakeKind = 'timer' | 'reminder' | 'timer-fallback';
export type WakeReason = 'delay' | 'retry' | 'signal-timeout' | 'notify-retry';

/** The one pending wake of a run. `token` is what makes a late, duplicate
 *  or re-armed wake harmless: only the token in state is honoured. */
export interface Wake {
    token: number;
    nodeId: NodeId;
    due: number;
    kind: WakeKind;
    reason: WakeReason;
}

export interface StartSpec {
    workflow: string;
    version?: number;
    /** Which template minted the run — the label every latency is
     *  reported under. Free text; the loadgen passes the template name. */
    template: string;
    input?: Record<string, unknown>;
    /** Set by a parent run on a child. */
    parent?: { runId: string; nodeId: NodeId };
    /** The load run this belongs to; the aggregator filters on it. */
    tag?: string;
}

export interface RunSummary {
    runId: string;
    status: RunStatus;
    cursor: NodeId | null;
    seq: number;
    startedAt: number;
    endedAt: number | null;
    wake: Wake | null;
    attempts: number;
    transitions: number;
    error: string | null;
}

export interface RunStats {
    /** Raw per-node durations by node type — small per run (a handful of
     *  nodes, `width` for a fan-out), aggregated by the stats actor. */
    nodeMs: Record<string, number[]>;
    /** Delay-node lag: actual − nominal, one entry per durable/volatile wake. */
    wakeLagMs: number[];
    wakes: { timers: number; reminders: number; fallback: number; lost: number; stale: number };
    signals: { delivered: number; buffered: number; late: number; timedOut: number };
    attempts: number;
    failures: number;
    compensations: number;
    children: number;
    transitions: number;
}

/** What a finished run publishes on the `workflow-events` topic. */
export interface CompletionEvent {
    runId: string;
    workflow: string;
    version: number;
    template: string;
    tag: string | null;
    parentRunId: string | null;
    status: RunStatus;
    startedAt: number;
    endedAt: number;
    error: string | null;
    stats: RunStats;
}
