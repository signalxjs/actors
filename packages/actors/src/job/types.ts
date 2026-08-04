/**
 * The job model — the user-facing durable-operation vocabulary. A job is
 * one long-running operation hosted in one actor (key = your run id),
 * built entirely on the `tasks:` primitive: the runtime keeps it alive,
 * cancellable and crash-resumable; this layer standardizes the state
 * machine, progress, checkpoints and the client surface around it.
 */
import type { ActorPolicy } from '../types';
import type { ActorPlacementStrategy, ReminderApi } from '../types';

export type JobStatus =
    /** The actor exists (virtually) but `start()` has not been called. */
    | 'pending'
    /** The run's task is live somewhere in the cluster (or resuming). */
    | 'running'
    /** `run()` checkpointed and yielded; waiting for `resume()`. */
    | 'paused'
    | 'completed'
    | 'failed'
    | 'cancelled';

export interface JobProgress {
    done: number;
    total?: number;
    message?: string;
}

/**
 * What `status()` and `watch()` return. Deliberately EXCLUDES the
 * checkpoint and the result — the former is the job's private resume
 * state, the latter is fetched once via `result()`; neither belongs in a
 * feed pushed to every watcher on every change.
 */
export interface JobInfo<Extra extends object = Record<never, never>> {
    key: string;
    status: JobStatus;
    progress: JobProgress | null;
    /** Attempts consumed: 1 on the first run, +1 per crash-resume.
     *  Pause-resume does not count. */
    attempts: number;
    startedAt: number | null;
    finishedAt: number | null;
    error: { message: string } | null;
    /** The definition's `state:` extra fields — your job's public shape
     *  (recent events, counters, whatever `job.update()` maintains). */
    extra: Extra;
}

/** The brand `run()` returns (via `job.pause()`) to park instead of finish. */
export const JOB_PAUSED: unique symbol = Symbol.for('sigx.actors.job.paused');
export type JobPaused = typeof JOB_PAUSED;

/**
 * The handle a `run()` body works through — the ONLY door into the actor
 * from detached code (each method is a serialized turn inside).
 */
export interface JobHandle<In, C, Extra extends object> {
    readonly key: string;
    readonly input: In;
    /** Fires on cancel/shutdown/migrate — observe it in every long await. */
    readonly signal: AbortSignal;
    /** 1 on the first run; crash-resumes count up. */
    readonly attempt: number;
    /** The last checkpoint, when this run is any kind of resume. */
    readonly resumedFrom: C | undefined;
    /** The payload `resume(data)` carried, on a pause-resume. */
    readonly resumeData: unknown;
    /**
     * The principal that ENQUEUED this job (rfc-server-v4 §7), recorded at
     * `start` and persisted with the run.
     *
     * Not `ctx.principal`: a job outlives the request that started it, so
     * a detached run body — and every crash-resume after it, possibly on
     * another host hours later — has no live caller to read. Authorization
     * happened once, at enqueue; this is who it happened for, which is what
     * an audit line or a downstream call should be attributed to.
     *
     * `null` when the starter was anonymous or the app configured no
     * `codec`. Treat it as unattributed, never as a default principal.
     */
    readonly principal: unknown;
    /** Publish progress. Pushed to `watch()`ers via the change feed with no
     *  write of its own — it is persisted only as part of the NEXT
     *  `checkpoint()` (or terminal) save, so after a crash, progress
     *  honestly regresses to the last checkpoint. */
    progress(p: JobProgress): Promise<void>;
    /** Persist a durable resume point (state save, etag-CAS). */
    checkpoint(c: C): Promise<void>;
    /** Mutate the job's `extra` fields (pushed to watchers, not saved). */
    update(fn: (extra: Extra) => void): Promise<void>;
    /**
     * Checkpoint, mark `paused`, and yield: `return job.pause(c)` from
     * `run()`. A paused job holds no task — it idles durably (zero cost
     * beyond its state record) until `resume()`.
     */
    pause(c: C): Promise<JobPaused>;
    /**
     * The actor's durable reminders — arm one before pausing to give a
     * human-in-the-loop wait a timeout; `onReminder(control, name)`
     * receives it and decides (resume with fallback data, or cancel).
     */
    readonly reminders: ReminderApi;
}

/** What `onReminder` receives — internal controls, NOT self-dispatch
 *  (calling your own methods from a turn would deadlock). */
export interface JobControl<Extra extends object = Record<never, never>> {
    info(): JobInfo<Extra>;
    /** Resume a paused job (no-op otherwise). */
    resume(data?: unknown): Promise<void>;
    /** Cancel unless already terminal. */
    cancel(): Promise<void>;
    /** The actor's durable reminders (re-arm, clear, inspect). */
    readonly reminders: ReminderApi;
}

export interface JobOptions<In, Out, C, Extra extends object> {
    /** Stable type id — wire, directory and storage name. */
    type: string;
    /**
     * Policy chain for this job's generated wire surface, decided at
     * ENQUEUE — `start`/`status`/`cancel`/`resume`/`update` — with
     * `op.resource.kind === 'job'` and the run id as `op.resource.key`.
     * The executing job runs under the principal snapshot recorded at
     * start and never re-authorizes: a job outlives the request that
     * enqueued it, so there is no live caller left to decide about.
     */
    authorize?: ActorPolicy | readonly ActorPolicy[];
    /** The explicit word for a job reachable without a principal. */
    allowAnonymous?: true;
    /**
     * Total attempts before a crash-looping job is marked `failed`.
     * Counts the first run and every crash-resume; pause-resume is free.
     * Default 3.
     */
    maxAttempts?: number;
    /** Keep a terminal job's record this long, then clear and deactivate
     *  (a one-shot reminder drives it). Omit to retain forever. */
    retainMs?: number;
    /** Extra public state fields, surfaced as `JobInfo.extra` and
     *  maintained via `job.update()`. */
    state?: (key: string) => Extra;
    /**
     * The operation. Runs detached (a `tasks:` body under the hood):
     * observe `job.signal`, persist resume points with `job.checkpoint()`,
     * and either return the result, throw (→ `failed`), or
     * `return job.pause(c)` (→ `paused`). `In`, `Out` and `C` must survive
     * the state codec.
     */
    run(job: JobHandle<In, C, Extra>, input: In): Promise<Out | JobPaused>;
    /** Durable-reminder passthrough — HITL timeouts, scheduled nudges.
     *  Reserved runtime names never reach it. */
    onReminder?(control: JobControl<Extra>, name: string): void | Promise<void>;
    idleAfterMs?: number;
    placement?: ActorPlacementStrategy;
}

/** The generated client surface (what `actor(MyJob, runId)` exposes). */
export type JobMethodTable<In, Out, Extra extends object> = {
    /** Idempotent: a non-pending job returns its current info and never
     *  restarts — safe under HTTP retry. Resolves once the run is durably
     *  recorded and launched, NOT when it finishes. */
    start(input: In): Promise<JobInfo<Extra>>;
    status(): JobInfo<Extra>;
    /** Marks `cancelled` immediately and aborts the run. Terminal no-op. */
    cancel(): Promise<JobInfo<Extra>>;
    /** Resume a `paused` job, optionally with data for the run body. */
    resume(data?: unknown): Promise<JobInfo<Extra>>;
    /** The result of a `completed` job; throws JobNotDoneError /
     *  JobFailedError / JobCancelledError otherwise. */
    result(): Promise<Out>;
    /** Forget a TERMINAL job: clear state, deactivate. */
    discard(): Promise<void>;
};

export type JobStreamTable<Extra extends object> = {
    /** Live `JobInfo` feed: current value first, then one per change. */
    watch(): AsyncIterable<JobInfo<Extra>>;
};
