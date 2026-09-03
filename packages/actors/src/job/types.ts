/**
 * The job model — the user-facing durable-operation vocabulary. A job is
 * one long-running operation hosted in one actor (key = your run id),
 * built entirely on the `tasks:` primitive: the runtime keeps it alive,
 * cancellable and crash-resumable; this layer standardizes the state
 * machine, progress, checkpoints and the client surface around it.
 */
import type { ActorPolicy } from '../types';
import type { ActorPlacementStrategy, ReminderApi, SaveOptions } from '../types';

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
export interface JobHandle<In, C, Extra extends object, E = never> {
    readonly key: string;
    readonly input: In;
    /** Fires on cancel/shutdown/migrate — observe it in every long await. */
    readonly signal: AbortSignal;
    /** 1 on the first run; crash-resumes count up. */
    readonly attempt: number;
    /** The last checkpoint, when this run is any kind of resume — with
     *  every `append`ed entry since folded in. A detached copy: mutating
     *  it changes nothing, and nothing changes it. */
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
    /**
     * Persist a resume point (state save, etag-CAS). `'immediate'`
     * (default): durable when the promise resolves. `'eventual'` (#320):
     * resolves at once and lets the host's write-behind debounce carry it,
     * so a burst of N steps costs one save instead of N; `pause()`, the
     * terminal transition and deactivation still flush synchronously, so
     * the record that matters is never eventual. The trade is crash
     * distance: a host death between an eventual checkpoint and its flush
     * resumes from the last checkpoint that reached storage, which may be
     * more than one step back — and on a platform where eviction is not
     * deactivation (Cloudflare) there is no final flush to narrow it.
     */
    checkpoint(c: C, options?: SaveOptions): Promise<void>;
    /**
     * Persist ONE step's worth instead of the whole checkpoint (#312):
     * `apply(checkpoint, entry)` folds the entry into the checkpoint, and
     * the entry alone is appended to the state record — O(entry) where
     * `checkpoint()` is O(checkpoint). Durable when the promise resolves,
     * etag-CAS like every write. After a crash, `resumedFrom` is the
     * checkpoint with every appended entry folded in, in order — the same
     * value the body saw — so nothing about resume changes. `checkpoint()`,
     * `pause()` and the terminal transition are full saves and therefore
     * the compaction points: append per step, checkpoint every N. Typed by
     * `apply`'s entry parameter, and uncallable (`never`) without one; on a
     * storage without `appendText` every append is a full save — the
     * recipe still holds, at the old cost. A late append from a winding-
     * down body is dropped like a late `progress()`.
     */
    append(entry: E): Promise<void>;
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

export interface JobOptions<In, Out, C, Extra extends object, E = never> {
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
    /** Server-internal, exactly as on `ActorOptions.internal`: the public
     *  wire never serves its generated surface. */
    internal?: true;
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
    run(job: JobHandle<In, C, Extra, E>, input: In): Promise<Out | JobPaused>;
    /**
     * The reducer behind `job.append(entry)` (#312): fold one entry into
     * the checkpoint. Return a value to REPLACE the checkpoint; return
     * nothing to say it was mutated in place. A checkpoint that is still
     * `undefined` (no `checkpoint()` or `append()` yet) can only be created
     * by returning one. Runs at every `append` on the live checkpoint and,
     * on every activation, replaying the record's appended entries onto the
     * stored checkpoint in order — so it must be a pure function of its two
     * arguments. `E` is inferred from this parameter; without `apply`,
     * `job.append` takes `never`.
     */
    apply?(checkpoint: C | undefined, entry: E): C | void;
    /** Durable-reminder passthrough — HITL timeouts, scheduled nudges.
     *  Reserved runtime names never reach it. */
    onReminder?(control: JobControl<Extra>, name: string): void | Promise<void>;
    /**
     * Every terminal transition, INCLUDING the ones the run body never sees:
     * the runtime's `maxAttempts` give-up (it refuses the restart, so no body
     * turn happens) and a `cancel()` that lands while the job is parked or
     * between attempts. This is the seam for a projection kept OUTSIDE the
     * job — a status row in your own database, a metric, a notification.
     * Without it, a projection maintained only from inside `run()` silently
     * asserts "still running" forever after either of those.
     *
     * Fires for `completed` too, not just the runtime-driven cases: a hook
     * that covered only some terminal transitions would leave the handler
     * needing to know which ones it must ALSO cover from the body, which is
     * the bug this exists to remove. Make the handler idempotent instead.
     *
     * Runs inside the settling turn, AFTER the state save — `info` is the
     * final state, and a throw cannot undo the transition (it is caught,
     * dev-warned and swallowed). Same no-self-dispatch rule as `onReminder`:
     * use the supplied `JobControl`, never a client call back into this
     * actor. Note that awaiting slow I/O here holds the actor's turn.
     */
    onSettled?(control: JobControl<Extra>, info: JobInfo<Extra>): void | Promise<void>;
    idleAfterMs?: number;
    placement?: ActorPlacementStrategy;
}

/** The generated client surface (what `actor(MyJob, runId)` exposes). */
export type JobMethodTable<In, Out, Extra extends object> = {
    /** Idempotent: a non-pending job returns its current info and never
     *  restarts — safe under HTTP retry. Resolves once the run is durably
     *  recorded and launched, NOT when it finishes. Rejects if the launch
     *  fails, and takes the transition back — the job reads `pending`
     *  again, live and on disk, so nothing resumes the run and the same
     *  call can be retried (#316). The revert is best-effort: if its own
     *  save fails too the durable record stays `running` and the run
     *  resumes on a later activation, at-least-once. */
    start(input: In): Promise<JobInfo<Extra>>;
    status(): JobInfo<Extra>;
    /** Marks `cancelled` immediately and aborts the run. Terminal no-op. */
    cancel(): Promise<JobInfo<Extra>>;
    /** Resume a `paused` job, optionally with data for the run body.
     *  Rejects if the launch fails, and takes the transition back — the
     *  job reads `paused` again with the rejected call's data dropped, so
     *  a retry does not find a stale answer waiting (#316). Best-effort as
     *  for `start`: if the revert's save fails too the run resumes on a
     *  later activation, with that data, at-least-once. */
    resume(data?: unknown): Promise<JobInfo<Extra>>;
    /** The result of a `completed` job; throws JobNotDoneError /
     *  JobFailedError / JobCancelledError otherwise. */
    result(): Promise<Out>;
    /** Forget a TERMINAL job: clear state, deactivate. */
    discard(): Promise<void>;
};

export type JobStreamTable<Extra extends object> = {
    /**
     * Live `JobInfo` feed: current value first, then one per change — or,
     * with `throttleMs`, at most one per window (leading edge plus a
     * trailing emit taken fresh, so it is never staler than the window).
     *
     * The knob is per-subscriber and matches `ctx.changes` exactly,
     * validation included. Worth setting when the consumer redraws rather
     * than accumulates: an unthrottled watcher costs one whole-state
     * snapshot per mutating turn, and a job reporting progress per step
     * over state that grows through the run pays that on every step
     * (#231). Never loses the end of the run: a window still owing an
     * emit when the actor deactivates is flushed — a throttled watcher
     * always sees the terminal `JobInfo` before the feed ends.
     */
    watch(opts?: { throttleMs?: number }): AsyncIterable<JobInfo<Extra>>;
};
