/**
 * The engine's host-side knobs (#297). Read ONCE at module load, so a test
 * that wants a different shape sets `process.env` before importing the
 * engine — the same rule `actors.app.ts` lives under. Every one of these
 * is part of INFRA_SHAPE (`WORKFLOW_KNOBS` in `testenv.mjs`): a run under
 * a different threshold is a different measurement.
 *
 *   WF_TIMER_THRESHOLD_MS   a delay shorter than this sleeps on a VOLATILE
 *                           `ctx.timer` (dies with the activation, re-armed
 *                           on the next activation); at or above it, on a
 *                           DURABLE reminder (survives host loss, costs a
 *                           shard CAS to arm and one to fire). Default 30 s.
 *   WF_IDLE_AFTER_MS        idle collection age for runs. Default 60 s —
 *                           far below the host's 20 min, so a sleeping run
 *                           that did not deactivate itself still leaves
 *                           memory before its reminder fires.
 *   WF_DEACTIVATE_ON_SLEEP  1 (default): a run sleeping on a reminder
 *                           deactivates itself at once — the shape a
 *                           million-sleeping-runs engine needs. 0 keeps it
 *                           resident, which is the memory-pressure arm.
 *   WF_STALE_WAKE_MS        how overdue a durable wake must be before a
 *                           touch (`status()`, activation) treats it as
 *                           LOST and advances anyway. Reminder firing is
 *                           at-most-once (the entry is deleted before the
 *                           dispatch), so this safety net is not optional.
 *                           Default 2 × the reminder tick + 5 s.
 *   WF_CHILD_STALE_MS       a fan-out child still `running` after this long
 *                           gets its idempotent `start()` re-issued and a
 *                           `status()` nudge by the parent's join watchdog.
 *                           Default 120 s.
 *   WF_STATS_SAVE_EVERY     the aggregator saves every N events (default 25;
 *                           1 is the "visibility store on the completion
 *                           path" arm — at 200 000 ring entries a save per
 *                           event was several MB of JSON per completion,
 *                           and part of how #302 built up).
 *   WF_STATS_RING           events the aggregator retains for `drain`
 *                           (default 50 000 — ~1000 s of history at 50/s).
 *   WF_NOTIFY_RETRY_MS      how long a finished child waits for its detached
 *                           `childDone` to land before sending it again.
 *                           Default 15 s.
 *   WF_COMPUTE_MAX_LOCAL    pool size of the compute worker; unset = the
 *   WF_IO_MAX_LOCAL         runtime default (cores, clamped 4..16).
 */

const num = (name: string, fallback: number): number => {
    const raw = process.env[name];
    if (raw === undefined || raw === '') return fallback;
    const value = Number(raw);
    if (!Number.isFinite(value) || value < 0) {
        throw new Error(`[workflow] ${name} must be a non-negative number, got '${raw}'`);
    }
    return value;
};

const optional = (name: string): number | undefined => {
    const raw = process.env[name];
    return raw === undefined || raw === '' ? undefined : num(name, 0);
};

const reminderTickMs = num('WF_REMINDER_TICK_MS', 30_000);

export const config = {
    timerThresholdMs: num('WF_TIMER_THRESHOLD_MS', 30_000),
    idleAfterMs: num('WF_IDLE_AFTER_MS', 60_000),
    deactivateOnSleep: process.env.WF_DEACTIVATE_ON_SLEEP !== '0',
    staleWakeMs: num('WF_STALE_WAKE_MS', 2 * reminderTickMs + 5_000),
    childStaleMs: num('WF_CHILD_STALE_MS', 120_000),
    statsSaveEvery: Math.max(1, num('WF_STATS_SAVE_EVERY', 25)),
    statsRing: num('WF_STATS_RING', 50_000),
    notifyRetryMs: num('WF_NOTIFY_RETRY_MS', 15_000),
    computeMaxLocal: optional('WF_COMPUTE_MAX_LOCAL'),
    ioMaxLocal: optional('WF_IO_MAX_LOCAL'),
    /** Reminder mutations throw after 3 CAS conflicts; the engine's own
     *  retry on top of that, with jitter, before it gives up and falls back
     *  to a volatile timer. */
    reminderSetAttempts: 3,
    /** The runtime's floor for a periodic reminder — the join watchdog runs
     *  at exactly this cadence. */
    joinCheckPeriodMs: 60_000
} as const;

/** Reminder names the run actor uses. */
export const REMINDER_WAKE = 'wake';
export const REMINDER_JOIN_CHECK = 'join-check';
