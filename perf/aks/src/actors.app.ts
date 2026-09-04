/**
 * The app module: storage picked by environment. With REDIS_URL set the
 * cluster's state lives in Redis (the only cluster-safe storage). The
 * in-memory fallback exists for embedding the app module without a Redis
 * (e.g. a test importing it directly) — the shipped `server.mjs` entry
 * itself REQUIRES `REDIS_URL` and exits without it.
 *
 * Deliberately imports nothing Vite-specific (there is no client build in
 * this example at all): plain Node loads this via type stripping.
 *
 * Two host defaults are knobs here, both for the workflow workload (#297)
 * and both PART OF INFRA_SHAPE — unset means the runtime's own default, so
 * a deployment that sets neither is byte-identical to the one every
 * earlier baseline was recorded against:
 *
 *   WF_REMINDER_TICK_MS   reminder loop cadence (runtime default 30 s). A
 *                         workflow sleeping on a durable reminder wakes no
 *                         sooner than the next tick, so the delay-node lag
 *                         floor IS this number.
 *   WF_CALL_TIMEOUT_MS    external-call deadline (runtime default 30 s).
 *
 * A third, `REMINDERS`, picks the reminder provider — see below.
 *
 * Two more knobs since #384, the admission caps — 0/unset = unlimited, the
 * pre-#384 contract, so a shape that sets neither is still the recorded one:
 *
 *   WF_MAX_QUEUED         `maxQueuedPerActor`: a call that would push one
 *                         activation's queue past this is refused
 *                         (`overloaded`) instead of queued.
 *   WF_MAX_INFLIGHT_TURNS `maxInflightTurns`: the same for the host as a
 *                         whole — every turn queued or running on it, the
 *                         engine's own timer-driven `advance` turns
 *                         included, so a saturated loop refuses new starts.
 */
import { Redis } from 'ioredis';
import { defineActorApp, memoryStorage } from '@sigx/actors/host';
import { redisReminders, redisStorage } from '@sigx/actors-redis';

const url = process.env.REDIS_URL;
const namespace = process.env.SIGX_NAMESPACE ?? 'sigx';
// ONE client for storage and reminders: a second connection per host
// would be a variable of its own in a run where REMINDERS is the one
// under test. Auto-pipelined, as `redisStorage({ url })` would have been.
const redis = url ? new Redis(url, { enableAutoPipelining: true }) : null;

/**
 * REMINDERS — which `ActorReminders` the host runs (#385), part of
 * INFRA_SHAPE: `sharded` (the runtime's default table over the storage —
 * unset means this, so every earlier baseline is unchanged) or `redis`
 * (`redisReminders()`, the due-time index; needs REDIS_URL). A run under one
 * is a different measurement from a run under the other.
 */
const REMINDERS = process.env.REMINDERS ?? 'sharded';
if (REMINDERS !== 'sharded' && REMINDERS !== 'redis') {
    throw new Error(`[perf-aks] REMINDERS must be sharded or redis, got '${REMINDERS}'`);
}
if (REMINDERS === 'redis' && !url) {
    throw new Error('[perf-aks] REMINDERS=redis needs REDIS_URL');
}

/**
 * A host default knob: absent means "the runtime's default", and 0 is
 * refused — `callTimeoutMs: 0` disables deadlines and `reminderTickMs: 0`
 * is not a cadence, and neither is a shape anyone means to measure.
 */
const knob = (name: string): number | undefined => {
    const raw = process.env[name];
    if (raw === undefined || raw === '') return undefined;
    const value = Number(raw);
    if (!Number.isInteger(value) || value < 1) {
        throw new Error(`[perf-aks] ${name} must be a positive integer, got '${raw}'`);
    }
    return value;
};

const reminderTickMs = knob('WF_REMINDER_TICK_MS');
const callTimeoutMs = knob('WF_CALL_TIMEOUT_MS');
const maxQueuedPerActor = knob('WF_MAX_QUEUED');
const maxInflightTurns = knob('WF_MAX_INFLIGHT_TURNS');

export const app = defineActorApp({
    storage: redis ? redisStorage({ client: redis, namespace }) : memoryStorage(),
    ...(REMINDERS === 'redis' && redis ? { reminders: redisReminders({ client: redis, namespace }) } : {}),
    // Spread conditionally: an `undefined` value would still be a key, and
    // the host treats a present-but-undefined default as the default anyway
    // — but keeping the object empty when nothing is set makes "unchanged
    // shape" visible in one look.
    defaults: {
        ...(reminderTickMs !== undefined ? { reminderTickMs } : {}),
        ...(callTimeoutMs !== undefined ? { callTimeoutMs } : {}),
        ...(maxQueuedPerActor !== undefined ? { maxQueuedPerActor } : {}),
        ...(maxInflightTurns !== undefined ? { maxInflightTurns } : {})
    }
});

/** Bound to this app's plugin set — actor modules import these. */
export const { defineActor, defineWorker } = app;
