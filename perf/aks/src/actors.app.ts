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
 */
import { defineActorApp, memoryStorage } from '@sigx/actors/host';
import { redisStorage } from '@sigx/actors-redis';

const url = process.env.REDIS_URL;

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

export const app = defineActorApp({
    storage: url
        ? redisStorage({ url, namespace: process.env.SIGX_NAMESPACE ?? 'sigx' })
        : memoryStorage(),
    // Spread conditionally: an `undefined` value would still be a key, and
    // the host treats a present-but-undefined default as the default anyway
    // — but keeping the object empty when nothing is set makes "unchanged
    // shape" visible in one look.
    defaults: {
        ...(reminderTickMs !== undefined ? { reminderTickMs } : {}),
        ...(callTimeoutMs !== undefined ? { callTimeoutMs } : {})
    }
});

/** Bound to this app's plugin set — actor modules import these. */
export const { defineActor, defineWorker } = app;
