/**
 * The app module: storage picked by environment. With REDIS_URL set the
 * cluster's state lives in Redis (the only cluster-safe storage). The
 * in-memory fallback exists for embedding the app module without a Redis
 * (e.g. a test importing it directly) — the shipped `server.mjs` entry
 * itself REQUIRES `REDIS_URL` and exits without it.
 *
 * Deliberately imports nothing Vite-specific (there is no client build in
 * this example at all): plain Node loads this via type stripping.
 */
import { defineActorApp, memoryStorage } from '@sigx/actors/host';
import { redisStorage } from '@sigx/actors-redis';

const url = process.env.REDIS_URL;

export const app = defineActorApp({
    storage: url
        ? redisStorage({ url, namespace: process.env.SIGX_NAMESPACE ?? 'sigx' })
        : memoryStorage()
});

/** Bound to this app's plugin set — actor modules import this. */
export const { defineActor } = app;
