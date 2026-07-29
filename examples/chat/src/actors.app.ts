/**
 * The app: ONE typed source of truth, imported by the dev server (through
 * `sigxActors({ app })`), by the production server entry, and by the actor
 * modules themselves.
 *
 * `virtual:sigx-actors` is the build-emitted registry of `*.actor.ts`
 * modules, served in dev as lazy `import()`s through Vite's module runner.
 * Lazy is what keeps this from being a cycle: actor modules import
 * `defineActor` from HERE, and are only loaded on first dispatch.
 *
 * Storage is picked by environment: with REDIS_URL the room history lives
 * in Redis — the cluster-safe choice the AKS deployment requires — and
 * without it dev keeps the fileStorage experience (history survives an
 * actor-file edit and a restart). This is a server-only module; the client
 * build stubs it, so the redis import never reaches the browser.
 */
import { defineActorApp } from '@sigx/actors/silo';
import { fileStorage } from '@sigx/actors/node';
import { redisStorage } from '@sigx/actors-redis';
import { actors } from 'virtual:sigx-actors';

const redisUrl = process.env.REDIS_URL;

export const app = defineActorApp({
    actors,
    storage: redisUrl
        ? redisStorage({ url: redisUrl, namespace: process.env.SIGX_NAMESPACE ?? 'chat' })
        : fileStorage({ dir: '.actors' })
});

/** Bound to this app's plugin set; actor modules import this. */
export const { defineActor } = app;
