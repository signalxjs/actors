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
import { defineActorApp } from '@sigx/actors/host';
import { fileStorage } from '@sigx/actors/node';
import { redisStorage } from '@sigx/actors-redis';
import { actors } from 'virtual:sigx-actors';

const redisUrl = process.env.REDIS_URL;

/**
 * A soft LRU cap on live activations — off (0) unless the deployment asks
 * for one. It rides the sweeper, so `sweepIntervalMs` decides how quickly a
 * host over the cap comes back under it; a cap with the sweeper disabled is
 * silently inert, which is why both live here rather than only the
 * interesting one.
 *
 * Soft on purpose: busy, queued and kept-alive activations are never shed,
 * so a loaded host may sit over the cap until it quiets. Correctness never
 * depends on the number — a shed room re-activates on its next call with
 * its history intact, and the infra suite asserts exactly that.
 */
const maxActivations = Number(process.env.MAX_ACTIVATIONS ?? 0);
const sweepIntervalMs = Number(process.env.SWEEP_INTERVAL_MS ?? 60_000);

export const app = defineActorApp({
    actors,
    storage: redisUrl
        ? redisStorage({ url: redisUrl, namespace: process.env.SIGX_NAMESPACE ?? 'chat' })
        : fileStorage({ dir: '.actors' }),
    defaults: { maxActivations, sweepIntervalMs }
});

/** Bound to this app's plugin set; actor modules import these. */
export const { defineActor, defineWorker } = app;
