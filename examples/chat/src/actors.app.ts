/**
 * The app: ONE typed source of truth, imported by the dev server (through
 * `sigxActors({ app })`), by the production entry, and by the actor modules
 * themselves.
 *
 * `virtual:sigx-actors` is the build-emitted registry of `*.actor.ts`
 * modules, served in dev as lazy `import()`s. Lazy is what keeps this from
 * being a cycle: actor modules import `defineActor` from HERE, and are only
 * loaded on first dispatch.
 */
import { defineActorApp } from '@sigx/actors/host';
import { fileStorage } from '@sigx/actors/node';
import { actors } from 'virtual:sigx-actors';

export const app = defineActorApp({
    actors,
    // Room history survives an edit and a restart. It is also per-process —
    // see "Things that will bite you" in the README before reaching for a
    // second host.
    storage: fileStorage({ dir: '.actors' })
});

/** Bound to this app's plugin set; actor modules import this. */
export const { defineActor } = app;
