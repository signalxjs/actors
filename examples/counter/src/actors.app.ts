/**
 * The app: ONE typed source of truth, imported by the dev server (via
 * `sigxActors({ app })`), the production entry, and the actor modules.
 *
 * `virtual:sigx-actors` is the build-emitted registry of `*.actor.ts`
 * modules, served in dev as lazy `import()`s through Vite's module runner.
 * Lazy is what keeps this from being a cycle: actor modules import
 * `defineActor` from HERE, and they are only loaded on first dispatch,
 * long after this module has finished evaluating.
 */
import { defineActorApp } from '@sigx/actors/silo';
import { fileStorage } from '@sigx/actors/node';
import { actors } from 'virtual:sigx-actors';

export const app = defineActorApp({
    actors,
    // Keeps dev state across actor-file edits and restarts — the same
    // storage the production entry uses, declared once.
    storage: fileStorage({ dir: '.actors' })
});

/** Bound to this app's plugin set; actor modules import this. */
export const { defineActor } = app;
