/**
 * The app: ONE typed source of truth, imported by the dev server (through
 * `sigxActors({ app })`), by the production server entry, and by the actor
 * modules themselves.
 *
 * `virtual:sigx-actors` is the build-emitted registry of `*.actor.ts`
 * modules, served in dev as lazy `import()`s through Vite's module runner.
 * Lazy is what keeps this from being a cycle: actor modules import
 * `defineActor` from HERE, and are only loaded on first dispatch.
 */
import { defineActorApp } from '@sigx/actors/silo';
import { fileStorage } from '@sigx/actors/node';
import { actors } from 'virtual:sigx-actors';

export const app = defineActorApp({
    actors,
    // The same storage in dev and prod, declared once — so chat history
    // survives both an actor-file edit and a server restart.
    storage: fileStorage({ dir: '.actors' })
});

/** Bound to this app's plugin set; actor modules import this. */
export const { defineActor } = app;
