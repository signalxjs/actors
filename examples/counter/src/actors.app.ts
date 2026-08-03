/**
 * The app: ONE source of truth for storage, defaults and plugins, shared by
 * the dev server (`sigxActors({ app })`), the production entry, and the
 * actor modules.
 *
 * Note what is NOT here: the actor registry. Under Vite the plugin supplies
 * the one it already builds, so this module imports nothing Vite-specific
 * and loads under plain Node too — which is what lets `server.mjs` share it
 * and why actor modules can safely import the bound `defineActor` below.
 */
import { defineActorApp } from '@sigx/actors/host';
import { fileStorage } from '@sigx/actors/node';

export const app = defineActorApp({
    // Keeps dev state across actor-file edits and restarts.
    storage: fileStorage({ dir: '.actors' })
});

/** Bound to this app's plugin set — actor modules import this. */
export const { defineActor } = app;
