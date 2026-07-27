/**
 * The app for the DEV server (`sigxActors({ app: '/src/actors.app.ts' })`):
 * storage, defaults and every plugin in one place, so dev runs the real
 * configuration rather than a stripped-down one.
 *
 * `virtual:sigx-actors` is the build-emitted registry of `*.actor.ts`
 * modules, served in dev as lazy `import()`s through Vite's module runner.
 * It only resolves UNDER VITE, which is why `server.mjs` and
 * `cluster-demo.mjs` — plain Node, via type stripping — build their own app
 * with an explicit actor array instead.
 *
 * For the same reason this module does not re-export a bound `defineActor`:
 * actor modules must load in both worlds, so they import `defineActor` from
 * `@sigx/actors` directly. With no plugins configured here the two are
 * identical anyway — the binding only earns its keep once a plugin adds
 * members to `ctx`.
 */
import { defineActorApp } from '@sigx/actors/silo';
import { fileStorage } from '@sigx/actors/node';
import { actors } from 'virtual:sigx-actors';

export const app = defineActorApp({
    actors,
    // The same storage the production entry uses.
    storage: fileStorage({ dir: '.actors' })
});
