/**
 * Three plugins, one build — the composition this example exists to prove.
 *
 *  - `sigx()`       owns the client/ssr environments and the builder order
 *  - `sigxServer()` extracts `*.server.ts` serverFns and swaps them for stubs
 *  - `sigxActors()` extracts `*.actor.ts` actors, runs the dev host, and
 *                   mounts the actor endpoint
 *
 * They compose with no coordination beyond `sigxActors` reading
 * `sigx`'s `api.adapter.serverBuild` to decide whether to emit its own
 * registry chunk (it does, since the node adapter builds the server
 * `external`).
 */
import { defineConfig } from 'vite';
import sigxPlugin from '@sigx/vite';
import { sigxServer } from '@sigx/vite/server';
import { sigxActors } from '@sigx/actors/vite';

export default defineConfig({
    // JSX compiles to sigx's runtime, not React's. Vite 8 transforms with
    // oxc, so this is where the import source is declared (tsconfig's
    // `jsxImportSource` only informs the type checker).
    oxc: { jsx: { runtime: 'automatic', importSource: 'sigx' } },
    server: {
        // `fileStorage` writes actor state under `.actors/` — inside the
        // Vite root, so the dev watcher sees every save. Two reasons that
        // has to be excluded: a save is a temp-file + rename, and the HMR
        // path that reads a newly-seen file loses the race with the rename
        // (`ENOENT … .actors/…/general.json.<pid>.tmp` in the overlay); and
        // chat state changing is not a source edit — nothing should reload.
        watch: { ignored: ['**/.actors/**'] }
    },
    plugins: [
        sigxPlugin({ ssr: { entry: 'src/entry-server.tsx' } }),
        sigxServer({ serverApp: '/src/server-app.ts' }),
        // Dev runs the very app module the production server imports.
        sigxActors({ app: '/src/actors.app.ts', serverApp: '/src/server-app.ts' })
    ]
});
