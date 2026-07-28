import { defineConfig } from 'vite';
import { sigxActors } from '@sigx/actors/vite';

export default defineConfig({
    // `fileStorage` writes under `.actors/`, inside the Vite root. Saves are
    // temp-file + rename, and the HMR reader loses that race — so exclude
    // the storage dir from the dev watcher (see the chat example for the
    // long version).
    server: { watch: { ignored: ['**/.actors/**'] } },
    plugins: [
        // Dev runs the real app config — storage, defaults, plugins and all.
        sigxActors({ app: '/src/actors.app.ts' })
    ]
});
