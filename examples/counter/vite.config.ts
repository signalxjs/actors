import { defineConfig } from 'vite';
import { sigxActors } from '@sigx/actors/vite';

export default defineConfig({
    plugins: [
        // Dev runs the real app config — storage, defaults, plugins and all.
        sigxActors({ app: '/src/actors.app.ts' })
    ]
});
