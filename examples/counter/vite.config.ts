import { defineConfig } from 'vite';
import { sigxActors } from '@sigx/actors/vite';

export default defineConfig({
    plugins: [
        sigxActors({
            // Keep dev state across actor-file edits and restarts:
            storage: '/src/storage.ts'
        })
    ]
});
