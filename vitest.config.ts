import { defineConfig } from 'vitest/config';
import { resolve } from 'path';

export default defineConfig({
    // `__DEV__` is the compile-time dev flag package sources guard on; the build
    // (vite.config.ts) replaces it in the dists, so tests must define it too.
    define: {
        __DEV__: 'true'
    },
    oxc: {
        jsx: {
            runtime: 'automatic',
            importSource: 'sigx'
        }
    },
    test: {
        environment: 'happy-dom',
        include: ['packages/**/__tests__/**/*.test.{ts,tsx}'],
        exclude: ['**/node_modules/**'],
        globals: true,
        typecheck: {
            // Enforce the *.test-d.ts typing-contract files on every test run.
            enabled: true,
            include: ['packages/**/__tests__/**/*.test-d.ts'],
        },
    },
    resolve: {
        alias: {
            '@sigx/actors/silo': resolve(__dirname, 'packages/actors/src/silo/index.ts'),
            '@sigx/actors/server': resolve(__dirname, 'packages/actors/src/server/index.ts'),
            '@sigx/actors/node': resolve(__dirname, 'packages/actors/src/node/index.ts'),
            '@sigx/actors/client': resolve(__dirname, 'packages/actors/src/client/index.ts'),
            '@sigx/actors/app': resolve(__dirname, 'packages/actors/src/app/index.ts'),
            // MORE SPECIFIC FIRST: vitest matches aliases in order by prefix,
            // so `/cluster` listed first would swallow `/cluster/testing`.
            '@sigx/actors/cluster/frames': resolve(
                __dirname,
                'packages/actors/src/cluster/frames.ts'
            ),
            '@sigx/actors/cluster/testing': resolve(
                __dirname,
                'packages/actors/src/cluster/testing.ts'
            ),
            '@sigx/actors/cluster': resolve(__dirname, 'packages/actors/src/cluster/index.ts'),
            '@sigx/actors/vite': resolve(__dirname, 'packages/actors/src/vite/index.ts'),
            '@sigx/actors': resolve(__dirname, 'packages/actors/src/index.ts'),
            '@sigx/actors-tcp': resolve(__dirname, 'packages/actors-tcp/src/index.ts'),
            '@sigx/actors-redis': resolve(__dirname, 'packages/actors-redis/src/index.ts'),
            '@sigx/actors-cloudflare': resolve(
                __dirname,
                'packages/actors-cloudflare/src/index.ts'
            )
        }
    }
});
