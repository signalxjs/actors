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
        setupFiles: ['./vitest.setup.ts'],
        // `perf/` carries two suites, and they are not alike. The INFRA one
        // tests a DEPLOYMENT — proxy behaviour, sealed mounts, chaos — which
        // no package-level test can reach; it skips itself without
        // INFRA_URL, so `pnpm test` and CI are unaffected. The other is
        // testenv.mjs's identity-validation guard, and that one runs
        // ALWAYS: it is what keeps a private estate's names from returning
        // as defaults in a public repo.
        include: [
            'packages/**/__tests__/**/*.test.{ts,tsx}',
            'perf/**/__tests__/**/*.test.ts',
            'examples/**/__tests__/**/*.test.ts',
            // The benchmarks themselves are never run by vitest — they are a
            // measurement, not an assertion. Their REPORTING is a different
            // matter: the A/B report is what a reviewer acts on in CI without
            // seeing the numbers behind it, so it is held to the usual bar.
            'benchmarks/__tests__/**/*.test.ts'
        ],
        // The workers pool has its own config, its own runtime and its own
        // CI job (wrangler needs Node >= 22, this matrix includes 20). These
        // files import `cloudflare:test`, which only resolves there.
        exclude: ['**/node_modules/**', '**/__tests__/workers/**'],
        globals: true,
        typecheck: {
            // Enforce the *.test-d.ts typing-contract files on every test run.
            enabled: true,
            include: ['packages/**/__tests__/**/*.test-d.ts'],
        },
    },
    resolve: {
        alias: {
            '@sigx/actors/host': resolve(__dirname, 'packages/actors/src/host/index.ts'),
            '@sigx/actors/server': resolve(__dirname, 'packages/actors/src/server/index.ts'),
            '@sigx/actors/node': resolve(__dirname, 'packages/actors/src/node/index.ts'),
            '@sigx/actors/client': resolve(__dirname, 'packages/actors/src/client/index.ts'),
            '@sigx/actors/app': resolve(__dirname, 'packages/actors/src/app/index.ts'),
            '@sigx/actors/job': resolve(__dirname, 'packages/actors/src/job/index.ts'),
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
            '@sigx/actors/socket-wire': resolve(__dirname, 'packages/actors/src/socket-wire.ts'),
            '@sigx/actors/testing': resolve(__dirname, 'packages/actors/src/testing/index.ts'),
            '@sigx/actors/vite': resolve(__dirname, 'packages/actors/src/vite/index.ts'),
            '@sigx/actors': resolve(__dirname, 'packages/actors/src/index.ts'),
            '@sigx/actors-ws/client': resolve(__dirname, 'packages/actors-ws/src/client.ts'),
            '@sigx/actors-ws': resolve(__dirname, 'packages/actors-ws/src/index.ts'),
            '@sigx/actors-otel/prometheus': resolve(
                __dirname,
                'packages/actors-otel/src/prometheus.ts'
            ),
            '@sigx/actors-otel': resolve(__dirname, 'packages/actors-otel/src/index.ts'),
            '@sigx/actors-k8s': resolve(__dirname, 'packages/actors-k8s/src/index.ts'),
            '@sigx/actors-tcp': resolve(__dirname, 'packages/actors-tcp/src/index.ts'),
            '@sigx/actors-pg': resolve(__dirname, 'packages/actors-pg/src/index.ts'),
            '@sigx/actors-surreal': resolve(
                __dirname,
                'packages/actors-surreal/src/index.ts'
            ),
            '@sigx/actors-redis': resolve(__dirname, 'packages/actors-redis/src/index.ts'),
            '@sigx/actors-cloudflare': resolve(
                __dirname,
                'packages/actors-cloudflare/src/index.ts'
            ),
            '@sigx/actors-cli/source': resolve(
                __dirname,
                'packages/actors-cli/src/source/index.ts'
            ),
            '@sigx/actors-cli': resolve(__dirname, 'packages/actors-cli/src/plugin.ts')
        }
    }
});
