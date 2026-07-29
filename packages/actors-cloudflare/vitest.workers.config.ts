/**
 * The real-`workerd` project, run by `pnpm test:workers`.
 *
 * Separate from the root config rather than a vitest project, for reasons
 * that are not stylistic:
 *
 *  - `wrangler@4` declares `engines.node >= 22`, and the root `test` matrix
 *    includes Node 20 to prove the package's own engines floor. Folding these
 *    in would mean that leg could not run the suite at all.
 *  - The root config is actively wrong here: `environment: 'happy-dom'`,
 *    `globals`, a `jsx.importSource` and `typecheck.enabled` would all have
 *    to be neutralised, while the pool wants to own the module transform.
 *  - Coverage stays measured by one runner, so the reported number does not
 *    move for reasons unrelated to the code.
 *
 * CI runs it as its own job, exactly like `test-redis`, which exists for the
 * same reason: a runtime dependency the main matrix cannot carry.
 *
 * NOTE on the API: `@cloudflare/vitest-pool-workers` 0.19 dropped the
 * `./config` subpath and its `defineWorkersConfig` helper. The pool is now a
 * Vite PLUGIN (`cloudflareTest`) inside an ordinary `defineConfig`, which is
 * why this file looks unlike most published examples.
 */
import { cloudflareTest } from '@cloudflare/vitest-pool-workers';
import { defineConfig } from 'vitest/config';
import { resolve } from 'node:path';

const root = resolve(__dirname, '../..');

export default defineConfig({
    plugins: [
        cloudflareTest({
            wrangler: { configPath: './__tests__/workers/wrangler.jsonc' }
        })
    ],
    define: {
        // The compile-time dev flag package sources guard on, as the root
        // config also defines.
        __DEV__: 'true'
    },
    test: {
        include: ['__tests__/workers/**/*.test.ts']
    },
    resolve: {
        alias: {
            // Same map as the root config, so the suite runs against SOURCE
            // rather than a stale dist. MORE SPECIFIC FIRST — aliases match
            // by prefix in order.
            '@sigx/actors/silo': resolve(root, 'packages/actors/src/silo/index.ts'),
            '@sigx/actors/server': resolve(root, 'packages/actors/src/server/index.ts'),
            '@sigx/actors/client': resolve(root, 'packages/actors/src/client/index.ts'),
            '@sigx/actors/app': resolve(root, 'packages/actors/src/app/index.ts'),
            '@sigx/actors/cluster': resolve(root, 'packages/actors/src/cluster/index.ts'),
            '@sigx/actors': resolve(root, 'packages/actors/src/index.ts'),
            '@sigx/actors-cloudflare': resolve(root, 'packages/actors-cloudflare/src/index.ts')
        }
    }
});
