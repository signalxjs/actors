import { defineLibConfig } from '@sigx/vite/lib';
import type { ConfigEnv, UserConfig } from 'vite';

// Standard sigx first-party lib build: a dev pass (`vite build`) emits
// dist/<entry>.js, a prod pass (`vite build --mode prod-dist`) emits
// dist/<entry>.prod.js with `__DEV__` pinned to `false` so the minifier strips
// dev-only blocks. `.d.ts` come from the separate `tsc --emitDeclarationOnly`.
const base = defineLibConfig({
    entry: {
        index: 'src/index.ts',
        silo: 'src/silo/index.ts',
        server: 'src/server/index.ts',
        node: 'src/node/index.ts',
        client: 'src/client/index.ts',
        app: 'src/app/index.ts',
        cluster: 'src/cluster/index.ts',
        'cluster-frames': 'src/cluster/frames.ts',
        vite: 'src/vite/index.ts'
    },
    // `sigx` is matched separately: the scoped pattern does not cover the
    // un-scoped umbrella package that `./app` imports.
    external: [/@sigx\/.*/, /^sigx(\/|$)/, /^node:/, 'vite'],
    root: import.meta.url
}) as (env: ConfigEnv) => UserConfig;

export default (env: ConfigEnv): UserConfig => {
    const config = base(env);
    // The dev/import dist must not assume a `process` global (store#36):
    // defineLibConfig emits bare `(process.env.NODE_ENV !== 'production')` for
    // the dev pass; keep the `typeof process` guard so unbundled runtimes
    // without `process` don't throw on a dev-guarded path. The prod-dist pass
    // keeps `__DEV__ = false` (stripped) — leave it untouched.
    if (env.mode !== 'prod-dist') {
        config.define = {
            ...config.define,
            __DEV__: "(typeof process !== 'undefined' && process.env.NODE_ENV !== 'production')"
        };
    }
    return config;
};
