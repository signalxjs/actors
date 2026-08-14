import { defineLibConfig } from '@sigx/vite/lib';
import type { ConfigEnv, UserConfig } from 'vite';

// Same two-pass build as packages/actors: dev dist + prod dist with __DEV__
// stripped; `.d.ts` from `tsc --emitDeclarationOnly`.
//
// `node:` is in `external` for symmetry with the rest of the workspace, NOT
// because anything here imports it — this package is browser-safe by
// construction, and a `node:` specifier appearing in the dist is a bug the
// dashboard would inherit.
const base = defineLibConfig({
    entry: {
        index: 'src/index.ts',
        // Its own entry, not just a namespace on the barrel. `count`, `rate`,
        // `gauge` and `percent` are names a consumer plausibly has of their
        // own, so they are never put on the main export — but reaching them
        // through `format.count(…)` at every call site in a dashboard is
        // noise, and a subpath gives both.
        format: 'src/format.ts'
    },
    external: [/@sigx\/.*/, /^node:/],
    root: import.meta.url
}) as (env: ConfigEnv) => UserConfig;

export default (env: ConfigEnv): UserConfig => {
    const config = base(env);
    if (env.mode !== 'prod-dist') {
        config.define = {
            ...config.define,
            __DEV__: "(typeof process !== 'undefined' && process.env.NODE_ENV !== 'production')"
        };
    }
    return config;
};
