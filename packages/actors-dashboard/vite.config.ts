import { defineLibConfig } from '@sigx/vite/lib';
import type { ConfigEnv, UserConfig } from 'vite';

// Same two-pass build as packages/actors: dev dist + prod dist with __DEV__
// stripped; `.d.ts` from `tsc --emitDeclarationOnly`.
const base = defineLibConfig({
    entry: {
        index: 'src/index.ts'
    },
    external: [/@sigx\/.*/, /^node:/],
    root: import.meta.url
}) as (env: ConfigEnv) => UserConfig;

export default (env: ConfigEnv): UserConfig => {
    const config = base(env);
    // `defineLibConfig` reasonably assumes `sigx` as the JSX source — most
    // packages in this repo that render anything are apps. This one is a
    // published library that must not depend on the umbrella (it imports
    // `@sigx/runtime-dom/platform` on its first line), so the factory is
    // runtime-core's. The matching half lives in `tsconfig.json`, and every
    // `.tsx` carries the pragma as well so a test runner with its own JSX
    // setting cannot rewrite it.
    config.esbuild = {
        ...(config.esbuild || {}),
        jsx: 'automatic',
        jsxImportSource: '@sigx/runtime-core'
    };
    if (env.mode !== 'prod-dist') {
        config.define = {
            ...config.define,
            __DEV__: "(typeof process !== 'undefined' && process.env.NODE_ENV !== 'production')"
        };
    }
    return config;
};
