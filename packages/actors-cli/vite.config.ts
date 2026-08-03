import { defineLibConfig } from '@sigx/vite/lib';
import type { ConfigEnv, UserConfig } from 'vite';

// A CLI plugin, so there is no prod-dist pass and no size budget: this is
// loaded by the `sigx` binary in Node, never bundled into a browser.
// `@sigx/actors` is external because it is an optional PEER — the HTTP
// source must work with no actor runtime installed at all.
const base = defineLibConfig({
    entry: {
        plugin: 'src/plugin.ts',
        source: 'src/source/index.ts'
    },
    external: [/@sigx\/.*/, /^node:/],
    root: import.meta.url
}) as (env: ConfigEnv) => UserConfig;

export default (env: ConfigEnv): UserConfig => {
    const config = base(env);
    // The JSX here is the TERMINAL's, not the DOM's. `defineLibConfig`
    // reasonably assumes `sigx` — every other package in this repo renders
    // to a browser — so the import source is overridden rather than
    // inherited. The matching half of this lives in `tsconfig.json`.
    config.esbuild = {
        ...(config.esbuild || {}),
        jsx: 'automatic',
        jsxImportSource: '@sigx/terminal'
    };
    return config;
};
