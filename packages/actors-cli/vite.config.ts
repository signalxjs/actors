import { defineLibConfig } from '@sigx/vite/lib';
import type { ConfigEnv, UserConfig } from 'vite';

// A CLI plugin, so there is no prod-dist pass and no size budget: this is
// loaded by the `sigx` binary in Node, never bundled into a browser.
// `@sigx/actors` is external because it is an optional PEER — the HTTP
// source must work with no actor runtime installed at all.
export default defineLibConfig({
    entry: {
        plugin: 'src/plugin.ts',
        source: 'src/source/index.ts'
    },
    external: [/@sigx\/.*/, /^node:/],
    root: import.meta.url
}) as (env: ConfigEnv) => UserConfig;
