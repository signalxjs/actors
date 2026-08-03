import { defineLibConfig } from '@sigx/vite/lib';
import type { ConfigEnv, UserConfig } from 'vite';

// Same two-pass build as packages/actors: dev dist + prod dist with __DEV__
// stripped; `.d.ts` from `tsc --emitDeclarationOnly`. Two entries so the
// Prometheus surface ships without a single OpenTelemetry import.
const base = defineLibConfig({
    entry: {
        index: 'src/index.ts',
        prometheus: 'src/prometheus.ts'
    },
    external: [/@sigx\/.*/, /^node:/, '@opentelemetry/api'],
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
