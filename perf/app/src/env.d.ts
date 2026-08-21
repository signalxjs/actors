/// <reference types="vite/client" />

// `virtual:sigx-actors` and `__DEV__` come from the runtime's own source
// (`virtual.d.ts` / `env.d.ts`), included by tsconfig.json — the
// `@sigx/actors/vite-client` types export points into `dist/`, which a
// clean checkout does not have when the root `typecheck` runs.
