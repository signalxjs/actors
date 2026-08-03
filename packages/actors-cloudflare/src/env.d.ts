/**
 * Compile-time environment flag, mirroring `packages/actors/src/env.d.ts`.
 * The build replaces it: the dev dist keeps a `typeof process` guard, the
 * prod dist pins it `false` so the minifier strips dev-only blocks. Tests
 * define it via vitest.config.ts.
 */
declare const __DEV__: boolean;
