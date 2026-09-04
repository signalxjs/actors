/**
 * Compile-time environment flags. `__DEV__` is replaced by the build
 * (vite.config.ts): the dev dist keeps a `typeof process` runtime guard, the
 * prod dist pins it `false` so the minifier strips dev-only blocks. Tests
 * define it via vitest.config.ts.
 */
declare const __DEV__: boolean;
