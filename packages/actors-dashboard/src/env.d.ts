/**
 * Compile-time environment flags, and the DOM JSX intrinsics.
 *
 * `__DEV__` is replaced by the build (vite.config.ts): the dev dist keeps a
 * `typeof process` runtime guard, the prod dist pins it `false` so the
 * minifier strips dev-only blocks. Tests define it via vitest.config.ts.
 *
 * The reference below is what makes `<div>` typecheck. `@sigx/runtime-core`
 * supplies the JSX FACTORY and declares no intrinsics — it is
 * platform-neutral by design — while `@sigx/runtime-dom` augments the GLOBAL
 * `JSX` namespace with the DOM ones. It is a types-only reference on purpose:
 * a value import would pull the DOM renderer into this package's runtime
 * graph, and the platform belongs to the app that mounts us.
 */
/// <reference types="@sigx/runtime-dom" />

declare const __DEV__: boolean;
