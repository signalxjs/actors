/**
 * The compile-time dev flag the runtime sources guard on.
 *
 * This program resolves `@sigx/actors*` from SOURCE (see `tsconfig.json` —
 * CI typechecks before it builds, so a dist-resolving config would fail on a
 * clean checkout), which pulls those `__DEV__` guards into it. Wrangler's
 * bundler replaces the flag at build time.
 */
declare const __DEV__: boolean;
