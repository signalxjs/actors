/**
 * `__DEV__`, re-declared for this package's own tsconfig program.
 *
 * This package resolves `@sigx/actors` from SOURCE (see `tsconfig.json`
 * `paths`), so that its own typecheck does not depend on `@sigx/actors`
 * having been built — CI runs `pnpm typecheck` before `pnpm build`. Pulling
 * in the source also pulls in its reliance on this ambient flag, which the
 * root program gets from `packages/actors/src/env.d.ts` and this one, being
 * `rootDir`-scoped to `src/`, cannot reach.
 *
 * Nothing in this package READS `__DEV__` — it is a CLI, and its dev/prod
 * behaviour is decided by flags rather than by the build. The declaration
 * exists purely so the imported source typechecks.
 */
declare const __DEV__: boolean;
