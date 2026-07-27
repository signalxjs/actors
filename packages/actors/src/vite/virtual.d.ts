/**
 * `virtual:sigx-actors` — the build-emitted actor registry, for
 * `defineActorApp({ actors })`.
 *
 * Ambient (no top-level import/export, so this is a declaration file rather
 * than an augmentation): `vite.config.ts` importing `@sigx/actors/vite`
 * brings it into the project, which is where the app module needs it.
 *
 * In a production build it is a chunk of lazy `import()`s; in dev the plugin
 * serves the same shape through Vite's module runner, so HMR keeps working
 * and the app-module/actor-module import cycle stays broken.
 */
declare module 'virtual:sigx-actors' {
    export const actors: Record<
        string,
        () => Promise<
            | import('@sigx/actors').AnyActorDefinition
            | Record<string, unknown>
        >
    >;
}
