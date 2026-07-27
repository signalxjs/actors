/**
 * The per-app client state `@sigx/actors/app` owns, and the DI token the
 * hooks resolve it through.
 *
 * Why a token rather than a module global: this state has a LIFETIME. It
 * belongs to one app and must die with `app.unmount()` — a server rendering
 * two requests concurrently has two of them. The module-global
 * `configureActors()` seam is page-global by nature and stays that way for
 * the transport itself; everything with per-app identity lives here.
 */
import { createToken, getProvided, type InjectionToken } from 'sigx/internals';
import { useAppContext } from 'sigx';
import { currentTransport, type ActorTransport } from '../client';

/** What `actorsPlugin()` provides. Grows as later milestones land. */
export interface ActorsContext {
    /**
     * The transport actor calls resolve at call time. Null on the server,
     * where `actor()` dispatches in-process through the silo seam and never
     * touches a transport at all.
     */
    readonly transport: ActorTransport | null;
}

/**
 * Description is namespaced and minted exactly once — a second token with
 * this description means a duplicated module graph, which is what
 * `hasForeignToken` exists to surface.
 */
export const ACTORS_TOKEN: InjectionToken<ActorsContext> = createToken<ActorsContext>(
    'sigx:actors'
);

let warnedMissing = false;

/**
 * The actors context for the current app, or a working default.
 *
 * Deliberately resolve-OR-DEFAULT rather than throwing: `actor()` works with
 * no plugin installed (the build bakes an endpoint into every client ref),
 * so a hook that threw would make the plugin mandatory for a case that
 * functions perfectly well without it. Dev builds warn once, naming what is
 * degraded rather than just noting the absence.
 */
export function useActorsContext(): ActorsContext {
    const provided = getProvided(useAppContext()?.provides, ACTORS_TOKEN);
    if (provided) return provided;
    if (__DEV__ && !warnedMissing) {
        warnedMissing = true;
        console.warn(
            '[sigx actors] no actorsPlugin() installed on this app — falling back to the ' +
                'page-global transport. Calls still work; what you lose is per-app transport ' +
                'isolation (an SSR render can pick up another request\'s config). Add ' +
                '`app.use(actorsPlugin({ transport }))`.'
        );
    }
    return { transport: isLiveClient() ? currentTransport() : null };
}

/**
 * Is this a real browser page, as opposed to a server render? Matches
 * `serverPlugin`'s test exactly — `__SIGX_LIVE_CLIENT__` is core's own
 * declaration for a client bundle running where `document` is absent.
 */
export function isLiveClient(): boolean {
    return (
        typeof document < 'u' ||
        (globalThis as { __SIGX_LIVE_CLIENT__?: boolean }).__SIGX_LIVE_CLIENT__ === true
    );
}
