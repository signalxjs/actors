/**
 * The SSR entry. Contract: export `createApp(url, request?, platform?)`
 * returning a FRESH app per request — per-request provides are what make
 * concurrent renders safe.
 *
 * The `@sigx/server` import is LOAD-BEARING, not incidental: importing it
 * stamps `globalThis.__SIGX_SERVERFN_SCOPE__`, which is what lets the
 * document handler open a request scope around the render. Without it every
 * guard that reads `rq.request` — including this app's `requireUser` —
 * throws mid-render. `serverPlugin()` brings it in, so installing the
 * plugin is enough.
 */
import { defineApp, type App } from 'sigx';
import { serverPlugin } from '@sigx/server/plugin';
import { actorsPlugin } from '@sigx/actors/app';
import { Room } from './Room';

/**
 * Re-exported so the production server starts the SAME actor app the dev
 * server does (`sigxActors({ app })` loads that module directly). One
 * config, two runtimes — the thing `examples/counter` still gets wrong.
 */
export { app as actorApp } from './actors.app';

export function createApp(
    // The handlers pass all three; this app is a single route with no
    // platform bindings, so it reads none of them. Named anyway so the
    // signature matches the contract you would extend for routing.
    _url?: string,
    _request?: Request,
    _platform?: unknown
): App<unknown> {
    return defineApp(Room({}))
        .use(serverPlugin())
        // No transport on the server: `actor()` dispatches in-process. The
        // plugin still provides the per-app context the hooks resolve, and
        // deliberately installs nothing page-global here.
        .use(actorsPlugin()) as App<unknown>;
}
