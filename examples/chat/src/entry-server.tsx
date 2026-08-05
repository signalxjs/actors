/**
 * The SSR entry. Contract: export `createApp(url, request?, platform?)`
 * returning a FRESH app per request — per-request provides are what make
 * concurrent renders safe.
 *
 * The `@sigx/server` import is LOAD-BEARING, not incidental. Importing it
 * stamps `globalThis.__SIGX_SERVERFN_SCOPE__`, which is what lets the
 * document handler open a request scope around the render. Without it,
 * anything reading `rq.request` mid-render — including this app's
 * `authenticate` — throws on a detached context. `serverPlugin()` brings it
 * in, so installing the plugin is enough.
 */
import { defineApp, type App } from 'sigx';
import { serverPlugin } from '@sigx/server/plugin';
import { actorsPlugin } from '@sigx/actors/app';
import { Room } from './Room';
import { roomFromPath } from './room-path';

/**
 * Re-exported so the production server starts the SAME actor app the dev
 * server does (`sigxActors({ app })` loads that module directly). One
 * config, two runtimes.
 */
export { app as actorApp } from './actors.app';

export function createApp(
    // `/r/<name>` picks the room; anything else renders #general. The
    // request/platform arguments stay unread — this is a single-route app.
    url?: string,
    _request?: Request,
    _platform?: unknown
): App<unknown> {
    const room = roomFromPath(url ? new URL(url, 'http://localhost').pathname : '/');
    return defineApp(Room({ room }))
        .use(serverPlugin())
        // No transport on the server: `actor()` dispatches in-process. The
        // plugin still provides the per-app context the hooks resolve.
        .use(actorsPlugin()) as App<unknown>;
}
