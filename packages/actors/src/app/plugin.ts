/**
 * `actorsPlugin()` — the sigx app plugin, mirroring `@sigx/server`'s
 * `serverPlugin()` posture deliberately and in detail.
 *
 * It exists for one reason that a module global cannot cover: a server app
 * calls `install()` PER REQUEST. `configureActors()` is page-global, so a
 * server installing a transport on every render would let one request's
 * config bleed into another's concurrent render. `serverPlugin` solves this
 * by installing the transport on LIVE CLIENTS ONLY and registering a
 * disposable; this does the same, and provides the per-app context that the
 * hooks (and later milestones' cell registry and live connection) resolve.
 *
 * It deliberately does NOT register type handlers. `../wire-shared` reads
 * core's `__SIGX_SERVERFN_CODEC__` seam directly, so a single
 * `serverPlugin({ types })` already covers the actor wire as well as the
 * serverFn wire. Registering them again here would be a second copy of one
 * vocabulary — a documentation gap, not a code gap.
 */
import type { App, Plugin } from 'sigx';
import { configureActors, fetchTransport, type ActorTransport, type ActorTransportConfig } from '../client';
import { ACTORS_TOKEN, isLiveClient, type ActorsContext } from './context';
import { setProvided } from 'sigx/internals';

export interface ActorsPluginOptions {
    /**
     * How actor calls reach the server. A plain config object is sugar for
     * `fetchTransport(config)`. Installed on live clients only — on the
     * server `actor()` dispatches in-process and needs no transport.
     */
    transport?: ActorTransportConfig | ActorTransport;
}

/** Page-global, like the seam it guards: last install wins. */
let installed: ActorTransport | null = null;

function isTransport(value: ActorTransportConfig | ActorTransport): value is ActorTransport {
    return typeof (value as ActorTransport).call === 'function';
}

export function actorsPlugin(options: ActorsPluginOptions = {}): Plugin {
    return {
        name: 'sigx:actors',
        install(app: App): void {
            const live = isLiveClient();
            let transport: ActorTransport | null = null;

            if (options.transport && live) {
                transport = isTransport(options.transport)
                    ? options.transport
                    : fetchTransport(options.transport);
                if (__DEV__ && installed && installed !== transport) {
                    console.warn(
                        '[sigx actors] actorsPlugin: overwriting a live actor transport ' +
                            'installed by another app — the transport seam is page-global, ' +
                            'last install wins.'
                    );
                }
                const mine = transport;
                installed = mine;
                configureActors(mine);
                app._context.disposables.add(() => {
                    // Only tear down if still ours: a second app may have
                    // installed over us, and clearing then would strip a
                    // transport we do not own.
                    if (installed === mine) {
                        installed = null;
                        configureActors(null);
                    }
                    void mine.close?.();
                });
            }

            const context: ActorsContext = { transport };
            setProvided(app._context.provides, ACTORS_TOKEN, context);
        }
    };
}
