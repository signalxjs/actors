/**
 * `createFetchHandler` — the WinterCG entry for a whole app: the public
 * actor endpoint plus every plugin-contributed route, as one
 * `(Request) => Response`.
 *
 * This is the portable half of `@sigx/actors/node`'s `createAppHandler`,
 * which is now just this plus an `IncomingMessage`/`ServerResponse` bridge.
 * Deno and Bun need no adapter package at all:
 *
 * ```ts
 * Deno.serve(createFetchHandler(app));
 * ```
 */
import type { ActorApp } from '../silo/app';
import type { Silo } from '../types';
import { handleActorRequest, matchesActorRequest } from './actor-endpoint';
import type { ActorRequestOptions } from './actor-endpoint';

export interface FetchHandlerOptions extends Omit<ActorRequestOptions, 'silo'> {
    /** URL prefix of the public actor endpoint. Default `/_sigx/actor`. */
    base?: string;
    /**
     * Answers requests this app does not own — mount a document handler
     * here to compose the two. Returning `undefined` falls through to the
     * default 404.
     *
     * The handler itself always resolves to a `Response`, never
     * `undefined`, so it can be handed straight to `Deno.serve` or a
     * Worker's `fetch`.
     */
    fallback?: (request: Request) => Response | Promise<Response> | undefined;
}

/**
 * Build the handler. Routes are read on the first REQUEST, not here:
 * reading `app.routes` seals the app, and building a handler must not
 * quietly freeze plugin registration.
 */
export function createFetchHandler(
    app: ActorApp,
    options: FetchHandlerOptions = {}
): (request: Request) => Promise<Response> {
    const { base = '/_sigx/actor', fallback, ...actorOptions } = options;

    return async (request: Request): Promise<Response> => {
        const silo = app.silo;
        if (!silo) {
            // Mounted but not running — 503, not 500 and not a fallthrough.
            return errorResponse(503, 'the actor app is not running');
        }
        for (const route of app.routes) {
            if (route.match(request)) return route.handle(request, silo);
        }
        if (matchesActorRequest(request, base)) {
            // `base` is forwarded, not just consumed for matching: the
            // redirect composes the owner endpoint from it, and a mount on
            // a custom base must name ITS path, not the default.
            return handleActorRequest(request, { ...actorOptions, base, silo });
        }
        return (await fallback?.(request)) ?? errorResponse(404, 'not found');
    };
}

/** The silo behind a running app, for hand-rolled mounts. */
export function requireSilo(app: ActorApp): Silo {
    const silo = app.silo;
    if (!silo) {
        throw new Error(
            '[sigx actors] the app is not running — await app.start() before serving requests.'
        );
    }
    return silo;
}

function errorResponse(status: number, message: string): Response {
    return new Response(
        JSON.stringify({ error: { message: `[sigx actors] ${message}`, status } }),
        { status, headers: { 'content-type': 'application/json' } }
    );
}
