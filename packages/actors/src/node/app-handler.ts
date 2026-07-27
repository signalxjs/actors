/**
 * Node mounting for a whole app: the public actor endpoint PLUS every
 * plugin-contributed route (the cluster's internal silo-to-silo mount being
 * the first of them).
 *
 * Core's connect adapter resolves a symbol to a wrapped function, which is
 * the wrong shape for a plugin route — those are plain
 * `(Request) => Promise<Response>`. Nothing in the sigx packages exposes a
 * general fetch-handler→Node bridge, so every clustered deployment has been
 * hand-writing one (header copying, body reading, abort wiring, response
 * streaming). It belongs here, written once and written carefully.
 */
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { NodeRequestHandler } from '@sigx/server/node';
import type { ActorApp, ActorRoute } from '../silo/app';
import type { Silo } from '../types';
import { createActorHandler, type ActorHandlerOptions } from './handler';

/** Core's serverFn default; kept in step so both mounts cap alike. */
const DEFAULT_MAX_BODY_BYTES = 1024 * 1024;

export interface AppHandlerOptions extends Omit<ActorHandlerOptions, 'silo'> {
    /**
     * Origin used to build the WinterCG `Request` for plugin routes when the
     * client sends no `Host` header. Only the origin matters — routes match
     * on pathname. Default `http://localhost`. (Distinct from `origin`,
     * which is the serverFn same-origin POLICY inherited above.)
     */
    fallbackOrigin?: string;
}

/** A failure that already knows its client-visible status. */
class HttpError extends Error {
    constructor(
        readonly status: number,
        message: string
    ) {
        super(message);
    }
}

/**
 * Mount an app on a Node server: plugin routes first (they own specific
 * prefixes), then the public actor endpoint, then `next()`.
 *
 * The silo is resolved per request, so the handler can be built before
 * `app.start()` — the ordinary case, since a cluster silo usually needs its
 * listener to exist before it can advertise an address.
 */
export function createAppHandler(
    app: ActorApp,
    options: AppHandlerOptions = {}
): NodeRequestHandler {
    const { fallbackOrigin = 'http://localhost', ...actorOptions } = options;
    const maxBodyBytes = options.maxBodyBytes ?? DEFAULT_MAX_BODY_BYTES;

    // Keyed on the silo instance, never captured: a handler must not outlive
    // the silo it resolves against.
    let cached: { silo: Silo; handler: NodeRequestHandler } | null = null;
    const actorMount = (silo: Silo): NodeRequestHandler => {
        if (cached?.silo !== silo) {
            cached = {
                silo,
                // The resolved cap, not `options.maxBodyBytes` — otherwise
                // the two mounts silently disagree when it is omitted.
                handler: createActorHandler({ ...actorOptions, maxBodyBytes, silo })
            };
        }
        return cached.handler;
    };

    return async (req, res, next) => {
        // `createServer(handler)` calls back with (req, res) ONLY, and that
        // is exactly how the README mounts this — so an unmatched request
        // must end here rather than await a `next` that does not exist.
        const fallthrough = next ?? ((): void => void sendError(res, 404, 'not found'));
        try {
            const silo = app.silo;
            if (!silo) {
                // The mount exists but the app is not running (yet, or any
                // more) — that is precisely 503, not a 500 or a fallthrough.
                sendError(res, 503, 'the actor app is not running');
                return;
            }
            // Read on first REQUEST, not at construction: `app.routes`
            // seals the app, and building a handler must not quietly freeze
            // plugin registration — mounting before `.use()` is legitimate.
            // The seal is memoized, so this is free after the first call.
            const routes = app.routes;
            const headers = toHeaders(req);
            const url = new URL(
                req.url ?? '/',
                req.headers.host ? `http://${req.headers.host}` : fallbackOrigin
            );
            if (routes.length > 0) {
                // Headers included: `ActorRoute.match` receives a Request, so
                // a route keying on auth or content-type must be able to see
                // them. (The body is not needed to route.)
                const probe = new Request(url, { method: req.method ?? 'GET', headers });
                const route = routes.find((candidate) => candidate.match(probe));
                if (route) {
                    await serveRoute(route, req, res, { url, headers, silo, maxBodyBytes });
                    return;
                }
            }
            await actorMount(silo)(req, res, fallthrough);
        } catch (error) {
            // A throw here would otherwise surface as an unhandled rejection
            // in whatever host called us with `void handler(...)`.
            if (__DEV__) console.error('[sigx actors] app handler failed:', error);
            sendError(res, 500, 'internal error');
        }
    };
}

interface ServeContext {
    url: URL;
    headers: Headers;
    silo: Silo;
    maxBodyBytes: number;
}

async function serveRoute(
    route: ActorRoute,
    req: IncomingMessage,
    res: ServerResponse,
    ctx: ServeContext
): Promise<void> {
    // A peer cancelling a cross-host stream closes the socket, and that has
    // to reach the actor's generator (so it releases its keep-alive) — so
    // the request's signal is tied to the connection.
    const aborter = new AbortController();
    res.once('close', () => {
        if (!res.writableEnded) aborter.abort();
    });

    let response: Response;
    try {
        const request = await toRequest(req, ctx, aborter.signal);
        response = await route.handle(request, ctx.silo);
    } catch (error) {
        // Preserve a status the failure already carries (413 for an
        // oversized body); only genuinely unknown failures become 500.
        const status = error instanceof HttpError ? error.status : 500;
        const message = error instanceof HttpError ? error.message : 'internal error';
        if (__DEV__ && status === 500) {
            console.error(`[sigx actors] route ${route.name} failed:`, error);
        }
        sendError(res, status, message);
        // Stop reading a body we have already refused. Done AFTER the
        // response so the client can actually read the status.
        if (!req.readableEnded) req.destroy();
        return;
    }
    await sendResponse(res, response);
}

function toHeaders(req: IncomingMessage): Headers {
    const headers = new Headers();
    for (const [name, value] of Object.entries(req.headers)) {
        if (value === undefined) continue;
        for (const one of Array.isArray(value) ? value : [value]) headers.append(name, one);
    }
    return headers;
}

async function toRequest(
    req: IncomingMessage,
    ctx: ServeContext,
    signal: AbortSignal
): Promise<Request> {
    const method = req.method ?? 'GET';
    if (method === 'GET' || method === 'HEAD') {
        return new Request(ctx.url, { method, headers: ctx.headers, signal });
    }

    // Reject on the declared length before reading a byte, when it is
    // trustworthy enough to be worth it.
    const declared = Number(req.headers['content-length']);
    if (Number.isFinite(declared) && declared > ctx.maxBodyBytes) {
        throw new HttpError(413, 'request body too large');
    }

    // Buffered, not streamed: these are wire-protocol JSON payloads, and a
    // buffered body keeps this bridge clear of the `duplex: 'half'` caveats
    // across Node versions. RESPONSES stream (below) — that is where NDJSON
    // actually matters. The cap is enforced WHILE reading: leaving it to the
    // endpoint's own `maxBodyBytes` would be too late, since the allocation
    // has already happened by then.
    const chunks: Buffer[] = [];
    let total = 0;
    for await (const chunk of req) {
        // A chunk is a string when some upstream middleware called
        // `req.setEncoding()`; Buffer.concat would throw on it, and its
        // `.length` counts CHARACTERS, not bytes — the cap would be wrong.
        const buffer = typeof chunk === 'string' ? Buffer.from(chunk) : (chunk as Buffer);
        total += buffer.length;
        if (total > ctx.maxBodyBytes) throw new HttpError(413, 'request body too large');
        chunks.push(buffer);
    }
    return new Request(ctx.url, {
        method,
        headers: ctx.headers,
        signal,
        body: Buffer.concat(chunks)
    });
}

function sendError(res: ServerResponse, status: number, message: string): void {
    if (res.writableEnded) return;
    if (!res.headersSent) {
        res.writeHead(status, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: { message: `[sigx actors] ${message}`, status } }));
        return;
    }
    res.end();
}

async function sendResponse(res: ServerResponse, response: Response): Promise<void> {
    const headers: Record<string, string | string[]> = {};
    response.headers.forEach((value, name) => {
        // set-cookie is the one header that legitimately repeats.
        if (name === 'set-cookie') {
            const existing = headers[name];
            headers[name] = Array.isArray(existing) ? [...existing, value] : [value];
        } else {
            headers[name] = value;
        }
    });
    res.writeHead(response.status, headers);
    if (!response.body) {
        res.end();
        return;
    }
    try {
        for await (const chunk of response.body as unknown as AsyncIterable<Uint8Array>) {
            // Respect backpressure: a slow peer must not balloon the heap.
            if (!res.write(chunk)) {
                await new Promise<void>((resolve) => res.once('drain', resolve));
            }
        }
    } catch {
        // Peer disconnected mid-stream; the abort wiring above already
        // cancelled the actor-side generator.
    }
    res.end();
}
