/**
 * The whole point of this example: a same-origin route that forwards to a
 * host's `ops()` endpoint with the bearer attached **server-side**.
 *
 * Two facts about `ops()` make this mandatory rather than a style choice:
 *
 *   - **It sets no CORS headers.** A browser cannot call it cross-origin at
 *     all, however you configure the fetch.
 *   - **It refuses to construct without a secret outside `__DEV__`**, because
 *     it reports your actor type names, traffic shape and cluster topology —
 *     and the activation list carries actor KEYS, which are user data.
 *
 * Put the secret in browser code to get around the first and you have handed
 * every visitor the second. So the browser talks to *your* origin, this
 * handler decides whether the caller may look, and only then does the token
 * appear.
 *
 * It is a plain Node handler so that ONE implementation serves both the Vite
 * dev server (`vite.config.ts` mounts it as middleware) and `server.mjs`. An
 * example whose lesson exists in two copies has already lost the lesson.
 */
import type { IncomingMessage, ServerResponse } from 'node:http';

export interface OpsProxyOptions {
    /** Origin of a host's mount, e.g. `http://127.0.0.1:5391`. */
    host: string;
    /** The `ops({ secret })` bearer token — server-side only. */
    secret: string;
    /** The path this handler answers on, e.g. `/ops`. */
    mount: string;
    /** The host's `ops({ base })`. Default `/_sigx/ops`. */
    base?: string;
    /**
     * May this caller look?
     *
     * A stub here, because an example has no login. In a real portal this is
     * where your existing session check goes — and it is the ONLY thing
     * standing between an anonymous visitor and your cluster topology, so it
     * is not the line to leave for later.
     */
    isOperator?: (request: IncomingMessage) => boolean | Promise<boolean>;
}

export type NextHandler = (error?: unknown) => void;

/** `{ name: value }` when the upstream sent it, `{}` when it did not. */
function forward(upstream: Response, name: string): Record<string, string> {
    const value = upstream.headers.get(name);
    return value === null ? {} : { [name]: value };
}

export function opsProxy(options: OpsProxyOptions) {
    const host = options.host.replace(/\/+$/, '');
    const base = (options.base ?? '/_sigx/ops').replace(/\/+$/, '');
    const mount = options.mount.replace(/\/+$/, '');
    const isOperator = options.isOperator ?? (() => true);

    return async function handle(
        request: IncomingMessage,
        response: ServerResponse,
        next: NextHandler
    ): Promise<void> {
        const target = request.url ?? '/';
        // `startsWith(mount)` alone would also match `/ops-internal`.
        if (target !== mount && !target.startsWith(`${mount}/`) && !target.startsWith(`${mount}?`)) {
            next();
            return;
        }
        if (!(await isOperator(request))) {
            response.writeHead(403, { 'content-type': 'application/json' });
            response.end('{"error":"not an operator"}');
            return;
        }

        // Rewrite only the MOUNT prefix and keep everything after it intact.
        // `httpSource` appends `/cluster` and the `?detail=1&host=…` query
        // itself, and a proxy that dropped either would turn the Cluster tab
        // into a 404 and the drill-down into an empty panel.
        const url = `${host}${base}${target.slice(mount.length)}`;
        // The METHOD is forwarded, not assumed. `ops()` serves GET and HEAD
        // and answers 405 to anything else — a proxy that turned every
        // request into a GET would make a HEAD probe silently expensive and
        // hide the 405 a wrong verb is supposed to produce.
        const method = request.method ?? 'GET';
        try {
            const upstream = await fetch(url, {
                method,
                headers: { authorization: `Bearer ${options.secret}` }
            });
            // A HEAD response has no body, by definition — reading one and
            // writing it back would make the reply not a HEAD reply.
            const body = method === 'HEAD' ? null : await upstream.text();
            response.writeHead(upstream.status, {
                'content-type': upstream.headers.get('content-type') ?? 'application/json',
                // The numbers are read precisely because they change.
                'cache-control': 'no-store',
                // Pass through the two headers that CARRY the diagnosis.
                // Flattening a 401 to a bare status is how you spend ten
                // minutes wondering whether the proxy's secret is stale or
                // the host is; `allow` does the same job for a 405.
                ...forward(upstream, 'www-authenticate'),
                ...forward(upstream, 'allow')
            });
            response.end(body ?? undefined);
        } catch (error) {
            // 502 rather than 500: the dashboard renders the status text, and
            // "the cluster is not answering" is a different thing to debug
            // from "the portal is broken".
            response.writeHead(502, { 'content-type': 'application/json' });
            response.end(
                JSON.stringify({
                    error: `cannot reach ${host} — is the cluster running? (${
                        (error as Error)?.message ?? String(error)
                    })`
                })
            );
        }
    };
}
