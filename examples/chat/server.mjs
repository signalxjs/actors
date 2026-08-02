/**
 * The production server — TWO listeners with different trust levels.
 *
 * PUBLIC (PORT, default 3000 local / 8080 in the chart) — what the ingress
 * routes to:
 *
 *   /_sigx/actor/*  actor endpoint      (default same-origin policy)
 *   /_sigx/fn/*     serverFn endpoint
 *   /assets/*       built client assets
 *   /_sigx/<else>   HARD 404 — health/ops/host must not be publicly
 *                   reachable, and must never fall through to the document
 *                   handler (which would 200 them as HTML)
 *   everything else the SSR document
 *
 * INTERNAL (INTERNAL_PORT, default 7311) — pod-network only: the full
 * `createAppHandler` with every plugin route (health for kubelet probes,
 * ops for `kubectl port-forward`, the cluster's host-to-host mount, and
 * the actor endpoint peers proxy misplaced calls through). `origin: false`
 * here — probes and peers are Node clients with no Origin header.
 *
 * With REDIS_URL set the app clusters: redisCluster membership/directory,
 * redisStorage state (see src/actors.app.ts), advertise = this pod's
 * internal listener. Without it: the same single-node dev server as ever.
 *
 * Importing `@sigx/server` (transitively, through the fn handler and the
 * SSR entry) stamps the request-scope seam the renderer opens around each
 * render — which is what makes the actors' `requireUser` guard work during
 * SSR rather than throwing on a detached context.
 */
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import { extname, join, resolve, sep } from 'node:path';
import { createServerFnHandler } from '@sigx/server/node';
import { createRequestHandler } from '@sigx/server-renderer/node';
import {
    createActorHandler,
    createAppHandler,
    attachSignalHandlers
} from '@sigx/actors/node';
import { health, metrics, ops } from '@sigx/actors/host';
import { cluster, clusterStats, preferLocalPolicy } from '@sigx/actors/cluster';
import { redisCluster } from '@sigx/actors-redis';

const here = import.meta.dirname;
const clientDir = join(here, 'dist/client');
const assetsDir = join(clientDir, 'assets');

const { createApp, actorApp } = await import('./dist/server/entry-server.js');
// The emitted registry exports `serverFns`; the handler option is `functions`.
const { serverFns } = await import('./dist/server/sigx-server-fns.js');

const PORT = Number(process.env.PORT ?? 3000);
const INTERNAL_PORT = Number(process.env.INTERNAL_PORT ?? 7311);
const REDIS_URL = process.env.REDIS_URL;

const need = (name) => {
    const value = process.env[name];
    if (!value) {
        console.error(`[chat] missing required env ${name} (required with REDIS_URL)`);
        process.exit(1);
    }
    return value;
};

// health() needs no wiring; metrics() feeds ops(); cluster() only with a
// Redis to cluster through.
let composed = actorApp.use(metrics()).use(health());
let plugin = null;
if (REDIS_URL) {
    const { Agent, fetch: undiciFetch } = await import('undici');
    const agent = new Agent({ connections: Number(process.env.FETCH_CONNECTIONS ?? 64) });
    plugin = cluster({
        providers: redisCluster({
            url: REDIS_URL,
            namespace: process.env.SIGX_NAMESPACE ?? 'chat'
        }),
        advertise: `http://${process.env.POD_IP ?? '127.0.0.1'}:${INTERNAL_PORT}`,
        secret: need('CLUSTER_SECRET'),
        // Half of the locality pair (the other half is the edge hashing
        // `x-sigx-actor-route` — see the chart's actor Ingress). The LB
        // sends every call for a room to the same host; this makes that
        // host ACTIVATE the room, so the two agree without either knowing
        // the other's algorithm. Measured before this: 213k cross-host
        // hops per 250k public requests, and adding hosts bought +7%
        // throughput for +60% CPU. Neither half works alone.
        policy: preferLocalPolicy(),
        fetch: (url, init) => undiciFetch(url, { ...init, dispatcher: agent })
    });
    composed = composed.use(plugin).use(
        ops({
            secret: need('OPS_SECRET'),
            // The second argument is what `?detail=1` on the ops route asked
                // for. Spreading it is what lets a dashboard drill into ONE
                // host without every poll paying for a fleet-wide actor walk.
                cluster: (signal, query) => clusterStats(plugin.placement, { signal, ...query })
        })
    );
}

// INTERNAL listener first — it is the advertise target, and it must be
// accepting before start() joins membership (peers may dial immediately).
let stopping = false;
const internalHandler = createAppHandler(composed, { origin: false });
const internal = createServer((req, res) => {
    if (stopping) res.setHeader('connection', 'close');
    internalHandler(req, res);
});
await new Promise((r) => internal.listen(INTERNAL_PORT, r));

const host = await composed.start();

// Vite rewrote the built template's <script>/<link> tags to the hashed
// asset names, so it needs no `document.assets` — that option is for
// entries the HTML does not already reference.
const template = await readFile(join(clientDir, 'index.html'), 'utf8');

// PUBLIC surface: the bare actor endpoint (NOT the app handler — plugin
// routes stay internal), with the DEFAULT same-origin policy. The ingress
// forwards x-forwarded-proto/host, which @sigx/server honours, so the
// browser's `Origin: https://chat...` compares like with like.
const publicActor = createActorHandler({ host });
const fns = createServerFnHandler({ functions: serverFns });
const document = createRequestHandler({ template, app: (url) => createApp(url) });

// Everything under /_sigx (including the bare prefix) that is not the
// actor or fn endpoint is 404 on the public side — no health, no ops, no
// host mount, and no SSR fallthrough.
const RESERVED = /^\/_sigx(?:\/(?!actor(?:\/|$)|fn(?:\/|$))|$)/;

const MIME = { '.js': 'text/javascript', '.css': 'text/css', '.html': 'text/html' };

function notFound(res) {
    // Headers may already be out: createReadStream on a DIRECTORY fires
    // 'open' and only then errors with EISDIR, so the 200 has been written
    // by the time this runs. Writing again throws ERR_HTTP_HEADERS_SENT,
    // which kills the process rather than the request.
    if (res.writableEnded) return;
    if (res.headersSent) return res.end();
    res.writeHead(404, { 'content-type': 'text/plain' });
    res.end('not found');
}

function serveAsset(req, res, next) {
    // `req.url` is the RAW request target — Node does not normalize it, and
    // a client that does not either (a raw socket) can send
    // `/assets/../../../package.json`. Decode, resolve, then require the
    // result to still be inside the assets directory.
    let pathname;
    try {
        // Not every request target is a parseable URL: `//` and absolute
        // forms like `http://[` both throw here.
        ({ pathname } = new URL(req.url ?? '/', 'http://localhost'));
    } catch {
        return next(); // unparseable is certainly not one of our assets
    }
    if (!pathname.startsWith('/assets/')) return next();

    let decoded;
    try {
        decoded = decodeURIComponent(pathname);
    } catch {
        return notFound(res); // malformed percent-encoding
    }

    const file = resolve(assetsDir, '.' + decoded.slice('/assets'.length));
    if (file !== assetsDir && !file.startsWith(assetsDir + sep)) return notFound(res);

    const stream = createReadStream(file);
    // Headers go out only once the file is known to exist. Writing them up
    // front means a miss cannot answer 404 — writeHead throws
    // ERR_HTTP_HEADERS_SENT from the stream's error handler, which takes the
    // whole server down rather than returning a status.
    stream.once('open', () => {
        res.writeHead(200, {
            'content-type': MIME[extname(file)] ?? 'application/octet-stream',
            // Vite fingerprints asset filenames, so they are immutable.
            'cache-control': 'public, max-age=31536000, immutable'
        });
        stream.pipe(res);
    });
    stream.once('error', () => notFound(res));
}

const pub = createServer((req, res) => {
    if (stopping) res.setHeader('connection', 'close');
    let pathname = '/';
    try {
        ({ pathname } = new URL(req.url ?? '/', 'http://localhost'));
    } catch {
        // fall through — the document handler answers unparseable targets
    }
    if (RESERVED.test(pathname)) {
        res.writeHead(404, { 'content-type': 'text/plain' });
        return res.end('not found');
    }
    publicActor(req, res, () =>
        fns(req, res, () => serveAsset(req, res, () => document(req, res)))
    );
});
await new Promise((r) => pub.listen(PORT, r));

// Drain BOTH edges on shutdown (the #142 pattern): once stopping, every
// response carries connection: close so client pools retire sockets one
// response at a time; listeners close and stragglers are cut only after
// the actor drain hands everything off. A failed drain exits 1 — the exit
// code is the pod's last diagnostic.
const shutdown = async () => {
    if (stopping) return;
    stopping = true;
    let code = 0;
    try {
        await host.stop({ timeoutMs: 30_000 });
    } catch (error) {
        console.error('[chat] drain failed:', error);
        code = 1;
    }
    pub.close();
    internal.close();
    pub.closeAllConnections();
    internal.closeAllConnections();
    process.exit(code);
};
process.once('SIGTERM', () => void shutdown());
process.once('SIGINT', () => void shutdown());

console.log(
    `[chat] public :${PORT}  internal :${INTERNAL_PORT}  ` +
        (plugin
            ? `host ${plugin.placement.identity.hostId} clustered via redis  `
            : 'single-node (no REDIS_URL)  ') +
        `NODE_ENV=${process.env.NODE_ENV ?? '(unset)'}`
);
console.log(`chat  http://localhost:${PORT}  — sign in from the page footer`);
