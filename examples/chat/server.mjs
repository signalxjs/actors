/**
 * The production server — one mount graph, three handlers.
 *
 *   /_sigx/actor/*  actor endpoint      (@sigx/actors/node)
 *   /_sigx/fn/*     serverFn endpoint   (@sigx/server/node)
 *   /assets/*       built client assets
 *   everything else the document        (@sigx/server-renderer/node)
 *
 * All connect-style, so composition is just `next()`. Importing
 * `@sigx/server` (transitively, through the fn handler and the SSR entry)
 * is what stamps the request-scope seam the renderer opens around each
 * render — which is what makes the actors' `requireUser` guard work during
 * SSR rather than throwing on a detached context.
 */
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import { extname, join, resolve, sep } from 'node:path';
import { createServerFnHandler } from '@sigx/server/node';
import { createRequestHandler } from '@sigx/server-renderer/node';
import { createAppHandler, attachSignalHandlers } from '@sigx/actors/node';

const here = import.meta.dirname;
const clientDir = join(here, 'dist/client');
const assetsDir = join(clientDir, 'assets');

const { createApp, actorApp } = await import('./dist/server/entry-server.js');
// The emitted registry exports `serverFns`; the handler option is `functions`.
const { serverFns } = await import('./dist/server/sigx-server-fns.js');

const silo = await actorApp.start();
attachSignalHandlers(silo);

// Vite rewrote the built template's <script>/<link> tags to the hashed
// asset names, so it needs no `document.assets` — that option is for
// entries the HTML does not already reference.
const template = await readFile(join(clientDir, 'index.html'), 'utf8');

const actors = createAppHandler(actorApp);
const fns = createServerFnHandler({ functions: serverFns });
const document = createRequestHandler({ template, app: (url) => createApp(url) });

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

const port = Number(process.env.PORT ?? 3000);
createServer((req, res) => {
    actors(req, res, () =>
        fns(req, res, () => serveAsset(req, res, () => document(req, res)))
    );
}).listen(port, () => {
    console.log(`chat  http://localhost:${port}`);
    console.log('sign in:  document.cookie = "user=ada"');
});
