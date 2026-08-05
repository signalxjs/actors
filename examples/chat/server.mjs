/**
 * The production entry. Four handlers in a chain, and that chain is the
 * whole lesson:
 *
 *   actor endpoint → serverFns → built assets → the SSR document
 *
 * Importing `@sigx/server` (transitively, through the fn handler and the SSR
 * entry) stamps the request scope the renderer opens around each render —
 * which is what lets `authenticate` read the cookie during SSR instead of
 * throwing on a detached context.
 */
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, sep } from 'node:path';
import { createServerFnHandler } from '@sigx/server/node';
import { createRequestHandler } from '@sigx/server-renderer/node';
import { createActorHandler, attachSignalHandlers } from '@sigx/actors/node';
import { resolveStatic } from './src/static.ts';

const here = import.meta.dirname;
const clientDir = join(here, 'dist/client');
const assetsDir = join(clientDir, 'assets');

const { createApp, actorApp } = await import('./dist/server/entry-server.js');
// The emitted registry exports `serverFns`; the handler option is `functions`.
const { serverFns } = await import('./dist/server/sigx-server-fns.js');

// Not 3000. That port is contended on every developer's machine.
const PORT = Number(process.env.PORT ?? 5290);

const host = await actorApp.start();

// Vite rewrote the built template's script/link tags to the hashed asset
// names, so it needs no `document.assets` — that option is for entries the
// HTML does not already reference.
const template = await readFile(join(clientDir, 'index.html'), 'utf8');

// `createActorHandler`, NOT `createAppHandler`. This mounts the actor route
// and nothing else, so there is no health, ops or host-to-host surface on
// this listener to leak. See the README before changing it.
const actors = createActorHandler({ host });
const fns = createServerFnHandler({ functions: serverFns });
const document = createRequestHandler({ template, app: (url) => createApp(url) });

const MIME = { '.js': 'text/javascript', '.css': 'text/css', '.html': 'text/html' };

// Everything under `/_sigx` that is not the actor or fn endpoint is a hard
// 404. Without this, an unmounted route falls through to the SSR document
// and answers 200 with a page — so `/_sigx/health` reports healthy on a
// host that never mounted health at all, and a probe believes it.
const RESERVED = /^\/_sigx(?:\/(?!actor(?:\/|$)|fn(?:\/|$))|$)/;

async function serveAsset(req, res, next) {
    const file = resolveStatic(clientDir, req.url ?? '/');
    // Outside the root, or not an asset request — let the document answer.
    // The `+ sep` is not decoration: a bare prefix check would also accept
    // a sibling `assets-other/`, which is the same mistake `static.ts`
    // guards against one level up.
    if (!file || (file !== assetsDir && !file.startsWith(assetsDir + sep))) return next();

    let body;
    try {
        // Read BEFORE writing headers: a miss (or a directory, which fails
        // with EISDIR) must still be able to answer 404, and writeHead after
        // the 200 is out throws ERR_HTTP_HEADERS_SENT — killing the process
        // rather than the request.
        body = await readFile(file);
    } catch {
        // A miss UNDER /assets/ is a 404. Falling through would render the
        // SSR document, so a missing script would arrive as 200 text/html
        // and fail somewhere far less obvious.
        res.writeHead(404, { 'content-type': 'text/plain' });
        return void res.end('not found');
    }
    res.writeHead(200, {
        'content-type': MIME[extname(file)] ?? 'application/octet-stream',
        // Vite fingerprints asset filenames, so they are immutable.
        'cache-control': 'public, max-age=31536000, immutable'
    });
    res.end(body);
}

let stopping = false;
const server = createServer((req, res) => {
    // Once shutdown starts, every response says `connection: close`, so
    // keep-alive clients retire their pooled socket after the response they
    // are already receiving.
    if (stopping) res.setHeader('connection', 'close');
    let pathname = '/';
    try {
        ({ pathname } = new URL(req.url ?? '/', 'http://localhost'));
    } catch {
        // Unparseable target — let the document answer it.
    }
    if (RESERVED.test(pathname)) {
        res.writeHead(404, { 'content-type': 'text/plain' });
        return void res.end('not found');
    }
    actors(req, res, () =>
        fns(req, res, () => void serveAsset(req, res, () => document(req, res)))
    );
});

// Pass the server AND onStopBegin: draining the actors is only half a
// graceful shutdown, and `server` alone closes the listener at the end
// without giving pools a chance to retire their sockets first.
attachSignalHandlers(host, { server, onStopBegin: () => (stopping = true) });

server.listen(PORT, () => {
    console.log(`chat  http://localhost:${PORT}  — sign in from the page footer`);
});
