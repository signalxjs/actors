/**
 * Production entry: serve the built client + mount the actor endpoint.
 * Composition stays visible — the sigx idiom: predicates in the entry.
 *
 *   pnpm build && pnpm start
 *
 * Needs Node >= 22.18 (built-in type stripping) for the .ts actor import.
 */
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join } from 'node:path';
import { createAppHandler, attachSignalHandlers } from '@sigx/actors/node';
import { app } from './src/actors.app.ts';
import { Counter } from './src/counter.actor.ts';
import { resolveStatic } from './src/static.ts';

// THE SAME app module the dev server runs: storage, defaults and every
// plugin shared, declared once. Only the registry differs — Vite hands the
// plugin's over, this entry names its actors.
const host = await app.withActors([Counter]).start();

// One handler for the public endpoint and any plugin-contributed route.
const actorHandler = createAppHandler(app);

const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css' };

// Resolve the built client against THIS file, not the working directory, so
// `node examples/counter/server.mjs` works from anywhere.
const clientDir = join(import.meta.dirname, 'dist');

// Once shutdown starts, every response says `connection: close` so keep-alive
// clients retire their pooled socket after the response they are already
// receiving — rather than reusing it right up to the moment it is destroyed.
let stopping = false;
const server = createServer((req, res) => {
    if (stopping) res.setHeader('connection', 'close');
    void actorHandler(req, res, async () => {
        // Static fallthrough: the built client app. `resolveStatic` is what
        // keeps a raw `/../package.json` inside dist/ — see src/static.ts.
        const file = resolveStatic(clientDir, req.url ?? '/');
        if (!file) return void res.writeHead(404).end('not found');
        let body;
        try {
            // Read BEFORE writing headers: a miss (or a directory, which
            // fails with EISDIR) must still be able to answer 404, and
            // writeHead after the 200 is out throws ERR_HTTP_HEADERS_SENT —
            // which kills the process rather than the request.
            body = await readFile(file);
        } catch {
            return void res.writeHead(404).end('not found');
        }
        res.writeHead(200, { 'content-type': MIME[extname(file)] ?? 'application/octet-stream' });
        res.end(body);
    });
});

// Pass the server AND onStopBegin: stopping the actors is only half a
// graceful shutdown, and `server` alone closes the listener at the end
// without giving pools a chance to retire their sockets first.
attachSignalHandlers(host, { server, onStopBegin: () => (stopping = true) });

server.listen(5199, () => {
    console.log('counter example on http://localhost:5199');
});
