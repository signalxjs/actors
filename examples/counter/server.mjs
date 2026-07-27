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
import { defineActorApp } from '@sigx/actors/silo';
import { createAppHandler, attachSignalHandlers, fileStorage } from '@sigx/actors/node';
import { Counter } from './src/counter.actor.ts';

// The app is the composition root: storage, defaults and every plugin in
// one place. `src/actors.app.ts` is the Vite-side equivalent — it takes
// its registry from `virtual:sigx-actors`, which only resolves under Vite,
// so this entry (plain Node type-stripping) lists its actors instead.
const app = defineActorApp({
    actors: [Counter],
    storage: fileStorage({ dir: '.actors' })
});
const silo = await app.start();
attachSignalHandlers(silo);

// One handler for the public endpoint and any plugin-contributed route.
const actorHandler = createAppHandler(app);

const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css' };

createServer((req, res) => {
    void actorHandler(req, res, async () => {
        // Static fallthrough: the built client app.
        const path = req.url === '/' ? '/index.html' : req.url ?? '/index.html';
        try {
            const file = await readFile(join('dist', path));
            res.writeHead(200, { 'content-type': MIME[extname(path)] ?? 'application/octet-stream' });
            res.end(file);
        } catch {
            res.writeHead(404).end('not found');
        }
    });
}).listen(5199, () => {
    console.log('counter example on http://localhost:5199');
});
