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
import { createSilo } from '@sigx/actors/silo';
import { createActorHandler, attachSignalHandlers, fileStorage } from '@sigx/actors/node';
import { Counter } from './src/counter.actor.ts';

const silo = createSilo({
    actors: [Counter],
    storage: fileStorage({ dir: '.actors' })
});
await silo.start();
attachSignalHandlers(silo);

const actorHandler = createActorHandler({ silo });

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
