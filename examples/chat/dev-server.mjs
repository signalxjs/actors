/**
 * The dev server: Vite in middleware mode, plus the renderer's dev document
 * handler.
 *
 * `vite.middlewares` already carries the actor endpoint — `sigxActors`
 * mounts it from its own `configureServer`, running the app module in the
 * SSR module runner's graph — and `sigxServer`'s serverFn endpoint. So the
 * whole composition is: everything Vite's middleware stack answers, then the
 * document handler for whatever is left.
 */
import { createServer } from 'node:http';
import { createServer as createViteServer } from 'vite';
import { createDevRequestHandler } from '@sigx/vite/ssr';

const vite = await createViteServer({
    root: import.meta.dirname,
    server: { middlewareMode: true },
    appType: 'custom'
});

const document = await createDevRequestHandler(vite, { entry: '/src/entry-server.tsx' });

// The client socket, in dev. Everything loads through the SSR module
// runner ON PURPOSE: `sigxActors({ app })` started the app from
// '/src/actors.app.ts' through that runner, and `ssrLoadModule` caches per
// graph — so this `app` is the plugin's own instance (`app.start()` returns
// the already-started host), and the socket adapter resolves the same
// module family as the app's `authenticate`. Importing either from plain
// node_modules instead would split the module graph and the upgrade would
// authenticate against a feature nobody configured (the #304 bug class).
const [{ attachActorSocket }, { app }] = await Promise.all([
    vite.ssrLoadModule('@sigx/actors-ws/node'),
    vite.ssrLoadModule('/src/actors.app.ts')
]);

const port = Number(process.env.PORT ?? 5273);
const server = createServer((req, res) => {
    vite.middlewares(req, res, () => {
        document(req, res).catch((error) => {
            vite.ssrFixStacktrace(error);
            console.error(error);
            if (!res.headersSent) res.writeHead(500, { 'content-type': 'text/plain' });
            res.end(String(error?.stack ?? error));
        });
    });
});

attachActorSocket(server, { host: await app.start() });

server.listen(port, () => {
    console.log(`chat dev  http://localhost:${port}`);
    console.log('sign in from the page footer — the cookie is HttpOnly and signed');
});
