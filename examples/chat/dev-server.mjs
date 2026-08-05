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

const port = Number(process.env.PORT ?? 5273);
createServer((req, res) => {
    vite.middlewares(req, res, () => {
        document(req, res).catch((error) => {
            vite.ssrFixStacktrace(error);
            console.error(error);
            if (!res.headersSent) res.writeHead(500, { 'content-type': 'text/plain' });
            res.end(String(error?.stack ?? error));
        });
    });
}).listen(port, () => {
    console.log(`chat dev  http://localhost:${port}`);
    console.log('sign in from the page footer — the cookie is HttpOnly and signed');
});
