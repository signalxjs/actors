/**
 * Production entry: serve the built client, and mount the ops proxy.
 *
 *   pnpm --filter dashboard-example build
 *   pnpm --filter dashboard-example start
 *
 * The proxy is the same handler the dev server mounts (`vite.config.ts`), so
 * the thing this example is about behaves identically in both — which is the
 * only way an example about a security boundary is worth anything.
 *
 * Needs Node >= 22.18 (built-in type stripping) for the `.ts` imports, the
 * same as `examples/counter/server.mjs`.
 */
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { join, extname } from 'node:path';
import { opsProxy } from './src/ops-proxy.ts';
import { resolveStatic } from './src/static.ts';
import { OPS_HOSTS, OPS_SECRET, PORT } from './src/config.server.ts';
import { OPS_MOUNT } from './src/config.public.ts';

const MIME = {
    '.html': 'text/html',
    '.js': 'text/javascript',
    '.css': 'text/css',
    '.svg': 'image/svg+xml',
    '.map': 'application/json'
};

// Resolve the built client against THIS file, not the working directory, so
// `node examples/dashboard/server.mjs` works from anywhere.
const clientDir = join(import.meta.dirname, 'dist');

const proxy = opsProxy({
    hosts: OPS_HOSTS,
    secret: OPS_SECRET,
    mount: OPS_MOUNT
    // isOperator: (req) => yourSessionCheck(req)   ← the line a real portal adds
});

const server = createServer((req, res) => {
    void proxy(req, res, async () => {
        // Static fallthrough: the built client. `resolveStatic` is what keeps
        // a raw `/../package.json` inside dist/ — see src/static.ts.
        const file = resolveStatic(clientDir, req.url ?? '/');
        if (!file) {
            res.writeHead(400).end('bad request');
            return;
        }
        try {
            const body = await readFile(file);
            res.writeHead(200, { 'content-type': MIME[extname(file)] ?? 'application/octet-stream' });
            res.end(body);
        } catch {
            // A single-page app: unknown paths are the app's, not 404s.
            try {
                res.writeHead(200, { 'content-type': 'text/html' });
                res.end(await readFile(join(clientDir, 'index.html')));
            } catch {
                res.writeHead(404).end('build the example first: pnpm --filter dashboard-example build');
            }
        }
    });
});

server.listen(PORT, () => {
    console.log(`dashboard    http://localhost:${PORT}`);
    console.log(`ops proxy    ${OPS_MOUNT}  →  ${OPS_HOSTS.join(", ")}  (bearer attached here, never in the browser)`);
});
