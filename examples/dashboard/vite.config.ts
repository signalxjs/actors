import { defineConfig } from 'vite';
import { opsProxy } from './src/ops-proxy';
import { OPS_HOST, OPS_MOUNT, OPS_SECRET, PORT } from './src/config.server';

export default defineConfig({
    server: { port: PORT, strictPort: true },
    preview: { port: PORT, strictPort: true },
    esbuild: { jsx: 'automatic', jsxImportSource: 'sigx' },
    plugins: [
        {
            // The SAME handler `server.mjs` mounts, as dev middleware. An
            // example whose lesson exists in two copies has already lost the
            // lesson — and a dev-only proxy configured in `server.proxy`
            // would put the bearer token in a build config, which is the
            // habit this example exists to argue against.
            name: 'ops-proxy',
            configureServer(server) {
                server.middlewares.use(
                    opsProxy({ host: OPS_HOST, secret: OPS_SECRET, mount: OPS_MOUNT })
                );
                // Printed because it is the thing to notice. Vite's banner
                // says where the PAGE is; what makes this example worth
                // reading is where the TOKEN is, and that is here.
                //
                // Wrapping `printUrls` rather than listening for the server's
                // `listening` event: that fires before the banner is written,
                // so the line would land above "VITE ready" instead of under
                // the URL it belongs with.
                const printUrls = server.printUrls.bind(server);
                server.printUrls = (): void => {
                    printUrls();
                    server.config.logger.info(
                        `  ➜  ops proxy: ${OPS_MOUNT}  →  ${OPS_HOST}  ` +
                            '(bearer attached here, never in the browser)'
                    );
                };
            }
        }
    ]
});
