/**
 * `@sigx/actors/vite` — `sigxActors()`: the build/dev integration.
 *
 * Satellite-owned and composable with core's `sigxServer()` with zero
 * coordination: actors get their OWN file convention (`*.actor.ts`) and
 * their own mount (`/_sigx/actor`), so neither plugin touches the other's
 * modules or middleware.
 *
 * Jobs, mirroring core's server-fn plugin:
 *  1. CLIENT env: swap each actor module wholesale for `__actorRef` stubs
 *     (values only — types stay the real module's, so the proxy is typed).
 *  2. Prod: emit `virtual:sigx-actors` — the lazy `{ type: () => import }`
 *     registry the server entry passes to `createSilo`.
 *  3. `requireGuards` build gate (default on).
 *  4. Dev: create/start a silo through the SSR module runner (module-graph
 *     identity + HMR survival), mount the actor endpoint middleware, and
 *     deactivate types through storage on actor-file edits.
 */
import type { Plugin, ViteDevServer } from 'vite';
import type { ActorApp } from '../silo/app';
import type { Silo } from '../types';
import { createFilter, normalizePath } from 'vite';
import * as fs from 'node:fs';
import * as path from 'node:path';
import type { IncomingMessage, ServerResponse } from 'node:http';
import {
    extractActors,
    isGeneratedClientModule,
    mayDefineActors,
    type ActorExtraction
} from './extract';

export {
    extractActors,
    mayDefineActors,
    type ActorExtraction,
    type ExtractedActor,
    type ExtractOptions
} from './extract';

export interface SigxActorsOptions {
    /** Which modules are actor modules. Default `**` + `/*.actor.{ts,tsx}`. */
    include?: string | string[];
    /** Excluded from matching. Default: node_modules and dist. */
    exclude?: string | string[];
    /** Server mount path (dev middleware + default endpoint). Default `/_sigx/actor`. */
    base?: string;
    /** Fetch target baked into client refs (absolute for remote backends).
     *  Default: `base`. Call-time precedence: configureActors > this. */
    endpoint?: string;
    /**
     * The guard build gate: every actor needs a `use:` chain or a literal
     * `unguarded: true`. `true` (default) = build error; `'warn'` | `false`
     * downgrade.
     */
    requireGuards?: boolean | 'warn';
    /**
     * Root-relative module exporting the `defineActorApp` app (as `app` or
     * default) — e.g. `'/src/actors.app.ts'`. THE source of truth: dev
     * loads the very module your production entry imports, so storage,
     * placement, codec handlers, defaults and every plugin are identical
     * across the two. Omit and dev runs a bare in-memory silo.
     *
     * It also makes the app-bound `defineActor` extractable, so actor
     * modules may import it from here instead of `@sigx/actors`.
     */
    app?: string;
    /** Origin policy forwarded to the dev endpoint. Default 'same-origin'. */
    origin?: 'same-origin' | 'verify-when-present' | string[] | false;
    /** Body cap forwarded to the dev endpoint. */
    maxBodyBytes?: number;
}

const VIRTUAL_ID = 'virtual:sigx-actors';
const RESOLVED_VIRTUAL_ID = '\0' + VIRTUAL_ID;
const REGISTRY_FILE = 'sigx-actors.js';

const DEFAULT_INCLUDE = ['**/*.actor.ts', '**/*.actor.tsx'];
const DEFAULT_EXCLUDE = ['**/node_modules/**', '**/dist/**'];
const DEFAULT_BASE = '/_sigx/actor';

interface SiloModule {
    createSilo(options: unknown): DevSilo;
}

/**
 * The real runtime contracts, not local look-alikes. These are TYPE-only
 * imports so nothing from the runtime is bundled into the plugin, and
 * aliasing them means a change to either contract (say `createAppHandler`
 * needing `app.silo`) breaks here loudly instead of hiding behind a cast.
 */
type DevApp = ActorApp;
type DevSilo = Silo;

export function sigxActors(options: SigxActorsOptions = {}): Plugin {
    const filter = createFilter(
        options.include ?? DEFAULT_INCLUDE,
        options.exclude ?? DEFAULT_EXCLUDE
    );
    /** Exclude patterns on their own — `filter()` cannot tell them apart. */
    const isExcluded = createFilter(options.exclude ?? DEFAULT_EXCLUDE);
    const base = options.base ?? DEFAULT_BASE;
    const endpoint = options.endpoint ?? base;
    const requireGuards = options.requireGuards ?? true;

    let root = process.cwd();
    let isServe = false;
    let bundledServerBuild = false;
    /** Latest extraction per absolute module path. */
    const extractions = new Map<string, ActorExtraction & { warnings: string[] }>();

    /** Absolute, normalized path of the app module, once root is known. */
    const appModulePath = (): string | null =>
        options.app ? normalizePath(path.resolve(root, options.app.replace(/^\//, ''))) : null;

    const stripExt = (file: string): string => file.replace(/\.[cm]?[jt]sx?$/, '');

    /**
     * Does `source`, imported from `fromFile`, resolve to the app module?
     *
     * Both spellings Vite accepts count. Missing one is not cosmetic: an
     * unrecognized `defineActor` import means the module is never extracted,
     * never client-swapped, and its implementation reaches the browser.
     */
    function isAppImport(source: string, fromFile: string): boolean {
        const target = appModulePath();
        if (!target) return false;
        let resolved: string;
        if (source.startsWith('/')) {
            // Root-relative, the same spelling `sigxActors({ app })` uses.
            resolved = normalizePath(path.resolve(root, source.slice(1)));
        } else if (source.startsWith('.')) {
            resolved = normalizePath(path.resolve(path.dirname(normalizePath(fromFile)), source));
        } else {
            return false;
        }
        return stripExt(resolved) === stripExt(target);
    }

    /** Substrings implying an app-module import, for the cheap pre-filter. */
    function appHints(): string[] {
        const target = appModulePath();
        if (!target) return [];
        return [stripExt(target.slice(target.lastIndexOf('/') + 1))];
    }

    const inRoot = (file: string): boolean => normalizePath(file).startsWith(root + '/');
    const relPath = (file: string): string =>
        inRoot(file) ? normalizePath(file).slice(root.length + 1) : normalizePath(file);
    const devSpec = (file: string): string =>
        inRoot(file) ? '/' + relPath(file) : '/@fs/' + normalizePath(file);

    const isClientOut = (ctx: { environment?: { name?: string } }): boolean =>
        ctx.environment?.name === 'client';

    function extractInto(
        file: string,
        code: string
    ): (ActorExtraction & { warnings: string[] }) | null {
        file = normalizePath(file);
        try {
            const extraction = extractActors(code, relPath(file), {
                endpoint,
                requireGuards,
                isDefineSource: (source) => isAppImport(source, file)
            });
            extractions.set(file, extraction);
            return extraction;
        } catch (error) {
            console.warn(`[sigx:actors] extraction failed for ${relPath(file)}:`, error);
            return null;
        }
    }

    /** Walk the project tree for actor modules (own walk — core's isn't public). */
    function discover(): void {
        const walk = (dir: string): void => {
            let entries: fs.Dirent[];
            try {
                entries = fs
                    .readdirSync(dir, { withFileTypes: true })
                    .sort((a, b) => a.name.localeCompare(b.name));
            } catch {
                return;
            }
            for (const entry of entries) {
                if (
                    entry.name === 'node_modules' ||
                    entry.name === 'dist' ||
                    entry.name.startsWith('.')
                ) {
                    continue;
                }
                const full = path.join(dir, entry.name);
                if (entry.isDirectory()) walk(full);
                else if (filter(normalizePath(full))) {
                    try {
                        extractInto(full, fs.readFileSync(full, 'utf8'));
                    } catch {
                        // unreadable file — transform will retry when served
                    }
                }
            }
        };
        walk(root);
    }

    /** type → { file, exportName } across every extraction. */
    function typeIndex(): Map<string, { file: string; exportName: string }> {
        const index = new Map<string, { file: string; exportName: string }>();
        for (const [file, extraction] of extractions) {
            for (const actor of extraction.actors) {
                index.set(actor.type, { file, exportName: actor.exportName });
            }
        }
        return index;
    }

    return {
        name: 'sigx:actors',
        enforce: 'pre',

        configResolved(config) {
            root = normalizePath(
                config.resolve?.preserveSymlinks
                    ? config.root
                    : safeRealpath(config.root)
            );
            isServe = config.command === 'serve';
            const sigxApi = config.plugins?.find((p) => p.name === 'sigx')?.api as
                | { adapter?: { serverBuild?: string } }
                | undefined;
            bundledServerBuild = sigxApi?.adapter?.serverBuild === 'bundled';
            extractions.clear();
            discover();
        },

        resolveId(id) {
            if (id === VIRTUAL_ID) return RESOLVED_VIRTUAL_ID;
        },

        load(id) {
            if (id !== RESOLVED_VIRTUAL_ID) return;
            const lines: string[] = ['export const actors = {'];
            const owners = new Map<string, string>();
            for (const [file, extraction] of extractions) {
                for (const actor of extraction.actors) {
                    const spec = JSON.stringify(devSpec(file));
                    lines.push(
                        `    ${JSON.stringify(actor.type)}: () => import(${spec})` +
                            `.then(m => m[${JSON.stringify(actor.exportName)}]),`
                    );
                    const owner = owners.get(actor.type);
                    if (owner && owner !== file) {
                        this.warn(
                            `[sigx:actors] actor type ${JSON.stringify(actor.type)} is defined in both ` +
                                `${relPath(owner)} and ${relPath(file)} — the later registration wins.`
                        );
                    }
                    owners.set(actor.type, file);
                }
            }
            lines.push('};');
            return lines.join('\n');
        },

        buildStart() {
            if (isServe) return;
            if (bundledServerBuild) return;
            if (this.environment?.name === 'client') return;
            let hasActors = false;
            for (const extraction of extractions.values()) {
                if (extraction.actors.length > 0) hasActors = true;
            }
            if (!hasActors) return;
            this.emitFile({ type: 'chunk', id: VIRTUAL_ID, fileName: REGISTRY_FILE });
        },

        transform(code, id) {
            let clean = normalizePath(id.split('?')[0]);
            if (clean.startsWith('/@fs/')) {
                clean = clean.slice('/@fs/'.length);
                if (!clean.startsWith('/') && !/^[a-zA-Z]:/.test(clean)) clean = '/' + clean;
            }
            if (!filter(clean)) {
                // defineActor outside *.actor.ts silently stays a server value
                // AND leaks its implementation to the browser — warn early.
                //
                // NOT for EXCLUDED files. `filter()` answers false for two
                // different things — "not an actor module" and "explicitly
                // excluded" — and only the first is worth warning about.
                // Conflating them meant `@sigx/actors`' own bundled dist
                // (which mentions defineActor, and is excluded by
                // `**/dist/**`) warned on every client build of an app that
                // links the package from a workspace.
                if (
                    !isExcluded(clean) &&
                    isClientOut(this) &&
                    mayDefineActors(code, appHints()) &&
                    !isGeneratedClientModule(code)
                ) {
                    // Name the patterns actually in force: the default set
                    // includes .actor.tsx too, and `options.include` can
                    // replace both — so a hard-coded "*.actor.ts" tells a
                    // developer using either of those they are wrong.
                    const patterns = [options.include ?? DEFAULT_INCLUDE]
                        .flat()
                        .map((p) => String(p))
                        .join(', ');
                    this.warn(
                        `[sigx:actors] ${relPath(clean)} calls defineActor but matches none of ` +
                            `the actor-module patterns (${patterns}) — it will NOT be swapped ` +
                            `for the browser. Move the definition into an actor module.`
                    );
                }
                return null;
            }
            // Rolldown may re-run the transform over our own stub output.
            if (isGeneratedClientModule(code)) return null;
            const extraction = extractInto(clean, code);
            for (const warning of extraction?.warnings ?? []) {
                this.warn(`[sigx:actors] ${relPath(clean)}: ${warning}`);
            }
            if (extraction && extraction.errors.length > 0) {
                const detail = extraction.errors
                    .map((e) => `${relPath(clean)}: ${e.message}`)
                    .join('\n');
                this.error(`[sigx:actors] actor extraction failed:\n${detail}`);
            }
            if (isClientOut(this)) {
                // NEVER serve the real module to the browser — on a failed
                // extraction (mid-edit syntax error) fall back to the last
                // good stub, else a loud refusal.
                const stub = (extraction ?? extractions.get(clean))?.clientModule;
                return {
                    code:
                        stub ??
                        `throw new Error(${JSON.stringify(
                            `[sigx:actors] could not extract ${relPath(clean)} (syntax error?) — ` +
                                `refusing to serve the actor module to the browser.`
                        )});`,
                    map: null
                };
            }
            // SSR/server environments keep the REAL module.
            return null;
        },

        async hotUpdate({ type, file, read }) {
            const key = normalizePath(file);
            if (!filter(key)) return;
            const before = extractions.get(key)?.actors.map((a) => a.type) ?? [];
            if (type === 'delete') extractions.delete(key);
            else extractInto(key, await read());
            const after = extractions.get(key)?.actors.map((a) => a.type) ?? [];
            // Deactivate every type this file defines (old and new sets):
            // state flushes through storage; the next call re-activates
            // against the freshly loaded module.
            const silo = peekDevSilo();
            if (silo) {
                for (const actorType of new Set([...before, ...after])) {
                    void silo.deactivateType(actorType).catch(() => {});
                }
            }
            const mod = this.environment.moduleGraph.getModuleById(RESOLVED_VIRTUAL_ID);
            if (mod) this.environment.moduleGraph.invalidateModule(mod);
        },

        configureServer(server) {
            // A Vite restart re-runs this with a fresh plugin instance; a
            // silo from the previous server may still be stamped. Stop it so
            // mailboxes and timers don't leak across restarts.
            let devApp: DevApp | null = null;
            const stale = peekDevSilo();
            if (stale) void stale.stop({ timeoutMs: 5_000 }).catch(() => {});

            // Create the dev silo EAGERLY (before the first document render
            // — the #304 bug class): through the SSR module runner so the
            // silo the render's actor() sees via the seam is the same module
            // family the render runs in.
            const siloReady = createDevSilo(server).catch((error: unknown) => {
                const message = error instanceof Error ? error.message : String(error);
                server.config.logger.error(
                    `[sigx:actors] could not start the dev silo — actor calls will fail: ${message}`
                );
                return null;
            });

            server.httpServer?.once('close', () => {
                void siloReady.then(async (silo) => {
                    // Stop the APP when there is one: stopping only its silo
                    // leaves `app.silo` set and the start promise cached, so
                    // the app would still look running to `createAppHandler`.
                    if (devApp) return devApp.stop({ timeoutMs: 5_000 }).catch(() => {});
                    return silo?.stop({ timeoutMs: 5_000 }).catch(() => {});
                });
            });

            const prefix = base.endsWith('/') ? base : base + '/';
            server.middlewares.use(async (req, res, next) => {
                if (!req.url?.startsWith(prefix)) return next();
                try {
                    await handleDevRequest(server, req, res, next);
                } catch (error) {
                    next(error);
                }
            });

            async function createDevSilo(devServer: ViteDevServer): Promise<DevSilo | null> {
                if (options.app) {
                    // THE unification: dev runs the very app module the
                    // production entry imports, so storage, placement, codec
                    // handlers, defaults and plugins are the same in both.
                    // Loaded through the SSR runner for module-graph identity
                    // with the render (the #304 bug class).
                    const appModule = (await devServer.ssrLoadModule(options.app)) as {
                        app?: DevApp;
                        default?: DevApp;
                    };
                    const app = appModule.app ?? appModule.default;
                    if (!app || typeof app.start !== 'function') {
                        throw new Error(
                            `${options.app} has no \`app\` (or default) export from ` +
                                'defineActorApp() for sigxActors({ app })'
                        );
                    }
                    devApp = app;
                    // Registration still comes from `virtual:sigx-actors`,
                    // which this plugin serves in dev as lazy `import()`s
                    // through the runner — so HMR keeps working, and the
                    // app-module/actor-module cycle stays broken.
                    return await app.start();
                }

                const siloModule = (await devServer.ssrLoadModule(
                    '@sigx/actors/silo'
                )) as unknown as SiloModule;
                devServer.config.logger.info(
                    '[sigx:actors] dev silo uses in-memory storage — actor state resets ' +
                        'when an actor file is edited. Pass sigxActors({ app }) to run your ' +
                        'real app config in dev.',
                    { timestamp: true }
                );
                // Lazy per-type loaders through the module runner: edits are
                // picked up because deactivateType also drops the silo's
                // resolved-definition cache.
                const actors = new Proxy(
                    {},
                    {
                        get: (_t, actorType: string | symbol) => {
                            if (typeof actorType !== 'string') return undefined;
                            const record = typeIndex().get(actorType);
                            if (!record) return undefined;
                            return () =>
                                devServer
                                    .ssrLoadModule(devSpec(record.file))
                                    .then((m) => m[record.exportName]);
                        },
                        ownKeys: () => [...typeIndex().keys()],
                        getOwnPropertyDescriptor: () => ({
                            enumerable: true,
                            configurable: true
                        })
                    }
                );
                const silo = siloModule.createSilo({ actors });
                await silo.start();
                return silo;
            }

            async function handleDevRequest(
                devServer: ViteDevServer,
                req: IncomingMessage,
                res: ServerResponse,
                next: (err?: unknown) => void
            ): Promise<void> {
                const silo = await siloReady;
                if (!silo) return next(new Error('[sigx:actors] dev silo failed to start'));
                // Through the SSR module runner for module-graph identity.
                const nodeEntry = (await devServer.ssrLoadModule(
                    '@sigx/actors/node'
                )) as unknown as typeof import('../node/index');
                if (devApp) {
                    // One handler for the public endpoint AND every
                    // plugin-contributed route, so a cluster's internal mount
                    // answers in dev exactly as it does in prod.
                    const handler = nodeEntry.createAppHandler(devApp, {
                        base,
                        origin: options.origin,
                        maxBodyBytes: options.maxBodyBytes
                    });
                    await handler(req, res, next);
                    return;
                }
                const handler = nodeEntry.createActorHandler({
                    silo,
                    base,
                    origin: options.origin,
                    maxBodyBytes: options.maxBodyBytes
                });
                await handler(req, res, next);
            }
        }
    };
}

/** The dev silo is reachable via its own seam — the runner and this plugin
 *  share `globalThis`, which is exactly why the seam is a global. */
function peekDevSilo(): DevSilo | undefined {
    return (globalThis as { __SIGX_ACTOR_SILO__?: DevSilo }).__SIGX_ACTOR_SILO__;
}

function safeRealpath(dir: string): string {
    try {
        return fs.realpathSync.native(dir);
    } catch {
        return dir;
    }
}
