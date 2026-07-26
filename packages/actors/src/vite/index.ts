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

export { extractActors, type ActorExtraction, type ExtractedActor } from './extract';

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
    /** Dev: root-relative module whose `storage` export feeds `createSilo`
     *  (e.g. '/src/actor-storage.ts'). Omit = in-memory (state resets on
     *  actor-file edits — a one-time dev log says so). */
    storage?: string;
    /** Dev: root-relative module whose `guard` export is the wire-level
     *  endpoint backstop (same posture as sigxServer's `guard`). */
    guard?: string;
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
interface DevSilo {
    start(): Promise<void>;
    stop(opts?: { timeoutMs?: number }): Promise<void>;
    deactivateType(type: string): Promise<void>;
}

export function sigxActors(options: SigxActorsOptions = {}): Plugin {
    const filter = createFilter(
        options.include ?? DEFAULT_INCLUDE,
        options.exclude ?? DEFAULT_EXCLUDE
    );
    const base = options.base ?? DEFAULT_BASE;
    const endpoint = options.endpoint ?? base;
    const requireGuards = options.requireGuards ?? true;

    let root = process.cwd();
    let isServe = false;
    let bundledServerBuild = false;
    /** Latest extraction per absolute module path. */
    const extractions = new Map<string, ActorExtraction & { warnings: string[] }>();

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
                requireGuards
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
                if (
                    isClientOut(this) &&
                    mayDefineActors(code) &&
                    !isGeneratedClientModule(code)
                ) {
                    this.warn(
                        `[sigx:actors] ${relPath(clean)} calls defineActor but does not match ` +
                            `the actor-module pattern (*.actor.ts) — it will NOT be swapped for ` +
                            `the browser. Move the definition into a *.actor.ts module.`
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
                void siloReady.then((silo) => silo?.stop({ timeoutMs: 5_000 }).catch(() => {}));
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
                const siloModule = (await devServer.ssrLoadModule(
                    '@sigx/actors/silo'
                )) as unknown as SiloModule;
                let storage: unknown;
                if (options.storage) {
                    const storageModule = await devServer.ssrLoadModule(options.storage);
                    storage = storageModule.storage;
                    if (!storage) {
                        throw new Error(
                            `${options.storage} has no \`storage\` export for sigxActors({ storage })`
                        );
                    }
                } else {
                    devServer.config.logger.info(
                        '[sigx:actors] dev silo uses in-memory storage — actor state resets ' +
                            'when an actor file is edited. Pass sigxActors({ storage }) to keep it.',
                        { timestamp: true }
                    );
                }
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
                const silo = siloModule.createSilo({ actors, storage });
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
                const guardModule = options.guard
                    ? await devServer.ssrLoadModule(options.guard)
                    : undefined;
                const handler = nodeEntry.createActorHandler({
                    silo: silo as never,
                    base,
                    origin: options.origin,
                    maxBodyBytes: options.maxBodyBytes,
                    guard: guardModule?.guard as never
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
