/**
 * Turning command flags into a `MonitorSource` — the one place that decides
 * between embedded and HTTP mode, so every command agrees.
 */
import { existsSync } from 'node:fs';
import { isAbsolute, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import type { ActorsArgs } from './commands/context';
import { embeddedSource } from './source/embedded';
import { httpSource } from './source/http';
import type { MonitorSource } from './source/types';

/**
 * Where an actor app module conventionally lives. Matched in order, so the
 * examples in this repo (`src/actors.app.ts`) work with no flag at all.
 */
const APP_CANDIDATES = [
    'src/actors.app.ts',
    'src/actors.app.js',
    'actors.app.ts',
    'actors.app.js',
    'src/actors/app.ts'
];

/**
 * `--url` wins; otherwise load the project's app in-process.
 *
 * The precedence is deliberate and not merely first-match: passing a URL is
 * an explicit statement about WHICH host you mean, and silently preferring
 * a local module over it would monitor the wrong process while looking
 * correct.
 */
export async function resolveSource(cwd: string, flags: ActorsArgs): Promise<MonitorSource> {
    if (flags.url) {
        return httpSource({
            url: flags.url,
            // The flag wins, but the environment is how a secret should
            // normally arrive — it does not end up in shell history.
            secret: flags.secret ?? process.env.SIGX_OPS_SECRET,
            base: flags.base,
            timeoutMs: flags.timeout
        });
    }

    const module = resolveAppModule(cwd, flags.app);
    return embeddedSource({ module });
}

/** Absolute file: URL of the app module, or a thrown explanation. */
export function resolveAppModule(cwd: string, app?: string): string {
    if (app) {
        // A `file:` URL is already the thing this function returns, and it
        // is what a caller copying the value back in would naturally pass.
        // Treating it as a relative path resolves it under `cwd` and then
        // fails `existsSync`, which reads as "your module is missing".
        if (app.startsWith('file:')) return app;
        const path = isAbsolute(app) ? app : resolve(cwd, app);
        if (!existsSync(path)) {
            throw new Error(`[sigx actors] --app "${app}" does not exist (looked at ${path}).`);
        }
        return pathToFileURL(path).href;
    }
    for (const candidate of APP_CANDIDATES) {
        const path = join(cwd, candidate);
        if (existsSync(path)) return pathToFileURL(path).href;
    }
    throw new Error(
        '[sigx actors] no actor app module found. Looked for ' +
            `${APP_CANDIDATES.join(', ')} under ${cwd}.\n` +
            'Pass --app <path> to name it, or --url <origin> to watch a running host ' +
            'over its ops endpoint instead.'
    );
}
