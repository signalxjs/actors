/**
 * `@sigx/actors-cli` — the sigx CLI plugin for observing actor silos.
 *
 * Delivered as a CLI plugin rather than its own binary for the reason
 * `@sigx/lynx-cli` is: `sigx` is already the command an app author runs, and
 * the plugin model means the actors panes can merge into ANOTHER plugin's
 * shell. A standalone binary could only ever be a separate window.
 *
 * Commands are dispatched by their first token, so the sub-verb (`top`,
 * `stats`, …) arrives as a positional rather than a nested command — which
 * is also how it reads: `sigx actors stats`.
 */
import { definePlugin, a } from '@sigx/cli/plugin';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import type { ActorsCommandContext } from './commands/context';
import { runHealth } from './commands/health';
import { runStats } from './commands/stats';

/** Sub-verbs of `sigx actors`. */
const VERBS = ['stats', 'health'] as const;
type Verb = (typeof VERBS)[number];

/**
 * Declared here and NOT exported: the inferred type of a builder literal
 * names `@sigx/args` internals that resolve through pnpm's store, which
 * declaration emit rejects as non-portable. `ActorsArgs` states the parsed
 * shape instead, and the handlers take that.
 */
const actorsArgs = {
    verb: a.positional().describe(`What to do: ${VERBS.join(' | ')}`),
    url: a.string().describe('Origin of a running silo, e.g. http://localhost:3000'),
    secret: a.string().describe('ops() bearer token (or $SIGX_OPS_SECRET)'),
    base: a.string().describe('ops() path prefix (default /_sigx/ops)'),
    app: a.string().describe('Actor app module to load in-process'),
    timeout: a.number().describe('Per-request budget in ms (default 5000)'),
    json: a.boolean().describe('Emit the raw snapshot as JSON')
};

/**
 * True when this project uses `@sigx/actors`.
 *
 * Reads the manifest rather than probing `node_modules`: a dependency the
 * author DECLARED is the signal, and a transitively-installed copy is not
 * this project's business.
 */
export function detect(cwd: string): boolean {
    const manifest = join(cwd, 'package.json');
    if (!existsSync(manifest)) return false;
    try {
        const pkg = JSON.parse(readFileSync(manifest, 'utf8')) as Record<
            string,
            Record<string, string> | undefined
        >;
        return ['dependencies', 'devDependencies', 'peerDependencies'].some(
            (field) => pkg[field]?.['@sigx/actors'] !== undefined
        );
    } catch {
        // A malformed package.json is the project's problem, not a reason
        // for the CLI to crash while merely deciding whether to offer a
        // command.
        return false;
    }
}

async function runActors(ctx: ActorsCommandContext): Promise<void> {
    const verb = (ctx.args.verb ?? 'stats') as Verb;
    if (!VERBS.includes(verb)) {
        ctx.logger.error(
            `[sigx actors] unknown subcommand "${verb}" — expected ${VERBS.join(', ')}.`
        );
        // 2 rather than 1: a usage error is not a failing probe, and
        // `actors health` uses 1 to mean "not ready".
        process.exitCode = 2;
        return;
    }
    if (verb === 'health') return runHealth(ctx);
    return runStats(ctx);
}

export default definePlugin({
    name: 'actors',
    detect,
    commands: {
        actors: {
            description: 'Observe actor silos, grains and clusters',
            args: actorsArgs,
            // The cast bridges the parser's inferred shape to the stated
            // one. They agree by construction — `ActorsArgs` mirrors the
            // builders above — and it is the only place the two meet.
            run: (ctx) => runActors(ctx as unknown as ActorsCommandContext)
        }
    }
});
