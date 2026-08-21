/**
 * `@sigx/actors-cli` — the sigx CLI plugin for observing actor hosts.
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
import { runTop } from './commands/top';

/** Sub-verbs of `sigx actors`. */
const VERBS = ['top', 'stats', 'health'] as const;
type Verb = (typeof VERBS)[number];

/**
 * Declared here and NOT exported: the inferred type of a builder literal
 * names `@sigx/args` internals that resolve through pnpm's store, which
 * declaration emit rejects as non-portable. `ActorsArgs` states the parsed
 * shape instead, and the handlers take that.
 */
const actorsArgs = {
    verb: a.positional().describe(`What to do: ${VERBS.join(' | ')}`),
    url: a.string().describe('Origin of a running host, e.g. http://localhost:3000'),
    secret: a.string().describe('ops() bearer token (or $SIGX_OPS_SECRET)'),
    base: a.string().describe('ops() path prefix (default /_sigx/ops)'),
    app: a.string().describe('Actor app module to load in-process'),
    timeout: a.number().describe('Per-request budget in ms (default 5000)'),
    json: a.boolean().describe('Emit the raw snapshot as JSON'),
    interval: a.number().describe('Dashboard poll interval in ms (default 1000)')
};

/**
 * Permissive on purpose (#116): the command group registers wherever the
 * plugin is installed, and the project requirement moved to the verb path.
 *
 * Only EMBEDDED mode needs a project that depends on `@sigx/actors` — it
 * imports the app module in-process. HTTP mode (`--url`) loads no user code
 * and holds no host; it is the mode built for an ops box, a control plane
 * watching tenants' clusters, or a CI probe — places that are not actor
 * apps. Gating registration on the manifest made those say
 * `Unknown command 'actors'`, which reads as a broken install, and pushed
 * users into declaring a dependency they never load just to satisfy the
 * check. Registering always keeps discovery honest and lets the verb say
 * which mode needs what.
 */
export function detect(_cwd: string): boolean {
    return true;
}

/**
 * True when this project declares `@sigx/actors` — the embedded-mode
 * requirement.
 *
 * Reads the manifest rather than probing `node_modules`: a dependency the
 * author DECLARED is the signal, and a transitively-installed copy is not
 * this project's business.
 */
export function projectDependsOnActors(cwd: string): boolean {
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
        // for the CLI to crash while merely deciding whether a mode is
        // available.
        return false;
    }
}

async function runActors(ctx: ActorsCommandContext): Promise<void> {
    // `top` is the default: bare `sigx actors` should open the dashboard,
    // which is the thing this command exists for.
    const verb = (ctx.args.verb ?? 'top') as Verb;
    if (!VERBS.includes(verb)) {
        ctx.logger.error(
            `[sigx actors] unknown subcommand "${verb}" — expected ${VERBS.join(', ')}.`
        );
        // 2 rather than 1: a usage error is not a failing probe, and
        // `actors health` uses 1 to mean "not ready".
        process.exitCode = 2;
        return;
    }
    // With --url, HTTP mode runs unconditionally — the URL is an
    // unambiguous statement of intent, and HTTP mode has no project
    // requirement to check. Without it we are about to import the project's
    // app module in-process, and THAT needs the dependency.
    if (!ctx.args.url && !projectDependsOnActors(ctx.cwd)) {
        ctx.logger.error(
            '[sigx actors] embedded mode needs a project that depends on @sigx/actors; ' +
                'pass --url <origin> to watch a running host instead.'
        );
        // A configuration/usage error, like the unknown-subcommand case —
        // not 1, which `actors health` uses to mean "not ready".
        process.exitCode = 2;
        return;
    }
    if (verb === 'health') return runHealth(ctx);
    if (verb === 'stats') return runStats(ctx);
    return runTop(ctx);
}

export default definePlugin({
    name: 'actors',
    detect,
    commands: {
        actors: {
            description: 'Observe actor hosts, actors and clusters (top | stats | health)',
            args: actorsArgs,
            // The cast bridges the parser's inferred shape to the stated
            // one. They agree by construction — `ActorsArgs` mirrors the
            // builders above — and it is the only place the two meet.
            run: (ctx) => runActors(ctx as unknown as ActorsCommandContext)
        }
    },
    /**
     * What we contribute to ANOTHER plugin's shell — a `sigx dev` running a
     * sigx app, say.
     *
     * Deliberately a chip and a pointer, not a live panel. Rendering one
     * would mean opening a source, and the only zero-config source starts a
     * host: it joins membership, claims actors and ticks reminders. Doing
     * that as a side effect of somebody else's dev server is a genuinely
     * bad surprise, and "the monitor quietly became a cluster member" is
     * not a sentence anyone should have to debug.
     *
     * So this says actors are here and how to look at them, and `top` — an
     * explicit command — is what opens a source.
     */
    tui: {
        status: () => [{ label: 'actors', value: 'ready', tone: 'dim' }],
        commands: [
            {
                name: '/actors',
                description: 'how to open the actors dashboard',
                run: (shell) => {
                    shell.say('sigx actors top                    — dashboard (loads this project)');
                    shell.say('sigx actors top --url http://…     — dashboard against a running host');
                    shell.say('sigx actors stats --json           — one snapshot, for piping');
                    shell.say('sigx actors health                 — probe; exit code is the answer');
                }
            }
        ]
    }
});
