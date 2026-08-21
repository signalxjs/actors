/**
 * The plugin's two-mode contract (#116).
 *
 * HTTP mode loads no user code and holds no host, so it must not require the
 * cwd project to depend on `@sigx/actors` — that requirement is real only
 * for embedded mode, which imports the project's app module in-process. So
 * `detect` is permissive (the command group always registers), and the
 * project check lives on the verb path, where it can say WHICH mode needs
 * what instead of vanishing the command ("Unknown command 'actors'" reads
 * as a broken install).
 *
 * @vitest-environment node
 * (The --url case makes a REAL loopback request; a DOM environment's fetch
 * blocks it as cross-origin.)
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { createServer, type Server } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import plugin, { detect } from '../src/plugin';
import type { ActorsArgs, ActorsCommandContext } from '../src/commands/context';

/** A throwaway project directory, optionally with a package.json. */
function project(manifest?: unknown): string {
    const dir = mkdtempSync(join(tmpdir(), 'sigx-actors-cli-plugin-'));
    if (manifest !== undefined) {
        writeFileSync(join(dir, 'package.json'), JSON.stringify(manifest));
    }
    return dir;
}

function context(cwd: string, args: ActorsArgs): ActorsCommandContext & {
    errors: string[];
} {
    const errors: string[] = [];
    return {
        cwd,
        args,
        errors,
        logger: {
            log: vi.fn(),
            warn: vi.fn(),
            error: (msg: string) => errors.push(msg)
        }
    };
}

const run = (ctx: ActorsCommandContext): Promise<void> =>
    plugin.commands.actors.run(ctx as never) as Promise<void>;

describe('detect', () => {
    it('is permissive: true without any package.json', () => {
        // An ops box or a jump host has no manifest at all, and HTTP mode
        // is exactly the mode built for it.
        expect(detect(project())).toBe(true);
    });

    it('is permissive: true when the manifest does not depend on @sigx/actors', () => {
        // A control plane watching tenants' clusters over --url should not
        // have to lie in its manifest to unlock a code path that loads none
        // of @sigx/actors.
        expect(detect(project({ name: 'control-plane', dependencies: {} }))).toBe(true);
    });

    it('is true for a project that does depend on @sigx/actors', () => {
        expect(
            detect(project({ name: 'app', dependencies: { '@sigx/actors': '^1.0.0' } }))
        ).toBe(true);
    });
});

describe('the verb-path project check', () => {
    const exitCodeBefore = process.exitCode;
    beforeEach(() => {
        process.exitCode = undefined;
    });
    afterEach(() => {
        process.exitCode = exitCodeBefore;
    });

    it('fails embedded mode (no --url) with the two-mode message when the project lacks @sigx/actors', async () => {
        const ctx = context(project({ name: 'control-plane' }), { verb: 'stats' });
        await run(ctx);
        expect(ctx.errors.join('\n')).toMatch(
            /embedded mode needs a project that depends on @sigx\/actors/
        );
        expect(ctx.errors.join('\n')).toMatch(/--url/);
        // A usage/configuration error, matching the unknown-subcommand code —
        // not 1, which `actors health` uses to mean "not ready".
        expect(process.exitCode).toBe(2);
    });

    it('lets a declaring project through to embedded resolution', async () => {
        const ctx = context(
            project({ name: 'app', devDependencies: { '@sigx/actors': 'workspace:*' } }),
            { verb: 'stats' }
        );
        // The project gate must NOT trip; the failure it runs into instead
        // is the ordinary "no app module found" from embedded resolution.
        await expect(run(ctx)).rejects.toThrow(/no actor app module found/);
    });

    it('runs HTTP mode unconditionally when --url is given, project or not', async () => {
        // A real (tiny) ops endpoint: the strongest proof that --url needs
        // no project is a successful probe from a directory without one.
        const server: Server = createServer((req, res) => {
            if (req.url === '/_sigx/ops') {
                res.setHeader('content-type', 'application/json');
                res.end(
                    JSON.stringify({
                        v: 1,
                        at: Date.now(),
                        uptimeMs: 1234,
                        stats: { activations: 0, queued: 0, transitional: { activating: 0, deactivating: 0 } },
                        health: { live: true, ready: true, fatal: false, uptimeMs: 1234, checks: {} }
                    })
                );
                return;
            }
            res.statusCode = 404;
            res.end();
        });
        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        const address = server.address();
        const port = typeof address === 'object' && address ? address.port : 0;
        try {
            const ctx = context(project(), {
                verb: 'health',
                url: `http://127.0.0.1:${port}`
            });
            await run(ctx);
            expect(ctx.errors).toEqual([]);
            expect(process.exitCode ?? 0).toBe(0);
        } finally {
            await new Promise<void>((resolve) => server.close(() => resolve()));
        }
    });
});
