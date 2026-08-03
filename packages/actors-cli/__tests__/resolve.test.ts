/**
 * Resolving `--app` to a module specifier.
 *
 * Small surface, but the failure mode is bad: every wrong answer here shows
 * up as "your actor app is missing" rather than "that flag was not
 * understood".
 */
import { describe, expect, it } from 'vitest';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { resolveAppModule } from '../src/resolve';

/** A throwaway project directory with an app module at `relative`. */
function project(relative?: string): string {
    const dir = mkdtempSync(join(tmpdir(), 'sigx-actors-cli-'));
    if (relative) writeFileSync(join(dir, relative), 'export const app = {};');
    return dir;
}

describe('resolveAppModule', () => {
    it('finds the conventional module with no flag at all', () => {
        const dir = project('actors.app.ts');
        expect(resolveAppModule(dir)).toBe(pathToFileURL(join(dir, 'actors.app.ts')).href);
    });

    it('accepts a relative path', () => {
        const dir = project('actors.app.ts');
        expect(resolveAppModule(dir, 'actors.app.ts')).toBe(
            pathToFileURL(join(dir, 'actors.app.ts')).href
        );
    });

    it('accepts an absolute path', () => {
        const dir = project('actors.app.ts');
        const path = join(dir, 'actors.app.ts');
        expect(resolveAppModule(dir, path)).toBe(pathToFileURL(path).href);
    });

    it('passes a file: URL through unchanged', () => {
        // It is already the shape this function returns, and it is what a
        // caller copying the value back in would naturally pass. Treating
        // it as a relative path resolves it under cwd and then fails
        // existsSync — which reads as "your module is missing".
        const dir = project('actors.app.ts');
        const url = pathToFileURL(join(dir, 'actors.app.ts')).href;
        expect(resolveAppModule(dir, url)).toBe(url);
        // Even for a path that does not exist on disk: a file: URL may name
        // something only the loader can resolve.
        expect(resolveAppModule(dir, 'file:///nowhere/app.ts')).toBe('file:///nowhere/app.ts');
    });

    it('names the path it looked at when --app is wrong', () => {
        const dir = project();
        expect(() => resolveAppModule(dir, 'nope.ts')).toThrow(/does not exist \(looked at/);
    });

    it('lists the candidates and suggests --url when nothing is found', () => {
        const dir = project();
        expect(() => resolveAppModule(dir)).toThrow(/no actor app module found/);
        expect(() => resolveAppModule(dir)).toThrow(/--url/);
    });
});
