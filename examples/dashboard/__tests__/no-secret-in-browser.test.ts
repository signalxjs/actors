/**
 * The example's one guarantee, enforced instead of asserted.
 *
 * `src/main.tsx` is browser code. Nothing reachable from it may mention the
 * ops secret or read `process.env` — because the whole argument of this
 * example is that the bearer token stays on the server, and an example that
 * leaked it would be teaching the opposite of what it says.
 *
 * This walks the actual import graph rather than grepping `dist/`, for two
 * reasons. It runs without a build, so CI catches a regression at the same
 * moment a human would. And it does not depend on tree-shaking: today the
 * bundler does drop an unused `OPS_SECRET` re-export, but a guarantee that
 * rests on an optimiser staying clever breaks quietly the first time somebody
 * adds a side effect, logs the config object, or re-exports it. The split
 * between `config.public.ts` and `config.server.ts` is what makes it
 * structural; this is what keeps the split honest.
 */
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const here = dirname(fileURLToPath(import.meta.url));
const ENTRY = resolve(here, '../src/main.tsx');

/**
 * Source with comments removed.
 *
 * The assertions below are about CODE. Without this they also fire on prose —
 * `config.public.ts` exists precisely to explain why it must not read
 * `process.env`, and saying so would fail a check on the word. A guard that
 * punishes you for documenting it is a guard people delete.
 *
 * The `(?<!:)` is the whole subtlety: `//` inside `http://…` is not a comment,
 * and a stripper that ate the rest of that line would quietly blind every
 * assertion after it. `__tests__` pins that case.
 */
export function stripComments(source: string): string {
    return source.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(?<!:)\/\/.*$/gm, '');
}

/** Relative-import specifiers in a module, resolved to real paths. */
function localImports(file: string, source: string): string[] {
    const found: string[] = [];
    // `from './x'` and `import './x'` — enough for this example, which has no
    // dynamic imports and no import maps.
    for (const match of source.matchAll(/(?:from|import)\s+['"](\.[^'"]*)['"]/g)) {
        const specifier = match[1]!;
        const base = resolve(dirname(file), specifier);
        // The specifier as written FIRST, then the extensions we author.
        //
        // Not `extname(base) ? [base] : [...]`: `./config.public` has an
        // "extension" of `.public` by that measure, so an extensionless
        // import with a dot in its name resolved to nothing and silently left
        // the module out of the graph. Which is precisely the failure the
        // vacuity check below exists to catch, and did.
        const candidates = [base, `${base}.ts`, `${base}.tsx`, `${base}/index.ts`];
        for (const path of candidates) {
            try {
                readFileSync(path, 'utf8');
                found.push(path);
                break;
            } catch {
                /* next candidate */
            }
        }
    }
    return found;
}

/** Every local module reachable from the browser entry, entry included. */
function browserGraph(): Map<string, string> {
    const seen = new Map<string, string>();
    const queue = [ENTRY];
    while (queue.length > 0) {
        const file = queue.pop()!;
        if (seen.has(file)) continue;
        const source = readFileSync(file, 'utf8');
        seen.set(file, stripComments(source));
        queue.push(...localImports(file, source));
    }
    return seen;
}

describe('stripComments', () => {
    it('removes both comment forms', () => {
        expect(stripComments('const a = 1; // secret\nconst b = 2;')).toContain('const b = 2;');
        expect(stripComments('const a = 1; // secret')).not.toContain('secret');
        expect(stripComments('/* secret */ const a = 1;')).not.toContain('secret');
    });

    it('does NOT treat the // in a URL as a comment', () => {
        // The failure this guards against is silent and total: eating the
        // rest of that line would blind every assertion below it, and the
        // suite would go green while checking nothing.
        const kept = stripComments("const host = 'http://127.0.0.1:5392'; const x = OPS_SECRET;");
        expect(kept).toContain('127.0.0.1:5392');
        expect(kept).toContain('OPS_SECRET');
    });
});

describe('the browser bundle cannot reach the ops secret', () => {
    const graph = browserGraph();

    it('walks a graph that actually contains something', () => {
        // A resolver that silently found nothing would make every assertion
        // below vacuous — the classic way this shape of test rots.
        expect(graph.size).toBeGreaterThanOrEqual(2);
        expect([...graph.keys()].some((file) => file.endsWith('config.public.ts'))).toBe(true);
    });

    it('never mentions the secret', () => {
        for (const [file, source] of graph) {
            expect(source, `${file} must not name the ops secret`).not.toMatch(
                /OPS_SECRET|demo-ops-secret/
            );
        }
    });

    it('never reads process.env', () => {
        // Not itself a leak, but it is how one arrives: a browser module that
        // reads the environment is a browser module somebody will put a
        // secret into.
        for (const [file, source] of graph) {
            expect(source, `${file} must not read process.env`).not.toMatch(/process\.env/);
        }
    });

    it('never names the host origin, only a same-origin path', () => {
        // The browser knows one thing: a path on its own server. An absolute
        // host URL here would mean a cross-origin request that `ops()` (which
        // sends no CORS headers) could never answer anyway.
        for (const [file, source] of graph) {
            expect(source, `${file} must not name the host origin`).not.toMatch(/OPS_HOST/);
        }
    });

    it('does not import the server config', () => {
        for (const [file, source] of graph) {
            expect(source, `${file} must not import config.server`).not.toMatch(/config\.server/);
        }
    });
});
