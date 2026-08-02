/**
 * The shipped package must not depend on a renderer.
 *
 * `sigx` is an umbrella whose first line is
 * `import '@sigx/runtime-dom/platform'`, so importing it anywhere in `src/`
 * drags the DOM runtime in behind it. That is wrong for every non-browser
 * consumer this runtime exists to serve — a terminal dashboard, a Lynx app,
 * a headless host — none of which have a DOM, and none of which use a
 * single DOM API through this package.
 *
 * `./app` genuinely needs framework primitives, but they all live in
 * `@sigx/runtime-core`, which is what `sigx` re-exports anyway.
 *
 * This is a source-level guard rather than a bundle assertion because it
 * should fail on the import, in review, rather than after a build.
 */
import { describe, expect, it } from 'vitest';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';

// From the repo root, where vitest runs. `import.meta.url` is not a `file:`
// URL under the happy-dom environment, so it cannot be used here.
const PACKAGE = join(process.cwd(), 'packages', 'actors');
const SRC = join(PACKAGE, 'src');

/** Every `.ts` under `src/`, recursively. */
function sources(dir: string): string[] {
    const found: string[] = [];
    for (const entry of readdirSync(dir)) {
        const path = join(dir, entry);
        if (statSync(path).isDirectory()) found.push(...sources(path));
        else if (entry.endsWith('.ts') || entry.endsWith('.tsx')) found.push(path);
    }
    return found;
}

/**
 * Every way to pull in a module, not just `from '…'`.
 *
 * A SIDE-EFFECT import is the form that matters most here — `sigx`'s own
 * first line is `import '@sigx/runtime-dom/platform'`, so the exact
 * pattern that causes this problem is the one a `from`-only regex misses.
 * Dynamic and `require` forms are covered for the same reason.
 *
 * Anchoring on the import keyword rather than on the bare specifier is
 * deliberate: `src/vite/index.ts` legitimately contains
 * `plugins.find((p) => p.name === 'sigx')`, which is a plugin lookup in the
 * same Vite pipeline, not a dependency.
 */
const UMBRELLA =
    /(?:\bfrom\s*|\bimport\s*\(?\s*|\brequire\s*\(\s*)['"]sigx(?:\/[^'"]*)?['"]/g;

/** Umbrella imports in one file's source. Exported so its own coverage is testable. */
export function findUmbrellaImports(text: string): string[] {
    return [...text.matchAll(UMBRELLA)].map((m) => m[0]);
}

describe('the guard itself', () => {
    // A guard whose coverage is asserted rather than demonstrated is how the
    // gap it was written to close reopens.
    it.each([
        ["import { useAppContext } from 'sigx';", 'named import'],
        ['import { x } from "sigx";', 'double quotes'],
        ["import type { App } from 'sigx';", 'type-only import'],
        ["import { setProvided } from 'sigx/internals';", 'subpath'],
        ["import 'sigx';", 'SIDE-EFFECT — the form sigx itself uses'],
        ["import 'sigx/internals';", 'side-effect subpath'],
        ["const m = await import('sigx');", 'dynamic'],
        ["const m = require('sigx');", 'require'],
        ["export { App } from 'sigx';", 're-export'],
        ["export * from 'sigx';", 'star re-export']
    ])('catches %s (%s)', (source) => {
        expect(findUmbrellaImports(source)).toHaveLength(1);
    });

    it.each([
        ["import { x } from '@sigx/runtime-core';", 'the scoped package we DO want'],
        ["import { x } from '@sigx/runtime-core/internals';", 'its internals'],
        ["import { x } from '@sigx/actors';", 'our own scope'],
        ["plugins.find((p) => p.name === 'sigx')", 'a Vite plugin lookup, not a dependency'],
        ['const label = "sigx";', 'a plain string'],
        ["import { x } from 'sigxfoo';", 'a different package that starts the same']
    ])('does not flag %s (%s)', (source) => {
        expect(findUmbrellaImports(source)).toEqual([]);
    });
});

describe('shipped code depends on no renderer', () => {
    it('imports @sigx/runtime-core, never the sigx umbrella', () => {
        const offenders: string[] = [];
        for (const file of sources(SRC)) {
            const text = readFileSync(file, 'utf8');
            for (const found of findUmbrellaImports(text)) {
                offenders.push(`${file.slice(SRC.length + 1)}: ${found}`);
            }
        }
        // If this fails: the symbol you want is almost certainly re-exported
        // by `@sigx/runtime-core` (or `/internals`) — `sigx` does
        // `export * from '@sigx/runtime-core'`, so the type is identical.
        expect(offenders).toEqual([]);
    });

    it('does not declare sigx as a peer dependency', () => {
        const manifest = JSON.parse(
            readFileSync(join(PACKAGE, 'package.json'), 'utf8')
        ) as { peerDependencies?: Record<string, string> };
        // A peer is a demand on the consumer. Demanding a DOM renderer from
        // a terminal app is the thing this whole guard is about.
        expect(manifest.peerDependencies?.sigx).toBeUndefined();
    });
});
