import { resolve, sep } from 'node:path';
import { describe, expect, it } from 'vitest';
import { resolveStatic } from '../src/static.ts';

const ROOT = resolve('/srv/app/dist');
const inside = (file: string | null) =>
    file === null || file === ROOT || file.startsWith(ROOT + sep);

describe('resolveStatic', () => {
    it('resolves an ordinary asset', () => {
        expect(resolveStatic(ROOT, '/assets/main.js')).toBe(
            ROOT + sep + 'assets' + sep + 'main.js'
        );
    });

    it('serves index.html for a directory target', () => {
        expect(resolveStatic(ROOT, '/')).toBe(ROOT + sep + 'index.html');
    });

    it('keeps a query string out of the path', () => {
        expect(resolveStatic(ROOT, '/index.html?counter=b')).toBe(ROOT + sep + 'index.html');
    });

    /**
     * The bug this file exists for. `req.url` is the RAW request target —
     * `curl --path-as-is`, or anything writing to the socket directly, sends
     * these verbatim. The previous `join('dist', req.url)` collapsed the `..`
     * and read the file: `/../package.json` returned the package manifest and
     * `/../../../AGENTS.md` reached the repo root.
     *
     * The contract is not "every one of these is null" — the WHATWG URL
     * parser normalizes bare `../` segments away, so most resolve to a
     * harmless miss INSIDE the root. The contract is that none of them ever
     * names a file outside it.
     */
    it.each([
        '/../package.json',
        '/../../../AGENTS.md',
        '/assets/../../package.json',
        '/%2e%2e/package.json',
        '/..%2fpackage.json',
        '/..%2F..%2Fpackage.json',
        '/assets/..%2f..%2fpackage.json',
        '/../dist-other/secret.js',
        '//evil.example.com/x',
        '/./../../package.json'
    ])('never escapes the root: %s', (target) => {
        expect(inside(resolveStatic(ROOT, target))).toBe(true);
    });

    it('rejects the percent-encoded separator outright', () => {
        // `%2f` is NOT decoded by the URL parser, so this is the one shape
        // that still carries a live `..` into the path — and it must be null,
        // not merely contained.
        expect(resolveStatic(ROOT, '/..%2fpackage.json')).toBeNull();
    });

    it('refuses a malformed percent-encoding rather than throwing', () => {
        expect(resolveStatic(ROOT, '/%E0%A4%A')).toBeNull();
    });

    it('refuses an unparseable request target', () => {
        expect(resolveStatic(ROOT, 'http://[')).toBeNull();
    });

    it('still resolves a name that merely shares the root prefix', () => {
        expect(resolveStatic(ROOT, '/distraction.js')).toBe(ROOT + sep + 'distraction.js');
    });

    it('normalizes the root, so a trailing separator is not a footgun', () => {
        // The containment check compares against `root + sep`. Were the root
        // taken verbatim, `/dist/` would produce `/dist//` and reject every
        // file genuinely inside it — failing closed, but on valid requests.
        expect(resolveStatic(ROOT + sep, '/assets/main.js')).toBe(
            ROOT + sep + 'assets' + sep + 'main.js'
        );
        expect(resolveStatic(ROOT + sep, '/..%2fpackage.json')).toBeNull();
    });
});
