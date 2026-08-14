import { resolve, sep } from 'node:path';

/**
 * Map a request target onto a file inside `root`, or `null` if it escapes.
 *
 * `req.url` is the RAW request target. Node does not normalize it, and a
 * client that does not either — `curl --path-as-is`, or anything writing to
 * the socket directly — can send `/../package.json`. Handing that straight to
 * `join`/`resolve` collapses the `..` for you and reads the file, so the
 * order here matters: decode first, resolve second, and only then check that
 * the result is still inside the directory you meant to serve.
 *
 * The return value is absolute. `root` is normalized first, so a caller
 * passing a relative path or a trailing separator gets the same answer —
 * the containment check below compares against `root + sep`, and `/dist/`
 * would otherwise reject every file genuinely inside it.
 */
export function resolveStatic(root: string, target: string): string | null {
    const base = resolve(root);
    let pathname: string;
    try {
        // Not every request target parses: `//` and absolute forms like
        // `http://[` both throw here.
        ({ pathname } = new URL(target, 'http://localhost'));
    } catch {
        return null;
    }
    if (pathname.endsWith('/')) pathname += 'index.html';

    let decoded: string;
    try {
        decoded = decodeURIComponent(pathname);
    } catch {
        return null; // half-encoded input — a URIError, not a file
    }

    const file = resolve(base, '.' + decoded);
    // `=== base` covers the root itself; the `+ sep` stops `/dist-other`
    // passing a naive prefix check against `/dist`.
    return file === base || file.startsWith(base + sep) ? file : null;
}
