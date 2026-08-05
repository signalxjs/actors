/**
 * How a URL becomes an actor call — the READING half of `./wire-url`. See
 * that file's header for the grammar and why it looks the way it does.
 *
 * Separate module for one reason: `/client` is size-limited (2 KB for the
 * no-router tree-shake guard) and imports only the writing half, while
 * `/server` imports only this one. Together they would become a single chunk
 * shared by both entries, and each would pay for logic it never runs.
 * `__tests__/wire-url.test.ts` round-trips the two together, which is what
 * actually keeps them in step.
 */

/**
 * Wire spelling → canonical symbol.
 *
 * Both spellings arrive here and BOTH are legitimate — this is not a
 * compatibility shim. A URL-shaped transport delivers the path form
 * (`Cart/addItem`), while the frame transports (`@sigx/actors-tcp`,
 * `@sigx/actors-ws`) and every in-process caller hand over the canonical form
 * (`Cart#addItem`) with no URL involved at any point. One resolver serves
 * both, so one of them has to normalize.
 *
 * `#` present ⇒ already canonical, and unambiguously so: `defineActor`
 * refuses a `#` in a type, and the writing half never emits a literal one. A
 * single `String.includes` is what a frame transport pays.
 *
 * Returns `''` for a spelling with no boundary at all (a bare `Cart`, or a
 * trailing separator) — the caller 404s, exactly as the old
 * `hash <= 0 || hash === length - 1` guard did.
 */
export function canonicalSymbol(wire: string): string {
    if (wire.includes('#')) {
        const hash = wire.lastIndexOf('#');
        return hash <= 0 || hash === wire.length - 1 ? '' : wire;
    }
    const slash = wire.lastIndexOf('/');
    if (slash <= 0 || slash === wire.length - 1) return '';
    return `${wire.slice(0, slash)}#${wire.slice(slash + 1)}`;
}

/**
 * A raw pathname → canonical symbol, percent-escapes and all.
 *
 * This exists because the internal mount's HMAC pre-check must compute the
 * EXACT string core's `decodeFnPath` will hand the resolver a moment later,
 * and core does not export it (`fn-url-decode` is not in `@sigx/server`'s
 * `exports` map). The two must agree or every secured call 403s, so the
 * per-segment decode is reproduced here rather than approximated — the old
 * approximation (decode the whole pathname, take the last segment) is
 * precisely what broke slash-containing types.
 *
 * Throws `URIError` on a malformed escape (`%FF`), like `decodeURIComponent`
 * itself; the mount catches it and answers rather than letting it escape.
 */
export function symbolFromPathname(pathname: string, base: string): string {
    const prefix = base.endsWith('/') ? base : `${base}/`;
    if (!pathname.startsWith(prefix)) return '';
    return canonicalSymbol(
        pathname
            .slice(prefix.length)
            .split('/')
            .map((segment) => decodeURIComponent(segment))
            .join('/')
    );
}
