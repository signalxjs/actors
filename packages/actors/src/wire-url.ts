/**
 * How an actor call becomes a URL — the WRITING half (rfc-server §4/§4.1,
 * signalxjs/core#355). `./wire-symbol` is the mirror; the two are one
 * grammar, and a change to either that forgets the other is a silent 404.
 *
 * ## The path
 *
 * The in-memory symbol is `` `${type}#${method}` `` and stays that way
 * everywhere — it is what the per-call HMAC signs (`cluster/envelope.ts`),
 * what `ServerFnInfo.symbol` reports to a policy, and what the frame
 * transport (`@sigx/actors-tcp`) carries in a frame with no
 * URL anywhere near it. Only the URL spelling changes: the separator becomes a
 * REAL path separator and encoding is PER SEGMENT, so a normal call spends no
 * `%` at all.
 *
 * ```
 * Cart#addItem          -> /_sigx/actor/Cart/addItem
 * acme/greeter#greet    -> /_sigx/actor/acme/greeter/greet
 * $live#subscribe       -> /_sigx/actor/$live/subscribe
 * $watch:Counter#read   -> /_sigx/host/$watch:Counter/read
 * ```
 *
 * A type may itself contain `/` (`acme/greeter`, the packaged-actor
 * convention the README recommends), which is why the reading half splits on
 * the LAST separator — exactly mirroring the `lastIndexOf('#')` rule the
 * runtime already uses everywhere else.
 *
 * Core restores only `%40`→`@`. We restore three, because the runtime's own
 * reserved vocabulary spends `$` and `:` (`$live`, `$watch:`, `$sigx:host`)
 * and stopping at `@` would leave `%24live/subscribe` — a strange place to
 * stop when the entire point is that the URL reads. All three are RFC 3986
 * `pchar` (`pchar = unreserved / pct-encoded / sub-delims / ":" / "@"`, and
 * `$` is a sub-delim), so they are legal literal path characters.
 *
 * The other legal sub-delims (`&`, `+`, `,`, `;`, `=`) stay escaped
 * deliberately. `+` especially: a naive query-ish parser decodes it as a
 * space, which is the same class of hazard as `%2F` in reverse. Every extra
 * unescape is decode surface bought for a case nobody has.
 *
 * ## The read query
 *
 * A declared read goes out as GET so a browser, CDN or reverse proxy can
 * answer it. Its arguments used to ride as `?args=<percent-encoded JSON>` —
 * `%5B%22`-noise for the overwhelmingly common case of a couple of scalars.
 * When EVERY argument is a simple scalar they ride as named params instead:
 *
 * ```
 * ?a0=p-9&a1=EUR
 * ```
 *
 * This grammar is core's, character for character, and is deliberately NOT
 * re-derived here: the same declared read must decode identically on
 * `/_sigx/fn` and `/_sigx/actor`. Actors' argument 0 is the actor key, so it
 * is `a0`. Strings that would read back as a number/`true`/`false`/`null`, or
 * that already start with a quote, are JSON-quoted (`a0="42"`) — the one
 * escape a normal actor read can still spend. Anything richer than scalars
 * falls back to the `args=` blob, all-or-nothing so one call never mixes the
 * two encodings and the cache key stays a pure function of the arguments.
 *
 * There is no decoder for this half anywhere in the package, and there must
 * not be: the actor endpoint delegates to `handleServerFnRequest`, which runs
 * core's `decodeReadQuery` on every GET before it ever reaches our resolver.
 */

/** `encodeURIComponent` escapes these three; all are RFC 3986 `pchar`. */
const RESTORE: Record<string, string> = { '%40': '@', '%24': '$', '%3A': ':' };
const RESTORABLE = /%(?:40|24|3A)/g;

/** One path segment. Never contains `/`: the caller has already split. */
function encodeSegment(segment: string): string {
    return encodeURIComponent(segment).replace(RESTORABLE, (escape) => RESTORE[escape]!);
}

/**
 * Symbol → path tail. Mirrors `symbolFromPath`; keep the two in step.
 *
 * A symbol with no `#` (nothing mints one, but the seam is public enough to
 * reach) is encoded as one opaque segment rather than inventing a boundary
 * the reading half would then have to guess at.
 */
export function encodeSymbolPath(symbol: string): string {
    const hash = symbol.lastIndexOf('#');
    if (hash < 0) return encodeSegment(symbol);
    const type = symbol.slice(0, hash);
    const method = symbol.slice(hash + 1);
    if (method.includes('/')) {
        // UNCONDITIONAL, not `__DEV__`: escaping it does not save the round
        // trip. `Type#a/b` encodes to `Type/a%2Fb`, the reading half decodes
        // each segment before it splits, and it then takes the LAST `/` — so
        // the symbol comes back as `Type/a#b`, a different actor, silently.
        // The one chokepoint every wire path goes through is the only place
        // this can be caught, and a misroute is not worth the bytes saved.
        throw new Error(
            `[sigx actors] actor method "${method}" must not contain "/" — it is the ` +
                `wire path separator.`
        );
    }
    return `${type.split('/').map(encodeSegment).join('/')}/${encodeSegment(method)}`;
}

// ---------------------------------------------------------------------------
// The read query — core's grammar, mirrored exactly (see the header).

/** Text a bare param would read back as a number. Deliberately strict:
 *  `007` and `+1` are NOT numbers here, so they survive as the strings they
 *  almost certainly were. */
const NUMERIC = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][-+]?\d+)?$/;

/** The three bare words that are not strings. */
const LITERALS = new Set(['true', 'false', 'null']);

/** Values that can ride as a named param at all. */
function isSimpleScalar(value: unknown): boolean {
    return (
        value === null ||
        typeof value === 'string' ||
        typeof value === 'boolean' ||
        (typeof value === 'number' && Number.isFinite(value))
    );
}

/** One argument → one param's raw text. */
function encodeScalar(value: unknown): string {
    if (typeof value !== 'string') return String(value);
    return NUMERIC.test(value) || LITERALS.has(value) || value.startsWith('"')
        ? JSON.stringify(value)
        : value;
}

/**
 * Arguments → query string (no leading `?`; empty when there are none, which
 * the endpoint already reads as `[]`).
 *
 * `encodeBlob` is passed in rather than imported so this module stays free of
 * the wire codec. The scalar test runs on the RAW arguments and `encodeBlob`
 * only on the fallback path — which is what core's client does, and matching
 * it is the whole point.
 */
export function encodeReadQuery(args: unknown[], encodeBlob: (args: unknown[]) => string): string {
    const params = new URLSearchParams();
    if (args.every(isSimpleScalar)) {
        args.forEach((arg, index) => params.set(`a${index}`, encodeScalar(arg)));
    } else {
        params.set('args', encodeBlob(args));
    }
    return params.toString();
}
