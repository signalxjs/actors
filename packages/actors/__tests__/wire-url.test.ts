/**
 * The actor URL grammar, both halves together.
 *
 * `wire-url.ts` writes it and `wire-symbol.ts` reads it; they are one grammar
 * living in two modules for a size-limit reason, and a change to either half
 * that forgets the other is a silent 404 or a silently retyped argument. So
 * the round-trip is asserted here rather than each half in isolation — the
 * same reason core's own `fn-url.test.ts` exists.
 */
import { describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import { encodeReadQuery, encodeSymbolPath } from '../src/wire-url';
import { canonicalSymbol, symbolFromPathname } from '../src/wire-symbol';

const BASE = '/_sigx/actor';

/** What the endpoint actually does: encode, mount, then read back. */
const roundTrip = (symbol: string): string =>
    symbolFromPathname(`${BASE}/${encodeSymbolPath(symbol)}`, BASE);

describe('the actor URL path grammar', () => {
    it.each([
        ['Cart#addItem', 'Cart/addItem'],
        // The packaged-actor convention: the type's own slashes are real
        // separators too, and only "the method is the last segment" tells the
        // two apart.
        ['acme/greeter#greet', 'acme/greeter/greet'],
        // The runtime's reserved mounts. `$` and `:` are RFC 3986 pchar, and
        // `%24live%23subscribe` was the single ugliest URL it emitted — on
        // every page using `useActorState(…, { live: true })`.
        ['$live#subscribe', '$live/subscribe'],
        ['$watch:Counter#read', '$watch:Counter/read'],
        ['$watch:acme/greeter#read', '$watch:acme/greeter/read'],
        ['$sigx:host#stats', '$sigx:host/stats']
    ])('%s -> %s, and back', (symbol, path) => {
        expect(encodeSymbolPath(symbol)).toBe(path);
        expect(roundTrip(symbol)).toBe(symbol);
    });

    it('spends no escape on a normal call', () => {
        // The whole point of the change. `@ . - _ ~` are pchar and survive,
        // as do `$` and `:`.
        expect(encodeSymbolPath('@acme/cart.v2#add_item~now')).toBe(
            '@acme/cart.v2/add_item~now'
        );
        expect(encodeSymbolPath('@acme/cart.v2#add_item~now')).not.toContain('%');
    });

    it('still escapes what is genuinely unsafe, and round-trips it', () => {
        // A `/` INSIDE a segment value is the one thing that must stay
        // escaped — it is the separator. `%` and non-ASCII too.
        expect(encodeSymbolPath('a%2Fb#m')).toBe('a%252Fb/m');
        expect(roundTrip('a%2Fb#m')).toBe('a%2Fb#m');
        expect(encodeSymbolPath('café#thé')).toBe('caf%C3%A9/th%C3%A9');
        expect(roundTrip('café#thé')).toBe('café#thé');
        expect(roundTrip('has space#m')).toBe('has space#m');
        // `+` and `&` stay escaped deliberately: a naive query-ish parser
        // reads `+` as a space, which is the `%2F` hazard in reverse.
        expect(encodeSymbolPath('a+b&c#m')).toBe('a%2Bb%26c/m');
        expect(roundTrip('a+b&c#m')).toBe('a+b&c#m');
    });

    it('refuses a method containing the separator, at the one chokepoint', () => {
        // Every wire path goes through the encoder, and the reading half would
        // otherwise re-split `Type/a/b` as type `Type/a`. `__DEV__` only, so
        // the production bundle pays nothing.
        expect(() => encodeSymbolPath('Type#a/b')).toThrow(/must not contain "\/"/);
    });
});

describe('canonicalSymbol', () => {
    it('accepts both spellings, because both are legitimate', () => {
        // The path form comes off a URL; the canonical form comes off a frame
        // (tcp, ws) or an in-process caller, with no URL anywhere. One
        // resolver serves both.
        expect(canonicalSymbol('Cart/addItem')).toBe('Cart#addItem');
        expect(canonicalSymbol('Cart#addItem')).toBe('Cart#addItem');
        expect(canonicalSymbol('acme/greeter/greet')).toBe('acme/greeter#greet');
    });

    it('is idempotent', () => {
        const once = canonicalSymbol('Cart/addItem');
        expect(canonicalSymbol(once)).toBe(once);
    });

    it('rejects a spelling with no boundary, so the caller 404s', () => {
        expect(canonicalSymbol('Cart')).toBe('');
        expect(canonicalSymbol('Cart/')).toBe('');
        expect(canonicalSymbol('/addItem')).toBe('');
        expect(canonicalSymbol('Cart#')).toBe('');
        expect(canonicalSymbol('#addItem')).toBe('');
        expect(canonicalSymbol('')).toBe('');
    });
});

describe('the canonical spelling is not a compatibility shim', () => {
    it('resolves `Type%23method` too, and that is load-bearing', () => {
        // Deleting this branch would break the FRAME transports, not old
        // clients: `@sigx/actors-tcp` and `@sigx/actors-ws` hand the resolver
        // `Cart#addItem` out of a frame with no URL anywhere in the picture,
        // and `resolveHostSymbol` is the same function the HTTP mounts use.
        // A cached old browser bundle keeping working is a side benefit, not
        // the reason. (The reverse direction is NOT supported: a new client
        // against an old host 404s — see the CHANGELOG's upgrade order.)
        expect(canonicalSymbol('Cart#addItem')).toBe('Cart#addItem');
        expect(symbolFromPathname('/_sigx/actor/Cart%23addItem', BASE)).toBe('Cart#addItem');
        expect(symbolFromPathname('/_sigx/host/acme%2Fgreeter%23greet', '/_sigx/host')).toBe(
            'acme/greeter#greet'
        );
    });
});

describe('symbolFromPathname', () => {
    it('is empty for a path outside the mount', () => {
        expect(symbolFromPathname('/other/Cart/addItem', BASE)).toBe('');
    });

    it('tolerates a base with or without a trailing slash', () => {
        expect(symbolFromPathname('/_sigx/actor/Cart/addItem', '/_sigx/actor/')).toBe(
            'Cart#addItem'
        );
    });

    it('throws URIError on a malformed escape, for the mount to catch', () => {
        // The internal mount answers 403 and the public one 400; neither may
        // let this surface as a masked 500.
        expect(() => symbolFromPathname('/_sigx/actor/Cart/%FF', BASE)).toThrow(URIError);
    });
});

describe('the read query grammar', () => {
    const blob = (args: unknown[]): string => JSON.stringify(args);

    it('sends all-scalar arguments as named params — arg 0 is the actor key', () => {
        expect(encodeReadQuery(['p-9', 'EUR'], blob)).toBe('a0=p-9&a1=EUR');
        expect(encodeReadQuery(['p1', 42, true, null], blob)).toBe(
            'a0=p1&a1=42&a2=true&a3=null'
        );
        expect(encodeReadQuery([], blob)).toBe('');
    });

    it('quotes only a string that would read back as something else', () => {
        expect(encodeReadQuery(['42'], blob)).toBe('a0=%2242%22');
        expect(encodeReadQuery(['true'], blob)).toBe('a0=%22true%22');
        expect(encodeReadQuery(['null'], blob)).toBe('a0=%22null%22');
        expect(encodeReadQuery(['"x'], blob)).toBe('a0=%22%5C%22x%22');
        // Deliberately strict: `007` and `+1` are not numbers, so they stay
        // the strings they almost certainly were — and spend no quote.
        expect(encodeReadQuery(['007', '+1'], blob)).toBe('a0=007&a1=%2B1');
    });

    it('falls back to the blob for anything richer, all-or-nothing', () => {
        // One non-scalar and the WHOLE call switches, so a call never mixes
        // the two encodings and the cache key stays a pure function of the
        // arguments.
        const at = new Date(1700000000000);
        expect(encodeReadQuery(['p1', at], blob)).toBe(
            `args=${encodeURIComponent(blob(['p1', at]))}`
        );
        expect(encodeReadQuery(['p1', { a: 1 }], blob)).toContain('args=');
        expect(encodeReadQuery(['p1', [1]], blob)).toContain('args=');
        expect(encodeReadQuery([undefined], blob)).toContain('args=');
        expect(encodeReadQuery([Number.NaN], blob)).toContain('args=');
        expect(encodeReadQuery([Number.POSITIVE_INFINITY], blob)).toContain('args=');
    });
});

describe('defineActor guards the type against the path grammar', () => {
    const withType = (type: string): void =>
        void defineActor({ type, allowAnonymous: true, state: () => ({}), methods: () => ({}) });

    it('refuses a dot or empty segment — `new URL()` would resolve it away', () => {
        // Not a 404 but a silent RETARGET, which is why it is refused at
        // definition time rather than caught on the wire.
        expect(() => withType('..')).toThrow(/must not contain an empty, "\." or "\.\." path/);
        expect(() => withType('acme/../greeter')).toThrow(/path segment/);
        expect(() => withType('acme/./greeter')).toThrow(/path segment/);
        expect(() => withType('acme//greeter')).toThrow(/path segment/);
        expect(() => withType('acme/')).toThrow(/path segment/);
    });

    it('still allows the packaged-actor convention', () => {
        expect(() => withType('acme/greeter')).not.toThrow();
    });
});
