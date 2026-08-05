/**
 * The routing token, in isolation: what it looks like, when it is emitted,
 * and how it survives keys that contain path separators.
 *
 * The URL-shape half — that a tokenized request still reaches the same actor
 * — lives in `route-wire.test.ts`, against the real endpoint.
 */
import { describe, expect, it } from 'vitest';
import { actorId } from '../src/types';
import {
    ACTOR_ROUTE_SEGMENT,
    actorRouteToken,
    hashRouteToken,
    routePath,
    routeTokenFor
} from '../src/route';
import { fnv1a } from '../src/hash';

const BASE = '/_sigx/actor';

describe('the routing token', () => {
    it('is opaque, fixed-width, and stable', () => {
        const token = hashRouteToken('Cart', 'cart-123');
        expect(token).toMatch(/^[0-9a-z]{7}$/);
        expect(hashRouteToken('Cart', 'cart-123')).toBe(token);
        // Opaque: the key does not appear in it.
        expect(token).not.toContain('cart');
    });

    it('pins its exact format — changing it re-homes every actor', () => {
        // Literals, deliberately NOT inline snapshots: `vitest -u` would
        // silently rewrite a snapshot, and these values ARE the wire contract
        // an operator's LB config hashes. Changing them costs a one-deploy
        // locality dip, so it must be a deliberate edit, never a refactor.
        expect(hashRouteToken('Cart', 'cart-123')).toBe('1xadm0a');
        expect(hashRouteToken('Counter', 'a')).toBe('16kide0');
        expect(hashRouteToken('$live', 'x')).toBe('1nuzeap');
    });

    it('separates type from key, so two actors cannot collide by concatenation', () => {
        // Without the NUL separator, ('ab','c') and ('a','bc') would collide.
        expect(hashRouteToken('ab', 'c')).not.toBe(hashRouteToken('a', 'bc'));
        expect(hashRouteToken('Cart', 'a')).not.toBe(hashRouteToken('Cart', 'b'));
        expect(hashRouteToken('Cart', 'a')).not.toBe(hashRouteToken('Basket', 'a'));
    });

    it('hashes exactly what actorId() produces, so the two cannot drift', () => {
        for (const [type, key] of [
            ['Cart', 'c1'],
            ['A/B', 'k:2'],
            ['Café', 'ключ']
        ] as const) {
            expect(hashRouteToken(type, key)).toBe(
                fnv1a(actorId({ type, key })).toString(36).padStart(7, '0')
            );
        }
    });
});

describe('routeTokenFor', () => {
    it("'key' mode returns the raw key, for readable routing", () => {
        expect(routeTokenFor('key', 'Cart', 'cart-123')).toBe('cart-123');
    });

    it("'none' emits nothing", () => {
        expect(routeTokenFor('none', 'Cart', 'cart-123')).toBeNull();
    });

    it('never tokenizes a $-reserved type, in any mode', () => {
        // $live#subscribe is ONE response fanning out to MANY actors: there
        // is no single token and no single owner. defineActor refuses
        // $-prefixed types, so this covers every future reserved mount too.
        expect(routeTokenFor('hash', '$live', 'x')).toBeNull();
        expect(routeTokenFor('key', '$live', 'x')).toBeNull();
        expect(routeTokenFor(() => 'forced', '$live', 'x')).toBeNull();
    });

    it('lets a custom function opt out per actor', () => {
        expect(routeTokenFor(() => null, 'Cart', 'c1')).toBeNull();
        expect(routeTokenFor(() => '', 'Cart', 'c1')).toBeNull();
        expect(routeTokenFor((ref) => `t-${ref.key}`, 'Cart', 'c1')).toBe('t-c1');
    });

    it('emits no token for an empty key in \'key\' mode', () => {
        // An empty segment would collapse the path and shift the symbol.
        expect(routeTokenFor('key', 'Cart', '')).toBeNull();
    });
});

describe('routePath', () => {
    it('puts the token BEFORE the symbol, which spans the rest of the path', () => {
        // Load-bearing: @sigx/server reads everything after the base as the
        // symbol, and the symbol's own separator is now a real `/`. The token
        // therefore has to precede it — appended after, it would be read as
        // part of the symbol.
        expect(routePath(BASE, 'abc123', 'Cart#add')).toBe(
            `${BASE}/${ACTOR_ROUTE_SEGMENT}/abc123/Cart/add`
        );
    });

    it('falls back to the bare URL with no token', () => {
        expect(routePath(BASE, null, 'Cart#add')).toBe(`${BASE}/Cart/add`);
    });

    it('tolerates a trailing slash on the base', () => {
        expect(routePath(`${BASE}/`, 'abc', 'Cart#add')).toBe(`${BASE}/r/abc/Cart/add`);
        expect(routePath(`${BASE}/`, null, 'Cart#add')).toBe(`${BASE}/Cart/add`);
    });

    it('encodes a key containing a path separator', () => {
        // The TOKEN keeps its percent-escape while the symbol sheds one: the
        // token must stay exactly one segment (both carriers transmit these
        // same bytes, so an LB hashing the path and one hashing the header
        // have to agree), whereas the symbol's slashes ARE its structure.
        expect(routePath(BASE, 'a/b', 'Cart#add')).toBe(`${BASE}/r/a%2Fb/Cart/add`);
    });

    it('encodes non-ASCII', () => {
        expect(routePath(BASE, 'café', 'Cart#add')).toBe(`${BASE}/r/caf%C3%A9/Cart/add`);
    });

    it('encodes a CUSTOM function\'s result too, so it cannot smuggle a segment', () => {
        // If `x/y` went in raw it would become two segments, and the server
        // would read `y/Cart/add` as the symbol.
        const token = routeTokenFor(() => 'x/y', 'Cart', 'c1');
        const url = routePath(BASE, token, 'Cart#add');
        expect(url).toBe(`${BASE}/r/x%2Fy/Cart/add`);
        expect(url.split('/')).toHaveLength(7);
    });

    it('keeps a type that contains a slash intact, and splits on the LAST one', () => {
        // `acme/greeter` is the packaged-actor convention the README
        // recommends; its slashes and the symbol separator are the same
        // character on the wire, and only "the method is the last segment"
        // tells them apart.
        expect(routePath(BASE, null, 'acme/greeter#greet')).toBe(`${BASE}/acme/greeter/greet`);
    });

    it('spends no escape on the runtime\'s own reserved symbols', () => {
        // `$` and `:` are RFC 3986 pchar. `%24live%23subscribe` was the single
        // ugliest URL the runtime emitted, and it is on every page that calls
        // `useActorState(…, { live: true })`.
        expect(routePath(BASE, null, '$live#subscribe')).toBe(`${BASE}/$live/subscribe`);
    });
});

describe('actorRouteToken', () => {
    const req = (url: string, headers?: Record<string, string>): Request =>
        new Request(`http://host.test${url}`, { method: 'POST', headers });

    it('round-trips the path segment', () => {
        expect(actorRouteToken(req(`${BASE}/r/abc123/Cart%23add`))).toBe('abc123');
    });

    it('decodes an escaped token', () => {
        expect(actorRouteToken(req(`${BASE}/r/a%2Fb/Cart%23add`))).toBe('a/b');
        expect(actorRouteToken(req(`${BASE}/r/caf%C3%A9/Cart%23add`))).toBe('café');
    });

    it('reads the header when the path carries no token', () => {
        expect(
            actorRouteToken(req(`${BASE}/Cart%23add`, { 'x-sigx-actor-route': 'abc' }))
        ).toBe('abc');
    });

    it('prefers the path — the carrier a proxy cannot silently strip', () => {
        expect(
            actorRouteToken(
                req(`${BASE}/r/frompath/Cart%23add`, { 'x-sigx-actor-route': 'fromheader' })
            )
        ).toBe('frompath');
    });

    it('is null for a bare URL', () => {
        expect(actorRouteToken(req(`${BASE}/Cart%23add`))).toBeNull();
    });

    it('is null, not a throw, for malformed percent-encoding', () => {
        // The token is a hint; a broken one is absent, never fatal.
        expect(actorRouteToken(req(`${BASE}/r/%E0%A4%A/Cart%23add`))).toBeNull();
        // …in EITHER carrier.
        expect(
            actorRouteToken(req(`${BASE}/Cart%23add`, { 'x-sigx-actor-route': '%E0%A4%A' }))
        ).toBeNull();
    });

    it('decodes the header, so both carriers report ONE form', () => {
        // Both are encoded on the way out, so both must be decoded on the
        // way in — otherwise an adapter sees `a/b` from the path and
        // `a%2Fb` from the header for the very same call.
        const viaPath = actorRouteToken(req(`${BASE}/r/a%2Fb/Cart%23add`));
        const viaHeader = actorRouteToken(
            req(`${BASE}/Cart%23add`, { 'x-sigx-actor-route': 'a%2Fb' })
        );
        expect(viaPath).toBe('a/b');
        expect(viaHeader).toBe('a/b');
    });

    it('does not mistake a lone /r/ for a token', () => {
        // No symbol follows, so there is no token segment to read.
        expect(actorRouteToken(req(`${BASE}/r/onlyone`))).toBeNull();
    });

    it('honours a custom base', () => {
        expect(actorRouteToken(req('/api/r/abc/Cart%23add'), '/api')).toBe('abc');
        // …and does not read a token minted under a different base.
        expect(actorRouteToken(req(`${BASE}/r/abc/Cart%23add`), '/api')).toBeNull();
    });
});
