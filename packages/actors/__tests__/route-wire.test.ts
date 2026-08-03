/**
 * The routing token against the REAL endpoint.
 *
 * The whole wire change rests on one property: `@sigx/server` decodes the
 * actor symbol as the LAST path segment, so a token inserted as a MIDDLE
 * segment is invisible to it. That is what makes this a routing hint an
 * intermediary can read rather than a protocol change the server must
 * understand — and it is what the first test here pins.
 *
 * The server never validates the token. A garbage one must still dispatch,
 * because routing is an optimization and never load-bearing for correctness.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { actor, defineActor } from '@sigx/actors';
import { createHost, type Host } from '@sigx/actors/host';
import {
    ACTOR_ROUTE_HEADER,
    actorRouteToken,
    handleActorRequest,
    matchesActorRequest
} from '@sigx/actors/server';
import {
    __actorRef,
    configureActors,
    hashRouteToken,
    type ActorRouteToken
} from '@sigx/actors/client';

const ENDPOINT = 'http://actors.test/_sigx/actor';
const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const cart = defineActor({
    type: 'Cart',
    state: () => ({ items: [] as string[] }),
    methods: (ctx) => ({
        async add(name: string) {
            ctx.state.items.push(name);
            await ctx.save();
            return ctx.state.items.length;
        },
        async total() {
            return ctx.state.items.length;
        }
    }),
    streams: () => ({
        async *countTo(to: number) {
            for (let i = 1; i <= to; i++) yield i;
        }
    })
});

const CartRef = __actorRef('Cart', ENDPOINT, ['countTo']) as typeof cart;

/** Every URL and route header the client produced, in order. */
interface Seen {
    url: string;
    header: string | null;
    /** What the SERVER would read back off the request. */
    token: string | null;
}

function wireHost(route?: ActorRouteToken): { host: Host; seen: Seen[] } {
    const host = createHost({ actors: [cart], defaults: quiet });
    const seen: Seen[] = [];
    configureActors({
        endpoint: ENDPOINT,
        ...(route === undefined ? {} : { route }),
        fetch: async (input, init) => {
            const request = new Request(input, init);
            seen.push({
                url: String(request.url),
                header: request.headers.get(ACTOR_ROUTE_HEADER),
                token: actorRouteToken(request)
            });
            // The mount still recognizes the request — the token changed the
            // URL but not what the endpoint matches on.
            expect(matchesActorRequest(request)).toBe(true);
            return await handleActorRequest(request, { host, origin: false });
        }
    });
    return { host, seen };
}

afterEach(() => configureActors(null));

describe('the routing token on the wire', () => {
    it('a tokenized call reaches the same actor as a bare one', async () => {
        // THE load-bearing test. If the token ever stopped being a middle
        // segment, the server would read it as the symbol and 404.
        const { seen } = wireHost();
        await expect(actor(CartRef, 'r1').add('socks')).resolves.toBe(1);
        await expect(actor(CartRef, 'r1').total()).resolves.toBe(1);

        expect(seen[0].url).toBe(`${ENDPOINT}/r/${hashRouteToken('Cart', 'r1')}/Cart%23add`);
        expect(seen[0].url.slice(seen[0].url.lastIndexOf('/') + 1)).toBe('Cart%23add');
    });

    it('is byte-identical in effect to the bare URL', async () => {
        const tokenized = wireHost();
        const withToken = await actor(CartRef, 'same').add('a');
        configureActors(null);

        const bare = wireHost('none');
        const withoutToken = await actor(CartRef, 'same').add('a');

        expect(withToken).toBe(withoutToken);
        expect(tokenized.seen[0].url).toContain('/r/');
        expect(bare.seen[0].url).toBe(`${ENDPOINT}/Cart%23add`);
    });

    it('sends the SAME token in both carriers', async () => {
        const { seen } = wireHost();
        await actor(CartRef, 'r2').add('a');
        // A path segment a mesh cannot silently strip, plus a header for
        // Envoy — which has no path-substring hash policy at all.
        expect(seen[0].header).toBe(hashRouteToken('Cart', 'r2'));
        expect(seen[0].url).toContain(`/r/${seen[0].header}/`);
        expect(seen[0].token).toBe(seen[0].header);
    });

    it('gives one actor ONE token across every method and stream', async () => {
        // The locality property itself: an LB hashing this sends every call
        // for `r3` to one host, whatever method it names.
        const { seen } = wireHost();
        await actor(CartRef, 'r3').add('a');
        await actor(CartRef, 'r3').total();
        for await (const _ of actor(CartRef, 'r3').countTo(2)) void _;

        expect(seen).toHaveLength(3);
        expect(new Set(seen.map((s) => s.token)).size).toBe(1);
        // …and a different actor gets a different one.
        await actor(CartRef, 'r4').total();
        expect(seen[3].token).not.toBe(seen[0].token);
    });

    it('dispatches a garbage token exactly as if it were valid', async () => {
        // Pins "the server never validates". A stale or hostile token is a
        // wrong ROUTE, never a wrong ANSWER.
        const host = createHost({ actors: [cart], defaults: quiet });
        const request = new Request(`${ENDPOINT}/r/zzzzzzz/Cart%23add`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args: ['r5', 'sock'] })
        });
        const response = await handleActorRequest(request, { host, origin: false });
        expect(response.status).toBe(200);
        await expect(response.json()).resolves.toEqual({ data: 1 });
        await host.stop();
    });

    it('routes a stream, and the NDJSON body is unaffected', async () => {
        const { seen } = wireHost();
        const got: number[] = [];
        for await (const value of actor(CartRef, 'r6').countTo(3)) got.push(value);
        expect(got).toEqual([1, 2, 3]);
        expect(seen[0].url).toBe(
            `${ENDPOINT}/r/${hashRouteToken('Cart', 'r6')}/Cart%23countTo`
        );
    });

    it("'key' mode routes on a key containing a slash", async () => {
        const { seen } = wireHost('key');
        await expect(actor(CartRef, 'tenant/a').add('x')).resolves.toBe(1);
        expect(seen[0].url).toBe(`${ENDPOINT}/r/tenant%2Fa/Cart%23add`);
        // The server decodes it back to the real key, not the escaped form.
        expect(seen[0].token).toBe('tenant/a');
    });

    it("'key' mode routes on a non-ASCII key", async () => {
        const { seen } = wireHost('key');
        await expect(actor(CartRef, 'café').add('x')).resolves.toBe(1);
        expect(seen[0].url).toBe(`${ENDPOINT}/r/caf%C3%A9/Cart%23add`);
        expect(seen[0].token).toBe('café');
    });

    it('sends BYTE-IDENTICAL carriers for a key that needs escaping', async () => {
        // Regression: the header used to carry the RAW token while the path
        // carried the encoded one. An LB hashes the bytes it sees, so
        // `tenant/a` in the header beside `tenant%2Fa` in the path hashes to
        // two different hosts — the carriers disagreed exactly when the key
        // was interesting, which is the one thing they must never do.
        for (const key of ['tenant/a', 'café', 'a b', 'p%2Fq', 'x?y&z']) {
            configureActors(null);
            const { seen } = wireHost('key');
            await expect(actor(CartRef, key).add('x')).resolves.toBe(1);

            const segment = seen[0].url.split('/').at(-2);
            expect(seen[0].header).toBe(segment);
            // …and both decode back to the same real key.
            expect(seen[0].token).toBe(key);
            // A header value must be safe ASCII: no raw non-ASCII, no CR/LF.
            expect(seen[0].header).toMatch(/^[\x21-\x7e]*$/);
        }
    });

    it('a header-only request (edge rewrote the path) reads the same token', async () => {
        // Envoy hashes the header and may normalize the path away. The
        // accessor must decode the header too, or an adapter would see
        // `tenant%2Fa` from one carrier and `tenant/a` from the other.
        const request = new Request(`${ENDPOINT}/Cart%23add`, {
            method: 'POST',
            headers: {
                'content-type': 'application/json',
                [ACTOR_ROUTE_HEADER]: encodeURIComponent('tenant/a')
            },
            body: JSON.stringify({ args: ['k', 'x'] })
        });
        expect(actorRouteToken(request)).toBe('tenant/a');
    });

    it('a custom token function routes, and its result is escaped', async () => {
        const { seen } = wireHost((ref) => `z/${ref.key}`);
        await expect(actor(CartRef, 'r7').add('x')).resolves.toBe(1);
        expect(seen[0].url).toBe(`${ENDPOINT}/r/z%2Fr7/Cart%23add`);
    });

    it('still reports 404 skew against the SYMBOL, not the token', async () => {
        const host = createHost({ actors: [cart], defaults: quiet });
        const request = new Request(`${ENDPOINT}/r/abc123/Ghost%23add`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args: ['g1'] })
        });
        const response = await handleActorRequest(request, { host, origin: false });
        expect(response.status).toBe(404);
        const body = (await response.json()) as { error: { message: string } };
        expect(body.error.message).toContain('Ghost#add');
        expect(body.error.message).not.toContain('abc123');
        await host.stop();
    });
});
