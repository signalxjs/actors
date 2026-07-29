/**
 * `onMiss` — what the PUBLIC mount does with a call for a grain another
 * silo owns.
 *
 * Two properties carry the whole design, and both have a test that fails
 * loudly if they regress:
 *
 * 1. **A redirect dispatches nothing.** 421 is a status a user agent may
 *    retry on its own, and actor calls are not idempotent — so the answer
 *    must be provably free of side effects. That is why the endpoint asks
 *    `locate()` BEFORE dispatching rather than letting a wrong-host escape
 *    the routing loop.
 * 2. **The internal address never crosses to a client.** `advertise` is a
 *    pod IP: unreachable from outside, and not ours to disclose.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { actor, defineActor } from '@sigx/actors';
import { __actorRef, configureActors, routedFetchTransport } from '@sigx/actors/client';
import { createSilo } from '@sigx/actors/silo';
import { handleActorRequest } from '@sigx/actors/server';
import { createCluster, type ClusterHarness } from './harness';

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ n: 0 }),
    methods: (ctx) => ({
        async bump() {
            ctx.state.n += 1;
            await ctx.save();
            return ctx.state.n;
        }
    }),
    streams: () => ({
        async *ticks(to: number) {
            for (let i = 1; i <= to; i++) yield i;
        }
    })
});

const PUBLIC = (i: number) => `http://silo${i}.test`;

let harness: ClusterHarness | undefined;

afterEach(async () => {
    configureActors(null);
    await harness?.stop();
    harness = undefined;
    vi.restoreAllMocks();
});

/** Place `key`, then return the index of a silo that does NOT own it. */
async function placeAndFindNonOwner(h: ClusterHarness, key: string): Promise<number> {
    await h.silos[0]!.dispatch({ type: 'Counter', key }, 'bump', [], call());
    for (let i = 0; i < h.silos.length; i++) {
        const at = await h.placements[i]!.locate({ type: 'Counter', key });
        if (!at.local) return i;
    }
    throw new Error('every silo claims to own the grain');
}

function call() {
    return { callChain: [], callId: `t-${Math.random()}`, abortSignal: undefined };
}

describe('onMiss: proxy (the default)', () => {
    it('answers a non-owner call in ONE request', async () => {
        // Pins today's behaviour before anything else changes it.
        const urls: string[] = [];
        harness = await createCluster(2, { actors: [Counter], onRequest: (u) => urls.push(u) });
        const from = await placeAndFindNonOwner(harness, 'c1');

        configureActors({ endpoint: harness.endpointOf(from), fetch: harness.fetch });
        const Ref = __actorRef('Counter', harness.endpointOf(from)) as typeof Counter;

        urls.length = 0;
        await expect(actor(Ref, 'c1').bump()).resolves.toBe(2);
        // One PUBLIC request; the internal hop is the silo's business.
        expect(urls.filter((u) => u.includes('/_sigx/actor'))).toHaveLength(1);
    });
});

describe('onMiss: redirect', () => {
    it('answers 421 naming the owner, and dispatches NOTHING', async () => {
        harness = await createCluster(2, {
            actors: [Counter],
            onMiss: 'redirect',
            publicAddress: PUBLIC
        });
        const from = await placeAndFindNonOwner(harness, 'c2');

        const before = harness.silos.map((s) => s.stats().activations);
        const remoteBefore = harness.placements.map((p) => p.counters().remoteDispatches);

        const response = await harness.fetch(`${harness.endpointOf(from)}/Counter%23bump`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args: ['c2'] })
        });

        expect(response.status).toBe(421);
        const body = (await response.json()) as {
            error: { data: { kind: string; owner: { siloId: string; endpoint: string } } };
        };
        expect(body.error.data.kind).toBe('wrong-host');
        expect(body.error.data.owner.endpoint).toMatch(/^http:\/\/silo\d\.test\/_sigx\/actor$/);
        expect(response.headers.get('x-sigx-actor-owner')).toBe(body.error.data.owner.endpoint);

        // THE property: no activation moved, and nothing was forwarded. A
        // client (or a proxy) auto-retrying this 421 cannot double-apply.
        expect(harness.silos.map((s) => s.stats().activations)).toEqual(before);
        expect(harness.placements.map((p) => p.counters().remoteDispatches)).toEqual(
            remoteBefore
        );
    });

    it('never puts the INTERNAL address in the body', async () => {
        harness = await createCluster(2, {
            actors: [Counter],
            onMiss: 'redirect',
            // A public origin deliberately different from `advertise`, so a
            // leak of the internal one is unmistakable.
            publicAddress: (i) => `https://public-${i}.example.com`
        });
        const from = await placeAndFindNonOwner(harness, 'c3');

        const response = await harness.fetch(`${harness.endpointOf(from)}/Counter%23bump`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args: ['c3'] })
        });
        const text = await response.text();

        expect(response.status).toBe(421);
        expect(text).toContain('https://public-');
        // The pod IP must not appear anywhere in what a client receives.
        expect(text).not.toContain('silo0.test');
        expect(text).not.toContain('silo1.test');
        expect(text).not.toContain('"address"');
    });

    it('PROXIES when the owner published no publicAddress', async () => {
        // Degrades to the old behaviour rather than dead-ending a client at
        // an address it cannot reach.
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        harness = await createCluster(2, { actors: [Counter], onMiss: 'redirect' });
        const from = await placeAndFindNonOwner(harness, 'c4');

        const response = await harness.fetch(`${harness.endpointOf(from)}/Counter%23bump`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args: ['c4'] })
        });

        expect(response.status).toBe(200);
        await expect(response.json()).resolves.toEqual({ data: 2 });
        expect(warn.mock.calls.flat().join(' ')).toContain('publishes no publicAddress');
    });

    it('proxies when the placement cannot locate at all', async () => {
        // A single-node silo has no `locate()`, so the seam stays optional
        // and the mount simply behaves as it always did.
        const silo = createSilo({ actors: [Counter] });
        const response = await handleActorRequest(
            new Request('http://s.test/_sigx/actor/Counter%23bump', {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ args: ['solo'] })
            }),
            { silo, origin: false, onMiss: 'redirect' }
        );
        expect(response.status).toBe(200);
        await silo.stop();
    });

    it('proxies once the caller has already followed maxHops redirects', async () => {
        harness = await createCluster(2, {
            actors: [Counter],
            onMiss: 'redirect',
            publicAddress: PUBLIC
        });
        const from = await placeAndFindNonOwner(harness, 'c5');

        const response = await harness.fetch(`${harness.endpointOf(from)}/Counter%23bump`, {
            method: 'POST',
            headers: { 'content-type': 'application/json', 'x-sigx-actor-hops': '2' },
            body: JSON.stringify({ args: ['c5'] })
        });
        // The loop is bounded by the SERVER too, not only by a cooperating
        // client: two silos disagreeing about ownership must not ping-pong.
        expect(response.status).toBe(200);
    });

    it('never redirects $live — one response, many grains', async () => {
        harness = await createCluster(2, {
            actors: [Counter],
            onMiss: 'redirect',
            publicAddress: PUBLIC
        });
        const from = await placeAndFindNonOwner(harness, 'c6');

        const response = await harness.fetch(
            `${harness.endpointOf(from)}/${encodeURIComponent('$live#subscribe')}`,
            {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ args: [[{ t: 'Counter', k: 'c6', m: 'bump' }]] })
            }
        );
        expect(response.status).toBe(200);
        await response.body?.cancel();
    });
});

describe('onMiss: auto', () => {
    it('proxies a caller that did not advertise it can follow', async () => {
        harness = await createCluster(2, {
            actors: [Counter],
            onMiss: 'auto',
            publicAddress: PUBLIC
        });
        const from = await placeAndFindNonOwner(harness, 'c7');
        const response = await harness.fetch(`${harness.endpointOf(from)}/Counter%23bump`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args: ['c7'] })
        });
        expect(response.status).toBe(200);
    });

    it('redirects a caller that did', async () => {
        harness = await createCluster(2, {
            actors: [Counter],
            onMiss: 'auto',
            publicAddress: PUBLIC
        });
        const from = await placeAndFindNonOwner(harness, 'c8');
        const response = await harness.fetch(`${harness.endpointOf(from)}/Counter%23bump`, {
            method: 'POST',
            headers: { 'content-type': 'application/json', 'x-sigx-actor-follow': '1' },
            body: JSON.stringify({ args: ['c8'] })
        });
        expect(response.status).toBe(421);
    });
});

describe('the client follows, and remembers', () => {
    it('costs 2 requests once, then 1 forever — the whole point', async () => {
        const urls: string[] = [];
        harness = await createCluster(2, {
            actors: [Counter],
            onMiss: 'redirect',
            publicAddress: PUBLIC,
            onRequest: (u) => urls.push(u)
        });
        const from = await placeAndFindNonOwner(harness, 'c9');
        const endpoint = harness.endpointOf(from);
        configureActors(routedFetchTransport({ endpoint, fetch: harness.fetch, follow: true }));
        const Ref = __actorRef('Counter', endpoint) as typeof Counter;

        const publicUrls = () => urls.filter((u) => u.includes('/_sigx/actor'));

        urls.length = 0;
        await expect(actor(Ref, 'c9').bump()).resolves.toBe(2);
        expect(publicUrls()).toHaveLength(2); // 421, then the owner

        urls.length = 0;
        await expect(actor(Ref, 'c9').bump()).resolves.toBe(3);
        expect(publicUrls()).toHaveLength(1); // straight to the owner
        expect(publicUrls()[0]).not.toContain(new URL(endpoint).host);
    });

    it('does not follow unless asked', async () => {
        const urls: string[] = [];
        harness = await createCluster(2, {
            actors: [Counter],
            onMiss: 'redirect',
            publicAddress: PUBLIC,
            onRequest: (u) => urls.push(u)
        });
        const from = await placeAndFindNonOwner(harness, 'c10');
        configureActors({ endpoint: harness.endpointOf(from), fetch: harness.fetch });
        const Ref = __actorRef('Counter', harness.endpointOf(from)) as typeof Counter;

        // Default is follow:false — the 421 surfaces as an error rather than
        // being silently retried, which is what a browser wants.
        await expect(actor(Ref, 'c10').bump()).rejects.toThrow();
    });

    it('forgets a learned endpoint that stops answering, and recovers', async () => {
        // Without this, one dead silo strands every client that learned it.
        const urls: string[] = [];
        harness = await createCluster(3, {
            actors: [Counter],
            onMiss: 'redirect',
            publicAddress: PUBLIC,
            onRequest: (u) => urls.push(u)
        });
        const from = await placeAndFindNonOwner(harness, 'c11');
        const endpoint = harness.endpointOf(from);
        configureActors(routedFetchTransport({ endpoint, fetch: harness.fetch, follow: true }));
        const Ref = __actorRef('Counter', endpoint) as typeof Counter;

        await actor(Ref, 'c11').bump();
        const owner = (await ownerIndex(harness, 'c11'))!;
        harness.crash(owner);

        // The learned endpoint now refuses connections; the client must fall
        // back to its configured endpoint rather than failing forever.
        await expect(actor(Ref, 'c11').bump()).resolves.toBeGreaterThan(0);
    });

    it('follows for a stream too, and reads NDJSON from the owner', async () => {
        harness = await createCluster(2, {
            actors: [Counter],
            onMiss: 'redirect',
            publicAddress: PUBLIC
        });
        const from = await placeAndFindNonOwner(harness, 'c12');
        const endpoint = harness.endpointOf(from);
        configureActors(routedFetchTransport({ endpoint, fetch: harness.fetch, follow: true }));
        const Ref = __actorRef('Counter', endpoint, ['ticks']) as typeof Counter;

        const got: number[] = [];
        for await (const v of actor(Ref, 'c12').ticks(3)) got.push(v);
        expect(got).toEqual([1, 2, 3]);
    });
});

/** Which silo currently owns `key`, via the public `locate()` seam. */
async function ownerIndex(h: ClusterHarness, key: string): Promise<number | undefined> {
    for (let i = 0; i < h.placements.length; i++) {
        const at = await h.placements[i]!.locate({ type: 'Counter', key });
        if (at.local) return i;
    }
    return undefined;
}

describe('the locate() seam', () => {
    it('answers without activating anything', async () => {
        harness = await createCluster(2, { actors: [Counter] });
        const before = harness.silos.map((s) => s.stats().activations);

        const at = await harness.placements[0]!.locate({ type: 'Counter', key: 'never-run' });

        expect(typeof at.local).toBe('boolean');
        // Asking WHERE must not create the grain — the endpoint calls this
        // on every miss, and activating there would defeat placement.
        expect(harness.silos.map((s) => s.stats().activations)).toEqual(before);
    });

    it('says local for a grain this silo owns, remote for one it does not', async () => {
        harness = await createCluster(2, { actors: [Counter] });
        const from = await placeAndFindNonOwner(harness, 'loc1');
        const owner = from === 0 ? 1 : 0;

        await expect(
            harness.placements[owner]!.locate({ type: 'Counter', key: 'loc1' })
        ).resolves.toEqual({ local: true });

        const remote = await harness.placements[from]!.locate({ type: 'Counter', key: 'loc1' });
        expect(remote.local).toBe(false);
        if (!remote.local) expect(remote.owner.siloId).toBeTruthy();
    });

    it('carries publicAddress through the composite placement', async () => {
        // Regression: `compositePlacement` forwards a fixed set of methods,
        // so a new one is silently dropped — and `locate` being dropped
        // makes every app-built silo (i.e. every real one) proxy forever
        // while looking correctly configured.
        harness = await createCluster(2, { actors: [Counter], publicAddress: PUBLIC });
        const from = await placeAndFindNonOwner(harness, 'loc2');

        expect(harness.silos[from]!.locate).toBeTypeOf('function');
        const at = await harness.silos[from]!.locate!({ type: 'Counter', key: 'loc2' });
        expect(at).toBeDefined();
        expect(at!.local).toBe(false);
        if (at && !at.local) {
            expect(at.owner.publicAddress).toMatch(/^http:\/\/silo\d\.test$/);
            // Both origins travel together; picking is the consumer's job.
            expect(at.owner.address).toMatch(/^http:\/\/silo\d\.test$/);
        }
    });

    it('publishes publicAddress in the membership descriptor', async () => {
        harness = await createCluster(2, { actors: [Counter], publicAddress: PUBLIC });
        expect(harness.placements[0]!.descriptor().publicAddress).toBe('http://silo0.test');
        expect(harness.placements[1]!.descriptor().publicAddress).toBe('http://silo1.test');
    });

    it('omits publicAddress entirely when unset', async () => {
        harness = await createCluster(2, { actors: [Counter] });
        expect(harness.placements[0]!.descriptor().publicAddress).toBeUndefined();
        expect('publicAddress' in harness.placements[0]!.descriptor()).toBe(false);
    });
});

describe('the 421 the client gives up on stays readable', () => {
    it('surfaces the server message when the hop cap is hit', async () => {
        // Regression: probing a 421 for its owner endpoint read the body, so
        // the SAME Response handed back at the hop cap arrived drained and
        // the caller could only report a generic skew hint. Only bites when
        // the mirror header is absent — i.e. behind a proxy that strips it,
        // which is exactly when the diagnosis matters.
        const body = JSON.stringify({
            error: {
                status: 421,
                message: '[sigx actors] Counter/x is owned by silo-9',
                data: {
                    kind: 'wrong-host',
                    owner: { siloId: 'silo-9', endpoint: 'http://silo-9.test/_sigx/actor' }
                }
            }
        });
        configureActors(routedFetchTransport({
            endpoint: 'http://edge.test/_sigx/actor',
            follow: { maxHops: 0 },
            // No x-sigx-actor-owner header: the body is the only source.
            fetch: async () => new Response(body, { status: 421 })
        }));
        const Ref = __actorRef('Counter', 'http://edge.test/_sigx/actor') as typeof Counter;

        await expect(actor(Ref, 'x').bump()).rejects.toThrow('is owned by silo-9');
    });
});

describe('the route memo respects its cap', () => {
    it('remembers nothing at cache: 0, and still answers correctly', async () => {
        // `cache: 0` must mean what it says. Correctness is unaffected either
        // way — the memo is an optimization — so the observable difference is
        // that every call pays the redirect again.
        const urls: string[] = [];
        harness = await createCluster(2, {
            actors: [Counter],
            onMiss: 'redirect',
            publicAddress: PUBLIC,
            onRequest: (u) => urls.push(u)
        });
        const from = await placeAndFindNonOwner(harness, 'cap0');
        const endpoint = harness.endpointOf(from);
        configureActors(routedFetchTransport({ endpoint, fetch: harness.fetch, follow: { cache: 0 } }));
        const Ref = __actorRef('Counter', endpoint) as typeof Counter;

        const publicUrls = () => urls.filter((u) => u.includes('/_sigx/actor'));

        urls.length = 0;
        await expect(actor(Ref, 'cap0').bump()).resolves.toBe(2);
        expect(publicUrls()).toHaveLength(2);

        urls.length = 0;
        await expect(actor(Ref, 'cap0').bump()).resolves.toBe(3);
        expect(publicUrls()).toHaveLength(2); // nothing was remembered
    });
});
