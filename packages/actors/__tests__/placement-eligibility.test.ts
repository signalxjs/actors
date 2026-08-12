/**
 * Registration-aware placement (#212): a cluster member is only ever CHOSEN
 * to host an actor type its app registers. The descriptor advertises the
 * registered types; every decision point — the policy stage, the route
 * cache, the directory — enforces the rule; a host receiving a call for a
 * type it does not register refuses with `wrong-host` (so the caller
 * re-places) instead of a late 404; and a type NO live host registers fails
 * with the branded, actionable `unplaceable` error.
 *
 * A descriptor WITHOUT the `types` field is an older build and stays
 * eligible for everything — the safe direction, pinned below.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { defineActor, defineWorker } from '@sigx/actors';
import { createHost, memoryStorage } from '@sigx/actors/host';
import { consistentHashPolicy, preferLocalPolicy, type PlacementPolicy } from '@sigx/actors/cluster';
import { createCluster, type ClusterHarness } from './harness';

const NUL = String.fromCharCode(0);
const call = (): { callChain: never[]; callId: string } => ({ callChain: [], callId: 'c' });

const Counter = defineActor({
    type: 'Counter',
    allowAnonymous: true,
    state: () => ({ n: 0 }),
    methods: (ctx) => ({
        async bump() {
            ctx.state.n++;
            await ctx.save();
            return ctx.state.n;
        },
        async read() {
            return ctx.state.n;
        }
    }),
    streams: () => ({
        async *countTo(to: number) {
            for (let i = 1; i <= to; i++) yield i;
        }
    })
});

const Cache = defineActor({
    type: 'Cache',
    allowAnonymous: true,
    state: () => ({ v: '' }),
    methods: (ctx) => ({
        async put(v: string) {
            ctx.state.v = v;
            return v;
        }
    })
});

const Relay = defineWorker({
    type: 'Relay',
    allowAnonymous: true,
    methods: () => ({
        async double(n: number) {
            return n * 2;
        }
    })
});

/** Registered by NO host in any topology below. */
const Ghost = defineActor({
    type: 'Ghost',
    allowAnonymous: true,
    state: () => ({}),
    methods: () => ({
        async poke() {
            return 'boo';
        }
    })
});

let cluster: ClusterHarness | null = null;
afterEach(async () => {
    await cluster?.stop();
    cluster = null;
});

/** Hosts 0 and 1 are "engine" (Counter only); host 2 is "web" (Cache + Relay). */
function engineWeb(
    extra: Partial<Parameters<typeof createCluster>[1]> = {}
): Promise<ClusterHarness> {
    return createCluster(3, {
        actors: [Counter],
        actorsFor: (i) => (i === 2 ? [Cache, Relay] : [Counter]),
        ...extra
    });
}

function engineIds(h: ClusterHarness): string[] {
    return [h.placements[0]!.identity.hostId, h.placements[1]!.identity.hostId];
}

describe('descriptor registration', () => {
    it('the descriptor advertises registered types, workers included, sorted', async () => {
        cluster = await createCluster(1, { actors: [Relay, Counter] });
        const p = cluster.placements[0]!;
        expect(p.descriptor().types).toEqual(['Counter', 'Relay']);
        // Round-trips through the membership view…
        expect(p.view().hosts[0]!.types).toEqual(['Counter', 'Relay']);
        // …and rides the ops report.
        expect(p.report().types).toEqual(['Counter', 'Relay']);
    });

    it('a heterogeneous cluster advertises per-host lists', async () => {
        cluster = await engineWeb();
        expect(cluster.placements[0]!.descriptor().types).toEqual(['Counter']);
        expect(cluster.placements[2]!.descriptor().types).toEqual(['Cache', 'Relay']);
    });

    it('lazy registry keys are advertised WITHOUT loading their modules', async () => {
        const loader = vi.fn(async () => Counter);
        const host = createHost({ actors: { Counter: loader }, storage: memoryStorage() });
        expect(host.registeredTypes!()).toEqual(['Counter']);
        expect(loader).not.toHaveBeenCalled();
    });
});

describe('default-deny eligibility', () => {
    it('random placement never chooses a host that does not register the type', async () => {
        cluster = await engineWeb();
        const engines = engineIds(cluster);
        const webId = cluster.placements[2]!.identity.hostId;
        // From the web host, every Counter lands on an engine…
        for (let i = 0; i < 20; i++) {
            await expect(cluster.hosts[2]!.actor(Counter, `k${i}`).bump()).resolves.toBe(1);
        }
        for (let i = 0; i < 20; i++) {
            const entry = await cluster.hub.directory.lookup(`Counter${NUL}k${i}`);
            expect(entry).not.toBeNull();
            expect(engines).toContain(entry!.hostId);
        }
        // …and from an engine host, every Cache lands on the web host.
        for (let i = 0; i < 5; i++) {
            await cluster.hosts[0]!.actor(Cache, `c${i}`).put('x');
            expect((await cluster.hub.directory.lookup(`Cache${NUL}c${i}`))!.hostId).toBe(webId);
        }
    });

    it('consistent-hash places only among registering hosts, and callers agree', async () => {
        cluster = await engineWeb({ policy: consistentHashPolicy() });
        const engines = engineIds(cluster);
        for (let i = 0; i < 10; i++) {
            await cluster.hosts[2]!.actor(Counter, `h${i}`).bump();
            expect(engines).toContain(
                (await cluster.hub.directory.lookup(`Counter${NUL}h${i}`))!.hostId
            );
        }
        // The same key from a DIFFERENT caller reaches the same activation.
        await expect(cluster.hosts[0]!.actor(Counter, 'h0').bump()).resolves.toBe(2);
    });

    it('prefer-local on a non-registering host converges on an eligible host', async () => {
        cluster = await engineWeb({ policy: preferLocalPolicy() });
        await cluster.hosts[2]!.actor(Counter, 'p').bump();
        expect(engineIds(cluster)).toContain(
            (await cluster.hub.directory.lookup(`Counter${NUL}p`))!.hostId
        );
    });

    it('prefer-local on a registering host still answers self', async () => {
        cluster = await engineWeb({ policy: preferLocalPolicy() });
        await cluster.hosts[0]!.actor(Counter, 'q').bump();
        expect((await cluster.hub.directory.lookup(`Counter${NUL}q`))!.hostId).toBe(
            cluster.placements[0]!.identity.hostId
        );
    });

    it('a type no live host registers fails with kind unplaceable, actionably', async () => {
        cluster = await engineWeb({ retries: 1, retryBackoffMs: 0 });
        const error = (await cluster.hosts[0]!
            .actor(Ghost, 'g')
            .poke()
            .catch((e: unknown) => e)) as Error & { kind?: string; cause?: Error & { kind?: string } };
        expect(error.kind).toBe('activation');
        expect(error.cause?.kind).toBe('unplaceable');
        expect(error.cause?.message).toContain('Ghost');
        expect(error.cause?.message).toMatch(/no active host/i);
    });

    it('unplaceable retries against a refreshed view and converges on a joiner', async () => {
        cluster = await createCluster(1, { actors: [Cache], retries: 8, retryBackoffMs: 25 });
        const pending = cluster.hosts[0]!.actor(Counter, 'late').bump();
        const settled = expect(pending).resolves.toBe(1);
        await cluster.add([Counter]);
        await settled;
        expect((await cluster.hub.directory.lookup(`Counter${NUL}late`))!.hostId).toBe(
            cluster.placements[1]!.identity.hostId
        );
    });

    it('a policy answering a host outside its eligible view fails loudly, naming itself', async () => {
        const rogue: PlacementPolicy = {
            name: 'rogue',
            choose: () => cluster!.placements[2]!.descriptor()
        };
        cluster = await engineWeb({ typePolicies: { Counter: rogue }, retries: 0 });
        await expect(cluster.hosts[0]!.actor(Counter, 'r').bump()).rejects.toThrow(/rogue/);
    });

    it('a policy answering a host outside the VIEW fails loudly on the all-eligible fast path too', async () => {
        // Homogeneous cluster — the identity fast path — and a policy
        // fabricating a descriptor that is in no view at all. Post-choose
        // validation must catch it here as well, not only when filtering
        // narrowed the view.
        const fabricator: PlacementPolicy = {
            name: 'fabricator',
            choose: () => ({
                hostId: 's.nowhere',
                epoch: 1,
                address: 'http://nowhere.test',
                status: 'active' as const
            })
        };
        cluster = await createCluster(2, {
            actors: [Counter],
            typePolicies: { Counter: fabricator },
            retries: 0
        });
        await expect(cluster.hosts[0]!.actor(Counter, 'f').bump()).rejects.toThrow(/fabricator/);
    });
});

describe('stale answers are re-validated', () => {
    it('a directory entry naming a live NON-registering host is evicted and re-placed', async () => {
        cluster = await engineWeb();
        const webId = cluster.placements[2]!.identity.hostId;
        await cluster.hub.directory.claim(`Counter${NUL}pois`, {
            hostId: webId,
            activationId: `${webId}/1/999`
        });
        await expect(cluster.hosts[0]!.actor(Counter, 'pois').bump()).resolves.toBe(1);
        expect(engineIds(cluster)).toContain(
            (await cluster.hub.directory.lookup(`Counter${NUL}pois`))!.hostId
        );
        expect(cluster.placements[0]!.counters().directoryEvictions).toBeGreaterThan(0);
    });

    it('an owner hint naming a non-registering host is dropped at the route cache, not dialed', async () => {
        let crafted = false;
        let webDialedForCounter = false;
        const pinToHost1: PlacementPolicy = {
            name: 'pin-host1',
            choose: (_ref, view, self) =>
                view.hosts.find((h) => h.address === 'http://host1.test') ?? self
        };
        cluster = await createCluster(3, {
            actors: [Counter],
            actorsFor: (i) => (i === 2 ? [Cache, Relay] : [Counter]),
            typePolicies: { Counter: pinToHost1 },
            retries: 3,
            retryBackoffMs: 0,
            onRequest: (url) => {
                if (url.includes('host2.test') && url.includes('Counter')) {
                    webDialedForCounter = true;
                }
            },
            wrapFetch: (inner) => async (input, init) => {
                const request = new Request(input, init);
                if (!crafted && request.url.includes('host1.test') && request.url.includes('Counter')) {
                    // A peer lying about the owner: a 421 naming the WEB host.
                    crafted = true;
                    return new Response(
                        JSON.stringify({
                            error: {
                                message: 'crafted redirect',
                                status: 421,
                                data: {
                                    kind: 'wrong-host',
                                    owner: {
                                        hostId: cluster!.placements[2]!.identity.hostId,
                                        address: 'http://host2.test'
                                    }
                                }
                            }
                        }),
                        { status: 421, headers: { 'content-type': 'application/json' } }
                    );
                }
                return inner(input, init);
            }
        });
        await expect(cluster.hosts[0]!.actor(Counter, 'lied').bump()).resolves.toBe(1);
        expect(crafted).toBe(true);
        // The learned route was discarded at read time — the web host was
        // never dialed for a type it does not register.
        expect(webDialedForCounter).toBe(false);
    });
});

describe('receiving-side refusal', () => {
    it('an inbound call for an unregistered type answers wrong-host, not method-not-found', async () => {
        cluster = await engineWeb();
        const web = cluster.placements[2]!;
        const error = (await web
            .dispatchInbound({ type: 'Counter', key: 'x' }, 'bump', [], call())
            .catch((e: unknown) => e)) as Error & { kind?: string; owner?: unknown };
        expect(error.kind).toBe('wrong-host');
        // No owner hint: the refusing host has no idea who owns it, and a
        // fabricated hint would be worse than none.
        expect(error.owner).toBeUndefined();
    });

    it('inbound stream and watch refuse on the first pull, same brand', async () => {
        cluster = await engineWeb();
        const web = cluster.placements[2]!;
        const stream = web.dispatchInboundStream({ type: 'Counter', key: 'x' }, 'countTo', [3], call());
        await expect(stream[Symbol.asyncIterator]().next()).rejects.toMatchObject({
            kind: 'wrong-host'
        });
        const watch = web.dispatchInboundWatch({ type: 'Counter', key: 'x' }, 'read', [], call());
        await expect(watch[Symbol.asyncIterator]().next()).rejects.toMatchObject({
            kind: 'wrong-host'
        });
    });

    it('an unknown method on a REGISTERED type still answers method-not-found', async () => {
        cluster = await engineWeb();
        await expect(
            cluster.hosts[0]!.dispatch({ type: 'Counter', key: 'm' }, 'nope', [], call())
        ).rejects.toMatchObject({ kind: 'method-not-found' });
        // …including across the hop from the non-registering host.
        await expect(
            cluster.hosts[2]!.dispatch({ type: 'Counter', key: 'm2' }, 'nope', [], call())
        ).rejects.toMatchObject({ kind: 'method-not-found' });
    });
});

describe('callers need no local registration', () => {
    it('a host can dispatch, stream from, and watch a type it does not register', async () => {
        cluster = await engineWeb();
        const web = cluster.hosts[2]!;
        await expect(web.actor(Counter, 'w').bump()).resolves.toBe(1);

        const got: number[] = [];
        for await (const v of web.dispatchStream!(
            { type: 'Counter', key: 'w' },
            'countTo',
            [3],
            call()
        )) {
            got.push(v as number);
        }
        expect(got).toEqual([1, 2, 3]);

        const iterator = web
            .dispatchWatch!({ type: 'Counter', key: 'w' }, 'read', [], call())
            [Symbol.asyncIterator]();
        expect((await iterator.next()).value).toBe(1);
        await cluster.hosts[0]!.actor(Counter, 'w').bump();
        expect((await iterator.next()).value).toBe(2);
        await iterator.return?.();

        expect(engineIds(cluster)).toContain(
            (await cluster.hub.directory.lookup(`Counter${NUL}w`))!.hostId
        );
    });
});

describe('mixed-version clusters', () => {
    it('a descriptor without types (an older build) stays eligible for everything', async () => {
        const pickLegacy: PlacementPolicy = {
            name: 'pick-legacy',
            choose: (_ref, view, self) =>
                view.hosts.find((h) => h.hostId === 's.legacy') ?? self
        };
        cluster = await createCluster(2, {
            actors: [Counter],
            actorsFor: (i) => (i === 1 ? [Cache] : [Counter]),
            typePolicies: { Counter: pickLegacy }
        });
        // A legacy host joins the view with NO `types` field, addressed at
        // host 0 — which really registers Counter, so its dispatches land.
        await cluster.hub.providers().membership.join({
            hostId: 's.legacy',
            epoch: 1,
            address: 'http://host0.test',
            status: 'active'
        });
        await expect(cluster.hosts[1]!.actor(Counter, 'via-legacy').bump()).resolves.toBe(1);
    });
});

describe('workers in a heterogeneous cluster', () => {
    it('local on a registering host with zero directory ops; routed to one from elsewhere', async () => {
        cluster = await engineWeb();
        const webP = cluster.placements[2]!;
        await expect(cluster.hosts[2]!.actor(Relay, 'k').double(4)).resolves.toBe(8);
        expect(webP.counters().directoryLookups).toBe(0);
        expect(webP.counters().directoryClaims).toBe(0);
        // From an engine host, the worker call routes to the web host and
        // executes THERE (workers run where the call lands).
        await expect(cluster.hosts[0]!.actor(Relay, 'k').double(5)).resolves.toBe(10);
        expect(cluster.placements[0]!.counters().remoteDispatches).toBeGreaterThan(0);
        expect(webP.counters().inboundDispatches).toBeGreaterThan(0);
        // Still no claim anywhere — a worker never touches the directory.
        await expect(cluster.hub.directory.lookup(`Relay${NUL}k`)).resolves.toBeNull();
    });
});
