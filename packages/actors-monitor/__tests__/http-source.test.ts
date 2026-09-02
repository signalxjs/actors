/**
 * `httpSource` — the mode that works against production.
 *
 * Two behaviours carry most of these tests. **404 on the cluster route is a
 * real answer**, not a failure: a single-node host genuinely has no cluster,
 * and treating that as an error would make the common case look broken.
 * Everything else non-2xx IS an error, because a dashboard that renders
 * zeroes for a 401 costs ten minutes debugging a healthy cluster.
 */
import { describe, expect, it, vi } from 'vitest';
import { httpSource, OpsRequestError, withSockets } from '@sigx/actors-monitor';
import { host } from './fixture';

const SECRET = 'ops-secret';

const opsBody = {
    v: 1,
    at: 1_700_000_000_000,
    uptimeMs: 65_000,
    stats: {
        activations: 3,
        queued: 1,
        perType: { Counter: 3 },
        transitional: { activating: 0, deactivating: 0 }
    },
    activations: [{ type: 'Counter', key: 'a', queued: 1, ageMs: 500, idleMs: 0, keptAlive: false }],
    health: { live: true, ready: true, uptimeMs: 65_000, checks: {}, host: null },
    ops: { metrics: { calls: { total: 10, failed: 1, streams: 0 } } }
};

/** The `sockets` ops section an app with a socket mount publishes (#166). */
const socketsSection = {
    connectionsOpened: 7,
    connectionsClosed: 4,
    connectionsRefused: 2,
    callsStarted: 40,
    callsFailed: 3,
    subscriptionsOpened: 9,
    subscriptionsClosed: 5,
    protocolBreaches: 1,
    lifetimeCloses: 2,
    deliveries: 1200,
    deliveryBytes: 48_000,
    throttleQuantized: 6,
    open: 3,
    inFlight: 1,
    subscriptions: 4,
    bufferedBytes: 512,
    lifetimeMs: null
};

const clusterBody = {
    at: 1_700_000_000_000,
    from: 'host-a',
    view: { version: 4, size: 2, active: 2 },
    hosts: [
        {
            v: 1,
            hostId: 'host-a',
            epoch: 1,
            address: 'http://a:3000',
            status: 'active',
            stats: {
                activations: 2,
                queued: 0,
                perType: {},
                transitional: { activating: 0, deactivating: 0 }
            },
            counters: { membershipVersion: 4, status: 'active', routedLocal: 5 },
            reminderShards: ['p0'],
            uptimeMs: 1000,
            transports: ['http'],
            // What a Kubernetes chart publishes from `spec.nodeName` (#51).
            meta: { node: 'aks-agentpool-1' }
        }
    ],
    unreachable: [{ hostId: 'host-b', address: 'http://b:3000', reason: 'timeout', message: 'x' }],
    partial: true,
    totals: { hosts: 2, activations: 2, queued: 0, perType: {}, counters: {} },
    reminderShards: { p0: ['host-a'], p1: [] }
};

/** A fetch that answers a fixed map of paths. */
function stubFetch(routes: Record<string, { status: number; body?: unknown }>) {
    return vi.fn(async (input: RequestInfo | URL) => {
        const url = new URL(typeof input === 'string' ? input : input.toString());
        const route = routes[url.pathname];
        if (!route) return new Response('nope', { status: 500 });
        return new Response(route.body === undefined ? null : JSON.stringify(route.body), {
            status: route.status,
            headers: { 'content-type': 'application/json' }
        });
    }) as unknown as typeof globalThis.fetch;
}

describe('httpSource', () => {
    it('reads a single-node host, with no cluster', async () => {
        const source = httpSource({
            url: 'http://host.test',
            secret: SECRET,
            fetch: stubFetch({
                '/_sigx/ops': { status: 200, body: opsBody },
                '/_sigx/ops/cluster': { status: 404 }
            })
        });
        const snapshot = await source.snapshot();

        // 404 on the cluster route is "no cluster", not an error.
        expect(snapshot.cluster).toBeNull();
        expect(snapshot.partial).toBe(false);
        expect(snapshot.hosts).toHaveLength(1);
        expect(snapshot.hosts[0]!.status).toBe('unknown');
        expect(snapshot.hosts[0]!.stats.activations).toBe(3);
        // No cluster means no descriptor, so no placement hints — null, not
        // an empty object that would render as "on no node".
        expect(snapshot.hosts[0]!.meta).toBeNull();
        expect(snapshot.activations).toHaveLength(1);
        expect(snapshot.health?.ready).toBe(true);
        expect(snapshot.metrics?.calls.total).toBe(10);
    });

    it('prefers the cluster fan-out for the host list, and carries partial through', async () => {
        const source = httpSource({
            url: 'http://host.test',
            secret: SECRET,
            fetch: stubFetch({
                '/_sigx/ops': { status: 200, body: opsBody },
                '/_sigx/ops/cluster': { status: 200, body: clusterBody }
            })
        });
        const snapshot = await source.snapshot();

        expect(snapshot.cluster?.from).toBe('host-a');
        expect(snapshot.hosts).toHaveLength(1);
        expect(snapshot.hosts[0]!.hostId).toBe('host-a');
        // membershipVersion and status are lifted OUT of counters, because
        // they are per-host facts a total would destroy.
        expect(snapshot.hosts[0]!.membershipVersion).toBe(4);
        expect(snapshot.hosts[0]!.counters).not.toHaveProperty('membershipVersion');
        // The placement hints ride through untouched — `node` is how a
        // packed fleet becomes visible (#51).
        expect(snapshot.hosts[0]!.meta).toEqual({ node: 'aks-agentpool-1' });
        // The one flag that must never be smoothed over.
        expect(snapshot.partial).toBe(true);
        expect(snapshot.cluster?.unreachable).toHaveLength(1);
    });

    it('carries the sockets section onto the polled host, and nowhere else (#166)', async () => {
        // Single node: the one row is the polled host.
        const single = httpSource({
            url: 'http://host.test',
            secret: SECRET,
            fetch: stubFetch({
                '/_sigx/ops': { status: 200, body: { ...opsBody, ops: { ...opsBody.ops, sockets: socketsSection } } },
                '/_sigx/ops/cluster': { status: 404 }
            })
        });
        const alone = await single.snapshot();
        expect(alone.hosts[0]!.sockets?.open).toBe(3);
        expect(alone.hosts[0]!.sockets?.bufferedBytes).toBe(512);

        // Cluster: the fan-out carries no socket digest, so the section
        // belongs to `from` — and a peer's null means "said nothing", not
        // "no sockets there".
        const clustered = httpSource({
            url: 'http://host.test',
            secret: SECRET,
            fetch: stubFetch({
                '/_sigx/ops': { status: 200, body: { ...opsBody, ops: { ...opsBody.ops, sockets: socketsSection } } },
                '/_sigx/ops/cluster': {
                    status: 200,
                    body: {
                        ...clusterBody,
                        hosts: [
                            clusterBody.hosts[0],
                            { ...clusterBody.hosts[0], hostId: 'host-b', address: 'http://b:3000' }
                        ]
                    }
                }
            })
        });
        const fleet = await clustered.snapshot();
        expect(fleet.hosts.map((h) => h.sockets?.open ?? null)).toEqual([3, null]);
    });

    it('reports no sockets section as null, never as zeroes', async () => {
        const source = httpSource({
            url: 'http://host.test',
            secret: SECRET,
            fetch: stubFetch({
                '/_sigx/ops': { status: 200, body: opsBody },
                '/_sigx/ops/cluster': { status: 404 }
            })
        });
        const snapshot = await source.snapshot();
        expect(snapshot.hosts[0]!.sockets).toBeNull();
    });

    it('sends the bearer token on every route', async () => {
        const fetchMock = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
            const url = new URL(typeof input === 'string' ? input : input.toString());
            return url.pathname.endsWith('/cluster')
                ? new Response(null, { status: 404 })
                : new Response(JSON.stringify(opsBody), { status: 200 });
        });
        const source = httpSource({
            url: 'http://host.test',
            secret: SECRET,
            fetch: fetchMock as unknown as typeof globalThis.fetch
        });
        await source.snapshot();

        expect(fetchMock.mock.calls.length).toBeGreaterThan(1);
        for (const [, init] of fetchMock.mock.calls) {
            const headers = (init as RequestInit).headers as Record<string, string>;
            expect(headers.authorization).toBe(`Bearer ${SECRET}`);
        }
    });

    it('falls back to $SIGX_OPS_SECRET only through resolveSource, not here', () => {
        // httpSource takes what it is given: an unauthenticated host (a dev
        // one) is a legitimate target, so a missing secret is not an error.
        const source = httpSource({ url: 'http://host.test' });
        expect(source.kind).toBe('http');
    });

    it('says plainly that the secret was rejected', async () => {
        const source = httpSource({
            url: 'http://host.test',
            secret: 'wrong',
            fetch: stubFetch({ '/_sigx/ops': { status: 401 } })
        });
        // A dashboard that rendered zeroes here would send you debugging a
        // perfectly healthy cluster.
        await expect(source.snapshot()).rejects.toThrow(/rejected the ops secret/);
    });

    it('explains a 404 on the snapshot route as "ops() is not wired in"', async () => {
        const source = httpSource({
            url: 'http://host.test',
            fetch: stubFetch({ '/_sigx/ops': { status: 404 } })
        });
        await expect(source.snapshot()).rejects.toThrow(/is not serving an ops endpoint/);
    });

    it('reports an unreachable origin rather than throwing a bare TypeError', async () => {
        const source = httpSource({
            url: 'http://host.test',
            fetch: (() => Promise.reject(new TypeError('fetch failed'))) as typeof globalThis.fetch
        });
        await expect(source.snapshot()).rejects.toThrow(OpsRequestError);
        await expect(source.snapshot()).rejects.toThrow(/is unreachable/);
    });

    it('gives up immediately on an already-aborted signal', async () => {
        const fetchMock = vi.fn(
            async () => new Response(JSON.stringify(opsBody), { status: 200 })
        ) as unknown as typeof globalThis.fetch;
        const source = httpSource({ url: 'http://host.test', fetch: fetchMock, timeoutMs: 30_000 });

        // `addEventListener('abort')` never fires on a signal that has
        // ALREADY aborted, so without an up-front check the request would
        // run to its full timeout and then be reported as a sick host
        // rather than as the cancellation it was.
        await expect(source.snapshot(AbortSignal.abort())).rejects.toThrow();
        expect(fetchMock).not.toHaveBeenCalled();
    });

    it('tolerates a host that reports no health, rather than trusting the type', async () => {
        // The restated OpsBody exists precisely because a peer on an older
        // build may omit fields; typing them required and defaulting them
        // would be the unsound half of both worlds.
        const { health: _health, ...withoutHealth } = opsBody;
        const source = httpSource({
            url: 'http://host.test',
            fetch: stubFetch({
                '/_sigx/ops': { status: 200, body: withoutHealth },
                '/_sigx/ops/cluster': { status: 404 }
            })
        });
        const snapshot = await source.snapshot();
        expect(snapshot.health).toBeNull();
        expect(snapshot.hosts[0]!.stats.activations).toBe(3);
    });

    it('honours a custom base and trims trailing slashes', async () => {
        const source = httpSource({
            url: 'http://host.test/',
            base: '/admin/ops/',
            fetch: stubFetch({
                '/admin/ops': { status: 200, body: opsBody },
                '/admin/ops/cluster': { status: 404 }
            })
        });
        await expect(source.snapshot()).resolves.toBeTruthy();
    });

    it('labels itself by origin, so it is never ambiguous what is being watched', () => {
        expect(httpSource({ url: 'http://host.test/' }).kind).toBe('http');
        expect(httpSource({ url: 'http://host.test/' }).label).toBe('http://host.test');
    });
});

describe('withSockets', () => {
    it('puts the section on the polled host and clears every other row, by construction (#166)', () => {
        // Rows that arrive already carrying a section — a reused snapshot,
        // a caller that guessed — must not leak it onto the wrong host: the
        // invariant is "exactly one row, the polled one", not "at least".
        const stale = { ...socketsSection, open: 99 };
        const rows = withSockets(
            [host({ hostId: 'a', sockets: stale }), host({ hostId: 'b', sockets: stale })],
            'b',
            socketsSection
        );
        expect(rows.map((row) => row.sockets?.open ?? null)).toEqual([null, 3]);
    });

    it('answers null on the polled host too when it published nothing', () => {
        const rows = withSockets([host({ hostId: 'a', sockets: socketsSection })], 'a', null);
        expect(rows[0]!.sockets).toBeNull();
    });
});
