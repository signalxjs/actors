/**
 * `httpSource` — the mode that works against production.
 *
 * Two behaviours carry most of these tests. **404 on the cluster route is a
 * real answer**, not a failure: a single-node silo genuinely has no cluster,
 * and treating that as an error would make the common case look broken.
 * Everything else non-2xx IS an error, because a dashboard that renders
 * zeroes for a 401 costs ten minutes debugging a healthy cluster.
 */
import { describe, expect, it, vi } from 'vitest';
import { httpSource, OpsRequestError } from '@sigx/actors-cli/source';

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
    health: { live: true, ready: true, uptimeMs: 65_000, checks: {}, silo: null },
    ops: { metrics: { calls: { total: 10, failed: 1, streams: 0 } } }
};

const clusterBody = {
    at: 1_700_000_000_000,
    from: 'silo-a',
    view: { version: 4, size: 2, active: 2 },
    silos: [
        {
            v: 1,
            siloId: 'silo-a',
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
            transports: ['http']
        }
    ],
    unreachable: [{ siloId: 'silo-b', address: 'http://b:3000', reason: 'timeout', message: 'x' }],
    partial: true,
    totals: { silos: 2, activations: 2, queued: 0, perType: {}, counters: {} },
    reminderShards: { p0: ['silo-a'], p1: [] }
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
    it('reads a single-node silo, with no cluster', async () => {
        const source = httpSource({
            url: 'http://silo.test',
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
        expect(snapshot.silos).toHaveLength(1);
        expect(snapshot.silos[0]!.status).toBe('unknown');
        expect(snapshot.silos[0]!.stats.activations).toBe(3);
        expect(snapshot.activations).toHaveLength(1);
        expect(snapshot.health?.ready).toBe(true);
        expect(snapshot.metrics?.calls.total).toBe(10);
    });

    it('prefers the cluster fan-out for the silo list, and carries partial through', async () => {
        const source = httpSource({
            url: 'http://silo.test',
            secret: SECRET,
            fetch: stubFetch({
                '/_sigx/ops': { status: 200, body: opsBody },
                '/_sigx/ops/cluster': { status: 200, body: clusterBody }
            })
        });
        const snapshot = await source.snapshot();

        expect(snapshot.cluster?.from).toBe('silo-a');
        expect(snapshot.silos).toHaveLength(1);
        expect(snapshot.silos[0]!.siloId).toBe('silo-a');
        // membershipVersion and status are lifted OUT of counters, because
        // they are per-silo facts a total would destroy.
        expect(snapshot.silos[0]!.membershipVersion).toBe(4);
        expect(snapshot.silos[0]!.counters).not.toHaveProperty('membershipVersion');
        // The one flag that must never be smoothed over.
        expect(snapshot.partial).toBe(true);
        expect(snapshot.cluster?.unreachable).toHaveLength(1);
    });

    it('sends the bearer token on every route', async () => {
        const fetchMock = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
            const url = new URL(typeof input === 'string' ? input : input.toString());
            return url.pathname.endsWith('/cluster')
                ? new Response(null, { status: 404 })
                : new Response(JSON.stringify(opsBody), { status: 200 });
        });
        const source = httpSource({
            url: 'http://silo.test',
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
        // httpSource takes what it is given: an unauthenticated silo (a dev
        // one) is a legitimate target, so a missing secret is not an error.
        const source = httpSource({ url: 'http://silo.test' });
        expect(source.kind).toBe('http');
    });

    it('says plainly that the secret was rejected', async () => {
        const source = httpSource({
            url: 'http://silo.test',
            secret: 'wrong',
            fetch: stubFetch({ '/_sigx/ops': { status: 401 } })
        });
        // A dashboard that rendered zeroes here would send you debugging a
        // perfectly healthy cluster.
        await expect(source.snapshot()).rejects.toThrow(/rejected the ops secret/);
    });

    it('explains a 404 on the snapshot route as "ops() is not wired in"', async () => {
        const source = httpSource({
            url: 'http://silo.test',
            fetch: stubFetch({ '/_sigx/ops': { status: 404 } })
        });
        await expect(source.snapshot()).rejects.toThrow(/is not serving an ops endpoint/);
    });

    it('reports an unreachable origin rather than throwing a bare TypeError', async () => {
        const source = httpSource({
            url: 'http://silo.test',
            fetch: (() => Promise.reject(new TypeError('fetch failed'))) as typeof globalThis.fetch
        });
        await expect(source.snapshot()).rejects.toThrow(OpsRequestError);
        await expect(source.snapshot()).rejects.toThrow(/is unreachable/);
    });

    it('gives up immediately on an already-aborted signal', async () => {
        const fetchMock = vi.fn(
            async () => new Response(JSON.stringify(opsBody), { status: 200 })
        ) as unknown as typeof globalThis.fetch;
        const source = httpSource({ url: 'http://silo.test', fetch: fetchMock, timeoutMs: 30_000 });

        // `addEventListener('abort')` never fires on a signal that has
        // ALREADY aborted, so without an up-front check the request would
        // run to its full timeout and then be reported as a sick silo
        // rather than as the cancellation it was.
        await expect(source.snapshot(AbortSignal.abort())).rejects.toThrow();
        expect(fetchMock).not.toHaveBeenCalled();
    });

    it('tolerates a silo that reports no health, rather than trusting the type', async () => {
        // The restated OpsBody exists precisely because a peer on an older
        // build may omit fields; typing them required and defaulting them
        // would be the unsound half of both worlds.
        const { health: _health, ...withoutHealth } = opsBody;
        const source = httpSource({
            url: 'http://silo.test',
            fetch: stubFetch({
                '/_sigx/ops': { status: 200, body: withoutHealth },
                '/_sigx/ops/cluster': { status: 404 }
            })
        });
        const snapshot = await source.snapshot();
        expect(snapshot.health).toBeNull();
        expect(snapshot.silos[0]!.stats.activations).toBe(3);
    });

    it('honours a custom base and trims trailing slashes', async () => {
        const source = httpSource({
            url: 'http://silo.test/',
            base: '/admin/ops/',
            fetch: stubFetch({
                '/admin/ops': { status: 200, body: opsBody },
                '/admin/ops/cluster': { status: 404 }
            })
        });
        await expect(source.snapshot()).resolves.toBeTruthy();
    });

    it('labels itself by origin, so it is never ambiguous what is being watched', () => {
        expect(httpSource({ url: 'http://silo.test/' }).kind).toBe('http');
        expect(httpSource({ url: 'http://silo.test/' }).label).toBe('http://silo.test');
    });
});
