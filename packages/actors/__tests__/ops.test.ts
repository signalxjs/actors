/**
 * `ops()` — the authenticated ops endpoint, and the `reportOps` seam under
 * it.
 *
 * Two invariants carry most of these tests.
 *
 * **It cannot ship open.** Unlike `/_sigx/health`, this endpoint names your
 * actor types and describes your cluster, so "no secret configured" must be
 * a construction error rather than a permissive default. The dev escape
 * hatch is deliberate and warns.
 *
 * **A broken plugin costs its own section and nothing else.** This is the
 * endpoint an operator opens when a host is already misbehaving. If one
 * throwing provider could 500 it, it would be reliably unavailable at
 * exactly the moment it matters.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { defineActor } from '@sigx/actors';
import {
    defineActorApp,
    health,
    memoryStorage,
    metrics,
    ops,
    type ActorPlugin,
    type OpsSnapshot
} from '@sigx/actors/host';
import { createFetchHandler } from '@sigx/actors/server';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };
const SECRET = 'ops-secret';

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        },
        async boom() {
            throw new Error('nope');
        }
    })
});

/** A plugin contributing one ops section under our control. */
function section(name: string, provider: () => unknown): ActorPlugin {
    return { name: `section:${name}`, setup: (registry) => registry.reportOps(name, provider) };
}

function appWith(...plugins: ActorPlugin[]) {
    let app = defineActorApp({ actors: [Counter], storage: memoryStorage(), defaults: quiet });
    for (const plugin of plugins) app = app.use(plugin);
    return app;
}

const get = (
    handler: (request: Request) => Promise<Response>,
    path: string,
    init?: { method?: string; secret?: string | null }
): Promise<Response> =>
    handler(
        new Request(`http://host.test${path}`, {
            method: init?.method ?? 'GET',
            headers:
                init?.secret === null
                    ? {}
                    : { authorization: `Bearer ${init?.secret ?? SECRET}` }
        })
    );

afterEach(() => {
    vi.restoreAllMocks();
});

describe('ops(): construction', () => {
    it('refuses to construct without a secret when __DEV__ is false', () => {
        vi.stubGlobal('__DEV__', false);
        try {
            expect(() => ops()).toThrow(/requires a secret outside development/);
        } finally {
            vi.unstubAllGlobals();
        }
    });

    it('serves unauthenticated in dev, but says so once', () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        ops();
        expect(warn).toHaveBeenCalledTimes(1);
        expect(warn.mock.calls[0]![0]).toMatch(/UNAUTHENTICATED/);
    });

    it('rejects a base without a leading slash', () => {
        // It would simply never match a pathname — a silent no-endpoint.
        expect(() => ops({ secret: SECRET, base: '_sigx/ops' })).toThrow(/must start with "\/"/);
    });
});

describe('ops(): the snapshot endpoint', () => {
    it('serves the snapshot with no-store, including perType', async () => {
        const app = appWith(ops({ secret: SECRET }));
        const handler = createFetchHandler(app);
        const host = await app.start();
        try {
            await host.actor(Counter, 'a').increment(1);
            const response = await get(handler, '/_sigx/ops');
            expect(response.status).toBe(200);
            expect(response.headers.get('cache-control')).toBe('no-store');
            const body = (await response.json()) as OpsSnapshot;
            expect(body.v).toBe(1);
            expect(body.health.live).toBe(true);
            expect(body.health.ready).toBe(true);
            // The whole reason this is a separate, authenticated route:
            // health deliberately withholds the per-type breakdown.
            expect(body.stats.perType).toEqual({ Counter: 1 });
            expect(body.uptimeMs).toBeGreaterThanOrEqual(0);
        } finally {
            await app.stop();
        }
    });

    it('carries the hottest activations, bounded and most-queued first', async () => {
        const app = appWith(ops({ secret: SECRET }));
        const handler = createFetchHandler(app);
        const host = await app.start();
        try {
            for (const key of ['a', 'b']) await host.actor(Counter, key).increment(1);
            const body = (await (await get(handler, '/_sigx/ops')).json()) as OpsSnapshot;
            expect(body.activations).toHaveLength(2);
            expect(body.activations.map((a) => a.key).sort()).toEqual(['a', 'b']);
            expect(body.stats.transitional).toEqual({ activating: 0, deactivating: 0 });
        } finally {
            await app.stop();
        }
    });

    it('honours the activations cap, and 0 omits the list entirely', async () => {
        const capped = appWith(ops({ secret: SECRET, activations: 1 }));
        const cappedHandler = createFetchHandler(capped);
        const hostA = await capped.start();
        try {
            for (const key of ['a', 'b', 'c']) await hostA.actor(Counter, key).increment(1);
            const body = (await (await get(cappedHandler, '/_sigx/ops')).json()) as OpsSnapshot;
            expect(body.activations).toHaveLength(1);
            // The totals still see all three — only the LIST is bounded.
            expect(body.stats.activations).toBe(3);
        } finally {
            await capped.stop();
        }

        const off = appWith(ops({ secret: SECRET, activations: 0 }));
        const offHandler = createFetchHandler(off);
        const hostB = await off.start();
        try {
            await hostB.actor(Counter, 'a').increment(1);
            // Actor keys are the one field here that can be personal data,
            // so turning the list off must not cost you the rest.
            const body = (await (await get(offHandler, '/_sigx/ops')).json()) as OpsSnapshot;
            expect(body.activations).toEqual([]);
            expect(body.stats.activations).toBe(1);
        } finally {
            await off.stop();
        }
    });

    it('falls back to the default on a non-finite activations cap', async () => {
        // Same NaN hole as `host.activations({ limit })`: it would defeat
        // the `=== 0` check and pass NaN straight through to the walk.
        const app = appWith(ops({ secret: SECRET, activations: Number.NaN }));
        const handler = createFetchHandler(app);
        const host = await app.start();
        try {
            for (let i = 0; i < 25; i++) await host.actor(Counter, `k${i}`).increment(1);
            const body = (await (await get(handler, '/_sigx/ops')).json()) as OpsSnapshot;
            expect(body.activations).toHaveLength(20);
        } finally {
            await app.stop();
        }
    });

    it('401s on a missing or wrong bearer, and advertises the scheme', async () => {
        const app = appWith(ops({ secret: SECRET }));
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const missing = await get(handler, '/_sigx/ops', { secret: null });
            expect(missing.status).toBe(401);
            expect(missing.headers.get('www-authenticate')).toBe('Bearer');

            const wrong = await get(handler, '/_sigx/ops', { secret: 'not-it' });
            expect(wrong.status).toBe(401);
            // Same answer for both: telling an unauthenticated caller which
            // half they got right is a free hint.
            expect(await wrong.json()).toEqual(await missing.json());
        } finally {
            await app.stop();
        }
    });

    it('405s a POST, with Allow', async () => {
        const app = appWith(ops({ secret: SECRET }));
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const response = await get(handler, '/_sigx/ops', { method: 'POST' });
            expect(response.status).toBe(405);
            expect(response.headers.get('allow')).toBe('GET, HEAD');
        } finally {
            await app.stop();
        }
    });

    it('serves 503 before start, like every other route', async () => {
        const app = appWith(ops({ secret: SECRET }));
        const handler = createFetchHandler(app);
        // Not started: a route only runs on a started host, so serving at
        // all is the liveness signal.
        const response = await get(handler, '/_sigx/ops');
        expect(response.status).toBe(503);
    });

    it('answers a HEAD with no body', async () => {
        const app = appWith(ops({ secret: SECRET }));
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const response = await get(handler, '/_sigx/ops', { method: 'HEAD' });
            expect(response.status).toBe(200);
            expect(await response.text()).toBe('');
        } finally {
            await app.stop();
        }
    });

    it('honours a custom base', async () => {
        const app = appWith(ops({ secret: SECRET, base: '/admin/ops/' }));
        const handler = createFetchHandler(app);
        await app.start();
        try {
            expect((await get(handler, '/admin/ops')).status).toBe(200);
            expect((await get(handler, '/_sigx/ops')).status).toBe(404);
        } finally {
            await app.stop();
        }
    });
});

describe('ops(): the reportOps seam', () => {
    it('collects every contributed section by name', async () => {
        const app = appWith(
            section('alpha', () => ({ n: 1 })),
            ops({ secret: SECRET }),
            // Registered AFTER ops(), to pin that the aggregate is read at
            // request time rather than captured at setup.
            section('beta', () => ({ n: 2 }))
        );
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const body = (await (await get(handler, '/_sigx/ops')).json()) as OpsSnapshot;
            expect(body.ops).toEqual({ alpha: { n: 1 }, beta: { n: 2 } });
        } finally {
            await app.stop();
        }
    });

    it('calls providers per read, not once at setup', async () => {
        let n = 0;
        const app = appWith(
            section('counter', () => ({ n: ++n })),
            ops({ secret: SECRET })
        );
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const first = (await (await get(handler, '/_sigx/ops')).json()) as OpsSnapshot;
            const second = (await (await get(handler, '/_sigx/ops')).json()) as OpsSnapshot;
            expect(first.ops.counter).toEqual({ n: 1 });
            expect(second.ops.counter).toEqual({ n: 2 });
        } finally {
            await app.stop();
        }
    });

    it('reports a throwing provider under its own name and keeps the rest', async () => {
        const app = appWith(
            section('good', () => ({ ok: true })),
            section('bad', () => {
                throw new Error('provider exploded');
            }),
            ops({ secret: SECRET })
        );
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const response = await get(handler, '/_sigx/ops');
            // Still 200: the tool that explains a broken host must not be
            // broken BY it.
            expect(response.status).toBe(200);
            const body = (await response.json()) as OpsSnapshot;
            expect(body.ops.good).toEqual({ ok: true });
            expect(body.ops.bad).toEqual({ error: 'provider exploded' });
        } finally {
            await app.stop();
        }
    });

    it('normalizes an undefined section to null, so the key survives JSON', async () => {
        const app = appWith(
            section('silent', () => undefined),
            ops({ secret: SECRET })
        );
        const handler = createFetchHandler(app);
        await app.start();
        try {
            // `JSON.stringify({ silent: undefined })` drops the key outright,
            // so a provider that returns nothing would erase its own section
            // from the wire — the exact failure the throwing-provider catch
            // exists to prevent.
            const body = (await (await get(handler, '/_sigx/ops')).json()) as OpsSnapshot;
            expect(Object.keys(body.ops)).toContain('silent');
            expect(body.ops.silent).toBeNull();
        } finally {
            await app.stop();
        }
    });

    it('keeps a failing section PRESENT — an absent key would read as "contributes nothing"', async () => {
        const app = appWith(
            section('bad', () => {
                throw new Error('x');
            }),
            ops({ secret: SECRET })
        );
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const body = (await (await get(handler, '/_sigx/ops')).json()) as OpsSnapshot;
            expect(Object.keys(body.ops)).toContain('bad');
        } finally {
            await app.stop();
        }
    });

    it('throws at setup when two plugins claim the same section name', async () => {
        const app = appWith(
            section('dup', () => 1),
            section('dup', () => 2)
        );
        // Names key the snapshot, so a duplicate would silently hand a
        // consumer somebody else's numbers.
        await expect(app.start()).rejects.toThrow(/both contributed an ops section named "dup"/);
    });

    it('throws at setup on an empty section name', async () => {
        const app = appWith(section('   ', () => 1));
        await expect(app.start()).rejects.toThrow(/empty name/);
    });
});

describe('ops(): the cluster fan-out route', () => {
    it('404s when no collector is wired', async () => {
        const app = appWith(ops({ secret: SECRET }));
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const response = await get(handler, '/_sigx/ops/cluster');
            expect(response.status).toBe(404);
            expect(JSON.stringify(await response.json())).toMatch(/reports no cluster/);
        } finally {
            await app.stop();
        }
    });

    it('serves the collector result when wired', async () => {
        const report = { at: 1, from: 'host-a', partial: false };
        const app = appWith(ops({ secret: SECRET, cluster: () => Promise.resolve(report) }));
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const response = await get(handler, '/_sigx/ops/cluster');
            expect(response.status).toBe(200);
            expect(await response.json()).toEqual(report);
        } finally {
            await app.stop();
        }
    });

    it('passes ?detail through to the collector', async () => {
        const asked: unknown[] = [];
        const app = appWith(
            ops({
                secret: SECRET,
                cluster: (_signal, query) => {
                    asked.push(query);
                    return Promise.resolve({});
                }
            })
        );
        const handler = createFetchHandler(app);
        await app.start();
        try {
            await get(handler, '/_sigx/ops/cluster');
            await get(handler, '/_sigx/ops/cluster?detail=1');
            await get(handler, '/_sigx/ops/cluster?detail=1&activations=5&host=a&host=b');
            // Absent means the cheap report — an upgraded COLLECTOR is what
            // grows the traffic, never an upgraded host.
            expect(asked[0]).toEqual({});
            expect(asked[1]).toEqual({ detail: true });
            expect(asked[2]).toEqual({ detail: { activations: 5, hosts: ['a', 'b'] } });
        } finally {
            await app.stop();
        }
    });

    it('falls back rather than through on a nonsense query', async () => {
        const asked: unknown[] = [];
        const app = appWith(
            ops({
                secret: SECRET,
                cluster: (_signal, query) => {
                    asked.push(query);
                    return Promise.resolve({});
                }
            })
        );
        const handler = createFetchHandler(app);
        await app.start();
        try {
            // A typo in a query string must not silently turn a 1 Hz poll
            // into a fleet-wide actor walk, and must not 500 either.
            // Detail is opted INTO: anything unrecognised is off, because
            // the expensive direction is the one you have to ask for
            // precisely.
            await get(handler, '/_sigx/ops/cluster?detail=0');
            await get(handler, '/_sigx/ops/cluster?detail=banana');
            await get(handler, '/_sigx/ops/cluster?detail=flase');
            await get(handler, '/_sigx/ops/cluster?detail=1&activations=banana');
            await get(handler, '/_sigx/ops/cluster?detail=TRUE');
            expect(asked[0]).toEqual({});
            expect(asked[1]).toEqual({});
            expect(asked[2]).toEqual({});
            expect(asked[3]).toEqual({ detail: true });
            expect(asked[4]).toEqual({ detail: true });
        } finally {
            await app.stop();
        }
    });

    it('still serves a collector written to the old one-argument signature', async () => {
        // The parameter was APPENDED, so every existing
        // `(signal) => clusterStats(placement)` keeps compiling and working.
        const app = appWith(ops({ secret: SECRET, cluster: () => Promise.resolve({ ok: true }) }));
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const response = await get(handler, '/_sigx/ops/cluster?detail=1');
            expect(response.status).toBe(200);
            expect(await response.json()).toEqual({ ok: true });
        } finally {
            await app.stop();
        }
    });

    it('requires the bearer on the cluster route too', async () => {
        const app = appWith(ops({ secret: SECRET, cluster: () => Promise.resolve({}) }));
        const handler = createFetchHandler(app);
        await app.start();
        try {
            expect((await get(handler, '/_sigx/ops/cluster', { secret: null })).status).toBe(401);
        } finally {
            await app.stop();
        }
    });

    it('404s an unauthenticated probe with 401, so paths cannot be enumerated', async () => {
        // Auth runs BEFORE the path split: an unauthenticated caller must
        // not be able to tell "/cluster is not enabled here" from "/cluster
        // exists", which would map the deployment for free.
        const app = appWith(ops({ secret: SECRET }));
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const response = await get(handler, '/_sigx/ops/cluster', { secret: null });
            expect(response.status).toBe(401);
        } finally {
            await app.stop();
        }
    });

    it('503s with the reason when the collector itself fails', async () => {
        const app = appWith(
            ops({
                secret: SECRET,
                cluster: () => Promise.reject(new Error('membership unavailable'))
            })
        );
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const response = await get(handler, '/_sigx/ops/cluster');
            // Not an empty report — that would read as a healthy cluster of
            // zero hosts, which is the one answer that is never true.
            expect(response.status).toBe(503);
            expect(JSON.stringify(await response.json())).toMatch(/membership unavailable/);
        } finally {
            await app.stop();
        }
    });
});

describe('ops(): what the plugins publish', () => {
    it('carries the metrics snapshot under "metrics"', async () => {
        const app = appWith(metrics(), ops({ secret: SECRET }));
        const handler = createFetchHandler(app);
        const host = await app.start();
        try {
            await host.actor(Counter, 'a').increment(1);
            await expect(host.actor(Counter, 'a').boom()).rejects.toThrow();
            const body = (await (await get(handler, '/_sigx/ops')).json()) as OpsSnapshot;
            const collected = body.ops.metrics as { calls: { total: number; failed: number } };
            expect(collected.calls.total).toBe(2);
            expect(collected.calls.failed).toBe(1);
        } finally {
            await app.stop();
        }
    });

    it('needs no metrics() — the section is simply absent', async () => {
        const app = appWith(ops({ secret: SECRET }));
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const body = (await (await get(handler, '/_sigx/ops')).json()) as OpsSnapshot;
            expect(body.ops).toEqual({});
        } finally {
            await app.stop();
        }
    });

    it('reports the health aggregate without health() being mounted', async () => {
        // `ops()` reads `registry.health()` directly, so the readiness seam
        // is visible whether or not the probe endpoints are served.
        const gate: ActorPlugin = {
            name: 'gate',
            setup: (registry) =>
                registry.reportHealth('gate', () => ({ ready: false, detail: 'draining' }))
        };
        const app = appWith(gate, ops({ secret: SECRET }));
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const body = (await (await get(handler, '/_sigx/ops')).json()) as OpsSnapshot;
            expect(body.health.ready).toBe(false);
            expect(body.health.checks.gate).toEqual({ ready: false, detail: 'draining' });
        } finally {
            await app.stop();
        }
    });

    it('agrees with health() when both are mounted', async () => {
        const app = appWith(health(), ops({ secret: SECRET }));
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const ready = await get(handler, '/_sigx/health/ready', { secret: null });
            const body = (await (await get(handler, '/_sigx/ops')).json()) as OpsSnapshot;
            expect(ready.status).toBe(200);
            expect(body.health.ready).toBe(true);
        } finally {
            await app.stop();
        }
    });

    it('snapshot() gives the same answer in process', async () => {
        const plugin = ops({ secret: SECRET });
        const app = appWith(plugin);
        const host = await app.start();
        try {
            await host.actor(Counter, 'a').increment(1);
            const inProcess = plugin.snapshot();
            expect(inProcess.v).toBe(1);
            expect(inProcess.stats.perType).toEqual({ Counter: 1 });
            expect(inProcess.health.live).toBe(true);
        } finally {
            await app.stop();
        }
    });
});
