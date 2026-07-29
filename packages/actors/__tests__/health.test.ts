/**
 * `health()` — the liveness/readiness endpoints and the readiness seam
 * underneath them.
 *
 * The invariant these pin is operational, not functional: **liveness and
 * readiness must disagree during a drain.** A silo that has announced
 * `leaving` is perfectly alive (killing it would abort the handoff) and
 * must not be receiving traffic. Every test below exists to keep those two
 * answers from collapsing into one.
 */
import { describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import {
    defineActorApp,
    health,
    memoryStorage,
    type ActorPlugin,
    type HealthCheck
} from '@sigx/actors/silo';
import { createFetchHandler } from '@sigx/actors/server';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        }
    })
});

/** A plugin that contributes one readiness check under our control. */
function gate(name: string, check: () => HealthCheck): ActorPlugin {
    return { name, setup: (registry) => registry.reportHealth(name, check) };
}

function appWith(...plugins: ActorPlugin[]) {
    let app = defineActorApp({ actors: [Counter], storage: memoryStorage(), defaults: quiet });
    for (const plugin of plugins) app = app.use(plugin);
    return app;
}

const get = (
    handler: (request: Request) => Promise<Response>,
    path: string,
    method = 'GET'
): Promise<Response> => handler(new Request(`http://silo.test${path}`, { method }));

describe('health: liveness and readiness', () => {
    it('serves 200 live / 200 ready once started, and no-store on both', async () => {
        const app = appWith(health());
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const live = await get(handler, '/_sigx/health');
            expect(live.status).toBe(200);
            expect(live.headers.get('cache-control')).toBe('no-store');
            expect(await live.json()).toMatchObject({ status: 'live' });

            const ready = await get(handler, '/_sigx/health/ready');
            expect(ready.status).toBe(200);
            expect(ready.headers.get('cache-control')).toBe('no-store');
            expect(await ready.json()).toMatchObject({ status: 'ready' });
        } finally {
            await app.stop();
        }
    });

    it('an app with NO checks is ready — a single-node silo needs no wiring', async () => {
        const app = appWith(health());
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const ready = await get(handler, '/_sigx/health/ready');
            expect(ready.status).toBe(200);
            expect((await ready.json()).checks).toEqual({});
        } finally {
            await app.stop();
        }
    });

    it('a failing check takes the silo out of rotation and names itself', async () => {
        const app = appWith(
            health(),
            gate('warmup', () => ({ ready: false, detail: 'cache cold' }))
        );
        const handler = createFetchHandler(app);
        await app.start();
        try {
            // Liveness is deliberately unmoved: the process is fine.
            expect((await get(handler, '/_sigx/health')).status).toBe(200);

            const ready = await get(handler, '/_sigx/health/ready');
            expect(ready.status).toBe(503);
            const body = await ready.json();
            expect(body.status).toBe('not-ready');
            expect(body.checks.warmup).toEqual({ ready: false, detail: 'cache cold' });
        } finally {
            await app.stop();
        }
    });

    it('every failing check is reported, not just the first', async () => {
        const app = appWith(
            health(),
            gate('a', () => ({ ready: false, detail: 'a down' })),
            gate('b', () => ({ ready: false, detail: 'b down' }))
        );
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const body = await (await get(handler, '/_sigx/health/ready')).json();
            expect(body.checks.a.detail).toBe('a down');
            expect(body.checks.b.detail).toBe('b down');
        } finally {
            await app.stop();
        }
    });

    it('a THROWING check is not-ready, never a 500 — a broken probe must still answer', async () => {
        const app = appWith(
            health(),
            gate('boom', () => {
                throw new Error('provider exploded');
            })
        );
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const ready = await get(handler, '/_sigx/health/ready');
            expect(ready.status).toBe(503);
            expect((await ready.json()).checks.boom).toEqual({
                ready: false,
                detail: 'provider exploded'
            });
        } finally {
            await app.stop();
        }
    });

    it('a check that throws a non-Error still reports its reason', async () => {
        const app = appWith(
            health(),
            gate('rude', () => {
                // eslint-disable-next-line no-throw-literal
                throw 'not an Error at all';
            })
        );
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const body = await (await get(handler, '/_sigx/health/ready')).json();
            expect(body.checks.rude).toEqual({ ready: false, detail: 'not an Error at all' });
        } finally {
            await app.stop();
        }
    });

    it('refuses a duplicate or empty check name, naming both plugins', () => {
        // Names key the report, so an overwrite could hide a FAILING check
        // behind a passing one — the one thing readiness must never do.
        expect(() =>
            appWith(
                gate('storage', () => ({ ready: true })),
                { name: 'other', setup: (r) => r.reportHealth('storage', () => ({ ready: false })) },
                health()
            ).routes
        ).toThrow(/both contributed a readiness check named "storage".*storage.*other/s);

        expect(() =>
            appWith({ name: 'blank', setup: (r) => r.reportHealth('  ', () => ({ ready: true })) })
                .routes
        ).toThrow(/empty name/);
    });

    it('never publishes a perType breakdown — the endpoint cannot be authenticated', async () => {
        const plugin = health();
        const app = appWith(plugin);
        const handler = createFetchHandler(app);
        const silo = await app.start();
        try {
            await silo.actor(Counter, 'a').increment(1);
            const body = await (await get(handler, '/_sigx/health/ready')).json();
            expect(body.silo).toEqual({ activations: 1, queued: 0 });
            expect(JSON.stringify(body)).not.toContain('Counter');
            expect(plugin.status().silo).toEqual({ activations: 1, queued: 0 });
        } finally {
            await app.stop();
        }
    });

    it('sees checks registered AFTER it — .use() order does not matter', async () => {
        // health() first, the contributor second: the aggregate is read per
        // request, so registration order cannot hide a check.
        const app = appWith(
            health(),
            gate('later', () => ({ ready: false }))
        );
        const handler = createFetchHandler(app);
        await app.start();
        try {
            expect((await get(handler, '/_sigx/health/ready')).status).toBe(503);
        } finally {
            await app.stop();
        }
    });

    it('detail:false keeps topology out of an unauthenticated probe body', async () => {
        const app = appWith(
            health({ detail: false }),
            gate('warmup', () => ({ ready: false, detail: 'cache cold' }))
        );
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const body = await (await get(handler, '/_sigx/health/ready')).json();
            expect(body.status).toBe('not-ready');
            expect(body.checks).toBeUndefined();
            expect(body.silo).toBeUndefined();
        } finally {
            await app.stop();
        }
    });

    it('HEAD answers with the status and no body; other methods are 405', async () => {
        const app = appWith(health());
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const head = await get(handler, '/_sigx/health/ready', 'HEAD');
            expect(head.status).toBe(200);
            expect(await head.text()).toBe('');

            const post = await get(handler, '/_sigx/health', 'POST');
            expect(post.status).toBe(405);
            expect(post.headers.get('allow')).toBe('GET, HEAD');
        } finally {
            await app.stop();
        }
    });

    it('honours a custom base and rejects one without a leading slash', async () => {
        expect(() => health({ base: 'healthz' })).toThrow(/must start with "\/"/);

        const app = appWith(health({ base: '/healthz' }));
        const handler = createFetchHandler(app);
        await app.start();
        try {
            expect((await get(handler, '/healthz')).status).toBe(200);
            expect((await get(handler, '/healthz/ready')).status).toBe(200);
        } finally {
            await app.stop();
        }
    });

    it('status() gives the same answer in process, and reports gauges', async () => {
        const plugin = health();
        const app = appWith(plugin);
        expect(plugin.status()).toMatchObject({ live: false, ready: false, silo: null });

        const silo = await app.start();
        try {
            await silo.actor(Counter, 'a').increment(1);
            const status = plugin.status();
            expect(status.live).toBe(true);
            expect(status.ready).toBe(true);
            expect(status.silo).toEqual({ activations: 1, queued: 0 });
        } finally {
            await app.stop();
        }
        // The silo handle is released on stop — health must not pin it.
        expect(plugin.status()).toMatchObject({ live: false, ready: false, silo: null });
    });

    it('before start the whole mount is 503 — the entry answers, not the route', async () => {
        // Documented and deliberate: a route only ever runs on a started
        // silo, so serving at all IS the liveness signal. Use a k8s
        // startupProbe to cover a slow boot.
        const app = appWith(health());
        const handler = createFetchHandler(app);
        const live = await get(handler, '/_sigx/health');
        expect(live.status).toBe(503);
        expect((await live.json()).error.message).toMatch(/not running/);
    });
});

describe('health: fatal checks fail liveness', () => {
    // The third state next to alive-and-ready and alive-but-draining:
    // a check can declare the process UNRECOVERABLE (`fatal: true`), and
    // then liveness — not just readiness — must fail, because a restart is
    // the only medicine and the kubelet is the one holding it. Found on
    // AKS (#141): a fenced silo sat live-200/ready-503 forever and the
    // cluster stayed dead until a human restarted the pods.
    it('ready:false alone keeps liveness 200 (drain, do not restart)', async () => {
        const plugin = health();
        const app = appWith(gate('draining', () => ({ ready: false, detail: 'leaving' })), plugin);
        const handler = createFetchHandler(app);
        await app.start();
        try {
            expect((await get(handler, '/_sigx/health')).status).toBe(200);
            expect((await get(handler, '/_sigx/health/ready')).status).toBe(503);
            expect(plugin.status().fatal).toBe(false);
        } finally {
            await app.stop();
        }
    });

    it('fatal:true fails liveness AND readiness, and names the check', async () => {
        let broken = false;
        const plugin = health();
        const app = appWith(
            gate('membership', () =>
                broken
                    ? { ready: false, fatal: true, detail: 'fenced' }
                    : { ready: true, detail: 'active' }
            ),
            plugin
        );
        const handler = createFetchHandler(app);
        await app.start();
        try {
            expect((await get(handler, '/_sigx/health')).status).toBe(200);

            broken = true;
            const live = await get(handler, '/_sigx/health');
            expect(live.status).toBe(503);
            expect((await live.json()).status).toBe('fatal');
            expect((await get(handler, '/_sigx/health/ready')).status).toBe(503);
            const status = plugin.status();
            expect(status.fatal).toBe(true);
            expect(status.checks.membership).toMatchObject({ fatal: true });
        } finally {
            await app.stop();
        }
    });

    it('a HEAD liveness probe carries the fatal status code with no body', async () => {
        const plugin = health();
        const app = appWith(gate('membership', () => ({ ready: false, fatal: true })), plugin);
        const handler = createFetchHandler(app);
        await app.start();
        try {
            const live = await get(handler, '/_sigx/health', 'HEAD');
            expect(live.status).toBe(503);
            expect(await live.text()).toBe('');
        } finally {
            await app.stop();
        }
    });
});
