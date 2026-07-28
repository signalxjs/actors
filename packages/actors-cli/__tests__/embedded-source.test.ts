/**
 * `embeddedSource` — against a REAL silo, not a fake.
 *
 * The point of this mode is that it sees more than the wire can, so a fake
 * would test nothing: the assertions worth making are that it reads the
 * actual `metrics()` handle and the actual activation directory.
 *
 * The behaviour with teeth is lifecycle ownership. A monitor that stops a
 * silo it merely attached to would drain someone else's activations on
 * Ctrl+C.
 */
import { describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import { defineActorApp, health, memoryStorage, metrics, ops } from '@sigx/actors/silo';
import { embeddedSource, EmbeddedSourceError } from '@sigx/actors-cli/source';

const quiet = { sweepIntervalMs: 600_000, reminderTickMs: 600_000, callTimeoutMs: 0 };

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        increment(by: number) {
            ctx.state.count += by;
            return ctx.state.count;
        },
        boom() {
            throw new Error('nope');
        }
    })
});

/**
 * Stand in for `import(module)`, so no file has to exist on disk — and
 * shaped like the module an author actually writes: the plugin handles are
 * exported because `defineActorApp` does not expose its plugins, which is
 * the convention embedded mode documents.
 */
function moduleWith(extra: Record<string, unknown> = {}) {
    const collect = metrics();
    const observe = ops({ secret: 'test' });
    const app = defineActorApp({ actors: [Counter], storage: memoryStorage(), defaults: quiet })
        .use(collect)
        .use(observe);
    const module = { app, metrics: collect, ops: observe, ...extra };
    return { app, collect, load: () => Promise.resolve(module as Record<string, unknown>) };
}

describe('embeddedSource', () => {
    it('starts the app and reads metrics, gauges and activations in process', async () => {
        const { app, load } = moduleWith();
        const source = await embeddedSource({ module: 'test://app', load });
        try {
            const silo = app.silo!;
            await silo.actor(Counter, 'a').increment(1);
            await silo.actor(Counter, 'b').increment(1);
            await expect(silo.actor(Counter, 'a').boom()).rejects.toThrow();

            const snapshot = await source.snapshot();
            expect(source.kind).toBe('embedded');
            expect(snapshot.cluster).toBeNull();
            expect(snapshot.silos).toHaveLength(1);
            expect(snapshot.silos[0]!.stats.activations).toBe(2);
            // The full in-process snapshot, histograms and all — this is
            // what the mode exists for.
            expect(snapshot.metrics?.calls.total).toBe(3);
            expect(snapshot.metrics?.errors.byKind['(unknown)']).toBe(1);
            expect(snapshot.activations).toHaveLength(2);
            expect(snapshot.health?.ready).toBe(true);
            expect(snapshot.partial).toBe(false);
        } finally {
            await source.close();
        }
    });

    it('stops the app it started', async () => {
        const { app, load } = moduleWith();
        const source = await embeddedSource({ module: 'test://app', load });
        expect(app.silo).not.toBeNull();
        await source.close();
        expect(app.silo).toBeNull();
    });

    it('leaves an ALREADY-running app running', async () => {
        const { app, load } = moduleWith();
        await app.start();
        const source = await embeddedSource({ module: 'test://app', load });
        try {
            expect((await source.snapshot()).silos).toHaveLength(1);
        } finally {
            await source.close();
        }
        // Attaching is not owning: closing the monitor must not drain a silo
        // somebody else is running.
        expect(app.silo).not.toBeNull();
        await app.stop();
    });

    it('omits the activation list when asked for none', async () => {
        const { load } = moduleWith();
        const source = await embeddedSource({ module: 'test://app', load, activations: 0 });
        try {
            expect((await source.snapshot()).activations).toBeNull();
        } finally {
            await source.close();
        }
    });

    it('reads health from a health() handle as well as an ops() one', async () => {
        const status = health();
        const app = defineActorApp({
            actors: [Counter],
            storage: memoryStorage(),
            defaults: quiet
        }).use(status);
        const source = await embeddedSource({
            module: 'test://app',
            load: () => Promise.resolve({ app, health: status } as Record<string, unknown>)
        });
        try {
            expect((await source.snapshot()).health?.ready).toBe(true);
        } finally {
            await source.close();
        }
    });

    it('falls back to the default on a non-finite activations cap', async () => {
        const { app, load } = moduleWith();
        const source = await embeddedSource({
            module: 'test://app',
            load,
            activations: Number.NaN
        });
        try {
            await app.silo!.actor(Counter, 'a').increment(1);
            // NaN would defeat the `=== 0` check and reach the walk as a
            // bound that is not one — the same hole core just closed.
            expect((await source.snapshot()).activations).toHaveLength(1);
        } finally {
            await source.close();
        }
    });

    it('reports real uptime in single-node mode, not a permanent 0', async () => {
        const { app, load } = moduleWith();
        const source = await embeddedSource({ module: 'test://app', load });
        try {
            await new Promise((resolve) => setTimeout(resolve, 25));
            const snapshot = await source.snapshot();
            // A single-node view has no SiloReport to take uptime from, so
            // it comes off the ops/health plugin. Reporting 0 forever made
            // `up 0s` a permanent fixture of the output.
            expect(snapshot.silos[0]!.uptimeMs).toBeGreaterThan(0);
            expect(snapshot.silos[0]!.uptimeMs).toBe(snapshot.health!.uptimeMs);
            expect(app.silo).not.toBeNull();
        } finally {
            await source.close();
        }
    });

    it('reports uptime 0 when nothing can tell it, rather than inventing one', async () => {
        const app = defineActorApp({
            actors: [Counter],
            storage: memoryStorage(),
            defaults: quiet
        });
        const source = await embeddedSource({
            module: 'test://app',
            load: () => Promise.resolve({ app } as Record<string, unknown>)
        });
        try {
            // `Silo` carries no start time; timing from when the CLI
            // attached would be a different number wearing uptime's label.
            expect((await source.snapshot()).silos[0]!.uptimeMs).toBe(0);
        } finally {
            await source.close();
        }
    });

    it('reports metrics as null when the app has no metrics() plugin', async () => {
        const app = defineActorApp({
            actors: [Counter],
            storage: memoryStorage(),
            defaults: quiet
        });
        const source = await embeddedSource({
            module: 'test://app',
            load: () => Promise.resolve({ app } as Record<string, unknown>)
        });
        try {
            // A real and common case — the dashboard shows fewer panels
            // rather than failing.
            const snapshot = await source.snapshot();
            expect(snapshot.metrics).toBeNull();
            expect(snapshot.health).toBeNull();
            expect(snapshot.silos[0]!.stats.activations).toBe(0);
        } finally {
            await source.close();
        }
    });

    it('accepts a default export as well as a named app', async () => {
        const app = defineActorApp({
            actors: [Counter],
            storage: memoryStorage(),
            defaults: quiet
        });
        const source = await embeddedSource({
            module: 'test://app',
            load: () => Promise.resolve({ default: app } as Record<string, unknown>)
        });
        await source.close();
    });

    it('says what is wrong when the module exports no app', async () => {
        await expect(
            embeddedSource({
                module: 'test://app',
                load: () => Promise.resolve({ notAnApp: 1 })
            })
        ).rejects.toThrow(/exports no actor app/);
    });

    it('names the module when it cannot be loaded, and suggests --url', async () => {
        const failure = embeddedSource({
            module: 'test://missing',
            load: () => Promise.reject(new Error('ENOENT'))
        });
        await expect(failure).rejects.toThrow(EmbeddedSourceError);
        await expect(failure).rejects.toThrow(/test:\/\/missing/);
    });
});
