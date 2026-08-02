/**
 * `reportDigest` / `registry.digest()` — the seam that lets `cluster()` put
 * a host's mergeable metrics into its `HostReport`.
 *
 * It exists as a SECOND seam rather than a read of the existing ops section
 * for two reasons, and both are asserted here: the ops section carries
 * derived percentiles (the un-mergeable shape), and reading it from
 * `report()` would re-enter `report()` itself, because `cluster()`
 * publishes that report AS an ops section.
 */
import { describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import { defineActorApp, memoryStorage, metrics, type ActorPlugin } from '@sigx/actors/host';

const Noop = defineActor({
    type: 'Noop',
    unguarded: true,
    state: () => ({ n: 0 }),
    methods: (ctx) => ({
        async bump() {
            ctx.state.n++;
            return ctx.state.n;
        }
    })
});

/** A plugin that captures the registry so a test can poke the seam. */
function probe(onSetup: (registry: Parameters<ActorPlugin['setup']>[0]) => void): ActorPlugin {
    return { name: 'probe', setup: onSetup };
}

const appWith = (...plugins: ActorPlugin[]) => {
    let app = defineActorApp({ actors: [Noop], storage: memoryStorage() });
    for (const plugin of plugins) app = app.use(plugin);
    return app;
};

describe('the digest seam', () => {
    it('reads a named digest, live, whatever the .use() order', async () => {
        let read: (() => unknown) | null = null;
        // Registered BEFORE `metrics()`, so a reader that captured the
        // providers at setup time would see nothing.
        const app = appWith(
            probe((registry) => {
                read = () => registry.digest('metrics');
            }),
            metrics()
        );
        const host = await app.start();
        try {
            await host.actor(Noop, 'a').bump();
            const digest = read!() as { v: number; calls: { total: number } };
            expect(digest.v).toBe(1);
            expect(digest.calls.total).toBe(1);
        } finally {
            await app.stop();
        }
    });

    it('answers undefined when nobody publishes that name', async () => {
        let read: (() => unknown) | null = null;
        const app = appWith(
            probe((registry) => {
                read = () => registry.digest('metrics');
            })
        );
        const host = await app.start();
        try {
            // A host with no `metrics()` is the ordinary case, not an error
            // — which is why the field is simply absent from its report.
            expect(read!()).toBeUndefined();
            expect(host).toBeDefined();
        } finally {
            await app.stop();
        }
    });

    it('never reaches an ops provider', async () => {
        // The structural guarantee. `cluster()` publishes `placement.report()`
        // as the `'cluster'` ops section, and `report()` is what calls
        // `digest()` — so if this walked ops providers it would recurse
        // until the stack gave out.
        let opsCalls = 0;
        let read: (() => unknown) | null = null;
        const app = appWith(
            probe((registry) => {
                registry.reportOps('trap', () => {
                    opsCalls++;
                    return {};
                });
                read = () => registry.digest('trap');
            }),
            metrics()
        );
        const host = await app.start();
        try {
            expect(read!()).toBeUndefined();
            expect(opsCalls).toBe(0);
            expect(host).toBeDefined();
        } finally {
            await app.stop();
        }
    });

    it('costs a throwing provider its own digest and nothing else', async () => {
        let read: ((name: string) => unknown) | null = null;
        const app = appWith(
            probe((registry) => {
                registry.reportDigest('broken', () => {
                    throw new Error('nope');
                });
                read = (name: string) => registry.digest(name);
            }),
            metrics()
        );
        const host = await app.start();
        try {
            // The one tool that explains a broken host must not be broken
            // BY it.
            expect(read!('broken')).toBeUndefined();
            expect(read!('metrics')).toMatchObject({ v: 1 });
            expect(host).toBeDefined();
        } finally {
            await app.stop();
        }
    });

    it('breaks a self-referential provider without throwing at the reader', async () => {
        // A provider that reads its own digest is a bug, but it is ITS bug:
        // throwing here would take out the surrounding ops or cluster read,
        // which is the same mistake as letting a failing section 500 the
        // endpoint that exists to explain a sick host.
        let read: (() => unknown) | null = null;
        const app = appWith(
            probe((registry) => {
                registry.reportDigest('loop', () => registry.digest('loop'));
                read = () => registry.digest('loop');
            }),
            metrics()
        );
        const host = await app.start();
        try {
            expect(() => read!()).not.toThrow();
            expect(read!()).toBeUndefined();
            // …and the reader is usable again afterwards, so one bad
            // provider does not poison the next read.
            expect(read!()).toBeUndefined();
            expect(host).toBeDefined();
        } finally {
            await app.stop();
        }
    });

    it('refuses two providers with the same name, naming both plugins', async () => {
        const first: ActorPlugin = {
            name: 'first',
            setup: (registry) => registry.reportDigest('metrics', () => ({}))
        };
        const second: ActorPlugin = {
            name: 'second',
            setup: (registry) => registry.reportDigest('metrics', () => ({}))
        };
        // A silent overwrite would hand the aggregator somebody else's
        // numbers under a name it trusts.
        await expect(appWith(first, second).start()).rejects.toThrow(/first.*second|second.*first/s);
    });

    it('refuses an empty name', async () => {
        const bad: ActorPlugin = {
            name: 'bad',
            setup: (registry) => registry.reportDigest('  ', () => ({}))
        };
        await expect(appWith(bad).start()).rejects.toThrow(/empty name/);
    });

    it('forwards the caller’s options to the provider', async () => {
        let seen: unknown = null;
        let read: (() => unknown) | null = null;
        const app = appWith(
            probe((registry) => {
                registry.reportDigest('spy', (options) => {
                    seen = options;
                    return { v: 1 };
                });
                read = () => registry.digest('spy', { types: 4 });
            })
        );
        const host = await app.start();
        try {
            read!();
            expect(seen).toEqual({ types: 4 });
            expect(host).toBeDefined();
        } finally {
            await app.stop();
        }
    });
});

describe('metrics().digest()', () => {
    it('agrees with the snapshot it is derived from', async () => {
        const plugin = metrics();
        const app = appWith(plugin);
        const host = await app.start();
        try {
            for (let i = 0; i < 5; i++) await host.actor(Noop, `g${i}`).bump();
            const snapshot = plugin.snapshot();
            const digest = plugin.digest();
            expect(digest.calls.total).toBe(snapshot.calls.total);
            expect(digest.calls.failed).toBe(snapshot.calls.failed);
            expect(digest.activations.created).toBe(snapshot.activations.created);
            expect(digest.turn?.count).toBe(snapshot.turnMs?.count);
            expect(digest.layout).toMatch(/^ll-\d+-\d+$/);
        } finally {
            await app.stop();
        }
    });

    it('keeps a capped breakdown summing to the total', async () => {
        const plugin = metrics();
        const app = appWith(plugin);
        const host = await app.start();
        try {
            for (let i = 0; i < 5; i++) await host.actor(Noop, `g${i}`).bump();
            // Two rows named, the rest folded — a breakdown whose rows
            // silently do not add up is worse than one that says "and this
            // much else".
            const digest = plugin.digest({ methods: 1 });
            const sum = Object.values(digest.byMethod).reduce((n, row) => n + row.calls, 0);
            expect(sum).toBe(digest.calls.total);
        } finally {
            await app.stop();
        }
    });

    it('leaves recent failures out unless asked', async () => {
        const plugin = metrics();
        const app = appWith(plugin);
        const host = await app.start();
        try {
            await host.actor(Noop, 'a').bump();
            // 32 messages is bigger than the rest of the digest put
            // together, and it is drill-down material rather than something
            // a total is computed from.
            expect(plugin.digest().errors.recent).toBeUndefined();
            expect(plugin.digest({ errors: 4 }).errors.recent).toEqual([]);
        } finally {
            await app.stop();
        }
    });

    it('carries no distributions when histograms are off', async () => {
        const plugin = metrics({ histograms: false });
        const app = appWith(plugin);
        const host = await app.start();
        try {
            await host.actor(Noop, 'a').bump();
            const digest = plugin.digest();
            expect(digest.latency).toBeNull();
            expect(digest.turn).toBeNull();
            // The counters are still there — that is the point of the split.
            expect(digest.calls.total).toBe(1);
        } finally {
            await app.stop();
        }
    });

    it('reading the digest does not move the counters it reads', async () => {
        const plugin = metrics();
        const app = appWith(plugin);
        const host = await app.start();
        try {
            await host.actor(Noop, 'a').bump();
            const before = plugin.digest();
            plugin.digest();
            plugin.digest();
            const after = plugin.digest();
            expect(after.calls).toEqual(before.calls);
            expect(after.turn?.count).toBe(before.turn?.count);
        } finally {
            await app.stop();
        }
    });
});
