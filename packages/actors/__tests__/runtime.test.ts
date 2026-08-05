import { describe, expect, it, vi } from 'vitest';
import {
    ActorWrongHostError,
    defineActor,
    isActorError,
    type ActorDispatcher,
    type ActorPlacement,
    type ActorStorage,
    type MigrateState,
    type PlacementBindings
} from '@sigx/actors';
import { createHost, memoryStorage, type Host } from '@sigx/actors/host';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

function counterActor(events: string[] = []) {
    return defineActor({
        type: 'Counter',
        allowAnonymous: true,
        state: () => ({ count: 0 }),
        onActivate(ctx) {
            events.push(`activate:${ctx.key}`);
        },
        onDeactivate(ctx, reason) {
            events.push(`deactivate:${ctx.key}:${reason}`);
        },
        methods: (ctx) => ({
            async increment(by: number) {
                ctx.state.count += by;
                await ctx.save();
                return ctx.state.count;
            },
            async get() {
                return ctx.state.count;
            },
            async slow(ms: number) {
                await new Promise((r) => setTimeout(r, ms));
                ctx.state.count++;
                return ctx.state.count;
            },
            async bye() {
                ctx.deactivate();
                return 'bye';
            }
        })
    });
}

describe('activation & dispatch', () => {
    it('activates lazily on first call and reuses the activation', async () => {
        const events: string[] = [];
        const host = createHost({ actors: [counterActor(events)], defaults: quiet });
        const def = counterActor();
        expect(host.stats().activations).toBe(0);
        // definitions are compared by type — use the registered instance
        const client = host.actor(def, 'a');
        await expect(client.increment(2)).resolves.toBe(2);
        await expect(client.increment(3)).resolves.toBe(5);
        expect(events.filter((e) => e.startsWith('activate'))).toEqual(['activate:a']);
        expect(host.stats()).toMatchObject({ activations: 1, perType: { Counter: 1 } });
    });

    it('two racing first calls join one activation (single-activation invariant)', async () => {
        const events: string[] = [];
        const host = createHost({ actors: [counterActor(events)], defaults: quiet });
        const client = host.actor(counterActor(), 'race');
        const [a, b] = await Promise.all([client.increment(1), client.increment(1)]);
        expect([a, b].sort()).toEqual([1, 2]);
        expect(events.filter((e) => e.startsWith('activate'))).toEqual(['activate:race']);
    });

    it('turns are serialized: no interleaving even across awaits', async () => {
        const host = createHost({ actors: [counterActor()], defaults: quiet });
        const client = host.actor(counterActor(), 'serial');
        const results = await Promise.all([client.slow(20), client.slow(1), client.slow(1)]);
        // Each turn saw the previous one fully committed.
        expect(results).toEqual([1, 2, 3]);
    });

    it('unknown method rejects with the method-not-found brand', async () => {
        const host = createHost({ actors: [counterActor()], defaults: quiet });
        const client = host.actor(counterActor(), 'x') as unknown as {
            nope(): Promise<unknown>;
        };
        await expect(client.nope()).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'method-not-found'
        );
    });

    it('a prototype member is not a method — in-process agrees with the wire', async () => {
        // `#methods[method]` walked up to Object.prototype: `toString` and
        // `constructor` ANSWERED, the rest threw a raw TypeError. Own keys
        // only, and callable, or it is not a method.
        const host = createHost({ actors: [counterActor()], defaults: quiet });
        const client = host.actor(counterActor(), 'proto') as unknown as Record<
            string,
            () => Promise<unknown>
        >;
        for (const member of ['toString', 'constructor', 'valueOf', 'hasOwnProperty']) {
            await expect(client[member]!()).rejects.toSatisfy(
                (e: unknown) => isActorError(e) && e.kind === 'method-not-found'
            );
        }
        await host.stop({ timeoutMs: 1000 });
    });

    it('a prototype member is not a STREAM either', async () => {
        // The stream table has the same shape and the same bug; the wire
        // path is shadowed by `streamNames.includes(...)`, but a direct
        // `dispatchStream` reaches it.
        const ticker = defineActor({
            type: 'Ticker',
            allowAnonymous: true,
            state: () => ({}),
            methods: () => ({ async noop() {} }),
            streams: () => ({
                async *ticks() {
                    yield 1;
                }
            })
        });
        const host = createHost({ actors: [ticker], defaults: quiet });
        const iterable = host.dispatchStream!({ type: 'Ticker', key: 'proto-s' }, 'toString', [], {
            deadline: Date.now() + 1000,
            callChain: [],
            callId: 'p.sig.0'
        });
        await expect(
            (async () => {
                for await (const _ of iterable) void _;
            })()
        ).rejects.toSatisfy((e: unknown) => isActorError(e) && e.kind === 'method-not-found');
        await host.stop({ timeoutMs: 1000 });
    });

    it('warns when a methods factory returns a class instance (its members are uncallable)', async () => {
        // Own-key resolution makes prototype methods answer "not found". That
        // shape never fully worked — `streamNames` and the Vite extractor
        // already read own keys — but a silent 404 for a method you can see
        // in the source is a bad way to learn it.
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        class Impl {
            async hello() {
                return 'hi';
            }
        }
        const classy = defineActor({
            type: 'Classy',
            allowAnonymous: true,
            state: () => ({}),
            methods: () => new Impl() as unknown as { hello(): Promise<string> }
        });
        const host = createHost({ actors: [classy], defaults: quiet });
        const client = host.actor(classy, 'c');
        await expect(client.hello()).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'method-not-found'
        );
        expect(warn.mock.calls.flat().join(' ')).toMatch(/Classy.*methods.*INHERITED/s);
        warn.mockRestore();
        await host.stop({ timeoutMs: 1000 });
    });

    it('unknown actor type rejects with a descriptive activation error', async () => {
        const host = createHost({ actors: [], defaults: quiet });
        const client = host.actor(counterActor(), 'x');
        await expect(client.get()).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'activation'
        );
    });

    it('onActivate throw fails ALL parked callers and forgets the slot', async () => {
        let attempts = 0;
        const flaky = defineActor({
            type: 'Flaky',
            allowAnonymous: true,
            state: () => ({}),
            onActivate() {
                attempts++;
                if (attempts === 1) throw new Error('first activation fails');
            },
            methods: () => ({
                async ping() {
                    return 'pong';
                }
            })
        });
        const host = createHost({ actors: [flaky], defaults: quiet });
        const client = host.actor(flaky, 'k');
        const [r1, r2] = await Promise.allSettled([client.ping(), client.ping()]);
        expect(r1.status).toBe('rejected');
        expect(r2.status).toBe('rejected');
        expect((r1 as PromiseRejectedResult).reason.kind).toBe('activation');
        // Nothing remembered: the next call activates from scratch.
        await expect(client.ping()).resolves.toBe('pong');
        expect(attempts).toBe(2);
    });
});

describe('persistence', () => {
    it('state survives deactivation via storage (save → deactivate → reload)', async () => {
        const storage = memoryStorage();
        const host = createHost({ actors: [counterActor()], storage, defaults: quiet });
        const client = host.actor(counterActor(), 'p');
        await client.increment(7);
        await host.deactivateType('Counter');
        expect(host.stats().activations).toBe(0);
        await expect(client.get()).resolves.toBe(7);
    });

    it('rich types (Date, Map) survive the storage round-trip', async () => {
        const rich = defineActor({
            type: 'Rich',
            allowAnonymous: true,
            state: () => ({ when: null as Date | null, tags: new Map<string, number>() }),
            methods: (ctx) => ({
                async stamp() {
                    ctx.state.when = new Date(1234567890000);
                    ctx.state.tags.set('a', 1);
                    await ctx.save();
                },
                async read() {
                    return {
                        isDate: ctx.state.when instanceof Date,
                        ms: ctx.state.when?.getTime(),
                        isMap: ctx.state.tags instanceof Map,
                        a: ctx.state.tags.get('a')
                    };
                }
            })
        });
        const storage = memoryStorage();
        const host = createHost({ actors: [rich], storage, defaults: quiet });
        const client = host.actor(rich, 'r');
        await client.stamp();
        await host.deactivateType('Rich');
        await expect(client.read()).resolves.toEqual({
            isDate: true,
            ms: 1234567890000,
            isMap: true,
            a: 1
        });
    });

    it('etag conflict rejects the turn, fails queued turns, and reloads fresh state', async () => {
        const storage = memoryStorage();
        const host = createHost({ actors: [counterActor()], storage, defaults: quiet });
        const client = host.actor(counterActor(), 'c');
        await client.increment(1); // etag now "1"
        // A second writer clobbers the record behind the activation's back.
        const record = await storage.load('Counter', 'c');
        await storage.save('Counter', 'c', { count: 99 }, record!.etag);

        const first = client.increment(1);
        const queued = client.increment(1);
        await expect(first).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'state-conflict'
        );
        await expect(queued).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'state-conflict'
        );
        // Fresh activation loads the winning state.
        await expect(client.get()).resolves.toBe(99);
    });

    it('write-behind persists after the debounce without explicit save()', async () => {
        const wb = defineActor({
            type: 'WB',
            allowAnonymous: true,
            state: () => ({ n: 0 }),
            persistence: { mode: 'write-behind', debounceMs: 10 },
            methods: (ctx) => ({
                async bump() {
                    ctx.state.n++;
                    return ctx.state.n;
                }
            })
        });
        const storage = memoryStorage();
        const host = createHost({ actors: [wb], storage, defaults: quiet });
        await host.actor(wb, 'w').bump();
        await vi.waitFor(async () => {
            const record = await storage.load('WB', 'w');
            expect(record).not.toBeNull();
            expect((record!.state as { n: number }).n).toBe(1);
        });
    });

    it('write-behind flushes on deactivation even before the debounce fires', async () => {
        const wb = defineActor({
            type: 'WB2',
            allowAnonymous: true,
            state: () => ({ n: 0 }),
            persistence: { mode: 'write-behind', debounceMs: 60_000 },
            methods: (ctx) => ({
                async bump() {
                    ctx.state.n++;
                }
            })
        });
        const storage = memoryStorage();
        const host = createHost({ actors: [wb], storage, defaults: quiet });
        await host.actor(wb, 'w').bump();
        await host.deactivateType('WB2');
        const record = await storage.load('WB2', 'w');
        expect((record?.state as { n: number }).n).toBe(1);
    });

    it('clearState resets to initial and deletes the stored record', async () => {
        const clearing = defineActor({
            type: 'Clearing',
            allowAnonymous: true,
            state: () => ({ n: 42 }),
            methods: (ctx) => ({
                async set(n: number) {
                    ctx.state.n = n;
                    await ctx.save();
                },
                async wipe() {
                    await ctx.clearState();
                    return ctx.state.n;
                }
            })
        });
        const storage = memoryStorage();
        const host = createHost({ actors: [clearing], storage, defaults: quiet });
        const client = host.actor(clearing, 'k');
        await client.set(7);
        await expect(client.wipe()).resolves.toBe(42);
        expect(await storage.load('Clearing', 'k')).toBeNull();
    });
});

describe('state migration', () => {
    type CartV1 = { items: string[] };
    type CartV2 = { v: 2; items: string[]; coupons: string[] };

    /** The issue's own example, as a definition factory. */
    function cartActor(migrateState: MigrateState<CartV2>) {
        return defineActor({
            type: 'Cart',
            allowAnonymous: true,
            state: (): CartV2 => ({ v: 2, items: [], coupons: [] }),
            migrateState,
            methods: (ctx) => ({
                async read() {
                    return { v: ctx.state.v, items: [...ctx.state.items], coupons: [...ctx.state.coupons] };
                },
                async add(item: string) {
                    ctx.state.items.push(item);
                    await ctx.save();
                }
            })
        });
    }

    /** A pre-migration record, written straight to storage. */
    function seedV1(storage: ActorStorage, key = 'k'): Promise<string> {
        return storage.save('Cart', key, { items: ['a'] } satisfies CartV1, null);
    }

    it('runs on a stored record and its result becomes the live state', async () => {
        const def = cartActor((stored) => {
            const s = stored as CartV1 | CartV2;
            if ('v' in s) return s;
            return { v: 2, items: s.items ?? [], coupons: [] };
        });
        const storage = memoryStorage();
        await seedV1(storage);
        const host = createHost({ actors: [def], storage, defaults: quiet });
        await expect(host.actor(def, 'k').read()).resolves.toEqual({
            v: 2,
            items: ['a'],
            coupons: []
        });
    });

    it('hands the hook the revived state and the raw encoded record', async () => {
        let sawRevivedDate: boolean | null = null;
        let sawRawDate: boolean | null = null;
        let sawKey: string | null = null;
        const stamped = defineActor({
            type: 'Stamped',
            allowAnonymous: true,
            state: () => ({ when: null as Date | null, seen: false }),
            migrateState: (stored, info) => {
                const s = stored as { when: unknown };
                sawRevivedDate = s.when instanceof Date;
                sawRawDate = (info.raw as { when: unknown }).when instanceof Date;
                sawKey = info.key;
                return { when: s.when as Date | null, seen: true };
            },
            methods: (ctx) => ({
                async read() {
                    return { ms: ctx.state.when?.getTime(), seen: ctx.state.seen };
                },
                async stamp() {
                    ctx.state.when = new Date(1234567890000);
                    await ctx.save();
                }
            })
        });
        const storage = memoryStorage();
        const host = createHost({ actors: [stamped], storage, defaults: quiet });
        const client = host.actor(stamped, 's');
        await client.stamp(); // writes a real record, codec-encoded
        await host.deactivateType('Stamped');

        await expect(client.read()).resolves.toEqual({ ms: 1234567890000, seen: true });
        // The hook sees the actor's own vocabulary; `raw` is the on-disk form.
        expect(sawRevivedDate).toBe(true);
        expect(sawRawDate).toBe(false);
        expect(sawKey).toBe('s');
    });

    it('a throwing hook fails ALL parked callers and leaves the record untouched', async () => {
        const def = cartActor(() => {
            throw new Error('corrupt');
        });
        const storage = memoryStorage();
        const etag = await seedV1(storage);
        const host = createHost({ actors: [def], storage, defaults: quiet });
        const client = host.actor(def, 'k');

        const [r1, r2] = await Promise.allSettled([client.read(), client.read()]);
        expect(r1.status).toBe('rejected');
        expect(r2.status).toBe('rejected');
        expect((r1 as PromiseRejectedResult).reason).toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'activation'
        );
        expect(host.stats().activations).toBe(0);
        // Corrupt state is loud, never silently reset: same bytes, same etag.
        const record = await storage.load('Cart', 'k');
        expect(record).toEqual({ state: { items: ['a'] }, etag });
    });

    it('a hook that forgets to return fails activation rather than seeding undefined', async () => {
        const def = cartActor((() => undefined) as unknown as MigrateState<CartV2>);
        const storage = memoryStorage();
        await seedV1(storage);
        const host = createHost({ actors: [def], storage, defaults: quiet });
        await expect(host.actor(def, 'k').read()).rejects.toSatisfy(
            (e: unknown) =>
                isActorError(e) &&
                e.kind === 'activation' &&
                /migrateState/.test(String((e as { cause?: Error }).cause?.message))
        );
    });

    it('is not called when there is no stored record', async () => {
        const migrate = vi.fn((stored: unknown) => stored as CartV2);
        const def = cartActor(migrate);
        const host = createHost({ actors: [def], storage: memoryStorage(), defaults: quiet });
        await expect(host.actor(def, 'fresh').read()).resolves.toEqual({
            v: 2,
            items: [],
            coupons: []
        });
        expect(migrate).not.toHaveBeenCalled();
    });

    it('clearState re-seeds from state(key) without running the hook', async () => {
        const migrate = vi.fn((stored: unknown) => {
            const s = stored as CartV1 | CartV2;
            return 'v' in s ? s : { v: 2 as const, items: s.items, coupons: [] };
        });
        const clearing = defineActor({
            type: 'Clearable',
            allowAnonymous: true,
            state: (): CartV2 => ({ v: 2, items: [], coupons: [] }),
            migrateState: migrate,
            methods: (ctx) => ({
                async wipe() {
                    await ctx.clearState();
                    return [...ctx.state.items];
                }
            })
        });
        const storage = memoryStorage();
        await storage.save('Clearable', 'k', { items: ['a'] } satisfies CartV1, null);
        const host = createHost({ actors: [clearing], storage, defaults: quiet });

        const client = host.actor(clearing, 'k');
        const before = migrate.mock.calls.length;
        expect(before).toBe(0);
        await expect(client.wipe()).resolves.toEqual([]);
        // One call: the activation's. `clearState` re-seeds through `state(key)`.
        expect(migrate.mock.calls.length).toBe(1);
        expect(await storage.load('Clearable', 'k')).toBeNull();
    });

    it('the migrated shape is written on the next save — and not before', async () => {
        const def = cartActor((stored) => {
            const s = stored as CartV1 | CartV2;
            return 'v' in s ? s : { v: 2 as const, items: s.items, coupons: [] };
        });
        const storage = memoryStorage();
        const etag = await seedV1(storage);
        const host = createHost({ actors: [def], storage, defaults: quiet });
        const client = host.actor(def, 'k');

        // A pure read activates and migrates — and writes nothing. Etag
        // equality is the real assertion: a rewrite of identical bytes would
        // still move it.
        await expect(client.read()).resolves.toEqual({ v: 2, items: ['a'], coupons: [] });
        expect(await storage.load('Cart', 'k')).toEqual({ state: { items: ['a'] }, etag });

        await client.add('b');
        const after = await storage.load('Cart', 'k');
        expect(after!.state).toEqual({ v: 2, items: ['a', 'b'], coupons: [] });
        expect(after!.etag).not.toBe(etag);
    });

    it('write-behind never writes a migration on its own', async () => {
        const wb = defineActor({
            type: 'CartWB',
            allowAnonymous: true,
            state: (): CartV2 => ({ v: 2, items: [], coupons: [] }),
            persistence: { mode: 'write-behind', debounceMs: 10 },
            migrateState: (stored) => {
                const s = stored as CartV1 | CartV2;
                return 'v' in s ? s : { v: 2 as const, items: s.items, coupons: [] };
            },
            methods: (ctx) => ({
                async read() {
                    return [...ctx.state.items];
                },
                async add(item: string) {
                    ctx.state.items.push(item);
                }
            })
        });
        const storage = memoryStorage();
        const etag = await storage.save('CartWB', 'k', { items: ['a'] } satisfies CartV1, null);
        const host = createHost({ actors: [wb], storage, defaults: quiet });
        const client = host.actor(wb, 'k');

        await expect(client.read()).resolves.toEqual(['a']);
        await new Promise((r) => setTimeout(r, 40)); // well past the debounce
        expect(await storage.load('CartWB', 'k')).toEqual({ state: { items: ['a'] }, etag });
        // Nor does the deactivation flush turn a read into a write.
        await host.deactivateType('CartWB');
        expect(await storage.load('CartWB', 'k')).toEqual({ state: { items: ['a'] }, etag });

        // A real mutation carries the migrated shape with it, as ever.
        await client.add('b');
        await vi.waitFor(async () => {
            const record = await storage.load('CartWB', 'k');
            expect(record!.state).toEqual({ v: 2, items: ['a', 'b'], coupons: [] });
        });
    });

    it("persist: 'eager' writes the migration back once, then leaves it alone", async () => {
        const migrate = vi.fn((stored: unknown) => {
            const s = stored as CartV1 | CartV2;
            return 'v' in s ? s : { v: 2 as const, items: s.items, coupons: [] };
        });
        const def = cartActor({ persist: 'eager', migrate });
        const storage = memoryStorage();
        const seeded = await seedV1(storage);
        const host = createHost({ actors: [def], storage, defaults: quiet });
        const client = host.actor(def, 'k');

        await expect(client.read()).resolves.toEqual({ v: 2, items: ['a'], coupons: [] });
        const migrated = await storage.load('Cart', 'k');
        expect(migrated!.state).toEqual({ v: 2, items: ['a'], coupons: [] });
        expect(migrated!.etag).not.toBe(seeded);

        // Second activation: the hook returns its input, so nothing is written.
        await host.deactivateType('Cart');
        await expect(client.read()).resolves.toEqual({ v: 2, items: ['a'], coupons: [] });
        expect(await storage.load('Cart', 'k')).toEqual(migrated);
        expect(migrate).toHaveBeenCalledTimes(2);
    });

    it('an eager write-back that loses the CAS adopts the winner', async () => {
        const inner = memoryStorage();
        await seedV1(inner);
        let raced = false;
        // A peer migrates and saves in the window between our read and our
        // write — the rolling-deploy race, made deterministic.
        const racing: ActorStorage = {
            async load(type, key) {
                const record = await inner.load(type, key);
                if (!raced && type === 'Cart' && key === 'k') {
                    raced = true;
                    await inner.save(
                        type,
                        key,
                        { v: 2, items: ['a'], coupons: ['PEER'] } satisfies CartV2,
                        record!.etag
                    );
                }
                return record;
            },
            save: (type, key, state, etag) => inner.save(type, key, state, etag),
            clear: (type, key, etag) => inner.clear(type, key, etag)
        };
        const def = cartActor({
            persist: 'eager',
            migrate: (stored) => {
                const s = stored as CartV1 | CartV2;
                return 'v' in s ? s : { v: 2 as const, items: s.items, coupons: [] };
            }
        });
        const host = createHost({ actors: [def], storage: racing, defaults: quiet });

        // The caller sees nothing unusual, and the peer's record survives.
        await expect(host.actor(def, 'k').read()).resolves.toEqual({
            v: 2,
            items: ['a'],
            coupons: ['PEER']
        });
        expect((await inner.load('Cart', 'k'))!.state).toEqual({
            v: 2,
            items: ['a'],
            coupons: ['PEER']
        });
    });

    it('two hosts migrating lazily: the second save loses the CAS, the record stays sound', async () => {
        const def = cartActor((stored) => {
            const s = stored as CartV1 | CartV2;
            return 'v' in s ? s : { v: 2 as const, items: s.items, coupons: [] };
        });
        const storage = memoryStorage();
        await seedV1(storage);
        const hostA = createHost({ actors: [def], storage, defaults: quiet });
        const hostB = createHost({ actors: [def], storage, defaults: quiet });
        const a = hostA.actor(def, 'k');
        const b = hostB.actor(def, 'k');

        // Both activate off the same v1 bytes and both migrate — the double
        // migration the docs promise is safe. Neither has written yet.
        await Promise.all([a.read(), b.read()]);
        await a.add('fromA');
        await expect(b.add('fromB')).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'state-conflict'
        );

        const record = await storage.load('Cart', 'k');
        expect(record!.state).toEqual({ v: 2, items: ['a', 'fromA'], coupons: [] });
        // The loser re-activates against the winner.
        await expect(b.read()).resolves.toEqual({ v: 2, items: ['a', 'fromA'], coupons: [] });
    });

    it('defineActor rejects a malformed migrateState at definition time', () => {
        const base = {
            type: 'BadMigrate',
            allowAnonymous: true,
            state: () => ({ n: 0 }),
            methods: () => ({})
        } as const;
        expect(() =>
            defineActor({ ...base, migrateState: 'nope' as unknown as MigrateState<{ n: number }> })
        ).toThrow(/`migrateState` must be a function/);
        expect(() =>
            defineActor({
                ...base,
                migrateState: { persist: 'sometimes' } as unknown as MigrateState<{ n: number }>
            })
        ).toThrow(/`migrateState` must be a function/);
        expect(() =>
            defineActor({
                ...base,
                migrateState: {
                    migrate: (s: unknown) => s as { n: number },
                    persist: 'sometimes'
                } as unknown as MigrateState<{ n: number }>
            })
        ).toThrow(/`persist` must be 'lazy' \(the default\) or 'eager'/);
    });
});

describe('reentrancy & deadlock', () => {
    function pairHost(reentrant: boolean): Host {
        const a = defineActor({
            type: 'A',
            allowAnonymous: true,
            reentrant,
            state: () => ({ hits: 0 }),
            methods: (ctx) => ({
                async start() {
                    return ctx.actor(b, 'b1').callBack();
                },
                async finish() {
                    ctx.state.hits++;
                    return `finished:${ctx.state.hits}`;
                }
            })
        });
        const b = defineActor({
            type: 'B',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx) => ({
                async callBack() {
                    return ctx.actor(a, 'a1').finish();
                }
            })
        });
        return createHost({ actors: [a, b], defaults: quiet });
    }

    it('A→B→A throws ActorDeadlockError with the full chain when non-reentrant', async () => {
        const host = pairHost(false);
        const def = await host.definition('A');
        const client = host.actor(def!, 'a1') as unknown as { start(): Promise<string> };
        await expect(client.start()).rejects.toMatchObject({
            kind: 'deadlock',
            chain: ['A\u0000a1', 'B\u0000b1', 'A\u0000a1']
        });
    });

    it('A→B→A runs inline when A is reentrant', async () => {
        const host = pairHost(true);
        const def = await host.definition('A');
        const client = host.actor(def!, 'a1') as unknown as { start(): Promise<string> };
        await expect(client.start()).resolves.toBe('finished:1');
    });

    /**
     * The tightest case, and the one a dispatch fast path gets wrong: the
     * activation is ALREADY `active` in the directory, so a warm-path
     * shortcut keyed only on slot phase would enqueue behind the turn that
     * is up-stack awaiting it — a permanent hang instead of a loud throw.
     * The call chain, not the slot, is what makes this a deadlock.
     */
    function selfHost(reentrant: boolean): Host {
        const self = defineActor({
            type: 'S',
            allowAnonymous: true,
            reentrant,
            state: () => ({ depth: 0 }),
            methods: (ctx) => ({
                async outer(): Promise<string> {
                    ctx.state.depth++;
                    return ctx.actor(self, 's1').inner();
                },
                async inner(): Promise<string> {
                    return `inner:${ctx.state.depth}`;
                }
            })
        });
        return createHost({ actors: [self], defaults: quiet });
    }

    it('S→S on a warm activation throws ActorDeadlockError, not a hang', async () => {
        const host = selfHost(false);
        const def = await host.definition('S');
        const client = host.actor(def!, 's1') as unknown as {
            inner(): Promise<string>;
            outer(): Promise<string>;
        };
        // Warm it first: the slot must be `active`, so the fast path is live.
        await expect(client.inner()).resolves.toBe('inner:0');
        await expect(client.outer()).rejects.toMatchObject({
            kind: 'deadlock',
            chain: ['S\u0000s1', 'S\u0000s1']
        });
    });

    it('S→S on a warm activation runs inline when reentrant', async () => {
        const host = selfHost(true);
        const def = await host.definition('S');
        const client = host.actor(def!, 's1') as unknown as {
            inner(): Promise<string>;
            outer(): Promise<string>;
        };
        await expect(client.inner()).resolves.toBe('inner:0');
        await expect(client.outer()).resolves.toBe('inner:1');
    });
});

describe('lifecycle', () => {
    it('ctx.deactivate() finishes queued work first, then deactivates', async () => {
        const events: string[] = [];
        const host = createHost({ actors: [counterActor(events)], defaults: quiet });
        const client = host.actor(counterActor(), 'd');
        const bye = client.bye();
        const after = client.increment(1); // queued behind bye — must still run
        await expect(bye).resolves.toBe('bye');
        await expect(after).resolves.toBe(1);
        await vi.waitFor(() => {
            expect(events).toContain('deactivate:d:explicit');
            expect(host.stats().activations).toBe(0);
        });
    });

    it('a call during deactivation waits and lands on a fresh activation', async () => {
        const events: string[] = [];
        const storage = memoryStorage();
        const host = createHost({ actors: [counterActor(events)], storage, defaults: quiet });
        const client = host.actor(counterActor(), 'w');
        await client.increment(5);
        const drain = host.deactivateType('Counter');
        const during = client.get(); // dispatched while deactivating
        await drain;
        await expect(during).resolves.toBe(5);
        expect(events.filter((e) => e.startsWith('activate'))).toEqual([
            'activate:w',
            'activate:w'
        ]);
    });

    it('host.stop() drains activations and rejects new external calls', async () => {
        const events: string[] = [];
        const host = createHost({ actors: [counterActor(events)], defaults: quiet });
        const client = host.actor(counterActor(), 's');
        await client.increment(1);
        await host.stop();
        expect(events).toContain('deactivate:s:shutdown');
        await expect(client.get()).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'host-shutdown'
        );
    });

    it('idle sweep deactivates past the collection age', async () => {
        const events: string[] = [];
        const host = createHost({
            actors: [counterActor(events)],
            defaults: { ...quiet, idleAfterMs: 30, sweepIntervalMs: 10 }
        });
        await host.start();
        try {
            await host.actor(counterActor(), 'idle').increment(1);
            await vi.waitFor(
                () => {
                    expect(events).toContain('deactivate:idle:idle');
                },
                { timeout: 2000 }
            );
        } finally {
            await host.stop();
        }
    });

    it('caller deadline rejects the caller without killing the turn', async () => {
        const host = createHost({
            actors: [counterActor()],
            defaults: { ...quiet, callTimeoutMs: 30 }
        });
        const client = host.actor(counterActor(), 't');
        await expect(client.slow(200)).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'call-timeout'
        );
        // The turn completed anyway (count was still incremented).
        await vi.waitFor(async () => {
            await expect(client.get()).resolves.toBe(1);
        });
    });
});

describe('timers', () => {
    it('timer ticks run as ordinary turns and are coalesced', async () => {
        const ticks: number[] = [];
        const ticking = defineActor({
            type: 'Ticking',
            allowAnonymous: true,
            state: () => ({ n: 0 }),
            onActivate(ctx) {
                ctx.timer(
                    'tick',
                    () => {
                        ctx.state.n++;
                        ticks.push(ctx.state.n);
                    },
                    { due: 5, period: 5 }
                );
            },
            methods: (ctx) => ({
                async n() {
                    return ctx.state.n;
                }
            })
        });
        const host = createHost({ actors: [ticking], defaults: quiet });
        const client = host.actor(ticking, 'k');
        await client.n();
        await vi.waitFor(() => expect(ticks.length).toBeGreaterThanOrEqual(2));
        await host.deactivateType('Ticking');
        const settled = ticks.length;
        await new Promise((r) => setTimeout(r, 30));
        expect(ticks.length).toBe(settled); // timers died with the activation
    });
});

describe('placement bindings (the cluster seam)', () => {
    function boundPlacement(bindings: PlacementBindings, out: { local?: ActorDispatcher; host?: Host } = {}) {
        const placement: ActorPlacement = {
            dispatcherFor: () => out.local!,
            bind(local, host) {
                out.local = local;
                out.host = host;
                return bindings;
            }
        };
        return { placement, out };
    }

    it('bind() receives the local dispatcher and the host before any dispatch', async () => {
        const { placement, out } = boundPlacement({});
        const host = createHost({ actors: [counterActor()], placement, defaults: quiet });
        expect(out.local).toBeDefined();
        expect(out.host).toBe(host);
        // The captured local dispatcher is what dispatcherFor answers with.
        await expect(host.actor(counterActor(), 'b1').increment(1)).resolves.toBe(1);
    });

    it('beforeActivate claims before activation; afterDeactivate releases with the reason', async () => {
        const log: string[] = [];
        const events: string[] = [];
        const { placement } = boundPlacement({
            beforeActivate(ref) {
                log.push(`claim:${ref.key}`);
            },
            afterDeactivate(ref, reason) {
                log.push(`release:${ref.key}:${reason}`);
            }
        });
        const host = createHost({ actors: [counterActor(events)], placement, defaults: quiet });
        await host.actor(counterActor(), 'b2').increment(1);
        expect(log).toEqual(['claim:b2']);
        // The claim ran before the activation lifecycle, not after.
        expect(events[0]).toBe('activate:b2');
        await host.deactivateType('Counter');
        expect(log).toEqual(['claim:b2', 'release:b2:explicit']);
    });

    it('a beforeActivate throw refuses the activation and remembers nothing', async () => {
        let refuse = true;
        const events: string[] = [];
        const { placement } = boundPlacement({
            beforeActivate(ref) {
                if (refuse) throw new ActorWrongHostError(`${ref.type}/${ref.key}`, { hostId: 's.other' });
            }
        });
        const host = createHost({ actors: [counterActor(events)], placement, defaults: quiet });
        const client = host.actor(counterActor(), 'b3');
        await expect(client.increment(1)).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'wrong-host'
        );
        expect(events).toEqual([]); // never activated
        // Claim allowed now: the failed attempt left no trace.
        refuse = false;
        await expect(client.increment(1)).resolves.toBe(1);
    });

    it('strictChainPresence turns the reentrant missing-slot fallback into a retryable error', async () => {
        const reentrant = defineActor({
            type: 'Re',
            allowAnonymous: true,
            reentrant: true,
            state: () => ({}),
            methods: () => ({
                async ping() {
                    return 'pong';
                }
            })
        });
        const forged = { callChain: ['Re\u0000k'], callId: 'test' };

        // Single-node posture: chain-hit + no slot falls back to a dispatch.
        const loose = createHost({ actors: [reentrant], defaults: quiet });
        await expect(loose.dispatch({ type: 'Re', key: 'k' }, 'ping', [], forged)).resolves.toBe(
            'pong'
        );

        // Cluster posture: the turn provably runs elsewhere — loud error.
        const { placement } = boundPlacement({ strictChainPresence: true });
        const strict = createHost({ actors: [reentrant], placement, defaults: quiet });
        await expect(
            strict.dispatch({ type: 'Re', key: 'k' }, 'ping', [], forged)
        ).rejects.toSatisfy(
            (e: unknown) =>
                isActorError(e) && e.kind === 'deadlock' && /mid-turn/.test((e as Error).message)
        );
    });
});

describe('placement stop lifecycle', () => {
    it('a throwing beginStop never aborts the drain', async () => {
        const events: string[] = [];
        const { placement } = (() => {
            const out: { local?: ActorDispatcher } = {};
            const placement: ActorPlacement = {
                dispatcherFor: () => out.local!,
                bind(local) {
                    out.local = local;
                },
                beginStop() {
                    throw new Error('announcement store is down');
                }
            };
            return { placement };
        })();
        const host = createHost({ actors: [counterActor(events)], placement, defaults: quiet });
        await host.actor(counterActor(), 'bs1').increment(1);
        await host.stop({ timeoutMs: 1000 }); // must not reject
        expect(events).toContain('deactivate:bs1:shutdown');
        expect(host.stats().activations).toBe(0);
    });
});

describe('storage conflict brand across module copies', () => {
    it('a foreign storage implementation throwing the brand still faults the activation', async () => {
        // Simulate a storage from another module graph: plain object error
        // carrying only the brand, not the class.
        const conflicting: ActorStorage = {
            async load() {
                return null;
            },
            async save() {
                throw Object.assign(new Error('conflict'), {
                    __sigxActorStorageConflict: true
                });
            },
            async clear() {}
        };
        const host = createHost({ actors: [counterActor()], storage: conflicting, defaults: quiet });
        const client = host.actor(counterActor(), 'f');
        await expect(client.increment(1)).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'state-conflict'
        );
    });
});
