import { describe, expect, it, vi } from 'vitest';
import {
    ActorWrongHostError,
    defineActor,
    isActorError,
    type ActorDispatcher,
    type ActorPlacement,
    type ActorStorage,
    type PlacementBindings
} from '@sigx/actors';
import { createSilo, memoryStorage, type Silo } from '@sigx/actors/silo';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

function counterActor(events: string[] = []) {
    return defineActor({
        type: 'Counter',
        unguarded: true,
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
        const silo = createSilo({ actors: [counterActor(events)], defaults: quiet });
        const def = counterActor();
        expect(silo.stats().activations).toBe(0);
        // definitions are compared by type — use the registered instance
        const client = silo.actor(def, 'a');
        await expect(client.increment(2)).resolves.toBe(2);
        await expect(client.increment(3)).resolves.toBe(5);
        expect(events.filter((e) => e.startsWith('activate'))).toEqual(['activate:a']);
        expect(silo.stats()).toMatchObject({ activations: 1, perType: { Counter: 1 } });
    });

    it('two racing first calls join one activation (single-activation invariant)', async () => {
        const events: string[] = [];
        const silo = createSilo({ actors: [counterActor(events)], defaults: quiet });
        const client = silo.actor(counterActor(), 'race');
        const [a, b] = await Promise.all([client.increment(1), client.increment(1)]);
        expect([a, b].sort()).toEqual([1, 2]);
        expect(events.filter((e) => e.startsWith('activate'))).toEqual(['activate:race']);
    });

    it('turns are serialized: no interleaving even across awaits', async () => {
        const silo = createSilo({ actors: [counterActor()], defaults: quiet });
        const client = silo.actor(counterActor(), 'serial');
        const results = await Promise.all([client.slow(20), client.slow(1), client.slow(1)]);
        // Each turn saw the previous one fully committed.
        expect(results).toEqual([1, 2, 3]);
    });

    it('unknown method rejects with the method-not-found brand', async () => {
        const silo = createSilo({ actors: [counterActor()], defaults: quiet });
        const client = silo.actor(counterActor(), 'x') as unknown as {
            nope(): Promise<unknown>;
        };
        await expect(client.nope()).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'method-not-found'
        );
    });

    it('unknown actor type rejects with a descriptive activation error', async () => {
        const silo = createSilo({ actors: [], defaults: quiet });
        const client = silo.actor(counterActor(), 'x');
        await expect(client.get()).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'activation'
        );
    });

    it('onActivate throw fails ALL parked callers and forgets the slot', async () => {
        let attempts = 0;
        const flaky = defineActor({
            type: 'Flaky',
            unguarded: true,
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
        const silo = createSilo({ actors: [flaky], defaults: quiet });
        const client = silo.actor(flaky, 'k');
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
        const silo = createSilo({ actors: [counterActor()], storage, defaults: quiet });
        const client = silo.actor(counterActor(), 'p');
        await client.increment(7);
        await silo.deactivateType('Counter');
        expect(silo.stats().activations).toBe(0);
        await expect(client.get()).resolves.toBe(7);
    });

    it('rich types (Date, Map) survive the storage round-trip', async () => {
        const rich = defineActor({
            type: 'Rich',
            unguarded: true,
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
        const silo = createSilo({ actors: [rich], storage, defaults: quiet });
        const client = silo.actor(rich, 'r');
        await client.stamp();
        await silo.deactivateType('Rich');
        await expect(client.read()).resolves.toEqual({
            isDate: true,
            ms: 1234567890000,
            isMap: true,
            a: 1
        });
    });

    it('etag conflict rejects the turn, fails queued turns, and reloads fresh state', async () => {
        const storage = memoryStorage();
        const silo = createSilo({ actors: [counterActor()], storage, defaults: quiet });
        const client = silo.actor(counterActor(), 'c');
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
            unguarded: true,
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
        const silo = createSilo({ actors: [wb], storage, defaults: quiet });
        await silo.actor(wb, 'w').bump();
        await vi.waitFor(async () => {
            const record = await storage.load('WB', 'w');
            expect(record).not.toBeNull();
            expect((record!.state as { n: number }).n).toBe(1);
        });
    });

    it('write-behind flushes on deactivation even before the debounce fires', async () => {
        const wb = defineActor({
            type: 'WB2',
            unguarded: true,
            state: () => ({ n: 0 }),
            persistence: { mode: 'write-behind', debounceMs: 60_000 },
            methods: (ctx) => ({
                async bump() {
                    ctx.state.n++;
                }
            })
        });
        const storage = memoryStorage();
        const silo = createSilo({ actors: [wb], storage, defaults: quiet });
        await silo.actor(wb, 'w').bump();
        await silo.deactivateType('WB2');
        const record = await storage.load('WB2', 'w');
        expect((record?.state as { n: number }).n).toBe(1);
    });

    it('clearState resets to initial and deletes the stored record', async () => {
        const clearing = defineActor({
            type: 'Clearing',
            unguarded: true,
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
        const silo = createSilo({ actors: [clearing], storage, defaults: quiet });
        const client = silo.actor(clearing, 'k');
        await client.set(7);
        await expect(client.wipe()).resolves.toBe(42);
        expect(await storage.load('Clearing', 'k')).toBeNull();
    });
});

describe('reentrancy & deadlock', () => {
    function pairSilo(reentrant: boolean): Silo {
        const a = defineActor({
            type: 'A',
            unguarded: true,
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
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                async callBack() {
                    return ctx.actor(a, 'a1').finish();
                }
            })
        });
        return createSilo({ actors: [a, b], defaults: quiet });
    }

    it('A→B→A throws ActorDeadlockError with the full chain when non-reentrant', async () => {
        const silo = pairSilo(false);
        const def = await silo.definition('A');
        const client = silo.actor(def!, 'a1') as unknown as { start(): Promise<string> };
        await expect(client.start()).rejects.toMatchObject({
            kind: 'deadlock',
            chain: ['A\u0000a1', 'B\u0000b1', 'A\u0000a1']
        });
    });

    it('A→B→A runs inline when A is reentrant', async () => {
        const silo = pairSilo(true);
        const def = await silo.definition('A');
        const client = silo.actor(def!, 'a1') as unknown as { start(): Promise<string> };
        await expect(client.start()).resolves.toBe('finished:1');
    });
});

describe('lifecycle', () => {
    it('ctx.deactivate() finishes queued work first, then deactivates', async () => {
        const events: string[] = [];
        const silo = createSilo({ actors: [counterActor(events)], defaults: quiet });
        const client = silo.actor(counterActor(), 'd');
        const bye = client.bye();
        const after = client.increment(1); // queued behind bye — must still run
        await expect(bye).resolves.toBe('bye');
        await expect(after).resolves.toBe(1);
        await vi.waitFor(() => {
            expect(events).toContain('deactivate:d:explicit');
            expect(silo.stats().activations).toBe(0);
        });
    });

    it('a call during deactivation waits and lands on a fresh activation', async () => {
        const events: string[] = [];
        const storage = memoryStorage();
        const silo = createSilo({ actors: [counterActor(events)], storage, defaults: quiet });
        const client = silo.actor(counterActor(), 'w');
        await client.increment(5);
        const drain = silo.deactivateType('Counter');
        const during = client.get(); // dispatched while deactivating
        await drain;
        await expect(during).resolves.toBe(5);
        expect(events.filter((e) => e.startsWith('activate'))).toEqual([
            'activate:w',
            'activate:w'
        ]);
    });

    it('silo.stop() drains activations and rejects new external calls', async () => {
        const events: string[] = [];
        const silo = createSilo({ actors: [counterActor(events)], defaults: quiet });
        const client = silo.actor(counterActor(), 's');
        await client.increment(1);
        await silo.stop();
        expect(events).toContain('deactivate:s:shutdown');
        await expect(client.get()).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'silo-shutdown'
        );
    });

    it('idle sweep deactivates past the collection age', async () => {
        const events: string[] = [];
        const silo = createSilo({
            actors: [counterActor(events)],
            defaults: { ...quiet, idleAfterMs: 30, sweepIntervalMs: 10 }
        });
        await silo.start();
        try {
            await silo.actor(counterActor(), 'idle').increment(1);
            await vi.waitFor(
                () => {
                    expect(events).toContain('deactivate:idle:idle');
                },
                { timeout: 2000 }
            );
        } finally {
            await silo.stop();
        }
    });

    it('caller deadline rejects the caller without killing the turn', async () => {
        const silo = createSilo({
            actors: [counterActor()],
            defaults: { ...quiet, callTimeoutMs: 30 }
        });
        const client = silo.actor(counterActor(), 't');
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
    it('timer ticks run as mailbox turns and are coalesced', async () => {
        const ticks: number[] = [];
        const ticking = defineActor({
            type: 'Ticking',
            unguarded: true,
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
        const silo = createSilo({ actors: [ticking], defaults: quiet });
        const client = silo.actor(ticking, 'k');
        await client.n();
        await vi.waitFor(() => expect(ticks.length).toBeGreaterThanOrEqual(2));
        await silo.deactivateType('Ticking');
        const settled = ticks.length;
        await new Promise((r) => setTimeout(r, 30));
        expect(ticks.length).toBe(settled); // timers died with the activation
    });
});

describe('placement bindings (the cluster seam)', () => {
    function boundPlacement(bindings: PlacementBindings, out: { local?: ActorDispatcher; silo?: Silo } = {}) {
        const placement: ActorPlacement = {
            dispatcherFor: () => out.local!,
            bind(local, silo) {
                out.local = local;
                out.silo = silo;
                return bindings;
            }
        };
        return { placement, out };
    }

    it('bind() receives the local dispatcher and the silo before any dispatch', async () => {
        const { placement, out } = boundPlacement({});
        const silo = createSilo({ actors: [counterActor()], placement, defaults: quiet });
        expect(out.local).toBeDefined();
        expect(out.silo).toBe(silo);
        // The captured local dispatcher is what dispatcherFor answers with.
        await expect(silo.actor(counterActor(), 'b1').increment(1)).resolves.toBe(1);
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
        const silo = createSilo({ actors: [counterActor(events)], placement, defaults: quiet });
        await silo.actor(counterActor(), 'b2').increment(1);
        expect(log).toEqual(['claim:b2']);
        // The claim ran before the activation lifecycle, not after.
        expect(events[0]).toBe('activate:b2');
        await silo.deactivateType('Counter');
        expect(log).toEqual(['claim:b2', 'release:b2:explicit']);
    });

    it('a beforeActivate throw refuses the activation and remembers nothing', async () => {
        let refuse = true;
        const events: string[] = [];
        const { placement } = boundPlacement({
            beforeActivate(ref) {
                if (refuse) throw new ActorWrongHostError(`${ref.type}/${ref.key}`, { siloId: 's.other' });
            }
        });
        const silo = createSilo({ actors: [counterActor(events)], placement, defaults: quiet });
        const client = silo.actor(counterActor(), 'b3');
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
            unguarded: true,
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
        const loose = createSilo({ actors: [reentrant], defaults: quiet });
        await expect(loose.dispatch({ type: 'Re', key: 'k' }, 'ping', [], forged)).resolves.toBe(
            'pong'
        );

        // Cluster posture: the turn provably runs elsewhere — loud error.
        const { placement } = boundPlacement({ strictChainPresence: true });
        const strict = createSilo({ actors: [reentrant], placement, defaults: quiet });
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
        const silo = createSilo({ actors: [counterActor(events)], placement, defaults: quiet });
        await silo.actor(counterActor(), 'bs1').increment(1);
        await silo.stop({ timeoutMs: 1000 }); // must not reject
        expect(events).toContain('deactivate:bs1:shutdown');
        expect(silo.stats().activations).toBe(0);
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
        const silo = createSilo({ actors: [counterActor()], storage: conflicting, defaults: quiet });
        const client = silo.actor(counterActor(), 'f');
        await expect(client.increment(1)).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'state-conflict'
        );
    });
});
