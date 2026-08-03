/**
 * `reentrant: 'always'` and `methodReentrancy` (#15) — full interleaving.
 *
 * The contract under test: interleavable turns launch immediately (they
 * neither wait for in-flight turns nor are waited for), serial turns stay
 * mutually exclusive with each other, every turn keeps ITS OWN call context
 * across awaits (the regression risk the per-turn store exists for), saves
 * from interleaved turns never fault the activation on their own sibling,
 * and the lifecycle (deactivation drain, idle request, sweeper) treats an
 * in-flight interleaved turn as work.
 */
import { describe, expect, it, vi } from 'vitest';
import {
    defineActor,
    isActorError,
    type AnyActorDefinition,
    type DeactivationReason
} from '@sigx/actors';
import { createHost, manualScheduler, memoryStorage } from '@sigx/actors/host';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

function deferred() {
    let resolve!: () => void;
    const promise = new Promise<void>((r) => {
        resolve = r;
    });
    return { promise, resolve };
}

const errKind = (e: unknown): string =>
    isActorError(e) ? e.kind : `not-actor-error:${String(e)}`;

describe('reentrant: always — interleaving', () => {
    it('two calls interleave at an await: the second completes while the first is parked', async () => {
        const gate = deferred();
        const events: string[] = [];
        const Al = defineActor({
            type: 'Al',
            unguarded: true,
            reentrant: 'always',
            state: () => ({}),
            methods: () => ({
                async park() {
                    events.push('park:start');
                    await gate.promise;
                    events.push('park:end');
                    return 'parked';
                },
                async quick() {
                    events.push('quick');
                    return 'quick';
                }
            })
        });
        const host = createHost({ actors: [Al], defaults: quiet });
        const client = host.actor(Al, 'a');
        const parked = client.park();
        await expect(client.quick()).resolves.toBe('quick');
        // Both turns are unsettled work: depth counts the parked one.
        expect(host.stats().queued).toBe(1);
        gate.resolve();
        await expect(parked).resolves.toBe('parked');
        expect(events).toEqual(['park:start', 'quick', 'park:end']);
    });

    it('A→B→A completes as a concurrent turn when A is always-reentrant', async () => {
        const a = defineActor({
            type: 'A',
            unguarded: true,
            reentrant: 'always',
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
        const host = createHost({ actors: [a, b], defaults: quiet });
        const client = host.actor(a, 'a1') as unknown as { start(): Promise<string> };
        await expect(client.start()).resolves.toBe('finished:1');
    });

    it('S→S on a warm always-reentrant activation completes, not a hang or throw', async () => {
        const self = defineActor({
            type: 'S',
            unguarded: true,
            reentrant: 'always',
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
        const host = createHost({ actors: [self], defaults: quiet });
        const client = host.actor(self, 's1') as unknown as {
            inner(): Promise<string>;
            outer(): Promise<string>;
        };
        // Warm it first so the dispatch fast path serves the self-call.
        await expect(client.inner()).resolves.toBe('inner:0');
        await expect(client.outer()).resolves.toBe('inner:1');
    });
});

describe('reentrant: always — per-turn call context', () => {
    /**
     * THE regression risk: two interleaved turns with different incoming
     * chains each hop outward AFTER an await. A single mutable
     * current-call field would hand one turn the other's chain — detected
     * here in both directions, because the chains differ in whether they
     * contain the non-reentrant actor X:
     *  - `one` arrived externally (chain []): its outbound call to X must
     *    NOT read as in-chain (a false deadlock);
     *  - `two` arrived via X (chain [X]): its outbound call to X MUST read
     *    as in-chain (a missed deadlock otherwise).
     */
    it('interleaved turns keep their own chains across awaits — no false or missed deadlock', async () => {
        const gate = deferred();
        const x = defineActor({
            type: 'X',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                async kick() {
                    return ctx.actor(a, 'a1').two();
                },
                async probe() {
                    return 'ok';
                }
            })
        });
        const a = defineActor({
            type: 'Actx',
            unguarded: true,
            reentrant: 'always',
            state: () => ({}),
            methods: (ctx) => ({
                async one() {
                    await gate.promise;
                    // External chain: X is NOT in it. Must not deadlock.
                    return ctx.actor(x, 'x1').probe();
                },
                async two() {
                    await gate.promise;
                    // Chain [X, Actx]: X's turn is up-stack awaiting us.
                    // The deadlock MUST still be detected.
                    return ctx.actor(x, 'x1').probe().then(
                        () => 'missed-deadlock',
                        (e: unknown) => errKind(e)
                    );
                }
            })
        });
        const host = createHost({ actors: [x, a], defaults: quiet });
        const xClient = host.actor(x, 'x1') as unknown as { kick(): Promise<string> };
        const aClient = host.actor(a, 'a1') as unknown as { one(): Promise<string> };
        const viaX = xClient.kick(); // parks A.two with chain [X, Actx]
        await new Promise((r) => setTimeout(r, 5));
        const direct = aClient.one(); // parks A.one with chain []
        await new Promise((r) => setTimeout(r, 5));
        gate.resolve(); // both resume; each reads its OWN context
        await expect(direct).resolves.toBe('ok');
        await expect(viaX).resolves.toBe('deadlock');
    });

    it("a timer tick on an always-reentrant actor starts a fresh chain, not the arming turn's", async () => {
        const events: string[] = [];
        const x = defineActor({
            type: 'Xt',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                async kick() {
                    return ctx.actor(a, 'a1').arm();
                },
                async probe() {
                    return 'ok';
                }
            })
        });
        const a = defineActor({
            type: 'At',
            unguarded: true,
            reentrant: 'always',
            state: () => ({}),
            methods: (ctx) => ({
                async arm() {
                    // The tick closure is created INSIDE this turn, whose
                    // chain contains Xt. Inheriting that context would make
                    // the tick's probe a (false) in-chain call.
                    ctx.timer('t', async () => {
                        const r = await ctx.actor(x, 'x1').probe().then(
                            (v: string) => v,
                            (e: unknown) => errKind(e)
                        );
                        events.push(`tick:${r}`);
                    }, { due: 5 });
                    return 'armed';
                }
            })
        });
        const host = createHost({ actors: [x, a], defaults: quiet });
        const xClient = host.actor(x, 'x1') as unknown as { kick(): Promise<string> };
        await expect(xClient.kick()).resolves.toBe('armed');
        await vi.waitFor(() => expect(events).toEqual(['tick:ok']));
    });

    it('a task body on an always-reentrant actor starts a fresh chain', async () => {
        const events: string[] = [];
        const x = defineActor({
            type: 'Xb',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                async kick() {
                    return ctx.actor(a, 'a1').startIt();
                },
                async probe() {
                    return 'ok';
                }
            })
        });
        const a = defineActor({
            type: 'Ab',
            unguarded: true,
            reentrant: 'always',
            state: () => ({}),
            methods: (ctx) => ({
                async startIt() {
                    await ctx.tasks.start('bg');
                    return 'started';
                }
            }),
            tasks: (ctx) => ({
                async bg() {
                    const r = await ctx.actor(x, 'x1').probe().then(
                        (v: string) => v,
                        (e: unknown) => errKind(e)
                    );
                    events.push(`task:${r}`);
                }
            })
        });
        const host = createHost({ actors: [x, a], storage: memoryStorage(), defaults: quiet });
        const xClient = host.actor(x, 'x1') as unknown as { kick(): Promise<string> };
        await expect(xClient.kick()).resolves.toBe('started');
        await vi.waitFor(() => expect(events).toEqual(['task:ok']));
    });
});

describe('methodReentrancy — per-method interleaving', () => {
    function mixedActor(events: string[], gates: { write: Promise<void>; read: Promise<void> }) {
        return defineActor({
            type: 'Mixed',
            unguarded: true,
            methodReentrancy: { read: 'always', parkedRead: 'always' },
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                async slowWrite() {
                    events.push('write:start');
                    await gates.write;
                    ctx.state.n++;
                    events.push('write:end');
                    return ctx.state.n;
                },
                async write() {
                    ctx.state.n++;
                    events.push('write');
                    return ctx.state.n;
                },
                async read() {
                    events.push('read');
                    return ctx.state.n;
                },
                async parkedRead() {
                    events.push('parkedRead:start');
                    await gates.read;
                    events.push('parkedRead:end');
                    return ctx.state.n;
                }
            })
        });
    }

    it('a mapped method overlaps serial turns in BOTH directions; serial turns still serialize', async () => {
        const events: string[] = [];
        const writeGate = deferred();
        const readGate = deferred();
        const Mixed = mixedActor(events, { write: writeGate.promise, read: readGate.promise });
        const host = createHost({ actors: [Mixed], defaults: quiet });
        const client = host.actor(Mixed, 'm') as unknown as {
            slowWrite(): Promise<number>;
            write(): Promise<number>;
            read(): Promise<number>;
            parkedRead(): Promise<number>;
        };
        // Direction 1: a read lands while a serial write is parked.
        const slow = client.slowWrite();
        await expect(client.read()).resolves.toBe(0);
        // Direction 2: a serial write lands while a mapped read is parked —
        // and serial-serial still queues: this write waits for slowWrite.
        const parked = client.parkedRead();
        const write = client.write();
        await new Promise((r) => setTimeout(r, 10));
        expect(events).toEqual(['write:start', 'read', 'parkedRead:start']);
        writeGate.resolve();
        await expect(slow).resolves.toBe(1);
        await expect(write).resolves.toBe(2);
        readGate.resolve();
        await parked;
        expect(events).toEqual([
            'write:start',
            'read',
            'parkedRead:start',
            'write:end',
            'write',
            'parkedRead:end'
        ]);
    });

    it('an in-chain call to a mapped method completes; to an unmapped one still deadlocks', async () => {
        const self = defineActor({
            type: 'SelfMap',
            unguarded: true,
            methodReentrancy: { read: 'always' },
            state: () => ({ n: 7 }),
            methods: (ctx) => ({
                async viaRead(): Promise<number> {
                    return ctx.actor(self, 'k').read();
                },
                async viaWrite(): Promise<string> {
                    return ctx.actor(self, 'k').write().then(
                        () => 'missed-deadlock',
                        (e: unknown) => errKind(e)
                    );
                },
                async read() {
                    return ctx.state.n;
                },
                async write() {
                    ctx.state.n++;
                    return 'wrote';
                }
            })
        });
        const host = createHost({ actors: [self], defaults: quiet });
        const client = host.actor(self, 'k') as unknown as {
            viaRead(): Promise<number>;
            viaWrite(): Promise<string>;
        };
        await expect(client.viaRead()).resolves.toBe(7);
        await expect(client.viaWrite()).resolves.toBe('deadlock');
    });

    it("on a 'call-chain' actor with a map, unmapped in-chain calls still run inline", async () => {
        const self = defineActor({
            type: 'ChainMap',
            unguarded: true,
            reentrant: 'call-chain',
            methodReentrancy: { read: 'always' },
            state: () => ({ depth: 0 }),
            methods: (ctx) => ({
                async outer(): Promise<string> {
                    ctx.state.depth++;
                    return ctx.actor(self, 'c').inner();
                },
                async inner(): Promise<string> {
                    return `inner:${ctx.state.depth}`;
                },
                async read() {
                    return ctx.state.depth;
                }
            })
        });
        const host = createHost({ actors: [self], defaults: quiet });
        const client = host.actor(self, 'c') as unknown as {
            inner(): Promise<string>;
            outer(): Promise<string>;
        };
        await expect(client.inner()).resolves.toBe('inner:0');
        // Inline: inner shares outer's turn and sees its half-done mutation.
        await expect(client.outer()).resolves.toBe('inner:1');
    });

    it("`reentrant: true` still means call-chain — unchanged v1 behavior", async () => {
        const self = defineActor({
            type: 'V1',
            unguarded: true,
            reentrant: true,
            state: () => ({ depth: 0 }),
            methods: (ctx) => ({
                async outer(): Promise<string> {
                    ctx.state.depth++;
                    return ctx.actor(self, 'v').inner();
                },
                async inner(): Promise<string> {
                    return `inner:${ctx.state.depth}`;
                }
            })
        });
        const host = createHost({ actors: [self], defaults: quiet });
        const client = host.actor(self, 'v') as unknown as { outer(): Promise<string> };
        await expect(client.outer()).resolves.toBe('inner:1');
    });
});

describe('reentrant: always — persistence', () => {
    it('two interleaved turns can both ctx.save() without faulting the activation', async () => {
        const gateA = deferred();
        const gateB = deferred();
        const Dual = defineActor({
            type: 'Dual',
            unguarded: true,
            reentrant: 'always',
            state: () => ({ a: 0, b: 0 }),
            methods: (ctx) => ({
                async setA() {
                    await gateA.promise;
                    ctx.state.a = 1;
                    await ctx.save();
                    return 'a-saved';
                },
                async setB() {
                    await gateB.promise;
                    ctx.state.b = 1;
                    await ctx.save();
                    return 'b-saved';
                }
            })
        });
        const storage = memoryStorage();
        const host = createHost({ actors: [Dual], storage, defaults: quiet });
        const client = host.actor(Dual, 'd') as unknown as {
            setA(): Promise<string>;
            setB(): Promise<string>;
        };
        const a = client.setA();
        const b = client.setB();
        await new Promise((r) => setTimeout(r, 5));
        // Release together: the two saves race and MUST coalesce, not
        // CAS-fail each other.
        gateA.resolve();
        gateB.resolve();
        await expect(a).resolves.toBe('a-saved');
        await expect(b).resolves.toBe('b-saved');
        const record = await storage.load('Dual', 'd');
        expect(record!.state).toEqual({ a: 1, b: 1 });
        // The activation did not fault: it still answers.
        await expect(client.setA()).resolves.toBe('a-saved');
    });

    it('an external conflict faults the activation; in-flight turns finish but cannot save', async () => {
        const gate1 = deferred();
        const gate2 = deferred();
        const Con = defineActor({
            type: 'Con',
            unguarded: true,
            reentrant: 'always',
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                async seed() {
                    ctx.state.n = 1;
                    await ctx.save();
                    return ctx.state.n;
                },
                async conflicted() {
                    await gate1.promise;
                    ctx.state.n = 2;
                    return ctx.save().then(
                        () => 'saved',
                        (e: unknown) => errKind(e)
                    );
                },
                async survivor() {
                    await gate2.promise;
                    const outcome = await ctx.save().then(
                        () => 'saved',
                        (e: unknown) => errKind(e)
                    );
                    return `finished:${outcome}`;
                },
                async get() {
                    return ctx.state.n;
                }
            })
        });
        const storage = memoryStorage();
        const host = createHost({ actors: [Con], storage, defaults: quiet });
        const client = host.actor(Con, 'c') as unknown as {
            seed(): Promise<number>;
            conflicted(): Promise<string>;
            survivor(): Promise<string>;
            get(): Promise<number>;
        };
        await client.seed();
        const conflicted = client.conflicted();
        const survivor = client.survivor();
        await new Promise((r) => setTimeout(r, 5));
        // A second writer moves the record behind the activation's back.
        const record = await storage.load('Con', 'c');
        await storage.save('Con', 'c', { n: 99 }, record!.etag);
        gate1.resolve();
        await expect(conflicted).resolves.toBe('state-conflict');
        // The sibling was in flight when the fault landed: it completes,
        // but its own save rejects with the fault.
        gate2.resolve();
        await expect(survivor).resolves.toBe('finished:state-conflict');
        // A fresh call re-activates on the winning state.
        await expect(client.get()).resolves.toBe(99);
    });

    it('write-behind on an always-reentrant actor persists merged state without self-faulting', async () => {
        const Wb = defineActor({
            type: 'AlWb',
            unguarded: true,
            reentrant: 'always',
            persistence: { mode: 'write-behind', debounceMs: 10 },
            state: () => ({ a: 0, b: 0 }),
            methods: (ctx) => ({
                async bumpA() {
                    await new Promise((r) => setTimeout(r, 5));
                    ctx.state.a++;
                    return ctx.state.a;
                },
                async bumpB() {
                    await new Promise((r) => setTimeout(r, 5));
                    ctx.state.b++;
                    return ctx.state.b;
                }
            })
        });
        const storage = memoryStorage();
        const host = createHost({ actors: [Wb], storage, defaults: quiet });
        const client = host.actor(Wb, 'w') as unknown as {
            bumpA(): Promise<number>;
            bumpB(): Promise<number>;
        };
        await Promise.all([client.bumpA(), client.bumpB(), client.bumpA()]);
        await vi.waitFor(async () => {
            const record = await storage.load('AlWb', 'w');
            expect(record!.state).toEqual({ a: 2, b: 1 });
        });
        // No fault: still answering.
        await expect(client.bumpB()).resolves.toBe(2);
    });
});

describe('reentrant: always — lifecycle', () => {
    it('deactivation drains in-flight interleaved turns before onDeactivate and the final flush', async () => {
        const gate = deferred();
        const events: string[] = [];
        const Dr = defineActor({
            type: 'Dr',
            unguarded: true,
            reentrant: 'always',
            persistence: { mode: 'write-behind', debounceMs: 60_000 },
            state: () => ({ n: 0 }),
            onDeactivate(_ctx, reason: DeactivationReason) {
                events.push(`deactivate:${reason}`);
            },
            methods: (ctx) => ({
                async park() {
                    events.push('park:start');
                    await gate.promise;
                    ctx.state.n = 1;
                    events.push('park:end');
                    return 'parked';
                }
            })
        });
        const storage = memoryStorage();
        const host = createHost({ actors: [Dr], storage, defaults: quiet });
        const client = host.actor(Dr, 'd') as unknown as { park(): Promise<string> };
        const parked = client.park();
        await new Promise((r) => setTimeout(r, 5));
        const stopping = host.deactivateType('Dr');
        await new Promise((r) => setTimeout(r, 10));
        // The drain is waiting on the parked turn — not done yet.
        expect(events).toEqual(['park:start']);
        gate.resolve();
        await expect(parked).resolves.toBe('parked');
        await stopping;
        expect(events).toEqual(['park:start', 'park:end', 'deactivate:explicit']);
        // The final write-behind flush covered the parked turn's mutation.
        const record = await storage.load('Dr', 'd');
        expect((record!.state as { n: number }).n).toBe(1);
    });

    it('ctx.deactivate() waits for the LAST in-flight turn before deactivating', async () => {
        const gate = deferred();
        const Bye = defineActor({
            type: 'Bye',
            unguarded: true,
            reentrant: 'always',
            state: () => ({}),
            methods: (ctx) => ({
                async park() {
                    await gate.promise;
                    return 'parked';
                },
                async bye() {
                    ctx.deactivate();
                    return 'bye';
                }
            })
        });
        const host = createHost({ actors: [Bye], defaults: quiet });
        const client = host.actor(Bye, 'b') as unknown as {
            park(): Promise<string>;
            bye(): Promise<string>;
        };
        const parked = client.park();
        await expect(client.bye()).resolves.toBe('bye');
        await new Promise((r) => setTimeout(r, 10));
        // The parked turn is still in flight — no deactivation yet.
        expect(host.stats().activations).toBe(1);
        gate.resolve();
        await expect(parked).resolves.toBe('parked');
        await vi.waitFor(() => expect(host.stats().activations).toBe(0));
    });

    it('the sweeper never collects an activation with an in-flight interleaved turn', async () => {
        const gate = deferred();
        const scheduler = manualScheduler();
        const Sw = defineActor({
            type: 'Sw',
            unguarded: true,
            reentrant: 'always',
            idleAfterMs: 1,
            state: () => ({}),
            methods: () => ({
                async park() {
                    await gate.promise;
                    return 'parked';
                }
            })
        });
        const host = createHost({
            actors: [Sw],
            scheduler,
            defaults: { ...quiet, sweepIntervalMs: 1_000 }
        });
        await host.start();
        try {
            const client = host.actor(Sw, 's') as unknown as { park(): Promise<string> };
            const parked = client.park();
            // Real time passes well beyond idleAfterMs while the turn is
            // in flight; the sweep must skip it (depth > 0).
            await new Promise((r) => setTimeout(r, 20));
            scheduler.advance(1_000);
            await new Promise((r) => setTimeout(r, 10));
            expect(host.stats().activations).toBe(1);
            gate.resolve();
            await expect(parked).resolves.toBe('parked');
            // Now it is genuinely idle — the next sweep collects it.
            await new Promise((r) => setTimeout(r, 20));
            scheduler.advance(1_000);
            await vi.waitFor(() => expect(host.stats().activations).toBe(0));
        } finally {
            await host.stop({ timeoutMs: 1000 });
        }
    });
});

describe('declaration validation (first activation, every build)', () => {
    // Validation runs at ACTIVATION rather than definition time (the root
    // entry's size gate keeps define.ts lean): the first call to a key of
    // the type fails with ActorActivationError carrying the message.
    const base = {
        unguarded: true as const,
        state: () => ({ n: 0 }),
        methods: () => ({
            async get() {
                return 0;
            }
        })
    };

    async function firstCallError(def: AnyActorDefinition): Promise<string> {
        const host = createHost({ actors: [def], defaults: quiet });
        const client = host.actor(def, 'k') as unknown as { get(): Promise<number> };
        try {
            await client.get();
            return 'resolved';
        } catch (error) {
            expect(isActorError(error) && error.kind === 'activation').toBe(true);
            return String((error as Error).cause);
        }
    }

    it('an unknown reentrant value fails the first activation', async () => {
        const bad = defineActor({
            ...base,
            type: 'Bad1',
            reentrant: 'sometimes' as unknown as 'always'
        });
        await expect(firstCallError(bad)).resolves.toMatch(/`reentrant` must be/);
    });

    it('a methodReentrancy value other than always fails', async () => {
        const bad = defineActor({
            ...base,
            type: 'Bad2',
            methodReentrancy: { get: 'sometimes' as 'always' }
        });
        await expect(firstCallError(bad)).resolves.toMatch(/must map to 'always'/);
    });

    it("reentrant: 'always' with a map is a redundancy failure — presence alone, empty included", async () => {
        const bad = defineActor({
            ...base,
            type: 'Bad3',
            reentrant: 'always',
            methodReentrancy: { get: 'always' }
        });
        await expect(firstCallError(bad)).resolves.toMatch(/redundant/);
        const empty = defineActor({
            ...base,
            type: 'Bad3e',
            reentrant: 'always',
            methodReentrancy: {}
        });
        await expect(firstCallError(empty)).resolves.toMatch(/redundant/);
    });

    it('a non-object methodReentrancy fails instead of slipping through', async () => {
        for (const [suffix, value] of [
            ['n', null],
            ['s', 'always'],
            ['a', ['get']]
        ] as const) {
            const bad = defineActor({
                ...base,
                type: `Bad6${suffix}`,
                methodReentrancy: value as unknown as Record<string, 'always'>
            });
            await expect(firstCallError(bad)).resolves.toMatch(/must be an object/);
        }
    });

    it('a map key naming a stream fails', async () => {
        const bad = defineActor({
            ...base,
            type: 'Bad4',
            methodReentrancy: { ticks: 'always' },
            streams: () => ({
                async *ticks() {
                    yield 1;
                }
            })
        });
        await expect(firstCallError(bad)).resolves.toMatch(/streams take no turn/);
    });

    it('a reserved $-name in the map fails', async () => {
        const bad = defineActor({
            ...base,
            type: 'Bad5',
            methodReentrancy: { '$sigx:reminder': 'always' }
        });
        await expect(firstCallError(bad)).resolves.toMatch(/reserved name/);
    });
});
