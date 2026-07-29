import { afterEach, describe, expect, it, vi } from 'vitest';
import { defineActor, SiloShutdownError } from '@sigx/actors';
import { createSilo, manualScheduler, memoryStorage } from '@sigx/actors/silo';

const quiet = { sweepIntervalMs: 600_000, reminderTickMs: 600_000, callTimeoutMs: 0 };

/**
 * Fail rather than hang: a mailbox regression parks forever, and a test
 * that parks forever reports as a timed-out RUN, not a failed assertion.
 */
function within<T>(promise: Promise<T>, ms: number): Promise<T> {
    return Promise.race([
        promise,
        new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error(`did not settle within ${ms}ms`)), ms).unref?.()
        )
    ]);
}

/** A latch the test opens and the task awaits (or vice versa). */
function gate(): { open: () => void; opened: Promise<void> } {
    let open!: () => void;
    const opened = new Promise<void>((r) => (open = r));
    return { open, opened };
}

/** Resolves when `signal` aborts (already-aborted resolves immediately). */
function aborted(signal: AbortSignal): Promise<void> {
    return signal.aborted
        ? Promise.resolve()
        : new Promise((r) => signal.addEventListener('abort', () => r(), { once: true }));
}

async function until(cond: () => boolean | Promise<boolean>, ms = 2000): Promise<void> {
    const deadline = Date.now() + ms;
    while (!(await cond())) {
        if (Date.now() > deadline) throw new Error('condition not reached');
        await new Promise((r) => setTimeout(r, 5));
    }
}

afterEach(() => {
    vi.restoreAllMocks();
});

describe('tasks: detached execution', () => {
    it('reads interleave while a task runs, and list() reports the run', async () => {
        const working = gate();
        const release = gate();
        const finished = gate();
        const worker = defineActor({
            type: 'Worker',
            unguarded: true,
            state: () => ({ n: 1 }),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('run'),
                read: () => ctx.snapshot().n,
                running: () => ctx.tasks.list().map((t) => t.name)
            }),
            tasks: () => ({
                async run() {
                    working.open();
                    await release.opened;
                    finished.open();
                }
            })
        });
        const silo = createSilo({ actors: [worker], defaults: quiet });
        const client = silo.actor(worker, 'a');
        await client.begin();
        await within(working.opened, 1000);
        // The mailbox is free mid-task: an ordinary read resolves NOW.
        expect(await within(client.read(), 1000)).toBe(1);
        expect(await client.running()).toEqual(['run']);
        release.open();
        await within(finished.opened, 1000);
        await silo.stop({ timeoutMs: 1000 });
    });

    it('turn() runs as an ordinary serialized turn: mutations land, changes() fires', async () => {
        const stepped = gate();
        const counter = defineActor({
            type: 'Counter',
            unguarded: true,
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('count'),
                read: () => ctx.snapshot().n
            }),
            streams: (ctx) => ({
                async *watch() {
                    for await (const s of ctx.changes()) yield s.n;
                }
            }),
            tasks: (ctx) => ({
                async count() {
                    for (let i = 1; i <= 3; i++) {
                        await ctx.turn((c) => {
                            c.state.n = i;
                        });
                        await new Promise((r) => setTimeout(r, 5));
                    }
                    stepped.open();
                }
            })
        });
        const silo = createSilo({ actors: [counter], defaults: quiet });
        const client = silo.actor(counter, 'a');

        // Subscribe BEFORE starting, so every turn's change is observed —
        // live progress during a running task is the whole point.
        const iterator = client.watch()[Symbol.asyncIterator]();
        const seen: number[] = [];
        const pulls = (async () => {
            for (let i = 0; i < 3; i++) {
                const { value, done } = await iterator.next();
                if (done) break;
                seen.push(value as number);
            }
        })();
        await new Promise((r) => setTimeout(r, 20)); // let the body subscribe
        await client.begin();
        await within(stepped.opened, 2000);
        await within(pulls, 2000);
        expect(seen).toEqual([1, 2, 3]);
        expect(await client.read()).toBe(3);
        await iterator.return?.();
        await silo.stop({ timeoutMs: 1000 });
    });

    it('start is single-flight per name', async () => {
        const entries: number[] = [];
        const release = gate();
        const worker = defineActor({
            type: 'Single',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('run')
            }),
            tasks: () => ({
                async run() {
                    entries.push(1);
                    await release.opened;
                }
            })
        });
        const silo = createSilo({ actors: [worker], defaults: quiet });
        const client = silo.actor(worker, 'a');
        await client.begin();
        await client.begin();
        await client.begin();
        expect(entries).toHaveLength(1);
        release.open();
        await silo.stop({ timeoutMs: 1000 });
    });

    it('rejects an unknown task name, and a start without a tasks: section', async () => {
        const withTasks = defineActor({
            type: 'HasTasks',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('nope')
            }),
            tasks: () => ({
                async run() {}
            })
        });
        const without = defineActor({
            type: 'NoTasks',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('run')
            })
        });
        const silo = createSilo({ actors: [withTasks, without], defaults: quiet });
        await expect(silo.actor(withTasks, 'a').begin()).rejects.toThrow(/nope/);
        await expect(silo.actor(without, 'a').begin()).rejects.toThrow(/tasks/);
        await silo.stop({ timeoutMs: 1000 });
    });

    it('save()/clearState() from a detached task body reject — turn() is the door', async () => {
        const outcomes: string[] = [];
        const worker = defineActor({
            type: 'NoDetachedSave',
            unguarded: true,
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('run')
            }),
            tasks: (ctx) => ({
                async run() {
                    // Typed away, but reachable from untyped code — must be a
                    // hard error, not a silent mid-turn write.
                    const untyped = ctx as unknown as {
                        save(): Promise<void>;
                        clearState(): Promise<void>;
                    };
                    await untyped.save().catch((e) => outcomes.push(String(e)));
                    await untyped.clearState().catch((e) => outcomes.push(String(e)));
                    await ctx.turn(async (c) => {
                        c.state.n = 1;
                        await c.save(); // the sanctioned path still works
                    });
                    outcomes.push('turn-save-ok');
                }
            })
        });
        const silo = createSilo({ actors: [worker], defaults: quiet });
        await silo.actor(worker, 'a').begin();
        await until(() => outcomes.length === 3);
        expect(outcomes[0]).toMatch(/save\(\).*DETACHED/s);
        expect(outcomes[1]).toMatch(/clearState\(\).*DETACHED/s);
        expect(outcomes[2]).toBe('turn-save-ok');
        await silo.stop({ timeoutMs: 1000 });
    });

    it('a prototype member is not a task — "toString" is a clean not-found', async () => {
        const worker = defineActor({
            type: 'ProtoSafe',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                begin: (name: string) => ctx.tasks.start(name)
            }),
            tasks: () => ({
                async run() {}
            })
        });
        const silo = createSilo({ actors: [worker], defaults: quiet });
        const client = silo.actor(worker, 'a');
        await expect(client.begin('toString')).rejects.toThrow(/toString/);
        await expect(client.begin('constructor')).rejects.toThrow(/constructor/);
        await silo.stop({ timeoutMs: 1000 });
    });

    it('a thrown task is terminal: it leaves list() and the actor keeps serving', async () => {
        const errors = vi.spyOn(console, 'error').mockImplementation(() => {});
        const boom = defineActor({
            type: 'Boom',
            unguarded: true,
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('explode'),
                running: () => ctx.tasks.list().length,
                bump: () => ++ctx.state.n
            }),
            tasks: () => ({
                async explode() {
                    throw new Error('kaboom');
                }
            })
        });
        const silo = createSilo({ actors: [boom], defaults: quiet });
        const client = silo.actor(boom, 'a');
        await client.begin();
        await until(() => errors.mock.calls.some((c) => String(c[0]).includes('task "explode"')));
        expect(await client.running()).toBe(0);
        expect(await client.bump()).toBe(1); // not poisoned
        await silo.stop({ timeoutMs: 1000 });
    });
});

describe('tasks: cancellation and deactivation', () => {
    it('cancel aborts with reason "cancelled"; the run leaves list() on settle', async () => {
        const reasons: unknown[] = [];
        const worker = defineActor({
            type: 'Cancellable',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('run'),
                cancel: () => ctx.tasks.cancel('run'),
                running: () => ctx.tasks.list().length
            }),
            tasks: (ctx) => ({
                async run() {
                    await aborted(ctx.abortSignal);
                    reasons.push(ctx.abortSignal.reason);
                }
            })
        });
        const silo = createSilo({ actors: [worker], defaults: quiet });
        const client = silo.actor(worker, 'a');
        await client.begin();
        await client.cancel();
        await until(() => reasons.length === 1);
        expect(reasons).toEqual(['cancelled']);
        expect(await client.running()).toBe(0);
        await silo.stop({ timeoutMs: 1000 });
    });

    it('cancelling from a method turn does not deadlock a task that turn()s on wind-down', async () => {
        const done = gate();
        const worker = defineActor({
            type: 'WindDown',
            unguarded: true,
            state: () => ({ phase: 'running' }),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('run'),
                cancel: () => ctx.tasks.cancel('run'),
                read: () => ctx.snapshot().phase
            }),
            tasks: (ctx) => ({
                async run() {
                    await aborted(ctx.abortSignal);
                    // The wind-down turn queues behind the method turn that
                    // called cancel — which must NOT be waiting on us.
                    await ctx.turn((c) => {
                        c.state.phase = 'cancelled';
                    });
                    done.open();
                }
            })
        });
        const silo = createSilo({ actors: [worker], defaults: quiet });
        const client = silo.actor(worker, 'a');
        await client.begin();
        await within(client.cancel(), 1000);
        await within(done.opened, 1000);
        expect(await client.read()).toBe('cancelled');
        await silo.stop({ timeoutMs: 1000 });
    });

    it('cancel wakes a task parked inside ctx.changes() on a quiet actor', async () => {
        const exited = gate();
        const observed: number[] = [];
        const watcher = defineActor({
            type: 'FeedParked',
            unguarded: true,
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('follow'),
                cancel: () => ctx.tasks.cancel('follow'),
                bump: () => ++ctx.state.n,
                running: () => ctx.tasks.list().length
            }),
            tasks: (ctx) => ({
                async follow() {
                    // Parks on the change feed's wake between mutations — on
                    // a quiet actor nothing else would ever resolve it, so
                    // the run's abort must close the feed from outside.
                    for await (const s of ctx.changes()) observed.push(s.n);
                    exited.open();
                }
            })
        });
        const silo = createSilo({ actors: [watcher], defaults: quiet });
        const client = silo.actor(watcher, 'a');
        await client.begin();
        await client.bump();
        await until(() => observed.length === 1);
        await client.cancel();
        await within(exited.opened, 1000); // the for-await ended, body ran on
        await until(async () => (await client.running()) === 0);
        expect(observed).toEqual([1]);
        await silo.stop({ timeoutMs: 1000 });
    });

    it('deactivation wakes a parked feed too — the wind-down checkpoint still lands', async () => {
        const storage = memoryStorage();
        const watcher = defineActor({
            type: 'FeedWindDown',
            unguarded: true,
            state: () => ({ n: 0, phase: 'idle' }),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('follow'),
                read: () => ctx.snapshot()
            }),
            tasks: (ctx) => ({
                async follow() {
                    await ctx.turn((c) => {
                        c.state.phase = 'following';
                    });
                    for await (const _ of ctx.changes()) void _;
                    await ctx.turn(async (c) => {
                        c.state.phase = 'wound-down';
                        await c.save();
                    });
                }
            })
        });
        const silo = createSilo({ actors: [watcher], storage, defaults: quiet });
        const client = silo.actor(watcher, 'a');
        await client.begin();
        await until(async () => (await client.read()).phase === 'following');
        await within(silo.stop({ timeoutMs: 5000 }), 2000);

        // Asserted on STORAGE, not through a fresh silo: activating the
        // grain again would RESUME the interrupted run (that's the
        // durability contract) and its first turn would overwrite the
        // phase before a read could see it.
        const record = await storage.load('FeedWindDown', 'a');
        expect((record?.state as { phase: string }).phase).toBe('wound-down');
    });

    it('deactivation aborts the run BEFORE the drain and lets it checkpoint via turn()', async () => {
        const storage = memoryStorage();
        const reasons: unknown[] = [];
        const worker = defineActor({
            type: 'Checkpointer',
            unguarded: true,
            state: () => ({ phase: 'idle' }),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('run'),
                read: () => ctx.snapshot().phase
            }),
            tasks: (ctx) => ({
                async run() {
                    await ctx.turn((c) => {
                        c.state.phase = 'running';
                    });
                    await aborted(ctx.abortSignal);
                    reasons.push(ctx.abortSignal.reason);
                    // The mailbox is still open through the grace window:
                    // this final checkpoint must land and persist.
                    await ctx.turn(async (c) => {
                        c.state.phase = 'wound-down';
                        await c.save();
                    });
                }
            })
        });
        const silo = createSilo({ actors: [worker], storage, defaults: quiet });
        const client = silo.actor(worker, 'a');
        await client.begin();
        // Wait for the task's first turn to land — it is then parked on its
        // abort signal, the state the stop must find it in.
        await until(async () => (await client.read()) === 'running');
        await within(silo.stop({ timeoutMs: 5000 }), 2000);
        expect(reasons).toEqual(['shutdown']);

        // On storage (see the feed test above for why not via a fresh
        // silo) — and the interrupted run RESUMES there: its restart flips
        // the phase back to 'running', which is the durability contract
        // doing its job.
        const record = await storage.load('Checkpointer', 'a');
        expect((record?.state as { phase: string }).phase).toBe('wound-down');
        const next = createSilo({ actors: [worker], storage, defaults: quiet });
        await until(async () => (await next.actor(worker, 'a').read()) === 'running');
        await next.stop({ timeoutMs: 1000 });
    });

    it('a task ignoring its signal is abandoned after taskGraceMs with a warning', async () => {
        const warns = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const worker = defineActor({
            type: 'Stubborn',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('forever')
            }),
            tasks: () => ({
                async forever() {
                    await new Promise(() => {}); // never settles, ignores abort
                }
            })
        });
        const silo = createSilo({
            actors: [worker],
            defaults: { ...quiet, taskGraceMs: 30 }
        });
        await silo.actor(worker, 'a').begin();
        await within(silo.stop({ timeoutMs: 5000 }), 2000);
        expect(
            warns.mock.calls.some((c) => String(c[0]).includes('ignored their abort signal'))
        ).toBe(true);
    });

    it('starting a task during deactivation throws SiloShutdownError', async () => {
        const outcomes: unknown[] = [];
        const worker = defineActor({
            type: 'LateStarter',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('run')
            }),
            tasks: (ctx) => ({
                async run() {
                    await aborted(ctx.abortSignal);
                    try {
                        await ctx.tasks.start('other');
                        outcomes.push('started');
                    } catch (error) {
                        outcomes.push(error instanceof SiloShutdownError);
                    }
                },
                async other() {}
            })
        });
        const silo = createSilo({ actors: [worker], defaults: quiet });
        await silo.actor(worker, 'a').begin();
        await within(silo.stop({ timeoutMs: 5000 }), 2000);
        expect(outcomes).toEqual([true]);
    });

    it("the base ctx.abortSignal now fires BEFORE the drain — a parked turn can observe it", async () => {
        const worker = defineActor({
            type: 'Parked',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                park: () => aborted(ctx.abortSignal).then(() => 'released')
            })
        });
        const silo = createSilo({ actors: [worker], defaults: quiet });
        const parked = silo.actor(worker, 'a').park();
        await new Promise((r) => setTimeout(r, 10)); // let the turn start
        // Under the OLD ordering (abort after drain) this would hang until
        // the stop deadline force-dropped the activation.
        await within(silo.stop({ timeoutMs: 60_000 }), 2000);
        expect(await within(parked, 1000)).toBe('released');
    });
});

describe('tasks: keep-alive', () => {
    it('the idle sweep skips a grain with a running task, and collects it after settle', async () => {
        const release = gate();
        const worker = defineActor({
            type: 'KeepAlive',
            unguarded: true,
            state: () => ({}),
            idleAfterMs: 0,
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('run')
            }),
            tasks: () => ({
                async run() {
                    await release.opened;
                }
            })
        });
        const scheduler = manualScheduler();
        const silo = createSilo({
            actors: [worker],
            scheduler,
            defaults: { ...quiet, sweepIntervalMs: 1000 }
        });
        await silo.start();
        await silo.actor(worker, 'k').begin();
        scheduler.advance(1000);
        await new Promise((r) => setTimeout(r, 20));
        expect(silo.stats().activations).toBe(1); // running task holds it
        // And the operator can SEE why it is held.
        const row = silo.activations()[0]!;
        expect(row.tasks).toBe(1);
        expect(row.keptAlive).toBe(true);

        release.open();
        await until(() => {
            scheduler.advance(1000);
            return silo.stats().activations === 0;
        });
        await silo.stop({ timeoutMs: 1000 });
    });
});
