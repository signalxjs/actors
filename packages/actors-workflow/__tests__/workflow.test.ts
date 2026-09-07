// @vitest-environment node
/**
 * The engine's contract: a run makes progress, survives its host, and
 * records what it did as a log rather than a series of whole-state saves.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createHost, memoryStorage, type Host } from '@sigx/actors/host';
import {
    applyRunEntry,
    defineWorkflow,
    emptyRun,
    type RunInfo,
    type WorkflowDefinition
} from '@sigx/actors-workflow';

const quiet = { sweepIntervalMs: 0, reminderTickMs: 50 } as const;

const hosts: Host[] = [];
let running: Host | null = null;
afterEach(async () => {
    for (const h of hosts.splice(0)) await h.stop({ timeoutMs: 1000 });
    await running?.stop({ timeoutMs: 1000 });
    running = null;
});

const order: WorkflowDefinition = {
    name: 'order',
    version: 1,
    start: 'charge',
    nodes: {
        charge: { type: 'task', handler: 'charge', input: { amount: '${amount}' }, assignTo: 'receipt', next: 'check' },
        check: { type: 'branch', var: 'receipt', op: 'exists', then: 'ship', else: 'refund' },
        ship: { type: 'task', handler: 'ship', input: 'to:${to}', next: 'done' },
        refund: { type: 'end', status: 'failed', reason: 'no receipt' },
        done: { type: 'end' }
    }
};

function engineWith(handlers: Record<string, (i: unknown, c: unknown) => unknown>, defs = [order]) {
    return defineWorkflow({ definitions: defs, handlers: handlers as never, timerThresholdMs: 200 });
}

async function start(
    wf: ReturnType<typeof engineWith>,
    id: string,
    vars: Record<string, unknown>,
    storage = memoryStorage()
) {
    const host = createHost({ actors: [...wf.actors], storage, defaults: quiet });
    running = host;
    await host.start();
    await (host.actor(wf.run, id) as never as { start(n: string, v: unknown): Promise<unknown> }).start('order', vars);
    return host;
}

/**
 * `memoryStorage()` whose WRITES can be made to fail on demand. Reads keep
 * working, so an actor still activates and still knows what it was doing —
 * which is the situation being tested: recovery runs, and the recovery
 * itself cannot reach its store.
 */
function breakableStorage(inner = memoryStorage()) {
    let failing = false;
    const boom = () => Promise.reject(new Error('storage is down'));
    const s = inner as unknown as Record<string, unknown>;
    const wrapped: Record<string, unknown> = { ...s };
    for (const m of ['save', 'saveText', 'appendText', 'clear']) {
        const orig = s[m] as ((...a: unknown[]) => Promise<unknown>) | undefined;
        if (!orig) continue;
        wrapped[m] = (...a: unknown[]) => (failing ? boom() : orig.call(inner, ...a));
    }
    wrapped['load'] = (...a: unknown[]) =>
        (s['load'] as (...x: unknown[]) => Promise<unknown>).call(inner, ...a);
    return {
        storage: wrapped as unknown as ReturnType<typeof memoryStorage>,
        break: () => {
            failing = true;
        },
        heal: () => {
            failing = false;
        }
    };
}

const read = (host: Host, wf: ReturnType<typeof engineWith>, id: string) =>
    (host.actor(wf.run, id) as never as { status(): Promise<RunInfo> }).status();

describe('a run makes progress', () => {
    it('walks task → branch → task → end and keeps what each step produced', async () => {
        const seen: string[] = [];
        const wf = engineWith({
            charge: (input: unknown) => {
                seen.push(`charge:${JSON.stringify(input)}`);
                return { id: 'r-1' };
            },
            ship: (input: unknown) => {
                seen.push(`ship:${String(input)}`);
                return null;
            }
        });
        const host = await start(wf, 'o1', { amount: 42, to: 'kiruna' });

        await vi.waitFor(async () => expect((await read(host, wf, 'o1')).status).toBe('completed'));
        // `${amount}` alone keeps the number; embedded, it is a string.
        expect(seen).toEqual(['charge:{"amount":42}', 'ship:to:kiruna']);
        const info = await read(host, wf, 'o1');
        expect(info.vars.receipt).toEqual({ id: 'r-1' });
        // start → charge → check → ship → done
        expect(info.transitions).toBe(4);
    });

    it('takes the else branch and can end failed', async () => {
        const wf = engineWith({ charge: () => null, ship: () => null });
        const host = await start(wf, 'o2', { amount: 1 });
        await vi.waitFor(async () => expect((await read(host, wf, 'o2')).status).toBe('failed'));
        expect((await read(host, wf, 'o2')).error).toBe('no receipt');
    });
});

describe('failure and retry', () => {
    it('retries with backoff and succeeds on a later attempt', async () => {
        let calls = 0;
        const flaky: WorkflowDefinition = {
            name: 'order',
            version: 1,
            start: 'charge',
            nodes: {
                charge: {
                    type: 'task',
                    handler: 'charge',
                    retry: { maxAttempts: 3, backoffMs: 5, maxBackoffMs: 10 },
                    next: 'done'
                },
                done: { type: 'end' }
            }
        };
        const wf = engineWith({
            charge: () => {
                calls += 1;
                if (calls < 3) throw new Error('nope');
                return 'ok';
            }
        }, [flaky]);
        const host = await start(wf, 'o3', {});
        await vi.waitFor(async () => expect((await read(host, wf, 'o3')).status).toBe('completed'), {
            timeout: 4000
        });
        expect(calls).toBe(3);
    });

    it('fails the run when the attempts are spent, carrying the message', async () => {
        const doomed: WorkflowDefinition = {
            name: 'order',
            version: 1,
            start: 'charge',
            nodes: {
                charge: { type: 'task', handler: 'charge', retry: { maxAttempts: 2, backoffMs: 1 }, next: 'done' },
                done: { type: 'end' }
            }
        };
        const wf = engineWith({ charge: () => { throw new Error('card declined'); } }, [doomed]);
        const host = await start(wf, 'o4', {});
        await vi.waitFor(async () => expect((await read(host, wf, 'o4')).status).toBe('failed'), {
            timeout: 4000
        });
        expect((await read(host, wf, 'o4')).error).toBe('card declined');
    });

    it('a missing handler fails the run rather than hanging it', async () => {
        const wf = engineWith({ ship: () => null });
        const host = await start(wf, 'o5', { amount: 1 });
        await vi.waitFor(async () => expect((await read(host, wf, 'o5')).status).toBe('failed'));
        expect((await read(host, wf, 'o5')).error).toMatch(/no handler registered/);
    });
});

describe('sleeping', () => {
    it('a delay under the threshold rides a timer and the run continues', async () => {
        const napping: WorkflowDefinition = {
            name: 'order',
            version: 1,
            start: 'nap',
            // Long enough that `sleeping` is observable, still under the
            // 200 ms threshold so it stays a volatile timer. A 30 ms nap
            // finished before the first poll and made the sleep
            // unobservable — the run was right and the test was racy.
            nodes: { nap: { type: 'delay', ms: 120, next: 'done' }, done: { type: 'end' } }
        };
        const wf = engineWith({}, [napping]);
        const host = await start(wf, 'o6', {});
        await vi.waitFor(async () => expect((await read(host, wf, 'o6')).status).toBe('sleeping'));
        // It really waited: the run is still asleep well after a
        // zero-delay hop would have carried it past.
        await new Promise((r) => setTimeout(r, 40));
        expect((await read(host, wf, 'o6')).status).toBe('sleeping');
        await vi.waitFor(async () => expect((await read(host, wf, 'o6')).status).toBe('completed'));
    });

    it('a delay at the threshold rides a durable reminder, and the run leaves memory', async () => {
        const napping: WorkflowDefinition = {
            name: 'order',
            version: 1,
            start: 'nap',
            nodes: { nap: { type: 'delay', ms: 250, next: 'done' }, done: { type: 'end' } }
        };
        const wf = engineWith({}, [napping]);
        const host = await start(wf, 'o7', {});
        // 250 >= the 200 ms threshold, so it is durable and deactivates.
        await vi.waitFor(() => expect(host.activations()).toHaveLength(0));
        // …and the reminder tick brings it back with no poll from us.
        await vi.waitFor(async () => expect((await read(host, wf, 'o7')).status).toBe('completed'), {
            timeout: 4000
        });
    });

    it('a recovery that cannot reach storage does not crash the host', async () => {
        // nudge() runs from onActivate and from a plain status() read,
        // neither of which awaits it. An unobserved rejection there
        // reaches Node's unhandled-rejection handler, which by default
        // ends the PROCESS — one run's storage error taking down a host
        // carrying every other run on it.
        const rejections: unknown[] = [];
        const onRejection = (e: unknown) => rejections.push(e);
        process.on('unhandledRejection', onRejection);
        try {
            const napping: WorkflowDefinition = {
                name: 'order',
                version: 1,
                start: 'nap',
                nodes: { nap: { type: 'delay', ms: 120, next: 'done' }, done: { type: 'end' } }
            };
            const broken = breakableStorage();
            const wf = engineWith({}, [napping]);
            const first = await start(wf, 'o9', {}, broken.storage);
            await vi.waitFor(async () => expect((await read(first, wf, 'o9')).status).toBe('sleeping'));

            // The host dies mid-nap, so the volatile timer dies with it
            // and the wake is owed by whoever loads the run next.
            await first.stop({ timeoutMs: 1000 });
            running = null;
            await new Promise((r) => setTimeout(r, 160)); // now overdue

            broken.break();
            const second = createHost({ actors: [...wf.actors], storage: broken.storage, defaults: quiet });
            hosts.push(second);
            await second.start();

            // Activating takes the overdue-wake path, whose append fails.
            // The host survives it and the read still answers — no
            // unhandled rejection, which is the whole point.
            await read(second, wf, 'o9');
            await new Promise((r) => setTimeout(r, 50));
            expect(rejections).toEqual([]);

            // And nothing was lost: recovery is idempotent, so once the
            // store is back a touch carries the same run to completion.
            // (`ctx.append` folds the entry into memory BEFORE it
            // persists, so a failed append leaves the in-memory view
            // ahead of the record — which is exactly why the assertion
            // that matters is this one and not the status mid-outage.)
            broken.heal();
            await vi.waitFor(async () => expect((await read(second, wf, 'o9')).status).toBe('completed'), {
                timeout: 4000
            });
            expect(rejections).toEqual([]);
        } finally {
            process.off('unhandledRejection', onRejection);
        }
    });
});

describe('the log is the state', () => {
    it('replays to the same run on a host that never saw it run', async () => {
        const storage = memoryStorage();
        const wf = engineWith({ charge: () => ({ id: 'r-9' }), ship: () => null });
        const first = await start(wf, 'o8', { amount: 7, to: 'abisko' }, storage);
        await vi.waitFor(async () => expect((await read(first, wf, 'o8')).status).toBe('completed'));

        // STOP it. Reading back from the same host would only prove the
        // in-memory state is intact, which is not what the log is for —
        // a broken reducer would sail past that.
        await first.stop({ timeoutMs: 1000 });
        running = null;

        // A second host over the same storage, which has never run this
        // workflow: everything it can tell us came off the record and
        // through `applyRunEntry`.
        const second = createHost({ actors: [...wf.actors], storage, defaults: quiet });
        hosts.push(second);
        await second.start();
        const info = await read(second, wf, 'o8');
        expect(info.status).toBe('completed');
        expect(info.vars).toEqual({ amount: 7, to: 'abisko', receipt: { id: 'r-9' } });
        expect(info.transitions).toBe(4);
        expect(info.cursor).toBe('done');
    });

    it('a malformed entry is ignored rather than stranding the run', () => {
        // The reducer runs on activation, so a throw here fails the
        // activation and stands the run up dead — which is why it is
        // total. An entry from a newer build is where a surprise shape
        // would come from.
        const state = emptyRun();
        for (const bad of [null, undefined, 42, 'nope', {}, { t: 'unknown' },
            { t: 'start', def: 'd', version: 1, at: 1, vars: null },
            { t: 'vars', set: 'not-a-record' }]) {
            expect(() => applyRunEntry(state, bad)).not.toThrow();
        }
        expect(state.vars).toEqual({});
    });
});
