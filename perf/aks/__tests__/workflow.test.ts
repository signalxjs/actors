/**
 * The workflow engine's correctness (#297), on ONE in-process host.
 *
 * Every case here is a behaviour the load generator's numbers silently
 * depend on: a wake honoured exactly once, a signal that arrived early
 * still delivered, a compensation walked backwards, a join that is a set
 * and not a counter, an overdue wake recovered by a touch. A perf rig
 * that gets one of them wrong reports a plausible figure for a broken
 * engine — which is why they are asserted rather than demonstrated.
 *
 * The shape is set through env BEFORE the engine is imported (the knobs
 * are read at module load, like `actors.app.ts`'s): a 100 ms timer
 * threshold so both wake kinds are reachable with millisecond delays, a
 * 50 ms reminder tick, and a 300 ms stale window. Real clock throughout —
 * fake timers do not drive the host scheduler.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { Host } from '@sigx/actors';
import type { RunStatus, WorkflowDef } from '../src/workflow/types.ts';

process.env.WF_TIMER_THRESHOLD_MS = '100';
process.env.WF_REMINDER_TICK_MS = '50';
process.env.WF_STALE_WAKE_MS = '300';
process.env.WF_CHILD_STALE_MS = '200';
process.env.WF_DEACTIVATE_ON_SLEEP = '1';
process.env.WF_IDLE_AFTER_MS = '600000';
process.env.WF_STATS_SAVE_EVERY = '1';
process.env.WF_NOTIFY_RETRY_MS = '300';

type Engine = typeof import('../src/workflow/index.ts');
type Workers = typeof import('../src/workflow/workers.ts');
let wf: Engine;
let roll: Workers['roll'];
let host: Host;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

beforeAll(async () => {
    const { app } = await import('../src/actors.app.ts');
    wf = await import('../src/workflow/index.ts');
    ({ roll } = await import('../src/workflow/workers.ts'));
    host = await app.withActors([...wf.workflowActors]).start();
});

afterAll(async () => {
    await host.stop();
});

beforeEach(() => {
    wf.resetCounters();
});

const knobs = (over: Partial<Engine['DEFAULT_KNOBS']> = {}): Engine['DEFAULT_KNOBS'] => ({
    ...wf.DEFAULT_KNOBS,
    taskMs: 5,
    delayMs: 20,
    fanoutWidth: 3,
    failureRate: 0,
    signalTimeoutMs: 5_000,
    retryBackoffMs: 10,
    ...over
});

let seq = 0;
const fresh = (prefix: string) => `${prefix}-${Date.now().toString(36)}-${++seq}`;

/** Seed every template definition under a fresh version and return it. */
async function seed(k: Engine['DEFAULT_KNOBS']): Promise<number> {
    const version = 1000 + ++seq;
    for (const def of wf.allDefinitions({ ...k, version })) {
        await host.actor(wf.WorkflowDefinition, def.name).put(def);
    }
    return version;
}

async function putDef(def: WorkflowDef): Promise<void> {
    await host.actor(wf.WorkflowDefinition, def.name).put(def);
}

const run = (id: string) => host.actor(wf.WorkflowRun, id);

async function untilTerminal(id: string, ms = 5_000) {
    const deadline = Date.now() + ms;
    for (;;) {
        const s = await run(id).status();
        if (wf.TERMINAL.has(s.status)) return s;
        if (Date.now() > deadline) throw new Error(`${id} still ${s.status} after ${ms}ms`);
        await sleep(10);
    }
}

async function untilStatus(id: string, status: RunStatus, ms = 5_000) {
    const deadline = Date.now() + ms;
    for (;;) {
        const s = await run(id).status();
        if (s.status === status) return s;
        if (wf.TERMINAL.has(s.status)) throw new Error(`${id} ended ${s.status} waiting for ${status}`);
        if (Date.now() > deadline) throw new Error(`${id} still ${s.status} after ${ms}ms`);
        await sleep(10);
    }
}

/** The completion event for one run, from the aggregator. */
async function eventFor(id: string, tag: string) {
    const { events } = await host.actor(wf.WorkflowStats, 'all').drain(tag, 0, 100_000);
    const event = events.find((e) => e.runId === id);
    if (!event) throw new Error(`no completion event for ${id}`);
    return event;
}

describe('WorkflowRun — the templates', () => {
    it('runs an order to completion, one transition per node', async () => {
        const version = await seed(knobs());
        const id = fresh('order');
        const tag = fresh('tag');
        const started = await run(id).start({ workflow: 'order', version, template: 'order', tag });
        expect(started.status).toBe('running');
        const done = await untilTerminal(id);
        expect(done.status).toBe('completed');
        // validate, reserve-and-charge, ship-delay, notify, end — the
        // parallel branches run INSIDE the parallel node's turn.
        expect(done.transitions).toBe(5);
        // validate + reserve-stock + charge + notify
        expect(done.attempts).toBe(4);
        const event = await eventFor(id, tag);
        expect(event.status).toBe('completed');
        expect(event.endedAt).toBeGreaterThanOrEqual(event.startedAt);
    });

    it('start() is idempotent', async () => {
        const version = await seed(knobs());
        const id = fresh('idem');
        await run(id).start({ workflow: 'order', version, template: 'order' });
        const again = await run(id).start({ workflow: 'order', version, template: 'order' });
        expect(again.status).not.toBe('pending');
        await untilTerminal(id);
    });

    it('a delay under the threshold sleeps on a timer; over it, on a reminder that lets the run leave memory', async () => {
        const short = await seed(knobs({ delayMs: 20 }));
        const a = fresh('timer');
        await run(a).start({ workflow: 'order', version: short, template: 'order', tag: 't' });
        await untilTerminal(a);
        const ea = await eventFor(a, 't');
        expect(ea.status).toBe('completed');
        expect(wf.workflowCounters.timersFired).toBeGreaterThanOrEqual(1);
        expect(wf.workflowCounters.remindersSet).toBe(0);

        wf.resetCounters();
        const long = await seed(knobs({ delayMs: 150 }));
        const b = fresh('reminder');
        await run(b).start({ workflow: 'order', version: long, template: 'order', tag: 't' });
        // Reaches the delay node in a few ms, arms the reminder, deactivates.
        await sleep(80);
        const resident = host.activations().filter((x) => x.type === 'WorkflowRun' && x.key === b);
        expect(resident).toHaveLength(0);
        expect(wf.workflowCounters.remindersSet).toBe(1);
        // No status() poll here — a touch would re-activate it. The
        // reminder tick (50 ms) re-activates it on its own.
        await sleep(400);
        const eb = await eventFor(b, 't');
        expect(eb.status).toBe('completed');
        expect(wf.workflowCounters.remindersFired).toBe(1);
        expect(wf.workflowCounters.wakesLost).toBe(0);
        // The run's OWN account of its wakes survived the deactivation
        // that followed arming the reminder.
        const snap = await host.actor(wf.WorkflowStats, 'all').snapshot();
        expect(snap.sums.reminders).toBeGreaterThanOrEqual(1);
    });

    it('a branch reads the run input', async () => {
        const name = fresh('branchy');
        await putDef({
            name,
            version: 1,
            start: 'decide',
            nodes: {
                decide: { type: 'branch', var: 'amount', op: 'gt', value: 100, then: 'big', else: 'small' },
                big: { type: 'delay', ms: 5, next: 'small' },
                small: { type: 'task', worker: 'io', ms: 1, next: 'end' },
                end: { type: 'end' }
            }
        });
        const big = fresh('big');
        await run(big).start({ workflow: name, template: 'b', input: { amount: 150 } });
        const small = fresh('small');
        await run(small).start({ workflow: name, template: 'b', input: { amount: 50 } });
        const [db, ds] = await Promise.all([untilTerminal(big), untilTerminal(small)]);
        expect(db.transitions).toBe(4); // decide, big, small, end
        expect(ds.transitions).toBe(3); // decide, small, end
    });
});

describe('WorkflowRun — signals', () => {
    it('a signal that arrives BEFORE the wait node is buffered and then delivered', async () => {
        const version = await seed(knobs({ taskMs: 60 }));
        const id = fresh('early');
        await run(id).start({ workflow: 'approval', version, template: 'approval', tag: 's' });
        // `submit` takes 60 ms; the signal lands while it runs.
        const r = await run(id).signal('approve', { by: 'test' });
        expect(r.accepted).toBe(true);
        const done = await untilTerminal(id);
        expect(done.status).toBe('completed');
        const vars = await run(id).vars();
        expect(vars['signal:approve']).toEqual({ by: 'test' });
        const e = await eventFor(id, 's');
        expect(e.status).toBe('completed');
        expect(wf.workflowCounters.signalsBuffered).toBe(1);
        expect(wf.workflowCounters.signalsDelivered).toBe(1);
        expect(wf.workflowCounters.signalTimeouts).toBe(0);
    });

    it('a signal that arrives WHILE waiting wakes the run', async () => {
        const version = await seed(knobs({ signalTimeoutMs: 5_000 }));
        const id = fresh('waiting');
        await run(id).start({ workflow: 'approval', version, template: 'approval' });
        const waiting = await untilStatus(id, 'waiting');
        expect(waiting.wake?.reason).toBe('signal-timeout');
        expect(waiting.wake?.kind).toBe('reminder');
        await run(id).signal('approve', 1);
        const done = await untilTerminal(id);
        expect(done.status).toBe('completed');
        expect(await run(id).vars()).toEqual({ 'signal:approve': 1 });
        expect(wf.workflowCounters.signalsBuffered).toBe(0);
        expect(wf.workflowCounters.signalsDelivered).toBe(1);
    });

    it('no signal → the timeout edge; a signal after that is late and ignored', async () => {
        const version = await seed(knobs({ signalTimeoutMs: 40 }));
        const id = fresh('timeout');
        await run(id).start({ workflow: 'approval', version, template: 'approval' });
        const done = await untilTerminal(id);
        expect(done.status).toBe('completed');
        expect(await run(id).vars()).toEqual({});
        expect(wf.workflowCounters.signalTimeouts).toBe(1);
        const late = await run(id).signal('approve', 1);
        expect(late.accepted).toBe(false);
        expect(wf.workflowCounters.signalsLate).toBe(1);
    });
});

describe('WorkflowRun — failure, retry, compensation', () => {
    it('retries with backoff until an attempt succeeds', async () => {
        const name = fresh('flaky');
        await putDef({
            name,
            version: 1,
            start: 'try',
            nodes: {
                try: {
                    type: 'task',
                    worker: 'io',
                    ms: 1,
                    failureRate: 0.5,
                    retry: { maxAttempts: 5, backoffMs: 10 },
                    next: 'end'
                },
                end: { type: 'end' }
            }
        });
        // Deterministic failure: pick a run id whose first two attempts
        // fail and whose third succeeds.
        let id = '';
        for (let i = 0; i < 10_000 && !id; i++) {
            const candidate = `r${i}`;
            const rolls = [1, 2, 3].map((a) => roll(`${candidate}:try:${a}`));
            if (rolls[0]! < 0.5 && rolls[1]! < 0.5 && rolls[2]! >= 0.5) id = candidate;
        }
        expect(id).not.toBe('');
        await run(id).start({ workflow: name, template: 'f' });
        const done = await untilTerminal(id);
        expect(done.status).toBe('completed');
        expect(done.attempts).toBe(3);
        expect(wf.workflowCounters.taskFailures).toBe(2);
        expect(wf.workflowCounters.timersFired).toBe(2); // two retry sleeps
    });

    it('fails the run when retries are exhausted and nothing compensates', async () => {
        const name = fresh('doomed');
        await putDef({
            name,
            version: 1,
            start: 'try',
            nodes: {
                try: { type: 'task', worker: 'io', ms: 1, failureRate: 1, retry: { maxAttempts: 2, backoffMs: 5 }, next: 'end' },
                end: { type: 'end' }
            }
        });
        const id = fresh('doomed');
        await run(id).start({ workflow: name, template: 'd', tag: 'd' });
        const done = await untilTerminal(id);
        expect(done.status).toBe('failed');
        expect(done.attempts).toBe(2);
        expect(done.error).toMatch(/injected/);
        expect((await eventFor(id, 'd')).status).toBe('failed');
    });

    it('a saga walks its compensations backwards and ends compensated', async () => {
        // failureRate 1: book-hotel fails every attempt (3), book-car is
        // never reached, so the only compensation is cancel-flight.
        const version = await seed(knobs({ failureRate: 1, retryMax: 3 }));
        const id = fresh('saga');
        await run(id).start({ workflow: 'saga', version, template: 'saga', tag: 'c' });
        const done = await untilTerminal(id);
        expect(done.status).toBe('compensated');
        // book-flight 1 + book-hotel 3 + cancel-flight 1
        expect(done.attempts).toBe(5);
        expect(wf.workflowCounters.compensations).toBe(1);
        const e = await eventFor(id, 'c');
        expect(e.status).toBe('compensated');
    });

    it('a parallel branch failing fails the node (and its done siblings are compensable)', async () => {
        const name = fresh('par');
        await putDef({
            name,
            version: 1,
            start: 'both',
            onFailure: 'compensate',
            nodes: {
                both: { type: 'parallel', branches: [['ok'], ['bad']], next: 'end' },
                ok: { type: 'task', worker: 'io', ms: 1, compensate: 'undo-ok', next: 'end' },
                bad: { type: 'task', worker: 'io', ms: 5, failureRate: 1, next: 'end' },
                'undo-ok': { type: 'task', worker: 'io', ms: 1, next: 'end' },
                end: { type: 'end' }
            }
        });
        const id = fresh('par');
        await run(id).start({ workflow: name, template: 'p' });
        const done = await untilTerminal(id);
        expect(done.status).toBe('compensated');
        expect(done.attempts).toBe(3); // ok, bad, undo-ok
    });
});

describe('WorkflowRun — fan-out and joins', () => {
    it('fans out to child runs and joins when every child reports', async () => {
        const version = await seed(knobs({ fanoutWidth: 3, fanoutMode: 'children' }));
        const id = fresh('etl');
        await run(id).start({ workflow: 'etl', version, template: 'etl', tag: 'e' });
        const done = await untilTerminal(id);
        expect(done.status).toBe('completed');
        expect(wf.workflowCounters.childStarts).toBe(3);
        expect(wf.workflowCounters.childDoneCalls).toBe(3);
        const { events } = await host.actor(wf.WorkflowStats, 'all').drain('e', 0, 100_000);
        const children = events.filter((e) => e.parent && e.runId.startsWith(`${id}.`));
        expect(children).toHaveLength(3);
        expect(children.every((c) => c.template === 'etl-chunk')).toBe(true);
    });

    it('a duplicate childDone is a no-op and an unknown child is refused', async () => {
        const version = await seed(knobs({ fanoutWidth: 2, fanoutMode: 'children' }));
        const id = fresh('dup');
        await run(id).start({ workflow: 'etl', version, template: 'etl' });
        await untilTerminal(id);
        const dup = await run(id).childDone(`${id}.extract.0`, 'completed');
        expect(dup).toEqual({ accepted: true, duplicate: true });
        expect(await run(id).childDone('nobody', 'completed')).toEqual({ accepted: false });
        expect(wf.workflowCounters.childDoneDuplicates).toBe(1);
    });

    it('fans out to pool tasks inside one turn', async () => {
        const version = await seed(knobs({ fanoutWidth: 4, fanoutMode: 'tasks' }));
        const id = fresh('pool');
        await run(id).start({ workflow: 'etl', version, template: 'etl' });
        const done = await untilTerminal(id);
        expect(done.status).toBe('completed');
        expect(done.attempts).toBe(1 + 4 + 1); // plan, 4 units, aggregate
        expect(wf.workflowCounters.childStarts).toBe(0);
    });

    it('a subworkflow completes its parent without a call-chain deadlock', async () => {
        const version = await seed(knobs());
        const name = fresh('outer');
        await putDef({
            name,
            version: 1,
            start: 'inner',
            nodes: {
                inner: { type: 'subworkflow', workflow: 'etl-chunk', version, next: 'end' },
                end: { type: 'end' }
            }
        });
        const id = fresh('outer');
        await run(id).start({ workflow: name, template: 'o' });
        const done = await untilTerminal(id);
        expect(done.status).toBe('completed');
        expect(wf.workflowCounters.childDoneCalls).toBe(1);
    });
});

describe('WorkflowRun — lost wakes and cancellation', () => {
    it('an overdue durable wake is recovered by a status() touch', async () => {
        const version = await seed(knobs({ delayMs: 150 }));
        const id = fresh('lost');
        await run(id).start({ workflow: 'order', version, template: 'order', tag: 'l' });
        const sleeping = await untilStatus(id, 'sleeping');
        expect(sleeping.wake?.kind).toBe('reminder');
        // The reminder vanishes (a host died between persist and dispatch).
        await run(id).debugDropWake();
        // Past due + the stale window, and nothing has fired.
        await sleep(150 + 300 + 100);
        const still = await run(id).status();
        expect(['sleeping', 'running', 'completed']).toContain(still.status);
        const done = await untilTerminal(id);
        expect(done.status).toBe('completed');
        expect(wf.workflowCounters.wakesLost).toBe(1);
        expect((await eventFor(id, 'l')).status).toBe('completed');
    });

    it('a volatile timer lost with its activation is re-armed on the next activation', async () => {
        const version = await seed(knobs({ delayMs: 60 }));
        const id = fresh('volatile');
        await run(id).start({ workflow: 'order', version, template: 'order' });
        const sleeping = await untilStatus(id, 'sleeping');
        expect(sleeping.wake?.kind).toBe('timer');
        await host.deactivate({ type: 'WorkflowRun', key: id });
        await sleep(100);
        // Nothing fired: the timer died with the activation.
        expect(wf.workflowCounters.timersFired).toBe(0);
        const done = await untilTerminal(id);
        expect(done.status).toBe('completed');
        expect(wf.workflowCounters.timersRearmed).toBeGreaterThanOrEqual(1);
    });

    it('cancel() from a sleep ends the run and the wake never applies', async () => {
        const version = await seed(knobs({ delayMs: 40 }));
        const id = fresh('cancel');
        await run(id).start({ workflow: 'order', version, template: 'order', tag: 'x' });
        await untilStatus(id, 'sleeping');
        const cancelled = await run(id).cancel();
        expect(cancelled.status).toBe('cancelled');
        await sleep(120);
        const after = await run(id).status();
        expect(after.status).toBe('cancelled');
        expect(after.wake).toBeNull();
        expect((await eventFor(id, 'x')).status).toBe('cancelled');
    });

    it('cancel() from a wait clears the durable wake', async () => {
        const version = await seed(knobs({ signalTimeoutMs: 200 }));
        const id = fresh('cancelwait');
        await run(id).start({ workflow: 'approval', version, template: 'approval' });
        await untilStatus(id, 'waiting');
        await run(id).cancel();
        await sleep(350);
        expect((await run(id).status()).status).toBe('cancelled');
        expect(wf.workflowCounters.signalTimeouts).toBe(0);
    });
});

describe('WorkflowDefinition', () => {
    it('put is idempotent by version and get returns the latest', async () => {
        const name = fresh('defs');
        const v1 = { ...wf.order({ ...knobs(), version: 1 }), name };
        const v2 = { ...wf.order({ ...knobs(), version: 2 }), name };
        const defs = host.actor(wf.WorkflowDefinition, name);
        expect(await defs.put(v1)).toEqual({ version: 1, created: true });
        expect(await defs.put(v1)).toEqual({ version: 1, created: false });
        expect(await defs.put(v2)).toEqual({ version: 2, created: true });
        expect((await defs.get()).version).toBe(2);
        expect((await defs.get(1)).version).toBe(1);
        expect(await defs.versions()).toEqual([1, 2]);
        await expect(defs.get(9)).rejects.toThrow(/unknown workflow/);
        expect(wf.workflowCounters.defReads).toBe(3);
    });

    it('refuses a definition put under the wrong key', async () => {
        await expect(
            host.actor(wf.WorkflowDefinition, 'other').put(wf.order(knobs()))
        ).rejects.toThrow(/put under key/);
    });
});

describe('WorkflowStats', () => {
    it('drains by tag and cursor, and snapshots per-template counts', async () => {
        const version = await seed(knobs());
        const tag = fresh('tag');
        const ids = [fresh('s1'), fresh('s2')];
        for (const id of ids) {
            await run(id).start({ workflow: 'order', version, template: 'order', tag });
        }
        await Promise.all(ids.map((id) => untilTerminal(id)));
        const stats = host.actor(wf.WorkflowStats, 'all');
        const first = await stats.drain(tag, 0);
        expect(first.events.map((e) => e.runId).sort()).toEqual([...ids].sort());
        expect(first.dropped).toBe(0);
        const second = await stats.drain(tag, first.cursor);
        expect(second.events).toEqual([]);
        const snap = await stats.snapshot();
        expect(snap.byTemplate.order?.completed).toBeGreaterThanOrEqual(2);
        expect(snap.latencyMs.order?.count).toBeGreaterThanOrEqual(2);
        expect(snap.nodeMs.task?.count).toBeGreaterThanOrEqual(8);
    });
});

describe('templates', () => {
    it('seeds a version per knob bag, not per run', () => {
        const a = wf.seedVersionFor({ ...wf.DEFAULT_KNOBS, delayMs: 2_000 });
        const b = wf.seedVersionFor({ ...wf.DEFAULT_KNOBS, delayMs: 90_000 });
        const c = wf.seedVersionFor({ ...wf.DEFAULT_KNOBS, delayMs: 2_000, version: 7 });
        expect(a).not.toBe(b);
        expect(a).toBe(c); // `version` itself is not part of the hash
        expect(Number.isInteger(a) && a > 0).toBe(true);
    });

    it('parses a mix and refuses an unknown template', () => {
        expect(wf.templateWeights('order:50,approval:20,etl:20,saga:10')).toEqual({
            order: 50,
            approval: 20,
            etl: 20,
            saga: 10
        });
        expect(wf.templateWeights('order')).toEqual({ order: 1, approval: 0, etl: 0, saga: 0 });
        expect(() => wf.templateWeights('orders:1')).toThrow(/unknown template/);
        expect(() => wf.templateWeights('order:0')).toThrow(/bad weight/);
        const w = wf.templateWeights('order:1,saga:1');
        expect(wf.pickTemplate(w, 0.1)).toBe('order');
        expect(wf.pickTemplate(w, 0.9)).toBe('saga');
    });
});
