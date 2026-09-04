/**
 * The workflow engine's recovery paths, driven by injected faults (#383).
 *
 * Three counters the engine keeps were, until this file, only ever
 * asserted to be ZERO: `reminderSetFailures` (a durable wake that could not
 * be armed), `publishFailures` (a completion the aggregator never
 * recorded) and `joinRepairs` (a child start the watchdog had to re-issue).
 * A counter that is only ever asserted zero is decoration — nothing proves
 * the path behind it does what its name says. Each case here makes ONE
 * write fail through `helpers/faults.ts` and asserts the engine's answer.
 *
 * One host, its own app (the unit suite's module-scope app cannot take a
 * decorated storage), the same short shape the unit suite runs under, set
 * through env BEFORE the engine is imported. `WF_DEACTIVATE_ON_SLEEP=0` so
 * a sleeping run's wake kind can be read back without re-activating it.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { Host } from '@sigx/actors';
import type { RunStatus } from '../src/workflow/types.ts';
import { defineActorApp, memoryStorage, shardedReminders } from '@sigx/actors/host';
import { faultReminders, faultStorage } from './helpers/faults.ts';

process.env.WF_TIMER_THRESHOLD_MS = '100';
process.env.WF_REMINDER_TICK_MS = '50';
process.env.WF_STALE_WAKE_MS = '300';
process.env.WF_CHILD_STALE_MS = '200';
process.env.WF_DEACTIVATE_ON_SLEEP = '0';
process.env.WF_IDLE_AFTER_MS = '600000';
process.env.WF_STATS_SAVE_EVERY = '1';
process.env.WF_NOTIFY_RETRY_MS = '300';

type Engine = typeof import('../src/workflow/index.ts');
let wf: Engine;
let host: Host;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const storage = faultStorage(memoryStorage());
const reminders = faultReminders(shardedReminders());

beforeAll(async () => {
    wf = await import('../src/workflow/index.ts');
    host = await defineActorApp({
        actors: [...wf.workflowActors],
        storage: storage.storage,
        reminders: reminders.reminders,
        defaults: { reminderTickMs: 50 }
    }).start();
});

afterAll(async () => {
    await host.stop();
});

beforeEach(() => {
    wf.resetCounters();
    storage.setRule(null);
    reminders.setRule(null);
    storage.rejected.length = 0;
    reminders.rejected.length = 0;
});

let seq = 0;
const fresh = (prefix: string) => `${prefix}-${Date.now().toString(36)}-${++seq}`;

const run = (id: string) => host.actor(wf.WorkflowRun, id);

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

async function untilTerminal(id: string, ms = 8_000) {
    const deadline = Date.now() + ms;
    for (;;) {
        const s = await run(id).status();
        if (wf.TERMINAL.has(s.status)) return s;
        if (Date.now() > deadline) throw new Error(`${id} still ${s.status} after ${ms}ms`);
        await sleep(10);
    }
}

async function eventsFor(tag: string) {
    const { events } = await host.actor(wf.WorkflowStats, 'all').drain(tag, 0, 100_000);
    return events;
}

describe('WorkflowRun — when a write fails', { timeout: 20_000 }, () => {
    it('a durable wake whose reminder cannot be armed falls back to a volatile timer', async () => {
        const name = fresh('napper');
        await host.actor(wf.WorkflowDefinition, name).put({
            name,
            version: 1,
            start: 'nap',
            nodes: { nap: { type: 'delay', ms: 1_500, next: 'end' }, end: { type: 'end' } }
        });
        // Every attempt the engine makes (its own retry on top of the
        // runtime's CAS retries) loses.
        reminders.setRule({ match: (_id, n) => n === 'wake', times: wf.workflowConfig.reminderSetAttempts });
        const id = fresh('fallback');
        const tag = 'fallback';
        await run(id).start({ workflow: name, template: 'napper', tag });
        // The sleep turn holds the actor through its retries, so the first
        // status that reads `sleeping` is the one after the decision.
        const sleeping = await untilStatus(id, 'sleeping');
        expect(sleeping.wake?.kind).toBe('timer-fallback');
        expect(reminders.rejected.length).toBe(wf.workflowConfig.reminderSetAttempts);
        expect(wf.workflowCounters.reminderSetFailures).toBe(1);
        expect(wf.workflowCounters.remindersSet).toBe(0);
        // The volatile timer is a real wake: the run still completes.
        const done = await untilTerminal(id);
        expect(done.status).toBe('completed');
        expect(wf.workflowCounters.timersFired).toBeGreaterThanOrEqual(1);
        const snapshot = await host.actor(wf.WorkflowStats, 'all').snapshot();
        expect(snapshot.sums.fallback).toBeGreaterThanOrEqual(1);
    });

    it("a completion the aggregator cannot persist is counted as a publish failure and is NOT in the ring", async () => {
        const name = fresh('quick');
        await host.actor(wf.WorkflowDefinition, name).put({
            name,
            version: 1,
            start: 'work',
            nodes: { work: { type: 'task', worker: 'io', ms: 5, next: 'end' }, end: { type: 'end' } }
        });
        // Warm the aggregator so it has a record, then make its NEXT save
        // lose the CAS: the handler's turn rejects, the publish reports a
        // failure, the aggregator faults and re-activates from storage.
        const warm = fresh('warm');
        await run(warm).start({ workflow: name, template: 'quick', tag: 'warm' });
        await untilTerminal(warm);
        await sleep(100);
        wf.resetCounters();
        storage.setRule({ match: (type) => type === 'WorkflowStats', times: 1 });
        const id = fresh('unreported');
        const tag = 'unreported';
        await run(id).start({ workflow: name, template: 'quick', tag });
        const done = await untilTerminal(id);
        // The run itself is fine — a publish failure never reaches it.
        expect(done.status).toBe('completed');
        const deadline = Date.now() + 3_000;
        while (wf.workflowCounters.publishes + wf.workflowCounters.publishFailures < 1 && Date.now() < deadline) {
            await sleep(10);
        }
        expect(storage.rejected.map((r) => r.type)).toEqual(['WorkflowStats']);
        expect(wf.workflowCounters.publishFailures).toBe(1);
        expect(wf.workflowCounters.runsFinished).toBe(1);
        // What the generator would see: `completedUnreported` — the run
        // finished, the ring never got it.
        await sleep(100);
        expect((await eventsFor(tag)).map((e) => e.runId)).not.toContain(id);
        // The aggregator is back, from storage, and still records.
        const after = fresh('after');
        await run(after).start({ workflow: name, template: 'quick', tag: 'after' });
        await untilTerminal(after);
        const deadline2 = Date.now() + 3_000;
        while (!(await eventsFor('after')).some((e) => e.runId === after) && Date.now() < deadline2) {
            await sleep(10);
        }
        expect((await eventsFor('after')).map((e) => e.runId)).toContain(after);
    });

    it('a child start that never lands is re-issued by the join watchdog', async () => {
        const child = fresh('chunk');
        await host.actor(wf.WorkflowDefinition, child).put({
            name: child,
            version: 1,
            start: 'work',
            nodes: { work: { type: 'task', worker: 'io', ms: 5, next: 'end' }, end: { type: 'end' } }
        });
        const parentDef = fresh('fanparent');
        await host.actor(wf.WorkflowDefinition, parentDef).put({
            name: parentDef,
            version: 1,
            start: 'spread',
            nodes: {
                spread: { type: 'fanout', width: 2, mode: 'children', child: { workflow: child }, next: 'end' },
                end: { type: 'end' }
            }
        });
        const id = fresh('parent');
        const tag = 'repair';
        const lostChild = `${id}.spread.1`;
        // The second child's FIRST write — its `start()` save — loses.
        storage.setRule({ match: (type, key) => type === 'WorkflowRun' && key === lostChild, times: 1 });
        await run(id).start({ workflow: parentDef, template: 'fan', tag });
        const done = await untilTerminal(id);
        expect(done.status).toBe('completed');
        expect(storage.rejected).toEqual([{ type: 'WorkflowRun', key: lostChild, op: 'save' }]);
        expect(wf.workflowCounters.childStartFailures).toBe(1);
        // The watchdog found the child still `running` past WF_CHILD_STALE_MS
        // and re-issued its idempotent start — once.
        expect(wf.workflowCounters.joinRepairs).toBe(1);
        // `childStarts` counts the fan-out's own successful starts; the
        // watchdog's re-issue is counted under `joinRepairs`, not here.
        expect(wf.workflowCounters.childStarts).toBe(1);
        expect(wf.workflowCounters.childDoneCalls).toBe(2);
        const lost = await run(lostChild).status();
        expect(lost.status).toBe('completed');
    });
});
