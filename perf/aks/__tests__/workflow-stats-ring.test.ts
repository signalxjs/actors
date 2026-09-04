/**
 * The aggregator's ring under overflow (#383). `WorkflowStats` retains the
 * last `WF_STATS_RING` events for `drain`; a poller slower than the ring
 * turns over loses events and is told so through `dropped` rather than
 * the counts quietly coming up short. Every other suite asserts `dropped`
 * is zero; this one fills the ring past capacity and asserts the gap is
 * REPORTED, and that the totals still count every run.
 *
 * Its own file because the ring size is read at module load: a 20-entry
 * ring under the unit suite's cases would break their `eventFor`.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Host } from '@sigx/actors';
import { defineActorApp, memoryStorage } from '@sigx/actors/host';

process.env.WF_TIMER_THRESHOLD_MS = '100';
process.env.WF_REMINDER_TICK_MS = '50';
process.env.WF_STALE_WAKE_MS = '300';
process.env.WF_CHILD_STALE_MS = '200';
process.env.WF_DEACTIVATE_ON_SLEEP = '1';
process.env.WF_IDLE_AFTER_MS = '600000';
process.env.WF_STATS_SAVE_EVERY = '1';
process.env.WF_NOTIFY_RETRY_MS = '300';
process.env.WF_STATS_RING = '20';

type Engine = typeof import('../src/workflow/index.ts');
let wf: Engine;
let host: Host;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

beforeAll(async () => {
    wf = await import('../src/workflow/index.ts');
    host = await defineActorApp({
        actors: [...wf.workflowActors],
        storage: memoryStorage(),
        defaults: { reminderTickMs: 50 }
    }).start();
});

afterAll(async () => {
    await host.stop();
});

describe('WorkflowStats — the ring past capacity', { timeout: 20_000 }, () => {
    it('reports the events it dropped, keeps the newest, and still counts every run', async () => {
        expect(wf.workflowConfig.statsRing).toBe(20);
        const name = 'quick';
        await host.actor(wf.WorkflowDefinition, name).put({
            name,
            version: 1,
            start: 'work',
            nodes: { work: { type: 'task', worker: 'io', ms: 5, next: 'end' }, end: { type: 'end' } }
        });
        const stats = host.actor(wf.WorkflowStats, 'all');
        await stats.reset();
        const total = 25;
        const tag = 'ring';
        const ids = Array.from({ length: total }, (_, i) => `ring-${i}`);
        await Promise.all(ids.map((id) => host.actor(wf.WorkflowRun, id).start({ workflow: name, template: 'q', tag })));
        const deadline = Date.now() + 8_000;
        while ((await stats.snapshot()).total < total) {
            if (Date.now() > deadline) throw new Error(`only ${(await stats.snapshot()).total}/${total} events after 8s`);
            await sleep(20);
        }
        const drained = await stats.drain(tag, 0, 100_000);
        expect(drained.dropped).toBe(total - 20);
        expect(drained.events.length).toBe(20);
        // The newest survive: the retained seqs are the last 20 of 25.
        expect(drained.events.map((e) => e.seq)).toEqual(Array.from({ length: 20 }, (_, i) => i + 6));
        expect(drained.cursor).toBe(total);
        // Draining from the cursor reports nothing new and nothing dropped.
        const again = await stats.drain(tag, drained.cursor, 100_000);
        expect(again.events).toEqual([]);
        expect(again.dropped).toBe(0);
        // The totals count every run, ring or no ring.
        const snapshot = await stats.snapshot();
        expect(snapshot.total).toBe(total);
        expect(snapshot.byTemplate['q']).toEqual({ completed: total });
        expect(wf.workflowCounters.statsEvents).toBe(total);
    });
});
