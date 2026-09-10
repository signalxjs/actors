/**
 * The aggregator sharded and appending (#432). On sixteen hosts the
 * singleton `WorkflowStats` was the knee: whichever host owned it was
 * liveness-killed past 100 offered, and every save of its 50 000-event
 * ring was ~12 MB of JSON and 12 MB of AOF. Two knobs answer it, both
 * defaulting to the recorded behaviour: `WF_STATS_SHARDS` hashes each run
 * id onto one of N actors, and `WF_STATS_APPEND` persists each event as an
 * O(entry) `ctx.append` with a full save only every
 * `WF_STATS_COMPACT_EVERY` events.
 *
 * Its own file because all three are read at module load.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { ActorStorage, Host } from '@sigx/actors';
import { defineActorApp, memoryStorage } from '@sigx/actors/host';

process.env.WF_TIMER_THRESHOLD_MS = '100';
process.env.WF_REMINDER_TICK_MS = '50';
process.env.WF_STALE_WAKE_MS = '300';
process.env.WF_CHILD_STALE_MS = '200';
process.env.WF_DEACTIVATE_ON_SLEEP = '1';
process.env.WF_IDLE_AFTER_MS = '600000';
process.env.WF_NOTIFY_RETRY_MS = '300';
process.env.WF_STATS_RING = '1000';
process.env.WF_STATS_SHARDS = '4';
process.env.WF_STATS_APPEND = '1';
process.env.WF_STATS_COMPACT_EVERY = '8';

type Engine = typeof import('../src/workflow/index.ts');
let wf: Engine;
let host: Host;
let storage: ActorStorage;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

beforeAll(async () => {
    wf = await import('../src/workflow/index.ts');
    storage = memoryStorage();
    host = await defineActorApp({
        actors: [...wf.workflowActors],
        storage,
        defaults: { reminderTickMs: 50 }
    }).start();
    const name = 'quick';
    await host.actor(wf.WorkflowDefinition, name).put({
        name,
        version: 1,
        start: 'work',
        nodes: { work: { type: 'task', worker: 'io', ms: 5, next: 'end' }, end: { type: 'end' } }
    });
});

afterAll(async () => {
    await host.stop();
    // The knobs are read at module load, but a worker that runs another
    // workflow suite next must not inherit them.
    for (const name of ['WF_STATS_SHARDS', 'WF_STATS_APPEND', 'WF_STATS_COMPACT_EVERY', 'WF_STATS_RING']) {
        delete process.env[name];
    }
});

beforeEach(() => {
    wf.resetCounters();
});

const SHARDS = ['s0', 's1', 's2', 's3'];

async function totalAcross(): Promise<number> {
    let total = 0;
    for (const key of SHARDS) total += (await host.actor(wf.WorkflowStats, key).snapshot()).total;
    return total;
}

async function runMany(prefix: string, count: number, tag: string): Promise<string[]> {
    const ids = Array.from({ length: count }, (_, i) => `${prefix}-${i}`);
    await Promise.all(
        ids.map((id) => host.actor(wf.WorkflowRun, id).start({ workflow: 'quick', template: 'q', tag }))
    );
    const deadline = Date.now() + 8_000;
    while ((await totalAcross()) < count) {
        if (Date.now() > deadline) throw new Error(`only ${await totalAcross()}/${count} events after 8s`);
        await sleep(20);
    }
    return ids;
}

describe('WorkflowStats — sharded and appending', { timeout: 20_000 }, () => {
    it('reads the shape it was given', () => {
        expect(wf.workflowConfig.statsShards).toBe(4);
        expect(wf.workflowConfig.statsAppend).toBe(true);
        expect(wf.workflowConfig.statsCompactEvery).toBe(8);
    });

    it('spreads completions over every shard, every run counted exactly once, and `all` says how many shards', async () => {
        for (const key of SHARDS) await host.actor(wf.WorkflowStats, key).reset();
        const ids = await runMany('shard', 40, 'shards');
        const perShard = await Promise.all(
            SHARDS.map((key) => host.actor(wf.WorkflowStats, key).snapshot().then((s) => s.total))
        );
        expect(perShard.reduce((a, b) => a + b, 0)).toBe(40);
        // FNV-1a over 40 ids does not leave a shard empty.
        for (const n of perShard) expect(n).toBeGreaterThan(0);
        // The generator learns N from a key that is not a shard.
        expect((await host.actor(wf.WorkflowStats, 'all').snapshot()).shards).toBe(4);
        // Draining every shard by its own cursor yields each run once.
        const seen = new Set<string>();
        for (const key of SHARDS) {
            const drained = await host.actor(wf.WorkflowStats, key).drain('shards', 0, 100_000);
            expect(drained.dropped).toBe(0);
            for (const e of drained.events) {
                expect(seen.has(e.runId)).toBe(false);
                seen.add(e.runId);
            }
        }
        expect([...seen].sort()).toEqual([...ids].sort());
        // A run's shard is a pure function of its id.
        for (const id of ids) expect(SHARDS).toContain(wf.statsShardKey(id));
    });

    it('persists each event as an appended entry, compacts every WF_STATS_COMPACT_EVERY, and a fresh activation replays the log', async () => {
        // One shard on its own: hash-pick ids that land on s0 until we have 20.
        for (const key of SHARDS) await host.actor(wf.WorkflowStats, key).reset();
        const ids: string[] = [];
        for (let i = 0; ids.length < 20; i++) {
            const id = `append-${i}`;
            if (wf.statsShardKey(id) === 's0') ids.push(id);
        }
        await Promise.all(
            ids.map((id) => host.actor(wf.WorkflowRun, id).start({ workflow: 'quick', template: 'q', tag: 'append' }))
        );
        const stats = host.actor(wf.WorkflowStats, 's0');
        const deadline = Date.now() + 8_000;
        while ((await stats.snapshot()).total < 20) {
            if (Date.now() > deadline) throw new Error('s0 did not see 20 events');
            await sleep(20);
        }
        // 20 events at a compaction every 8: two full saves (at 8 and 16),
        // so the record holds the state as of event 16 plus a 4-entry log.
        const record = await storage.load('WorkflowStats', 's0');
        expect(record).not.toBeNull();
        const stored = record!.state as { total: number; events: unknown[] };
        expect(stored.total).toBe(16);
        expect(record!.log).toHaveLength(4);
        // The log replays to the live state: a fresh activation folds it.
        const drained = await stats.drain('append', 0, 100_000);
        expect(drained.events).toHaveLength(20);
        // `seq` is monotonic across resets (a cursor never rewinds), so the
        // twenty are consecutive and end at the shard's current seq.
        const seqs = drained.events.map((e) => e.seq);
        const last = (await stats.snapshot()).seq;
        expect(seqs).toEqual(Array.from({ length: 20 }, (_, i) => last - 19 + i));

        // The reason append mode is safe to run: a fresh activation on
        // another host sees the compacted state PLUS the replayed log, not
        // just the last full save. Same storage, new host, nothing else.
        const before = await stats.snapshot();
        await host.stop();
        host = await defineActorApp({
            actors: [...wf.workflowActors],
            storage,
            defaults: { reminderTickMs: 50 }
        }).start();
        const after = await host.actor(wf.WorkflowStats, 's0').snapshot();
        expect(after.total).toBe(20);
        expect(after.seq).toBe(before.seq);
        expect(after.byTemplate).toEqual(before.byTemplate);
        expect(after.sums).toEqual(before.sums);
        const replayed = await host.actor(wf.WorkflowStats, 's0').drain('append', 0, 100_000);
        expect(replayed.events).toHaveLength(20);
        // The compaction cadence survives the restart: the four replayed
        // entries count, so four more events (24 = 3 × 8) compact the ring
        // rather than starting a fresh count of eight.
        const more: string[] = [];
        for (let i = 1000; more.length < 4; i++) {
            const id = `append-${i}`;
            if (wf.statsShardKey(id) === 's0') more.push(id);
        }
        await Promise.all(
            more.map((id) => host.actor(wf.WorkflowRun, id).start({ workflow: 'quick', template: 'q', tag: 'append' }))
        );
        const deadline2 = Date.now() + 8_000;
        while ((await host.actor(wf.WorkflowStats, 's0').snapshot()).total < 24) {
            if (Date.now() > deadline2) throw new Error('s0 did not see 24 events');
            await sleep(20);
        }
        const compacted = await storage.load('WorkflowStats', 's0');
        expect((compacted!.state as { total: number }).total).toBe(24);
        expect(compacted!.log).toHaveLength(0);
    });
});
