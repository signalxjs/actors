/**
 * The workflow engine under host loss (#297, #383) — the cases, written
 * ONCE and parameterised by the cluster's database, so the memory-backed
 * file and the Redis-backed file cannot drift apart (the conformance-suite
 * rule from AGENTS.md: a seam with several implementations gets one set of
 * assertions, not N copies).
 *
 * Three in-process hosts on a `memoryClusterHub`, one shared storage as
 * the cluster's database, the `examples/counter/job-demo.mjs` recipe.
 * Every case kills the host that OWNS something mid-flight and asserts the
 * run still ends correctly through a survivor. A kill is abrupt (socket
 * closed, membership entry dropped) and then the dead host's process-side
 * is stopped, because in this one process a "dead" host's timers would
 * otherwise keep firing and finish the run from beyond the grave — a
 * zombie the etag CAS fences in production, but here the point is to
 * prove the SURVIVOR path, so the corpse is made to hold still.
 *
 * The engine's env (`WF_*`) is read at module load, so the CALLER sets it
 * before `workflowClusterSuite` imports the engine — the suite only pins
 * the one knob it cannot do without: `WF_DEACTIVATE_ON_SLEEP=0`, because a
 * run that deactivates the moment it sleeps has no owner to kill. The unit
 * suite covers the deactivating shape.
 */
import { createServer, type Server } from 'node:http';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { ActorStorage, Host } from '@sigx/actors';
import type { RunStatus } from '../../src/workflow/types.ts';
import { defineActorApp } from '@sigx/actors/host';
import { createAppHandler } from '@sigx/actors/node';
import { cluster, memoryClusterHub, type ClusterPlacement } from '@sigx/actors/cluster';

type Engine = typeof import('../../src/workflow/index.ts');

const NUL = String.fromCharCode(0);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface Member {
    host: Host;
    placement: ClusterPlacement;
    server: Server;
    hostId: string;
    dead: boolean;
}

export interface WorkflowClusterHarness {
    /** The cluster's one database, shared by every host. */
    storage: ActorStorage;
    /** Release what `storage` holds — a Redis namespace, a client. */
    stop?(): Promise<void>;
}

export function workflowClusterSuite(name: string, harness: () => Promise<WorkflowClusterHarness>): void {
    let wf: Engine;
    let h: WorkflowClusterHarness;
    const hub = memoryClusterHub();
    const members: Member[] = [];

    async function boot(): Promise<Member> {
        // Listen on an ephemeral port first: the advertise address has to be
        // known before the plugin exists, and the plugin before the handler.
        let handler: ReturnType<typeof createAppHandler> | null = null;
        const server = createServer((req, res) => handler!(req, res));
        await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
        const address = server.address();
        const port = typeof address === 'object' && address ? address.port : 0;
        const plugin = cluster({
            providers: hub.providers(),
            advertise: `http://127.0.0.1:${port}`,
            secret: 'test-secret'
        });
        const app = defineActorApp({
            actors: [...wf.workflowActors],
            storage: h.storage,
            defaults: { reminderTickMs: 50 }
        }).use(plugin);
        handler = createAppHandler(app, { origin: false });
        const host = await app.start();
        return { host, placement: plugin.placement, server, hostId: plugin.placement.identity.hostId, dead: false };
    }

    beforeAll(async () => {
        h = await harness();
        wf = await import('../../src/workflow/index.ts');
        for (let i = 0; i < 3; i++) members.push(await boot());
    });

    afterAll(async () => {
        for (const m of members) {
            if (m.dead) continue;
            await m.host.stop().catch(() => {});
            m.server.close();
        }
        await h.stop?.();
    });

    beforeEach(() => {
        wf.resetCounters();
    });

    const alive = () => members.filter((m) => !m.dead);
    const anyHost = () => alive()[0]!.host;

    async function ownerOf(type: string, key: string): Promise<Member> {
        const entry = await hub.directory.lookup(`${type}${NUL}${key}`);
        const owner = entry ? members.find((m) => m.hostId === entry.hostId) : undefined;
        if (!owner) throw new Error(`${type}/${key} has no owner in the directory`);
        return owner;
    }

    /** Abrupt: socket gone, membership gone — and then the corpse held still. */
    async function kill(m: Member): Promise<void> {
        m.dead = true;
        m.server.close();
        hub.kill(m.hostId);
        await m.host.stop({ timeoutMs: 500 }).catch(() => {});
        // A replacement keeps the cluster at three so later cases have a
        // spread to kill into.
        members.push(await boot());
    }

    let seq = 0;
    const fresh = (prefix: string) => `${prefix}-${Date.now().toString(36)}-${++seq}`;

    async function seed(over: Partial<Engine['DEFAULT_KNOBS']>): Promise<number> {
        const version = 5000 + ++seq;
        const k = { ...wf.DEFAULT_KNOBS, taskMs: 5, delayMs: 300, failureRate: 0, ...over, version };
        for (const def of wf.allDefinitions(k)) {
            await anyHost().actor(wf.WorkflowDefinition, def.name).put(def);
        }
        return version;
    }

    const run = (id: string, host = anyHost()) => host.actor(wf.WorkflowRun, id);

    async function untilStatus(id: string, wanted: RunStatus[], ms = 8_000) {
        const deadline = Date.now() + ms;
        for (;;) {
            const s = await run(id).status();
            if (wanted.includes(s.status)) return s;
            if (wf.TERMINAL.has(s.status)) throw new Error(`${id} ended ${s.status} waiting for ${wanted}`);
            if (Date.now() > deadline) throw new Error(`${id} still ${s.status} after ${ms}ms`);
            await sleep(20);
        }
    }

    /** Wait WITHOUT touching the run — a touch is a nudge, and some cases
     *  need to prove the runtime recovered on its own. */
    async function untilEvent(id: string, tag: string, ms = 8_000) {
        const deadline = Date.now() + ms;
        for (;;) {
            const { events } = await anyHost().actor(wf.WorkflowStats, 'all').drain(tag, 0, 100_000);
            const e = events.find((x) => x.runId === id);
            if (e) return e;
            if (Date.now() > deadline) throw new Error(`no completion event for ${id} after ${ms}ms`);
            await sleep(25);
        }
    }

    /** Wait on a process-wide engine counter — the one way to observe a
     *  turn that is still holding its actor (a `status()` would queue
     *  behind it). */
    async function untilCounter(read: () => number, atLeast: number, ms = 5_000) {
        const deadline = Date.now() + ms;
        while (read() < atLeast) {
            if (Date.now() > deadline) throw new Error(`counter still ${read()} < ${atLeast} after ${ms}ms`);
            await sleep(10);
        }
    }

    describe(`WorkflowRun across hosts (${name})`, { timeout: 20_000 }, () => {
        it('places runs across the cluster', async () => {
            const version = await seed({ delayMs: 20 });
            const ids = Array.from({ length: 12 }, () => fresh('spread'));
            await Promise.all(
                ids.map((id) => run(id).start({ workflow: 'order', version, template: 'order', tag: 'spread' }))
            );
            await Promise.all(ids.map((id) => untilEvent(id, 'spread')));
            expect(wf.workflowCounters.runsFinished).toBe(12);
        });

        it('survives the owner dying while the run sleeps on a durable reminder — no node re-runs', async () => {
            const version = await seed({ delayMs: 400 });
            const id = fresh('sleeper');
            const tag = 'sleeper';
            await run(id).start({ workflow: 'order', version, template: 'order', tag });
            const sleeping = await untilStatus(id, ['sleeping']);
            expect(sleeping.wake?.kind).toBe('reminder');
            const attemptsBefore = sleeping.attempts;
            const owner = await ownerOf('WorkflowRun', id);
            await kill(owner);
            // No touch: the reminder shard is re-owned by a survivor, the tick
            // fires it, placement re-activates the run wherever it lands.
            const event = await untilEvent(id, tag);
            expect(event.status).toBe('completed');
            const done = await run(id).status();
            // notify ran once after the wake; nothing before it re-ran.
            expect(done.attempts).toBe(attemptsBefore + 1);
            expect(wf.workflowCounters.remindersFired).toBeGreaterThanOrEqual(1);
            expect(wf.workflowCounters.wakesLost).toBe(0);
        });

        it('a run sleeping on a VOLATILE timer needs a touch after its owner dies, then completes', async () => {
            const version = await seed({ delayMs: 60 });
            const id = fresh('volatile');
            await run(id).start({ workflow: 'order', version, template: 'order', tag: 'v' });
            const sleeping = await untilStatus(id, ['sleeping']);
            expect(sleeping.wake?.kind).toBe('timer');
            const owner = await ownerOf('WorkflowRun', id);
            await kill(owner);
            await sleep(200);
            // Nothing has fired: the timer died with the host. This is the
            // documented cost of the volatile kind — and what the parent's
            // join watchdog and the loadgen's final sweep exist to recover.
            expect(wf.workflowCounters.timersFired).toBe(0);
            const touched = await run(id).status(); // the touch: re-activation re-arms
            expect(['sleeping', 'running', 'completed']).toContain(touched.status);
            const event = await untilEvent(id, 'v');
            expect(event.status).toBe('completed');
            expect(wf.workflowCounters.timersRearmed).toBeGreaterThanOrEqual(1);
        });

        it('a task in flight when its owner dies runs AGAIN on a survivor — at-least-once', async () => {
            // The "activity scheduled" save before every attempt is the
            // whole claim: a crash after it re-runs the attempt on the next
            // activation. One `io` task long enough to outlive the kill.
            const name = fresh('longtask');
            await anyHost().actor(wf.WorkflowDefinition, name).put({
                name,
                version: 1,
                start: 'work',
                nodes: {
                    work: { type: 'task', worker: 'io', ms: 1_500, next: 'end' },
                    end: { type: 'end' }
                }
            });
            const id = fresh('midtask');
            const tag = 'midtask';
            await run(id).start({ workflow: name, template: 'longtask', tag });
            // The attempt's turn holds the actor while the worker runs, so
            // a `status()` here would queue behind it — read the counter.
            await untilCounter(() => wf.workflowCounters.taskAttempts, 1);
            await sleep(100); // past the attempt's own save
            const owner = await ownerOf('WorkflowRun', id);
            await kill(owner);
            // A touch through a survivor: the record says `running` with no
            // wake, so the nudge re-arms the advance and the task re-runs.
            const touched = await run(id).status();
            expect(['running', 'completed']).toContain(touched.status);
            const event = await untilEvent(id, tag);
            expect(event.status).toBe('completed');
            const done = await run(id).status();
            // Attempt 1 died with the host; attempt 2 completed it. The
            // node moved once (work → end) and the run ended once.
            expect(done.attempts).toBe(2);
            expect(done.transitions).toBe(2);
            expect(wf.workflowCounters.taskAttempts).toBe(2);
        });

        it('a signal through a survivor reaches a run whose owner died while waiting', async () => {
            const version = await seed({ signalTimeoutMs: 5_000 });
            const id = fresh('waiter');
            await run(id).start({ workflow: 'approval', version, template: 'approval', tag: 'w' });
            await untilStatus(id, ['waiting']);
            const owner = await ownerOf('WorkflowRun', id);
            await kill(owner);
            const r = await run(id).signal('approve', { via: 'survivor' });
            expect(r.accepted).toBe(true);
            const event = await untilEvent(id, 'w');
            expect(event.status).toBe('completed');
            expect(await run(id).vars()).toEqual({ 'signal:approve': { via: 'survivor' } });
            expect(wf.workflowCounters.signalTimeouts).toBe(0);
        });

        it('a parent killed mid-fan-out is re-placed by its children reporting in', async () => {
            const version = await seed({});
            const child = fresh('slowchild');
            await anyHost().actor(wf.WorkflowDefinition, child).put({
                name: child,
                version: 1,
                start: 'work',
                nodes: {
                    work: { type: 'task', worker: 'io', ms: 5, next: 'nap' },
                    nap: { type: 'delay', ms: 400, next: 'end' },
                    end: { type: 'end' }
                }
            });
            const parentDef = fresh('fanparent');
            await anyHost().actor(wf.WorkflowDefinition, parentDef).put({
                name: parentDef,
                version: 1,
                start: 'spread',
                nodes: {
                    spread: { type: 'fanout', width: 2, mode: 'children', child: { workflow: child }, next: 'end' },
                    end: { type: 'end' }
                }
            });
            void version;
            const id = fresh('parent');
            await run(id).start({ workflow: parentDef, template: 'fan', tag: 'fan' });
            await untilStatus(id, ['blocked']);
            const owner = await ownerOf('WorkflowRun', id);
            await kill(owner);
            // The children (asleep on reminders on other hosts, or re-placed
            // themselves) finish and call childDone — which re-places the
            // parent on a survivor. No touch from here.
            const event = await untilEvent(id, 'fan');
            expect(event.status).toBe('completed');
            expect(wf.workflowCounters.childDoneCalls).toBeGreaterThanOrEqual(2);
        });

        it("a child's owner dying mid-run still completes the parent", async () => {
            const version = await seed({ delayMs: 400, fanoutWidth: 2, fanoutMode: 'children' });
            // etl-chunk has no delay; give the children one so there is a
            // moment to kill into.
            const chunk = fresh('napchunk');
            await anyHost().actor(wf.WorkflowDefinition, chunk).put({
                name: chunk,
                version: 1,
                start: 'nap',
                nodes: { nap: { type: 'delay', ms: 400, next: 'end' }, end: { type: 'end' } }
            });
            const parentDef = fresh('parent2');
            await anyHost().actor(wf.WorkflowDefinition, parentDef).put({
                name: parentDef,
                version: 1,
                start: 'spread',
                nodes: {
                    spread: { type: 'fanout', width: 2, mode: 'children', child: { workflow: chunk }, next: 'end' },
                    end: { type: 'end' }
                }
            });
            void version;
            const id = fresh('parent');
            await run(id).start({ workflow: parentDef, template: 'fan2', tag: 'fan2' });
            const childId = `${id}.spread.0`;
            await untilStatus(childId, ['sleeping']);
            const owner = await ownerOf('WorkflowRun', childId);
            await kill(owner);
            const event = await untilEvent(id, 'fan2');
            expect(event.status).toBe('completed');
        });

        it('the stats aggregator re-places after its owner dies and keeps counting', async () => {
            const version = await seed({ delayMs: 20 });
            const warm = fresh('warm');
            await run(warm).start({ workflow: 'order', version, template: 'order', tag: 'stats' });
            await untilEvent(warm, 'stats');
            const owner = await ownerOf('WorkflowStats', 'all');
            await kill(owner);
            const ids = Array.from({ length: 4 }, () => fresh('after'));
            await Promise.all(
                ids.map((id) => run(id).start({ workflow: 'order', version, template: 'order', tag: 'stats2' }))
            );
            const events = await Promise.all(ids.map((id) => untilEvent(id, 'stats2')));
            expect(events.every((e) => e.status === 'completed')).toBe(true);
            // The ring is durable state: the warm-up run's event survived the
            // move too.
            const { events: before } = await anyHost().actor(wf.WorkflowStats, 'all').drain('stats', 0);
            expect(before.map((e) => e.runId)).toContain(warm);
        });
    });
}
