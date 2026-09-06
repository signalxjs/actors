// @vitest-environment node
/**
 * #409: a run that DEACTIVATES while it waits, and loses its host.
 *
 * `helpers/wf-cluster.ts` pins `WF_DEACTIVATE_ON_SLEEP=0`, because a run
 * that leaves memory the moment it blocks has no owner to kill — so the
 * production shape, where the knob is on by default, is the one shape the
 * cluster suite cannot reach. The chaos run of 2026-09-05 ran with it ON
 * and stranded 9 runs of 2 986: 2 `sleeping`, 5 `waiting`, 2 `blocked`.
 *
 * Those two statuses are also the two that `engine.nudge()` cannot rescue
 * once a run has no wake left in state: its last branch re-arms an advance
 * only for `running`, `compensating` and `blocked`. A `waiting` or
 * `sleeping` run whose wake is gone is therefore invisible to every touch
 * the engine has — `status()`, a fresh activation, the join watchdog.
 *
 * These cases ask whether a deactivated waiter survives losing the host
 * that was going to serve it.
 */
import { createServer, type Server } from 'node:http';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Host } from '@sigx/actors';
import { defineActorApp, memoryStorage } from '@sigx/actors/host';
import { createAppHandler } from '@sigx/actors/node';
import { cluster, memoryClusterHub, type ClusterPlacement } from '@sigx/actors/cluster';

process.env.WF_TIMER_THRESHOLD_MS = '100';
process.env.WF_REMINDER_TICK_MS = '50';
process.env.WF_STALE_WAKE_MS = '300';
process.env.WF_CHILD_STALE_MS = '200';
// The production shape, and the whole point of this file.
process.env.WF_DEACTIVATE_ON_SLEEP = '1';
process.env.WF_IDLE_AFTER_MS = '600000';
process.env.WF_STATS_SAVE_EVERY = '1';
process.env.WF_NOTIFY_RETRY_MS = '300';

type Engine = typeof import('../src/workflow/index.ts');
const NUL = String.fromCharCode(0);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const closed = (s: Server) => new Promise<void>((r) => s.close(() => r()));

interface Member {
    host: Host;
    placement: ClusterPlacement;
    server: Server;
    hostId: string;
    dead: boolean;
}

describe('a deactivated waiter whose host dies (#409)', { timeout: 30_000 }, () => {
    let wf: Engine;
    const hub = memoryClusterHub();
    const storage = memoryStorage();
    const members: Member[] = [];

    async function boot(): Promise<Member> {
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
            storage,
            defaults: { reminderTickMs: 50 }
        }).use(plugin);
        handler = createAppHandler(app, { origin: false });
        const host = await app.start();
        return {
            host,
            placement: plugin.placement,
            server,
            hostId: plugin.placement.identity.hostId,
            dead: false
        };
    }

    beforeAll(async () => {
        wf = await import('../src/workflow/index.ts');
        for (let i = 0; i < 3; i++) members.push(await boot());
    });

    afterAll(async () => {
        for (const m of members) {
            if (m.dead) continue;
            await m.host.stop().catch(() => {});
            await closed(m.server);
        }
    });

    const alive = () => members.filter((m) => !m.dead);
    const anyHost = () => alive()[0]!.host;
    const run = (id: string, host = anyHost()) => host.actor(wf.WorkflowRun, id);

    async function kill(m: Member): Promise<void> {
        m.dead = true;
        await closed(m.server);
        hub.kill(m.hostId);
        await m.host.stop({ timeoutMs: 500 }).catch(() => {});
        members.push(await boot());
    }

    let seq = 0;
    const fresh = (p: string) => `${p}-${Date.now().toString(36)}-${++seq}`;

    async function seed(over: Partial<Engine['DEFAULT_KNOBS']>): Promise<number> {
        const version = 7000 + ++seq;
        const k = { ...wf.DEFAULT_KNOBS, taskMs: 5, delayMs: 300, failureRate: 0, ...over, version };
        for (const def of wf.allDefinitions(k)) {
            await anyHost().actor(wf.WorkflowDefinition, def.name).put(def);
        }
        return version;
    }

    async function untilStatus(id: string, wanted: string[], ms = 8_000) {
        const deadline = Date.now() + ms;
        for (;;) {
            const s = await run(id).status();
            if (wanted.includes(s.status)) return s;
            if (Date.now() > deadline) throw new Error(`${id} still ${s.status} after ${ms}ms`);
            await sleep(20);
        }
    }

    /** The shard the run's wake lives on is ticked by whichever host owns
     *  it; a deactivated run has no owner of its own to kill. */
    async function ownerOfShard(id: string): Promise<Member | undefined> {
        const entry = await hub.directory.lookup(`WorkflowRun${NUL}${id}`);
        return entry ? members.find((m) => m.hostId === entry.hostId) : undefined;
    }

    it('a deactivated waiter still times out after the host that ran it dies', async () => {
        const version = await seed({ signalTimeoutMs: 400 });
        const id = fresh('deact-wait');
        const tag = 'deact-wait';
        await run(id).start({ workflow: 'approval', version, template: 'approval', tag });
        const waiting = await untilStatus(id, ['waiting']);
        expect(waiting.wake?.kind).toBe('reminder');

        // Whoever last held it — a deactivated run's claim may already be
        // released, in which case the kill lands on a bystander and the
        // case is still a fair test of the survivor path.
        const owner = (await ownerOfShard(id)) ?? alive()[0]!;
        await kill(owner);

        // No signal ever arrives, and nothing touches the run: the timeout
        // is the only thing that can end it.
        const deadline = Date.now() + 10_000;
        for (;;) {
            const { events } = await anyHost().actor(wf.WorkflowStats, 'all').drain(tag, 0, 100_000);
            const e = events.find((x) => x.runId === id);
            if (e) {
                expect(e.status).toBe('completed');
                return;
            }
            if (Date.now() > deadline) {
                const s = await run(id).status();
                throw new Error(
                    `stranded: status=${s.status} wake=${JSON.stringify(s.wake)} ` +
                        `— no completion 10s after a 400ms timeout`
                );
            }
            await sleep(50);
        }
    });

    it('a touch re-arms a waiting run whose armed wake vanished', async () => {
        // The recovery that DOES work, pinned here for the deactivating
        // shape: the wake stays in state, so `nudge` sees it, finds it past
        // the stale window and re-arms it as a timer-fallback.
        //
        // This is also the boundary of what a touch can do, and the reason
        // the strand in #409 is still unexplained: `nudge`'s last branch
        // re-arms an advance only for `running`, `compensating` and
        // `blocked`, so a `waiting` or `sleeping` run with wake === null
        // would be inert — but no path here can produce that state, because
        // the engine always writes a status and its wake in one save.
        const version = await seed({ signalTimeoutMs: 400 });
        const id = fresh('inert');
        await run(id).start({ workflow: 'approval', version, template: 'approval', tag: 'inert' });
        await untilStatus(id, ['waiting']);
        await run(id).debugDropWake();
        // Past due and past the stale window.
        await sleep(400 + 300 + 200);
        const touched = await run(id).status();
        // A touch must leave it heading somewhere: either recovered already,
        // or with a wake armed to recover from.
        expect(
            wf.TERMINAL.has(touched.status) || touched.status === 'running' || touched.wake !== null
        ).toBe(true);
    });
});
