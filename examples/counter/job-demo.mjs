/**
 * Durable-job demo — the "put a long-running operation on a cluster and
 * check on it" story, end to end. Three silos, one job:
 *
 *   1. start a multi-second CrunchJob through silo 0 and return instantly
 *   2. watch live progress from a NON-owner silo (routed watch stream)
 *   3. kill the silo that owns the job, mid-run
 *   4. a status() call through a survivor re-places the grain, and the
 *      task ledger RESUMES the run from its last checkpoint (attempt 2)
 *   5. the job completes on the survivor; result() proves nothing was lost
 *
 * The client-visible timeline never breaks: the same key keeps answering
 * status() and watch() before, during and after the crash.
 *
 * Run after `pnpm build`:   node examples/counter/job-demo.mjs
 * Needs Node >= 22.18 (built-in type stripping for the .ts job import).
 * Ports default to 5394-5396; override with JOB_DEMO_PORTS=7001,7002,7003.
 *
 * (Without any client call, the liveness reminder re-activates the grain
 * on a survivor within ~60-90s — the demo calls status() so you don't
 * have to watch paint dry; see the README's Tasks section.)
 */
import { createServer } from 'node:http';
import { defineActorApp, memoryStorage } from '@sigx/actors/silo';
import { createAppHandler } from '@sigx/actors/node';
import { cluster, memoryClusterHub } from '@sigx/actors/cluster';
import { CrunchJob } from './src/crunch.job.ts';

const SEP = String.fromCharCode(0);
const PORTS = (process.env.JOB_DEMO_PORTS ?? '5394,5395,5396')
    .split(',')
    .map((p) => Number(p.trim()));
const secret = 'demo-secret';
const hub = memoryClusterHub();
const storage = memoryStorage(); // shared = the cluster's database

const log = (...args) => console.log(...args);
const step = (title) => log(`\n=== ${title} ===`);

const members = [];
for (const port of PORTS) {
    const plugin = cluster({
        providers: hub.providers(),
        advertise: `http://127.0.0.1:${port}`,
        secret
    });
    const app = defineActorApp({ actors: [CrunchJob], storage }).use(plugin);
    const server = createServer(createAppHandler(app));
    await new Promise((r) => server.listen(port, r));
    const silo = await app.start();
    members.push({ app, silo, placement: plugin.placement, server, port });
    log(`silo ${plugin.placement.identity.siloId} listening on :${port}`);
}

step('1. Start the job through silo 0 — the call returns immediately');
const runId = `run-${Date.now().toString(36)}`;
const started = await members[0].silo.actor(CrunchJob, runId).start({ steps: 12, stepMs: 250 });
log(`started ${runId}: status=${started.status} attempts=${started.attempts}`);

const entry = await hub.directory.lookup(`Crunch${SEP}${runId}`);
const owner = members.find((m) => m.placement.identity.siloId === entry.siloId);
const watcherSilo = members.find((m) => m !== owner);
log(`the job landed on ${entry.siloId} (:${owner.port})`);

step(`2. Watch live progress from a NON-owner (${watcherSilo.placement.identity.siloId})`);
let killed = false;
const timeline = [];
const watching = (async () => {
    // One watch stream, opened before the crash. It ends when the owner
    // dies; the loop below reopens it through the survivor — the actor
    // KEY is the stable address, not the silo behind it.
    for (;;) {
        try {
            for await (const info of watcherSilo.silo.actor(CrunchJob, runId).watch()) {
                timeline.push(info);
                log(
                    `  watch: status=${info.status} progress=${info.progress?.done ?? 0}/` +
                        `${info.progress?.total ?? '?'} attempts=${info.attempts}`
                );
                if (info.status === 'completed' || info.status === 'failed') return;
            }
        } catch {
            if (!killed) throw new Error('watch dropped before the crash?');
            log('  (watch dropped by the crash — resubscribing via a survivor)');
        }
        await new Promise((r) => setTimeout(r, 100));
    }
})();

// Let it make some progress — past at least one durable checkpoint.
await new Promise((r) => setTimeout(r, 1200));

step(`3. Kill the owner ${entry.siloId} MID-RUN`);
killed = true;
// An ABRUPT crash, not a graceful stop: close the socket and drop the
// membership entry. Deliberately NO silo.stop() — a drain would give the
// run a wind-down grace, which is not what a crash looks like. The
// orphaned in-process task becomes a zombie writer, and the etag CAS on
// state saves is what fences it off once the survivor re-claims.
owner.server.close();
hub.kill(owner.placement.identity.siloId);
log('owner is gone; the run was interrupted between checkpoints');

step('4. A survivor call re-places the grain — the task ledger resumes the run');
const survivor = members.find((m) => m !== owner);
await new Promise((r) => setTimeout(r, 100));
const resumed = await survivor.silo.actor(CrunchJob, runId).status();
log(
    `status via ${survivor.placement.identity.siloId}: status=${resumed.status} ` +
        `attempts=${resumed.attempts} (crash-resume counted)`
);

step('5. The job completes on the survivor');
await watching;
const result = await survivor.silo.actor(CrunchJob, runId).result();
const expected = (12 * 11) / 2; // sum 0..11
log(`result:`, result);
if (result.sum !== expected) throw new Error(`state lost in failover! ${result.sum} != ${expected}`);
if (result.attempts < 2) throw new Error('the crash was never observed?');
const finalInfo = await survivor.silo.actor(CrunchJob, runId).status();
log(
    `final: status=${finalInfo.status} attempts=${finalInfo.attempts} — ` +
        `resumed from the last checkpoint, no work lost past it`
);

log('\nDone. The job outlived the silo that ran it.');
for (const m of members.filter((m) => m !== owner)) {
    await m.app.stop();
    m.server.close();
}
