/**
 * A single-host entry for the workflow workload (#297) — no cluster, no
 * Redis, no Azure. The local rung of the verification ladder: does a run
 * complete, does the aggregator drain, does the load generator's JSON line
 * come out with `stuck` all zero — checked on a laptop before any of it
 * costs a paid session. `server.mjs` REQUIRES the cluster env because a
 * member with optional membership is a different thing being tested;
 * `ws-dev.mjs` is the same idea for the socket axis.
 *
 *   PORT           listen port                default 7311
 *   OPS_SECRET     /_sigx/ops bearer token    default dev-ops-secret
 *   EXIT_AFTER_S   stop the host after N s    default 0 (run until killed)
 *   plus every WF_* host knob `src/workflow/config.ts` documents.
 *
 * Run:  node perf/aks/wf-dev.mjs
 * Then: TARGET_URL=http://127.0.0.1:7311 MODE=workflow WF_START_RATE=20 \
 *         DURATION_S=20 WF_DELAY_MS=500 node perf/aks/loadgen.mjs
 *
 * Under `--conditions=production` the prod dist is used and `ops()` enforces
 * its bearer; without it, the dev build. Either works here.
 */
import { createServer } from 'node:http';
import { health, metrics, ops } from '@sigx/actors/host';
import { createAppHandler, attachSignalHandlers } from '@sigx/actors/node';
import './src/server-app.ts';
import { app } from './src/actors.app.ts';
import { Counter } from './src/counter.actor.ts';
import { Crunch } from './src/crunch.actor.ts';
import { Fanout } from './src/fanout.actor.ts';
import { SweepJob } from './src/sweep.job.ts';
import { workflowActors, snapshotCounters } from './src/workflow/index.ts';

const PORT = Number(process.env.PORT ?? 7311);
const OPS_SECRET = process.env.OPS_SECRET ?? 'dev-ops-secret';
const EXIT_AFTER_S = Number(process.env.EXIT_AFTER_S ?? 0);

const composed = app
    .withActors([Counter, Crunch, Fanout, SweepJob, ...workflowActors])
    .use(metrics())
    .use(health())
    .use(ops({ secret: OPS_SECRET }))
    .use({
        name: 'workflow-counters',
        setup: (registry) => {
            registry.reportOps('workflow', () => snapshotCounters());
        }
    });

const handler = createAppHandler(composed, { origin: false });
const server = createServer(handler);
await new Promise((resolve) => server.listen(PORT, resolve));
const host = await composed.start();
attachSignalHandlers(host, { server, timeoutMs: 10_000 });
console.error(`[wf-dev] host on :${PORT} — single node, memory storage; workflow actors registered`);

if (EXIT_AFTER_S > 0) {
    setTimeout(() => {
        process.kill(process.pid, 'SIGTERM');
    }, EXIT_AFTER_S * 1000).unref();
}
