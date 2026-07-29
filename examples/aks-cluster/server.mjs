/**
 * The silo entry — one pod, one silo. Everything deployment-specific
 * arrives as environment (nothing hardcoded, the chart owns the values):
 *
 *   PORT                listen port                        default 7311
 *   POD_IP              this pod's IP (downward API)       default 127.0.0.1
 *   REDIS_URL           redis://host:6379                  REQUIRED
 *   CLUSTER_SECRET      silo-to-silo HMAC secret           REQUIRED
 *   OPS_SECRET          /_sigx/ops bearer token            REQUIRED
 *   SIGX_NAMESPACE      Redis key namespace                default sigx
 *   FETCH_CONNECTIONS   undici pool size per peer origin   default 64
 *   MEMBERSHIP          redis | k8s                        default redis
 *   POD_NAMESPACE       k8s namespace (MEMBERSHIP=k8s)     downward API
 *
 * MEMBERSHIP=k8s swaps silo liveness onto coordination.k8s.io Leases
 * (@sigx/actors-k8s) while the actor directory stays in Redis — the two
 * seams are independent by design, and this toggle is how the same chart
 * validates both providers.
 *
 * Needs Node >= 22.18 (type stripping for the .ts imports). Run with
 * --conditions=production for the prod dist — ops() only enforces its
 * bearer secret there, which scenario (a) of the runbook checks from the
 * outside.
 */
import { createServer } from 'node:http';
import { Agent, fetch as undiciFetch } from 'undici';
import { health, metrics, ops } from '@sigx/actors/silo';
import { createAppHandler } from '@sigx/actors/node';
import { cluster, clusterStats } from '@sigx/actors/cluster';
import { redisCluster, redisDirectory } from '@sigx/actors-redis';
import { k8sMembership } from '@sigx/actors-k8s';
import { Redis } from 'ioredis';
import { app } from './src/actors.app.ts';
import { Counter } from './src/counter.actor.ts';
import { Crunch } from './src/crunch.actor.ts';

const need = (name) => {
    const value = process.env[name];
    if (!value) {
        console.error(`[aks-cluster] missing required env ${name}`);
        process.exit(1);
    }
    return value;
};

const PORT = Number(process.env.PORT ?? 7311);
const POD_IP = process.env.POD_IP ?? '127.0.0.1';
const REDIS_URL = need('REDIS_URL');
const CLUSTER_SECRET = need('CLUSTER_SECRET');
const OPS_SECRET = need('OPS_SECRET');
const SIGX_NAMESPACE = process.env.SIGX_NAMESPACE ?? 'sigx';
const MEMBERSHIP = process.env.MEMBERSHIP ?? 'redis';

// One bounded pool for silo-to-silo calls. Node's global fetch is
// unbounded (~2 sockets per in-flight request per peer); this caps it at
// the per-peer concurrency we actually want.
const agent = new Agent({ connections: Number(process.env.FETCH_CONNECTIONS ?? 64) });

const providers = (() => {
    if (MEMBERSHIP === 'redis') {
        return redisCluster({ url: REDIS_URL, namespace: SIGX_NAMESPACE });
    }
    if (MEMBERSHIP === 'k8s') {
        // Liveness via Leases, directory stays in Redis — independent seams.
        return {
            membership: k8sMembership({ namespace: process.env.POD_NAMESPACE }),
            directory: redisDirectory(new Redis(REDIS_URL), { namespace: SIGX_NAMESPACE })
        };
    }
    console.error(`[aks-cluster] MEMBERSHIP must be redis or k8s, got '${MEMBERSHIP}'`);
    process.exit(1);
})();

const plugin = cluster({
    providers,
    advertise: `http://${POD_IP}:${PORT}`,
    secret: CLUSTER_SECRET,
    fetch: (url, init) => undiciFetch(url, { ...init, dispatcher: agent })
});

const composed = app
    .withActors([Counter, Crunch])
    .use(plugin)
    .use(metrics())
    .use(health())
    .use(
        ops({
            secret: OPS_SECRET,
            cluster: (signal) => clusterStats(plugin.placement, { signal })
        })
    );

// origin: false — the load generator and the CLI are Node clients that
// send no Origin header; the default 'same-origin' policy would 403 them.
// No browser reaches this service (ClusterIP only), so the check buys
// nothing here.
const handler = createAppHandler(composed, { origin: false });
// The `stopping` wrap is the drain mechanism for kept-alive client
// connections: once shutdown begins, every response carries an explicit
// `connection: close`, so pools (undici et al) retire the socket after the
// in-flight response and re-dial through the Service to a live pod — no
// resets. Header is set BEFORE the app handler writes anything.
let stopping = false;
const server = createServer((req, res) => {
    if (stopping) res.setHeader('connection', 'close');
    handler(req, res);
});

// Listen BEFORE starting — app.start() joins membership, and from that
// moment peers may place actors here and call them.
await new Promise((resolve) => server.listen(PORT, resolve));
const silo = await composed.start();

// Shutdown drains the HTTP edge, not just the actors (#142). The preStop
// sleep + ready-503 dance only steers NEW connections away — established
// keep-alive flows survive endpoint removal (conntrack) and ride into the
// exiting pod, where they get RST at process exit. So on SIGTERM:
//  1. stopping = true           — every response now carries
//                                 `connection: close` (the wrap above), so
//                                 client pools drain themselves one
//                                 response at a time, gracefully. NO
//                                 server.close() yet: on Node >= 19 close()
//                                 destroys idle sockets immediately, which
//                                 races the client writing its next request
//                                 onto one — a reset, the very thing being
//                                 fixed.
//  2. silo.stop()               — the actor drain (announces 'leaving',
//                                 hands activations off; peers with pooled
//                                 connections keep flowing throughout).
//  3. close() + closeAll...()   — at the very end. Anything still open is
//                                 either a socket that stayed idle through
//                                 the whole drain (an idle-close is not an
//                                 error to a client pool) or a genuine
//                                 straggler.
const shutdown = async () => {
    if (stopping) return;
    stopping = true;
    let code = 0;
    try {
        await silo.stop({ timeoutMs: 30_000 });
    } catch (error) {
        // A failed drain must not exit 0 and vanish — the exit code is the
        // only diagnostic a terminated pod leaves behind.
        console.error('[aks-cluster] drain failed:', error);
        code = 1;
    }
    server.close();
    server.closeAllConnections();
    process.exit(code);
};
process.once('SIGTERM', () => void shutdown());
process.once('SIGINT', () => void shutdown());

console.log(
    `[aks-cluster] silo ${plugin.placement.identity.siloId} on :${PORT} ` +
        `advertise=http://${POD_IP}:${PORT} membership=${MEMBERSHIP} ` +
        `NODE_ENV=${process.env.NODE_ENV ?? '(unset)'}`
);
