/**
 * The host entry — one pod, one host. Everything deployment-specific
 * arrives as environment (nothing hardcoded, the chart owns the values):
 *
 *   PORT                listen port                        default 7311
 *   POD_IP              this pod's IP (downward API)       default 127.0.0.1
 *   REDIS_URL           redis://host:6379                  REQUIRED
 *   CLUSTER_SECRET      host-to-host HMAC secret           REQUIRED
 *   OPS_SECRET          /_sigx/ops bearer token            REQUIRED
 *   SIGX_NAMESPACE      Redis key namespace                default sigx
 *   FETCH_CONNECTIONS   undici pool size per peer origin   default 64
 *   MEMBERSHIP          redis | k8s                        default redis
 *   POD_NAMESPACE       k8s namespace (MEMBERSHIP=k8s)     downward API
 *
 * The WebSocket endpoint (#172), all optional and all OFF by default — an
 * existing deployment setting none of these behaves exactly as before:
 *
 *   ENABLE_SOCKET       mount /_sigx/socket                default off
 *   SOCKET_PATH         upgrade path (matched EXACTLY)     default /_sigx/socket
 *   SOCKET_ORIGIN       same-origin | (anything else=off)  default off
 *   SOCKET_MAX_CONCURRENT     in-flight calls per conn     runtime default 256
 *   SOCKET_MAX_SUBSCRIPTIONS  live subs per conn           runtime default 256
 *   SOCKET_MAX_MESSAGE_BYTES  per-message cap              runtime default 1 MiB
 *   SOCKET_PING_MS      keepalive after send-silence       runtime default 30000
 *   SOCKET_REVALIDATE_MS      re-auth cadence              runtime default 0 (off)
 *   SOCKET_MAX_CONNECTION_MS  hard lifetime cap            runtime default 0 (off)
 *   ENABLE_SESSIONS     verify signed session cookies      default off
 *   SESSION_SECRET      HMAC secret (required if enabled in prod)
 *
 * MEMBERSHIP=k8s swaps host liveness onto coordination.k8s.io Leases
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
import { health, metrics, ops } from '@sigx/actors/host';
import { socketStats } from '@sigx/actors/server';
import { createAppHandler, attachSignalHandlers } from '@sigx/actors/node';
import { cluster, clusterStats } from '@sigx/actors/cluster';
import { redisCluster, redisDirectory } from '@sigx/actors-redis';
import { k8sMembership } from '@sigx/actors-k8s';
import { Redis } from 'ioredis';
// Side-effect import: `createServerApp` registers the app's `authenticate`
// and `codec` on evaluation, and the actor pipeline resolves it lazily on
// first use. It must therefore be imported BEFORE the first request, which
// module scope guarantees. With ENABLE_SESSIONS unset it authenticates
// nobody, so this changes nothing for the existing scenarios.
import './src/server-app.ts';
import { app } from './src/actors.app.ts';
import { Counter } from './src/counter.actor.ts';
import { Crunch } from './src/crunch.actor.ts';
import { Fanout } from './src/fanout.actor.ts';
import { SweepJob } from './src/sweep.job.ts';

const need = (name) => {
    const value = process.env[name];
    if (!value) {
        console.error(`[perf-aks] missing required env ${name}`);
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

// The client-facing WebSocket endpoint (#172). Off by default: an existing
// deployment that sets none of this keeps the exact behaviour its HTTP
// baselines were recorded against.
const ENABLE_SOCKET = process.env.ENABLE_SOCKET === '1';
const SOCKET_PATH = process.env.SOCKET_PATH ?? '/_sigx/socket';
// The load generators are Node clients and send no Origin header, so the
// default 'same-origin' posture would refuse every upgrade — the same
// reason the HTTP handler below passes `origin: false`. Set
// SOCKET_ORIGIN=same-origin to exercise the real browser posture (what the
// cross-origin-upgrade-is-refused assertion runs against).
const SOCKET_ORIGIN = process.env.SOCKET_ORIGIN === 'same-origin' ? 'same-origin' : false;
/** A socket knob: absent means "the runtime's default", never 0. */
const socketNum = (name) => {
    const raw = process.env[name];
    if (raw === undefined || raw === '') return undefined;
    const value = Number(raw);
    if (!Number.isInteger(value) || value < 0) {
        console.error(`[perf-aks] ${name} must be a non-negative integer, got '${raw}'`);
        process.exit(1);
    }
    return value;
};

// One bounded pool for host-to-host calls. Node's global fetch is
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
    console.error(`[perf-aks] MEMBERSHIP must be redis or k8s, got '${MEMBERSHIP}'`);
    process.exit(1);
})();

const plugin = cluster({
    providers,
    advertise: `http://${POD_IP}:${PORT}`,
    secret: CLUSTER_SECRET,
    fetch: (url, init) => undiciFetch(url, { ...init, dispatcher: agent })
});

// One recorder shared by every session on this host. The session records
// at the event sites it owns; publishing is the app's job — there is
// deliberately no counter-recording API on the runtime.
const sockets = ENABLE_SOCKET ? socketStats() : null;

const composed = app
    .withActors([Counter, Crunch, Fanout, SweepJob])
    .use(plugin)
    .use(metrics())
    .use(health())
    .use(
        ops({
            secret: OPS_SECRET,
            // The second argument is what `?detail=1` on the ops route asked
                // for. Spreading it is what lets a dashboard drill into ONE
                // host without every poll paying for a fleet-wide actor walk.
                cluster: (signal, query) => clusterStats(plugin.placement, { signal, ...query })
        })
    );

// The socket counters ride the EXISTING ops endpoint as one more section —
// no endpoint change, and the bearer posture is already there. `.use()`
// returns the same app, so registering conditionally after the chain is
// equivalent to being inside it; it only has to happen before `start()`.
if (sockets) {
    composed.use({
        name: 'socket-stats',
        setup: (registry) => {
            // Both seams, because they answer different questions: the ops
            // section is read by a human on ONE host, the digest is what
            // `clusterStats()` merges across the fleet (percentiles have to
            // be re-derived from raw buckets, never averaged).
            registry.reportOps('sockets', () => sockets.snapshot());
            registry.reportDigest('sockets', () => sockets.digest());
        }
    });
}

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
const host = await composed.start();

// AFTER start: the session needs a running host, and an upgrade arriving
// before this point has no listener and is destroyed by Node — correct,
// since the host could not have served it anyway. `attachActorSocket`
// imports `ws` lazily, on the first matching upgrade only.
if (sockets) {
    const { attachActorSocket } = await import('@sigx/actors-ws/node');
    attachActorSocket(server, {
        host,
        stats: sockets,
        path: SOCKET_PATH,
        origin: SOCKET_ORIGIN,
        // Left undefined = the runtime's own defaults (256 in-flight calls,
        // 256 subscriptions, 30 s keepalive, no revalidation, no lifetime
        // cap). Every one of them is a scale variable, so every one is a
        // knob — and each has to reach INFRA_SHAPE, or two runs under
        // different caps would compare as if they were the same shape.
        maxConcurrent: socketNum('SOCKET_MAX_CONCURRENT'),
        maxSubscriptions: socketNum('SOCKET_MAX_SUBSCRIPTIONS'),
        maxMessageBytes: socketNum('SOCKET_MAX_MESSAGE_BYTES'),
        pingMs: socketNum('SOCKET_PING_MS'),
        revalidateMs: socketNum('SOCKET_REVALIDATE_MS'),
        maxConnectionMs: socketNum('SOCKET_MAX_CONNECTION_MS')
    });
}

// Shutdown drains the HTTP edge, not just the actors (#142/#150/#157). The
// preStop sleep + ready-503 dance only steers NEW connections away —
// established keep-alive flows survive endpoint removal (conntrack) and
// ride into the exiting pod, where they get RST at process exit.
//
// `attachSignalHandlers` owns the sequence now, so this deployment and the
// documented recipe cannot drift apart: onStopBegin (every response gets
// `connection: close`, so pools retire sockets one response at a time) →
// host.stop() → close() + closeAllConnections() at the very end.
attachSignalHandlers(host, {
    server,
    timeoutMs: 30_000,
    onStopBegin: () => {
        stopping = true;
    },
    // The drain already exits non-zero on failure; this is the log line,
    // since a terminated pod leaves little else behind.
    onError: (error) => console.error('[perf-aks] drain failed:', error)
});

console.log(
    `[perf-aks] host ${plugin.placement.identity.hostId} on :${PORT} ` +
        `advertise=http://${POD_IP}:${PORT} membership=${MEMBERSHIP} ` +
        `NODE_ENV=${process.env.NODE_ENV ?? '(unset)'}`
);
