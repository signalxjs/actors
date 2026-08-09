/**
 * A single-host entry for the socket workload (#172) — no cluster, no Redis,
 * no Azure.
 *
 * `server.mjs` is the deployed host and REQUIRES `REDIS_URL`,
 * `CLUSTER_SECRET` and `OPS_SECRET`, because a cluster member with optional
 * membership is a different thing being tested. That makes it useless for
 * the half of this work that can be checked on a laptop: does the upgrade
 * happen, does a subscription deliver, and do `ops.sockets.open` and the
 * load generator's `connected` agree.
 *
 * This entry answers exactly that and nothing else. It is the local rung of
 * the verification ladder in #172 — the one that catches an fd limit, a
 * wrong URL or a broken frame before any of it costs a paid session.
 *
 *   PORT           listen port                default 7311
 *   OPS_SECRET     /_sigx/ops bearer token    default dev-ops-secret
 *   plus every ENABLE_SOCKET / SOCKET_* / ENABLE_SESSIONS knob `server.mjs`
 *   documents — with ENABLE_SOCKET defaulting to ON here, since mounting
 *   the socket is the entire point.
 *
 * Run:  node perf/aks/ws-dev.mjs
 * Then: TARGET_URL=http://127.0.0.1:7311 MODE=hot CONNECTIONS=200 \
 *         node perf/aks/ws-loadgen.mjs
 */
import { createServer } from 'node:http';
import { health, metrics, ops } from '@sigx/actors/host';
import { socketStats } from '@sigx/actors/server';
import { createAppHandler, attachSignalHandlers } from '@sigx/actors/node';
import { attachActorSocket } from '@sigx/actors-ws/node';
import './src/server-app.ts';
import { app } from './src/actors.app.ts';
import { Counter } from './src/counter.actor.ts';
import { Crunch } from './src/crunch.actor.ts';
import { Fanout } from './src/fanout.actor.ts';

const PORT = Number(process.env.PORT ?? 7311);
const OPS_SECRET = process.env.OPS_SECRET ?? 'dev-ops-secret';
const SOCKET_ORIGIN = process.env.SOCKET_ORIGIN === 'same-origin' ? 'same-origin' : false;
// Validated exactly as `server.mjs` validates it: an unset knob means "the
// runtime's default", but a MISTYPED one must fail fast rather than reach
// `attachActorSocket` as NaN — a cap silently disabled is the kind of thing
// that gets discovered as a wrong number in a baseline.
const socketNum = (name) => {
    const raw = process.env[name];
    if (raw === undefined || raw === '') return undefined;
    const value = Number(raw);
    if (!Number.isInteger(value) || value < 0) {
        console.error(`[ws-dev] ${name} must be a non-negative integer, got '${raw}'`);
        process.exit(1);
    }
    return value;
};

const sockets = socketStats();

const composed = app
    .withActors([Counter, Crunch, Fanout])
    .use(metrics())
    .use(health())
    .use(ops({ secret: OPS_SECRET }))
    .use({
        name: 'socket-stats',
        setup: (registry) => {
            registry.reportOps('sockets', () => sockets.snapshot());
            registry.reportDigest('sockets', () => sockets.digest());
        }
    });

const handler = createAppHandler(composed, { origin: false });
const server = createServer(handler);
await new Promise((resolve) => server.listen(PORT, resolve));
const host = await composed.start();

attachActorSocket(server, {
    host,
    stats: sockets,
    origin: SOCKET_ORIGIN,
    maxConcurrent: socketNum('SOCKET_MAX_CONCURRENT'),
    maxSubscriptions: socketNum('SOCKET_MAX_SUBSCRIPTIONS'),
    pingMs: socketNum('SOCKET_PING_MS'),
    revalidateMs: socketNum('SOCKET_REVALIDATE_MS'),
    maxConnectionMs: socketNum('SOCKET_MAX_CONNECTION_MS')
});

attachSignalHandlers(host, { server, timeoutMs: 10_000 });

console.log(
    `[ws-dev] host on :${PORT} — socket at ws://127.0.0.1:${PORT}/_sigx/socket, ` +
        `ops at http://127.0.0.1:${PORT}/_sigx/ops (bearer ${OPS_SECRET})`
);
