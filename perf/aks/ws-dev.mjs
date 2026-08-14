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
 *   SAMPLE_MS      host-load sample interval  default 5000 (0 disables)
 *   EXIT_AFTER_S   stop the host after N s    default 0 (run until killed)
 *   plus every ENABLE_SOCKET / SOCKET_* / ENABLE_SESSIONS knob `server.mjs`
 *   documents — with ENABLE_SOCKET defaulting to ON here, since mounting
 *   the socket is the entire point.
 *
 * Run:  node perf/aks/ws-dev.mjs
 * Then: TARGET_URL=http://127.0.0.1:7311 MODE=hot CONNECTIONS=200 \
 *         node perf/aks/ws-loadgen.mjs
 *
 * ## Why it samples its own load (#245)
 *
 * A CPU profile says where the time went; it does NOT say whether there was
 * any time to spend. `deliveries/s ÷ hosts` is a THROUGHPUT RECIPROCAL, and it
 * bounds per-delivery CPU only if the host was actually saturated — so a
 * profile read without a utilisation figure beside it cannot tell "saturated,
 * and here is where the CPU goes" from "idle, blocked on something else", and
 * the second reading makes every share in the profile meaningless as an
 * attribution. `cpu` and `loopDelay` below exist so that question is answered
 * by the run rather than assumed by the reader.
 */
import { monitorEventLoopDelay } from 'node:perf_hooks';
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
const SAMPLE_MS = Number(process.env.SAMPLE_MS ?? 5000);
const EXIT_AFTER_S = Number(process.env.EXIT_AFTER_S ?? 0);
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

// One JSON line per window, so a run can be pasted into a table or piped
// through a filter. Everything here is a DELTA over the window, never a
// process total: a run that starts mid-load must not inherit the dial phase's
// CPU, which is exactly the mistake the `settleGc()` per-arm rule exists to
// prevent in the bench harness.
if (SAMPLE_MS > 0) {
    // `resolution: 20` keeps the histogram's own sampling cost negligible
    // while still resolving the ~50 ms watch-throttle window this workload
    // runs at. The handle is unref'd via `timer.unref()` below, not here —
    // `monitorEventLoopDelay` does not hold the loop open on its own.
    const loop = monitorEventLoopDelay({ resolution: 20 });
    loop.enable();
    let lastCpu = process.cpuUsage();
    let lastAt = performance.now();

    const timer = setInterval(() => {
        // Subtract explicitly rather than using `process.cpuUsage(lastCpu)`
        // and re-reading: two reads leave the CPU spent between them
        // unaccounted for, every window, forever.
        const cpuNow = process.cpuUsage();
        const cpu = { user: cpuNow.user - lastCpu.user, system: cpuNow.system - lastCpu.system };
        const now = performance.now();
        const wallUs = (now - lastAt) * 1000;
        lastCpu = cpuNow;
        lastAt = now;
        const snap = sockets.snapshot();
        console.log(
            JSON.stringify({
                t: '[ws-dev] load',
                // Fraction of ONE core. Above 1 means several cores are busy;
                // the AKS shape this rig stands in for is `limits.cpu: 1000m`,
                // so ~1.0 is the saturation line to read against.
                cpu: round((cpu.user + cpu.system) / wallUs),
                cpuUser: round(cpu.user / wallUs),
                cpuSystem: round(cpu.system / wallUs),
                // Milliseconds. A loop that is merely BUSY shows up here as
                // well as in `cpu`; a loop that is blocked shows up here
                // alone, which is the case a profile cannot see.
                loopMeanMs: round(loop.mean / 1e6),
                loopP50Ms: round(loop.percentile(50) / 1e6),
                loopP99Ms: round(loop.percentile(99) / 1e6),
                loopMaxMs: round(loop.max / 1e6),
                rssMb: Math.round(process.memoryUsage().rss / 1048576),
                open: snap.open,
                subscriptions: snap.subscriptions
            })
        );
        loop.reset();
    }, SAMPLE_MS);
    timer.unref();
}

function round(n) {
    return Number.isFinite(n) ? Math.round(n * 1000) / 1000 : null;
}

// `--cpu-prof` writes its `.cpuprofile` when the process EXITS, and there is
// no portable way to ask a backgrounded host to exit: Windows cannot deliver
// SIGINT to another console process at all (`process.kill(pid, 'SIGINT')` is
// emulated as a hard terminate), so a profiling run there would kill the host
// and lose the profile every time. A self-imposed deadline is the one exit
// path every platform shares. `process.exit(0)` still flushes the profile —
// verified, not assumed.
if (EXIT_AFTER_S > 0) {
    setTimeout(() => {
        console.log(`[ws-dev] EXIT_AFTER_S=${EXIT_AFTER_S} reached — exiting`);
        process.exit(0);
    }, EXIT_AFTER_S * 1000).unref();
}

console.log(
    `[ws-dev] host on :${PORT} — socket at ws://127.0.0.1:${PORT}/_sigx/socket, ` +
        `ops at http://127.0.0.1:${PORT}/_sigx/ops (bearer ${OPS_SECRET})` +
        (SAMPLE_MS > 0 ? `, load sampled every ${SAMPLE_MS}ms` : '')
);
