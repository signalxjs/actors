/**
 * `wf-fleet.mjs` — N hosts on ONE machine, over one local Redis (#381).
 *
 * The first multi-core rig. Every recorded workflow figure is three 1 vCPU
 * pods on three nodes; nothing had ever measured N hosts sharing a box.
 * `server.mjs` already supports it (same `REDIS_URL`, a different `PORT`
 * each, `POD_IP` defaulting to 127.0.0.1 — see the README), so this file
 * is an ORCHESTRATOR, not a new host: it forks N copies of the real host
 * entry, points the real load generator at them, and reads the same
 * `ops.workflow` counters `deploy/wf-load.mjs` sums across pods.
 *
 * ## The Service, stood in for by a round-robin proxy
 *
 * On Kubernetes the generator talks to a Service that spreads its
 * connections over the pods. Here one generator process talks to a tiny
 * in-process HTTP proxy that forwards each request to the next host in
 * turn. ONE generator, not one per host, for a reason that is a harness
 * fact rather than a choice: every generator resets the `WorkflowStats`
 * ring when it seeds (`src/loadgen/workflow-mode.ts`), so N generators
 * starting together wipe each other's completions and the run reports
 * `droppedEvents` and `completedUnreported` that the engine never caused.
 * #380 makes the reset index-0-only; until then a single generator is
 * the honest shape, and its own CPU is sampled and reported so a rung it
 * bottlenecks says so.
 *
 * ## What is evidence here
 *
 * The Tier-2 rule (`benchmarks/BASELINES.md`, tier legend): N processes
 * share the cores, so absolute throughput is context, not evidence. Counts
 * — `stuck`, `wakesLost`, reminder CAS failures, `completedUnreported`,
 * `remoteDispatches` — are evidence, and the RATIO of two arms measured
 * back to back on one box is the decision number: completed runs/s at
 * hosts=8 over hosts=1 is what says whether one host per core scales.
 *
 * ## Per-process CPU
 *
 * Sampled with `ps -o %cpu` every `sampleIntervalMs`: on macOS a decaying
 * average over the last seconds, on Linux the process-lifetime average —
 * good enough to say "this host sat at a core" or "the generator was at
 * 30%", not a profile. Absent (null) where `ps` is unavailable.
 *
 * CLI:  node perf/aks/wf-fleet.mjs hosts=4 rate=100 [sweep=50,100] \
 *         [durationS=30] [WF_TASK_MS=2 …other WF_* knobs] [--json]
 * Env:  REDIS_URL (default redis://127.0.0.1:6379), WF_FLEET_BASE_PORT
 *       (default 7411), FETCH_CONNECTIONS, TRANSPORT — passed to the hosts.
 *
 * Run with `--conditions=production` (the `bench:wf-local` script does) so
 * the hosts and the generator measure the built prod dist; the children
 * inherit the flag through `execArgv`.
 */
import { spawn } from 'node:child_process';
import { createServer, request as httpRequest, Agent as HttpAgent } from 'node:http';
import { connect as tcpConnect } from 'node:net';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import { resolve as resolvePath } from 'node:path';
import { Redis } from 'ioredis';
import { mergeWfRows } from './deploy/wf-load.mjs';

const execFileP = promisify(execFile);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const HERE = fileURLToPath(new URL('.', import.meta.url));
const SERVER = new URL('./server.mjs', import.meta.url);
const LOADGEN = new URL('./loadgen.mjs', import.meta.url);

/** The cluster counters `wf-load.mjs` carries — the same allowlist, so a
 *  local row and a Tier-3 row name the same things. */
const CLUSTER_COUNTERS = [
    'remoteDispatches',
    'routedLocal',
    'dispatchesLocal',
    'dispatchesRemote',
    'directoryLookups',
    'directoryClaims',
    'claimConflicts',
    'inboundDispatches',
    'retries',
    'unreachableRetries'
];

/** How long a host may take to answer `/ready` and to see the whole fleet. */
const READY_TIMEOUT_MS = 60_000;
/** A quiet fleet: queued turns summed over hosts at or under this. */
const QUIET_QUEUED = 50;
const QUIET_TIMEOUT_MS = 300_000;
/** `attachSignalHandlers` drains for up to 30 s; give it that and a little. */
const STOP_TIMEOUT_MS = 35_000;

/**
 * Parse `key=value` CLI words into options; `WF_*`, `FETCH_CONNECTIONS`
 * and `TRANSPORT` go to `env`, the rest are rig options. Pure, so it is
 * tested.
 */
export function parseFleetArgs(words) {
    const options = { env: {}, json: false };
    for (const word of words) {
        if (word === '--json') {
            options.json = true;
            continue;
        }
        const eq = word.indexOf('=');
        if (eq < 0) throw new Error(`[wf-fleet] expected key=value, got '${word}'`);
        const key = word.slice(0, eq);
        const value = word.slice(eq + 1);
        if (/^WF_/.test(key) || key === 'FETCH_CONNECTIONS' || key === 'TRANSPORT') {
            options.env[key] = value;
        } else if (key === 'hosts' || key === 'rate' || key === 'durationS' || key === 'basePort') {
            const n = Number(value);
            if (!Number.isFinite(n) || n <= 0) throw new Error(`[wf-fleet] ${key} must be a positive number, got '${value}'`);
            options[key] = n;
        } else if (key === 'sweep') {
            options.sweep = value
                .split(',')
                .map((s) => Number(s.trim()))
                .filter((n) => Number.isFinite(n) && n > 0);
            if (options.sweep.length === 0) throw new Error(`[wf-fleet] sweep must list positive rates, got '${value}'`);
        } else if (key === 'redisUrl') {
            options.redisUrl = value;
        } else {
            throw new Error(`[wf-fleet] unknown option '${key}'`);
        }
    }
    return options;
}

/** The tcp listeners sit `TCP_PORT_OFFSET` above the http ones. */
const TCP_PORT_OFFSET = 100;

/**
 * The http ports are `basePort … basePort+hosts-1` and the tcp ones sit
 * `TCP_PORT_OFFSET` above them, so both ranges must be valid ports and
 * must not overlap. Returns the integer base port or throws — a `NaN` from
 * an env typo would otherwise bind port 0 on every host and pass every
 * readiness check against the same one.
 */
export function validateBasePort(raw, hosts) {
    const base = Number(raw);
    if (!Number.isInteger(base) || base < 1 || base > 65535) {
        throw new Error(`[wf-fleet] base port must be an integer in 1..65535, got '${raw}'`);
    }
    if (hosts > TCP_PORT_OFFSET) {
        throw new Error(`[wf-fleet] at most ${TCP_PORT_OFFSET} hosts per fleet (the tcp ports sit ${TCP_PORT_OFFSET} above the http ones)`);
    }
    if (base + TCP_PORT_OFFSET + hosts - 1 > 65535) {
        throw new Error(`[wf-fleet] base port ${base} leaves no room for ${hosts} host(s) plus their tcp ports`);
    }
    return base;
}

/** Sum `%cpu` samples: peak over the run and mean over samples. */
export function cpuSummary(samples) {
    const values = samples.filter((v) => typeof v === 'number' && Number.isFinite(v));
    if (values.length === 0) return { peak: null, avg: null, samples: 0 };
    const peak = Math.max(...values);
    const avg = values.reduce((a, b) => a + b, 0) / values.length;
    return { peak: Math.round(peak * 10) / 10, avg: Math.round(avg * 10) / 10, samples: values.length };
}

/** `%cpu` of each pid as a fraction of ONE core (100 = a full core). */
async function psCpu(pids) {
    if (pids.length === 0) return [];
    try {
        const { stdout } = await execFileP('ps', ['-o', 'pid=,%cpu=', '-p', pids.join(',')]);
        const byPid = new Map();
        for (const line of stdout.split('\n')) {
            const m = /^\s*(\d+)\s+([\d.]+)/.exec(line);
            if (m) byPid.set(Number(m[1]), Number(m[2]));
        }
        return pids.map((pid) => byPid.get(pid) ?? null);
    } catch {
        return pids.map(() => null);
    }
}

/** A host that accepts the connection and then says nothing must not hang
 *  the sampler or the quiet check: an overloaded loop is exactly when
 *  `/_sigx/ops` answers late, and the run has to keep observing it. */
const OPS_TIMEOUT_MS = 10_000;

async function getJson(url, headers = {}) {
    const res = await fetch(url, { headers, signal: AbortSignal.timeout(OPS_TIMEOUT_MS) });
    if (!res.ok) {
        await res.text().catch(() => {});
        throw new Error(`${url} → ${res.status}`);
    }
    return await res.json();
}

/**
 * `ops.workflow` plus the allowlisted cluster counters summed over the
 * fleet, read over HTTP from each host — the local twin of
 * `workflowTotals` in `wf-load.mjs`, which shells to `kubectl exec`.
 */
async function fleetTotals(members, opsSecret) {
    const totals = {};
    const queued = {};
    let hosts = 0;
    let clusterHosts = 0;
    let activations = 0;
    let activationsHosts = 0;
    for (const m of members) {
        let body;
        try {
            body = await getJson(`http://127.0.0.1:${m.port}/_sigx/ops`, {
                authorization: `Bearer ${opsSecret}`
            });
        } catch {
            continue;
        }
        const ops = body?.ops;
        const counters = ops?.cluster?.counters;
        if (counters && !ops.cluster.error) {
            clusterHosts++;
            for (const key of CLUSTER_COUNTERS) {
                const value = counters[key];
                if (typeof value === 'number') totals[`cluster/${key}`] = (totals[`cluster/${key}`] ?? 0) + value;
            }
        }
        // The top-level `stats` gauges, with the metrics section's copy as
        // the fallback — the same pair `workflowTotals` in `wf-load.mjs`
        // reads, so a host build that carries only one of them is not
        // read as idle.
        const live = body?.stats?.activations ?? ops?.metrics?.gauges?.activations;
        if (typeof live === 'number') {
            activations += live;
            activationsHosts++;
        }
        const q = body?.stats?.queued ?? ops?.metrics?.gauges?.queued;
        if (typeof q === 'number') queued[`h${m.index}`] = q;
        const section = ops?.workflow;
        if (!section || section.error) continue;
        hosts++;
        for (const [key, value] of Object.entries(section)) {
            if (typeof value === 'number') totals[key] = (totals[key] ?? 0) + value;
        }
    }
    return {
        hosts,
        pods: members.length,
        totals,
        hostsComplete: members.length > 0 && hosts === members.length && clusterHosts === members.length,
        activations: activationsHosts > 0 ? activations : null,
        queued
    };
}

const sumQueued = (snapshot) => Object.values(snapshot.queued).reduce((a, b) => a + b, 0);

/** Prefix a child's output lines so N hosts' logs stay tellable apart. */
function relay(stream, prefix, onLog) {
    let rest = '';
    stream.setEncoding('utf8');
    stream.on('data', (chunk) => {
        rest += chunk;
        const lines = rest.split('\n');
        rest = lines.pop() ?? '';
        for (const line of lines) if (line.trim()) onLog(`${prefix} ${line}`);
    });
    stream.on('end', () => {
        if (rest.trim()) onLog(`${prefix} ${rest}`);
    });
}

/** The `--conditions=…` flag this process was launched with, if any. */
const conditionsFlag = () => process.execArgv.filter((a) => a.startsWith('--conditions='));

/** True when something accepts a TCP connection on the port. */
function portTaken(port) {
    return new Promise((resolve) => {
        const socket = tcpConnect({ host: '127.0.0.1', port });
        socket.once('connect', () => {
            socket.destroy();
            resolve(true);
        });
        socket.once('error', () => {
            // A refused connect still owns a handle until destroyed; two
            // hundred of them (hosts=100 under tcp) would keep the process
            // alive past the run.
            socket.destroy();
            resolve(false);
        });
    });
}

/**
 * Refuse a port something else already listens on: a stale host from an
 * earlier run would pass the readiness probe in this one's place, and a
 * stale tcp listener only surfaces later as "exited before ready". Both
 * ranges are probed when the transport is tcp.
 */
async function assertPortsFree(basePort, hosts, transport) {
    const ports = [];
    for (let index = 0; index < hosts; index++) {
        ports.push(['http', basePort + index]);
        if (transport === 'tcp') ports.push(['tcp', basePort + TCP_PORT_OFFSET + index]);
    }
    for (const [kind, port] of ports) {
        if (await portTaken(port)) {
            throw new Error(`[wf-fleet] something already listens on :${port} (${kind}; a stale host?) — kill it or set WF_FLEET_BASE_PORT`);
        }
    }
}

/** Spawns into `members` (the caller's array) so a failure part-way still
 *  leaves every child where the caller's cleanup can reach it. */
async function startHosts(members, { hosts, redisUrl, namespace, basePort, env, secrets, onLog }) {
    // The option wins, the environment is the fallback — the header says
    // `TRANSPORT=tcp` in the environment reaches the hosts, so it must —
    // and anything else is refused before a host is spawned.
    const transport = env.TRANSPORT ?? process.env.TRANSPORT ?? 'http';
    if (transport !== 'http' && transport !== 'tcp') {
        throw new Error(`[wf-fleet] TRANSPORT must be http or tcp, got '${transport}'`);
    }
    await assertPortsFree(basePort, hosts, transport);
    for (let index = 0; index < hosts; index++) {
        const port = basePort + index;
        const child = spawn(
            process.execPath,
            [...conditionsFlag(), fileURLToPath(SERVER)],
            {
                cwd: HERE,
                env: {
                    ...process.env,
                    ...env,
                    REDIS_URL: redisUrl,
                    PORT: String(port),
                    POD_IP: '127.0.0.1',
                    NODE_NAME: `local-${index}`,
                    SIGX_NAMESPACE: namespace,
                    CLUSTER_SECRET: secrets.cluster,
                    OPS_SECRET: secrets.ops,
                    TRANSPORT: transport,
                    TCP_PORT: String(basePort + TCP_PORT_OFFSET + index)
                },
                stdio: ['ignore', 'pipe', 'pipe']
            }
        );
        relay(child.stdout, `[h${index}]`, onLog);
        relay(child.stderr, `[h${index}]`, onLog);
        const member = { index, port, child, pid: child.pid, exited: false };
        members.push(member);
        // Both events: a spawn failure (executable missing, permissions)
        // is an `error` with no `exit`, and a host that never marks itself
        // exited would spin the readiness loop to its timeout with nothing
        // to say. The member object, not `members[index]`, so this is
        // right whatever the array holds.
        child.once('exit', (code, signal) => {
            member.exited = true;
            member.exit = { code, signal };
        });
        child.once('error', (error) => {
            member.exited = true;
            member.exit = { error: error.message };
        });
    }
    // Ready one by one, then the whole fleet visible from every host.
    const deadline = Date.now() + READY_TIMEOUT_MS;
    for (const m of members) {
        for (;;) {
            if (m.exited) throw new Error(`[wf-fleet] host ${m.index} exited before ready (${JSON.stringify(m.exit)})`);
            try {
                const res = await fetch(`http://127.0.0.1:${m.port}/_sigx/health/ready`);
                await res.text().catch(() => {});
                if (res.ok) break;
            } catch {
                // not listening yet
            }
            if (Date.now() > deadline) throw new Error(`[wf-fleet] host ${m.index} not ready in ${READY_TIMEOUT_MS} ms`);
            await sleep(200);
        }
    }
    // Converged: every host's own membership view lists the whole fleet as
    // active, read through the fan-out route (`/_sigx/ops/cluster`) so it
    // is the host's VIEW, not Redis's index, that is checked.
    for (;;) {
        const dead = members.find((m) => m.exited);
        if (dead) throw new Error(`[wf-fleet] host ${dead.index} exited (${JSON.stringify(dead.exit)})`);
        let converged = true;
        let seen = null;
        for (const m of members) {
            try {
                const report = await getJson(`http://127.0.0.1:${m.port}/_sigx/ops/cluster`, {
                    authorization: `Bearer ${secrets.ops}`
                });
                seen = report?.view?.active ?? null;
                if (seen !== hosts || report.partial) converged = false;
            } catch {
                converged = false;
            }
            if (!converged) break;
        }
        if (converged) break;
        if (Date.now() > deadline) {
            throw new Error(`[wf-fleet] fleet did not converge on ${hosts} active hosts in ${READY_TIMEOUT_MS} ms (view.active=${seen})`);
        }
        await sleep(250);
    }
}

async function stopHosts(members, onLog) {
    for (const m of members) if (!m.exited) m.child.kill('SIGTERM');
    const deadline = Date.now() + STOP_TIMEOUT_MS;
    while (members.some((m) => !m.exited) && Date.now() < deadline) await sleep(100);
    for (const m of members) {
        if (!m.exited) {
            onLog(`[wf-fleet] host ${m.index} did not drain in ${STOP_TIMEOUT_MS} ms — killed`);
            m.child.kill('SIGKILL');
        }
    }
}

/** The Service stand-in: each request goes to the next host in turn; any
 *  number may be in flight at once, as through a Service. */
function startProxy(members) {
    const agent = new HttpAgent({ keepAlive: true, maxSockets: 256 });
    let next = 0;
    let requests = 0;
    const server = createServer((req, res) => {
        const target = members[next++ % members.length];
        requests++;
        const out = httpRequest(
            { host: '127.0.0.1', port: target.port, method: req.method, path: req.url, headers: req.headers, agent },
            (upstream) => {
                res.writeHead(upstream.statusCode ?? 502, upstream.headers);
                upstream.pipe(res);
            }
        );
        out.on('error', () => {
            if (!res.headersSent) res.writeHead(502);
            res.end();
        });
        req.pipe(out);
    });
    return new Promise((resolve) => {
        server.listen(0, '127.0.0.1', () => {
            const { port } = server.address();
            resolve({
                url: `http://127.0.0.1:${port}`,
                requests: () => requests,
                close: () =>
                    new Promise((done) => {
                        server.closeAllConnections?.();
                        server.close(() => done());
                        agent.destroy();
                    })
            });
        });
    });
}

/** One generator invocation: one rate, `durationS` of arrivals, its rows.
 *  Returns the pid at once (the sampler needs it) and the rows as `done`. */
function startGenerator({ targetUrl, rate, durationS, env, runId, onLog }) {
    const child = spawn(
        process.execPath,
        [...conditionsFlag(), fileURLToPath(LOADGEN)],
        {
            cwd: HERE,
            env: {
                ...process.env,
                ...env,
                TARGET_URL: targetUrl,
                MODE: 'workflow',
                WF_START_RATE: String(rate),
                SWEEP: '',
                DURATION_S: String(durationS),
                RUN_ID: runId
            },
            stdio: ['ignore', 'pipe', 'pipe']
        }
    );
    const rows = [];
    let rest = '';
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', (chunk) => {
        rest += chunk;
        const lines = rest.split('\n');
        rest = lines.pop() ?? '';
        for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed.startsWith('{')) continue;
            try {
                const row = JSON.parse(trimmed);
                if (row.mode === 'workflow') rows.push(row);
            } catch {
                // a progress line that merely begins with a brace
            }
        }
    });
    relay(child.stderr, '[gen]', onLog);
    const done = new Promise((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', (code, signal) => {
            if (code === 0) resolve(rows);
            else reject(new Error(`[wf-fleet] generator exited (code ${code}, signal ${signal})`));
        });
    });
    return { pid: child.pid, done, kill: () => child.kill('SIGKILL') };
}

/**
 * Quiet means EVERY host answered and their queued turns sum to at most
 * `QUIET_QUEUED`. A host that did not answer `/_sigx/ops` — overloaded,
 * mid-restart — is the one most likely to be holding the backlog, and
 * counting it as zero would start the next rung on top of it.
 */
const isQuiet = (snapshot) => snapshot.hostsComplete && sumQueued(snapshot) <= QUIET_QUEUED;

async function waitForQuiet(members, opsSecret, onLog) {
    const deadline = Date.now() + QUIET_TIMEOUT_MS;
    let last = await fleetTotals(members, opsSecret);
    if (isQuiet(last)) return last;
    onLog(
        `[wf-fleet] fleet has ${sumQueued(last)} queued turn(s) on ${last.hosts}/${last.pods} answering host(s) — ` +
            `waiting for it to quiet (<= ${QUIET_QUEUED}, every host answering)`
    );
    for (;;) {
        await sleep(2000);
        last = await fleetTotals(members, opsSecret);
        if (isQuiet(last)) return last;
        if (Date.now() > deadline) {
            throw new Error(
                `[wf-fleet] fleet not quiet after ${QUIET_TIMEOUT_MS / 1000}s: ${sumQueued(last)} queued turn(s), ` +
                    `${last.hosts}/${last.pods} host(s) answering — refusing to start a rung that would ` +
                    'measure the previous one\'s backlog (#302)'
            );
        }
    }
}

async function cleanupNamespace(redisUrl, namespace) {
    const client = new Redis(redisUrl, { lazyConnect: true });
    try {
        await client.connect();
        let cursor = '0';
        do {
            const [next, keys] = await client.scan(cursor, 'MATCH', `${namespace}:*`, 'COUNT', 1000);
            cursor = next;
            if (keys.length > 0) await client.del(...keys);
        } while (cursor !== '0');
    } catch {
        // Best effort: a leftover namespace is inert, every run mints a fresh one.
    } finally {
        client.disconnect();
    }
}

/**
 * Start `hosts` hosts, run one generator rung per rate in `sweep` (or one
 * at `rate`), read the counters before and after each, stop the fleet.
 *
 * Returns `{ hosts, namespace, rungs: [{ rate, row, delta,
 * countersTrustworthy, peakActivations, queuedAfter, hostCpu, generatorCpu,
 * proxyRequests, samples }], wallMs }` — each rung shaped so the workflow
 * scenario's metric mapping applies to it unchanged.
 */
export async function runWfFleet(options) {
    const {
        hosts,
        rate,
        sweep = rate ? [rate] : [],
        durationS = 30,
        env = {},
        redisUrl = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379',
        sampleIntervalMs = 5000,
        onLog = (line) => console.error(line)
    } = options;
    if (!Number.isInteger(hosts) || hosts < 1) throw new Error('[wf-fleet] hosts must be a positive integer');
    if (sweep.length === 0) throw new Error('[wf-fleet] give rate= or sweep=');
    const basePort = validateBasePort(options.basePort ?? process.env.WF_FLEET_BASE_PORT ?? 7411, hosts);

    const started = Date.now();
    const namespace = `wf-fleet-${started}-${Math.floor(Math.random() * 10000)}`;
    const secrets = { cluster: `fleet-${started}`, ops: `ops-${started}` };
    const members = [];
    let proxy = null;
    const generators = [];
    const killAll = () => {
        for (const g of generators) g.kill();
        for (const m of members) if (!m.exited) m.child.kill('SIGKILL');
    };
    const onExit = () => killAll();
    const onSigint = () => {
        killAll();
        process.exit(130);
    };
    process.once('exit', onExit);
    process.once('SIGINT', onSigint);

    try {
        onLog(`[wf-fleet] starting ${hosts} host(s) on :${basePort}… namespace=${namespace}`);
        await startHosts(members, { hosts, redisUrl, namespace, basePort, env, secrets, onLog });
        proxy = await startProxy(members);
        onLog(`[wf-fleet] fleet up; generator → ${proxy.url} (round-robin over ${hosts})`);

        const rungs = [];
        for (const rung of sweep) {
            const before = await waitForQuiet(members, secrets.ops, onLog);
            const proxyBefore = proxy.requests();
            const runId = `fleet-h${hosts}-${started}`;
            let peakActivations = null;
            let samples = 0;
            const hostCpu = [];
            const genCpu = [];
            let genPid = null;
            let polling = true;
            const sampler = (async () => {
                while (polling) {
                    await sleep(sampleIntervalMs);
                    if (!polling) break;
                    const live = await fleetTotals(members, secrets.ops);
                    if (live.hosts > 0) {
                        samples++;
                        if (typeof live.activations === 'number') {
                            peakActivations = Math.max(peakActivations ?? 0, live.activations);
                        }
                    }
                    const pids = members.map((m) => m.pid);
                    const cpu = await psCpu(genPid ? [...pids, genPid] : pids);
                    const hostsCpu = cpu.slice(0, pids.length).filter((v) => v !== null);
                    if (hostsCpu.length > 0) hostCpu.push(Math.max(...hostsCpu));
                    if (genPid && cpu[pids.length] !== null && cpu[pids.length] !== undefined) genCpu.push(cpu[pids.length]);
                }
            })();
            const generator = startGenerator({ targetUrl: proxy.url, rate: rung, durationS, env, runId, onLog });
            genPid = generator.pid;
            generators.push(generator);
            let rows;
            try {
                rows = await generator.done;
            } finally {
                polling = false;
                await sampler;
            }
            const after = await fleetTotals(members, secrets.ops);
            const countersTrustworthy = before.hostsComplete && after.hostsComplete;
            const delta = {};
            if (countersTrustworthy) {
                for (const [key, value] of Object.entries(after.totals)) delta[key] = value - (before.totals[key] ?? 0);
            }
            const [row] = mergeWfRows(rows);
            if (!row) throw new Error(`[wf-fleet] rate ${rung}: the generator printed no row`);
            rungs.push({
                rate: rung,
                row,
                delta,
                countersTrustworthy,
                peakActivations,
                queuedAfter: after.queued,
                hostCpu: cpuSummary(hostCpu),
                generatorCpu: cpuSummary(genCpu),
                proxyRequests: proxy.requests() - proxyBefore,
                samples
            });
            onLog(
                `[wf-fleet] hosts=${hosts} rate=${rung}: completed/s=${row.runsCompletedPerSec} ` +
                    `stuck=${row.stuck.total} errors=${row.errors.total} deferred=${row.startsDeferred} ` +
                    `hostCpuPeak=${cpuSummary(hostCpu).peak} genCpuPeak=${cpuSummary(genCpu).peak}`
            );
        }
        return { hosts, namespace, rungs, wallMs: Date.now() - started };
    } finally {
        if (proxy) await proxy.close();
        await stopHosts(members, onLog);
        process.off('exit', onExit);
        process.off('SIGINT', onSigint);
        await cleanupNamespace(redisUrl, namespace);
    }
}

// ---- CLI --------------------------------------------------------------
// `argv[1]` is resolved by Node for a script path, but compare resolved to
// resolved anyway, and case-insensitively on Windows, where a drive letter
// can arrive in either case: a relative invocation must still run the CLI
// rather than silently import and exit.
const samePath = (a, b) => (process.platform === 'win32' ? a.toLowerCase() === b.toLowerCase() : a === b);
const invokedDirectly =
    process.argv[1] !== undefined && samePath(resolvePath(process.argv[1]), fileURLToPath(import.meta.url));
if (invokedDirectly) {
    const options = parseFleetArgs(process.argv.slice(2));
    if (!options.hosts) {
        console.error('usage: node perf/aks/wf-fleet.mjs hosts=N (rate=R | sweep=R1,R2) [durationS=30] [WF_*=…] [--json]');
        process.exit(2);
    }
    const result = await runWfFleet(options);
    if (options.json) {
        console.log(JSON.stringify(result));
    } else {
        for (const r of result.rungs) {
            console.log(
                JSON.stringify({
                    hosts: result.hosts,
                    rate: r.rate,
                    completedPerSec: r.row.runsCompletedPerSec,
                    transitionsPerSec: r.row.transitionsPerSec,
                    startP50: r.row.startMs?.p50 ?? null,
                    taskP50: r.row.nodeMs?.task?.p50 ?? null,
                    stuck: r.row.stuck.total,
                    errors: r.row.errors.total,
                    deferred: r.row.startsDeferred,
                    completedUnreported: r.row.completedUnreported,
                    droppedEvents: r.row.droppedEvents,
                    wakesLost: r.row.wakesLost,
                    reminderSetFailures: r.delta.reminderSetFailures ?? null,
                    publishFailures: r.delta.publishFailures ?? null,
                    remoteDispatches: r.delta['cluster/dispatchesRemote'] ?? null,
                    localDispatches: r.delta['cluster/dispatchesLocal'] ?? null,
                    peakActivations: r.peakActivations,
                    queuedAfter: r.queuedAfter,
                    hostCpu: r.hostCpu,
                    generatorCpu: r.generatorCpu
                })
            );
        }
    }
    process.exit(result.rungs.some((r) => r.row.stuck.total > 0) ? 1 : 0);
}
