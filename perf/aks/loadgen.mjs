/**
 * The load generator — a closed loop against the public wire endpoint,
 * runnable as a k8s Job (same image as the host, different command) or
 * locally. Everything is env-driven:
 *
 *   TARGET_URL        http://host-service:7311                REQUIRED
 *   MODE              counter | crunch | mixed | verify       default counter
 *   CONCURRENCY       closed-loop workers                     default 32
 *   DURATION_S        run length per rung                     default 60
 *   KEY_COUNT         distinct keys k-0..k-(N-1)              default 1000
 *   HOT_RATIO         fraction of calls aimed at key 'hot'    default 0.2
 *   SWEEP             e.g. "1,2,4,8,16" — one run per rung    default off
 *   CRUNCH_ITERS      sha256 rounds per crunch call           default 200
 *   CRUNCH_PAYLOAD    crunch payload bytes                    default 1024
 *   REPORT_INTERVAL_S progress cadence (stderr)               default 10
 *   RUN_ID            tag echoed in the summary               default hostname
 *
 * Output contract: ONE JSON line per run on stdout — everything else goes
 * to stderr — so `kubectl logs job/... | jq -s` is the whole result
 * pipeline. MODE=verify reads Counter.current() for 'hot' and every k-i
 * key and prints their actuals; compare with the acked counts of previous
 * counter runs (state loss = actual < acked; actual > acked is legal — an
 * increment can commit after its response was lost).
 *
 * Wire protocol: POST {TARGET_URL}/_sigx/actor/{Type}/{method} with
 * {"args":[key, ...args]} — the actor key is the first wire argument.
 */
import { hostname } from 'node:os';
import { closedLoop } from './src/loadgen/loop.ts';

const need = (name) => {
    const value = process.env[name];
    if (!value) {
        console.error(`[loadgen] missing required env ${name}`);
        process.exit(1);
    }
    return value;
};

const TARGET_URL = need('TARGET_URL').replace(/\/$/, '');
const MODE = process.env.MODE ?? 'counter';
const CONCURRENCY = Number(process.env.CONCURRENCY ?? 32);
const DURATION_S = Number(process.env.DURATION_S ?? 60);
const KEY_COUNT = Number(process.env.KEY_COUNT ?? 1000);
const HOT_RATIO = Number(process.env.HOT_RATIO ?? 0.2);
const CRUNCH_ITERS = Number(process.env.CRUNCH_ITERS ?? 200);
const CRUNCH_PAYLOAD = Number(process.env.CRUNCH_PAYLOAD ?? 1024);
const REPORT_INTERVAL_S = Number(process.env.REPORT_INTERVAL_S ?? 10);
const RUN_ID = process.env.RUN_ID ?? hostname();

const log = (...args) => console.error('[loadgen]', ...args);

/** One wire call. Returns null on success, an error-kind string on failure. */
// RETRY_CONN_ERRORS=1: retry pre-response connection failures once, the
// way production HTTP clients and meshes do. Off by default — a strict
// client is a better failure detector — but the honest configuration for
// the rolling-restart scenario: the ~0.004% residue there is the
// irreducible race between a client writing onto a pooled socket and the
// exiting server retiring it, and it is exactly what client retries exist
// for. Only connection-level kinds are retried; an HTTP status or a
// response that died mid-body is NOT (the call may have executed).
const RETRY_CONN_ERRORS = process.env.RETRY_CONN_ERRORS === '1';
const CONN_ERROR_CODES = new Set(['UND_ERR_SOCKET', 'ECONNRESET', 'ECONNREFUSED', 'EPIPE']);
let retriedConnErrors = 0;

async function wireCall(type, method, args, attempt = 0) {
    const url = `${TARGET_URL}/_sigx/actor/${type}/${method}`;
    try {
        const res = await fetch(url, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args })
        });
        if (!res.ok) {
            // Read the body — leaving it unread leaks the connection.
            await res.text().catch(() => {});
            return String(res.status);
        }
        const body = await res.json();
        return body.error ? `app:${body.error.status ?? 'error'}` : null;
    } catch (error) {
        const code = error?.cause?.code ?? error?.name ?? 'unknown';
        if (RETRY_CONN_ERRORS && attempt === 0 && CONN_ERROR_CODES.has(code)) {
            retriedConnErrors++;
            return wireCall(type, method, args, 1);
        }
        return `fetch:${code}`;
    }
}

const keyFor = (i) =>
    Math.random() < HOT_RATIO ? 'hot' : `k-${i % KEY_COUNT}`;

/** MODE=verify: print every key's actual count and exit. */
if (MODE === 'verify') {
    const keys = ['hot', ...Array.from({ length: KEY_COUNT }, (_, i) => `k-${i}`)];
    const actuals = {};
    let failed = 0;
    for (const key of keys) {
        const url = `${TARGET_URL}/_sigx/actor/Counter/current`;
        try {
            const res = await fetch(url, {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ args: [key] })
            });
            const body = await res.json();
            if (body.error) failed++;
            else actuals[key] = body.data;
        } catch {
            failed++;
        }
    }
    console.log(JSON.stringify({ runId: RUN_ID, mode: 'verify', target: TARGET_URL, actuals, failed }));
    process.exit(failed ? 1 : 0);
}

/**
 * MODE=jobs — the long-running-worker scenario. Starts JOB_COUNT SweepJob
 * runs (steps × stepMs each, one checkpoint per step) and polls until
 * every job is terminal or JOB_TIMEOUT_S passes. This is the workload the
 * failover scenarios kill hosts under: every job must still complete, and
 * sumAttempts > completed is the proof that crash-resume actually ran.
 * Keys are RUN_ID-prefixed so repeated runs never collide with retained
 * terminal records. start() is idempotent, so RETRY_CONN_ERRORS is safe.
 */
if (MODE === 'jobs') {
    const JOB_COUNT = Number(process.env.JOB_COUNT ?? 20);
    const JOB_STEPS = Number(process.env.JOB_STEPS ?? 60);
    const JOB_STEP_MS = Number(process.env.JOB_STEP_MS ?? 1000);
    const JOB_TIMEOUT_S = Number(
        process.env.JOB_TIMEOUT_S ?? Math.ceil((JOB_STEPS * JOB_STEP_MS) / 1000) * 3 + 120
    );
    const keys = Array.from({ length: JOB_COUNT }, (_, i) => `${RUN_ID}-j${i}`);

    const started = performance.now();
    let startFailures = 0;
    for (const key of keys) {
        const kind = await wireCall('SweepJob', 'start', [
            key,
            { steps: JOB_STEPS, stepMs: JOB_STEP_MS }
        ]);
        if (kind !== null) {
            startFailures++;
            log(`start failed for ${key}: ${kind}`);
        }
    }
    log(`started ${JOB_COUNT - startFailures}/${JOB_COUNT} jobs (${JOB_STEPS}x${JOB_STEP_MS}ms)`);

    const TERMINAL = new Set(['completed', 'failed', 'cancelled']);
    const deadline = started + JOB_TIMEOUT_S * 1000;
    let infos = new Map();
    for (;;) {
        infos = new Map();
        const url = `${TARGET_URL}/_sigx/actor/SweepJob/status`;
        for (const key of keys) {
            try {
                const res = await fetch(url, {
                    method: 'POST',
                    headers: { 'content-type': 'application/json' },
                    body: JSON.stringify({ args: [key] })
                });
                if (!res.ok) {
                    // Drain — an unread body holds the pooled connection.
                    await res.text().catch(() => {});
                    continue;
                }
                const body = await res.json();
                if (!body.error) infos.set(key, body.data);
            } catch {
                // transient poll failure — the next round retries
            }
        }
        const byStatus = {};
        for (const info of infos.values()) byStatus[info.status] = (byStatus[info.status] ?? 0) + 1;
        const done = [...infos.values()].filter((i) => TERMINAL.has(i.status)).length;
        log(`t=${new Date().toISOString()} jobs=${JSON.stringify(byStatus)} polled=${infos.size}/${JOB_COUNT}`);
        if (done === JOB_COUNT) break;
        if (performance.now() > deadline) break;
        await new Promise((resolve) => setTimeout(resolve, 5000));
    }

    const all = [...infos.values()];
    const completed = all.filter((i) => i.status === 'completed');
    const sumAttempts = all.reduce((a, i) => a + (i.attempts ?? 0), 0);
    const summary = {
        runId: RUN_ID,
        mode: 'jobs',
        target: TARGET_URL,
        jobs: JOB_COUNT,
        steps: JOB_STEPS,
        stepMs: JOB_STEP_MS,
        wallMs: Math.round(performance.now() - started),
        startFailures,
        completed: completed.length,
        failed: all.filter((i) => i.status === 'failed').length,
        cancelled: all.filter((i) => i.status === 'cancelled').length,
        // Anything non-terminal at the deadline (or unpollable) is stuck —
        // the failure mode where a dead host's job never got revived.
        stuck: JOB_COUNT - all.filter((i) => TERMINAL.has(i.status)).length,
        sumAttempts,
        // Every started job costs one attempt; anything beyond that was a
        // crash-resume the cluster survived. Counted over jobs that were
        // actually pollable AND started, so an unpollable straggler in the
        // last round can't skew the metric negative.
        crashResumes: sumAttempts - all.filter((i) => (i.attempts ?? 0) > 0).length
    };
    console.log(JSON.stringify(summary));
    process.exit(summary.completed === JOB_COUNT ? 0 : 1);
}

// -- load modes ------------------------------------------------------------

const errors = new Map(); // kind -> count
const acked = { hot: 0, cold: 0 }; // successful increments per key class
let inFlightErrors = 0;

const callers = {
    counter: async (i) => {
        const key = keyFor(i);
        const kind = await wireCall('Counter', 'increment', [key, 1]);
        if (kind === null) acked[key === 'hot' ? 'hot' : 'cold']++;
        return kind;
    },
    crunch: async (i) => {
        const key = keyFor(i);
        return wireCall('Crunch', 'burn', [key, CRUNCH_ITERS, CRUNCH_PAYLOAD]);
    },
    mixed: async (i) => (i % 4 === 0 ? callers.crunch(i) : callers.counter(i))
};

const caller = callers[MODE];
if (!caller) {
    console.error(`[loadgen] MODE must be counter|crunch|mixed|verify|jobs, got '${MODE}'`);
    process.exit(1);
}

/** The closed loop must keep issuing — errors are tallied, never thrown. */
const call = async (i) => {
    const kind = await caller(i);
    if (kind !== null) {
        inFlightErrors++;
        errors.set(kind, (errors.get(kind) ?? 0) + 1);
    }
};

const rungs = (process.env.SWEEP ?? '')
    .split(',')
    .map((s) => Number(s.trim()))
    .filter((n) => Number.isFinite(n) && n > 0);
if (rungs.length === 0) rungs.push(CONCURRENCY);

// Progress heartbeat: errors surface DURING the run (failure windows in
// the failover scenarios are timed from these lines), not just at the end.
const progress = setInterval(() => {
    log(
        `t=${new Date().toISOString()} errors=${inFlightErrors} ` +
            `kinds=${JSON.stringify(Object.fromEntries(errors))}`
    );
}, REPORT_INTERVAL_S * 1000);
progress.unref();

for (const concurrency of rungs) {
    errors.clear();
    retriedConnErrors = 0;
    inFlightErrors = 0;
    acked.hot = 0;
    acked.cold = 0;
    log(`run: mode=${MODE} c=${concurrency} duration=${DURATION_S}s target=${TARGET_URL}`);

    const outcome = await closedLoop({
        call,
        concurrency,
        durationMs: DURATION_S * 1000,
        latency: true
    });

    const p = outcome.percentiles;
    const errorTotal = [...errors.values()].reduce((a, b) => a + b, 0);
    console.log(
        JSON.stringify({
            runId: RUN_ID,
            mode: MODE,
            target: TARGET_URL,
            concurrency,
            durationMs: Math.round(outcome.elapsedMs),
            ops: outcome.ops,
            opsPerSec: Math.round(outcome.opsPerSec * 10) / 10,
            latencyMs: p
                ? {
                      p50: round(p.p50),
                      p90: round(p.p90),
                      p99: round(p.p99),
                      p999: round(p.p999),
                      max: round(p.max)
                  }
                : null,
            errors: { total: errorTotal, byKind: Object.fromEntries(errors) },
            acked: MODE === 'crunch' ? undefined : { ...acked },
            keyCount: KEY_COUNT,
            hotRatio: HOT_RATIO,
            ...(RETRY_CONN_ERRORS ? { retriedConnErrors } : {})
        })
    );
}

function round(n) {
    return Math.round(n * 1000) / 1000;
}
