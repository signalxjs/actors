/**
 * The load generator — a closed loop against the public wire endpoint,
 * runnable as a k8s Job (same image as the silo, different command) or
 * locally. Everything is env-driven:
 *
 *   TARGET_URL        http://silo-service:7311                REQUIRED
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
 * Wire protocol: POST {TARGET_URL}/_sigx/actor/{Type}%23{method} with
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
async function wireCall(type, method, args) {
    const url = `${TARGET_URL}/_sigx/actor/${encodeURIComponent(`${type}#${method}`)}`;
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
        return `fetch:${error?.cause?.code ?? error?.name ?? 'unknown'}`;
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
        const url = `${TARGET_URL}/_sigx/actor/${encodeURIComponent('Counter#current')}`;
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
    console.error(`[loadgen] MODE must be counter|crunch|mixed|verify, got '${MODE}'`);
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
            hotRatio: HOT_RATIO
        })
    );
}

function round(n) {
    return Math.round(n * 1000) / 1000;
}
