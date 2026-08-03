/**
 * Prove a deployment works, from outside it.
 *
 * Deliberately NOT a general-purpose load generator. That kind of tool
 * sweeps concurrency across N hosts to find where *your* cluster stops
 * scaling. On Workers, Cloudflare owns placement and scaling — a sweep would
 * be measuring Cloudflare, and publishing the numbers as a `@sigx/actors`
 * result would be misleading.
 *
 * So this checks CORRECTNESS by default, and offers one load mode for the
 * number that is genuinely instructive: a hot key saturates at one Durable
 * Object's single-threaded ceiling, while K keys scale out.
 *
 * Output contract matches loadgen.mjs so the same tooling works: exactly one
 * JSON line on stdout, everything else on stderr.
 *
 *   TARGET_URL   required, e.g. http://127.0.0.1:8787
 *   MODE         verify (default) | load
 *   KEYS         distinct actors to use. Default 8.
 *   OPS          increments per key in verify mode. Default 25.
 *   CONCURRENCY  in-flight requests in load mode. Default 16.
 *   DURATION_S   load mode seconds. Default 10.
 *   HOT          load mode: 1 = every call hits ONE key. Default 0.
 */

const TARGET = process.env.TARGET_URL?.replace(/\/$/, '');
const MODE = process.env.MODE ?? 'verify';
const KEYS = Number(process.env.KEYS ?? 8);
const OPS = Number(process.env.OPS ?? 25);
const CONCURRENCY = Number(process.env.CONCURRENCY ?? 16);
const DURATION_S = Number(process.env.DURATION_S ?? 10);
const HOT = process.env.HOT === '1';

if (!TARGET) {
    process.stderr.write('TARGET_URL is required (e.g. http://127.0.0.1:8787)\n');
    process.exit(2);
}

const log = (msg) => process.stderr.write(`${msg}\n`);

async function callActor(type, method, args) {
    const symbol = encodeURIComponent(`${type}#${method}`);
    const res = await fetch(`${TARGET}/_sigx/actor/${symbol}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ args })
    });
    const text = await res.text();
    let body;
    try {
        body = JSON.parse(text);
    } catch {
        throw new Error(`HTTP ${res.status}: ${text.slice(0, 200)}`);
    }
    if (!res.ok || body.error) {
        throw new Error(body.error?.message ?? `HTTP ${res.status}`);
    }
    return body.data;
}

const keyFor = (i) => `verify-${i}`;

/** Exact counts, per-key isolation, a live stream frame, and a real alarm. */
async function verify() {
    const failures = [];
    const check = (name, ok, detail) => {
        if (ok) log(`  ok    ${name}`);
        else {
            log(`  FAIL  ${name} — ${detail}`);
            failures.push({ name, detail });
        }
    };

    log(`verify against ${TARGET}`);

    // 1. Exact counts. Every increment is a storage write, and turns
    //    serializes them, so N sequential calls must read back exactly N.
    const hot = keyFor(0);
    let last = 0;
    for (let i = 0; i < OPS; i++) last = await callActor('Counter', 'increment', [hot, 1]);
    const read = await callActor('Counter', 'read', [hot]);
    check('exact counts under sequential increments', read === last && read >= OPS,
        `read ${read}, last ${last}, expected >= ${OPS}`);

    // 2. Concurrency against ONE actor. A Durable Object is single-threaded
    //    and turns serialize, so nothing is lost.
    const before = await callActor('Counter', 'read', [hot]);
    await Promise.all(
        Array.from({ length: 20 }, () => callActor('Counter', 'increment', [hot, 1]))
    );
    const after = await callActor('Counter', 'read', [hot]);
    check('no lost updates under 20 concurrent increments', after === before + 20,
        `expected ${before + 20}, got ${after}`);

    // 3. Per-key isolation: K keys are K objects with independent state.
    await Promise.all(
        Array.from({ length: KEYS }, (_, i) => callActor('Counter', 'increment', [keyFor(i + 1), 1]))
    );
    const reads = await Promise.all(
        Array.from({ length: KEYS }, (_, i) => callActor('Counter', 'read', [keyFor(i + 1)]))
    );
    check('K keys are K independent actors', reads.every((n) => n === 1),
        `expected all 1, got ${JSON.stringify(reads)}`);

    // 4. Cross-actor: the call must LEAVE the object and land in the callee's.
    const peer = `${hot}-peer`;
    await callActor('Counter', 'bumpPeer', [hot, peer]);
    const peerCount = await callActor('Counter', 'read', [peer]);
    check('a cross-actor call lands in the callee', peerCount >= 1, `got ${peerCount}`);

    // 5. Streaming through the edge.
    const streamRes = await fetch(
        `${TARGET}/_sigx/actor/${encodeURIComponent('Counter#watch')}`,
        {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args: [hot] })
        }
    );
    let frame = null;
    if (streamRes.ok && streamRes.body) {
        const reader = streamRes.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        const deadline = Date.now() + 5_000;
        while (!buffer.includes('\n') && Date.now() < deadline) {
            const { value, done } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
        }
        if (buffer.includes('\n')) frame = JSON.parse(buffer.slice(0, buffer.indexOf('\n')));
        await reader.cancel();
    }
    check('a watch stream delivers a frame', frame?.chunk !== undefined,
        `status ${streamRes.status}, frame ${JSON.stringify(frame)}`);

    // 6. A REAL alarm, with nothing driving it. This is the only check here
    //    that a plain request cannot stand in for.
    const tick = `tick-${Date.now().toString(36)}`;
    await callActor('Ticker', 'once', [tick, 1_000]);
    let ticks = 0;
    const alarmDeadline = Date.now() + 30_000;
    while (Date.now() < alarmDeadline) {
        ticks = await callActor('Ticker', 'ticks', [tick]);
        if (ticks >= 1) break;
        await new Promise((r) => setTimeout(r, 500));
    }
    check('a reminder fires from a real alarm', ticks >= 1, `ticks ${ticks} after 30s`);

    return { mode: 'verify', checks: 6, failures };
}

/**
 * One hot key vs K keys.
 *
 * The lesson, and the only load number worth publishing here: one Durable
 * Object is single-threaded, so a hot key plateaus at its ceiling no matter
 * the concurrency, while spreading across keys scales out. That is the
 * Workers analogue of the AKS scale-out scenario.
 */
async function load() {
    const latencies = [];
    let ops = 0;
    let errors = 0;
    const started = Date.now();
    const until = started + DURATION_S * 1_000;

    const worker = async (id) => {
        let i = id;
        while (Date.now() < until) {
            const key = HOT ? keyFor(0) : keyFor(i % KEYS);
            const t0 = Date.now();
            try {
                await callActor('Counter', 'increment', [key, 1]);
                latencies.push(Date.now() - t0);
                ops++;
            } catch {
                errors++;
            }
            i += CONCURRENCY;
        }
    };
    log(`load: ${HOT ? '1 hot key' : `${KEYS} keys`}, concurrency ${CONCURRENCY}, ${DURATION_S}s`);
    await Promise.all(Array.from({ length: CONCURRENCY }, (_, i) => worker(i)));

    latencies.sort((a, b) => a - b);
    const at = (p) => latencies[Math.min(latencies.length - 1, Math.floor(latencies.length * p))] ?? 0;
    const elapsedMs = Date.now() - started;
    return {
        mode: 'load',
        hot: HOT,
        keys: HOT ? 1 : KEYS,
        concurrency: CONCURRENCY,
        elapsedMs,
        ops,
        errors,
        opsPerSec: Math.round((ops / elapsedMs) * 1_000),
        p50: at(0.5),
        p90: at(0.9),
        p99: at(0.99)
    };
}

const summary = MODE === 'load' ? await load() : await verify();
process.stdout.write(`${JSON.stringify(summary)}\n`);
if (summary.failures?.length) process.exit(1);
