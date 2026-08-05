// Capacity ladder against the PUBLIC endpoint, run from the same region so
// RTT stops being the ceiling. Forks W worker processes (one Node process
// is single-core for this workload — the laptop runs proved a single
// process caps out), each driving C/W concurrent closed-loop calls.
import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';

// No default: the target is deployment identity, and every caller
// (`testenv.mjs load` and the Tier-3 driver) always sets it.
const O = process.env.TARGET_URL ?? '';
if (!O) {
    console.error('TARGET_URL is required (the deployment\'s public endpoint)');
    process.exit(1);
}
const COOKIE = process.env.COOKIE ?? '';
const ROOMS = Number(process.env.ROOMS ?? 64);
const MIX = Number(process.env.MIX ?? 0);          // fraction of writes
const DURATION_MS = Number(process.env.DURATION_MS ?? 20_000);
const WORKERS = Number(process.env.WORKERS ?? 4);
const LADDER = (process.env.LADDER ?? '32,64,128,256,512,1024').split(',').map(Number);
// The `postMessage` serverFn's wire id. NO DEFAULT, on purpose: it is
// emitted by the build and the hash moves, and the literal that used to sit
// here had silently rotted. A stale id 404s, a 404 is CHEAPER than a real
// write, and the ladder then reports the breakage as extra throughput — so
// an absent id must stop a write run rather than start a 404 ladder.
// `perf/app/deploy/post-fn.mjs` derives it from the build; testenv and
// the Tier-3 scenarios pass it in from there.
const POST_FN = process.env.POST_FN ?? '';

// ---- what to drive ---------------------------------------------------------
//
// Defaults reproduce the chat read/write mix byte for byte, so the recorded
// `infra/read-ladder`, `infra/write-mix` and `infra/locality-ab` baselines
// keep meaning what they meant. Everything below exists so ONE ladder can
// also drive the worker pool and a cold-key run.
const TYPE = process.env.ACTOR_TYPE ?? 'Room';
const READ_METHOD = process.env.READ_METHOD ?? 'recent';
/** Args AFTER the key, as JSON. Default `[20]` — i.e. `recent(20)`. */
const READ_ARGS = JSON.parse(process.env.READ_ARGS ?? '[20]');
/** `fn` writes through the attributed serverFn; `actor` calls the type. */
const WRITE_MODE = process.env.WRITE_MODE ?? 'fn';
const WRITE_METHOD = process.env.WRITE_METHOD ?? 'setTopic';
const WRITE_ARGS = JSON.parse(process.env.WRITE_ARGS ?? '[]');
const KEY_PREFIX = process.env.KEY_PREFIX ?? 'edge';
// A per-run nonce, so every key is COLD and every call pays an activation.
// Off by default: a warm ladder and a cold one measure different things.
const KEY_SALT = process.env.KEY_FRESH === '1' ? `-${Date.now().toString(36)}` : '';

if (MIX > 0 && WRITE_MODE === 'fn' && !POST_FN) {
    console.error('edge-ladder: MIX>0 with WRITE_MODE=fn needs POST_FN (see post-fn.mjs)');
    process.exit(1);
}
// The routing token, byte-identical to what the client library mints:
// fnv1a(type + NUL + key).toString(36).padStart(7,'0'), carried BOTH in the
// path (/_sigx/actor/r/<token>/<symbol>) and in x-sigx-actor-route. Without
// it an edge hash has nothing to hash and locality stays at 1/N.
const ROUTE = process.env.ROUTE !== '0';
const fnv1a = (input) => {
    let h = 0x811c9dc5;
    for (let i = 0; i < input.length; i++) { h ^= input.charCodeAt(i); h = Math.imul(h, 0x01000193); }
    return h >>> 0;
};
const tokenFor = (type, key) => fnv1a(`${type}\u0000${key}`).toString(36).padStart(7, '0');

// ---- child role -----------------------------------------------------------
if (process.send) {
    // `{Type}/{method}` — real path separator, per-segment encoding, the
    // grammar the client library emits (`wire-url.ts`).
    const symbol = (method) =>
        `${TYPE.split('/').map(encodeURIComponent).join('/')}/${encodeURIComponent(method)}`;
    const call = async (i) => {
        const write = MIX > 0 && Math.random() < MIX;
        const key = `${KEY_PREFIX}${KEY_SALT}-${i % ROOMS}`;
        // A serverFn is not an actor call: it carries no token and its own
        // handler picks the actor, so the edge hash never sees this one.
        const viaFn = write && WRITE_MODE === 'fn';
        const method = write && !viaFn ? WRITE_METHOD : READ_METHOD;
        const tok = ROUTE && !viaFn ? tokenFor(TYPE, key) : null;
        const url = viaFn
            ? `${O}/_sigx/fn/${POST_FN}`
            : tok
              ? `${O}/_sigx/actor/r/${tok}/${symbol(method)}`
              : `${O}/_sigx/actor/${symbol(method)}`;
        const body = viaFn
            ? JSON.stringify({ args: [{ room: key, text: `x${i}` }] })
            : JSON.stringify({ args: [key, ...(write ? WRITE_ARGS : READ_ARGS)] });
        const res = await fetch(url, {
            method: 'POST',
            headers: {
                'content-type': 'application/json',
                origin: O,
                cookie: COOKIE,
                ...(tok ? { 'x-sigx-actor-route': tok } : {})
            },
            body
        });
        await res.text();
        return res.ok;
    };
    process.on('message', async (msg) => {
        const { c, durationMs } = msg;
        const lat = [];
        let ops = 0, errs = 0, i = 0;
        const deadline = performance.now() + durationMs;
        await Promise.all(Array.from({ length: c }, async () => {
            while (performance.now() < deadline) {
                const t0 = performance.now();
                try { (await call(i++)) ? ops++ : errs++; } catch { errs++; }
                if (lat.length < 40_000) lat.push(performance.now() - t0);
            }
        }));
        process.send({ ops, errs, lat });
    });
} else {
    // ---- parent role ------------------------------------------------------
    const self = fileURLToPath(import.meta.url);
    const kids = Array.from({ length: WORKERS }, () => fork(self));
    // Split c EXACTLY: floor(c/W) each, remainder to the first few. Rounding
    // per worker (and clamping to >=1) would silently run a different total
    // than the ladder point claims — c=5 over 4 workers is 2+1+1+1, not
    // 4×1 or 4×2, and a worker with a 0 share simply reports nothing.
    const shares = (c) =>
        Array.from({ length: WORKERS }, (_, i) => Math.floor(c / WORKERS) + (i < c % WORKERS ? 1 : 0));
    const round = (c) => {
        const split = shares(c);
        return Promise.all(
            kids.map(
                (k, i) =>
                    new Promise((resolve) => {
                        k.once('message', resolve);
                        k.send({ c: split[i], durationMs: DURATION_MS });
                    })
            )
        );
    };
    console.log(
        `# target=${O} workers=${WORKERS} rooms=${ROOMS} mix=${MIX} dur=${DURATION_MS}ms ` +
            `type=${TYPE} read=${READ_METHOD} write=${WRITE_MODE}:${WRITE_METHOD} ` +
            `keys=${KEY_PREFIX}${KEY_SALT} route=${ROUTE ? 1 : 0}`
    );
    for (const c of LADDER) {
        const parts = await round(c);
        const actualC = shares(c).reduce((a, b) => a + b, 0);
        const ops = parts.reduce((a, p) => a + p.ops, 0);
        const errs = parts.reduce((a, p) => a + p.errs, 0);
        const lat = parts.flatMap((p) => p.lat).sort((a, b) => a - b);
        const q = (p) => +lat[Math.min(lat.length - 1, Math.ceil(p * lat.length) - 1)].toFixed(1);
        console.log(
            JSON.stringify({
                c: actualC,
                // `ops` alongside `opsPerSec`: an error RATE needs the raw
                // successful count as its denominator, and a rate is what
                // distinguishes a few edge resets from a wholly broken run.
                ops,
                opsPerSec: +(ops / (DURATION_MS / 1000)).toFixed(0),
                p50: q(0.5), p90: q(0.9), p99: q(0.99), max: q(1), errs
            })
        );
    }
    kids.forEach((k) => k.kill());
}
