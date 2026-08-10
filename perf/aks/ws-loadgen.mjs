/**
 * The WebSocket load generator (#172) — how many clients can one host hold
 * open, and how many of them actually receive a message.
 *
 * Sibling to `loadgen.mjs`, which is HTTP and measures ops/s. This one
 * measures the two numbers a socket deployment is actually sized by:
 * CONNECTIONS held, and DELIVERIES pushed to them. It runs as a k8s Job
 * against the in-cluster Service (same image, command swapped), or locally.
 *
 *   TARGET_URL        http://sigx-host:7311                  REQUIRED
 *   MODE              idle | hot | spread | calls            default hot
 *   LADDER            connection rungs, e.g. "1000,5000"     default CONNECTIONS
 *   CONNECTIONS       one rung, when LADDER is unset         default 1000
 *   ACTORS            distinct keys (hot)                    default 1
 *   SUBS_PER_CONN     subscriptions each connection holds    default 1
 *   READ              current | shared | mine                default current
 *                     `current` identity-blind UNDECLARED, `shared` the
 *                     same read DECLARED principalIndependent (#138),
 *                     `mine` identity-dependent. With PRINCIPAL=per-user
 *                     the first two differ in exactly one thing.
 *   PRINCIPAL         anon | per-user                        default anon
 *   SESSION_SECRET    must match the host's, for per-user    default dev secret
 *   PUBLISH_RATE      publishes/sec across the key space     default 10
 *   PAYLOAD_BYTES     bytes of payload per publish           default 0
 *   PERSIST           1 = ctx.save() per publish             default 0
 *   DURATION_S        measured window per rung               default 30
 *   PROBES            latency-sampling connections           default 20
 *   SLOW_FRACTION     0..1 of subscribers that STOP READING     default 0
 *   SLOW_AFTER_S      seconds after the rung seeds before they
 *                     stall                                     default 5
 *   SLOW_RESUME_MS    >0 duty-cycles (pause/resume) instead of
 *                     stalling outright                         default 0
 *   CONNECT_BATCH     dials issued concurrently              default 250
 *   CONNECT_TIMEOUT_S per-connection dial+seed deadline      default 30
 *   KEY_PREFIX        actor key prefix                       default fan
 *   PUBLISHER         1 = this pod publishes and probes      default 1
 *   REPORT_INTERVAL_S progress cadence (stderr)              default 10
 *   RUN_ID            tag echoed in the summary              default hostname
 *
 * Output contract, matching `loadgen.mjs`: ONE JSON line per rung on
 * stdout, everything else on stderr, so `kubectl logs job/… | jq -s` is the
 * whole result pipeline.
 *
 * ## Three things this measures that nothing else in the repo does
 *
 *  - **Held connections.** `connected` is the number that completed the
 *    upgrade AND received its first value. Cross-check it against the
 *    host's own `ops.sockets.open`: if the two disagree, one of them is
 *    wrong and the run is void.
 *  - **Deliveries, not calls.** Each subscriber has its own correlation id,
 *    so the session encodes and stringifies the frame once PER SUBSCRIBER.
 *    Fan-out is O(1) actor reads and O(N) serializations, and
 *    `deliveries_per_publish` is what puts a number on it.
 *  - **Send-buffer growth.** Nothing in the runtime reads `bufferedAmount`
 *    — the socket send path is fire-and-forget. `maxBufferedBytes` is the
 *    client-side half of that story: if it climbs, the server is producing
 *    faster than this client drains, and the ceiling is backpressure rather
 *    than CPU.
 *
 * ## Why latency is measured only on `PROBES` connections
 *
 * Both timestamps have to come off ONE clock. The publisher lives in this
 * process, so `performance.now()` before the publish and again in the
 * subscriber's callback is a true end-to-end measurement; comparing against
 * the actor's own `at` would be measuring clock skew between two pods.
 * Bulk connections therefore only count deliveries, and in a multi-pod Job
 * only the publishing pod carries probes.
 */
import { hostname } from 'node:os';
import { createHmac } from 'node:crypto';
import WebSocket from 'ws';
import { socketTransport } from '@sigx/actors-ws/client';
import { Samples } from './src/loadgen/histogram.ts';

const need = (name) => {
    const value = process.env[name];
    if (!value) {
        console.error(`[ws-loadgen] missing required env ${name}`);
        process.exit(1);
    }
    return value;
};
const num = (name, fallback) => {
    const raw = process.env[name];
    if (raw === undefined || raw === '') return fallback;
    const value = Number(raw);
    if (!Number.isFinite(value)) {
        console.error(`[ws-loadgen] ${name} must be a number, got '${raw}'`);
        process.exit(1);
    }
    return value;
};

const TARGET_URL = need('TARGET_URL').replace(/\/$/, '');
const MODE = process.env.MODE ?? 'hot';
const CONNECTIONS = num('CONNECTIONS', 1000);
const ACTORS = Math.max(1, num('ACTORS', 1));
const SUBS_PER_CONN = Math.max(1, num('SUBS_PER_CONN', 1));
const READ = process.env.READ ?? 'current';
const PRINCIPAL = process.env.PRINCIPAL ?? 'anon';
const SESSION_SECRET = process.env.SESSION_SECRET ?? 'perf-aks-dev-secret';
const PUBLISH_RATE = num('PUBLISH_RATE', 10);
const PAYLOAD_BYTES = num('PAYLOAD_BYTES', 0);
const PERSIST = process.env.PERSIST === '1';
const DURATION_S = num('DURATION_S', 30);
const PROBES = num('PROBES', 20);
// The #182 arm: a deliberately slow consumer. See `stall()` for why this
// pauses the TCP socket rather than delaying the message handler.
const SLOW_FRACTION = num('SLOW_FRACTION', 0);
const SLOW_AFTER_S = num('SLOW_AFTER_S', 5);
const SLOW_RESUME_MS = num('SLOW_RESUME_MS', 0);
const CONNECT_BATCH = Math.max(1, num('CONNECT_BATCH', 250));
const CONNECT_TIMEOUT_S = num('CONNECT_TIMEOUT_S', 30);
const KEY_PREFIX = process.env.KEY_PREFIX ?? 'fan';
// Indexed Job: pod 0 publishes and carries the probes, everyone else is
// pure subscriber capacity. Exactly ONE publisher is a correctness
// requirement, not a tidiness one — the latency pairing predicts the next
// seq, and a second publisher on the same key invalidates every sample.
// `PUBLISHER` remains an explicit override for a local run.
const JOB_INDEX = num('JOB_INDEX', 0);
const PUBLISHER =
    process.env.PUBLISHER !== undefined && process.env.PUBLISHER !== ''
        ? process.env.PUBLISHER !== '0'
        : JOB_INDEX === 0;
// Per-user principals must not collide across pods, or two pods' clients
// share an identity and the coalescing this arm measures is understated.
const USER_OFFSET = JOB_INDEX * 1_000_000;
const REPORT_INTERVAL_S = num('REPORT_INTERVAL_S', 10);
const RUN_ID = process.env.RUN_ID ?? hostname();
const SOCKET_PATH = process.env.SOCKET_PATH ?? '/_sigx/socket';

const MODES = new Set(['idle', 'hot', 'spread', 'calls']);
if (!MODES.has(MODE)) {
    console.error(`[ws-loadgen] MODE must be ${[...MODES].join('|')}, got '${MODE}'`);
    process.exit(1);
}

// READ and PRINCIPAL are validated rather than defaulted, unlike most knobs
// here, because a typo in either does not fail — it silently runs a
// DIFFERENT EXPERIMENT and reports it under the name you asked for.
// `READ=shred` would measure the undeclared arm and be recorded as the
// declared one (#138); `PRINCIPAL=peruser` would measure anonymous fan-out
// and be recorded as per-user, which is the distinction the whole socket
// axis rests on. A wrong number is worse than no number.
const READS = new Set(['current', 'shared', 'mine']);
if (!READS.has(READ)) {
    console.error(`[ws-loadgen] READ must be ${[...READS].join('|')}, got '${READ}'`);
    process.exit(1);
}
// All three are validated, not merely defaulted. A negative `SLOW_AFTER_S`
// arms the stall immediately, which measures failed ESTABLISHMENT (#180's
// axis) instead of an established subscriber falling behind — the exact
// thing the delay exists to avoid. A negative `SLOW_RESUME_MS` turns the
// duty cycle into a zero-delay interval that spins the event loop, so the
// generator becomes the bottleneck and the run measures itself.
if (!(SLOW_FRACTION >= 0 && SLOW_FRACTION <= 1)) {
    console.error(`[ws-loadgen] SLOW_FRACTION must be between 0 and 1, got '${SLOW_FRACTION}'`);
    process.exit(1);
}
if (!(Number.isFinite(SLOW_AFTER_S) && SLOW_AFTER_S >= 0)) {
    console.error(`[ws-loadgen] SLOW_AFTER_S must be >= 0, got '${SLOW_AFTER_S}'`);
    process.exit(1);
}
if (!(Number.isFinite(SLOW_RESUME_MS) && SLOW_RESUME_MS >= 0)) {
    console.error(`[ws-loadgen] SLOW_RESUME_MS must be >= 0, got '${SLOW_RESUME_MS}'`);
    process.exit(1);
}

const PRINCIPALS = new Set(['anon', 'per-user']);
if (!PRINCIPALS.has(PRINCIPAL)) {
    console.error(`[ws-loadgen] PRINCIPAL must be ${[...PRINCIPALS].join('|')}, got '${PRINCIPAL}'`);
    process.exit(1);
}

const log = (...args) => console.error('[ws-loadgen]', ...args);

// ws:// from http:// — the session folds the schemes when it checks origin,
// but the client still has to dial the right one.
const SOCKET_URL = `${TARGET_URL.replace(/^http/, 'ws')}${SOCKET_PATH}`;

/** The rig's signed session cookie — byte-identical to `src/server-app.ts`. */
const cookieFor = (name) =>
    encodeURIComponent(`${name}.${createHmac('sha256', SESSION_SECRET).update(name).digest('hex')}`);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * One simulated client: the REAL shipped transport over a real `ws` socket,
 * so this measures the code a browser runs rather than a hand-rolled
 * approximation of it.
 *
 * The link seam is what makes that possible without a browser: `connect`
 * hands back a `SocketLink`, and holding the raw socket alongside it is the
 * only way to sample `bufferedAmount`.
 */
function createClient(index) {
    const state = { socket: null, maxBuffered: 0, drops: 0 };
    const headers =
        PRINCIPAL === 'per-user'
            ? { cookie: `user=${cookieFor(`u${USER_OFFSET + index}`)}` }
            : undefined;
    const transport = socketTransport({
        connect: (handlers) => {
            // permessage-deflate OFF on purpose: at fan-out scale compression
            // would dominate CPU on both ends and turn a delivery
            // measurement into a zlib benchmark. The server side does not
            // negotiate it either.
            const socket = new WebSocket(SOCKET_URL, {
                headers,
                perMessageDeflate: false
            });
            state.socket = socket;
            socket.on('open', () => handlers.onOpen());
            socket.on('message', (data, isBinary) => {
                if (!isBinary) handlers.onMessage(String(data));
            });
            socket.on('close', () => {
                state.drops++;
                handlers.onClose();
            });
            // `ws` emits 'error' before 'close'; the transport treats close
            // as the only failure signal, so this only has to not throw.
            socket.on('error', () => {});
            return {
                send: (message) => socket.send(message),
                close: () => socket.close()
            };
        }
    });
    return { transport, state };
}

/**
 * Make one connection a SLOW CONSUMER, by pausing the TCP socket underneath
 * it (#182).
 *
 * It has to be the socket, not the message handler. `ws` delivers frames as
 * events whether or not the application does anything with them, so a
 * delayed or empty `onMessage` still drains the kernel buffer and the server
 * never notices — you would measure the generator's own event loop and
 * report it as backpressure. Pausing the `net.Socket` stops reading, the
 * receive window closes, and the server's send buffer is where the
 * unconsumed data actually piles up. That is the thing `maxBufferedBytes`
 * counts, and the thing #182 says nothing in the runtime reacts to.
 *
 * Nothing evicts these connections, which is the point: the socket session's
 * `{ p: 1 }` is an APPLICATION frame with no pong requirement, and
 * `@sigx/actors-ws` installs no WebSocket-level keepalive and no idle
 * timeout. So a stalled subscriber is not disconnected — it is unbounded
 * memory on the host, and the run should show `maxBufferedBytes` climbing
 * with `drops` staying flat. A rung where drops climb instead is a finding,
 * not a broken rig: something DID react.
 *
 * `SLOW_RESUME_MS > 0` duty-cycles instead — a merely slow client rather
 * than a dead one, which is the shape a real bad network produces.
 */
function stall(state) {
    // `ws` exposes the underlying socket once the upgrade completes; before
    // that there is nothing to pause and the connection is not yet holding
    // any deliveries anyway.
    const socket = state.socket?._socket;
    if (!socket || typeof socket.pause !== 'function') return () => {};
    let stopped = false;
    socket.pause();
    if (SLOW_RESUME_MS <= 0) {
        // Stall outright: never read again for the life of the rung.
        return () => {
            if (!stopped) socket.resume();
            stopped = true;
        };
    }
    // Duty cycle: drain briefly, then close the window again.
    const timer = setInterval(() => {
        if (stopped) return;
        socket.resume();
        setTimeout(() => {
            if (!stopped) socket.pause();
        }, SLOW_RESUME_MS).unref();
    }, SLOW_RESUME_MS * 4);
    timer.unref();
    return () => {
        stopped = true;
        clearInterval(timer);
        socket.resume();
    };
}

/** The keys this rung publishes to and subscribes over. */
const keysFor = (rung) => {
    const count = MODE === 'spread' ? rung * SUBS_PER_CONN : ACTORS;
    return Array.from({ length: count }, (_, i) => `${KEY_PREFIX}-${i}`);
};

/**
 * One rung: dial `n` connections, hold them for `DURATION_S` while the
 * publisher runs, then tear everything down and report.
 */
async function runRung(n) {
    const keys = keysFor(n);
    const clients = [];
    const unsubscribes = [];
    const connectSamples = new Samples();
    const latency = new Samples();

    /** Connections chosen to stop reading — see `stall()` and #182. */
    const slowClients = [];
    let resumeSlow = () => {};
    let connected = 0;
    let connectFailures = 0;
    let deliveries = 0;
    let subscriptionErrors = 0;

    // "<key>#<seq>" -> the local clock reading taken just before that
    // publish was issued. Only the publishing pod fills it, and only probe
    // connections read it, so both ends of every latency sample come off
    // this process's clock.
    //
    // Keyed by KEY AND seq, not by seq alone: in spread mode every actor
    // has its own sequence, so a bare seq collides across keys and would
    // pair a delivery with some other actor's publish.
    const publishedAt = new Map();
    const stamp = (key, seq) => `${key}#${seq}`;
    // BOUNDED. A stamp is only ever read by a delivery that lands within a
    // throttle window plus a round trip of its publish, so anything older
    // is dead weight — and at a high rate over a long rung an unbounded map
    // is a load-generator memory leak that would show up as a server-side
    // ceiling. Map preserves insertion order, so the oldest entry is the
    // first key.
    const STAMP_CAP = Math.max(1000, keysFor(n).length * 4);
    const rememberPublish = (key, seq, at) => {
        publishedAt.set(stamp(key, seq), at);
        while (publishedAt.size > STAMP_CAP) {
            publishedAt.delete(publishedAt.keys().next().value);
        }
    };
    let probeDeliveries = 0;

    /** Bring one connection up and seed every subscription it holds. */
    const connectOne = async (index) => {
        const client = createClient(index);
        clients.push(client);
        const isProbe = PUBLISHER && index < PROBES;
        const started = performance.now();
        try {
            if (MODE === 'calls') {
                // No subscription: the first call is what opens the socket.
                await withTimeout(
                    client.transport.call('Fanout#ping', [keys[index % keys.length]]),
                    CONNECT_TIMEOUT_S * 1000,
                    'the first call'
                );
                connectSamples.record(performance.now() - started);
                connected++;
                return;
            }
            if (MODE === 'idle') {
                // Still a real session — one cheap call forces the upgrade
                // and the prelude, which is what "a held connection" costs.
                await withTimeout(
                    client.transport.call('Fanout#ping', [keys[0]]),
                    CONNECT_TIMEOUT_S * 1000,
                    'the first call'
                );
                connectSamples.record(performance.now() - started);
                connected++;
                return;
            }

            const channel = client.transport.live();
            const seeded = [];
            for (let s = 0; s < SUBS_PER_CONN; s++) {
                const key =
                    MODE === 'spread'
                        ? keys[(index * SUBS_PER_CONN + s) % keys.length]
                        : keys[(index + s) % keys.length];
                let resolveSeed;
                const seed = new Promise((resolve) => {
                    resolveSeed = resolve;
                });
                let first = true;
                const stop = channel.subscribe(
                    { type: 'Fanout', key, method: READ, args: [] },
                    (value) => {
                        if (first) {
                            first = false;
                            resolveSeed();
                            // The seed is not a delivery: every subscriber
                            // gets one at subscribe time, so counting it
                            // would inflate every rung by exactly `n`.
                            return;
                        }
                        deliveries++;
                        if (!isProbe) return;
                        const t0 = publishedAt.get(stamp(key, value?.seq));
                        if (t0 !== undefined) {
                            latency.record(performance.now() - t0);
                            probeDeliveries++;
                        }
                    },
                    () => {
                        subscriptionErrors++;
                    }
                );
                unsubscribes.push(stop);
                seeded.push(seed);
            }
            await withTimeout(
                Promise.all(seeded),
                CONNECT_TIMEOUT_S * 1000,
                'the initial subscription values'
            );
            connectSamples.record(performance.now() - started);
            connected++;
            // DETERMINISTIC selection, never `Math.random()`: two runs of the
            // same rung must stall the same connections or the arm is not
            // reproducible. Probes are excluded — they carry the latency
            // samples, and a paused socket would report the stall as
            // delivery latency for the whole run.
            if (SLOW_FRACTION > 0 && !isProbe && index % 1000 < Math.round(SLOW_FRACTION * 1000)) {
                slowClients.push(client);
            }
        } catch (error) {
            connectFailures++;
            // Close NOW, not at teardown. The upgrade may well have
            // succeeded and only the seeding timed out, and a socket left
            // open by a connection that is not counted in `connected` is
            // precisely what breaks the `ops.sockets.open` cross-check this
            // run is validated by. `close()` is idempotent, so the teardown
            // sweep below can still run over it.
            client.transport.close();
            if (connectFailures <= 5) log(`connect failed (#${index}): ${error?.message ?? error}`);
        }
    };

    log(`rung n=${n} mode=${MODE} keys=${keys.length} dialing…`);
    const dialStarted = performance.now();
    for (let i = 0; i < n; i += CONNECT_BATCH) {
        const batch = [];
        for (let j = i; j < Math.min(i + CONNECT_BATCH, n); j++) batch.push(connectOne(j));
        await Promise.all(batch);
        if (i % (CONNECT_BATCH * 10) === 0) {
            log(`  connected ${connected}/${n} (failures ${connectFailures})`);
        }
    }
    const dialMs = performance.now() - dialStarted;
    log(`rung n=${n} up: connected=${connected} failures=${connectFailures} in ${Math.round(dialMs)}ms`);

    // -- the measured window ------------------------------------------------

    const deliveriesAtStart = deliveries;
    let publishes = 0;
    let publishFailures = 0;
    let seqMismatches = 0;
    let peakBuffered = 0;

    const measureStarted = performance.now();
    const deadline = measureStarted + DURATION_S * 1000;

    // Stall AFTER seeding, not before: a connection that never reads cannot
    // complete its subscription handshake, so stalling at dial time would
    // measure failed establishment (which #180 already covers) instead of an
    // established subscriber that stops keeping up.
    if (slowClients.length > 0) {
        const stops = [];
        const armed = setTimeout(() => {
            for (const c of slowClients) stops.push(stall(c.state));
            log(`  ${stops.length} connection(s) stopped reading (#182)`);
        }, SLOW_AFTER_S * 1000);
        armed.unref();
        resumeSlow = () => {
            clearTimeout(armed);
            for (const stop of stops) stop();
        };
    }

    const sampler = setInterval(() => {
        for (const c of clients) {
            const buffered = c.state.socket?.bufferedAmount ?? 0;
            if (buffered > c.state.maxBuffered) c.state.maxBuffered = buffered;
            if (buffered > peakBuffered) peakBuffered = buffered;
        }
    }, 250);
    sampler.unref();

    const progress = setInterval(() => {
        log(
            `  t=${new Date().toISOString()} connected=${connected} ` +
                `deliveries=${deliveries - deliveriesAtStart} publishes=${publishes} ` +
                `peakBuffered=${peakBuffered} rss=${Math.round(process.memoryUsage().rss / 1048576)}MB`
        );
    }, REPORT_INTERVAL_S * 1000);
    progress.unref();

    if (PUBLISHER && (MODE === 'hot' || MODE === 'spread')) {
        // A publisher connection of its own, so publish traffic never
        // shares a socket with a subscriber being measured.
        const publisher = createClient(-1);
        clients.push(publisher);
        // Latency pairing needs the seq a delivery will carry, and the
        // delivery can land BEFORE the publish call resolves — so the
        // timestamp has to be filed under a PREDICTED seq.
        //
        // Prediction is sound because publishes to one key are issued
        // strictly in order here, but only once the actor's current seq is
        // known: an actor carrying state from an earlier rung (or an
        // earlier run against a live host) does not start at 1. The first
        // publish to each key therefore CALIBRATES — its own reply
        // establishes the base and contributes no latency sample. After
        // that a mismatch means a second publisher exists, and the samples
        // for that key are not trustworthy; it is counted, never silently
        // used.
        const localSeq = new Map();
        const intervalMs = PUBLISH_RATE > 0 ? 1000 / PUBLISH_RATE : 0;
        let k = 0;
        while (performance.now() < deadline) {
            const issuedAt = performance.now();
            const key = keys[k++ % keys.length];
            const known = localSeq.get(key);
            const expected = known === undefined ? null : known + 1;
            if (expected !== null) {
                localSeq.set(key, expected);
                rememberPublish(key, expected, issuedAt);
            }
            try {
                const seq = await publisher.transport.call('Fanout#publish', [
                    key,
                    PAYLOAD_BYTES,
                    PERSIST
                ]);
                publishes++;
                if (expected === null) localSeq.set(key, seq);
                else if (seq !== expected) {
                    seqMismatches++;
                    // Drop the stamp that was filed under the wrong seq —
                    // it can only ever pair a delivery with the wrong
                    // publish — then re-anchor so ONE surprise does not
                    // poison every later sample for this key.
                    publishedAt.delete(stamp(key, expected));
                    localSeq.set(key, seq);
                }
            } catch (error) {
                publishFailures++;
                if (publishFailures <= 5) log(`publish failed: ${error?.message ?? error}`);
            }
            const elapsed = performance.now() - issuedAt;
            if (intervalMs > elapsed) await sleep(intervalMs - elapsed);
        }
    } else {
        // idle / calls / a non-publishing pod: just hold everything open.
        while (performance.now() < deadline) await sleep(Math.min(500, deadline - performance.now()));
    }

    const elapsedMs = performance.now() - measureStarted;
    clearInterval(sampler);
    clearInterval(progress);

    // Let the last throttle window flush before counting: the watch throttle
    // is trailing, so a delivery for the final publish can still be up to
    // one window away. Without this every run under-reports by a hair.
    // Stalled sockets are still PAUSED here, deliberately — they did not
    // receive during the window and must not appear to have.
    await sleep(250);
    // FROZEN before the slow sockets are resumed. Resuming floods each one's
    // buffered backlog through `onMessage`, and counting that would inflate
    // `deliveriesPerSec` with traffic the window never delivered — worse, it
    // would MASK the one outcome that changes the sizing rule, since a burst
    // from the stalled subscribers papers over healthy ones having degraded.
    const delivered = deliveries - deliveriesAtStart;

    // Only now, and only for teardown: a paused socket cannot complete a
    // close handshake, and the leftovers would land on the next rung as
    // drops rather than as this arm's doing.
    resumeSlow();

    const summary = {
        runId: RUN_ID,
        mode: MODE,
        target: TARGET_URL,
        n,
        connected,
        // What the host's `ops.sockets.open` must read while this rung is
        // held: the subscribers PLUS the publisher's own connection. The
        // two agreeing is the run's validity check — stating the expected
        // number here is what stops the publisher being misread as an
        // off-by-one.
        socketsExpected: connected + (PUBLISHER && (MODE === 'hot' || MODE === 'spread') ? 1 : 0),
        connectFailures,
        connectMs: percentiles(connectSamples),
        dialMs: Math.round(dialMs),
        durationMs: Math.round(elapsedMs),
        keys: keys.length,
        subsPerConn: MODE === 'idle' || MODE === 'calls' ? 0 : SUBS_PER_CONN,
        read: READ,
        principal: PRINCIPAL,
        // Reported even when zero: a reader comparing two rungs must be able
        // to see that one of them had stalled subscribers and the other did
        // not, without going back to the invocation.
        slowConnections: slowClients.length,
        publishes,
        publishFailures,
        deliveries: delivered,
        deliveriesPerSec: round(delivered / (elapsedMs / 1000)),
        // The COALESCING RATIO, not a constant: client subscriptions run at
        // the runtime's fixed 50 ms watch throttle, so above ~20 publishes/s
        // per key this falls below the subscriber count by design.
        deliveriesPerPublish: publishes > 0 ? round(delivered / publishes) : 0,
        latencyMs: percentiles(latency),
        probeDeliveries,
        seqMismatches,
        subscriptionErrors,
        drops: clients.reduce((a, c) => a + c.state.drops, 0),
        maxBufferedBytes: peakBuffered,
        clientRssMb: Math.round(process.memoryUsage().rss / 1048576),
        payloadBytes: PAYLOAD_BYTES,
        persist: PERSIST,
        publisher: PUBLISHER
    };

    for (const stop of unsubscribes) {
        try {
            stop();
        } catch {
            // teardown is best-effort; the socket close below covers it
        }
    }
    for (const c of clients) c.transport.close();
    // Give the closes a moment so the next rung starts from a quiet host.
    await sleep(500);

    return summary;
}

function withTimeout(promise, ms, what) {
    let timer;
    const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`timed out waiting for ${what}`)), ms);
        timer.unref?.();
    });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function percentiles(samples) {
    const p = samples.percentiles();
    if (!p || p.count === 0) return null;
    return {
        count: p.count,
        p50: round(p.p50),
        p90: round(p.p90),
        p99: round(p.p99),
        max: round(p.max)
    };
}

function round(n) {
    return Math.round(n * 1000) / 1000;
}

// -- the ladder -------------------------------------------------------------

const rungs = (process.env.LADDER ?? '')
    .split(',')
    .map((s) => Number(s.trim()))
    .filter((n) => Number.isFinite(n) && n > 0);
if (rungs.length === 0) rungs.push(CONNECTIONS);

log(
    `target=${SOCKET_URL} mode=${MODE} ladder=${rungs.join(',')} actors=${ACTORS} ` +
        `read=${READ} principal=${PRINCIPAL} rate=${PUBLISH_RATE}/s payload=${PAYLOAD_BYTES}B ` +
        `persist=${PERSIST} publisher=${PUBLISHER}`
);

for (const rung of rungs) {
    const summary = await runRung(rung);
    console.log(JSON.stringify(summary));
    // A rung that could not hold its connections is the ceiling; climbing
    // past it measures nothing but the failure mode.
    if (summary.connectFailures > 0) {
        log(`stopping the ladder: ${summary.connectFailures} connect failures at n=${rung}`);
        break;
    }
}
