// @vitest-environment node
//
// NOT the repo-default happy-dom: its fetch enforces browser CORS and runs
// in a Window context, so every cross-origin request here fails as a
// NetworkError before it reaches the wire. This suite IS the HTTP client.
/**
 * The deployment's own test suite — env-gated, like the Redis and
 * kubeconfig suites, so `pnpm test` is unaffected when no environment is
 * up. What it asserts is everything a unit test cannot: that the PROXY
 * passes encoded paths through, that the internal mounts are unreachable
 * from outside, that a signed cookie is the only way in, that a stream
 * outlives an idle proxy window, and — behind `INFRA_CHAOS=1` — that a
 * rolling restart drops nothing and a killed host loses no committed state.
 *
 *   INFRA_URL=https://chat.example.net \
 *   INFRA_AUTH_SECRET=… INFRA_OPS_SECRET=… pnpm test:infra
 *
 * `testenv.mjs test` exports all three from the live release, so the
 * normal way to run this is `node testenv.mjs test`.
 *
 * Deliberately pure HTTP: the suite knows nothing about Kubernetes, so it
 * works against any deployment of this app. Only the chaos section shells
 * out to kubectl, and only when explicitly enabled.
 */
import { execFile, execFileSync } from 'node:child_process';
import { promisify } from 'node:util';
import { createHmac } from 'node:crypto';
import { describe, expect, it } from 'vitest';
// The one thing this suite does import from the app: the serverFn id is
// emitted by the build, so pasting it in is how it rots. See post-fn.mjs.
import { postMessageFnId } from '../../../examples/chat/deploy/post-fn.mjs';

const URL_BASE = process.env.INFRA_URL?.replace(/\/$/, '');
const AUTH_SECRET = process.env.INFRA_AUTH_SECRET;
const OPS_SECRET = process.env.INFRA_OPS_SECRET;
const CHAOS = process.env.INFRA_CHAOS === '1';
const NS = process.env.INFRA_NS ?? 'sigx-chat';
const CONTEXT = process.env.INFRA_CONTEXT ?? '';
const RELEASE = process.env.INFRA_RELEASE ?? 'chat';

/**
 * The knobs the deployment may have been brought up with. Each gates the
 * suite that asserts it, so a default deployment stays green — and each is
 * also part of `INFRA_SHAPE`, so the perf half refuses to compare across
 * them. `testenv.mjs` exports all of these from the live release.
 */
const MAX_ACTIVATIONS = Number(process.env.INFRA_MAX_ACTIVATIONS ?? 0);
const SWEEP_INTERVAL_MS = Number(process.env.INFRA_SWEEP_INTERVAL_MS ?? 60_000);
const REBALANCE = process.env.INFRA_REBALANCE === '1';
const REBALANCE_THRESHOLD = Number(process.env.INFRA_REBALANCE_THRESHOLD ?? 1.2);
const PLACEMENT = process.env.INFRA_PLACEMENT ?? 'prefer-local';
/** A room written by the PREVIOUS image — see the migrateState suite. */
const MIGRATED_ROOM = process.env.INFRA_MIGRATED_ROOM ?? '';

/** The reserved wire symbol for the multiplexed live mount. */
const LIVE_SYMBOL = '$live#subscribe';

/** Sign a session the way the app's guard verifies it. */
const cookieFor = (name: string): string => {
    const sig = createHmac('sha256', AUTH_SECRET ?? '').update(name).digest('hex');
    return `user=${encodeURIComponent(`${name}.${sig}`)}`;
};

/** fnv1a(type NUL key) in base36 — the routing token clients send. */
const routeToken = (type: string, key: string): string => {
    const input = `${type}\u0000${key}`;
    let h = 0x811c9dc5;
    for (let i = 0; i < input.length; i++) {
        h ^= input.charCodeAt(i);
        h = Math.imul(h, 0x01000193);
    }
    return (h >>> 0).toString(36).padStart(7, '0');
};

interface CallOptions {
    cookie?: string | null;
    origin?: string | null;
    route?: boolean;
}

/** One actor call, shaped exactly as a browser shapes it. */
async function actorCall(
    type: string,
    method: string,
    args: readonly unknown[],
    options: CallOptions = {}
): Promise<Response> {
    const { cookie = cookieFor('tester'), origin = URL_BASE, route = true } = options;
    const key = String(args[0] ?? '');
    const token = route ? routeToken(type, key) : null;
    const path = token
        ? `/_sigx/actor/r/${token}/${encodeURIComponent(`${type}#${method}`)}`
        : `/_sigx/actor/${encodeURIComponent(`${type}#${method}`)}`;
    return fetch(`${URL_BASE}${path}`, {
        method: 'POST',
        headers: {
            'content-type': 'application/json',
            ...(origin ? { origin } : {}),
            ...(cookie ? { cookie } : {}),
            ...(token ? { 'x-sigx-actor-route': token } : {})
        },
        body: JSON.stringify({ args })
    });
}

const dataOf = async (res: Response): Promise<unknown> => (await res.json()).data;
const errorOf = async (res: Response): Promise<{ status?: number; message?: string }> =>
    (await res.json()).error ?? {};

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** Poll until `ok()` or the budget runs out; throws rather than passing. */
async function waitFor(
    ok: () => boolean | Promise<boolean>,
    timeoutMs: number,
    everyMs = 250
): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
        if (await ok()) return;
        if (Date.now() >= deadline) throw new Error(`timed out after ${timeoutMs}ms`);
        await sleep(everyMs);
    }
}

// --- the NDJSON envelope ----------------------------------------------------

/** One wire line. `chunk` carries a `$live` frame: `{i,v}`, `{i,e}` or `{p:1}`. */
interface LiveLine {
    chunk?: { i?: number; v?: unknown; e?: { status: number }; p?: number };
    done?: number;
    error?: { status?: number; message?: string };
}

/** Parse a held-open NDJSON response line by line. */
async function* ndjson(res: Response): AsyncGenerator<LiveLine> {
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    for (;;) {
        const { done, value } = await reader.read();
        if (done) return;
        buffer += decoder.decode(value, { stream: true });
        for (;;) {
            const nl = buffer.indexOf('\n');
            if (nl < 0) break;
            const line = buffer.slice(0, nl).trim();
            buffer = buffer.slice(nl + 1);
            if (line) yield JSON.parse(line) as LiveLine;
        }
    }
}

// --- ops ---------------------------------------------------------------------

/** Where the internal listener is reachable — a port-forward, never the
 *  public host (see the sealed-surface suite). */
const OPS_URL = process.env.INFRA_OPS_URL ?? 'http://127.0.0.1:7399';

/** Only the parts of `ClusterStatsReport` this suite reads. */
interface ClusterReport {
    view: { size: number; active: number };
    hosts: { hostId: string; status: string; stats: { activations: number } }[];
    partial: boolean;
    totals: {
        hosts: number;
        activations: number;
        counters: {
            directoryLookups: number;
            directoryClaims: number;
            directoryReleases: number;
            rebalanceRounds: number;
            rebalanceMigrations: number;
        };
        metrics: { activations: { byReason: Record<string, number> } } | null;
    };
}

async function clusterReport(): Promise<ClusterReport> {
    const res = await fetch(`${OPS_URL}/_sigx/ops/cluster`, {
        headers: { authorization: `Bearer ${OPS_SECRET}` }
    });
    if (!res.ok) throw new Error(`ops/cluster answered ${res.status}`);
    return (await res.json()) as ClusterReport;
}

/** Live activations per host id. */
const spreadOf = (report: ClusterReport): Map<string, number> =>
    new Map(report.hosts.map((h) => [h.hostId, h.stats.activations]));

const kubectl = (...args: string[]): string =>
    execFileSync('kubectl', [...(CONTEXT ? ['--context', CONTEXT] : []), '-n', NS, ...args], {
        encoding: 'utf8'
    }).trim();

/**
 * The chaos paths MUST use this, not the sync one: `execFileSync` blocks
 * the event loop, so a `rollout status` wait would freeze the very load the
 * test claims to be running — the assertions would then pass while
 * measuring an idle cluster.
 */
const kubectlAsync = async (...args: string[]): Promise<string> => {
    const { stdout } = await promisify(execFile)(
        'kubectl',
        [...(CONTEXT ? ['--context', CONTEXT] : []), '-n', NS, ...args]
    );
    return stdout.trim();
};

const ready = Boolean(URL_BASE && AUTH_SECRET);

/**
 * Does this deployment carry the `Digest` worker pool?
 *
 * Probed rather than env-gated, because the suite's whole posture is that it
 * works against ANY deployment of this app — including one built before
 * `defineWorker` existed, which answers 404 for the type. Top-level so the
 * answer is in hand when `describe.skipIf` is evaluated; a `beforeAll` runs
 * too late to skip anything.
 */
const workersAvailable = ready
    ? await actorCall('Digest', 'summarize', ['probe', 'x', 1])
          .then((res) => res.status === 200)
          .catch(() => false)
    : false;

describe.skipIf(!ready)('infra: the public surface is sealed', () => {
    // Nothing under /_sigx except the actor and serverFn endpoints may
    // answer from outside — and a miss must be a 404, never the SSR
    // document (which would be a 200 page for a probe of your internals).
    it.each([
        ['/_sigx', 'the bare prefix'],
        ['/_sigx/health', 'liveness — unauthenticated by design'],
        ['/_sigx/health/ready', 'readiness'],
        ['/_sigx/ops', 'ops — bearer-authenticated, still not public'],
        ['/_sigx/ops/cluster', 'the cluster fan-out'],
        ['/_sigx/host', 'the host-to-host mount'],
        ['/_sigx/host/anything', 'anything under it']
    ])('404s %s (%s)', async (path) => {
        const res = await fetch(`${URL_BASE}${path}`);
        expect(res.status).toBe(404);
        // Not the document handler: a 200 of HTML here would mean the
        // reserved-path guard fell through rather than sealing.
        expect(res.headers.get('content-type') ?? '').not.toMatch(/html/);
    });

    it('serves the app itself', async () => {
        const res = await fetch(`${URL_BASE}/`);
        expect(res.status).toBe(200);
        expect(res.headers.get('content-type')).toMatch(/html/);
    });
});

describe.skipIf(!ready)('infra: only a signed session gets in', () => {
    it('rejects a call with no cookie', async () => {
        const res = await actorCall('Room', 'recent', ['auth-probe', 5], { cookie: null });
        expect((await errorOf(res)).status).toBe(401);
    });

    it('rejects a forged signature', async () => {
        const res = await actorCall('Room', 'recent', ['auth-probe', 5], {
            cookie: 'user=tester.deadbeef'
        });
        expect((await errorOf(res)).status).toBe(401);
    });

    it('rejects a valid signature for a DIFFERENT name (no name swapping)', async () => {
        const stolen = cookieFor('tester').split('.').slice(-1)[0];
        const res = await actorCall('Room', 'recent', ['auth-probe', 5], {
            cookie: `user=${encodeURIComponent(`admin.${stolen}`)}`
        });
        expect((await errorOf(res)).status).toBe(401);
    });

    it('accepts a properly signed session', async () => {
        const res = await actorCall('Room', 'recent', ['auth-probe', 5]);
        expect(res.status).toBe(200);
        expect(await dataOf(res)).toBeInstanceOf(Array);
    });

    it('refuses a cross-origin call', async () => {
        const res = await actorCall('Room', 'recent', ['auth-probe', 5], {
            origin: 'https://evil.example'
        });
        expect(res.status).toBe(403);
    });
});

describe.skipIf(!ready)('infra: the proxy does not mangle the wire', () => {
    // `Room#topic` travels as Room%23topic. A proxy that decodes or
    // normalizes encoded paths turns the symbol into a 404 — silently, and
    // only in deployment.
    it('passes a percent-encoded symbol through untouched', async () => {
        const res = await actorCall('Room', 'topic', ['encoding-probe']);
        expect(res.status).toBe(200);
        expect(typeof (await dataOf(res))).toBe('string');
    });

    it('passes the routing-token path form through', async () => {
        const token = routeToken('Room', 'encoding-probe');
        const res = await fetch(
            `${URL_BASE}/_sigx/actor/r/${token}/${encodeURIComponent('Room#topic')}`,
            {
                method: 'POST',
                headers: {
                    'content-type': 'application/json',
                    origin: URL_BASE!,
                    cookie: cookieFor('tester'),
                    'x-sigx-actor-route': token
                },
                body: JSON.stringify({ args: ['encoding-probe'] })
            }
        );
        expect(res.status).toBe(200);
    });

    it('rejects a non-JSON content type (CSRF floor)', async () => {
        const res = await fetch(`${URL_BASE}/_sigx/actor/${encodeURIComponent('Room#topic')}`, {
            method: 'POST',
            headers: { 'content-type': 'text/plain', origin: URL_BASE!, cookie: cookieFor('tester') },
            body: JSON.stringify({ args: ['encoding-probe'] })
        });
        expect(res.status).toBe(415);
    });
});

describe.skipIf(!ready)('infra: the app works end to end', () => {
    it('seeds the SSR document rather than shipping an empty shell', async () => {
        const room = `ssr-${Date.now().toString(36)}`;
        const post = await actorCall('Room', 'post', [room, 'ssrprobe', 'seeded by the server']);
        expect(post.status).toBe(200);

        const html = await (
            await fetch(`${URL_BASE}/r/${room}`, { headers: { cookie: cookieFor('ssrprobe') } })
        ).text();
        // The message is IN the markup — the whole point of useActorState's
        // SSR seeding is that the first paint costs no request.
        expect(html).toContain('seeded by the server');
        expect(html).toContain(room);
    });

    it('reads a write back through the cluster (any host answers for any actor)', async () => {
        const room = `xhost-${Date.now().toString(36)}`;
        for (let i = 0; i < 3; i++) {
            const res = await actorCall('Room', 'post', [room, 'tester', `m${i}`]);
            expect(res.status).toBe(200);
        }
        // Round-robin across hosts means these reads land wherever; the
        // directory makes them agree.
        for (let i = 0; i < 5; i++) {
            const res = await actorCall('Room', 'recent', [room, 20], { route: false });
            expect(((await dataOf(res)) as unknown[]).length).toBe(3);
        }
    });

    it('streams: a quiet $live connection outlives the proxy idle window', async () => {
        // ingress-nginx cuts an idle proxied response at 60s by default; the
        // chart raises both proxy timeouts to 3600 AND turns buffering off,
        // and the endpoint emits a `{"chunk":{"p":1}}` keepalive every 30s
        // (DEFAULT_LIVE_PING_MS). This is the test that catches a deployment
        // where any of those was lost.
        //
        // `$live` is the mount every open room page holds — one held-open
        // response multiplexing many actors — so it deliberately carries NO
        // routing token (a `$`-prefixed type never gets one), and the chart
        // carves it out of the hashed Ingress for exactly that reason.
        //
        // This used to drive a per-actor `Room#watch` stream. The app stopped
        // having one when `useActorState(…, { live: true })` replaced it, so
        // the test had been 404ing against every deployment since.
        const room = `quiet-${Date.now().toString(36)}`;
        const controller = new AbortController();
        const res = await fetch(`${URL_BASE}/_sigx/actor/${encodeURIComponent(LIVE_SYMBOL)}`, {
            method: 'POST',
            headers: {
                'content-type': 'application/json',
                origin: URL_BASE!,
                cookie: cookieFor('tester')
            },
            body: JSON.stringify({ args: [[{ t: 'Room', k: room, m: 'recent', a: [20] }]] }),
            signal: controller.signal
        });
        expect(res.status).toBe(200);
        expect(res.headers.get('content-type')).toMatch(/ndjson/);

        // Drain in the background: a ping that arrives during the quiet
        // window has to be READ to have been observed, and the quiet window
        // is precisely when nothing else is calling `read()`.
        const seen: LiveLine[] = [];
        const pump = (async () => {
            for await (const line of ndjson(res)) seen.push(line);
        })().catch(() => undefined);

        try {
            // The first frame is the initial snapshot for subscription 0.
            await waitFor(
                () => seen.some((l) => l.chunk?.i === 0 && l.chunk?.v !== undefined),
                30_000
            );

            const idleMs = Number(process.env.INFRA_STREAM_IDLE_MS ?? 75_000);
            await new Promise((r) => setTimeout(r, idleMs));
            // Silence is the point: with a 30s ping and a >=75s window, a
            // connection that survived must have carried keepalives.
            expect(seen.some((l) => l.chunk?.p === 1)).toBe(true);

            const before = seen.length;
            await actorCall('Room', 'post', [room, 'tester', 'after the quiet window']);
            await waitFor(
                () =>
                    seen
                        .slice(before)
                        .some((l) =>
                            JSON.stringify(l.chunk?.v ?? '').includes('after the quiet window')
                        ),
                30_000
            );
        } finally {
            controller.abort();
            await pump;
        }
    }, 180_000);
});

describe.skipIf(!ready)('infra: the serverFn id the load ladder drives still resolves', () => {
    // `postMessage_fn_<hash>` is emitted by the build, and the hash moves.
    // `edge-ladder.mjs` used to carry one pasted in, and it had rotted from
    // `…_6c5508cb` to `…_2b42ef63` unnoticed — silently, because a stale id
    // 404s, a 404 is CHEAPER than a real write, and `infra/write-mix` reads
    // the difference as extra throughput rather than as a broken run. One
    // request here, and it fails before any VM budget is spent.
    it('accepts a signed call to the postMessage serverFn', async () => {
        const room = `fnid-${Date.now().toString(36)}`;
        const res = await fetch(`${URL_BASE}/_sigx/fn/${postMessageFnId()}`, {
            method: 'POST',
            headers: {
                'content-type': 'application/json',
                origin: URL_BASE!,
                cookie: cookieFor('tester')
            },
            body: JSON.stringify({ args: [{ room, text: 'wire id probe' }] })
        });
        expect(res.status).toBe(200);
        expect((await res.json()).error).toBeUndefined();
    });
});

describe.skipIf(!ready || !OPS_SECRET)('infra: ops reports a healthy cluster', () => {
    // Reached through a port-forward (OPS_URL), never the public host — see
    // the sealed-surface suite above.
    it('every host is active and the view agrees on its size', async () => {
        const body = await clusterReport();
        expect(body.view.active).toBe(body.view.size);
        expect(body.hosts.every((s) => s.status === 'active')).toBe(true);
    });

    it('rejects an unauthenticated ops read', async () => {
        expect((await fetch(`${OPS_URL}/_sigx/ops`)).status).toBe(401);
    });
});

describe.skipIf(!ready || !OPS_SECRET)('infra: the edge hash actually pins a token', () => {
    // `infra/locality-ab` measures what the routing token BUYS. It cannot
    // tell "locality does not help here" apart from "the edge never hashed
    // anything", and it says so in its own docs — but a perf scenario
    // reporting ~1.0x is a shrug, not a failure, so the second case can sit
    // there indefinitely looking like a finding.
    //
    // It did. ingress-nginx has defaulted `--annotations-risk-level` to
    // `High` since v1.12, and `upstream-hash-by` is rated Critical because
    // its value takes an nginx variable — so the controller silently drops
    // the value, keeps the Ingress object exactly as written, warns about
    // nothing, and load-balances round-robin. Observed on v1.15.1: 40
    // requests carrying ONE token spread 6/6/6/5/6/0/6 across seven hosts,
    // and the owning host received none of them directly.
    //
    // The fix is a controller flag (`--annotations-risk-level=Critical`),
    // which a chart cannot set for a shared ingress — so the least a
    // deployment can do is find out.
    it('sends one routing token to one host', async () => {
        const hosts = (await clusterReport()).totals.hosts;
        // One host makes every request local by definition; nothing to pin.
        if (hosts < 2) return;

        const key = `pin-${Date.now().toString(36)}`;
        const received = async (): Promise<Map<string, number>> =>
            new Map(
                (await clusterReport()).hosts.map((h) => [
                    h.hostId,
                    // What this host was handed by the edge, whether it then
                    // served the call itself or forwarded it to the owner.
                    h.counters.routedLocal + h.counters.remoteDispatches
                ])
            );

        await actorCall('Room', 'recent', [key, 20]);
        await sleep(1_000);
        const before = await received();
        const N = 20;
        for (let i = 0; i < N; i++) await actorCall('Room', 'recent', [key, 20]);
        await sleep(1_000);
        const after = await received();

        const deltas = [...after].map(([id, n]) => n - (before.get(id) ?? 0));
        const top = Math.max(...deltas);
        console.log(`  edge hash: one token over ${hosts} hosts → ${deltas.join('/')}`);
        // A working hash puts all N on one host. Round-robin puts N/hosts on
        // each. Anything below half is not a hash.
        expect(top).toBeGreaterThan(N / 2);
    }, 120_000);
});

describe.skipIf(!ready)('infra: topics fan out across real hosts', () => {
    // In-process fakes already pin the delivery COUNTS. What only a real
    // deployment can show is that the fan-out survives the parts a fake
    // stands in for: a subscriber owned by another pod, reached over the
    // HMAC-signed internal mount, through a directory in Redis.
    it("every room's publish reaches the singleton ActivityFeed", async () => {
        const stamp = Date.now().toString(36);
        const rooms = [`topic-a-${stamp}`, `topic-b-${stamp}`, `topic-c-${stamp}`];
        for (const room of rooms) {
            expect((await actorCall('Room', 'post', [room, 'tester', 'hello'])).status).toBe(200);
        }
        // Three room keys hash to (very likely) different hosts while
        // ActivityFeed('all') lives on exactly one, so at least one of these
        // deliveries crossed a pod boundary. `ctx.publish` settles before
        // `post` returns, so this needs no polling — a missing entry means a
        // delivery was LOST, not that it is late.
        const feed = (await dataOf(
            await actorCall('ActivityFeed', 'recent', ['all', 50])
        )) as { room: string; what: string }[];
        for (const room of rooms) {
            expect(feed.map((e) => e.room)).toContain(room);
        }
    });
});

describe.skipIf(!ready || !workersAvailable)('infra: a worker pool runs one key concurrently', () => {
    const BURST = 4;
    /**
     * Per-call work, in hash iterations.
     *
     * Sized so one call comfortably outlasts the jitter between four
     * requests LEAVING this machine — otherwise the overlap this asserts is
     * a race against the network rather than a property of the pool.
     * Measured against a real deployment across a WAN: at 20k a call runs
     * ~26ms against an RTT of the same order, and the burst intermittently
     * serializes by arrival alone; at 150k it runs ~160ms and members
     * overlap every time. Still well inside the call deadline, and under
     * the server's own `DIGEST_MAX_ITERS` ceiling — which clamps this, so a
     * deployment that lowers the ceiling makes calls short again.
     */
    const ITERS = Number(process.env.INFRA_DIGEST_ITERS ?? 150_000);

    const burst = async (type: 'Digest' | 'DigestActor', key: string) =>
        Promise.all(
            Array.from({ length: BURST }, () =>
                actorCall(type, 'summarize', [key, 'payload', ITERS]).then(
                    (r) => dataOf(r) as Promise<{ member: string; startedAt: number; endedAt: number }>
                )
            )
        );

    /** Do any two intervals intersect? */
    const overlaps = (runs: { startedAt: number; endedAt: number }[]): boolean =>
        runs.some((a, i) =>
            runs.some((b, j) => i !== j && a.startedAt < b.endedAt && b.startedAt < a.endedAt)
        );

    it('the same key runs on several members at once; the stateful twin does not', async () => {
        const stamp = Date.now().toString(36);
        // Both bursts carry a routing token, so each lands wholly on ONE
        // host — which is what makes the timestamps comparable at all.
        const pool = await burst('Digest', `pool-${stamp}`);
        const single = await burst('DigestActor', `single-${stamp}`);

        expect(new Set(pool.map((r) => r.member)).size).toBeGreaterThan(1);
        expect(overlaps(pool)).toBe(true);

        // The contrast, and the reason the control arm exists: one
        // activation per key means one activation, so these serialize.
        expect(new Set(single.map((r) => r.member)).size).toBe(1);
        expect(overlaps(single)).toBe(false);
    }, 120_000);

    it.skipIf(!OPS_SECRET)('costs the directory nothing', async () => {
        // The structural claim behind `cluster/stateless-directory-ops`:
        // a worker is always placed locally, so it never looks up, claims
        // or releases. Asserted here against a REAL directory in Redis.
        const before = (await clusterReport()).totals.counters;
        await burst('Digest', `nodir-${Date.now().toString(36)}`);
        const after = (await clusterReport()).totals.counters;
        expect(after.directoryLookups).toBe(before.directoryLookups);
        expect(after.directoryClaims).toBe(before.directoryClaims);
        expect(after.directoryReleases).toBe(before.directoryReleases);
    }, 120_000);

    it('answers on whichever host the call lands on', async () => {
        // No routing token: the ingress round-robins, so these spray across
        // the fleet. A stateful actor would be proxied to its owner; a
        // worker is local everywhere, so every host answers for itself and
        // none of them ever redirects.
        const results = await Promise.all(
            Array.from({ length: 6 }, () =>
                actorCall('Digest', 'summarize', ['anywhere', 'payload', 100], { route: false })
            )
        );
        for (const res of results) expect(res.status).toBe(200);
    }, 120_000);
});

describe.skipIf(!ready || !OPS_SECRET || MAX_ACTIVATIONS <= 0)(
    'infra: maxActivations sheds under the cap',
    () => {
        it('settles under the cap, and a shed room keeps its history', async () => {
            const stamp = Date.now().toString(36);
            const rooms = Array.from(
                { length: MAX_ACTIVATIONS * 3 },
                (_, i) => `cap-${stamp}-${i}`
            );
            for (const room of rooms) {
                expect((await actorCall('Room', 'post', [room, 'tester', 'x'])).status).toBe(200);
            }

            // Shedding rides the SWEEPER, so nothing happens until one runs.
            // Two intervals plus slack: the first may have fired mid-burst.
            const hosts = (await clusterReport()).totals.hosts;
            await waitFor(
                async () => {
                    const now = await clusterReport();
                    return (
                        now.totals.activations <= MAX_ACTIVATIONS * hosts + hosts &&
                        (now.totals.metrics?.activations.byReason.capacity ?? 0) > 0
                    );
                },
                SWEEP_INTERVAL_MS * 3 + 30_000,
                Math.min(5_000, SWEEP_INTERVAL_MS)
            );

            // The cap is a memory knob, never a correctness one: the first
            // room was shed long ago and must still read back whole.
            const back = (await dataOf(
                await actorCall('Room', 'recent', [rooms[0]!, 20])
            )) as unknown[];
            expect(back.length).toBe(1);
        }, 600_000);
    }
);

describe.skipIf(!ready || !MIGRATED_ROOM)(
    'infra: migrateState upgrades a record written by the previous image',
    () => {
        // The only assertion here that structurally needs TWO deploys —
        // `testenv.mjs migrate-check` sets it up and exports the room name.
        // No fixture and no test-only endpoint: a v1 record is simply what
        // the previous image wrote.
        it('reads the old history back, at the new version', async () => {
            const history = (await dataOf(
                await actorCall('Room', 'recent', [MIGRATED_ROOM, 100])
            )) as unknown[];
            expect(history.length).toBeGreaterThan(0);
            expect(await dataOf(await actorCall('Room', 'version', [MIGRATED_ROOM]))).toBe(2);
        });
    }
);

describe.skipIf(!ready || !CHAOS || !OPS_SECRET || !REBALANCE)(
    'infra: chaos — rebalance converges after a scale-out',
    () => {
        it('migrates its own idlest actors down toward the mean', async () => {
            const stamp = Date.now().toString(36);
            // Give the incumbents something to be imbalanced ABOUT.
            for (let i = 0; i < 60; i++) {
                await actorCall('Room', 'post', [`bal-${stamp}-${i}`, 'tester', 'x']);
            }
            const before = await clusterReport();
            const wasReplicas = before.totals.hosts;
            const incumbents = new Set(before.hosts.map((h) => h.hostId));
            const migrationsBefore = before.totals.counters.rebalanceMigrations;

            try {
                await kubectlAsync('scale', `deploy/${RELEASE}-host`, `--replicas=${wasReplicas + 2}`);
                await kubectlAsync(
                    'rollout',
                    'status',
                    `deploy/${RELEASE}-host`,
                    '--timeout=300s'
                );

                // Waited for SEPARATELY, each with its own message. Rolled
                // into one predicate these are indistinguishable on
                // failure: a timeout tells you the cluster did not reach
                // some state, and not which half of it was missing — which
                // is the difference between "the pods never joined" and
                // "rebalance never fired", two completely different bugs.
                await waitFor(
                    async () => {
                        const now = await clusterReport();
                        return !now.partial && now.totals.hosts > wasReplicas;
                    },
                    300_000,
                    5_000
                ).catch(() => {
                    throw new Error(
                        `the scaled-up hosts never joined the view (was ${wasReplicas})`
                    );
                });

                // A round runs per intervalMs and only moves activations
                // idle for minIdleMs, so this is minutes, not seconds.
                await waitFor(
                    async () =>
                        (await clusterReport()).totals.counters.rebalanceMigrations >
                        migrationsBefore,
                    600_000,
                    10_000
                ).catch(() => {
                    throw new Error(
                        `no rebalance migration in 10min (still ${migrationsBefore}) — ` +
                            `nothing idle enough to move, or the round never ran`
                    );
                });

                // The new hosts took work, and they took it by MIGRATION —
                // nothing here activated a new room after the scale-out.
                //
                // Deliberately NOT a max/mean ratio. Rebalance sheds only
                // down to the mean and only what has been idle, so on a
                // cluster whose activation count is small or moving — which
                // is every cluster this suite has just run chaos against —
                // a tight ratio is a coin flip that says nothing about
                // whether rebalancing works. That new hosts hold actors they
                // never activated is the property, and it is binary.
                const after = await clusterReport();
                const joined = after.hosts.filter((h) => !incumbents.has(h.hostId));
                expect(joined.length).toBeGreaterThan(0);
                expect(joined.some((h) => h.stats.activations > 0)).toBe(true);
                const counts = [...spreadOf(after).values()];
                const mean = counts.reduce((a, b) => a + b, 0) / counts.length;
                console.log(
                    `  rebalance: ${after.totals.counters.rebalanceMigrations - migrationsBefore}` +
                        ` migration(s) onto ${joined.length} joined host(s); spread ` +
                        `max/mean=${(Math.max(...counts) / (mean || 1)).toFixed(2)} ` +
                        `(threshold ${REBALANCE_THRESHOLD})`
                );
            } finally {
                await kubectlAsync('scale', `deploy/${RELEASE}-host`, `--replicas=${wasReplicas}`);
                await kubectlAsync(
                    'rollout',
                    'status',
                    `deploy/${RELEASE}-host`,
                    '--timeout=300s'
                );
            }
        }, 1_200_000);
    }
);

describe.skipIf(!ready || !CHAOS || !OPS_SECRET || PLACEMENT !== 'activation-count')(
    'infra: chaos — activationCountPolicy steers new work at the cold hosts',
    () => {
        it('a freshly joined host attracts more than its uniform share', async () => {
            const stamp = Date.now().toString(36);
            for (let i = 0; i < 60; i++) {
                await actorCall('Room', 'post', [`warm-${stamp}-${i}`, 'tester', 'x']);
            }
            const before = await clusterReport();
            const wasReplicas = before.totals.hosts;
            const incumbents = new Set(before.hosts.map((h) => h.hostId));

            try {
                await kubectlAsync('scale', `deploy/${RELEASE}-host`, `--replicas=${wasReplicas + 2}`);
                await kubectlAsync(
                    'rollout',
                    'status',
                    `deploy/${RELEASE}-host`,
                    '--timeout=300s'
                );
                await waitFor(
                    async () => (await clusterReport()).totals.hosts > wasReplicas,
                    300_000,
                    5_000
                );

                const baseline = spreadOf(await clusterReport());
                // No routing token: the edge sends these round-robin, so the
                // PLACEMENT decides where they activate rather than the hash.
                const fresh = 90;
                for (let i = 0; i < fresh; i++) {
                    await actorCall('Room', 'post', [`cold-${stamp}-${i}`, 'tester', 'x'], {
                        route: false
                    });
                }
                const after = spreadOf(await clusterReport());

                let onJoiners = 0;
                let total = 0;
                for (const [hostId, count] of after) {
                    const delta = Math.max(0, count - (baseline.get(hostId) ?? 0));
                    total += delta;
                    if (!incumbents.has(hostId)) onJoiners += delta;
                }
                expect(total).toBeGreaterThan(0);
                // A host with no known load reads as COLD, so the joiners
                // should out-draw a uniform split (2 of N). Asserted with
                // room to spare — this is a bias, not a guarantee, and the
                // directory stays the sole arbiter of single-activation.
                const uniform = total * (after.size - incumbents.size) / after.size;
                expect(onJoiners).toBeGreaterThan(uniform);
                console.log(
                    `  cold-host draw: ${onJoiners}/${total} new activations went to the ` +
                        `${after.size - incumbents.size} joined host(s) (uniform would be ~${uniform.toFixed(1)})`
                );
            } finally {
                await kubectlAsync('scale', `deploy/${RELEASE}-host`, `--replicas=${wasReplicas}`);
                await kubectlAsync(
                    'rollout',
                    'status',
                    `deploy/${RELEASE}-host`,
                    '--timeout=300s'
                );
            }
        }, 1_200_000);
    }
);

describe.skipIf(!ready || !CHAOS)('infra: chaos — the invariants that actually break', () => {
    /** Drive steady load, run `disrupt`, and report what the callers saw. */
    async function underLoad(
        disrupt: () => Promise<void> | void,
        { seconds = 60, concurrency = 8 } = {}
    ): Promise<{
        ops: number;
        errors: Record<string, number>;
        acked: number;
        actual: number;
        duringDisruption: number;
    }> {
        const room = `chaos-${Date.now().toString(36)}`;
        const errors: Record<string, number> = {};
        let ops = 0;
        let acked = 0;
        const deadline = Date.now() + seconds * 1000;
        const workers = Array.from({ length: concurrency }, async () => {
            while (Date.now() < deadline) {
                try {
                    const res = await actorCall('Room', 'post', [room, 'chaos', 'x']);
                    if (res.ok && !(await res.json()).error) {
                        ops++;
                        acked++;
                    } else {
                        errors[String(res.status)] = (errors[String(res.status)] ?? 0) + 1;
                    }
                } catch (error) {
                    const code =
                        (error as { cause?: { code?: string } })?.cause?.code ?? 'fetch';
                    errors[code] = (errors[code] ?? 0) + 1;
                }
            }
        });
        // Let load establish before disrupting, and keep going after.
        await new Promise((r) => setTimeout(r, 5_000));
        const before = ops + Object.values(errors).reduce((a, b) => a + b, 0);
        await disrupt();
        const duringDisruption =
            ops + Object.values(errors).reduce((a, b) => a + b, 0) - before;
        await Promise.all(workers);
        // `acked` is the floor the actor's state must not fall below: an
        // acknowledged post means the CAS committed.
        const actual = (await dataOf(
            await actorCall('Room', 'recent', [room, 1_000_000])
        )) as unknown[];
        return { ops, errors, acked, actual: actual.length, duringDisruption };
    }

    it('a rolling restart under load loses no committed state', async () => {
        const result = await underLoad(async () => {
            await kubectlAsync('rollout', 'restart', `deploy/${RELEASE}-host`);
            await kubectlAsync('rollout', 'status', `deploy/${RELEASE}-host`, '--timeout=300s');
        }, { seconds: 90 });
        // Connection-level errors are expected at the edge (issue #142) —
        // what must hold is that nothing acknowledged was lost.
        expect(result.acked).toBeGreaterThan(0);
        expect(result.actual).toBeGreaterThanOrEqual(result.acked);
        // Proof the loop kept issuing THROUGH the restart rather than
        // pausing on a blocking kubectl and measuring an idle cluster.
        expect(result.duringDisruption).toBeGreaterThan(0);
        console.log(
            `  rolling restart: acked=${result.acked} actual=${result.actual} during=${result.duringDisruption} errors=${JSON.stringify(result.errors)}`
        );
    }, 600_000);

    it('a hard pod kill loses no committed state', async () => {
        const result = await underLoad(async () => {
            const victim = await kubectlAsync(
                'get', 'pods', '-l', 'app.kubernetes.io/component=host',
                '-o', 'jsonpath={.items[0].metadata.name}'
            );
            await kubectlAsync('delete', 'pod', victim, '--grace-period=0', '--force');
        }, { seconds: 120 });
        expect(result.acked).toBeGreaterThan(0);
        expect(result.actual).toBeGreaterThanOrEqual(result.acked);
        expect(result.duringDisruption).toBeGreaterThan(0);
        console.log(
            `  hard kill: acked=${result.acked} actual=${result.actual} during=${result.duringDisruption} errors=${JSON.stringify(result.errors)}`
        );
    }, 600_000);

    it('a Redis outage fences the hosts and they recover unattended', async () => {
        await kubectlAsync('scale', `deploy/${RELEASE}-redis`, '--replicas=0');
        // Past the membership TTL every host self-fences, which is now
        // FATAL (#141) — the kubelet restarts them.
        await new Promise((r) => setTimeout(r, 50_000));
        await kubectlAsync('scale', `deploy/${RELEASE}-redis`, '--replicas=1');

        const deadline = Date.now() + 240_000;
        let recovered = false;
        while (Date.now() < deadline && !recovered) {
            await new Promise((r) => setTimeout(r, 10_000));
            const res = await actorCall('Room', 'topic', ['recovery-probe']).catch(() => null);
            recovered = res?.status === 200;
        }
        // No helm, no kubectl rollout — the point is that nobody had to.
        expect(recovered).toBe(true);
    }, 600_000);
});
