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
 * rolling restart drops nothing and a killed silo loses no committed state.
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

const URL_BASE = process.env.INFRA_URL?.replace(/\/$/, '');
const AUTH_SECRET = process.env.INFRA_AUTH_SECRET;
const OPS_SECRET = process.env.INFRA_OPS_SECRET;
const CHAOS = process.env.INFRA_CHAOS === '1';
const NS = process.env.INFRA_NS ?? 'sigx-chat';
const CONTEXT = process.env.INFRA_CONTEXT ?? '';
const RELEASE = process.env.INFRA_RELEASE ?? 'chat';

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
        ['/_sigx/silo', 'the silo-to-silo mount'],
        ['/_sigx/silo/anything', 'anything under it']
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

    it('reads a write back through the cluster (any silo answers for any grain)', async () => {
        const room = `xsilo-${Date.now().toString(36)}`;
        for (let i = 0; i < 3; i++) {
            const res = await actorCall('Room', 'post', [room, 'tester', `m${i}`]);
            expect(res.status).toBe(200);
        }
        // Round-robin across silos means these reads land wherever; the
        // directory makes them agree.
        for (let i = 0; i < 5; i++) {
            const res = await actorCall('Room', 'recent', [room, 20], { route: false });
            expect(((await dataOf(res)) as unknown[]).length).toBe(3);
        }
    });

    it('streams: a quiet watch() outlives the proxy idle window', async () => {
        // ingress-nginx cuts an idle proxied response at 60s by default;
        // the chart raises it because per-actor streams send no keepalive
        // ping (issue #178). This is the test that catches a deployment
        // where that annotation was lost.
        const room = `quiet-${Date.now().toString(36)}`;
        const token = routeToken('Room', room);
        const controller = new AbortController();
        const res = await fetch(
            `${URL_BASE}/_sigx/actor/r/${token}/${encodeURIComponent('Room#watch')}`,
            {
                method: 'POST',
                headers: {
                    'content-type': 'application/json',
                    origin: URL_BASE!,
                    cookie: cookieFor('tester'),
                    'x-sigx-actor-route': token
                },
                body: JSON.stringify({ args: [room] }),
                signal: controller.signal
            }
        );
        expect(res.status).toBe(200);
        expect(res.headers.get('content-type')).toMatch(/ndjson/);

        const reader = res.body!.getReader();
        // The first chunk is the `{ initial: true }` snapshot.
        const first = await reader.read();
        expect(first.done).toBe(false);

        const idleMs = Number(process.env.INFRA_STREAM_IDLE_MS ?? 75_000);
        // Hold it open with no traffic, then prove it still delivers.
        await new Promise((r) => setTimeout(r, idleMs));
        await actorCall('Room', 'post', [room, 'tester', 'after the quiet window']);
        const next = await reader.read();
        expect(next.done).toBe(false);
        expect(new TextDecoder().decode(next.value)).toContain('after the quiet window');
        controller.abort();
    }, 180_000);
});

describe.skipIf(!ready || !OPS_SECRET)('infra: ops reports a healthy cluster', () => {
    // Reached through a port-forward, never the public host — see the
    // sealed-surface suite above.
    const opsUrl = process.env.INFRA_OPS_URL ?? 'http://127.0.0.1:7399';

    it('every silo is active and the view agrees on its size', async () => {
        const res = await fetch(`${opsUrl}/_sigx/ops/cluster`, {
            headers: { authorization: `Bearer ${OPS_SECRET}` }
        });
        expect(res.status).toBe(200);
        const body = (await res.json()) as {
            view: { size: number; active: number };
            silos: { status: string }[];
        };
        expect(body.view.active).toBe(body.view.size);
        expect(body.silos.every((s) => s.status === 'active')).toBe(true);
    });

    it('rejects an unauthenticated ops read', async () => {
        expect((await fetch(`${opsUrl}/_sigx/ops`)).status).toBe(401);
    });
});

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
            await kubectlAsync('rollout', 'restart', `deploy/${RELEASE}-silo`);
            await kubectlAsync('rollout', 'status', `deploy/${RELEASE}-silo`, '--timeout=300s');
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
                'get', 'pods', '-l', 'app.kubernetes.io/component=silo',
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

    it('a Redis outage fences the silos and they recover unattended', async () => {
        await kubectlAsync('scale', `deploy/${RELEASE}-redis`, '--replicas=0');
        // Past the membership TTL every silo self-fences, which is now
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
