/**
 * `internal: true` (#74) — a server-internal actor the public wire never
 * serves.
 *
 * The common case an app was reaching for `allowAnonymous: true` to express
 * was "this type is only ever called in-process or host-to-host", and the
 * only line of defense was a consumer-side bearer wrapper around the mount.
 * The flag says it once, on the definition, and every PUBLIC entry point —
 * the HTTP mount, the `$live` multiplex, the socket session — answers
 * exactly as it does for a type that does not exist. Anything else confirms
 * the type's existence to whoever is probing.
 *
 * The in-process client and the HMAC-authenticated host-to-host mount keep
 * serving it: that is the whole point of the flag.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor, defineWorker, type Host } from '@sigx/actors';
import { defineJob } from '@sigx/actors/job';
import { createHost } from '@sigx/actors/host';
import { createActorSocketSession, handleActorRequest } from '@sigx/actors/server';
import type { ActorSocketSession } from '@sigx/actors/server';
import type { SocketReply } from '@sigx/actors/socket-wire';
import {
    encodeEnvelope,
    handleHostRequestForRuntime,
    HOST_CALL_HEADER,
    hostEndpointRuntime,
    resolveHostSymbol
} from '@sigx/actors/cluster';

const ENDPOINT = 'http://actors.test/_sigx/actor';
const HOST_BASE = '/_sigx/host';
const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

/** Anonymous on purpose: the refusal must hold with no policy in the way. */
const Ledger = defineActor({
    type: 'Ledger',
    internal: true,
    allowAnonymous: true,
    // A declared read, so the GET path has something it WOULD admit.
    reads: { peek: { maxAge: 5 } },
    state: () => ({ n: 0 }),
    methods: (ctx) => ({
        async bump() {
            ctx.state.n += 1;
            await ctx.save();
            return ctx.state.n;
        },
        async peek() {
            return ctx.state.n;
        }
    })
});

const Front = defineActor({
    type: 'Front',
    allowAnonymous: true,
    state: () => ({}),
    methods: (ctx) => ({
        async viaLedger(key: string) {
            return ctx.actor(Ledger, key).bump();
        },
        async ping() {
            return 'pong';
        }
    })
});

const running: Host[] = [];
const sessions: ActorSocketSession[] = [];

async function startHost(): Promise<Host> {
    const host = createHost({ actors: [Ledger, Front], defaults: quiet });
    await host.start();
    running.push(host);
    return host;
}

function post(host: Host, symbol: string, args: readonly unknown[]): Promise<Response> {
    return handleActorRequest(
        new Request(`${ENDPOINT}/${encodeURIComponent(symbol)}`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args })
        }),
        { host, origin: false }
    );
}

/** A GET straight into the endpoint, args in the query like the proxy sends. */
function get(host: Host, symbol: string, args: readonly unknown[]): Promise<Response> {
    const query = encodeURIComponent(JSON.stringify(args));
    return handleActorRequest(
        new Request(`${ENDPOINT}/${encodeURIComponent(symbol)}?args=${query}`, {
            method: 'GET'
        }),
        { host, origin: false }
    );
}

/** Read `count` NDJSON frames off a `$live` response, then cancel it. */
async function readFrames(response: Response, count: number): Promise<unknown[]> {
    const reader = response.body!.getReader();
    const decoder = new TextDecoder();
    const frames: unknown[] = [];
    let buffer = '';
    while (frames.length < count) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let nl;
        while ((nl = buffer.indexOf('\n')) >= 0) {
            const line = buffer.slice(0, nl);
            buffer = buffer.slice(nl + 1);
            // Each NDJSON line is core's stream envelope; the live frame is
            // its `chunk`.
            if (line) frames.push((JSON.parse(line) as { chunk: unknown }).chunk);
        }
    }
    await reader.cancel();
    return frames;
}

interface FakeLink {
    sent: SocketReply[];
    send(message: string): void;
    close(code: number, reason: string): void;
}

async function connect(host: Host): Promise<{ session: ActorSocketSession; link: FakeLink }> {
    const link: FakeLink = {
        sent: [],
        send(message) {
            link.sent.push(JSON.parse(message) as SocketReply);
        },
        close() {}
    };
    const session = await createActorSocketSession({
        host,
        request: new Request('http://actors.test/socket'),
        send: link.send,
        close: link.close,
        origin: false,
        pingMs: 0
    });
    sessions.push(session);
    return { session, link };
}

async function until(predicate: () => boolean, ms = 2000): Promise<void> {
    const started = Date.now();
    while (!predicate()) {
        if (Date.now() - started > ms) throw new Error('timed out waiting');
        await new Promise((resolve) => setTimeout(resolve, 5));
    }
}

const framesFor = (link: FakeLink, id: number): SocketReply[] =>
    link.sent.filter((f) => 'i' in f && f.i === id);

function hostCall(symbol: string, args: readonly unknown[]): Request {
    return new Request(`https://host.invalid${HOST_BASE}/${encodeURIComponent(symbol)}`, {
        method: 'POST',
        headers: {
            'content-type': 'application/json',
            [HOST_CALL_HEADER]: encodeEnvelope({ callChain: [], callId: 'c1.test.0' }, 'origin')
        },
        body: JSON.stringify({ args })
    });
}

afterEach(async () => {
    for (const s of sessions.splice(0)) s.close();
    for (const host of running.splice(0)) await host.stop({ timeoutMs: 1000 });
});

describe('internal actors on the public HTTP mount', () => {
    it('refuses the call with a 404', async () => {
        const host = await startHost();
        const response = await post(host, 'Ledger#bump', ['k']);
        expect(response.status).toBe(404);
        // Nothing ran: the state a later in-process read sees is untouched.
        expect(await host.actor(Ledger, 'k').peek()).toBe(0);
    });

    it('is indistinguishable from a type that does not exist', async () => {
        const host = await startHost();
        const internal = await post(host, 'Ledger#bump', ['k']);
        const missing = await post(host, 'Nope#bump', ['k']);
        expect(internal.status).toBe(missing.status);
        const internalBody = (await internal.json()) as { error: { kind: string; message: string } };
        const missingBody = (await missing.json()) as { error: { kind: string; message: string } };
        expect(internalBody.error.kind).toBe(missingBody.error.kind);
        // Same wording, differing only in the type name it names.
        expect(internalBody.error.message.replaceAll('Ledger', 'Nope')).toBe(
            missingBody.error.message
        );
    });

    it('refuses a declared GET read exactly as it does an unknown type', async () => {
        // `reads: { peek }` would make `Ledger#peek` a GET target if the
        // resolver ever synthesized it. It does not: the not-found wrapper
        // is what core's GET admission sees, and it is not GET-capable, so
        // BOTH answer core's own 405 — status, `Allow` and body byte-equal,
        // and no `max-age` that would betray a declared read behind it.
        const host = await startHost();
        const internal = await get(host, 'Ledger#peek', ['k']);
        const missing = await get(host, 'Nope#peek', ['k']);
        expect(internal.status).not.toBe(200);
        expect(internal.status).toBe(missing.status);
        expect(internal.headers.get('allow')).toBe(missing.headers.get('allow'));
        expect(internal.headers.get('cache-control')).toBe(missing.headers.get('cache-control'));
        expect(internal.headers.get('cache-control') ?? '').not.toContain('max-age');
        expect(await internal.text()).toBe(await missing.text());
        expect(await host.actor(Ledger, 'k').peek()).toBe(0);
    });

    it('still answers a public actor on the same host', async () => {
        const host = await startHost();
        const response = await post(host, 'Front#ping', ['k']);
        expect(response.status).toBe(200);
        expect(((await response.json()) as { data: string }).data).toBe('pong');
    });
});

describe('internal actors on the $live mount', () => {
    it('refuses the subscription with a 404 frame, and only that one', async () => {
        const host = await startHost();
        const response = await handleActorRequest(
            new Request(`${ENDPOINT}/${encodeURIComponent('$live#subscribe')}`, {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({
                    args: [
                        [
                            { t: 'Ledger', k: 'a', m: 'peek' },
                            { t: 'Front', k: 'b', m: 'ping' }
                        ]
                    ]
                })
            }),
            { host, origin: false }
        );
        expect(response.status).toBe(200);
        const frames = (await readFrames(response, 2)) as Array<{
            i: number;
            e?: { status: number };
            v?: unknown;
        }>;
        expect(frames.find((f) => f.i === 0)?.e?.status).toBe(404);
        expect(frames).toContainEqual({ i: 1, v: 'pong' });
    });
});

describe('internal actors on the socket session', () => {
    it('refuses a unary call with a 404 frame, like an unknown type', async () => {
        const host = await startHost();
        const { session, link } = await connect(host);
        session.handle(JSON.stringify({ i: 1, s: 'Ledger#bump', a: ['k'] }));
        session.handle(JSON.stringify({ i: 2, s: 'Nope#bump', a: ['k'] }));
        await until(() => framesFor(link, 1).length === 1 && framesFor(link, 2).length === 1);
        type Refusal = { e: { status: number; kind?: string; message: string } };
        const internal = framesFor(link, 1)[0] as Refusal;
        const missing = framesFor(link, 2)[0] as Refusal;
        expect(internal.e.status).toBe(404);
        expect(missing.e.status).toBe(404);
        // Parity on the socket too, not just the status: same kind, same
        // wording, differing only in the type name it names.
        expect(internal.e.kind).toBe(missing.e.kind);
        expect(internal.e.message.replaceAll('Ledger', 'Nope')).toBe(missing.e.message);
        expect(await host.actor(Ledger, 'k').peek()).toBe(0);
    });

    it('refuses a subscription with a 404 frame', async () => {
        const host = await startHost();
        const { session, link } = await connect(host);
        session.handle(JSON.stringify({ i: 3, sub: { t: 'Ledger', k: 'a', m: 'peek' } }));
        await until(() => framesFor(link, 3).length === 1);
        expect((framesFor(link, 3)[0] as { e: { status: number } }).e.status).toBe(404);
    });
});

describe('internal actors stay reachable where they should', () => {
    it('in-process, directly and through ctx.actor()', async () => {
        const host = await startHost();
        expect(await host.actor(Ledger, 'k').bump()).toBe(1);
        expect(await host.actor(Front, 'f').viaLedger('k')).toBe(2);
    });

    it('over the authenticated host-to-host mount', async () => {
        const host = await startHost();
        expect(await resolveHostSymbol(host, 'Ledger#bump')).toEqual({
            type: 'Ledger',
            method: 'bump',
            mode: 'unary'
        });
        const res = await handleHostRequestForRuntime(hostCall('Ledger#bump', ['k']), {
            runtime: hostEndpointRuntime(host)
        });
        expect(res.status).toBe(200);
        expect((await res.json()).data).toBe(1);
    });
});

describe('the flag threads through defineWorker and defineJob', () => {
    it('a worker', () => {
        const Digest = defineWorker({
            type: 'Digest',
            internal: true,
            allowAnonymous: true,
            methods: () => ({ async run() {} })
        });
        expect(Digest.__sigxActor.internal).toBe(true);
    });

    it('a job', () => {
        const Nightly = defineJob({
            type: 'Nightly',
            internal: true,
            allowAnonymous: true,
            run: async () => 'done'
        });
        expect(Nightly.__sigxActor.internal).toBe(true);
    });

    it('is absent, not false, when undeclared', () => {
        const Plain = defineWorker({
            type: 'Plain',
            allowAnonymous: true,
            methods: () => ({ async run() {} })
        });
        expect('internal' in Plain.__sigxActor).toBe(false);
    });
});
