/**
 * The proxy — the one piece of this example that is not a demonstration.
 *
 * It is the security boundary: everything on the browser side of it is
 * unprivileged, and everything on the other side carries a bearer token that
 * reads actor type names, traffic shape and cluster topology. So its
 * behaviour is pinned rather than eyeballed.
 *
 * Two of these fail SILENTLY if they regress — a dropped query string turns
 * the drill-down into a permanently empty panel, and a prefix match that is
 * too loose exposes a neighbouring route — which is exactly the shape of bug
 * that survives a manual click-through.
 */
import { describe, expect, it, vi } from 'vitest';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { opsProxy } from '../src/ops-proxy.ts';

const SECRET = 'demo-ops-secret';

/** A fake response that records what was written to it. */
function fakeResponse() {
    const written = { status: 0, headers: {} as Record<string, string>, body: '' };
    const response = {
        writeHead(status: number, headers?: Record<string, string>) {
            written.status = status;
            Object.assign(written.headers, headers ?? {});
            return response;
        },
        end(body?: string) {
            written.body = body ?? '';
        }
    };
    return { response: response as unknown as ServerResponse, written };
}

/** Run one request through the proxy with a stubbed upstream. */
async function call(
    url: string,
    options: {
        method?: string;
        upstream?: (input: string, init?: RequestInit) => Promise<Response>;
        isOperator?: () => boolean;
    } = {}
) {
    const fetches: { url: string; method: string | undefined; auth: string | undefined }[] = [];
    const fetchStub = vi.fn(async (input: string, init?: RequestInit) => {
        fetches.push({
            url: String(input),
            method: init?.method,
            auth: (init?.headers as Record<string, string> | undefined)?.authorization
        });
        return (
            (await options.upstream?.(String(input), init)) ??
            new Response('{"v":1}', { status: 200, headers: { 'content-type': 'application/json' } })
        );
    });
    const original = globalThis.fetch;
    globalThis.fetch = fetchStub as unknown as typeof fetch;

    const handle = opsProxy({
        host: 'http://127.0.0.1:5392',
        secret: SECRET,
        mount: '/ops',
        isOperator: options.isOperator
    });
    const { response, written } = fakeResponse();
    let nexted = false;
    try {
        await handle(
            { url, method: options.method ?? 'GET' } as IncomingMessage,
            response,
            () => {
                nexted = true;
            }
        );
    } finally {
        globalThis.fetch = original;
    }
    return { written, fetches, nexted };
}

describe('opsProxy', () => {
    it('attaches the bearer on the SERVER side', async () => {
        const { fetches } = await call('/ops');
        expect(fetches[0]?.auth).toBe(`Bearer ${SECRET}`);
    });

    it('maps its mount onto the host’s ops base', async () => {
        const { fetches } = await call('/ops');
        expect(fetches[0]?.url).toBe('http://127.0.0.1:5392/_sigx/ops');
    });

    it('preserves the sub-path AND the query string', async () => {
        // `httpSource` appends `/cluster` and `?detail=1&host=…` itself. A
        // proxy that dropped the first turns the Cluster tab into a 404; one
        // that dropped the second turns every drill-down into a permanently
        // empty "waiting for a detail poll…" — and neither says anything.
        const { fetches } = await call('/ops/cluster?detail=1&host=s.abc');
        expect(fetches[0]?.url).toBe(
            'http://127.0.0.1:5392/_sigx/ops/cluster?detail=1&host=s.abc'
        );
    });

    it('passes through anything that is not its route', async () => {
        const { nexted, fetches } = await call('/assets/index.js');
        expect(nexted).toBe(true);
        expect(fetches).toHaveLength(0);
    });

    it('does NOT swallow a route that merely starts with the same letters', async () => {
        // `startsWith('/ops')` alone would proxy `/opsummary` — and forward a
        // bearer token to a URL nobody meant to build.
        const { nexted, fetches } = await call('/opsummary');
        expect(nexted).toBe(true);
        expect(fetches).toHaveLength(0);
    });

    it('refuses a caller that is not an operator, without contacting the host', async () => {
        const { written, fetches } = await call('/ops', { isOperator: () => false });
        expect(written.status).toBe(403);
        // The token must not travel on a request that was going to be
        // rejected anyway.
        expect(fetches).toHaveLength(0);
    });

    it('forwards the host’s own status rather than flattening it', async () => {
        // A 401 from the host means the proxy's secret is stale. Rendering it
        // as a 200 of zeroes is how you spend ten minutes debugging a healthy
        // cluster.
        const { written } = await call('/ops', {
            upstream: async () => new Response('{"error":{}}', { status: 401 })
        });
        expect(written.status).toBe(401);
    });

    it('answers 502 when the cluster is unreachable, not 500', async () => {
        const { written } = await call('/ops', {
            upstream: () => Promise.reject(new Error('ECONNREFUSED'))
        });
        expect(written.status).toBe(502);
        // "the cluster is not answering" and "the portal is broken" are
        // different things to go and look at.
        expect(written.body).toContain('is the cluster running?');
    });

    it('never lets a snapshot be cached', async () => {
        const { written } = await call('/ops');
        expect(written.headers['cache-control']).toBe('no-store');
    });

    it('forwards the METHOD rather than assuming GET', async () => {
        // `ops()` serves GET and HEAD and answers 405 to anything else.
        // Rewriting every request as a GET makes a HEAD probe silently
        // expensive and hides the 405 a wrong verb should produce.
        const { fetches } = await call('/ops', { method: 'HEAD' });
        expect(fetches[0]?.method).toBe('HEAD');
    });

    it('sends no body on a HEAD, by definition', async () => {
        const { written } = await call('/ops', { method: 'HEAD' });
        expect(written.status).toBe(200);
        expect(written.body).toBe('');
    });

    it('passes through www-authenticate on a 401', async () => {
        // Flattening it to a bare status is how you spend ten minutes
        // wondering whether the PROXY's secret is stale or the host is.
        const { written } = await call('/ops', {
            upstream: async () =>
                new Response('{"error":{}}', {
                    status: 401,
                    headers: { 'www-authenticate': 'Bearer' }
                })
        });
        expect(written.headers['www-authenticate']).toBe('Bearer');
    });

    it('passes through allow on a 405', async () => {
        const { written } = await call('/ops', {
            method: 'POST',
            upstream: async () =>
                new Response('{"error":{}}', { status: 405, headers: { allow: 'GET, HEAD' } })
        });
        expect(written.headers.allow).toBe('GET, HEAD');
    });

    it('omits a header the host did not send, rather than sending an empty one', async () => {
        const { written } = await call('/ops');
        expect('www-authenticate' in written.headers).toBe(false);
        expect('allow' in written.headers).toBe(false);
    });
});
