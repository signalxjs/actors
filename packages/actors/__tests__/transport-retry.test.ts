/**
 * #55 — `fetchTransport` retries a PRE-RESPONSE connection failure once.
 *
 * The race is structural: a client writes onto a pooled keep-alive socket in
 * the same instant an exiting server retires it, and no server-side drain can
 * win that alone. A rejection of the `fetch()` await itself — before any
 * `Response` existed — is provably pre-dispatch, so one retry cannot
 * double-execute a turn. Everything AFTER a Response arrives (an HTTP status,
 * a mid-body NDJSON failure) may have executed and is never retried; an
 * abort is the caller's decision and is never retried either.
 */
import { describe, expect, it, vi } from 'vitest';
import { fetchTransport } from '@sigx/actors/client';

const ENDPOINT = 'http://actors.test/_sigx/actor';

function dataResponse(data: unknown): Response {
    return new Response(JSON.stringify({ data }), { status: 200 });
}

function ndjsonResponse(lines: string[]): Response {
    const encoder = new TextEncoder();
    const body = new ReadableStream<Uint8Array>({
        start(controller) {
            for (const line of lines) controller.enqueue(encoder.encode(`${line}\n`));
            controller.close();
        }
    });
    return new Response(body, { status: 200 });
}

/** Node's undici shape: an opaque TypeError with the real code on `cause`. */
function undiciError(code: string): TypeError {
    return new TypeError('fetch failed', { cause: Object.assign(new Error(code), { code }) });
}

async function collect(iterable: AsyncIterable<unknown>): Promise<unknown[]> {
    const out: unknown[] = [];
    for await (const value of iterable) out.push(value);
    return out;
}

describe('pre-response connection retry (#55)', () => {
    it('retries a rejected fetch exactly once, and the call resolves', async () => {
        const fetchSpy = vi
            .fn<typeof globalThis.fetch>()
            .mockRejectedValueOnce(undiciError('ECONNRESET'))
            .mockResolvedValueOnce(dataResponse(7));
        const transport = fetchTransport({ endpoint: ENDPOINT, fetch: fetchSpy });

        await expect(transport.call('Cart#add', ['c1', 'apple'])).resolves.toBe(7);
        expect(fetchSpy).toHaveBeenCalledTimes(2);
        // The retry re-sends the SAME request.
        expect(String(fetchSpy.mock.calls[1][0])).toBe(String(fetchSpy.mock.calls[0][0]));
    });

    it('retries the opaque browser TypeError (no code anywhere)', async () => {
        const fetchSpy = vi
            .fn<typeof globalThis.fetch>()
            .mockRejectedValueOnce(new TypeError('Failed to fetch'))
            .mockResolvedValueOnce(dataResponse('ok'));
        const transport = fetchTransport({ endpoint: ENDPOINT, fetch: fetchSpy });

        await expect(transport.call('Cart#total', ['c1'])).resolves.toBe('ok');
        expect(fetchSpy).toHaveBeenCalledTimes(2);
    });

    it('retries a raw connection error carrying its code directly', async () => {
        const fetchSpy = vi
            .fn<typeof globalThis.fetch>()
            .mockRejectedValueOnce(Object.assign(new Error('socket hang up'), { code: 'UND_ERR_SOCKET' }))
            .mockResolvedValueOnce(dataResponse(1));
        const transport = fetchTransport({ endpoint: ENDPOINT, fetch: fetchSpy });

        await expect(transport.call('Cart#add', ['c1', 'fig'])).resolves.toBe(1);
        expect(fetchSpy).toHaveBeenCalledTimes(2);
    });

    it('a second rejection propagates — exactly one retry, never two', async () => {
        const fetchSpy = vi
            .fn<typeof globalThis.fetch>()
            .mockRejectedValue(undiciError('ECONNREFUSED'));
        const transport = fetchTransport({ endpoint: ENDPOINT, fetch: fetchSpy });

        await expect(transport.call('Cart#add', ['c1', 'pear'])).rejects.toThrow('fetch failed');
        expect(fetchSpy).toHaveBeenCalledTimes(2);
    });

    it('a non-connection rejection is not retried', async () => {
        const fetchSpy = vi
            .fn<typeof globalThis.fetch>()
            .mockRejectedValue(new Error('token factory exploded'));
        const transport = fetchTransport({ endpoint: ENDPOINT, fetch: fetchSpy });

        await expect(transport.call('Cart#add', ['c1'])).rejects.toThrow('token factory exploded');
        expect(fetchSpy).toHaveBeenCalledTimes(1);
    });

    it('a Response that arrived is never retried, whatever the status', async () => {
        const fetchSpy = vi
            .fn<typeof globalThis.fetch>()
            .mockResolvedValue(new Response(JSON.stringify({ error: { message: 'boom' } }), { status: 500 }));
        const transport = fetchTransport({ endpoint: ENDPOINT, fetch: fetchSpy });

        await expect(transport.call('Cart#add', ['c1'])).rejects.toThrow();
        expect(fetchSpy).toHaveBeenCalledTimes(1);
    });

    it('a mid-body NDJSON failure is not retried — the turn may have executed', async () => {
        const encoder = new TextEncoder();
        const body = new ReadableStream<Uint8Array>({
            start(controller) {
                controller.enqueue(encoder.encode('{"chunk":1}\n'));
                controller.error(new TypeError('terminated'));
            }
        });
        const fetchSpy = vi
            .fn<typeof globalThis.fetch>()
            .mockResolvedValue(new Response(body, { status: 200 }));
        const transport = fetchTransport({ endpoint: ENDPOINT, fetch: fetchSpy });

        await expect(collect(transport.stream('Cart#watch', ['c1']))).rejects.toThrow();
        expect(fetchSpy).toHaveBeenCalledTimes(1);
    });

    it("the stream() path's INITIAL dispatch is retried like a call", async () => {
        const fetchSpy = vi
            .fn<typeof globalThis.fetch>()
            .mockRejectedValueOnce(undiciError('EPIPE'))
            .mockResolvedValueOnce(ndjsonResponse(['{"chunk":1}', '{"chunk":2}', '{"done":true}']));
        const transport = fetchTransport({ endpoint: ENDPOINT, fetch: fetchSpy });

        await expect(collect(transport.stream('Cart#watch', ['c1']))).resolves.toEqual([1, 2]);
        expect(fetchSpy).toHaveBeenCalledTimes(2);
    });

    it('an aborted call is not retried, even when the abort reason looks like a connection error', async () => {
        const controller = new AbortController();
        const fetchSpy = vi.fn<typeof globalThis.fetch>(
            (_url, init) =>
                new Promise<Response>((_resolve, reject) => {
                    init?.signal?.addEventListener('abort', () => reject(init.signal!.reason));
                })
        );
        const transport = fetchTransport({ endpoint: ENDPOINT, fetch: fetchSpy });

        const pending = transport.call('Cart#add', ['c1'], { signal: controller.signal });
        // A TypeError reason would match the browser conn-error shape — the
        // aborted signal must win.
        controller.abort(new TypeError('aborted mid-flight'));
        await expect(pending).rejects.toThrow('aborted mid-flight');
        expect(fetchSpy).toHaveBeenCalledTimes(1);
    });

    it('a plain abort (AbortError) is not retried either', async () => {
        const controller = new AbortController();
        const fetchSpy = vi.fn<typeof globalThis.fetch>(
            (_url, init) =>
                new Promise<Response>((_resolve, reject) => {
                    init?.signal?.addEventListener('abort', () => reject(init.signal!.reason));
                })
        );
        const transport = fetchTransport({ endpoint: ENDPOINT, fetch: fetchSpy });

        const pending = transport.call('Cart#add', ['c1'], { signal: controller.signal });
        controller.abort();
        await expect(pending).rejects.toThrow();
        expect(fetchSpy).toHaveBeenCalledTimes(1);
    });

    it('retryConnectionErrors: false opts out entirely', async () => {
        const fetchSpy = vi
            .fn<typeof globalThis.fetch>()
            .mockRejectedValueOnce(undiciError('ECONNRESET'))
            .mockResolvedValueOnce(dataResponse(7));
        const transport = fetchTransport({
            endpoint: ENDPOINT,
            fetch: fetchSpy,
            retryConnectionErrors: false
        });

        await expect(transport.call('Cart#add', ['c1'])).rejects.toThrow('fetch failed');
        expect(fetchSpy).toHaveBeenCalledTimes(1);
    });
});
