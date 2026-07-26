/**
 * Wire plumbing shared by the public client entry and (soon) the cluster's
 * silo-to-silo transport: codec-seam accessors, the pollution-safe JSON
 * reviver, branded wire-error re-creation, and the NDJSON stream reader.
 * Internal — exported from no entry point; the wire contract itself stays
 * pinned by the client/server integration tests.
 */
import {
    encodeWithHandlers,
    reviveWithHandlers,
    type TypeHandler
} from '@sigx/serialize';

// Codec binding — the same `__SIGX_SERVERFN_CODEC__` seam the serverFn wire
// uses (documented in core's docs/seams.md). Actor payloads share the
// serverFn vocabulary by design: one `serverPlugin({ types })` registration
// covers both wires.
function extraHandlers(): readonly TypeHandler[] {
    const extra = (globalThis as { __SIGX_SERVERFN_CODEC__?: TypeHandler[] })
        .__SIGX_SERVERFN_CODEC__;
    return Array.isArray(extra) ? extra : [];
}

export const encodeWire = (value: unknown): unknown => encodeWithHandlers(value, extraHandlers());
export const reviveWire = (value: unknown): unknown => reviveWithHandlers(value, extraHandlers());

/** Prototype-pollution keys DROPPED from every parsed payload. */
const DANGEROUS_KEYS = new Set(['__proto__', 'constructor', 'prototype']);
export const reviver = (key: string, value: unknown): unknown =>
    DANGEROUS_KEYS.has(key) ? undefined : value;

export interface WireError {
    message?: string;
    status?: number;
    data?: unknown;
}

/** Re-create a wire error with the `__sigxServerFnError` brand. */
export function wireFail(status: number, wire: WireError | undefined, message: string): Error {
    return Object.assign(new Error(wire?.message ?? message), {
        __sigxServerFnError: true,
        status: wire?.status ?? status,
        data: wire?.data !== undefined ? reviveWire(wire.data) : undefined
    });
}

/**
 * Read an OK NDJSON response body: yields revived `chunk` lines, returns on
 * the `done` terminator, re-throws in-band `error` lines branded. A body
 * that ends without a terminator (connection lost) throws.
 */
export async function* readNdjson(res: Response, symbol: string): AsyncGenerator<unknown> {
    if (!res.body) {
        throw new Error(`[sigx actors] stream "${symbol}" response has no body to read`);
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    const parseLine = (text: string): { chunk?: unknown; done?: number; error?: WireError } =>
        JSON.parse(text, reviver) as { chunk?: unknown; done?: number; error?: WireError };
    for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let nl;
        while ((nl = buffer.indexOf('\n')) >= 0) {
            const text = buffer.slice(0, nl);
            buffer = buffer.slice(nl + 1);
            if (!text) continue;
            const obj = parseLine(text);
            if ('error' in obj) {
                throw wireFail(500, obj.error, `[sigx actors] stream "${symbol}" failed`);
            }
            if ('done' in obj) return;
            yield reviveWire(obj.chunk);
        }
    }
    // EOF: honor a final line missing its trailing newline (proxies may
    // strip it); a partial line is genuine truncation.
    buffer += decoder.decode();
    const tail = buffer.trim();
    if (tail) {
        let obj: { chunk?: unknown; done?: number; error?: WireError } | null = null;
        try {
            obj = parseLine(tail);
        } catch {
            obj = null;
        }
        if (obj) {
            if ('error' in obj) {
                throw wireFail(500, obj.error, `[sigx actors] stream "${symbol}" failed`);
            }
            if ('done' in obj) return;
            yield reviveWire(obj.chunk);
        }
    }
    throw new Error(
        `[sigx actors] stream "${symbol}" ended without a done/error terminator ` +
            `(connection lost?)`
    );
}
