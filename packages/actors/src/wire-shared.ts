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

// ---------------------------------------------------------------------------
// The `$live` wire contract
//
// Here rather than in `./server/live-endpoint` because BOTH sides speak it and
// the client must never import the server: `./app`'s live channel builds the
// subscription records, the mount parses them. One definition, two importers —
// the same reason the codec and the NDJSON reader live in this module.

/** The reserved wire symbol. `defineActor` refuses `$`-prefixed types. */
export const LIVE_SYMBOL = '$live#subscribe';

/** One requested subscription. Short keys: this rides every reconnect. */
export interface LiveSubscription {
    /** Actor type. */
    t: string;
    /** Actor key. */
    k: string;
    /** Read method. */
    m: string;
    /** Arguments. */
    a?: readonly unknown[];
}

/** A frame, tagged with the subscription index it belongs to. */
export type LiveFrame =
    | { i: number; v: unknown }
    | { i: number; e: { message: string; status: number } }
    | { p: 1 };

/** Re-create a wire error with the `__sigxServerFnError` brand. */
export function wireFail(status: number, wire: WireError | undefined, message: string): Error {
    return Object.assign(new Error(wire?.message ?? message), {
        __sigxServerFnError: true,
        status: wire?.status ?? status,
        data: wire?.data !== undefined ? reviveWire(wire.data) : undefined
    });
}

/**
 * The transport-level keepalive line, emitted by the endpoint during silence.
 *
 * Deliberately its own line type rather than a `chunk`: a chunk is USER data
 * on every stream method, so any sentinel value put there could collide with
 * something an actor legitimately yields. `ping` sits beside `chunk` / `done`
 * / `error` in the envelope vocabulary, where nothing can mistake it.
 */
interface NdjsonLine {
    chunk?: unknown;
    done?: number;
    error?: WireError;
    ping?: number;
}

/**
 * Read an OK NDJSON response body: yields revived `chunk` lines, skips
 * keepalive `ping` lines, returns on the `done` terminator, re-throws in-band
 * `error` lines branded. A body that ends without a terminator (connection
 * lost) throws.
 */
export async function* readNdjson(res: Response, symbol: string): AsyncGenerator<unknown> {
    if (!res.body) {
        throw new Error(`[sigx actors] stream "${symbol}" response has no body to read`);
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    const parseLine = (text: string): NdjsonLine => JSON.parse(text, reviver) as NdjsonLine;
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
            // Keepalive: it exists to move bytes, and carries nothing.
            if ('ping' in obj) continue;
            yield reviveWire(obj.chunk);
        }
    }
    // EOF: honor a final line missing its trailing newline (proxies may
    // strip it); a partial line is genuine truncation.
    buffer += decoder.decode();
    const tail = buffer.trim();
    if (tail) {
        let obj: NdjsonLine | null = null;
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
            // A final ping is still a truncated stream — fall through to the
            // no-terminator throw below rather than yielding a keepalive as
            // if it were the last value.
            if (!('ping' in obj)) yield reviveWire(obj.chunk);
        }
    }
    throw new Error(
        `[sigx actors] stream "${symbol}" ended without a done/error terminator ` +
            `(connection lost?)`
    );
}
