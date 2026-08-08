/**
 * `@sigx/actors/cluster/frames` — the wire shared by every
 * connection-oriented host transport: the frame codec below, plus the
 * `HostConnection` state machine re-exported at the bottom.
 *
 * It lives in the core package rather than in the transport because
 * `wire-shared.ts` is internal — an out-of-repo package cannot reach it, and
 * this subpath is published precisely so out-of-repo transports can build on
 * it. One implementation beats a copy per transport that drifts.
 *
 * Transports differ ONLY in their `FrameLink`: a `node:net` socket
 * (`@sigx/actors-tcp`) is a byte stream and needs the length prefix; a
 * message-oriented link (`messageOriented: true`, e.g. a WebSocket) already
 * delivers whole messages and does not. Multiplexing, cancellation, credit
 * and error mapping are the same code for every link.
 *
 * ## The frame
 *
 * ```
 * 0  u32 length   bytes AFTER this field (>= 8)
 * 4  u8  type
 * 5  u8  flags    bit0 = stream call
 * 6  u16 status   HTTP-shaped code; 0 except on ERROR/GOAWAY
 * 8  u32 corrId
 * 12 ..  payload  UTF-8 JSON, through the wire codec
 * ```
 *
 * Big-endian because it reads correctly in a hexdump and `readUInt32BE`
 * costs nothing extra.
 *
 * **`status` is a header field on purpose.** It looks like an HTTP detail and
 * is not: `clusterStats` classifies peer failures by numeric status (403 →
 * `unauthorized`, 404 → `unsupported`), so a transport that drops the number
 * degrades every auth failure to a bare `'error'` in the ops surface.
 *
 * **The payload stays JSON.** `wire/endpoint-roundtrip` is already 115 k ops/s
 * with the codec and JSON and no socket at all, so a second serialization
 * format buys very little while forking the vocabulary `wire-shared.ts` pins
 * — including its prototype-pollution reviver.
 *
 * WebSocket reuses this layout **minus the u32 length**, since a WS frame
 * already carries its own length. `encodeFrame` therefore emits the header
 * and body separately from the length prefix.
 */
import { parseWireWith } from '../wire-parse';

export const FRAME_HEADER_BYTES = 12;
/** Header size once the length prefix is stripped (the WebSocket form). */
export const FRAME_HEADER_BYTES_UNPREFIXED = 8;

/** Wire protocol version, carried in the handshake. */
export const FRAME_PROTO = 1;

export const FrameType = {
    /** Dialer → acceptor: identity + protocol version. */
    HELLO: 0x01,
    /** Acceptor → dialer: accepted, here is mine. */
    WELCOME: 0x02,
    /** Invoke a method. `flags` bit0 marks a stream call. */
    CALL: 0x10,
    /** Unary result. */
    REPLY: 0x11,
    /** One stream chunk. */
    CHUNK: 0x12,
    /** Stream finished normally — the terminator, never an EOF. */
    END: 0x13,
    /** Failure, carrying the wire-shaped error and its status. */
    ERROR: 0x14,
    /** Consumer gave up on a stream. NOT the socket closing. */
    CANCEL: 0x15,
    /** Flow control: the consumer will take `n` more chunks. */
    CREDIT: 0x16,
    PING: 0x20,
    PONG: 0x21,
    /** Connection is going away; `status` says why. */
    GOAWAY: 0x7f
} as const;

export type FrameTypeValue = (typeof FrameType)[keyof typeof FrameType];

export const FLAG_STREAM = 0x01;

export interface Frame {
    type: number;
    flags: number;
    status: number;
    corrId: number;
    /** Decoded payload, or undefined when the frame carries none. */
    payload?: unknown;
}

/**
 * Encode one frame WITHOUT the length prefix — the WebSocket form, and the
 * body a TCP writer prefixes.
 */
export function encodeFrameBody(frame: Frame): Uint8Array {
    const json = frame.payload === undefined ? '' : JSON.stringify(frame.payload);
    const body = json === '' ? new Uint8Array(0) : new TextEncoder().encode(json);
    const out = new Uint8Array(FRAME_HEADER_BYTES_UNPREFIXED + body.length);
    const view = new DataView(out.buffer);
    out[0] = frame.type;
    out[1] = frame.flags;
    view.setUint16(2, frame.status, false);
    view.setUint32(4, frame.corrId, false);
    out.set(body, FRAME_HEADER_BYTES_UNPREFIXED);
    return out;
}

/** Encode one frame WITH the u32 length prefix — the TCP form. */
export function encodeFrame(frame: Frame): Uint8Array {
    const body = encodeFrameBody(frame);
    const out = new Uint8Array(4 + body.length);
    new DataView(out.buffer).setUint32(0, body.length, false);
    out.set(body, 4);
    return out;
}

/**
 * Decode a length-prefix-free frame body. `reviver` is the codec's
 * prototype-pollution-safe reviver — a payload is never parsed without its
 * protection. Under the default reviver `parseWireWith` takes the guarded
 * fast path (pre-scan, then plain `JSON.parse` — #218); a custom codec
 * reviver runs on every node, always.
 */
export function decodeFrameBody(
    body: Uint8Array,
    reviver: (key: string, value: unknown) => unknown
): Frame {
    if (body.length < FRAME_HEADER_BYTES_UNPREFIXED) {
        throw new Error(`[sigx actors] short frame: ${body.length} bytes`);
    }
    const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
    const frame: Frame = {
        type: body[0] as number,
        flags: body[1] as number,
        status: view.getUint16(2, false),
        corrId: view.getUint32(4, false)
    };
    if (body.length > FRAME_HEADER_BYTES_UNPREFIXED) {
        const text = new TextDecoder().decode(body.subarray(FRAME_HEADER_BYTES_UNPREFIXED));
        frame.payload = parseWireWith<unknown>(text, reviver);
    }
    return frame;
}

/**
 * Incremental reader for a byte stream.
 *
 * The length prefix is checked against `maxFrameBytes` **before a single
 * payload byte is buffered**, so an oversized or hostile frame cannot make
 * the receiver allocate. There is no resynchronising past a frame we refused
 * to count, so that case is terminal for the connection — the caller is
 * expected to GOAWAY and close.
 */
export class FrameReader {
    #chunks: Uint8Array[] = [];
    #buffered = 0;
    readonly #max: number;
    readonly #reviver: (key: string, value: unknown) => unknown;

    constructor(maxFrameBytes: number, reviver: (key: string, value: unknown) => unknown) {
        this.#max = maxFrameBytes;
        this.#reviver = reviver;
    }

    push(chunk: Uint8Array): void {
        this.#chunks.push(chunk);
        this.#buffered += chunk.length;
    }

    /**
     * Pull every complete frame currently buffered. Throws on an oversized
     * frame, which the caller must treat as fatal for that connection.
     */
    *drain(): Generator<Frame> {
        for (;;) {
            if (this.#buffered < 4) return;
            const header = this.#peek(4);
            const length = new DataView(
                header.buffer,
                header.byteOffset,
                header.byteLength
            ).getUint32(0, false);
            if (length > this.#max) {
                // Tagged so a transport can answer 413 rather than lumping it
                // in with "these bytes were not a frame", which is 400.
                throw Object.assign(
                    new Error(
                        `[sigx actors] frame of ${length} bytes exceeds the ${this.#max}-byte ` +
                            `cap — refusing before allocating; this connection cannot ` +
                            `resynchronise`
                    ),
                    { code: 'FRAME_TOO_LARGE' as const }
                );
            }
            if (this.#buffered < 4 + length) return;
            this.#skip(4);
            const body = this.#take(length);
            yield decodeFrameBody(body, this.#reviver);
        }
    }

    #peek(n: number): Uint8Array {
        const first = this.#chunks[0] as Uint8Array;
        if (first.length >= n) return first.subarray(0, n);
        this.#coalesce(n);
        return (this.#chunks[0] as Uint8Array).subarray(0, n);
    }

    #coalesce(n: number): void {
        const merged = new Uint8Array(Math.max(n, this.#buffered));
        let offset = 0;
        for (const chunk of this.#chunks) {
            merged.set(chunk, offset);
            offset += chunk.length;
        }
        this.#chunks = [merged.subarray(0, offset)];
    }

    #skip(n: number): void {
        let remaining = n;
        while (remaining > 0) {
            const first = this.#chunks[0] as Uint8Array;
            if (first.length <= remaining) {
                remaining -= first.length;
                this.#buffered -= first.length;
                this.#chunks.shift();
            } else {
                this.#chunks[0] = first.subarray(remaining);
                this.#buffered -= remaining;
                remaining = 0;
            }
        }
    }

    #take(n: number): Uint8Array {
        if (n === 0) return new Uint8Array(0);
        const first = this.#chunks[0] as Uint8Array;
        if (first.length >= n) {
            const out = first.subarray(0, n);
            this.#skip(n);
            // `subarray` aliases the buffer we are about to drop a reference
            // to; copy so a retained frame cannot pin the whole read buffer.
            return out.slice();
        }
        this.#coalesce(n);
        return this.#take(n);
    }
}

// The connection state machine rides this codec, and both the TCP and
// WebSocket transports need it — re-exported here so a transport package
// imports one entry rather than reaching into internals.
export {
    HostConnection,
    DEFAULT_CREDIT,
    DEFAULT_MAX_FRAME_BYTES,
    type ConnectionOptions,
    type FrameLink
} from './connection';
