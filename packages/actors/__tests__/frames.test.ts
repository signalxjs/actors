/**
 * The framed host-to-host wire (`@sigx/actors/cluster/frames`): the encoder
 * writes prefix, header and body into ONE buffer (#440), so what the reader
 * recovers must be byte-for-byte what the old two-copy encoder produced —
 * for an empty payload, a small one that takes the `encodeInto` fast path,
 * a multi-byte one whose UTF-8 is longer than its UTF-16, and a large one
 * past the fast path's cap.
 */
import { describe, expect, it } from 'vitest';
import {
    FRAME_HEADER_BYTES,
    FRAME_HEADER_BYTES_UNPREFIXED,
    FrameReader,
    FrameType,
    decodeFrameBody,
    encodeFrame,
    encodeFrameBody,
    type Frame
} from '@sigx/actors/cluster/frames';

const reviver = (_key: string, value: unknown): unknown => value;

const payloads: [string, unknown][] = [
    ['empty', undefined],
    ['small', { s: 'Counter#increment', a: ['k', 1] }],
    ['multi-byte', { text: 'héllo wörld — ünïcödé ✓ 日本語 🚀', n: 1 }],
    ['large', { rows: Array.from({ length: 400 }, (_, i) => ({ id: i, name: `row-${i}`, tags: ['a', 'b'] })) }]
];

describe('frames: one-buffer encode round-trips', () => {
    for (const [label, payload] of payloads) {
        it(`${label}: body form (WebSocket) decodes to the same frame`, () => {
            const frame: Frame = { type: FrameType.CALL, flags: 1, status: 403, corrId: 0xdeadbeef, ...(payload === undefined ? {} : { payload }) };
            const body = encodeFrameBody(frame);
            expect(body.length).toBeGreaterThanOrEqual(FRAME_HEADER_BYTES_UNPREFIXED);
            const decoded = decodeFrameBody(body, reviver);
            expect(decoded).toEqual(frame);
            // The buffer handed out is exactly the frame — no slack behind it
            // for the writer to send.
            const utf8 = payload === undefined ? 0 : new TextEncoder().encode(JSON.stringify(payload)).length;
            expect(body.length).toBe(FRAME_HEADER_BYTES_UNPREFIXED + utf8);
        });

        it(`${label}: prefixed form (TCP) carries the right length and reads back through FrameReader`, () => {
            const frame: Frame = { type: FrameType.REPLY, flags: 0, status: 200, corrId: 7, ...(payload === undefined ? {} : { payload }) };
            const bytes = encodeFrame(frame);
            const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
            expect(view.getUint32(0, false)).toBe(bytes.length - 4);
            expect(bytes.length).toBeGreaterThanOrEqual(FRAME_HEADER_BYTES);
            const reader = new FrameReader(1 << 20, reviver);
            // Split across two chunks so the reader's reassembly is exercised.
            const cut = Math.min(5, bytes.length);
            reader.push(bytes.subarray(0, cut));
            expect([...reader.drain()]).toEqual([]);
            reader.push(bytes.subarray(cut));
            expect([...reader.drain()]).toEqual([frame]);
        });
    }

    it('two frames in one chunk come out as two frames', () => {
        const a: Frame = { type: FrameType.PING, flags: 0, status: 0, corrId: 1 };
        const b: Frame = { type: FrameType.CHUNK, flags: 0, status: 0, corrId: 2, payload: [1, 2, 3] };
        const ea = encodeFrame(a);
        const eb = encodeFrame(b);
        const joined = new Uint8Array(ea.length + eb.length);
        joined.set(ea, 0);
        joined.set(eb, ea.length);
        const reader = new FrameReader(1 << 20, reviver);
        reader.push(joined);
        expect([...reader.drain()]).toEqual([a, b]);
    });
});
