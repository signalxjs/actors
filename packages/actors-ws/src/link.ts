/**
 * A WebSocket as a `FrameLink`.
 *
 * `messageOriented: true` — a WebSocket message already carries its own
 * length, so frames ride it WITHOUT the u32 prefix a TCP stream needs and
 * there is nothing to reassemble. That is the entire difference between this
 * transport and `@sigx/actors-tcp`; everything above the link is shared.
 *
 * Frames are always sent as BINARY. Text would force a UTF-8 round trip on a
 * payload that is already bytes, and `ws` treats the two differently on
 * receipt.
 */
import type { FrameLink } from '@sigx/actors/cluster/frames';

/**
 * The slice of the WebSocket API this needs — satisfied by both the WHATWG
 * `WebSocket` (Node 22+, and every WinterCG runtime) and by `ws`.
 */
export interface MinimalWebSocket {
    send(data: Uint8Array): void;
    close(code?: number, reason?: string): void;
    addEventListener(
        type: 'open' | 'close' | 'error' | 'message',
        listener: (event: { data?: unknown }) => void,
        options?: { once?: boolean }
    ): void;
    binaryType?: string;
    readyState?: number;
}

/** Bytes out of whatever the runtime handed us: ArrayBuffer, view, or Blob. */
async function toBytes(data: unknown): Promise<Uint8Array | null> {
    if (data instanceof Uint8Array) return data;
    if (data instanceof ArrayBuffer) return new Uint8Array(data);
    if (ArrayBuffer.isView(data)) {
        return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    }
    if (typeof (data as Blob)?.arrayBuffer === 'function') {
        return new Uint8Array(await (data as Blob).arrayBuffer());
    }
    return null;
}

export function webSocketLink(socket: MinimalWebSocket): FrameLink {
    // `ws` defaults to Buffer, browsers to Blob; ArrayBuffer is the one shape
    // both produce synchronously, which keeps frame delivery in order.
    if ('binaryType' in socket) socket.binaryType = 'arraybuffer';
    return {
        messageOriented: true,
        send(bytes) {
            socket.send(bytes);
            // A WebSocket exposes no synchronous backpressure signal, so the
            // per-stream credit window is the whole flow-control story here —
            // which is why credit is applied at the generator rather than at
            // a buffer.
            return true;
        },
        close: () => socket.close(),
        onData(cb) {
            socket.addEventListener('message', (event) => {
                void toBytes(event.data).then((bytes) => {
                    if (bytes) {
                        cb(bytes);
                        return;
                    }
                    // A message we cannot turn into bytes is a protocol
                    // error, and ignoring it is not free: a peer that only
                    // ever sends text never completes a handshake, so the
                    // connection sits pending and holds resources forever.
                    // 1003 is "unsupported data".
                    socket.close(1003, 'sigx: expected a binary frame');
                });
            });
        },
        onClose(cb) {
            socket.addEventListener('close', () => cb());
            socket.addEventListener('error', () => cb());
        },
        onDrain() {
            // No drain event to subscribe to; see `send` above.
        }
    };
}
