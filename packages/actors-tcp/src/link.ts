/**
 * A `node:net` socket as a `FrameLink`.
 *
 * A TCP stream has no message boundaries, so frames ride it length-prefixed
 * (`messageOriented: false`) and the shared `FrameReader` finds the edges.
 * `setNoDelay` matters more here than the framing does: Nagle would add up to
 * 40 ms to a small unary call, which is far more than anything the wire format
 * saves.
 */
import type { Socket } from 'node:net';
import type { FrameLink } from '@sigx/actors/cluster/frames';

export function socketLink(socket: Socket): FrameLink {
    socket.setNoDelay(true);
    return {
        messageOriented: false,
        // A destroyed socket takes a write silently and errors on the next
        // tick; throwing here instead lets the connection classify the call
        // as provably unsent — retryable — rather than "outcome unknown".
        send: (bytes) => {
            if (socket.destroyed) throw new Error('socket destroyed');
            return socket.write(bytes);
        },
        close: () => socket.destroy(),
        onData: (cb) =>
            void socket.on('data', (chunk: Buffer) =>
                cb(new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength))
            ),
        onClose: (cb) => {
            socket.on('close', cb);
            socket.on('error', cb);
        },
        onDrain: (cb) => void socket.on('drain', cb)
    };
}
