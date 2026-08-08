/**
 * `@sigx/actors-ws` — the client-facing WebSocket transport for
 * `@sigx/actors` (#99). Runtime-neutral re-exports; the implementation
 * lives on `./client`, which is WinterCG-clean.
 */
export {
    socketTransport,
    type SocketHandlers,
    type SocketLink,
    type SocketTransportOptions
} from './client';
