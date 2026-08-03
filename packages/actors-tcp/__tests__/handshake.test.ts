/**
 * @vitest-environment node
 *
 * Inbound connections must not be able to sit in the pending set forever.
 *
 * The listener adopts every socket, adds it to `pendingInbound` and sends
 * WELCOME. That set is drained on HELLO, on close, or at `stop()` — so a peer
 * that connects and never says HELLO held a file descriptor and a
 * `maxFrameBytes`-capable read buffer until the host shut down. No timer
 * existed anywhere on the path.
 *
 * The transport binds all interfaces by default and has no TLS, so "the peer
 * is trusted" is not an assumption the listener itself can make.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { createServer, type Server } from 'node:http';
import { connect, type Socket } from 'node:net';
import { defineActor, type Host } from '@sigx/actors';
import { defineActorApp, memoryStorage, type ActorApp } from '@sigx/actors/host';
import { createAppHandler } from '@sigx/actors/node';
import { cluster, memoryClusterHub, type HostDescriptor } from '@sigx/actors/cluster';
import { tcpTransport, TCP_TRANSPORT_NAME } from '../src/index.ts';

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ n: 0 }),
    methods: (ctx) => ({
        bump() {
            return ++ctx.state.n;
        }
    })
});

interface Started {
    app: ActorApp;
    server: Server;
    tcpPort: number;
    host: Host;
}

const running: Started[] = [];
const sockets: Socket[] = [];

afterEach(async () => {
    for (const socket of sockets.splice(0)) socket.destroy();
    for (const { app, server } of running.splice(0)) {
        await app.stop({ timeoutMs: 1000 });
        await new Promise<void>((resolve) => server.close(() => resolve()));
    }
});

/** One host whose TCP listener is configured by the caller. */
async function startHost(
    tcpOptions: Parameters<typeof tcpTransport>[0] = {},
    providers = memoryClusterHub().providers()
): Promise<Started> {
    const server = createServer();
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const httpPort = (server.address() as { port: number }).port;

    const plugin = cluster({
        providers,
        advertise: `http://127.0.0.1:${httpPort}`,
        secret: 'handshake-test',
        transport: tcpTransport({ host: '127.0.0.1', keepAliveMs: 0, ...tcpOptions })
    });
    const app = defineActorApp({
        actors: [Counter],
        storage: memoryStorage(),
        defaults: { sweepIntervalMs: 3_600_000, reminderTickMs: 3_600_000, callTimeoutMs: 0 }
    }).use(plugin);
    server.on('request', createAppHandler(app));
    const host = await app.start();

    const descriptor = (await plugin.placement.descriptor()) as HostDescriptor;
    const address = descriptor.addresses?.[TCP_TRANSPORT_NAME];
    if (!address) throw new Error('the transport advertised no tcp address');
    const port = Number(address.slice(address.lastIndexOf(':') + 1));

    const started: Started = { app, server, tcpPort: port, host };
    running.push(started);
    return started;
}

/** Connect and resolve when the peer closes us, or `null` on timeout. */
function connectSilently(port: number, waitMs: number): Promise<'closed' | 'still-open'> {
    return new Promise((resolve) => {
        const socket = connect({ port, host: '127.0.0.1' });
        sockets.push(socket);
        // Deliberately never WRITES a HELLO frame — but it must still READ.
        // A socket with no `data` listener stays paused, and a paused socket
        // whose peer has hung up does not emit `end`/`close` until its
        // buffered bytes are consumed. The listener answers WELCOME
        // immediately, so without this the test would report `still-open` for
        // a connection the server had already destroyed.
        socket.resume();
        const timer = setTimeout(() => resolve('still-open'), waitMs);
        const done = (): void => {
            clearTimeout(timer);
            resolve('closed');
        };
        socket.on('close', done);
        socket.on('end', done);
        socket.on('error', done);
    });
}

describe('tcpTransport(): inbound handshake deadline', () => {
    it('closes a connection that never sends HELLO', async () => {
        const { tcpPort } = await startHost({ handshakeTimeoutMs: 150 });
        await expect(connectSilently(tcpPort, 3_000)).resolves.toBe('closed');
    });

    it('leaves a silent connection alone when the deadline is disabled', async () => {
        // 0 is the escape hatch for a deployment that genuinely wants the old
        // behaviour; it must actually disable the timer rather than clamp to
        // some minimum.
        const { tcpPort } = await startHost({ handshakeTimeoutMs: 0 });
        await expect(connectSilently(tcpPort, 400)).resolves.toBe('still-open');
    });

    it.each([
        ['handshakeTimeoutMs', -1],
        ['handshakeTimeoutMs', Number.NaN],
        ['handshakeTimeoutMs', Number.POSITIVE_INFINITY],
        ['maxPendingInbound', -1],
        ['maxPendingInbound', 1.5],
        ['maxPendingInbound', Number.NaN]
    ])('refuses %s: %s rather than silently disabling the bound', (name, value) => {
        // Both bounds are guarded by `> 0`, so before this every one of these
        // turned the protection off — misconfiguration arriving at exactly the
        // state the option exists to prevent. The factory is what throws, so
        // it fails at construction rather than at the first hostile connection.
        expect(() =>
            tcpTransport({ host: '127.0.0.1', [name]: value })({
                hostId: 'h',
                epoch: 1
            } as never)
        ).toThrow(new RegExp(name));
    });

    it('still accepts 0 as the deliberate escape hatch', () => {
        expect(() =>
            tcpTransport({ host: '127.0.0.1', handshakeTimeoutMs: 0, maxPendingInbound: 0 })({
                hostId: 'h',
                epoch: 1
            } as never)
        ).not.toThrow();
    });

    it('does not reap a peer link that completed the handshake', async () => {
        // The deadline has to be CANCELLED on HELLO, not merely outlived by a
        // long timeout: a real peer link stays open for the process's life,
        // far past any handshake window. Two hosts on one hub, a cross-host
        // call to force the link up, then a wait well past the deadline, then
        // another call over the same link.
        const providers = memoryClusterHub().providers();
        const a = await startHost({ handshakeTimeoutMs: 150 }, providers);
        await startHost({ handshakeTimeoutMs: 150 }, providers);

        // Whichever host owns it, this crosses the wire in one direction.
        expect(await a.host.actor(Counter, 'shared').bump()).toBe(1);

        await new Promise((resolve) => setTimeout(resolve, 500));

        // If the established link had been reaped, this would have to redial
        // — and with a 150 ms deadline still armed it would be flaky rather
        // than merely slow. It answers 2, on the same activation.
        expect(await a.host.actor(Counter, 'shared').bump()).toBe(2);
    });
});
