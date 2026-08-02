/**
 * Graceful shutdown drains the HTTP EDGE, not just the actors.
 *
 * The bug this pins was measured on a real cluster (#142): a rolling
 * restart under load lost 122 calls out of ~1.7M, all connection-level and
 * none in the actor layer. An orchestrator's preStop sleep and
 * readiness-503 only steer NEW connections away; sockets already in a
 * client's keep-alive pool survive endpoint removal via conntrack, ride
 * into the exiting pod, and are reset when the process exits.
 *
 * So these tests use a REAL `node:http` server and a REAL keep-alive agent.
 * A mock would assert that we call the methods; only a live socket can
 * show that the connection actually goes away before exit would have
 * killed it.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createServer, Agent, request, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import type { Socket } from 'node:net';
import { attachSignalHandlers } from '@sigx/actors/node';
import { createHost, type Host } from '@sigx/actors/host';
import { defineActor } from '@sigx/actors';

const Noop = defineActor({
    type: 'Noop',
    unguarded: true,
    state: () => ({}),
    methods: () => ({
        ping() {
            return 'pong';
        }
    })
});

interface Rig {
    server: Server;
    host: Host;
    agent: Agent;
    port: number;
    /** Every socket the server accepted. */
    sockets: Socket[];
    detach: () => void;
}

const rigs: Rig[] = [];

afterEach(async () => {
    for (const rig of rigs.splice(0)) {
        rig.detach();
        rig.agent.destroy();
        rig.server.closeAllConnections?.();
        await new Promise<void>((resolve) => rig.server.close(() => resolve()));
        await rig.host.stop({ timeoutMs: 500 }).catch(() => {});
    }
});

async function rig(options?: { server?: boolean }): Promise<Rig> {
    const host = createHost({ actors: [Noop] });
    const sockets: Socket[] = [];
    const server = createServer((_req, res) => {
        res.writeHead(200, { 'content-type': 'text/plain' });
        res.end('ok');
    });
    server.on('connection', (socket) => sockets.push(socket));
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const port = (server.address() as AddressInfo).port;
    const agent = new Agent({ keepAlive: true, maxSockets: 4 });
    const detach = attachSignalHandlers(host, {
        exit: false,
        timeoutMs: 500,
        ...(options?.server === false ? {} : { server })
    });
    const created = { server, host, agent, port, sockets, detach };
    rigs.push(created);
    return created;
}

/** One keep-alive GET; resolves when the response is fully read, leaving the
 *  socket IDLE in the agent's pool — the state that causes the bug. */
function get(rig: Rig): Promise<number> {
    return new Promise((resolve, reject) => {
        const req = request(
            { host: '127.0.0.1', port: rig.port, path: '/', agent: rig.agent },
            (res) => {
                res.resume();
                res.on('end', () => resolve(res.statusCode ?? 0));
            }
        );
        req.on('error', reject);
        req.end();
    });
}

/** Wait for `check()` to hold, or throw. Shutdown is async by nature —
 *  polling beats guessing a sleep duration. */
async function until(check: () => boolean, what: string, ms = 2000): Promise<void> {
    const deadline = Date.now() + ms;
    while (!check()) {
        if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
        await new Promise((r) => setTimeout(r, 10));
    }
}

describe('attachSignalHandlers({ server })', () => {
    it('destroys an IDLE pooled keep-alive socket instead of leaving it for exit', async () => {
        // THE regression. Without the drain the socket stays open and alive
        // until process.exit() resets it — which is what the client sees as
        // ECONNRESET on a connection it thought was good.
        const r = await rig();
        await expect(get(r)).resolves.toBe(200);
        expect(r.sockets).toHaveLength(1);
        expect(r.sockets[0]!.destroyed).toBe(false);

        process.emit('SIGTERM');

        await until(() => r.sockets[0]!.destroyed, 'the idle socket to be destroyed');
        expect(r.server.listening).toBe(false);
    });

    it('stops accepting new connections', async () => {
        const r = await rig();
        await expect(get(r)).resolves.toBe(200);

        process.emit('SIGTERM');
        await until(() => !r.server.listening, 'the listener to close');

        // A fresh agent cannot get in — the port is gone rather than
        // answering and then dying mid-response.
        const fresh = new Agent({ keepAlive: false });
        await expect(get({ ...r, agent: fresh })).rejects.toThrow();
        fresh.destroy();
    });

    it('still stops the host', async () => {
        const r = await rig();
        process.emit('SIGTERM');
        await until(() => r.host.stats().activations === 0, 'the host to drain');
        // A second stop is a no-op, which is what "already stopped" looks
        // like from outside.
        await expect(r.host.stop({ timeoutMs: 200 })).resolves.toBeUndefined();
    });

    it('is idempotent across repeated signals', async () => {
        const r = await rig();
        await get(r);
        process.emit('SIGTERM');
        process.emit('SIGTERM');
        process.emit('SIGINT');
        await until(() => r.sockets[0]!.destroyed, 'the socket to be destroyed');
        expect(r.server.listening).toBe(false);
    });
});

describe('without a server', () => {
    it('behaves exactly as before — the option is additive', async () => {
        const r = await rig({ server: false });
        await expect(get(r)).resolves.toBe(200);

        process.emit('SIGTERM');
        await until(() => r.host.stats().activations === 0, 'the host to drain');

        // Unchanged: the listener is untouched and the socket survives, which
        // is precisely the old behaviour this option exists to opt out of.
        expect(r.server.listening).toBe(true);
        expect(r.sockets[0]!.destroyed).toBe(false);
    });

    it('tolerates a server without the Node >= 18.2 connection methods', async () => {
        // An older Node, or a non-Node server object. Shutdown must not
        // throw at the one moment nothing is left to catch it.
        const host = createHost({ actors: [Noop] });
        let closed = false;
        const minimal = {
            close: () => {
                closed = true;
            }
        };
        const detach = attachSignalHandlers(host, {
            exit: false,
            timeoutMs: 500,
            server: minimal
        });
        process.emit('SIGTERM');
        await until(() => closed, 'close() to be called');
        await until(() => host.stats().activations === 0, 'the host to drain');
        detach();
        await host.stop({ timeoutMs: 200 }).catch(() => {});
    });
});

describe('a failed drain', () => {
    it('reports through onError and still finishes the shutdown', async () => {
        // Silently exiting 0 after a failed drain hides a real failure —
        // on a terminated pod the exit code is often the only diagnostic
        // that survives. `exit: false` keeps the process, so this asserts
        // the reporting half.
        const host = createHost({ actors: [Noop] });
        const boom = new Error('storage flush failed');
        host.stop = () => Promise.reject(boom);

        const seen: unknown[] = [];
        let closedAll = false;
        const detach = attachSignalHandlers(host, {
            exit: false,
            server: { close: () => {}, closeAllConnections: () => void (closedAll = true) },
            onError: (error) => seen.push(error)
        });

        process.emit('SIGTERM');
        await until(() => seen.length > 0, 'onError to fire');

        expect(seen[0]).toBe(boom);
        // The edge is still torn down — a failed actor drain must not leave
        // sockets alive behind it.
        expect(closedAll).toBe(true);
        detach();
    });

    it('a throwing onError cannot break the shutdown', async () => {
        const host = createHost({ actors: [Noop] });
        host.stop = () => Promise.reject(new Error('nope'));
        let closedAll = false;
        const detach = attachSignalHandlers(host, {
            exit: false,
            server: { close: () => {}, closeAllConnections: () => void (closedAll = true) },
            onError: () => {
                throw new Error('the reporter itself is broken');
            }
        });

        process.emit('SIGTERM');
        await until(() => closedAll, 'shutdown to complete regardless');
        detach();
    });
});

describe('the listener stays open until the drain finishes', () => {
    it('does NOT close early — closing early is what causes the resets', async () => {
        // Pins the finding from #150, which reverted an earlier "close the
        // listener first" fix. On Node >= 19 `close()` also destroys IDLE
        // connections, and "idle" from the server's side includes a socket
        // the client is at that instant writing its next request onto. So
        // closing early manufactures the very reset it was meant to prevent.
        //
        // The naive sequence looks more decisive and is worse; without this
        // test, someone reasonably "improves" it back.
        const r = await rig();
        await expect(get(r)).resolves.toBe(200);

        let release: () => void = () => {};
        const slow = new Promise<void>((resolve) => (release = resolve));
        const realStop = r.host.stop.bind(r.host);
        r.host.stop = async (opts) => {
            await slow;
            return realStop(opts);
        };

        process.emit('SIGTERM');
        await new Promise((resolve) => setTimeout(resolve, 50));

        // Mid-drain: still serving, and a pooled request still completes.
        expect(r.server.listening).toBe(true);
        expect(r.sockets[0]!.destroyed).toBe(false);
        await expect(get(r)).resolves.toBe(200);

        release();
        await until(() => !r.server.listening, 'the listener to close after the drain');
    });

    it('calls onStopBegin before the drain, so responses can say connection: close', async () => {
        const r = await rig();
        const order: string[] = [];
        const realStop = r.host.stop.bind(r.host);
        r.host.stop = async (opts) => {
            order.push('stop');
            return realStop(opts);
        };
        r.detach();
        const detach = attachSignalHandlers(r.host, {
            exit: false,
            timeoutMs: 500,
            server: r.server,
            onStopBegin: () => order.push('begin')
        });

        process.emit('SIGTERM');
        await until(() => order.length === 2, 'both phases to run');
        expect(order).toEqual(['begin', 'stop']);
        detach();
    });

    it('a throwing onStopBegin cannot abort the shutdown', async () => {
        const r = await rig();
        r.detach();
        const detach = attachSignalHandlers(r.host, {
            exit: false,
            timeoutMs: 500,
            server: r.server,
            onStopBegin: () => {
                throw new Error('caller bookkeeping blew up');
            }
        });
        process.emit('SIGTERM');
        await until(() => !r.server.listening, 'shutdown to complete regardless');
        detach();
    });
});

describe('a throwing server teardown', () => {
    it('still exits — tearing down the edge is best-effort', async () => {
        // `DrainableServer` is structural, so close() is somebody else's
        // implementation and may throw. Unguarded, that throw escapes the
        // .finally() as an unhandled rejection and SKIPS process.exit —
        // leaving a process that was told to terminate running forever.
        // So this asserts the exit itself, not merely that we got here.
        const host = createHost({ actors: [Noop] });
        const exits: (number | undefined)[] = [];
        const spy = vi
            .spyOn(process, 'exit')
            .mockImplementation(((code?: number) => {
                exits.push(code);
                return undefined as never;
            }) as never);

        const detach = attachSignalHandlers(host, {
            timeoutMs: 500,
            server: {
                close: () => {
                    throw new Error('close() is broken');
                },
                closeAllConnections: () => {
                    throw new Error('and so is this');
                }
            }
        });

        process.emit('SIGTERM');
        await until(() => exits.length > 0, 'process.exit to be called');

        expect(exits).toEqual([0]); // the DRAIN succeeded; only teardown threw
        spy.mockRestore();
        detach();
        await host.stop({ timeoutMs: 200 }).catch(() => {});
    });
});
