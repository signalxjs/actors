/**
 * One host, in its own OS process, listening on a real loopback socket.
 *
 * This is the half of the rig that the in-process harness cannot be: every
 * cross-host call here crosses a real TCP connection through Node's HTTP
 * stack, which is exactly the behaviour the socket-cost work is about and
 * which `pipeFetch` makes invisible.
 *
 * Two things are load-bearing about the startup order below:
 *
 *  - **Listen on port 0 FIRST, then build `cluster({ advertise })`.** The
 *    port is not knowable earlier, and the seam's whole invariant is that
 *    something answers before any peer can learn the address.
 *  - **Membership and the directory live in the parent**, reached over IPC.
 *    `view()` is sync on the provider interface, so it is served from a
 *    local cache the parent pushes to — the shape a real provider has.
 */
import { createServer, type Server } from 'node:http';
import type { Socket } from 'node:net';
import { defineActor } from '@sigx/actors';
import { defineActorApp, memoryStorage, type ActorApp } from '@sigx/actors/host';
import { createAppHandler } from '@sigx/actors/node';
import { cluster, httpTransport, type ClusterPlugin } from '@sigx/actors/cluster';
import type { HostTransportFactory } from '@sigx/actors/cluster';
import { tcpTransport } from '@sigx/actors-tcp';
import type {
    ActorDirectory,
    ClusterMembership,
    DirectoryEntry,
    MembershipView,
    PlacementPolicy,
    HostDescriptor,
    HostStatus
} from '@sigx/actors/cluster';
import type { Host } from '@sigx/actors';
import { closedLoop } from '../loop.ts';
import type { ChildStats, DispatcherKind, StoreOp, ToChild, ToParent } from './protocol.ts';

/** The internal mount's default prefix — matched by hand on the hot path. */
const INTERNAL_BASE = '/_sigx/host';

/**
 * Build the outbound client for this run's dispatcher arm.
 *
 * `undici` is imported dynamically and ONLY for the tuned arms, so the
 * `default` arm measures exactly what ships — the global `fetch` — with no
 * chance of the import itself perturbing it. It is a benchmarks-only
 * devDependency: nothing in `@sigx/actors` may depend on it, since
 * `./cluster` is zero-dep and WinterCG-clean so that Workers keep working.
 */
async function dispatcherFetch(
    kind: DispatcherKind,
    connections: number
): Promise<typeof globalThis.fetch | null> {
    if (kind === 'default') return null;
    const { Agent, fetch: undiciFetch } = await import('undici');
    const agent = new Agent({
        connections,
        ...(kind === 'h2' ? { allowH2: true } : {})
    });
    return ((input: RequestInfo | URL, init?: RequestInit) =>
        undiciFetch(input as never, {
            ...init,
            dispatcher: agent
        } as never)) as unknown as typeof globalThis.fetch;
}

const send = (message: ToParent): void => {
    process.send?.(message);
};

function fail(error: unknown): never {
    send({ t: 'failed', message: error instanceof Error ? `${error.message}\n${error.stack}` : String(error) });
    process.exit(1);
}

// A parent that dies must never leave a host holding a port.
process.on('disconnect', () => process.exit(0));
process.on('uncaughtException', fail);
process.on('unhandledRejection', fail);

// ---------------------------------------------------------------------------
// The store, proxied to the parent

let storeOps = 0;
let nextRpc = 1;
const pending = new Map<number, { resolve(v: unknown): void; reject(e: Error): void }>();

function rpc(op: StoreOp, ...args: readonly unknown[]): Promise<unknown> {
    storeOps++;
    const id = nextRpc++;
    return new Promise((resolve, reject) => {
        pending.set(id, { resolve, reject });
        send({ t: 'store', id, op, args });
    });
}

/** The view the parent last pushed. Served synchronously — `dispatcherFor`
 *  is on the hot path, which is why the provider interface demands it. */
let cachedView: MembershipView = { version: 0, hosts: [] };
const changeCbs = new Set<(view: MembershipView) => void>();
const suspectCbs = new Set<() => void>();

const membership: ClusterMembership = {
    join: async (self: HostDescriptor) => void (await rpc('membership.join', self)),
    setStatus: async (status: HostStatus) => void (await rpc('membership.setStatus', status)),
    leave: async () => void (await rpc('membership.leave')),
    view: () => cachedView,
    refresh: async () => {
        cachedView = (await rpc('membership.refresh')) as MembershipView;
        return cachedView;
    },
    isAlive: async (hostId: string) => (await rpc('membership.isAlive', hostId)) as boolean,
    onChange(cb) {
        changeCbs.add(cb);
        return () => changeCbs.delete(cb);
    },
    onSelfSuspect(cb) {
        suspectCbs.add(cb);
        return () => suspectCbs.delete(cb);
    }
};

/**
 * Casts are to the real return types, not `never`: everything here DOES
 * resolve, and the IPC boundary is the one place a wrong shape would surface
 * as a confusing runtime failure rather than a type error.
 */
const directory: ActorDirectory = {
    lookup: (actorId) =>
        rpc('directory.lookup', actorId) as Promise<DirectoryEntry | null>,
    claim: (actorId, mine) =>
        rpc('directory.claim', actorId, mine) as Promise<DirectoryEntry>,
    release: async (actorId, expected) => void (await rpc('directory.release', actorId, expected)),
    evict: (actorId, expected) => rpc('directory.evict', actorId, expected) as Promise<boolean>,
    evictHost: (hostId) => rpc('directory.evictHost', hostId) as Promise<number>
};

// ---------------------------------------------------------------------------
// The actor under test — trivial on purpose, so the wire dominates

const Bench = defineActor({
    type: 'Tier2Bench',
    allowAnonymous: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        noop() {
            return 0;
        },
        increment(by: number) {
            ctx.state.count += by;
            return ctx.state.count;
        }
    })
});

/** Every host activates its own new actors — pins locality deterministically
 *  so a driver knows exactly which calls cross the wire. */
const selfPolicy: PlacementPolicy = { name: 'tier2-self', choose: (_r, _v, self) => self };

// ---------------------------------------------------------------------------
// Socket accounting.
//
// Counting ACCEPTS is how outbound sockets get measured without hooking
// undici: if this host accepted K connections from a peer, that peer opened
// K to us. Summed cluster-wide it is exactly "sockets ever opened".

let accepted = 0;
let concurrentNow = 0;
let concurrentPeak = 0;
let bytesIn = 0;
let bytesOut = 0;
let inboundRequests = 0;
let callsIssued = 0;

function watchSockets(server: Server): void {
    server.on('connection', (socket: Socket) => {
        accepted++;
        concurrentNow++;
        if (concurrentNow > concurrentPeak) concurrentPeak = concurrentNow;
        socket.on('close', () => {
            concurrentNow--;
            // Read at close: the counters are cumulative per socket, so
            // sampling earlier would double-count on a long-lived one.
            bytesIn += socket.bytesRead;
            bytesOut += socket.bytesWritten;
        });
    });
}

/**
 * Live TCP handles from libuv's own table — an independent cross-check on
 * the accept counter above, and portable across darwin and linux (unlike
 * `/proc/self/fd`, and unlike shelling out to `lsof` per sample).
 */
function tcpHandles(): number {
    try {
        const report = process.report.getReport() as unknown as {
            libuv?: { type?: string }[];
        };
        return (report.libuv ?? []).filter((handle) => handle.type === 'tcp').length;
    } catch {
        return -1;
    }
}

function snapshot(hostId: string, liveSockets: Set<Socket>): ChildStats {
    // Add the in-flight sockets' current byte counts to the closed ones, so
    // a run that never closes a connection still reports its traffic.
    let liveIn = 0;
    let liveOut = 0;
    for (const socket of liveSockets) {
        liveIn += socket.bytesRead;
        liveOut += socket.bytesWritten;
    }
    return {
        hostId,
        accepted,
        concurrentPeak,
        concurrentNow,
        bytesIn: bytesIn + liveIn,
        bytesOut: bytesOut + liveOut,
        inboundRequests,
        storeOps,
        callsIssued,
        tcpHandles: tcpHandles(),
        rssBytes: process.memoryUsage.rss()
    };
}

// ---------------------------------------------------------------------------

async function main(): Promise<void> {
    let app: ActorApp | null = null;
    let plugin: ClusterPlugin | null = null;
    let host: Host | null = null;
    let hostId = '';
    const live = new Set<Socket>();

    // The handler is swapped in after `cluster()` knows the port. Until then
    // the listener answers 503 — it is bound, but nothing is advertised yet,
    // so nobody can be calling it.
    let handler: (req: never, res: never) => void = (_req, res) => {
        (res as unknown as { statusCode: number; end(): void }).statusCode = 503;
        (res as unknown as { end(): void }).end();
    };
    // A prefix test, not `matchesHostRequest`: that takes a `Request`, and
    // constructing one per inbound call would put allocation on the very hot
    // path this rig exists to measure.
    const server = createServer((req, res) => {
        if (req.url?.startsWith(`${INTERNAL_BASE}/`)) inboundRequests++;
        handler(req as never, res as never);
    });
    watchSockets(server);
    server.on('connection', (socket) => {
        live.add(socket);
        socket.on('close', () => live.delete(socket));
    });

    process.on('message', (message: ToChild) => {
        void handle(message);
    });

    async function handle(message: ToChild): Promise<void> {
        switch (message.t) {
            case 'init': {
                // Bind FIRST so the port exists, then advertise it.
                await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
                const port = (server.address() as { port: number }).port;

                // `tcp` REPLACES the transport; the others tune the fetch
                // behind the default one.
                let transport: HostTransportFactory | undefined;
                if (message.dispatcher === 'tcp') {
                    transport = tcpTransport({ host: '127.0.0.1', keepAliveMs: 0 });
                } else {
                    const tuned = await dispatcherFetch(message.dispatcher, message.connections);
                    if (tuned) transport = httpTransport({ fetch: tuned });
                }

                plugin = cluster({
                    providers: { membership, directory },
                    advertise: `http://127.0.0.1:${port}`,
                    policy: selfPolicy,
                    // The `fetch` seam is the pool escape hatch; the
                    // `transport` option is the host-transport seam. Both are
                    // exercised here without any runtime change.
                    ...(transport ? { transport } : {}),
                    // Passed, not omitted — see cluster-harness.ts. The prod
                    // dist throws on an omitted secret; `null` is how the
                    // no-HMAC arm declares itself.
                    secret: message.secret ?? null
                });
                app = defineActorApp({
                    actors: [Bench],
                    storage: memoryStorage(),
                    // No background churn, no call deadlines: this measures
                    // the transport, not the sweeper.
                    defaults: {
                        sweepIntervalMs: 3_600_000,
                        reminderTickMs: 3_600_000,
                        callTimeoutMs: 0
                    }
                }).use(plugin);
                handler = createAppHandler(app) as unknown as typeof handler;
                send({ t: 'ready', index: message.index, port });
                return;
            }
            case 'start': {
                host = await app!.start();
                // The plugin exposes its placement immediately, so identity
                // is readable without reaching into the host.
                hostId = plugin!.placement.identity.hostId;
                send({ t: 'started', hostId });
                return;
            }
            case 'view': {
                cachedView = message.view;
                for (const cb of changeCbs) cb(cachedView);
                return;
            }
            case 'selfSuspect': {
                for (const cb of suspectCbs) cb();
                return;
            }
            case 'storeReply': {
                const entry = pending.get(message.id);
                pending.delete(message.id);
                if (!entry) return;
                if (message.ok) entry.resolve(message.value);
                else entry.reject(new Error(message.error));
                return;
            }
            case 'warm': {
                // Prime activations and the route cache, so the measured
                // pass does no directory work — the `ipc_ops_per_call` guard
                // asserts that this actually worked.
                for (const target of message.targets) {
                    await host!.dispatch(target, 'noop', [], {
                        callChain: [],
                        callId: `warm.${target.key}`
                    });
                }
                send({ t: 'warmed', id: message.id });
                return;
            }
            case 'drive': {
                const { targets, concurrency, durationMs, latency } = message.request;
                const outcome = await closedLoop({
                    concurrency,
                    durationMs,
                    latency,
                    call: (i) => {
                        callsIssued++;
                        const target = targets[i % targets.length]!;
                        return host!.dispatch(target, 'noop', [], {
                            callChain: [],
                            callId: `d.${i}`
                        });
                    }
                });
                send({
                    t: 'drove',
                    id: message.id,
                    ops: outcome.ops,
                    opsPerSec: outcome.opsPerSec,
                    ...(outcome.percentiles ? { percentiles: outcome.percentiles } : {})
                });
                return;
            }
            case 'stats':
                send({ t: 'stats', id: message.id, stats: snapshot(hostId, live) });
                return;
            case 'resetStats': {
                accepted = 0;
                concurrentPeak = concurrentNow;
                bytesIn = 0;
                bytesOut = 0;
                inboundRequests = 0;
                storeOps = 0;
                callsIssued = 0;
                for (const socket of live) {
                    // Rebase the live sockets so their pre-reset traffic is
                    // not attributed to the measured window.
                    bytesIn -= socket.bytesRead;
                    bytesOut -= socket.bytesWritten;
                }
                send({ t: 'reset', id: message.id });
                return;
            }
            case 'stop': {
                try {
                    await app?.stop({ timeoutMs: 2000 });
                } catch {
                    // Best effort; the parent kills us either way.
                }
                server.closeAllConnections?.();
                await new Promise<void>((resolve) => server.close(() => resolve()));
                send({ t: 'stopped', id: message.id });
                process.exit(0);
            }
        }
    }

}

await main();
