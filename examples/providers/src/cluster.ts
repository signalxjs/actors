/**
 * The three-host walk every demo here runs — `examples/counter`'s
 * `cluster-demo.mjs`, with its three seams lifted out as options:
 *
 *   storage     the cluster's database        memoryStorage → pgStorage / surrealStorage
 *   providers   membership + directory        memoryClusterHub → pgCluster / surrealCluster
 *   transport   the host-to-host link         httpTransport → [tcpTransport, httpTransport]
 *
 * plus `plugins`, for the exporters. Nothing else differs between the
 * demos, which is the point: a provider package is a handful of lines at
 * the seam, and the walk proves the same four things over each of them.
 * Every step throws on a wrong result, so a demo that prints `COMPLETE`
 * has asserted its way there.
 */
import { createServer, type Server } from 'node:http';
import type { Host } from '@sigx/actors';
import {
    defineActorApp,
    health,
    metrics,
    ops,
    type ActorApp,
    type ActorPlugin,
    type ActorStorage
} from '@sigx/actors/host';
import { createAppHandler } from '@sigx/actors/node';
import {
    cluster,
    clusterStats,
    type ClusterPlacement,
    type ClusterProviders,
    type ClusterStatsReport,
    type HostTransportFactory
} from '@sigx/actors/cluster';
import { Counter } from './counter.actor.ts';

/** The runtime's actor-id separator: an actor id is `${type}${SEP}${key}`. */
export const SEP = String.fromCharCode(0);

export interface DemoClusterOptions {
    /** What the log calls the store — `Postgres`, `memoryStorage()`. */
    label: string;
    /** The shared store. ONE instance for the whole cluster: it is the database. */
    storage: ActorStorage;
    /** Membership + directory for ONE host. Called once per host. */
    providers: () => ClusterProviders;
    /**
     * The host-to-host link for host `index`. Default: the plugin's own
     * `httpTransport()`. A per-host factory because a socket transport owns
     * a listener, and three hosts need three of them.
     */
    transport?: (index: number) => HostTransportFactory | readonly HostTransportFactory[];
    /** Extra plugins for host `index` — the exporters, in the otel demo. */
    plugins?: (index: number) => ActorPlugin[];
    /** Listener ports. Default `PROVIDERS_DEMO_PORTS`, else 5591-5593. */
    ports?: readonly number[];
    /** Bearer token for `ops()` and anything mounted beside it. */
    opsSecret?: string;
    log?: (...args: unknown[]) => void;
}

export interface Member {
    index: number;
    port: number;
    app: ActorApp;
    host: Host;
    placement: ClusterPlacement;
    providers: ClusterProviders;
    server: Server;
}

export interface DemoCluster {
    readonly members: Member[];
    /** Per-run suffix on every key, so a persistent store starts each run fresh. */
    readonly run: string;
    readonly opsSecret: string;
    key(name: string): string;
    hostId(m: Member): string;
    /**
     * Print a numbered section header. The harness numbers them itself so
     * a demo can slot its own step between the shared ones and the run
     * still reads 1, 2, 3 … in the order it actually happened.
     */
    step(title: string): void;
    /** Nine counters through host 0 alone; placement spreads them. */
    spread(): Promise<Record<string, number>>;
    /** One key hammered through every host at once: one activation, one owner. */
    singleActivation(): Promise<{ owner: Member; results: number[] }>;
    /**
     * Calls from a host that does NOT own the actor — the wire, both ways.
     * Leaves `cart` at `CROSS_HOST_COUNT`.
     */
    crossHost(owner: Member): Promise<{ via: Member; count: number }>;
    /**
     * The owner leaves; a survivor serves the key from the SHARED store and
     * must read back `expected` — `CROSS_HOST_COUNT` unless the demo added
     * calls of its own in between. Graceful on purpose — it is what makes
     * the step deterministic over a TTL-judged membership. The crash
     * variant is `examples/counter`'s.
     */
    failover(
        owner: Member,
        expected?: number
    ): Promise<{ survivor: Member; count: number; reclaimedBy: string }>;
    /** `clusterStats()` from a survivor, printed as a table. */
    report(from: Member): Promise<ClusterStatsReport>;
    /** Stop every host still running and close its listener. */
    stop(): Promise<void>;
}

/** Where `crossHost` leaves the `cart` counter: 1+1+1+1+1+1 then +10 +100. */
export const CROSS_HOST_COUNT = 116;

const DEFAULT_PORTS = '5591,5592,5593';

/** An env value, with `""` treated as unset — `FOO= node demo.mjs` should not blank a default. */
export const envOr = (value: string | undefined, fallback: string): string =>
    value === undefined || value === '' ? fallback : value;

/**
 * `PROVIDERS_DEMO_PORTS` as three listener ports (0 = ephemeral), or a
 * message naming the variable — `Number('abc')` is NaN, and `listen(NaN)`
 * fails somewhere deep in Node with no hint of where the value came from.
 */
export function parsePorts(value: string | undefined): number[] {
    const raw = envOr(value, DEFAULT_PORTS);
    const ports = raw.split(',').map((p) => p.trim());
    const valid = (p: string): boolean => /^\d+$/.test(p) && Number(p) <= 65535;
    if (ports.length !== 3 || !ports.every(valid)) {
        throw new Error(
            `PROVIDERS_DEMO_PORTS must be three comma-separated ports (0-65535), e.g. "${DEFAULT_PORTS}"; got "${raw}"`
        );
    }
    return ports.map(Number);
}

/** Poll `check` until it holds, or throw after `timeoutMs`. */
async function until(check: () => boolean, what: string, timeoutMs = 10_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (!check()) {
        if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
        await new Promise((r) => setTimeout(r, 50));
    }
}

export async function startCluster(options: DemoClusterOptions): Promise<DemoCluster> {
    const log = options.log ?? ((...args: unknown[]) => console.log(...args));
    const ports = options.ports ?? parsePorts(process.env.PROVIDERS_DEMO_PORTS);
    const opsSecret = options.opsSecret ?? envOr(process.env.PROVIDERS_DEMO_OPS_SECRET, 'demo-ops-secret');
    // Host-to-host HMAC. Separate from the ops token on purpose: they
    // authenticate different things to different people.
    const secret = 'demo-secret';
    const run = Date.now().toString(36);
    const key = (name: string): string => `${name}-${run}`;
    const hostId = (m: Member): string => m.placement.identity.hostId;
    let steps = 0;
    const step = (title: string): void => log(`\n=== ${++steps}. ${title} ===`);

    const members: Member[] = [];
    for (const [index, wanted] of ports.entries()) {
        // Listen FIRST, on whatever port was asked for — `0` binds an
        // ephemeral one, which is what the test suite uses — then read the
        // real port back and build the host around it. The handler is bound
        // late for exactly that reason: `advertise` needs the port, and the
        // plugin needs `advertise`. Listening before `app.start()` is also
        // the correct order in its own right: start() joins membership, and
        // from that moment peers may place actors here and call them.
        let handler: ReturnType<typeof createAppHandler> | null = null;
        const server = createServer((req, res) => {
            if (handler) handler(req, res);
            else res.writeHead(503).end();
        });
        await new Promise<void>((r) => server.listen(wanted, '127.0.0.1', r));
        const address = server.address();
        const port = typeof address === 'object' && address ? address.port : wanted;

        const providers = options.providers();
        const transport = options.transport?.(index);
        const plugin = cluster({
            providers,
            advertise: `http://127.0.0.1:${port}`,
            secret,
            ...(transport ? { transport } : {})
        });
        let app = defineActorApp({ actors: [Counter], storage: options.storage })
            .use(plugin)
            .use(metrics())
            .use(health())
            .use(
                ops({
                    secret: opsSecret,
                    cluster: (signal, query) => clusterStats(plugin.placement, { signal, ...query })
                })
            );
        for (const extra of options.plugins?.(index) ?? []) app = app.use(extra);

        // ONE handler for both mounts: the public endpoint and whatever the
        // plugins contributed (the internal route, the metrics route).
        handler = createAppHandler(app);
        const host = await app.start();
        const member: Member = { index, port, app, host, placement: plugin.placement, providers, server };
        members.push(member);
        const descriptor = plugin.placement.descriptor();
        const addresses = descriptor.addresses
            ? Object.entries(descriptor.addresses)
                  .map(([name, addr]) => `${name}=${addr}`)
                  .join(' ')
            : `http=${descriptor.address}`;
        log(`host ${hostId(member)} listening on :${port}  (${addresses})`);
    }

    const spreadLine = (): string =>
        members.map((m) => `${hostId(m)}=${m.host.stats().activations}`).join('  ');
    const owned = async (name: string): Promise<Member> => {
        // Resolve through a member that is still up: after `failover` the
        // leaver's providers may be stopped along with its host.
        const reader = members.find((m) => m.server.listening) ?? members[0]!;
        const entry = await reader.providers.directory.lookup(`Counter${SEP}${key(name)}`);
        const owner = entry && members.find((m) => hostId(m) === entry.hostId);
        if (!owner) throw new Error(`'${key(name)}' has no owner in the directory`);
        return owner;
    };

    const demo: DemoCluster = {
        members,
        run,
        opsSecret,
        key,
        hostId,
        step,

        async spread() {
            step('Placement spread — 9 counters created via host 0 alone');
            for (let i = 0; i < 9; i++) {
                await members[0]!.host.actor(Counter, key(`counter-${i}`)).increment(1);
            }
            log('activations per host:', spreadLine());
            log('(host 0 took the calls; the placement policy spread the actors)');
            const per: Record<string, number> = {};
            for (const m of members) per[hostId(m)] = m.host.stats().activations;
            return per;
        },

        async singleActivation() {
            step('Single activation — the same key hammered through ALL hosts');
            const results = await Promise.all(
                members.flatMap((m) => [
                    m.host.actor(Counter, key('cart')).increment(1),
                    m.host.actor(Counter, key('cart')).increment(1)
                ])
            );
            results.sort((a, b) => a - b);
            log(`6 concurrent increments via ${members.length} hosts →`, results);
            if (results.join() !== '1,2,3,4,5,6') throw new Error('lost or duplicated an update!');
            const owner = await owned('cart');
            log(`'${key('cart')}' has exactly one owner: ${hostId(owner)} (:${owner.port})`);
            return { owner, results };
        },

        async crossHost(owner) {
            step('Cross-host calls — through a host that does NOT own the actor');
            const via = members.find((m) => m !== owner)!;
            const before = via.placement.counters().remoteDispatches;
            await via.host.actor(Counter, key('cart')).increment(10);
            const count = await via.host.actor(Counter, key('cart')).increment(100);
            const remote = via.placement.counters().remoteDispatches - before;
            log(`increment(10), increment(100) via ${hostId(via)} → count=${count}`);
            log(`(${remote} remote dispatches left ${hostId(via)}; the turns ran on ${hostId(owner)})`);
            if (count !== CROSS_HOST_COUNT) throw new Error(`expected ${CROSS_HOST_COUNT}, got ${count}`);
            return { via, count };
        },

        async failover(owner, expected = CROSS_HOST_COUNT) {
            step(`Owner ${hostId(owner)} leaves — a survivor re-loads from ${options.label}`);
            await owner.app.stop({ timeoutMs: 2000 });
            await new Promise<void>((r) => owner.server.close(() => r()));
            const survivors = members.filter((m) => m !== owner);
            // Wait for the survivors' VIEW to drop the leaver, not for a
            // guessed interval: over a store-backed membership the view
            // arrives by push or poll, and placing on a departed host would
            // only be retried, which is not what this step is about.
            const gone = (m: Member): boolean =>
                !m.placement.view().hosts.some((h) => h.hostId === hostId(owner));
            await until(() => survivors.every(gone), `${hostId(owner)} to leave the view`);
            log(`survivors' view: ${survivors.map((m) => `${hostId(m)} sees ${m.placement.view().hosts.length} host(s)`).join(', ')}`);

            const survivor = survivors[0]!;
            const { count } = await survivor.host.actor(Counter, key('cart')).current();
            const reclaimedBy = hostId(await owned('cart'));
            log(`survivor ${hostId(survivor)} serves '${key('cart')}' → count=${count}`);
            log(`directory re-claimed by: ${reclaimedBy}`);
            if (count !== expected) throw new Error(`state lost in failover! expected ${expected}, got ${count}`);
            log(`(the activation died with its host; the state came back from ${options.label})`);
            return { survivor, count, reclaimedBy };
        },

        async report(from) {
            step('Ops surface — clusterStats() from a survivor');
            const stats = await clusterStats(from.placement, { timeoutMs: 1000 });
            log(`view v${stats.view.version}: ${stats.view.size} members, ${stats.view.active} active`);
            log('  host          status  activations  out  in  fallbacks  transports');
            for (const s of stats.hosts) {
                log(
                    `  ${s.hostId}  ${s.status.padEnd(6)}  ${String(s.stats.activations).padStart(11)}  ` +
                        `${String(s.counters.remoteDispatches).padStart(3)}  ` +
                        `${String(s.counters.inboundDispatches).padStart(2)}  ` +
                        `${String(s.counters.transportFallbacks).padStart(9)}  ` +
                        `${(s.transports ?? []).join(',')}`
                );
            }
            log(`totals: ${stats.totals.activations} activations`, stats.totals.perType);
            log(`partial: ${stats.partial}`);
            return stats;
        },

        async stop() {
            await Promise.all(
                members.filter((m) => m.server.listening).map((m) => m.app.stop({ timeoutMs: 2000 }))
            );
            // Await the close callbacks, or a test's afterEach resolves with
            // listeners still winding down and the next suite trips over them.
            await Promise.all(
                members
                    .filter((m) => m.server.listening)
                    .map((m) => new Promise<void>((r) => m.server.close(() => r())))
            );
        }
    };
    return demo;
}
