/**
 * Multi-silo cluster demo — three silos on real localhost HTTP sockets,
 * one actor system. Uses the in-process memory cluster hub (so it needs no
 * Redis) with shared storage, and drives the same Counter actor the rest
 * of the example uses. Shows: placement spread, the single-activation
 * guarantee under cross-silo calls, a watch stream consumed from a
 * non-owner silo, and crash failover with state recovered from storage.
 *
 * Run after `pnpm build`:   node examples/counter/cluster-demo.mjs
 *
 * `--serve` (or CLUSTER_DEMO_SERVE=1) skips the shutdown and keeps the
 * cluster up under steady traffic, so there is something live to point a
 * dashboard at:
 *
 *     # terminal 1
 *     node examples/counter/cluster-demo.mjs --serve
 *     # terminal 2 — from THIS package, since the sigx CLI discovers its
 *     # plugins from the dependencies of the project it runs in
 *     pnpm --filter counter-example exec sigx actors top \
 *         --url http://127.0.0.1:5391 --secret demo-ops-secret
 *
 * Needs Node >= 22.18 (built-in type stripping for the .ts actor import).
 * Ports default to 5391-5393; override with
 * CLUSTER_DEMO_PORTS=7001,7002,7003.
 *
 * Each silo is a `defineActorApp` with the `cluster()` plugin, mounted
 * with ONE `createAppHandler`. The plugin contributes the silo-to-silo
 * route, so `secret` is stated once and the internal mount needs no
 * hand-written Node bridge.
 */
import { createServer } from 'node:http';
import { defineActorApp, health, memoryStorage, metrics, ops } from '@sigx/actors/silo';
import { createAppHandler } from '@sigx/actors/node';
import { cluster, clusterStats, memoryClusterHub } from '@sigx/actors/cluster';
import { Counter } from './src/counter.actor.ts';

const SEP = String.fromCharCode(0); // actorId separator: `${type}${SEP}${key}`
const PORTS = (process.env.CLUSTER_DEMO_PORTS ?? '5391,5392,5393')
    .split(',')
    .map((p) => Number(p.trim()));
const secret = 'demo-secret';
// The ops endpoint's bearer token. Separate from the cluster secret on
// purpose: they authenticate different things to different people — one is
// silo-to-silo, the other is an operator with a dashboard.
const opsSecret = process.env.CLUSTER_DEMO_OPS_SECRET ?? 'demo-ops-secret';
const serve = process.argv.includes('--serve') || process.env.CLUSTER_DEMO_SERVE === '1';
const hub = memoryClusterHub();
const storage = memoryStorage(); // one shared store = the cluster's database

const log = (...args) => console.log(...args);
const step = (title) => log(`\n=== ${title} ===`);

const members = []; // { app, silo, placement, server, port }
for (const port of PORTS) {
    const plugin = cluster({
        providers: hub.providers(),
        advertise: `http://127.0.0.1:${port}`,
        secret
    });
    // `health()` needs no cluster wiring: `cluster()` contributes its own
    // readiness check, so /ready reports `leaving` and `fenced` by itself.
    // `metrics()` is what gives the dashboard latency, the queue/turn split
    // and error kinds; `ops()` is what makes any of it readable from
    // outside the process. The cluster fan-out is wired by hand because
    // `ops()` lives in /silo and `clusterStats` in /cluster — a single-node
    // silo must not pay for the cluster bundle to have an ops endpoint.
    const app = defineActorApp({ actors: [Counter], storage })
        .use(plugin)
        .use(metrics())
        .use(health())
        .use(
            ops({
                secret: opsSecret,
                // The second argument is what `?detail=1` on the ops route asked
                // for. Spreading it is what lets a dashboard drill into ONE
                // silo without every poll paying for a fleet-wide grain walk.
                cluster: (signal, query) => clusterStats(plugin.placement, { signal, ...query })
            })
        );

    // ONE handler for both mounts: the public endpoint and the internal
    // silo-to-silo route the plugin contributed.
    const server = createServer(createAppHandler(app));
    // Listen BEFORE starting — app.start() joins membership, and from that
    // moment peers may place actors here and call them.
    await new Promise((r) => server.listen(port, r));
    const silo = await app.start();

    members.push({ app, silo, placement: plugin.placement, server, port });
    log(`silo ${plugin.placement.identity.siloId} listening on :${port}`);
}

const spread = () =>
    members
        .map((m) => `${m.placement.identity.siloId}=${m.silo.stats().activations}`)
        .join('  ');

step('1. Placement spread — 9 counters created via silo 0 alone');
for (let i = 0; i < 9; i++) {
    await members[0].silo.actor(Counter, `counter-${i}`).increment(1);
}
log('activations per silo:', spread());
log('(silo 0 took the calls; the placement policy spread the actors)');

step('2. Single activation — the same key hammered through ALL silos');
const results = await Promise.all(
    members.flatMap((m) => [
        m.silo.actor(Counter, 'cart').increment(1),
        m.silo.actor(Counter, 'cart').increment(1)
    ])
);
log('6 concurrent increments via 3 silos →', results.sort((a, b) => a - b));
const entry = await hub.directory.lookup(`Counter${SEP}cart`);
const owner = members.find((m) => m.placement.identity.siloId === entry.siloId);
log(`'cart' has exactly one owner: ${entry.siloId} (:${owner.port})`);

step('3. Cross-host stream — watch() consumed from a NON-owner silo');
const nonOwner = members.find((m) => m !== owner);
const snapshots = [];
let subscribed;
const attached = new Promise((r) => {
    subscribed = r;
});
const watching = (async () => {
    for await (const s of nonOwner.silo.actor(Counter, 'cart').watch()) {
        snapshots.push(s.count);
        log(`  watch (via ${nonOwner.placement.identity.siloId}): count=${s.count}`);
        if (snapshots.length === 1) subscribed();
        if (snapshots.length >= 3) break;
    }
})();
// Wait for the subscription to actually attach (its first snapshot), not
// for a guessed interval.
await attached;
await owner.silo.actor(Counter, 'cart').increment(10);
await owner.silo.actor(Counter, 'cart').increment(100);
await watching;

step(`4. Crash failover — killing the owner ${entry.siloId}`);
owner.server.close();
hub.kill(owner.placement.identity.siloId);
await new Promise((r) => setTimeout(r, 50));
const survivor = members.find((m) => m !== owner);
const recovered = await survivor.silo.actor(Counter, 'cart').current();
log(`survivor ${survivor.placement.identity.siloId} serves 'cart' →`, recovered);
const newEntry = await hub.directory.lookup(`Counter${SEP}cart`);
log(`directory re-claimed by: ${newEntry.siloId}`);
if (recovered.count !== 116) throw new Error('state lost in failover!');
log('(state came back from shared storage — nothing was lost)');

step('5. Ops surface — clusterStats() and the health probes');
const survivors = members.filter((m) => m !== owner);
const stats = await clusterStats(survivors[0].placement, { timeoutMs: 500 });
log(`view v${stats.view.version}: ${stats.view.size} members, ${stats.view.active} active`);
log('  silo          status  activations  queued  claimed  shards');
for (const s of stats.silos) {
    log(
        `  ${s.siloId}  ${s.status.padEnd(6)}  ${String(s.stats.activations).padStart(11)}  ` +
            `${String(s.stats.queued).padStart(6)}  ${String(s.counters.claimed).padStart(7)}  ` +
            `${String(s.reminderShards.length).padStart(6)}`
    );
}
log(`totals: ${stats.totals.activations} activations`, stats.totals.perType);

// The crashed owner is LISTED, not thrown — a report you can't get during
// an incident is worthless.
log(`partial: ${stats.partial} — unreachable:`, stats.unreachable.map((u) => `${u.siloId} (${u.reason})`));

// Reminder shard coverage: 16 shards, rendezvous-hashed over the LIVE view.
// This is the number that shows only N silos ever do reminder work.
const owners = Object.values(stats.reminderShards);
const orphaned = Object.entries(stats.reminderShards).filter(([, o]) => o.length === 0);
log(
    `reminder shards: 16 shards over ${new Set(owners.flat()).size} silo(s), ` +
        `${orphaned.length} orphaned (re-formed after the crash)`
);

// Routing counters, after the cross-silo traffic of steps 1-4.
const c = stats.silos.find((s) => s.siloId === survivors[0].placement.identity.siloId).counters;
log(
    `counters(${survivors[0].placement.identity.siloId}): ` +
        `local=${c.routedLocal} out=${c.remoteDispatches} in=${c.inboundDispatches} ` +
        `cacheHit=${c.routeCacheHits}/miss=${c.routeCacheMisses} ` +
        `dirLookups=${c.directoryLookups} wrongHost=${c.wrongHostRedirects} ` +
        `sweeps=${c.siloSweeps} authFailures=${c.authFailures}`
);
log('(out and in are never summed — the same call, counted on each side)');

// The probes a load balancer actually calls. Unauthenticated by necessity:
// a kubelet cannot sign the cluster HMAC.
const probe = async (port, path) => {
    const res = await fetch(`http://127.0.0.1:${port}${path}`);
    return `${res.status} ${JSON.stringify(await res.json())}`;
};
log(`GET /_sigx/health      -> ${await probe(survivors[0].port, '/_sigx/health')}`);
log(`GET /_sigx/health/ready -> ${await probe(survivors[0].port, '/_sigx/health/ready')}`);

// Drain: `beginStop` announces `leaving` BEFORE activations hand off, so the
// balancer stops sending while the silo is still alive and serving.
await survivors[1].placement.beginStop();
log(`draining silo ${survivors[1].placement.identity.siloId}:`);
log(`  GET /_sigx/health      -> ${await probe(survivors[1].port, '/_sigx/health')}`);
log(`  GET /_sigx/health/ready -> ${await probe(survivors[1].port, '/_sigx/health/ready')}`);
log('(live 200 but ready 503 — drain it, do NOT restart it)');

if (serve) {
    step('6. Serving — the cluster stays up');
    log('Point the dashboard at any surviving silo:\n');
    for (const m of members.filter((m) => m.server.listening)) {
        log(
            `  pnpm --filter counter-example exec sigx actors top ` +
                `--url http://127.0.0.1:${m.port} --secret ${opsSecret}`
        );
    }
    log('\nOr one snapshot:');
    log(
        `  pnpm --filter counter-example exec sigx actors stats ` +
            `--url http://127.0.0.1:${survivors[0].port} ` +
            `--secret ${opsSecret} --json | jq .cluster.totals`
    );
    log('\nDriving steady traffic. Ctrl+C to stop.');

    // Enough shape to make the dashboard worth looking at: a hot grain that
    // builds queue depth, a spread of cold ones, and a steady trickle of
    // failures so the error panel is not empty.
    let tick = 0;
    const traffic = setInterval(() => {
        tick++;
        const from = survivors[tick % survivors.length].silo;
        const work = [
            from.actor(Counter, 'hot').increment(1),
            from.actor(Counter, 'hot').increment(1),
            from.actor(Counter, `cold-${tick % 25}`).increment(1)
        ];
        // One in seven calls asks for an actor type that does not exist, so
        // `errors.byKind` shows a real `method-not-found` rather than zeroes.
        if (tick % 7 === 0) {
            work.push(
                from
                    .dispatch({ type: 'Counter', key: 'hot' }, 'nope', [], {
                        callChain: [],
                        callId: `demo-${tick}`
                    })
                    .catch(() => {})
            );
        }
        Promise.allSettled(work);
    }, 250);
    traffic.unref?.();

    await new Promise((resolve) => {
        const stop = () => resolve();
        process.once('SIGINT', stop);
        process.once('SIGTERM', stop);
    });
    clearInterval(traffic);
    log('\nstopping…');
}

step(serve ? '7. Graceful shutdown of the survivors' : '6. Graceful shutdown of the survivors');
await Promise.all(
    members.filter((m) => m !== owner).map((m) => m.app.stop({ timeoutMs: 2000 }))
);
// The crashed owner's server is already closed — closing twice
// reports ERR_SERVER_NOT_RUNNING.
members.filter((m) => m.server.listening).forEach((m) => m.server.close());
log('\nCLUSTER DEMO COMPLETE — one actor system across three HTTP silos.');
