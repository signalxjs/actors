/**
 * Multi-silo cluster demo — three silos on real localhost HTTP sockets,
 * one actor system. Uses the in-process memory cluster hub (so it needs no
 * Redis) with shared storage, and drives the same Counter actor the rest
 * of the example uses. Shows: placement spread, the single-activation
 * guarantee under cross-silo calls, a watch stream consumed from a
 * non-owner silo, and crash failover with state recovered from storage.
 *
 * Run after `pnpm build`:   node examples/counter/cluster-demo.mjs
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
import { defineActorApp, health, memoryStorage } from '@sigx/actors/silo';
import { createAppHandler } from '@sigx/actors/node';
import { cluster, clusterStats, memoryClusterHub } from '@sigx/actors/cluster';
import { Counter } from './src/counter.actor.ts';

const SEP = String.fromCharCode(0); // actorId separator: `${type}${SEP}${key}`
const PORTS = (process.env.CLUSTER_DEMO_PORTS ?? '5391,5392,5393')
    .split(',')
    .map((p) => Number(p.trim()));
const secret = 'demo-secret';
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
    const app = defineActorApp({ actors: [Counter], storage })
        .use(plugin)
        .use(health());

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

step('6. Graceful shutdown of the survivors');
await Promise.all(
    members.filter((m) => m !== owner).map((m) => m.app.stop({ timeoutMs: 2000 }))
);
// The crashed owner's server is already closed — closing twice
// reports ERR_SERVER_NOT_RUNNING.
members.filter((m) => m.server.listening).forEach((m) => m.server.close());
log('\nCLUSTER DEMO COMPLETE — one actor system across three HTTP silos.');
