/**
 * Multi-silo cluster demo — three silos on real localhost HTTP sockets,
 * one actor system. Uses the in-process memory cluster hub (so it needs no
 * Redis) with shared storage, and drives the same Counter actor the rest
 * of the example uses. Shows: placement spread, the single-activation
 * guarantee under cross-silo calls, a watch stream consumed from a
 * non-owner silo, and crash failover with state recovered from storage.
 *
 * Run after `pnpm build`:   node examples/counter/cluster-demo.mjs
 * Needs Node >= 22.18 (built-in type stripping for the .ts actor import,
 * same as server.mjs). Ports default to 5391-5393; override with
 * CLUSTER_DEMO_PORTS=7001,7002,7003.
 */
import { createServer } from 'node:http';
import { createSilo, memoryStorage } from '@sigx/actors/silo';
import { createActorHandler } from '@sigx/actors/node';
import {
    clusterPlacement,
    handleSiloRequest,
    matchesSiloRequest,
    memoryClusterHub
} from '@sigx/actors/cluster';
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

/**
 * Node req/res ↔ WinterCG bridge for the INTERNAL silo-to-silo mount.
 * The abort wiring matters: a peer cancelling a cross-host stream closes
 * the socket, and that must reach the actor's generator (keep-alive
 * release) — so the request's AbortSignal is tied to the connection.
 */
function bridgeSiloRequest(req, res, url, silo, placement) {
    const abort = new AbortController();
    res.on('close', () => abort.abort());
    const headers = new Headers();
    for (const [name, value] of Object.entries(req.headers)) {
        if (value === undefined) continue;
        for (const v of Array.isArray(value) ? value : [value]) headers.append(name, v);
    }
    const method = req.method ?? 'GET';
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
        void handleSiloRequest(
            new Request(url, {
                method,
                headers,
                signal: abort.signal,
                ...(method === 'GET' || method === 'HEAD'
                    ? {}
                    : { body: Buffer.concat(chunks) })
            }),
            { silo, placement, secret }
        ).then(async (response) => {
            res.writeHead(response.status, Object.fromEntries(response.headers));
            if (response.body) {
                try {
                    for await (const chunk of response.body) res.write(chunk);
                } catch {
                    // Peer disconnected mid-stream; the abort above already
                    // cancelled the actor-side generator.
                }
            }
            res.end();
        });
    });
}

const members = []; // { silo, placement, server, port }
for (const port of PORTS) {
    const placement = clusterPlacement({
        ...hub.providers(),
        advertise: `http://127.0.0.1:${port}`,
        secret
    });
    const silo = createSilo({ actors: [Counter], storage, placement });
    const actorHandler = createActorHandler({ silo });
    const server = createServer((req, res) => {
        const url = new URL(req.url ?? '/', `http://127.0.0.1:${port}`);
        if (matchesSiloRequest(new Request(url, { method: req.method ?? 'GET' }))) {
            bridgeSiloRequest(req, res, url, silo, placement);
            return;
        }
        void actorHandler(req, res, () => res.writeHead(404).end());
    });
    await new Promise((r) => server.listen(port, r));
    await silo.start();
    members.push({ silo, placement, server, port });
    log(`silo ${placement.identity.siloId} listening on :${port}`);
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
const watching = (async () => {
    for await (const s of nonOwner.silo.actor(Counter, 'cart').watch()) {
        snapshots.push(s.count);
        log(`  watch (via ${nonOwner.placement.identity.siloId}): count=${s.count}`);
        if (snapshots.length >= 3) break;
    }
})();
await new Promise((r) => setTimeout(r, 50));
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

step('5. Graceful shutdown of the survivors');
await Promise.all(
    members.filter((m) => m !== owner).map((m) => m.silo.stop({ timeoutMs: 2000 }))
);
members.forEach((m) => m.server.close());
log('\nCLUSTER DEMO COMPLETE — one actor system across three HTTP silos.');
