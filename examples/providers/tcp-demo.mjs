/**
 * `@sigx/actors-tcp` — the host-to-host link swapped for one multiplexed
 * TCP connection per peer, on the same three-host cluster.
 *
 *     pnpm --filter providers-example tcp            # after `pnpm build`
 *     pnpm --filter providers-example tcp -- --http  # the same run over HTTP, to compare
 *
 * Nothing else changes: `memoryClusterHub()` and `memoryStorage()` are the
 * coordination and the database, exactly as in `examples/counter`. The
 * one line that differs is the `transport` option — and it is a CHAIN,
 * `[tcpTransport(), httpTransport()]`, never `tcpTransport()` alone: a
 * single transport is strict, so a peer publishing no `tcp` address would
 * be unreachable rather than reached over HTTP. During the rolling deploy
 * that introduces the transport, that peer is half the cluster.
 */
import { memoryStorage } from '@sigx/actors/host';
import { httpTransport, memoryClusterHub } from '@sigx/actors/cluster';
import { tcpTransport } from '@sigx/actors-tcp';
import { Counter } from './src/counter.actor.ts';
import { CROSS_HOST_COUNT, startCluster } from './src/cluster.ts';

const overHttp = process.argv.includes('--http');
const hub = memoryClusterHub();

const demo = await startCluster({
    label: 'memoryStorage()',
    storage: memoryStorage(),
    providers: () => hub.providers(),
    // Port 0: the transport binds an ephemeral port, reads it back after
    // listening and publishes it under `addresses.tcp` — that is the
    // `tcp=tcp://127.0.0.1:NNNNN` in the "listening on" lines above.
    // `host` pins the loopback interface; the default binds ALL of them.
    ...(overHttp
        ? {}
        : { transport: () => [tcpTransport({ port: 0, host: '127.0.0.1' }), httpTransport()] })
});

await demo.spread();
const { owner } = await demo.singleActivation();
await demo.crossHost(owner);

demo.step('Socket count — 32 concurrent calls into the owner from ONE peer');
const BURST = 32;
const via = demo.members.find((m) => m !== owner);
await Promise.all(
    Array.from({ length: BURST }, () => via.host.actor(Counter, demo.key('cart')).increment(1))
);
// The owner's HTTP listener, right after the burst. This is the number the
// transport exists to change: HTTP's pool opens one connection per
// in-flight request and keeps them alive; the TCP transport carries every
// call on the one connection it already holds to that peer, so the HTTP
// listener sees none of it.
const httpConnections = await new Promise((resolve, reject) =>
    owner.server.getConnections((error, count) => (error ? reject(error) : resolve(count)))
);
const { transportFallbacks } = via.placement.counters();
console.log(
    `owner ${demo.hostId(owner)}'s HTTP listener holds ${httpConnections} connection(s) after the burst`
);
if (overHttp) {
    console.log('(HTTP: one pooled connection per concurrent call, kept alive for the next one)');
} else {
    console.log(
        `(TCP: the calls rode ${demo.hostId(via)}'s single connection to ` +
            `${owner.placement.descriptor().addresses?.tcp}; fallbacks to http: ${transportFallbacks})`
    );
    if (httpConnections !== 0) throw new Error('host-to-host traffic reached the HTTP listener!');
    if (transportFallbacks !== 0) throw new Error('a call fell back to HTTP!');
}

// The burst moved the counter on, so failover has to read THAT back.
const { survivor } = await demo.failover(owner, CROSS_HOST_COUNT + BURST);
await demo.report(survivor);
await demo.stop();
console.log(
    `\nTCP DEMO COMPLETE — the same cluster over ${overHttp ? 'HTTP' : 'one TCP connection per peer'}.`
);
