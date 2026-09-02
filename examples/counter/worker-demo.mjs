/**
 * `defineWorker` where you can see it work: the same method under a worker
 * pool and under a plain actor, two concurrent calls to the SAME key each,
 * and the one property that separates them — the pool's calls overlap, the
 * actor's take turns.
 *
 *     pnpm --filter counter-example worker    # after `pnpm build`
 *
 * Why the difference exists, and when it buys nothing, is in
 * src/resolver.actor.ts and README.md. Every step throws on a wrong
 * result, so this file is an assertion suite that narrates itself.
 */
import { defineActorApp, memoryStorage } from '@sigx/actors/host';
import { Resolver, SerialResolver } from './src/resolver.actor.ts';

const log = (...args) => console.log(...args);
const step = (title) => log(`\n=== ${title} ===`);
const LATENCY = 300; // the simulated upstream, per call

const app = defineActorApp({ actors: [Resolver, SerialResolver], storage: memoryStorage() });
const host = await app.start();

/** Two calls to ONE key, fired together. Each reports what it saw on entry. */
async function burst(client, label) {
    const t0 = performance.now();
    const [a, b] = await Promise.all([client.lookup(LATENCY), client.lookup(LATENCY)]);
    const ms = Math.round(performance.now() - t0);
    log(
        `  ${label}: members=[${a.member},${b.member}] ` +
            `overlapping=[${a.overlapping},${b.overlapping}]  wall=${ms}ms for 2×${LATENCY}ms`
    );
    return { a, b, ms };
}

step('1. defineActor — one activation per key, one turn at a time');
const serial = await burst(host.actor(SerialResolver, 'k'), 'SerialResolver "k"');
if (serial.a.overlapping !== 0 || serial.b.overlapping !== 0) {
    throw new Error('an actor let two turns overlap!');
}
log('(the second call waited for the first — about twice the latency)');

step('2. defineWorker — a pool per key; the second call gets a second member');
const pooled = await burst(host.actor(Resolver, 'k'), 'Resolver "k"');
if (pooled.a.overlapping + pooled.b.overlapping !== 1) {
    throw new Error('the pool serialized two calls to one key!');
}
if (pooled.a.member === pooled.b.member) throw new Error('one member served both calls?');
log('(both were inside at once — about one latency, on two activations of one key)');

step('3. Growth is pressure-driven, and capped at maxLocal');
log('activations so far:', host.stats().perType);
// Eight at once against `maxLocal: 4`: four run, four queue, none is refused.
const eight = await Promise.all(
    Array.from({ length: 8 }, () => host.actor(Resolver, 'k').lookup(LATENCY))
);
const members = new Set(eight.map((r) => r.member)).size;
const peak = Math.max(...eight.map((r) => r.overlapping)) + 1;
log(`8 concurrent calls on "k" → ${members} members, peak ${peak} in flight`);
log('activations now:', host.stats().perType);
if (members !== 4 || peak !== 4) throw new Error(`cap not honoured: ${members} members, peak ${peak}`);
log('(the cap counts per key per host — backpressure past it queues, like an actor)');

step('4. No pressure, no growth — sequential calls stay on one member');
const seq = [];
for (let i = 0; i < 3; i++) seq.push(await host.actor(Resolver, 'quiet').lookup(20));
log(`3 sequential calls on "quiet" → members=[${seq.map((r) => r.member)}]`);
if (new Set(seq.map((r) => r.member)).size !== 1) throw new Error('an idle pool grew');

await host.stop();
log('\nWORKER DEMO COMPLETE — a pool overlaps, an actor serializes, and the cap holds.');
