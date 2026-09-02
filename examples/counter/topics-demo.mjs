/**
 * Topics with no framework — `ctx.publish` on one actor, `subscriptions:`
 * on another, one host, nothing else. The pattern belongs to the runtime;
 * the sigx app in examples/chat only happens to sit on top of it.
 *
 *     pnpm --filter counter-example topics    # after `pnpm build`
 *
 * Every step throws on a wrong result, so this file is an assertion suite
 * that narrates itself. README.md has the walkthrough.
 */
import { defineActorApp, memoryStorage } from '@sigx/actors/host';
import { Gate, gatePassed } from './src/gate.actor.ts';
import { Tally } from './src/tally.actor.ts';

const log = (...args) => console.log(...args);
const step = (title) => log(`\n=== ${title} ===`);

// The subscriber set is a pure function of the deploy: Tally subscribes
// because it is in `actors`, not because anything registered it.
const app = defineActorApp({ actors: [Gate, Tally], storage: memoryStorage() });
const host = await app.start();

step('1. A turn publishes — the report says who heard it');
const first = await host.actor(Gate, 'north').pass();
log('north.pass() →', first);
if (first.subscribers !== 1 || first.delivered !== 1) throw new Error('nobody heard the publish');
log('(subscribers=1 is the Tally TYPE; delivered=1 is its one aggregate instance)');
await host.actor(Gate, 'north').pass();
await host.actor(Gate, 'south').pass();

step('2. Nothing ever called the subscriber — the publish activated it');
log('activations:', host.stats().perType);
const tally = await host.actor(Tally, 'all').totals();
log(`Tally "all" →`, tally);
if (tally.deliveries !== 3 || tally.byGate.north !== 2 || tally.byGate.south !== 1) {
    throw new Error(`deliveries lost: ${JSON.stringify(tally)}`);
}
log(`(every gate's key mapped to "all", and event.publisher named the last one: ${tally.lastFrom})`);

step('3. Publishing from outside any actor');
// The same fan-out from plain server code — a script, a serverFn, a cron.
const report = await host.publish(gatePassed('side-door'), { count: 1 });
log('host.publish() →', report);
const after = await host.actor(Tally, 'all').totals();
log(`Tally "all" → deliveries=${after.deliveries} lastFrom=${after.lastFrom}`);
if (after.deliveries !== 4 || after.lastFrom !== 'outside any actor') {
    throw new Error(`host.publish not delivered: ${JSON.stringify(after)}`);
}
log('(no publishing turn, so the event carries no publisher — the handler saw that)');

step('4. No subscriber in the deploy, no delivery — and no error');
// Same Gate, a host started WITHOUT Tally. (Stopped first: one process
// carries one ambient host, and starting a second over it is a warning.)
await host.stop();
const lonely = await defineActorApp({ actors: [Gate], storage: memoryStorage() }).start();
const unheard = await lonely.actor(Gate, 'north').pass();
log('north.pass() on a host without Tally →', unheard);
if (unheard.subscribers !== 0 || unheard.delivered !== 0) throw new Error('phantom subscriber');
await lonely.stop();

log('\nTOPICS DEMO COMPLETE — publish, subscribe and activate-on-delivery, with no framework.');
