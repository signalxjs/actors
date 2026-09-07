# @sigx/actors-workflow

Durable workflows for [`@sigx/actors`](https://github.com/signalxjs/actors):
one actor per run, definitions as data, state as a durable event log.

A definition is JSON — nodes, not functions — so it can be stored,
versioned and read by a run that resumes days later on a host that has
never seen the process that wrote it. A run pins the version it started
on, so editing a workflow never changes a run already in flight.

```sh
pnpm add @sigx/actors-workflow @sigx/actors
```

Requires `@sigx/actors` as a peer dependency, and Node 20.19+ or 22.12+.

```ts
import { defineWorkflow } from '@sigx/actors-workflow';

const engine = defineWorkflow({
    definitions: [{
        name: 'order', version: 1, start: 'charge',
        nodes: {
            charge: { type: 'task', handler: 'charge', input: { amount: '${amount}' },
                      assignTo: 'receipt', retry: { maxAttempts: 3, backoffMs: 500 }, next: 'ship' },
            ship:   { type: 'task', handler: 'ship', next: 'done' },
            done:   { type: 'end' }
        }
    }],
    handlers: {
        charge: async (input, ctx) => charge(input, ctx.idempotencyKey),
        ship: async () => ship()
    }
});

// Register engine.actors with defineActorApp, then:
await host.actor(engine.run, 'order-1').start('order', { amount: 42 });
```

Two things worth knowing before you use it. A task handler **may run
twice** for one node — the attempt is recorded before the call, so a host
that dies mid-task re-runs it rather than skipping work it may not have
done; `ctx.idempotencyKey` is stable per run and node and is what to
deduplicate on. And a delay at or above the threshold rides a durable
reminder and the run **leaves memory**, which is what lets a fleet hold
many sleeping runs at once.

Full documentation: <https://sigx.dev/actors>.
