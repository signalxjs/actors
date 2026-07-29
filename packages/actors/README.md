# @sigx/actors

Orleans-style **virtual actors** for SignalX: addressable, single-threaded,
persistent server objects that integrate with sigx's server layer — actor
calls ride the same wire protocol, codec, and security posture as `serverFn`.

```ts
// cart.actor.ts
import { defineActor } from '@sigx/actors';
import { requireUser } from './guards.server';

export const CartActor = defineActor({
    type: 'Cart',
    use: [requireUser],
    state: () => ({ items: [] as Item[] }),
    methods: (ctx) => ({
        async addItem(item: Item) {
            ctx.state.items.push(item);   // deep signal — just mutate
            await ctx.save();             // Orleans WriteStateAsync
            return ctx.state.items.length;
        }
    })
});
```

```ts
// Anywhere — browser, serverFn, SSR render. Same expression.
import { actor } from '@sigx/actors';
const count = await actor(CartActor, cartId).addItem(item);
```

The actor **need not exist**: the first call activates it (loading its state
from storage), and idle actors deactivate automatically. No create, no
destroy, no connection management — that is the virtual-actor model.

## The model, in five guarantees

1. **Addressable.** An actor is `(type, key)`. `actor(CartActor, 'user-42')`
   always reaches *the* `user-42` cart — one activation per key.
2. **Single-threaded.** One turn (method call) at a time per activation.
   Plain mutation on `ctx.state` is race-free; no locks, ever.
3. **`await` holds the mailbox.** A turn ends when the method's promise
   settles — an awaited `fetch` inside a method blocks every queued call to
   that actor until it resolves. This is the non-reentrant Orleans default
   and the model's core trade: state safety over intra-actor concurrency.
   (Dev builds warn when a turn exceeds `slowTurnMs`.)
4. **Persistent.** `ctx.save()` writes state through the pluggable
   `ActorStorage` (etag optimistic concurrency); activation loads it back.
   A conflicting writer faults the stale activation
   (`ActorStateConflictError`) — the next call loads the winning state.
5. **Deadlock-detected.** Every call carries its chain; `A → B → A` into a
   non-reentrant actor **throws `ActorDeadlockError` immediately** with the
   full chain, instead of Orleans's hang-until-timeout. `reentrant: true`
   allows call-chain re-entry (the cycle runs inline against your own turn).

## Setup

```sh
pnpm add @sigx/actors
```

**Vite app** — add the plugin; it runs a dev silo for you and mounts the
endpoint at `/_sigx/actor`:

```ts
// vite.config.ts
import { sigxActors } from '@sigx/actors/vite';
export default { plugins: [sigxActors({ app: '/src/actors.app.ts' })] };
```

Actors live in **`*.actor.ts`** modules. The build swaps them wholesale for
typed client stubs — implementations never reach the browser (values are
swapped, types are not, so the client proxy is fully typed).

`sigxActors({ app })` points dev at the same app module your production
entry imports, so storage, placement, codec handlers, defaults and every
plugin are identical in both. The app module gets its registry from
`virtual:sigx-actors`; add its types once, next to your other Vite types:

```ts
// src/env.d.ts
/// <reference types="@sigx/actors/vite-client" />
```

The app module leaves out its registry, so it imports nothing Vite-specific
and loads under any runtime — which is what lets a plain-Node entry share
it, and what makes it safe for actor modules to import the bound
`defineActor` from it:

```ts
// src/actors.app.ts — no `actors`, no virtual import
export const app = defineActorApp({ storage: fileStorage({ dir: '.actors' }) });
export const { defineActor } = app;
```

Under Vite the plugin supplies the registry it already builds (its loaders
go through the module runner, so HMR keeps working). Anywhere else, name the
actors:

```ts
// server.mjs — the SAME app module
const silo = await app.withActors([Counter]).start();
```

`withActors` throws if the app already declared `actors`, so a host can
never silently replace what the author configured. `examples/counter` runs
exactly this shape in dev and in production.

**Production server entry** — explicit composition, the sigx idiom:

```ts
import { createSilo } from '@sigx/actors/silo';
import { handleActorRequest, matchesActorRequest } from '@sigx/actors/server';
import { actors } from './dist/server/sigx-actors.js'; // build-emitted registry

const silo = createSilo({ actors, storage });
await silo.start();

export default {
    async fetch(request: Request): Promise<Response> {
        if (matchesActorRequest(request)) return handleActorRequest(request, { silo });
        if (matchesServerFn(request))     return handleServerFnRequest(request, { resolve });
        return documentHandler(request);
    }
};
```

Node servers use `createActorHandler` / `attachSignalHandlers` from
`@sigx/actors/node` (see `examples/counter/server.mjs`).

## The app: one config, many runtimes

`createSilo` stays the low-level primitive, but it takes exactly ONE
`placement` and ONE `storage`, and `ActorPlacement.bind()` is its only
lifecycle-hook shape — so two things that both want `beforeActivate` cannot
coexist. `defineActorApp` is the composition root that fixes that: it folds
every plugin's contributions into the single placement, storage and context
`createSilo` already understands.

```ts
// src/actors.app.ts — one typed source of truth
import { defineActorApp } from '@sigx/actors/silo';
import { fileStorage } from '@sigx/actors/node';

export const app = defineActorApp({
    actors,
    storage: fileStorage({ dir: '.actors' }),
    defaults: { idleAfterMs: 60_000 }
}).use(metrics());

/** Bound to this app's plugin set — import it from your actor modules. */
export const { defineActor } = app;
```

```ts
const silo = await app.start();   // builds the silo, starts it, runs onStart
await app.stop();                 // drains the silo, then onStop in reverse
```

The app is an inert **description** until `start()`, which is what lets the
same module be started by a Node entry, the Vite dev server, or a Worker.

A started app is **single-use**: `start()` is idempotent while running, but
after `stop()` it refuses to restart (a plugin placement mints its identity
per run — a cluster silo id is gone once its membership entry is), so build a
new app instead. A start that *fails* is the exception: the rejection is not
cached, so fixing the cause and calling `start()` again really retries.

### Writing a plugin

`setup()` receives a registry. Everything composes across plugins except
`setPlacement`, which is exclusive by nature — a second claim throws, naming
both plugins.

```ts
import type { ActorPlugin } from '@sigx/actors/silo';

interface Logger { info(message: string): void }

export function logging(logger: Logger): ActorPlugin<{ log: Logger }> {
    return {
        name: 'logging',
        setup(registry) {
            registry.extendContext(() => ({ log: logger }));
            registry.onBeforeActivate((ref) => logger.info(`activating ${ref.type}/${ref.key}`));
            registry.useDispatch((next) => ({
                dispatch: async (ref, method, args, call) => {
                    logger.info(`${ref.type}#${method}`);
                    return next.dispatch(ref, method, args, call);
                },
                // Forward streaming — `dispatchStream` is optional, so a
                // middleware that omits it silently breaks every
                // `streams:` method. Dev-warns if you forget.
                ...(next.dispatchStream && {
                    dispatchStream: (ref, method, args, call) =>
                        next.dispatchStream!(ref, method, args, call)
                })
            }));
        }
    };
}
```

The `ActorPlugin<{ log: Logger }>` type argument is what makes `ctx.log`
**typed inside every actor** that imports the app-bound `defineActor` — no
global declaration merging, so the additions stay per-app:

```ts
// src/counter.actor.ts
import { defineActor } from './actors.app';

export const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        increment(by: number) {
            ctx.log.info('increment');   // typed, contributed by .use(logging(...))
            return (ctx.state.count += by);
        }
    })
});
```

| Registry hook | Composes? | Notes |
|---|---|---|
| `addTypeHandlers` | concatenated | codec handlers for state persistence |
| `decorateStorage` | chained | last registered is **outermost** |
| `setPlacement` | **exclusive** | a factory, run once the silo exists; a second claim throws, naming both plugins |
| `onBeforeActivate` | in order | throwing **refuses** the activation |
| `onAfterDeactivate` | reverse order | errors caught per hook and dev-logged |
| `useDispatch` | outside-in | first registered is **outermost**; must forward `dispatchStream` |
| `onStart` / `onStop` | in order / reverse | `onStop` runs *after* the drain |
| `route` | collected | exposed as `app.routes` for adapters |
| `extendContext` | merged | never overwrites a built-in `ctx` member |

A placement's own hooks **bracket** the plugins': its `beforeActivate` (a
cluster's directory claim) runs first and its `afterDeactivate` (the release)
runs last, so plugin hooks always observe an activation the placement already
owns.

### Custom placement

Two independent axes, the same split Orleans draws:

- **The backend** — *who hosts an actor at all*: the local host, a cluster,
  Durable Objects. One per app, claimed with `setPlacement`.
- **The strategy** — *which silo a new activation goes to*, Orleans's
  `IPlacementDirector`. That is `PlacementPolicy` from `@sigx/actors/cluster`;
  ship your own `choose(ref, view, self)` alongside the built-in
  `randomPlacementPolicy()`, `consistentHashPolicy()` and
  `preferLocalPolicy()`.

A strategy can be declared **on the actor**, which is Orleans's placement
attribute and beats the central `typePolicies` map:

```ts
export const Session = defineActor({
    type: 'Session',
    placement: preferLocalPolicy(),   // pin hot session-shaped types local
    // ...
});
```

`setPlacement` takes a **factory**, not an instance, precisely so a backend
can read those declarations — it runs once the silo exists, so the context
can resolve definitions:

```ts
registry.setPlacement((ctx) => clusterPlacement({
    ...providers,
    // per-type strategies resolve lazily, since a `virtual:sigx-actors`
    // registry only loads a type's module on demand
    definition: ctx.definition,
    secret,
}));
```

Precedence for a new activation: `defineActor({ placement })` →
`clusterPlacement({ typePolicies })` → `clusterPlacement({ policy })` →
uniform random.

A declared strategy the cluster **cannot use** — no `choose()` — is an error,
not a fallback: silently placing a grain somewhere other than where its author
declared is the kind of failure that leaves no signal pointing at its cause. A
strategy intended for a *different* placement backend should say so, and is
then ignored in silence:

```ts
placement: { name: 'my-do-strategy', backend: 'durable-objects' }
```

## Other runtimes

`createFetchHandler(app)` is the portable entry — the public actor endpoint
plus every plugin route as one `(Request) => Response`:

```ts
import { createFetchHandler } from '@sigx/actors/server';

await app.start();
Deno.serve(createFetchHandler(app));           // Deno
export default { fetch: createFetchHandler(app) };  // Bun, Workers
```

`@sigx/actors/node`'s `createAppHandler` stays the Node mount — it keeps
core's connect adapter for the public endpoint (backpressure-aware body
pumping) rather than routing everything through the generic bridge.

### Pluggable reminders

Durable reminders are a seam too. The default `shardedReminders()` keeps the
table in `ActorStorage` under a reserved type, split into 16 hash shards that
silos divide between them — which assumes **many actors per silo**:

```ts
defineActorApp({ actors, reminders: shardedReminders() })   // the default
```

Where that assumption is false, replace it. Under Cloudflare's
one-Durable-Object-per-actor model each actor's reminders live in its own DO
and fire from its own alarm, so there is nothing to shard and nothing to
poll. An implementation gets `bind({ storage, scheduler, tickMs, ownsShard,
deliver })` once before `start()`, mirroring `ActorPlacement.bind()`.

### The clock seam

**Background** work — the idle sweeper, the reminder tick, `ctx.timer`,
write-behind flushes — runs through an `ActorScheduler`. Those are the jobs
that must keep running *between* requests, so they are the ones a runtime
has to redirect. Call deadlines and the shutdown drain stay on host timers,
since they are scoped to an in-flight request or stop:

```ts
defineActorApp({ actors, scheduler: timerScheduler() })   // the default
```

That matters for two things. Tests can drive time exactly:

```ts
const scheduler = manualScheduler();
const silo = createSilo({ actors, scheduler, defaults: { idleAfterMs: 0 } });
scheduler.advance(60_000);   // an hour of sweeps, instantly
```

And it is what makes a runtime with **no background execution** possible at
all: a Cloudflare Worker only runs while handling a request, so an interval
registered at startup never fires. That is an architectural difference, not
something a polyfill can hide — hence a seam rather than a shim.

## Shipping actors in a package

An app registers actors from anywhere — `withActors([Greeter, Presence])`.
The build is the part that needs care: `sigxActors()` only transforms
first-party source (`node_modules` is excluded), so a **package must do its
own client swap**, through its exports map:

```json
{
    "exports": {
        ".": {
            "types": "./dist/server.d.ts",
            "browser": "./dist/client.js",
            "import": "./dist/server.js"
        }
    }
}
```

```ts
// client.ts — the browser half
import { __actorRef } from '@sigx/actors/client';
import type { Greeter as GreeterDef } from './server';

// Types come from the real definition; the value is a ref. The `import
// type` is erased, so no implementation reaches the browser.
export const Greeter = __actorRef('acme/greeter', '/_sigx/actor', ['watch']) as typeof GreeterDef;
```

That is the same swap the Vite plugin performs for `*.actor.ts`, done by
static resolution instead — so it works with any bundler.

Three things are on the package author, because the consuming app's build
never sees the source:

- **Guards.** `requireGuards` cannot inspect a package, so declare `use` or
  `unguarded` yourself. The silo dev-warns for a registered actor that
  declares neither.
- **Stream names** in the ref must match the definition; they drive wire
  routing.
- **`type` is public API.** It is the wire, directory *and* storage key, so
  renaming it breaks deployed state. Two different actors claiming one type
  is refused at startup. Namespace it after the package (`acme/greeter`), but
  **not** with the npm scope form — a `type` starting with `@` or `$` is
  refused: those heads belong to the runtime's own data keys (`@actor`) and
  mounts (`$live`).

## Guards

Actor methods are as security-sensitive as server functions, so the build
**requires a decision** per actor: a `use: [...]` guard chain, or a literal
`unguarded: true`. Guards are core's `ServerFnGuard` shape
(`(rq, info) => void`, veto by throwing), run on **every transport** — the
wire endpoint and in-process `actor()` calls — and always **outside the
mailbox**, so a slow auth check never occupies the actor's turn.
`methodUse: { methodName: [...] }` adds per-method chains. Actor-to-actor
calls (`ctx.actor`) are intra-system and do not re-run guards.

The endpoint-level `guard` option on `handleActorRequest` remains the
wire-only backstop, exactly like the serverFn endpoint's.

## Persistence

- Default is **explicit**: only `ctx.save()` writes — a method that returns
  success has persisted what it acked.
- `persistence: { mode: 'write-behind', debounceMs }` saves automatically
  after mutating turns. Sharp edge, stated plainly: **acked ≠ persisted**
  under write-behind — use it for lossy-tolerant state only. Pending writes
  flush on deactivation and shutdown.
- Rich types (`Date`, `Map`, `Set`, `BigInt`, `URL`, `RegExp`, plus your
  `serverPlugin({ types })` handlers) survive storage and the wire — the
  same `@sigx/serialize` vocabulary everywhere.
- Providers: `memoryStorage()` (tests/dev), `fileStorage({ dir })`
  (dev; one cat-able JSON file per actor). Implement `ActorStorage`
  (load/save/clear with etags) for real databases.
- Under Vite, a `fileStorage` `dir` inside the project root belongs in
  `server.watch.ignored` — actor state is not source, and a save is a temp
  file plus a rename, which the HMR file reader races and reports as
  `ENOENT … .tmp`:

  ```ts
  export default defineConfig({
      server: { watch: { ignored: ['**/.actors/**'] } }
  });
  ```

## Timers & reminders

- `ctx.timer(name, cb, { due, period, keepAlive })` — **volatile**: ticks run
  as ordinary mailbox turns (coalesced under load) and die with the
  activation. Timers don't keep an actor alive unless `keepAlive: true`.
- `ctx.reminders.set(name, { due, period })` — **durable**: stored through
  `ActorStorage`, fired by the silo's scheduler, and they **re-activate an
  idle or restarted actor** (`onReminder(ctx, name)`). Minimum period 60s;
  coarse resolution ("at or after"); at-most-once per tick.

## Streams

Declare server→client streams in the `streams:` factory as async generators;
clients get them as `AsyncIterable` over NDJSON:

```ts
streams: (ctx) => ({
    async *watch() {
        yield* ctx.changes({ initial: true });
    }
})
// client: for await (const s of actor(CartActor, id).watch()) render(s);
```

Stream bodies are **observers, not turns**: they run detached from the
mailbox (a stream that waited on its own actor's next turn while holding the
mailbox would self-deadlock), so they must read `ctx.snapshot()` /
`ctx.changes()` — never mutate live state. Dev builds warn when a stream
body reads live `ctx.state`. An open stream keeps the activation alive;
consumer disconnect runs the generator's `finally`. `ctx.changes()` yields a
detached snapshot after every mutating turn (bounded buffer, drop-oldest).

- **Use `changes({ initial: true })` to seed**, not a `yield ctx.snapshot()`
  prologue. The prologue subscribes only once the consumer resumes past that
  first yield, so every mutation in between is lost — and the snapshot it
  yielded is already stale. `{ initial: true }` queues the current snapshot
  in the same synchronous call that registers the subscription, leaving no
  gap.

- The `streams:` factory must not touch `ctx` while *constructing* the table
  (its method names are read at definition time); inside generator bodies,
  anything goes. `computed`/`watch` setup belongs in `methods:`.

- It runs **once per subscription**, with a context of its own. That is what
  lets a disconnect close the feeds a body opened: an async generator parked
  inside `ctx.changes()` is suspended at an internal await, where the spec
  queues `return()` instead of running it, so the subscription has to be
  closable from outside the body. Nothing an author writes changes — the
  factory is a table constructor, not a place for per-activation state.

## Tasks (long-running operations)

A method call holds the mailbox until it settles — fine for milliseconds,
wrong for a sync job or an AI workflow run. Declare that kind of work in the
`tasks:` factory and start it with `ctx.tasks.start(name, input?)`: the body
runs **detached from the mailbox**, so ordinary reads, streams and watches
keep answering while it works.

```ts
const Sync = defineActor({
    type: 'Sync',
    unguarded: true,
    state: () => ({ done: 0, total: 0, phase: 'idle' as string }),
    methods: (ctx) => ({
        begin: (total: number) => ctx.tasks.start('run', total),
        status: () => ctx.snapshot(),
        stop: () => ctx.tasks.cancel('run')
    }),
    tasks: (ctx) => ({
        async run(total: number) {
            await ctx.turn((c) => { c.state.phase = 'running'; c.state.total = total; });
            for (let i = 0; i < total; i++) {
                ctx.abortSignal.throwIfAborted();
                await syncOne(i, { signal: ctx.abortSignal });
                await ctx.turn((c) => { c.state.done = i + 1; });
            }
            await ctx.turn(async (c) => { c.state.phase = 'done'; await c.save(); });
        }
    })
});
```

The rules, and why they hold the actor model together:

- **State only through `ctx.turn(fn)`.** A task body is detached, so it gets
  no `state`/`save()` — `turn()` enqueues `fn` as one ordinary serialized
  mailbox turn with the full context. Every mutation stays race-free, and
  everything downstream of a turn (change feeds, watches, write-behind) works
  unmodified. Reads in the body use `ctx.snapshot()` / `ctx.changes()`.
- **`ctx.abortSignal` in a task is the RUN's signal.** It fires on
  `ctx.tasks.cancel(name)` (reason `'cancelled'`) and on deactivation for
  any reason (reason: the `DeactivationReason`) — *before* the mailbox
  drain. Deactivation gives signalled tasks a bounded grace
  (`taskGraceMs`, default 10s) with the mailbox still open, so a
  winding-down task can run one final `turn()` checkpoint.
- **`cancel` is a request, not a join.** It aborts and returns; the run
  leaves `ctx.tasks.list()` when its body settles. (Awaiting settlement from
  a method turn would deadlock with the task's own wind-down `turn()`.)
- **A running task keeps the grain alive** — the idle sweeper skips it, like
  an open stream.
- **`start` is single-flight per name** and resolves when the body is
  *launched*, not finished. A task that throws is terminal — no automatic
  retry; that policy belongs to the layer above.
- **No wire surface.** Start/cancel/status go through your own methods, so
  your guard chain governs them like any other call.

Durable crash-resume (a ledger + liveness reminder that restarts a dead
silo's tasks elsewhere) ships next — today a task dies with its activation
and restarting is the caller's business.

## Lifecycle

- `onActivate(ctx)` / `onDeactivate(ctx, reason)` hooks; an `onActivate`
  throw fails all parked callers and forgets the activation (nothing is
  remembered — the next call retries from scratch).
- Idle actors deactivate after `idleAfterMs` (default 20 min; per-actor
  override). `ctx.deactivate()` = Orleans `DeactivateOnIdle`: finish the
  queue, then go.
- `silo.stop()` drains every mailbox, flushes persistence, ends open streams
  and rejects new external calls; `attachSignalHandlers(silo)` wires it to
  SIGTERM/SIGINT.
- Deactivation fires `ctx.abortSignal` **first** — before draining the
  mailbox — so a parked turn or a running task can observe it and wind down
  inside the drain window instead of holding it hostage.
- Calls that arrive during deactivation wait and land on a fresh activation.
- External calls get a deadline (`callTimeoutMs`, default 30s) — on expiry
  the **caller** gets `ActorCallTimeoutError`; the turn itself always runs
  to completion.

### Seeing which grains are live

`silo.stats()` gives you the counts. `silo.activations()` gives you the
grains themselves — bounded, sorted, and safe to poll:

```ts
silo.activations({ sortBy: 'queued', limit: 20 });
// [{ type: 'Cart', key: 'user-42', queued: 7, ageMs: 812_004,
//    idleMs: 0, keptAlive: false }, …]
```

`sortBy` picks which end you care about: `'queued'` (default) is the hot
grains, `'age'` the long-lived ones, `'idle'` the next sweep's candidates.
`type` filters. Ties break on the actor id so the order is **stable between
polls** — a table that reshuffles equal rows at 1 Hz is unreadable.

It walks the directory, so it costs O(activations) and allocates a record
per candidate; `limit` defaults to 100 because this is a "top N" view and a
silo can hold millions. Poll it at human rates, not per request.

`ageMs` is monotonic and `idleMs` is wall-clock — deliberately different
clocks. Age is a duration and must survive an NTP step; idle is compared
against `idleAfterMs` by the sweeper, which genuinely wants wall time.

`stats()` also reports `transitional: { activating, deactivating }`. Those
slots have no activation to read yet, so they are not in `activations` and
never were in the counts — which meant a silo in the middle of an
activation storm read as **idle**, at exactly the moment you were looking
at it.

## Metrics

`metrics()` is a plugin that counts what the silo is doing. Pull-based: no
exporter, no push pipeline, no metrics-library dependency — you read
`snapshot()` whenever you want, from a route, a health check, or a test.

```ts
import { defineActorApp, memoryStorage, metrics } from '@sigx/actors/silo';

const m = metrics();
const app = defineActorApp({ actors, storage: memoryStorage() }).use(m);
const silo = await app.start();

m.snapshot();
// {
//   windowMs: 60_000,
//   calls:       { total: 12_400, failed: 3, streams: 2 },
//   latencyMs:   { count, minMs, maxMs, meanMs, p50Ms, p90Ms, p99Ms },
//   queueMs:     { ... },   // waiting for the mailbox
//   turnMs:      { ... },   // holding the mailbox
//   byType:      { Cart: { calls, failed, latencyMs, queueMs, turnMs } },
//   byMethod:    { 'Cart#checkout': { calls, failed, latencyMs, queueMs, turnMs } },
//   errors:      { byKind: { 'call-timeout': 3 },
//                  recent: [{ at, type, method, kind, message }] },
//   activations: { created: 91, destroyed: 88, byReason: { idle: 88 } },
//   storage:     { loads, saves, clears, conflicts, latencyMs },
//   gauges:      { activations: 3, queued: 0, perType: { Cart: 3 } }
// }
```

**Read `queueMs` against `turnMs` first.** They are the two halves of every
call's latency and they mean opposite things:

| | meaning | fix |
|---|---|---|
| high `turnMs` | the method itself is slow | move I/O out of the turn, split the method |
| high `queueMs` | the grain is a hotspot — callers are waiting behind each other | shard the key, or reduce traffic to it |

A dispatch middleware only ever sees the sum, which is why `queueMs` is the
number people usually lack. `metrics()` gets it from `observeTurns`, the one
seam this needed (see below).

`conflicts` is worth an alert rather than a graph: each one is an etag
mismatch that discarded an activation.

### Per method, and why calls fail

`byType` tells you a type is slow; it never tells you *which of its methods*
is. `byMethod` carries the same five numbers keyed `Type#method`, and the
queue/turn split is most useful there — within one type, a hot grain and one
slow method look identical until you separate the methods.

```ts
m.snapshot().byMethod['Cart#checkout'];
// { calls: 4_100, failed: 2, latencyMs, queueMs, turnMs }
```

`errors.byKind` counts `ActorErrorKind` — `'call-timeout'`, `'wrong-host'`,
`'state-conflict'`, `'unreachable'`, `'deadlock'`, `'activation'`,
`'method-not-found'`, `'silo-shutdown'` — plus `'(unknown)'` for anything an
actor method threw itself. `calls.failed` says a silo is failing; this says
what is wrong with it, and the two are very different questions: a rising
`'unreachable'` is a network or membership problem, a rising `'(unknown)'` is
your code.

`errors.recent` keeps the last few failures (default 32, `recentErrors: 0` to
disable) as `{ at, type, method, kind, message }`. **Message only** — no args
and no state, because this is read over an HTTP endpoint and a failing call's
arguments are exactly where the secrets are.

Both breakdowns are capped like `byType`, overflowing into `'(other)'`.
Methods get their own cap (`maxMethods`, default 256) rather than sharing
`maxTypes`, because methods multiply types: 64 types under the type cap would
leave under one method each and the breakdown would be almost entirely
`'(other)'` on a perfectly ordinary app.

### Turning it on and off

Collection can be switched at runtime, and switching it off genuinely stops
paying for it:

```ts
const m = metrics({ enabled: false });   // wired in, collecting nothing
m.enable();                              // ...investigate...
m.disable();                             // back to ~free; counters keep their values
m.enabled;                               // boolean
```

`disable()` drops the turn subscription rather than returning early inside
it. That distinction is the whole point: the runtime only takes the per-turn
timestamps while an observer is attached, so an inert-but-attached observer
would keep paying for the larger half of the cost. Counters freeze at their
current values — use `reset()` to clear them.

The intended shape is to leave `metrics({ enabled: false })` wired into
production and switch it on when you need to look.

### What it costs

Measured on a `noop` dispatch — the cheapest call there is (~0.5µs) — with
each configuration in its own process, median of 9 runs:

| | throughput | vs no plugin |
|---|---:|---|
| no plugin | 2.05 M ops/s | — |
| an inert plugin (the control) | 2.08 M ops/s | ~0 |
| `metrics({ enabled: false })` | 2.05 M ops/s | **~0** |
| `metrics({ histograms: false })` | 1.88 M ops/s | −8% |
| `metrics()` | 1.43 M ops/s | −30% |

Three things to read off that. **Disabled is indistinguishable from not
having the plugin at all** — the residual branch in the dispatch wrapper is
below measurement noise. **Not attaching it is free**: with no observer the
dispatch path is unchanged, verified by comparing against a baseline of the
previous commit with the benchmark suite (`benchmarks/`). And **plugins
themselves cost nothing** — the inert control says so, which is what makes
the other rows attributable to metrics rather than to the plugin machinery.

Read the −30% in absolute terms before it alarms you: full metrics adds
~200ns per call. It looks like a third of throughput only because the
measured call does nothing at all — for an actor whose turn takes 100µs it
is under 0.25%. Durations use `performance.now()` rather than the wall
clock, which costs one extra read per observed turn and buys immunity to NTP
or a VM host stepping the clock backwards mid-turn.

The per-method breakdown is ~3.5% of that per dispatch — it was −28% before
`byMethod` existed. `maxMethods: 0` gets the old cost back; what it buys is
per-method call counts, failure counts and the queue/turn split, which is
usually the trade you want.

That 3.5% was measured by loading **both builds into one process** and
interleaving their rounds, because separate processes could not resolve it:
the machine drifts ~10% between runs, larger than the effect, and the inert
controls disagreed by more than the thing being measured (see
`benchmarks/README.md`, "Trusting the numbers"). The `metrics()` row's
absolute ops/s above is therefore the original machine's figure rescaled by
that ratio rather than re-measured — the **percentages** are what was
observed, the ops/s is derived.

### `observeTurns`

The seam behind the split, available to any plugin:

```ts
registry.observeTurns((ref, method, queuedMs, elapsedMs, failed) => { ... });
```

Fires for dispatched turns only — the ones a caller waited for, including
reminder delivery. Volatile `ctx.timer` ticks and write-behind flushes are
excluded: they have no caller, and their cost is already visible as queue
wait on whatever was behind them. Reentrant `ctx.actor` calls run inline
against the caller's turn and are excluded too. Throwing from an observer is
swallowed and dev-logged — it can never fail a turn.

Registering no observer leaves the hot path exactly as it was: the
timestamps are only taken when someone is listening.

**In a cluster, `metrics()` is per-silo and the two halves of a cross-silo
call land on different silos.** `calls.total` and `latencyMs` are counted on
the silo that *originated* the call; `queueMs`/`turnMs` and the activation
gauges on the silo that *executed* it. That is a split, not a double count —
an inbound hop goes through the raw local dispatcher and never touches
`useDispatch` — so summing `calls.total` across silos gives the true number
of calls the cluster served, once each.

## Health & readiness

`health()` adds two endpoints. It knows nothing about clustering: it serves
the aggregate of every plugin's `reportHealth()` contribution, and
`cluster()` contributes its own. So this is the whole wiring, clustered or
not:

```ts
import { defineActorApp, health } from '@sigx/actors/silo';

export const app = defineActorApp({ actors, storage })
    .use(cluster({ providers, advertise, secret }))   // optional
    .use(health());
```

| route | question | on failure |
|---|---|---|
| `GET /_sigx/health` | **liveness** — is this process worth keeping? | restart it |
| `GET /_sigx/health/ready` | **readiness** — should it be receiving traffic? | drain it |

**They must be allowed to disagree, and the drain is why.** A graceful
`silo.stop()` announces `leaving` *before* activations hand off, so for the
whole handoff window the silo is `200 live` and `503 not-ready`: take it out
of the load balancer, do **not** restart it — a restart would abort the
handoff that makes rolling deploys drop zero calls.

The cluster check fails for three states, each with a `detail` you can read
off the probe body:

- **`leaving`** — draining (post-`beginStop`). Not-ready, still live.
- **`fenced`** — this silo lost its membership heartbeat and now refuses
  every activation. Its *published* status still reads `active`, so without
  this check a balancer would happily keep feeding a black hole. Fenced is
  also **fatal** (below): the fence is permanent for this silo identity, so
  liveness fails too and the orchestrator restarts the pod — the fresh
  process mints a new identity and rejoins. Without that, an outage of the
  membership store longer than `ttlMs` leaves every pod `200 live /
  503 not-ready` forever and the cluster stays dead until a human restarts
  it.
- **`joining`** — the membership join has not completed.

Any plugin can gate readiness — a cache warmer, a migration check:

```ts
registry.reportHealth('warmup', () => ({ ready: cache.loaded, detail: 'cache cold' }));
```

A check can also declare the process **unrecoverable** with `fatal: true`
(implies not-ready): liveness then answers `503 { status: 'fatal' }` and the
orchestrator's restart is the medicine. Reserve it for terminal states —
`ready: false` alone means "drain me, I am still alive", and conflating the
two turns every drain into a restart.

Every contributed check must pass; all of them are evaluated, so a failing
probe names *every* reason rather than the first. A check that throws is
reported not-ready with its message — a broken check must never 500 the
endpoint that is supposed to diagnose it.

**Before `start()` the whole mount answers 503**, including `/_sigx/health` —
routes only run on a started silo, so serving at all *is* the liveness
signal. On Kubernetes that means a `startupProbe`, which exists for exactly
this and also suppresses the liveness probe until the first success:

```yaml
startupProbe:                 # covers a slow boot (Redis join, storage warm-up)
  httpGet: { path: /_sigx/health, port: 7311 }
  failureThreshold: 30
  periodSeconds: 2
livenessProbe:
  httpGet: { path: /_sigx/health, port: 7311 }
readinessProbe:               # 503s for the whole drain window
  httpGet: { path: /_sigx/health/ready, port: 7311 }
lifecycle:
  preStop: { exec: { command: ["sleep", "10"] } }   # let the LB notice first
```

These routes **cannot be authenticated** — a kubelet cannot sign the cluster
HMAC — so do not expose `/_sigx/health` through a public ingress. By default
the body carries the `checks` map and activation totals; it never includes a
`perType` breakdown or anything key-shaped, so it cannot name your actor
types. `health({ detail: false })` drops both, leaving the status code and a
bare `{ status, uptimeMs }`.

## The ops endpoint

`health()` answers *may I take traffic?* in a status code. `ops()` answers
*what is going on in here?* in a body — and unlike health it **is**
authenticated, which is the whole reason it is a separate route.

```ts
import { metrics, health, ops } from '@sigx/actors/silo';

const collect = metrics();
const app = defineActorApp({ actors })
    .use(collect)
    .use(health())
    .use(ops({ secret: process.env.OPS_SECRET }));
```

| Route | Answers |
| --- | --- |
| `GET /_sigx/ops` | `OpsSnapshot` — `{ v, at, uptimeMs, stats, activations, health, ops }` |
| `GET /_sigx/ops/cluster` | a `clusterStats()` fan-out; 404 when unwired |

Everything a monitoring tool needs was already being collected and none of it
was reachable from outside the process: `metrics().snapshot()` contributes no
route, `clusterStats()` takes a placement *object* rather than a URL, and the
one remote surface that did exist — the `$sigx:silo#stats` symbol — is
cluster-only, carries no latency distributions, and disappears entirely in a
cluster of socket-only transports. A single-node silo was unobservable from
outside.

**The secret is mandatory.** `ops()` throws at construction without one unless
`__DEV__`, so there is no configuration in which it ships open by accident.
This endpoint reports your actor type names, per-method latencies and cluster
topology, and an ops endpoint that is unauthenticated *by omission* is worse
than none: nothing about the response tells you it is happening. `401` covers
both a missing and a wrong token, and auth runs before the path split so paths
cannot be enumerated. In dev, omitting the secret serves open and warns once.

The `cluster` fan-out is wired by the caller rather than discovered:

```ts
import { cluster } from '@sigx/actors/cluster';
import { clusterStats } from '@sigx/actors/cluster';

const c = cluster({ providers, secret });
app.use(c).use(ops({
    secret: process.env.OPS_SECRET,
    cluster: (signal) => clusterStats(c.placement, { signal, timeoutMs: 2000 })
}));
```

`ops()` lives in `@sigx/actors/silo` and `clusterStats` in
`@sigx/actors/cluster`, so a single-node silo must not pay for the cluster
bundle to have an ops endpoint. Passing a thunk also leaves `timeoutMs` and
`concurrency` where they already are, on `ClusterStatsOptions`.

### Contributing a section

`registry.reportOps(name, provider)` is the counterpart to `reportHealth` —
any plugin can publish to the snapshot without knowing an endpoint exists.
`metrics()` contributes under `'metrics'` and `cluster()` under `'cluster'`,
which is why the example above needs no wiring between them.

```ts
registry.reportOps('cache', () => ({ entries: cache.size, hits, misses }));
```

Providers run **per read**, so return live numbers rather than a value
captured at setup. They must stay sync, for the same reason a readiness check
must: this is the endpoint you reach for when the silo is *already* unwell,
and it must not be able to hang. A throwing provider is caught and its section
replaced with `{ error }`, leaving every other section intact — the one tool
that explains a broken silo must not be broken by it. Names must be unique; a
clash throws at setup naming both plugins.

`ops().snapshot()` gives the same answer in process, for a test or for a tool
embedded in the silo rather than polling it.

### Cluster-wide stats

`clusterStats()` fans out across the membership view and returns one report:

```ts
import { clusterStats } from '@sigx/actors/cluster';

const report = await clusterStats(plugin.placement, { timeoutMs: 2000 });
// {
//   view:    { version: 12, size: 3, active: 3 },
//   silos:   [{ siloId, address, status, stats: { activations, queued, perType },
//               counters, reminderShards: ['p3','p7',…], uptimeMs }],
//   totals:  { silos, activations, queued, perType, counters },
//   reminderShards: { p0: ['s.ab12'], p1: ['s.cd34'], … },
//   unreachable: [{ siloId, address, reason: 'unreachable', message }],
//   partial: false
// }
```

It travels as a reserved symbol (`$sigx:silo#stats`) on the **existing**
internal mount, so it inherits the per-request HMAC, the envelope, the codec
and the body cap — there is no second, unauthenticated way to read your
topology. It needs `secret` configured, like every other silo-to-silo call.

**It never throws because a peer is sick.** A silo that times out, refuses
the secret, or predates this build lands in `unreachable` with a `reason`,
and `partial: true` marks the totals as a lower bound. A report you cannot
get during an incident is worthless. Peers are queried with bounded
concurrency (default 16), so a 100-silo fan-out is waves rather than 100
simultaneous connections, and the collector answers for itself in process.

`reminderShards` maps each of the 16 shards to the silos *claiming* it, built
from the reports rather than recomputed centrally — so **two** claimants means
views have diverged (safe: the per-shard etag CAS keeps reminders
at-most-once) and an **empty** list means nothing is ticking that shard. The
number of distinct silos across that map is also how many silos do reminder
work at all, which is otherwise invisible.

### Cluster counters

`placement.counters()` is the per-silo, pull-based routing view — the same
posture as `metrics()`, and always on (integer increments on paths already
doing network and directory work; the local fast path is not instrumented at
all).

| counter | what it diagnoses |
|---|---|
| `routedLocal` / `remoteDispatches` / `inboundDispatches` | locality. `remoteDispatches` and `inboundDispatches` are the two sides of one hop — reported side by side, **never summed**; their gap is in-flight and retried attempts |
| `remoteWatches` / `inboundWatches` | live subscriptions crossing a hop. Counted apart from streams because a watch holds a keep-alive on the OWNER until its subscriber leaves — read `inboundWatches` next to that host's activation count |
| `retries`, `routingFailures` | convergence. Every `routingFailures` is a user-visible error after the last attempt |
| `wrongHostRedirects` | the directory and the placement policy disagree — usually membership flapping |
| `unreachableRetries` / `drainingRetries` | a peer flapping, versus a rolling deploy in progress (should be zero at rest) |
| `routeCacheHits` / `routeCacheMisses` / `routeCacheSize` | directory load. A collapsing hit rate is what precedes a directory melt-down |
| `locates` / `locateRemote` | the miss rate the EDGE is producing. `locateRemote / locates` staying high means whatever routes in front of the cluster is not agreeing with placement — the number to watch after wiring up routing-token hashing |
| `directoryLookups`, `directoryClaims`, `claimConflicts`, `directoryReleases` | activation races, and claim leaks — a widening claims-vs-releases gap strands keys |
| `directoryEvictions`, `siloSweeps`, `sweptEntries` | failover actually happening. Zero sweeps after a crash means dead entries are only being reclaimed lazily |
| `membershipChanges`, `membershipVersion` | store load. One join notifies every member, so this is the counter that makes membership cost visible |
| `selfFences` | this silo was a black hole. Anything above zero needs investigating |
| `authFailures` | secret rotation gone wrong — a 403 on the internal mount is otherwise completely silent |
| `claimed`, `status` | actors owned here, and `'fenced'` where the published status still says `active` |

## Reads and writes in components

`useActorState` reads an actor method as component data; `useActorAction`
runs a mutation. Both come from `@sigx/actors/app`:

```ts
import { useActorAction, useActorState } from '@sigx/actors/app';

const Cart = component(({ props }) => {
    const total = useActorState(CartActor, props.id, 'total');
    const add = useActorAction(CartActor, props.id, 'add');
    return () => (
        <button disabled={add.loading} onClick={() => add.run(['apple'])}>
            {total.match({ ready: (n) => `${n} items`, pending: () => '…' })}
        </button>
    );
});
```

Method names, argument types and results all come from the definition, so a
rename surfaces at every call site. Pass a getter for a reactive key —
`useActorState(CartActor, () => [selectedId(), 'total'])` — and a falsy
return parks the read in `'idle'`.

A write **refreshes the reads it staled** — no manual `refresh()`:

```ts
await add.run(['apple']);   // every read of this cart re-runs
```

The default is the whole-actor prefix `actorKey(def, key)`, because a write
changing what a *different* method returns is the normal case and
under-invalidating leaves stale data on screen. Narrow or widen it with
`invalidates` — a pattern list, a function of `(result, key)`, or `false`:

```ts
useActorAction(CartActor, id, 'add', {
    invalidates: [actorKey(CartActor, id, 'total'), ['@actor', 'Order']]
});
```

These are built on core's `useData`/`useAction`, which is where the
behaviour comes from: an `AsyncState` with `match()`/`refresh()` that
`errorScope` and `all()` already understand, and **in-flight dedupe by
canonical key** — ten components reading one actor make one dispatch.

**SSR seeding is free.** `actor()` is isomorphic, so during a server render
the read dispatches in-process through the silo (guards and all), resolves
into the markup, and serializes into the page under its canonical actor key.
The browser restores it on mount **without refetching**.

`actorKey(def, key, method?, ...args)` is that key, and is isomorphic too —
a definition and a build-swapped client ref produce identical tuples. It is
a tuple rather than a single string because the prefix relation is what
invalidation needs: `['@actor','Cart','c1']` addresses every read of that
cart, `['@actor','Cart']` every cart on the page.

The raw `useData` recipe still works if you want it —
`useData(['cart', id], () => actor(CartActor, id).getSummary())` — but it
does not share keys with `useActorState`.

## Dev & HMR

The vite plugin's dev silo lives in the SSR module runner's graph and is
reachable through the `__SIGX_ACTOR_SILO__` seam. Editing a `*.actor.ts`
file deactivates that type through storage — **state survives edits iff your
app configures persistent storage** (`sigxActors({ app })` runs your real
config, so it does); with the bare in-memory default it resets (the dev log
says so once). Mid-edit syntax errors never reach the browser:
the last good client stub is served, or a loud refusal.

## Wire protocol

`POST {base}/r/{token}/{Type}%23{method}` with `{"args": [key, ...args]}` →
`{"data"}` / `{"error"}` envelope (NDJSON for streams). The `/r/{token}/`
part is an optional routing hint — under `route: 'none'`, and for the
`$`-reserved symbols below, the URL is plain `{base}/{Type}%23{method}` and
everything else is unchanged. It is the serverFn
protocol verbatim — same origin policy, body caps, prototype-pollution
guards, error masking, and codec — because `handleActorRequest` *is*
`handleServerFnRequest` with a silo-backed resolver. `configureActors()`
(from `@sigx/actors/client`) points remote/native clients at another base,
independently of `configureServerFn`.

### The routing token

`{token}` is a stable per-grain routing hint, mirrored into the
`x-sigx-actor-route` header. It exists because the actor **key** rides in
the JSON body, and no load balancer will parse a body to route — so without
it the edge cannot tell which grain a request is for, and locality decays as
1/N (measured 1.00, 0.50, 0.12, 0.02, 0.01 for N = 1, 2, 10, 50, 100).

It is a **middle** segment, deliberately: the symbol is decoded as the
*last* path segment, so the token slots in ahead of it and the endpoint
neither parses nor validates it. Routing is an optimization and is never
load-bearing for correctness — a stale, wrong, or absent token costs a
network hop, never a wrong answer.

| `route` | token | for |
|---|---|---|
| `'hash'` *(default)* | opaque hash of the actor id | production — keys stay out of access logs |
| `'key'` | the raw actor key | debugging, human-readable routing |
| `'none'` | *(no token; URL stays bare)* | opting out |
| `(ref) => string \| null` | yours | tenant affinity, an existing sharding scheme |

```ts
configureActors({ route: 'key' });   // or 'none', or a function
```

The default is a hash because actor keys are frequently user ids or emails,
and a raw key in the path lands in every access log, proxy trace and
referrer header. **This is log hygiene, not privacy:** an unkeyed hash of an
email is one dictionary lookup from plaintext at any width. Because the
composition below needs only *stability* and not agreement, a hash routes
exactly as well as the key.

Two carriers, always the same value, because neither alone covers every
edge — a path segment cannot be silently stripped by a mesh (and a mangled
one 404s loudly rather than degrading to zero locality in silence), while
Envoy has no path-substring hash policy at all.

Both carry the token **percent-encoded**, byte for byte identical: a load
balancer hashes what it sees, so `tenant%2Fa` in the path beside `tenant/a`
in the header would route the two carriers to different silos. It also keeps
the header value safe ASCII for keys that are not. Hash-mode tokens are
`[0-9a-z]{7}`, so encoding is a no-op there; it matters for `'key'` mode and
custom functions. `actorRouteToken()` decodes either carrier back to the
real value.

```nginx
map $uri $grain { ~^/_sigx/actor/r/([^/]+)/ $1; }
upstream silos { hash $grain consistent; server ...; }
# or, equivalently:  hash $http_x_sigx_actor_route consistent;
```

```
HAProxy   balance uri depth 4
Envoy     hash_policy: { header: { header_name: x-sigx-actor-route } }
```

**Pair it with `preferLocalPolicy()` — the token alone changes nothing.**
The composition is what produces locality: the LB hashes the token to silo
X, the first call activates the grain *there*, and every later call for that
key hashes to X again. The LB and the cluster then agree on nothing but
stability, which is why the LB's algorithm need not match the cluster's.

Measured (`pnpm bench:run locality`, `cluster/locality-routed`), local
fraction in the steady state:

| edge × placement | N=2 | N=10 | N=50 | N=100 |
|---|---:|---:|---:|---:|
| round-robin × `randomPlacementPolicy()` *(default)* | 0.50 | 0.12 | 0.02 | 0.01 |
| hash token × `consistentHashPolicy()` | 0.48 | 0.09 | 0.02 | 0.01 |
| **hash token × `preferLocalPolicy()`** | **1.00** | **1.00** | **1.00** | **1.00** |

The middle row is an **anti-pattern**, not a middle ground: the edge's hash
and the cluster's rendezvous hash are different functions over different
sets, so they disagree on most keys and guarantee a hop for every grain.
Deterministic placement does not help here — caller affinity does.

Two limits worth knowing:

- **`$live` carries no token** (nor does any `$`-reserved symbol). One
  held-open response fans out to *many* grains, so there is no single token
  and no single owner; sharding that connection would defeat the reason it
  exists. Live subscriptions still dispatch correctly through placement —
  they just keep paying the hop.
- **A migrated grain does not follow the LB.** `preferLocalPolicy()` applies
  to *new* activations, and the directory keeps a live grain where it is. So
  after a scale-out, already-hot grains stay misrouted until they deactivate.

The internal silo-to-silo mount carries no token: the caller has already
resolved the exact owner.

Renaming an actor's `type` or its methods is a **wire break** (and `type`
is also the storage identity).

The runtime reserves two symbol shapes on top of that, which is why
`defineActor` refuses a `type` starting with `$` or `@`:

| symbol | what it is |
|---|---|
| `$live#subscribe` | the public multiplexed subscribe mount — many live reads on one held-open NDJSON response |
| `$watch:{Type}#{method}` | INTERNAL, silo-to-silo only: the same read as `{Type}#{method}`, but opened as a subscription |

The second exists because a watch is an ordinary read dispatched in watch
mode, so the receiving silo cannot tell which is meant from the method
alone — `streamNames` separates streams from calls, and a watch is neither.
It rides the **symbol** rather than the call envelope because the per-call
HMAC signs `proto\nsymbol\ncallId\ntimestamp`: the envelope is not covered,
so the intent would otherwise be free for anyone reaching the mount to
flip. Its payload leads with the actor key and the per-subscription
options: `{"args": [key, {"throttleMs": 50} | null, ...args]}`.

### Transports are pluggable, on both sides

The client proxy never speaks HTTP itself — it delegates to an
`ActorTransport`, so batching, a different auth scheme, or a protocol other
than fetch drops in without any call site changing:

```ts
import { configureActors, fetchTransport } from '@sigx/actors/client';

configureActors({ endpoint: '/actors', headers: () => ({ authorization: token() }) });
// …sugar for fetchTransport(config). Or supply the whole seam:
configureActors({
    name: 'batching',
    call: (symbol, args, init) => /* … */,
    stream: (symbol, args, init) => /* … */,
    live: () => /* optional push channel */
});
```

`fetchTransport()` is the default and implements exactly the wire contract
above. `init.endpoint` carries the endpoint the build baked into the ref, so
`configureActors({ headers })` can override headers alone without restating
where the server is.

**Server-side**, a transport is a plugin: `PluginRegistry.route()` lets one
contribute its own mount, which `createAppHandler` / `createFetchHandler`
serve in dev and prod alike. (One current limit: `ActorRoute.handle` returns
a `Response`, which cannot express a Node WebSocket upgrade — that needs the
raw socket. Workers can express it.)

That limit applies to this **client-facing** transport. A *silo-to-silo*
transport is not bound by it, because it is free to bring its own listener
instead of a route — `httpTransport()`, the default, does contribute a route
like any other plugin, but a socket transport does not have to. See
[the silo-to-silo transport seam](#the-silo-to-silo-transport-is-pluggable).

### The app plugin

```ts
import { actorsPlugin } from '@sigx/actors/app';
app.use(actorsPlugin({ transport: { endpoint: '/actors' } }));
```

`actorsPlugin()` exists for a reason `configureActors()` alone cannot cover:
a **server app installs per request**, and the transport seam is
page-global, so a server writing to it would let one request's config bleed
into another's concurrent render. The plugin installs a transport on live
clients only, tears it down on `app.unmount()`, and provides the per-app
context the hooks resolve through. It is optional — `actor()` works without
it, because the build bakes an endpoint into every client ref.

It deliberately does **not** register codec type handlers: the actor wire
reads core's `__SIGX_SERVERFN_CODEC__` seam directly, so one
`serverPlugin({ types })` already covers both wires.

## Clustering (multi-host)

One silo per host, many hosts, one actor system — add the `cluster()`
plugin:

```ts
import { defineActorApp } from '@sigx/actors/silo';
import { cluster } from '@sigx/actors/cluster';
import { redisCluster } from '@sigx/actors-redis';

export const app = defineActorApp({ actors, storage }).use(
    cluster({
        providers: redisCluster({ url: process.env.REDIS_URL }), // membership + directory
        advertise: 'http://10.0.4.7:7311',   // this silo's peer-reachable origin
        secret: process.env.SILO_SECRET      // declared ONCE
    })
);
```

The plugin contributes the internal silo-to-silo mount as a route, so
`secret` and `internalBase` are stated once instead of being repeated at
the endpoint, and an adapter that mounts `app.routes` picks up
silo-to-silo traffic automatically:

```ts
import { createServer } from 'node:http';
import { createAppHandler, attachSignalHandlers } from '@sigx/actors/node';

// ONE handler for both mounts — public endpoint and internal route.
const server = createServer(createAppHandler(app));

// Listen BEFORE starting: `app.start()` joins membership, and from that
// moment peers may place actors here and call them. Bind first and there
// is no window where this silo is routable but nothing is listening.
await new Promise<void>((resolve) => server.listen(7311, resolve));
const silo = await app.start();
attachSignalHandlers(silo);
```

On other runtimes, route `app.routes` yourself — each is a
`{ match(request), handle(request, silo) }` pair. The lower-level
`clusterPlacement` / `handleSiloRequest` / `matchesSiloRequest` remain
exported for hand-rolled mounts.

#### Redirect on a miss, instead of proxying

When a call arrives at a silo that does not own the grain, the mount
**proxies** it: one client round trip, one internal hop, every time. With
the routing token in front of an LB that hop should be rare — but for
callers that can reach silos directly, it can be removed entirely.

```ts
handleActorRequest(request, { silo, onMiss: 'redirect' });   // or 'auto'
```

| `onMiss` | behaviour |
|---|---|
| `'proxy'` *(default)* | forward and answer. Correct for browsers. |
| `'redirect'` | answer `421` naming the owner; the client retries there. |
| `'auto'` | redirect callers that advertise they can follow, proxy the rest. |

It goes on the **mount**, not the silo, so one cluster can serve a browser
origin that proxies and an internal origin that redirects.

The client opts in, and remembers:

```ts
configureActors({ endpoint, follow: true });
```

Remembering is the point. Without it a redirect costs *two* client round
trips versus one trip plus an internal hop — strictly worse than proxying.
With it, only the first call to a grain pays: 2 requests once, then 1
forever, straight to the owner. A learned endpoint is dropped as soon as it
proves wrong (a connection failure or a 5xx), falling back to the configured
endpoint rather than stranding the grain.

**The owner must publish where clients can reach it:**

```ts
cluster({
    advertise: 'http://10.0.4.7:7311',        // INTERNAL, peer-to-peer
    publicAddress: 'https://silo-3.example.com' // what a client can reach
});
```

Without `publicAddress` the mount **proxies anyway** (and dev-warns once).
That is deliberate: `advertise` is typically a pod IP, so redirecting a
client there would hang, and publishing it would hand out internal topology
to anyone who can reach the public mount. The 421 body carries
`owner.endpoint` and never `address`.

Three things worth knowing before turning it on:

- **`'proxy'` is the right answer for browsers.** A redirect to another
  origin makes the retry cross-origin, so it preflights and is refused by
  the endpoint's default same-origin policy. Redirecting a browser requires
  an explicit `origin` allowlist and CORS on the owner's mount — which is
  why the single-origin deployment (all silos behind one public origin, the
  LB hashing the routing token) is usually the better answer there.
- **`$live` is never redirected.** One held-open response fans out to many
  grains, so there is no single owner to point at.
- **421 is safe to retry.** The endpoint resolves the owner *before*
  dispatching, so a redirect has provably run no code — which matters
  because actor calls are not idempotent and a 421 is a status user agents
  may retry on their own.

Redirect chains are bounded at both ends (default 2 hops): the client cap is
the useful one, the server cap is what makes a loop impossible even against
a client that ignores its own.

#### The silo-to-silo transport is pluggable

How silos reach *each other* is a seam. `httpTransport()` is the default and
needs no configuration; it is also the only one that is WinterCG-clean,
which is what makes clustering work on Workers-style runtimes:

```ts
import { cluster, httpTransport } from '@sigx/actors/cluster';

cluster({
    providers, advertise, secret,
    // The default. Spelled out here only to show where the knobs live —
    // `fetch` is where a tuned dispatcher goes.
    transport: httpTransport({ fetch: myTunedFetch })
});
```

`cluster({ fetch })` and `cluster({ endpoint })` remain as sugar for exactly
those two options; passing them *alongside* an explicit `transport` throws,
because they only ever reach the HTTP transport and a chain need not contain
one.

A transport declares its own peer-reachable address, published in the
membership descriptor under `addresses[name]` — so a silo can speak more
than one. A **list is a fallback chain**, tried in order:

```ts
transport: [tcpTransport({ port: 11111 }), httpTransport()]
```

That is the rolling-deploy story, and the reason `addresses` exists: while
the deploy is half-done, silos that already advertise `tcp` talk over it and
the rest are still reached over HTTP, with no window in which any peer is
unreachable. A descriptor with no `addresses` at all — a silo from a build
that predates the field — reads as HTTP-only, which is the safe direction.

A **single** transport is strict: a peer that advertises no address for it is
unreachable, loudly, rather than silently falling back. That is deliberate —
a silent fallback means you deploy a transport, benchmark it, and measure the
old one without ever knowing. Fallbacks that *do* happen are counted
(`counters().transportFallbacks`), reported (`SiloReport.transports`), and
dev-warned once per peer.

Because the internal mount is now just a route a transport declares, a
cluster configured with only socket transports has **no internal HTTP
endpoint at all** — a smaller attack surface, and nothing to `curl`. The
public actor wire is unaffected either way.

#### Bound the connection pool on Node

Node's global `fetch` is undici with an **unbounded** pool and
`pipelining: 1`, which means one connection per in-flight request — measured
at *two* per in-flight request against a silo (`benchmarks/BASELINES.md`,
Tier 2). At concurrency 64 across 99 peers that projects to ~12 600 sockets
per silo, which is file descriptors, kernel buffers and a connection burst
every time a peer restarts.

The fix is four lines through the `fetch` seam:

```ts
import { Agent, fetch as undiciFetch } from 'undici';
import { cluster, httpTransport } from '@sigx/actors/cluster';

// One pool per silo process, bounded per peer origin. Size it to your
// per-peer concurrency — see the caveat below before going lower.
const agent = new Agent({ connections: 64 });

cluster({
    providers, advertise, secret,
    transport: httpTransport({
        fetch: (url, init) => undiciFetch(url, { ...init, dispatcher: agent })
    })
});
```

Measured at concurrency 64 against one peer (`benchmarks/BASELINES.md`):

| `connections` | sockets | throughput |
|---|---:|---|
| unbounded (default) | 128 | baseline |
| **64** (= concurrency) | **64** | **~+6%** |
| 8 | 8 | **~3× slower** |
| 1 | 1 | ~2.4× slower |

**Match `connections` to your per-peer concurrency.** That halves the socket
count for free — slightly better than free — because the unbounded default's
*second* connection per in-flight request is pure waste. Going below your
concurrency trades throughput for sockets steeply, and is only worth it if you
are actually running out of file descriptors.

`undici` is not a dependency of this package and is not required — this is a
recipe, not an API. The numbers above are undici 7.x, which is what Node
currently bundles for the global `fetch`; other majors have measured
differently, so re-check on yours before tuning aggressively.

> **HTTP/2 does not currently help.** `allowH2: true` measures identical to
> plain keep-alive at every pool size, because `createAppHandler` serves over
> `node:http`, which is HTTP/1.1 only — the client negotiates nothing and
> falls back. Multiplexing would need a `node:http2` server first, which is a
> larger change than the pool cap for the same socket reduction.

#### Which transport should you use?

Measured, not argued — the full table and the gate are in
`benchmarks/BASELINES.md`:

| | portable? | connections per peer | relative throughput |
|---|---|---:|---:|
| `httpTransport()` **(default)**, unbounded pool | **everywhere, incl. Workers** | 2 × concurrency | 0.9× |
| `httpTransport()` with a bounded pool (above) | **everywhere, incl. Workers** | concurrency | 1.0× (baseline) |
| `tcpTransport()` (`@sigx/actors-tcp`) | Node only | **1** | 4.9× |
| `wsTransport()` (`@sigx/actors-ws`) | Node server, any WinterCG client | **1** | 4.4× |

Throughput is relative to **bounded** HTTP, since that is the fair baseline —
the shipped default is the first row, and bounding it is a free improvement.

- **On Node, prefer TCP** — or WebSocket when one port, proxy traversal or a
  WinterCG client matters. Both clear every criterion the decision gate set out
  in advance.
- **HTTP stays the default, and must.** This entry is zero-dep and
  WinterCG-clean so Cloudflare Workers keep working, and HTTP is the only
  transport that runs everywhere. With a bounded pool (above) it is a
  perfectly reasonable choice.

Two caveats worth carrying: the throughput ratio is a **loopback software**
number that a real LAN round trip largely absorbs (the connection-count win is
not absorbed), and a chain like `[tcpTransport(), httpTransport()]` is what
makes adopting one safe mid-deploy.

**Writing one?** There is a conformance suite — `transportConformance` in
`packages/actors/src/cluster/testing.ts` — holding the cases a transport must
pass, and so the definition of correct behaviour. Supply a harness that builds
an N-silo cluster over your wire and every case runs against it. The rule it
enforces throughout is *assert on the error `kind`, never on an HTTP status*,
which is what makes the contract expressible off HTTP at all.

It is currently **contributor-facing only**: reachable inside this workspace
as `@sigx/actors/cluster/testing` through a tsconfig/vitest alias, but not in
the published `exports` map, so an out-of-repo package cannot import it yet.
Promoting the subpath is a deliberate step for whenever a transport ships
outside this repo.

How it works, in one paragraph: every activation writes a **claim** into a
distributed directory (create-if-absent; released on deactivation), so a
key activates on exactly one silo; calls for actors placed elsewhere are
forwarded over the internal endpoint with the full call context (chain,
call id, deadline as *remaining* ms — clock-skew-proof) so deadlock
detection and timeouts work across hosts; a misdirected call answers
**421 wrong-host** with the owner and the caller re-routes (bounded, never
proxied); membership is TTL-heartbeat liveness in the shared store; the
reminder table is split into 16 hash shards, each ticked by exactly one
silo via rendezvous hashing over the view (the per-shard etag CAS keeps
firing at-most-once even if views transiently diverge). Under all of it,
the storage etag CAS remains the integrity floor — a briefly-stale route
costs a rejected save and a fault-and-reload, never corrupted state.

Guards still run once, at the public edge — silo-to-silo hops are
intra-system, authenticated per request with an HMAC signature derived
from the shared `secret` (bound to the call, freshness-windowed; run
mTLS/VPC between hosts — transport encryption is deliberately out of
scope). Streams cross
hops with cancellation and keep-alive release intact. Placement is
pluggable per cluster and per type (`policy`, `typePolicies`:
`consistentHashPolicy()`, `preferLocalPolicy()`, uniform random by
default), a graceful `silo.stop()` hands actors off (`'migrated'`
deactivations, claims released as they drain, callers retry through
routing — rolling deploys drop zero calls), and
`placement.migrate(ref)` moves one actor explicitly. For tests,
`memoryClusterHub()` gives an N-silo in-process cluster with no external
store.

To see inside a running cluster — readiness that drains a `leaving` or
fenced silo, `clusterStats()` across the view, and the routing/directory
counters — see [Health & readiness](#health--readiness) above.

## Entry points

| Entry | Contents |
|---|---|
| `@sigx/actors` | `defineActor`, `actor`, `useActor`, `actorKey`, errors, types — isomorphic, light |
| `@sigx/actors/silo` | `defineActorApp`, `createSilo`, `memoryStorage`, `metrics()`, `health()`, `ops()`, storage/placement/plugin seams — server-only |
| `@sigx/actors/server` | `handleActorRequest`, `matchesActorRequest`, `createActorResolver` — WinterCG-clean |
| `@sigx/actors/node` | `createAppHandler` (all mounts), `createActorHandler`, `attachSignalHandlers`, `fileStorage` |
| `@sigx/actors/client` | `__actorRef`, `configureActors`, `fetchTransport`, the `ActorTransport` seam — the build-swap target |
| `@sigx/actors/app` | `actorsPlugin()`, `useActorState`, `useActorAction` — the sigx app integration (the only entry that imports `@sigx/runtime-core`) |
| `@sigx/actors/cluster` | `cluster()` plugin, `clusterPlacement`, `clusterStats`, `handleSiloRequest`, `memoryClusterHub`, provider seams — WinterCG-clean |
| `@sigx/actors/vite` | `sigxActors()`, `extractActors` |
| `@sigx/actors/vite-client` | ambient types for `virtual:sigx-actors` (types only) |

## Design notes & deliberate limits (v1)

- **One silo per process; many processes via `./cluster`.** The
  `ActorDispatcher`/`ActorPlacement` seams remain the extension point for
  other distributed backends (Cloudflare Durable Objects map naturally);
  every call already flows through them, and dev-mode `devSerializeChecks`
  verifies your arguments would survive a remote hop.
- **String keys**; POST-only wire (no GET caching/forms); no WebSocket/SSE
  push layer (NDJSON streams cover server→client per call); full
  `[Reentrant]` arbitrary interleaving is not offered — `reentrant: true`
  is call-chain re-entry only.
- Reserved names: actor types starting with `$sigx:` and the method name
  `$sigx:reminder`. `$sigx:silo#stats` is the cluster's ops channel and is
  answered before any definition lookup, so an actor type named `$sigx:silo`
  would simply be uncallable across silos.
