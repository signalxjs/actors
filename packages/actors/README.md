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
export default { plugins: [sigxActors({ storage: '/src/storage.ts' })] };
```

Actors live in **`*.actor.ts`** modules. The build swaps them wholesale for
typed client stubs — implementations never reach the browser (values are
swapped, types are not, so the client proxy is fully typed).

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
        yield ctx.snapshot();
        for await (const state of ctx.changes()) yield state;
    }
})
// client: for await (const s of actor(CartActor, id).watch()) render(s);
```

Stream bodies are **observers, not turns**: they run detached from the
mailbox (a stream that waited on its own actor's next turn while holding the
mailbox would self-deadlock), so they must read `ctx.snapshot()` /
`ctx.changes()` — never mutate live state. An open stream keeps the
activation alive; consumer disconnect runs the generator's `finally`.
`ctx.changes()` yields a detached snapshot after every mutating turn
(bounded buffer, drop-oldest).

- The `streams:` factory must not touch `ctx` while *constructing* the table
  (its method names are read at definition time); inside generator bodies,
  anything goes. `computed`/`watch` setup belongs in `methods:`.

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
- Calls that arrive during deactivation wait and land on a fresh activation.
- External calls get a deadline (`callTimeoutMs`, default 30s) — on expiry
  the **caller** gets `ActorCallTimeoutError`; the turn itself always runs
  to completion.

## SSR reads

`actor()` is isomorphic, so the standard `useData` recipe transfers actor
state to the client like any other server data — no bespoke integration:

```ts
const summary = useData(['cart', id], () => actor(CartActor, id).getSummary());
```

## Dev & HMR

The vite plugin's dev silo lives in the SSR module runner's graph and is
reachable through the `__SIGX_ACTOR_SILO__` seam. Editing a `*.actor.ts`
file deactivates that type through storage — **state survives edits iff you
configured `sigxActors({ storage })`**; with the in-memory default it resets
(the dev log says so once). Mid-edit syntax errors never reach the browser:
the last good client stub is served, or a loud refusal.

## Wire protocol

`POST {base}/{Type}%23{method}` with `{"args": [key, ...args]}` →
`{"data"}` / `{"error"}` envelope (NDJSON for streams). It is the serverFn
protocol verbatim — same origin policy, body caps, prototype-pollution
guards, error masking, and codec — because `handleActorRequest` *is*
`handleServerFnRequest` with a silo-backed resolver. `configureActors()`
(from `@sigx/actors/client`) points remote/native clients at another base,
independently of `configureServerFn`.

Renaming an actor's `type` or its methods is a **wire break** (and `type`
is also the storage identity).

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

## Entry points

| Entry | Contents |
|---|---|
| `@sigx/actors` | `defineActor`, `actor`, `useActor`, errors, types — isomorphic, light |
| `@sigx/actors/silo` | `defineActorApp`, `createSilo`, `memoryStorage`, storage/placement/plugin seams — server-only |
| `@sigx/actors/server` | `handleActorRequest`, `matchesActorRequest`, `createActorResolver` — WinterCG-clean |
| `@sigx/actors/node` | `createAppHandler` (all mounts), `createActorHandler`, `attachSignalHandlers`, `fileStorage` |
| `@sigx/actors/client` | `__actorRef`, `configureActors` — the build-swap target |
| `@sigx/actors/cluster` | `cluster()` plugin, `clusterPlacement`, `handleSiloRequest`, `memoryClusterHub`, provider seams — WinterCG-clean |
| `@sigx/actors/vite` | `sigxActors()` |

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
  `$sigx:reminder`.
