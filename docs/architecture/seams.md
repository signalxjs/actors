# Seams

Every extension point in the runtime, what it promises, and who implements it.
Signatures here are abbreviated — `packages/actors/src/types.ts` is the
authority, and its doc comments carry the reasoning that did not fit below.

Usage documentation for the ones a user configures lives on the docs site:
[storage](https://sigx.dev/actors/docs/storage/),
[placement policies](https://sigx.dev/actors/docs/placement-policies/),
[transports](https://sigx.dev/actors/docs/transports/),
[the client](https://sigx.dev/actors/docs/client/),
[the app and its plugins](https://sigx.dev/actors/docs/the-app/).

## Why there are this many

Core must run on Cloudflare Workers, so it may not import `node:*` or any
client library. Every capability that needs one is therefore a seam, and the
platform package plugs in. The test suite benefits by accident: nearly every
seam has an in-memory or manual implementation, so a host can be driven with no
I/O at all.

---

## `ActorStorage`

```ts
interface ActorStorage {
    load(type: string, key: string): Promise<ActorStorageRecord | null>;
    save(type: string, key: string, state: unknown, expectedEtag: string | null): Promise<string>;
    clear(type: string, key: string, expectedEtag: string | null): Promise<void>;
}
```

Three methods, etag optimistic concurrency, no transactions.

**The etag CAS is the integrity floor of the whole system.** Placement,
directory and membership are all allowed to be briefly wrong; when they are,
the cost is a rejected save (`ActorStorageConflict`, which the runtime turns
into fault-and-reload) rather than corruption. Do not add a code path that
writes state without passing through it.

Etags are opaque and implementation-minted — every shipped provider uses a
UUID. The runtime only ever compares them for equality.

Implementers: `memoryStorage` (`./host`), `fileStorage` (`./node`, dev only),
`redisStorage`, `pgStorage`, `surrealStorage`, `durableObjectStorage`,
`unhostedStorage` (Cloudflare's Worker half — every operation throws, because
in-memory storage on a host that hosts nothing would be a silent lie).

Plugins wrap it via `decorateStorage`; the last registered is outermost.

## `ActorScheduler` — the clock seam

```ts
interface ActorScheduler {
    every(intervalMs: number, tick: () => void): () => void;
    after(delayMs: number, run: () => void): () => void;
}
```

**Background** work goes through it — the idle sweeper, the reminder tick,
`ctx.timer`, write-behind flushes. Those are the jobs that must keep running
*between* requests, so they are the ones a runtime has to be able to redirect.
Call deadlines and the shutdown drain deliberately stay on host timers, because
they are scoped to an in-flight request or to `stop()`.

```ts
defineActorApp({ actors, scheduler: timerScheduler() });   // the default
```

Two things depend on this being a seam.

**Tests can drive time exactly**, instead of sleeping:

```ts
const scheduler = manualScheduler();
const host = createHost({ actors, scheduler, defaults: { idleAfterMs: 0 } });
scheduler.advance(60_000);   // an hour of sweeps, instantly
```

**And it is what makes a runtime with no background execution possible at
all.** A Cloudflare Worker runs only while handling a request, so an interval
registered at startup never fires. That is an architectural difference, not
something a polyfill can hide — hence a seam rather than a shim.

## `ActorReminders`

```ts
interface ActorReminders {
    bind(context: ActorRemindersContext): void;
    start(): void | Promise<void>;
    stop(): void | Promise<void>;
    apiFor(ref: ActorRef): ReminderApi;      // what backs ctx.reminders
}
```

`bind()` runs once before `start()`, mirroring `ActorPlacement.bind()`, and
hands over everything the implementation would otherwise have to be told twice
and could be told wrongly:

```ts
interface ActorRemindersContext {
    readonly storage: ActorStorage;      // AFTER any plugin decorators
    readonly scheduler: ActorScheduler;  // so ticks are drivable too
    readonly tickMs: number;
    ownsShard(shard: string): boolean | Promise<boolean>;
    deliver(ref: ActorRef, name: string): Promise<unknown>;
}
```

The default `shardedReminders()` keeps the table in `ActorStorage` under a
reserved type, split into 16 hash shards that hosts divide between them by
rendezvous hashing — which **assumes many actors per host**. Where that
assumption is false, replace it: under Cloudflare's one-DO-per-actor model each
actor's reminders live in its own object and fire from its own alarm, so there
is nothing to shard and nothing to poll (and no cadence floor — an alarm fires
*at* the due time, where `shardedReminders()` promises only "at or after").

`ownsShard` is meaningless to an implementation that does not shard;
`pgReminders` ignores it because `SKIP LOCKED` lets every host claim disjoint
rows from one scan, while `surrealReminders` partitions by it because SurrealDB
has no lock primitive to make the other approach safe.

## `ActorPlacement` and `ActorDispatcher`

`ActorDispatcher` is the narrow waist of the entire runtime — the thing every
call goes through, local or remote:

```ts
interface ActorDispatcher {
    dispatch(ref, method, args, call): Promise<unknown>;
    dispatchStream?(ref, method, args, call): AsyncIterable<unknown>;
}
```

`dispatchStream` is optional: a transport that cannot stream simply omits it
and the runtime rejects with a descriptive error rather than hanging.

`ActorPlacement` decides *which* dispatcher a ref gets:

```ts
interface ActorPlacement {
    dispatcherFor(ref): ActorDispatcher | Promise<ActorDispatcher>;
    locate?(ref): ActorLocation | Promise<ActorLocation> | undefined;
    bind?(local: ActorDispatcher, host: Host): PlacementBindings | void;
    start?(): void | Promise<void>;
    // …plus stop hooks; see types.ts
}
```

Three invariants worth stating:

- **`locate()` is a hint and is allowed to be stale.** By the time a caller
  acts on it the actor may have moved. That is safe because the *directory*,
  not the placement, arbitrates single-activation. A placement that cannot
  answer may omit the method or return `undefined` — both, rather than just
  the first, because a composing placement has to define the method in order
  to forward it and only learns at call time whether the inner one implements
  it.
- **`bind()` is how a distributed placement hooks the activation lifecycle**
  (directory claims) and the reminder tick. It receives the host's own local
  dispatcher, which is what lets a remote placement fall back to local.
- **`setPlacement` is exclusive.** A second claim throws, naming both plugins.

Registering it is a *factory*, not an instance — it runs once the host exists:

```ts
registry.setPlacement(() => placement);
```

There is no `definition` option on `clusterPlacement`. Per-type strategies are
resolved from the bound host at call time (`host.definition(type)`), which is
why the factory needs nothing from the setup context.

## `HostTransport` — host to host

Cluster-side, in `@sigx/actors/cluster`. Full contract in
`packages/actors/src/cluster/seam.ts`; the load-bearing parts:

- **`name` keys this host's entry in `HostDescriptor.addresses`, so renaming it
  is a wire break.** Peers on the old name stop finding an address for the new
  one and fall through the chain, or off the end of it.
- **`routes?`** are mounts the transport needs on the app's HTTP listener,
  contributed through `PluginRegistry.route()`. This is why the internal
  host-to-host endpoint is not special-cased: HTTP's receiving half is just the
  route it declares. A transport owning its own socket declares none — and a
  cluster configured without an HTTP transport therefore has **no internal HTTP
  mount at all**.
- **`start()` runs before `membership.join()`**, so no peer can learn this
  host's address before something answers on it. It returns the peer-reachable
  address, which is how a transport binding an ephemeral port learns its own.
- **`dispatcherFor(target)` returning `null` is a routing answer, not a
  failure** — the placement moves to the next transport in the chain. Throw
  only when reaching the peer failed for a reason the next transport would hit
  too. It is called per dispatch, so cache per `target.hostId`.

Implementers: `httpTransport()` (core, the default and the only one that runs
everywhere), `tcpTransport()`, `wsTransport()`, and Cloudflare's stub-swapped
`httpTransport`.

## `ActorTransport` and `ActorRouter` — client side

In `@sigx/actors/client`, which is the build-swap target: the Vite plugin
replaces actor modules with typed client stubs that call through here.

`ActorTransport` is how a call leaves the process (`fetchTransport` by
default). `ActorRouter` is the opt-in locality layer — `routedTransport`,
`learningRouter`, `routedFetchTransport` — kept behind a separate import so it
tree-shakes out of apps that do not use it.

See [wire-and-frames.md](wire-and-frames.md) for the routing token these mint,
and https://sigx.dev/actors/docs/client/ for configuration.

## Cluster providers

`ClusterMembership` (who is alive) and `ActorDirectory` (who owns which actor)
are independent seams, deliberately — `@sigx/actors-k8s` provides membership
from Leases while the directory stays store-backed. See
[clustering.md](clustering.md).

## `PluginRegistry`

The hooks (`src/host/app.ts`) are documented in full at
https://sigx.dev/actors/docs/the-app/. The composition rules are the part worth
keeping in mind while editing: `decorateStorage` last-registered-outermost,
`useDispatch` outside-in and **must forward `dispatchStream`**, `onStart` in
order, `onStop` in reverse and *after* the drain, and a placement's own hooks
bracket the plugins'.

## Mounts, and other runtimes

`createFetchHandler(app)` from `@sigx/actors/server` is the portable entry —
the public actor endpoint plus every plugin route as one
`(Request) => Response`:

```ts
await app.start();
Deno.serve(createFetchHandler(app));                 // Deno
export default { fetch: createFetchHandler(app) };   // Bun, Workers
```

`createAppHandler` from `@sigx/actors/node` stays the Node mount: it keeps
core's connect adapter for the public endpoint (backpressure-aware body
pumping) rather than routing everything through the generic bridge.
