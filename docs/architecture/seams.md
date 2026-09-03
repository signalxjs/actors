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
    /** Optional (#238) — the same save, one walk earlier. */
    saveText?(type: string, key: string, json: string, expectedEtag: string | null): Promise<string>;
}
```

Three required methods, etag optimistic concurrency, no transactions.

**The etag CAS is the integrity floor of the whole system.** Placement,
directory and membership are all allowed to be briefly wrong; when they are,
the cost is a rejected save (`ActorStorageConflict`, which the runtime turns
into fault-and-reload) rather than corruption. Do not add a code path that
writes state without passing through it.

Etags are opaque and implementation-minted — `redisStorage`, `pgStorage` and
`surrealStorage` use a UUID; the rest an integer counter (`memoryStorage`
store-wide, `fileStorage`, `sqliteStorage` and `durableObjectStorage`
per-record). The runtime only ever compares them for equality.

Implementers: `memoryStorage` (`./host`), `fileStorage` (`./node`, dev only),
`redisStorage`, `pgStorage`, `surrealStorage`, `sqliteStorage` (embedded,
single-node, on `node:sqlite`), `durableObjectStorage`,
`unhostedStorage` (Cloudflare's Worker half — every operation throws, because
in-memory storage on a host that hosts nothing would be a silent lie).

### `saveText` — the optional single-walk path (#238)

A durable save used to walk the same state twice: the host encoded it to a
JSON-safe tree, then the adapter ran `JSON.stringify` over that tree, because
what a store wants is a string. The second walk measured at **+51%** on top of
the first, per checkpoint. `saveText` lets the host emit the string in one
pass (`stringifyWithHandlers`, `@sigx/serialize/stringify`) and hand it over.

Three rules make it safe to have two doors into one write:

1. **It is not a second FORMAT.** `saveText(t, k, json, etag)` must be
   indistinguishable from `save(t, k, JSON.parse(json), etag)` — same CAS,
   same conflict brand, same record on the next `load()`. Implement `save` in
   terms of `saveText` so they cannot drift; the three shipped providers do.
2. **Absent is a supported answer, and the host is correct either way.** It
   keeps the encoded-tree path for exactly that case. `memoryStorage` stores
   the tree by reference and `durableObjectStorage` hands a structured value
   to the platform — for both, a string would force a parse back on load.
   `fileStorage` declines for a different reason: its pretty-printed
   `{ etag, state }` envelope is what makes the store `cat`-able.
3. **A decorator must forward it, conditionally.** `decorateStorage` wrappers
   that return a fixed three-method literal drop the fast path silently — the
   host reverts to two walks with no error and no counter to show for it. And
   forwarding it unconditionally is the mirror bug: it advertises a capability
   the inner storage cannot honour.

The host takes the text path only when **nothing else needs the encoded
tree** — a boundary that also emits reuses the save's encode for its snapshot
(#233), and reviving from text instead would fork that back into two walks.
`#wantsSnapshotAt` is the predicate; correctness never depends on it, only
which of two correct paths runs.

Load is deliberately unchanged: `JSON.parse` + `reviveWithHandlers`, off the
hot path. There is no revive-from-string variant and none is wanted.

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
    undelivered?(ref: ActorRef, name: string, error: unknown): void;
}
```

`undelivered` is how a failed `deliver()` reaches the host's own numbers —
`HostStats.remindersUndelivered`, so `ops()`, `metrics()` gauges and the
cluster's per-host report all carry it (#306). Call it per failed attempt;
an implementation that does not call it leaves the counter at a `0` that
means "said nothing". The three provider implementations all call it and all
retry a failed dispatch one tick out under the rules below (#326) — each
against what its own claim wrote: `pgReminders` compares the row's `next_due`
with the value the claim set, in one statement per claimed batch;
`surrealReminders` compares `d` with `$at + p` for the `$at` its claim took
once per transaction; `durableObjectReminders` re-arms in the alarm's final
gated write, so the platform alarm is re-scheduled for the retry.

The default `shardedReminders()` keeps the table in `ActorStorage` under a
reserved type, split into 16 hash shards that hosts divide between them by
rendezvous hashing — which **assumes many actors per host**. It advances or
deletes an entry *before* dispatching (the per-shard etag CAS is what keeps
two tickers from double-firing), and puts an entry whose dispatch rejected
back for the next tick (`nextDue = now + tickMs`, one write per shard for
all of that tick's failures) unless the actor set it again meanwhile — so a
deadline or a restarting host costs one tick, not the wake, and a target
that never answers costs one attempt per tick. A one-shot the actor
*cleared* while its dispatch was failing may still be retried once (the
tick had already deleted it, so the clear left nothing for the re-arm to
see), which is one more reason `onReminder` must be idempotent. Where that
assumption is false, replace it: under Cloudflare's one-DO-per-actor model each
actor's reminders live in its own object and fire from its own alarm, so there
is nothing to shard and nothing to poll (and no cadence floor — an alarm fires
*at* the due time, where `shardedReminders()` promises only "at or after").

`ownsShard` is meaningless to an implementation that does not shard;
`pgReminders` ignores it because `SKIP LOCKED` lets every host claim disjoint
rows from one scan, while `surrealReminders` partitions by it because SurrealDB
has no lock primitive to make the other approach safe.

## `ActorTaskLiveness`

```ts
interface ActorTaskLiveness {
    bind(context: ActorTaskLivenessContext): void;
    start(): void | Promise<void>;
    stop(): void | Promise<void>;
    track(ref: ActorRef): Promise<void>;     // durable before a run launches
    untrack(ref: ActorRef): Promise<void>;
}
```

How a dead host's in-flight detached runs (`ctx.tasks`, and so every
`defineJob` run) get found again (#310). The run itself is durable in the
actor's state or its `$sigx:tasks` ledger; this seam only answers *which
actors to touch* when a host dies with nobody calling.

`bind()` receives the storage and scheduler as the reminders seam does,
plus `hostId`, `isHostLive(hostId)` and `ownsShard(shard)` from the
placement bindings (a cluster answers from its membership view; single-node
is live only to itself and owns every shard), and `touch(ref)` — the
`$sigx:reminder`/`TASK_REMINDER` delivery that activates an actor and lets
it resume.

The default `rosterTaskLiveness()` keeps one roster per host under
`$sigx:tasks-roster` — `{hostId}/p0..p15`, sub-sharded by the reminder hash,
plus a `$hosts` index written once per host. **The host is the sole writer
of its own roster**, which is the whole design: the table and etag are
cached, `track`/`untrack` are one CAS each with no load, and mutations that
land during a write ride the next one. Adoption runs on the reminder tick:
the owner of `reminderShardOf(hostId)` touches every actor of a host
`isHostLive` says is gone, drops what it touched, and deletes the drained
roster. `reminderTaskLiveness()` is the mechanism this replaced — a durable
reminder per running task — and remains right where a reminder is the
platform's own wake-up: `createHostDurableObject` selects it, because a
Durable Object's alarm re-activates the object and a per-host roster would
be a roster of one.

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

A `PlacementPolicy`'s `choose(ref, view, self)` receives a view **pre-filtered
to the hosts registering `ref.type`** (#212) — never empty, `self` possibly not
in it, and the answer must be a member of it (or `self`, which means "local"
and is guarded there) or the dispatch fails loudly.
`PolicyRuntime.view()` remains the full view (load probes are host-level). See
[clustering — registration-aware placement](clustering.md#registration-aware-placement).

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
everywhere), `tcpTransport()`, and Cloudflare's stub-swapped
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

The hooks (`src/host/app.ts`) are documented in full in
[the app guide](https://sigx.dev/actors/docs/the-app/). The composition rules
are the part worth
keeping in mind while editing: `decorateStorage` last-registered-outermost,
`useDispatch` outside-in and **must forward `dispatchStream`**, `onStart` in
order, `onStop` in reverse and *after* the drain, and a placement's own hooks
bracket the plugins'.

## `defineJob` hooks — `onReminder` and `onSettled`

Not runtime seams: extension points on the job layer, for the two things a run
body cannot observe from inside itself.

```ts
onReminder?(control: JobControl<Extra>, name: string): void | Promise<void>;
onSettled?(control: JobControl<Extra>, info: JobInfo<Extra>): void | Promise<void>;
```

`onSettled` fires on **every** terminal transition because `finish()` in
`src/job/define-job.ts` is the only one there is — `doCancel`, the `maxAttempts`
give-up, completion and body failure all route through it. That single funnel is
what makes the guarantee cheap to keep; a second terminal write elsewhere would
break it silently, so don't add one.

The two transitions that motivate it are precisely the ones with no body turn:
the give-up **refuses** the restart, and a `cancel()` on a paused job finds no
task to abort. An app projecting job status into its own store (a status row, a
metric) and maintaining it only from inside `run()` therefore strands that
projection at "running" forever. The hook runs inside the settling turn *after*
the save, so a throwing handler cannot unwind a transition the runtime has
already committed — it is caught and dev-warned. Same no-self-dispatch rule as
`onReminder`.

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
