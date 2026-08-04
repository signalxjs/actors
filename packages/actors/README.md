# @sigx/actors

**Virtual actors** for SignalX: addressable, single-threaded,
persistent server objects that integrate with sigx's server layer — actor
calls ride the same wire protocol, codec, and security posture as `serverFn`.

```ts
// cart.actor.ts
import { defineActor } from '@sigx/actors';
import { requireUser } from './guards.server';

export const CartActor = defineActor({
    type: 'Cart',
    authorize: [requireUser],
    state: () => ({ items: [] as Item[] }),
    methods: (ctx) => ({
        async addItem(item: Item) {
            ctx.state.items.push(item);   // deep signal — just mutate
            await ctx.save();             // explicit persistence
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
   (`reentrant: 'always'` and `methodReentrancy` opt out per actor or per
   method — see [Reentrancy & interleaving](#reentrancy--interleaving).)
3. **`await` does not yield the actor.** A turn ends when the method's promise
   settles — an awaited `fetch` inside a method blocks every queued call to
   that actor until it resolves. This is the non-reentrant default
   and the model's core trade: state safety over intra-actor concurrency.
   (Dev builds warn when a turn exceeds `slowTurnMs`.)
4. **Persistent.** `ctx.save()` writes state through the pluggable
   `ActorStorage` (etag optimistic concurrency); activation loads it back.
   A conflicting writer faults the stale activation
   (`ActorStateConflictError`) — the next call loads the winning state.
5. **Deadlock-detected.** Every call carries its chain; `A → B → A` into a
   non-reentrant actor **throws `ActorDeadlockError` immediately** with the
   full chain, instead of hanging until a timeout. `reentrant: true`
   allows call-chain re-entry (the cycle runs inline against your own turn);
   `reentrant: 'always'` interleaves everything, making the deadlock
   impossible by construction.

### Turns

Guarantees 2 and 3 are one mechanism, and most surprises trace back to it.

A **turn** is one method invocation on one activation, from the moment it
starts running to the moment its promise settles. Every activation runs its
turns one at a time, in the order they arrived.

That is the whole reason `ctx.state.count++` is safe with no lock — while
your turn is running, nothing else on that actor is. It is also why `await`
inside a turn costs more than it does in ordinary JS: the turn has not ended,
so every later call to that actor waits. **Slow I/O inside a turn is a queue
for that actor**, and `metrics()` splits the two halves so you can tell them
apart — `queueMs` (waiting for a turn; high means the actor is hot) versus
`turnMs` (holding the activation; high means the turn itself is slow).

Three things deliberately run *outside* a turn, so they cannot block one:
guards, task bodies (`ctx.tasks`) and stream iteration. Each can re-enter
with `ctx.turn(...)` when it needs to touch state safely.
`reentrant: 'always'` lets an actor's own turns overlap instead.

## Setup

```sh
pnpm add @sigx/actors
```

**Vite app** — add the plugin; it runs a dev host for you and mounts the
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
const host = await app.withActors([Counter]).start();
```

`withActors` throws if the app already declared `actors`, so a host can
never silently replace what the author configured. `examples/counter` runs
exactly this shape in dev and in production.

**Production server entry** — explicit composition, the sigx idiom:

```ts
import { createHost } from '@sigx/actors/host';
import { handleActorRequest, matchesActorRequest } from '@sigx/actors/server';
import { actors } from './dist/server/sigx-actors.js'; // build-emitted registry

const host = createHost({ actors, storage });
await host.start();

export default {
    async fetch(request: Request): Promise<Response> {
        if (matchesActorRequest(request)) return handleActorRequest(request, { host });
        if (matchesServerFn(request))     return handleServerFnRequest(request, { resolve });
        return documentHandler(request);
    }
};
```

Node servers use `createActorHandler` / `attachSignalHandlers` from
`@sigx/actors/node` (see `examples/counter/server.mjs`).

## The app: one config, many runtimes

`createHost` stays the low-level primitive, but it takes exactly ONE
`placement` and ONE `storage`, and `ActorPlacement.bind()` is its only
lifecycle-hook shape — so two things that both want `beforeActivate` cannot
coexist. `defineActorApp` is the composition root that fixes that: it folds
every plugin's contributions into the single placement, storage and context
`createHost` already understands.

```ts
// src/actors.app.ts — one typed source of truth
import { defineActorApp } from '@sigx/actors/host';
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
const host = await app.start();   // builds the host, starts it, runs onStart
await app.stop();                 // drains the host, then onStop in reverse
```

The app is an inert **description** until `start()`, which is what lets the
same module be started by a Node entry, the Vite dev server, or a Worker.

A started app is **single-use**: `start()` is idempotent while running, but
after `stop()` it refuses to restart (a plugin placement mints its identity
per run — a cluster host id is gone once its membership entry is), so build a
new app instead. A start that *fails* is the exception: the rejection is not
cached, so fixing the cause and calling `start()` again really retries.

### Writing a plugin

`setup()` receives a registry. Everything composes across plugins except
`setPlacement`, which is exclusive by nature — a second claim throws, naming
both plugins.

```ts
import type { ActorPlugin } from '@sigx/actors/host';

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
    allowAnonymous: true,
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
| `setPlacement` | **exclusive** | a factory, run once the host exists; a second claim throws, naming both plugins |
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

> Choosing between the shipped policies? See
> [Which placement policy should you use?](#which-placement-policy-should-you-use)
> — the short answer is that the default is right unless your load balancer
> hashes the routing token.


Two independent axes:

- **The backend** — *who hosts an actor at all*: the local host, a cluster,
  Durable Objects. One per app, claimed with `setPlacement`.
- **The strategy** — *which host a new activation goes to*. That is
  `PlacementPolicy` from `@sigx/actors/cluster`;
  ship your own `choose(ref, view, self)` alongside the built-in
  `randomPlacementPolicy()`, `consistentHashPolicy()`,
  `preferLocalPolicy()` and `activationCountPolicy()`.

A strategy can be declared **on the actor**, a per-type placement
attribute that beats the central `typePolicies` map:

```ts
export const Session = defineActor({
    type: 'Session',
    placement: preferLocalPolicy(),   // pin hot session-shaped types local
    // ...
});
```

`setPlacement` takes a **factory**, not an instance, precisely so a backend
can read those declarations — it runs once the host exists, so the context
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
not a fallback: silently placing an actor somewhere other than where its author
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
hosts divide between them — which assumes **many actors per host**:

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
const host = createHost({ actors, scheduler, defaults: { idleAfterMs: 0 } });
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

- **Authorization.** `requireAuthorization` cannot inspect a package, so
  declare `authorize` or `allowAnonymous` yourself. The host warns for a
  registered actor that
  declares neither.
- **Stream names** in the ref must match the definition; they drive wire
  routing.
- **Cacheable read names** likewise, as the ref's fourth argument
  (`__actorRef(type, endpoint, streams, reads)`): they are what makes the
  client issue GET, and a name that is in one place but not the other means
  either a 405 or a read that quietly never caches.
- **`type` is public API.** It is the wire, directory *and* storage key, so
  renaming it breaks deployed state. Two different actors claiming one type
  is refused at startup. Namespace it after the package (`acme/greeter`), but
  **not** with the npm scope form — a `type` starting with `@` or `$` is
  refused: those heads belong to the runtime's own data keys (`@actor`) and
  mounts (`$live`).

## Authorization

Actor methods are as security-sensitive as server functions, and the runtime
is **fail-closed**: an actor that declares nothing, in a process with no
server app configured, denies every call with 401. Access is decided by an
`authorize` policy, or waived explicitly with `allowAnonymous: true`, or
left to the app's default policy.

```ts
// src/server-app.ts — the ONE place app-wide policy lives
export const app = createServerApp<User>({
    middleware: [requestId, auditLog],   // work, every transport
    authenticate: sessionFromCookie,     // -> User | null
    codec: { encode: (u) => u.id, decode: (id) => ({ id }) }
});
```

```ts
defineActor({ type: 'Room', methods });                      // app default decides
defineActor({ type: 'Auth', allowAnonymous: true, methods }); // deliberately public
```

A policy is core's `ServerPolicy` — `(principal, rq, op) => boolean`,
**strict-`true`**: anything else denies (403; 401 when the principal is
null). A thrown `ServerFnError` passes through verbatim for a custom
status. Policies run on **every transport** — the wire endpoint, `$live`,
and in-process `actor()` calls — and always **outside any turn**, so a slow
check never occupies the actor's turn.

**The policy sees the instance**, which is what makes the common actor
question expressible at all:

```ts
defineActor({
    type: 'Cart',
    // op.resource is { kind: 'actor', type: 'Cart', key, method }
    authorize: (user, _rq, op) => op.resource.key === user.id,
    methods
});
```

`methodAuthorize: { methodName: [...] }` adds per-method policies, ANDed
**after** `authorize`. Actor-to-actor calls (`ctx.actor`) do not
re-authorize: authentication is per REQUEST and authorization is per ENTRY
POINT — the wire endpoint, the live endpoint, an in-process call, a job
enqueue — and a hop is none of those.

The endpoint-level `guard` option is **gone** (it was wire-only by
construction). App `middleware` runs in the same pre-decode slot and
reaches in-process calls too; a wire-only concern is one
`if (fn.transport !== 'wire') return;` in the middleware body.

### Identity: `ctx.principal`

The authenticated principal reaches every actor as `ctx.principal`, with
nothing to stamp and nothing to thread through arguments:

```ts
defineActor({
    type: 'Room',
    methods: (ctx) => ({
        async post(text: string) {
            const from = (ctx.principal as User | null)?.id;
            // …
        }
    })
});
```

It rides a **first-class slot on the call envelope**, not the context bag —
so it cannot be forged through `.with({ bag })`, and it cannot be dropped
by forgetting to stamp it. It is inherited unchanged by `ctx.actor` and
`ctx.publish` hops and carried host-to-host, so a downstream actor sees
**whoever entered the system**, not the actor that called it. Decoding is
lazy and memoized, so an actor that never reads it pays nothing.

Inside a `defineJob` run body it is `job.principal` instead: a job outlives
the request that started it, so authorization happens once at enqueue and
the run reads the snapshot recorded there — persisted, so it survives
deactivation and every crash-resume.

It needs `codec` on `createServerApp` (a principal has to round-trip as a
string to ride the envelope). Without one it propagates nothing and
dev-warns once — fail-closed at the reader. **Treat `null` as
unauthenticated, never as a default principal**: between hosts this rides
outside the cluster HMAC, exactly like the bag, so its trust is the
deployment perimeter rather than a proof.

### The request-context bag

The bag is the channel for app DATA — a correlation id, a tenant hint —
that inner hops would otherwise never see. (Before the guard split it also
carried identity; that is `ctx.principal`'s job now, precisely so a
forgotten stamp cannot silently drop the caller mid-chain.)

```ts
import { stampCallBag } from '@sigx/actors';

const withTenant: ServerMiddleware = (rq) => {
    stampCallBag(rq, { tenant: tenantOf(rq) });   // ← stamped once…
};

defineActor({
    type: 'Room',
    methods: (ctx) => ({
        async post(text: string) {
            const tenant = ctx.bag.tenant;        // ← …read anywhere
        }
    })
});
```

`stampCallBag(rq, entries)` merges string entries (last wins) onto the
request; after the pipeline runs, the endpoint lifts them into the call
context, where they ride the whole chain: `ctx.bag` on the called actor,
inherited by `ctx.actor(...)` and `ctx.publish(...)` hops (policies do not
re-run on those — a hop is not an entry point),
across host-to-host hops on the envelope, and into `$live` watches. The
in-process `actor()` entry lifts the same store, so a serverFn's stamp
reaches the actor with no HTTP hop. `actor(Def, key).with({ bag })`
sets entries explicitly (server-side scripts, tests, ops), merging over the
stamped/inherited ones — explicit wins.

The rules, all deliberate:

- **String-only and size-capped**: at most 8 entries, 64-byte keys,
  256-byte values, 1 KiB total (UTF-8 bytes; the `CALL_BAG_MAX_*` exports).
  The bag rides an HTTP header host-to-host, and a cap that exists keeps
  the wire honest. `stampCallBag` and `.with({ bag })` **throw** on
  violation — developer input fails at the developer's stack.
- **Never client-settable.** The public endpoint reads no header into the
  bag; the only sources are server-side. A browser-settable `user` entry
  would be an authorization bypass.
- **Dropped whole, never partially, and never a 400.** En route (the
  envelope), a malformed or over-cap bag is silently dropped in one piece —
  a partial identity is worse than none, and a header-triggered 400 is a
  DoS lever. Consequence: **treat a missing entry as unauthenticated**,
  never as some default principal.
- **Not integrity-protected between hosts.** The cluster HMAC signs the
  call identity, not the envelope body, so the bag's integrity rests on the
  same perimeter posture as the rest of the envelope: run mTLS/VPC between
  hosts.
- **Detached work does not inherit it**: task bodies and volatile timer
  ticks read an empty `ctx.bag`, exactly as they carry no `traceparent` —
  they outlive (or have no) caller.
- `ctx.bag` is frozen and resolved per read, so it is turn-correct on
  interleaving activations, and an empty frozen object outside any turn.

## Cacheable reads (`reads:`)

Declare a method a cacheable read and the endpoint accepts `GET` for it and
emits the `Cache-Control` you asked for, so browsers, CDNs and reverse
proxies absorb read traffic that would otherwise reach an actor:

```ts
defineActor({
    type: 'Product',
    allowAnonymous: true,
    reads: {
        summary: { maxAge: 5 },
        price: { maxAge: 60, public: true, staleWhileRevalidate: 30 }
    },
    state: () => ({ cents: 999 }),
    methods: (ctx) => ({
        async summary(currency: string) { … },
        async price() { return ctx.state.cents; }
    })
});
```

`GET {base}/r/{token}/Product%23price?args=["p1"]` → the same envelope a POST
returns, plus `Cache-Control: public, max-age=60, s-maxage=60,
stale-while-revalidate=30`. The declaration is core's `ServerFnReadCache`
vocabulary, unchanged, and the build stamps the names onto the client ref so
the proxy issues GET on its own — nothing at the call site changes.

**What you are trading, stated plainly:** a cached read **bypasses the
turn ordering guarantee**. For `maxAge` seconds the response an
intermediary serves may be older than the actor's state, and nothing on the
server can pull it back — not `ctx.save()`, not `useActorAction`, not
`cells.invalidate()`, which refresh this page's cells and never a CDN's copy.
Declare it where staleness is a product decision, not where it would be a
bug. The declaration is also a promise the runtime cannot check: a listed
method must be side-effect-free and idempotent, exactly as with core's
`cache` on a serverFn — a mutating method declared cacheable re-opens CSRF.

The rest follows from that, and is checked:

- **`public` is gated.** It puts the response in SHARED caches, where one
  caller's copy is served to the next, so core's contract is args-only —
  never cookies, auth or headers. A guard is the one thing here that provably
  reads the request and nothing can inspect *what* it reads, so `public` on a
  guarded read is a definition-time throw. Without `public` the read is still
  cached, per client, and the endpoint adds `Vary: Cookie`.
- **Guards still run**, on GET exactly as on POST. A rejection answers its
  status with `Cache-Control: no-store` — a failed read is never cacheable.
- **Streams cannot be declared**, and saying so is a definition-time throw
  rather than a silently ignored declaration.
- **POST keeps working** for every declared read: the declaration lives on
  the definition, not on the wire, so a hand-built host with no build
  transform, an older client, or a service calling by hand all still work.
- **Routing is unchanged** — the token travels in both carriers, and an actor
  another host owns is proxied as usual, with the answering host making the
  caching promise.
- **No `content-type` on the GET**: it would describe a body that does not
  exist, and it is a non-safelisted header, so leaving it off is one fewer
  reason to preflight. Not a promise of no preflight — the routing token
  header ships by default and triggers one on its own, so a cross-origin
  caller who needs a genuinely simple GET wants `route: 'none'` and no custom
  headers too. Same-origin (the usual case) never preflights either way.
- **A GET puts the actor key and every argument in the URL**, where a POST
  body kept them out of access logs, proxy traces and referrer headers. That
  is the same log-hygiene concern the hashed routing token exists for, and it
  now applies to the arguments too, in plaintext. `maxAge` values are the
  whole non-negative seconds `Cache-Control` actually defines — a fractional
  one is a malformed directive, so it is refused rather than emitted.

Per call, `actor(Def, key).with({ get: false }).summary()` sends a declared
read as a POST instead: no caching, but no arguments in the URL either, and
no query-length cap (a long enough query is a 414). The endpoint accepts both
carriers for a declared read, so this is a client-side choice.

## One-way calls

```ts
await actor(Notifier, userId).with({ oneWay: true }).notify(event);
```

A one-way call resolves as `Promise<void>` when it is **accepted into the
target's activation** — locally when scheduled, remotely when the transport reply
comes back from the receiving host's enqueue — never when the turn completes.
Fire-and-forget with backpressure and error accounting, instead of the
floating-promise workaround that has neither: awaiting the call still tells
you the runtime has taken responsibility for it, without paying for the turn.
It works on every proxy — the browser client, the server entry, and
`ctx.actor(...)` — and types narrow to `Promise<void>` at the call site.

What happens to failures depends on when they happen, and the line is
acceptance:

| Failure | When | The caller sees |
|---|---|---|
| guard veto, unknown type, activation failure, wrong host, auth, unreachable peer, host shutting down | before acceptance | a normal rejection |
| the method throws, a deadline expires mid-turn | after acceptance | nothing — dropped, counted as `oneWayFailures` in `metrics()` on the host that ran the turn (and a dev-mode console warning) |

The details that follow from "the caller is gone":

- **Ordering holds.** The call was enqueued before the promise resolved, so
  an awaited call issued afterwards runs after it — same turn FIFO as
  always.
- **A one-way self-call cannot deadlock.** Awaited, `A → A` on a
  non-reentrant actor is a detected `ActorDeadlockError`; one-way it is
  "schedule more work for myself" and simply queues behind the current turn.
- **The caller's deadline race is skipped** — there is no caller left to
  race. The deadline still rides the context and bounds the turn's own
  nested awaited calls.
- **Streams refuse it** (a stream is consumed, not fired-and-forgotten), and
  a declared read goes out as a **POST** even under `get: true` — an ack
  must never be served from an HTTP cache.
- **On the public wire** the proxy sends `x-sigx-one-way: 1` and the
  endpoint answers at acceptance; host-to-host the flag rides the envelope
  as an additive `ow` field, no protocol bump.
- **Mixed-version clusters degrade gracefully**: an older receiving host
  ignores the flag and answers at turn completion — the call is still
  delivered exactly once; the sender just waits longer, and a post-acceptance
  failure can reach it as a rejection during the rolling deploy.
- **Delivery is at-most-once after the ack**, exactly as for normal calls: a
  connection lost between acceptance and the reply can be retried by the
  cluster's routing, the same window every unary call already has.

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
- `migrateState` evolves a record whose shape predates this deploy — the
  answer to "the `state:` shape changed and the stored records didn't". It
  runs between the storage read and activation, and only on a load that
  **found** a record: never on the `state(key)` fresh path, never on
  `ctx.clearState()`. It also runs before `onActivate`, which therefore
  always sees migrated state.

  ```ts
  defineActor({
      type: 'Cart',
      state: () => ({ v: 2, items: [], coupons: [] }),
      migrateState: (stored) => {
          const s = stored as CartV1 | CartV2;
          if ('v' in s) return s;                              // fast path
          return { v: 2, items: s.items ?? [], coupons: [] };  // v1 → v2
      },
      // …
  });
  ```

  `stored` is already codec-revived (per the bullet above — `Date`/`Map` are
  real objects), so `unknown` means unknown *shape*, not raw JSON. A second
  argument carries `{ raw, key }` when the revived view can't tell two stored
  versions apart: `raw` is the encoded record as storage holds it.

  **Returning the input unchanged is the fast path**, and identity is how
  that's detected — so to migrate, return a *new* object.

  The migrated shape is written back **lazily**: it rides the next save the
  actor would have made anyway, so a read-only activation still issues zero
  writes and a rolling deploy costs no extra ones. That rule holds in both
  persistence modes — `migrateState` never causes a write by itself, so a
  write-behind actor that is only ever *read* after a migration doesn't
  persist it. For a record that would otherwise never be saved at all (and so
  would be re-migrated on every activation forever),
  `{ persist: 'eager', migrate }` opts into one CAS write-back at activation;
  if a peer migrated first, the loser adopts the winner's record rather than
  failing.

  The trade is stated rather than hidden: a fleet mid-deploy can migrate the
  same record on several hosts. That's safe because the hook is a pure
  function of the stored value and every write is etag-CAS'd — first save
  wins, and the loser either adopts the winner (eager) or gets
  `ActorStateConflictError` and re-activates against it (lazy).

  Synchronous, and a throw fails activation with `ActorActivationError` —
  the same posture as a throwing `onActivate`. Corrupt state is loud, and the
  stored record is never silently reset. Version bookkeeping is your
  convention: the runtime neither reads nor writes a version field, and this
  is deliberately not a scheme for versioning an actor's *interface* across a
  mixed-version fleet. **`defineJob` does not take `migrateState`**: a job's
  stored record is the job envelope (`status`/`progress`/`checkpoint`/… with
  your own state under `extra`), so a hook over it would hand you a runtime
  shape you don't own. Migrating the `extra` half wants its own option, and
  is not part of this.
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

## Reentrancy & interleaving

By default an actor is strictly serial (guarantees 2–3) and a call cycle
back into it is a detected deadlock (guarantee 5). `reentrant` widens that,
in two steps:

```ts
reentrant: 'call-chain'   // alias: true — the v1 behavior
reentrant: 'always'       // full interleaving, per actor
methodReentrancy: { stats: 'always' }   // full interleaving, per method
```

- **`'call-chain'`** (`true`): `A → B → A` runs *inline* against your own
  up-stack turn instead of deadlocking. Unrelated calls still serialize —
  no foreign interleaving, so state stays turn-consistent.
- **`'always'`**: every call is its own turn, launched immediately —
  unrelated calls interleave at every `await` (the Orleans `[Reentrant]`
  model). The single-threaded guarantee narrows to what JS itself gives
  you: no two turns run *between* awaits, but **your state can change
  across every `await`** — re-read, don't cache, anything another turn may
  move. In-chain calls complete as concurrent turns (never inline), so a
  self-cycle cannot deadlock by construction.
- **`methodReentrancy`** marks individual methods `'always'` on an
  otherwise serial (or `'call-chain'`) actor — the canonical case is a
  read-only `get`-style method that must not queue behind a slow write
  (pairs well with `reads:`). A mapped method never waits and is never
  waited for; unlisted methods keep the actor-level behavior, including
  their mutual exclusion. Keys must name `methods:` entries; the runtime's
  own deliveries (`$sigx:reminder`, `$sigx:topic`) follow the actor-level
  setting only. Redundant next to `reentrant: 'always'`, so that
  combination is refused.

What interleaving changes elsewhere — saves from concurrent turns are
single-flighted (last-writer-wins at whole-state granularity; a save
resolves once a snapshot at-or-after your mutations is durable), a
write-behind flush may capture mid-logical-turn state (it is still a
synchronously-consistent frame), and deactivation drains *all* in-flight
turns before `onDeactivate`. Declarations are validated at the type's first
activation, loudly, in every build. Interleaving needs `AsyncLocalStorage`
(per-turn call context): built into Node/Deno/Bun; on Cloudflare Workers it
rides the `nodejs_compat` flag the DO package already requires. Serial
actors never touch it.

## Timers & reminders

- `ctx.timer(name, cb, { due, period, keepAlive })` — **volatile**: ticks run
  as ordinary turns (coalesced under load) and die with the
  activation. Timers don't keep an actor alive unless `keepAlive: true`.
- `ctx.reminders.set(name, { due, period })` — **durable**: stored through
  `ActorStorage`, fired by the host's scheduler, and they **re-activate an
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
any turn (a stream that waited on its own actor's next turn while holding
the activation would self-deadlock), so they must read `ctx.snapshot()` /
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

- **A quiet stream is kept alive at the byte layer.** A stream that yields
  nothing sends nothing, and every intermediary with an idle timeout
  (ingress at 60 s, cloud load balancers at ~4 min, mobile NATs) closes it;
  the client then sees a stream that "ended without a done/error
  terminator". The endpoint therefore emits a `{"ping":1}` line after 30 s of
  silence, which the client's reader skips. Tune or disable it with
  `handleActorRequest({ streamPingMs })` — `0` is off.

**Reading current state? Prefer `useActorState(…, { live: true })`** over a
hand-written `streams:` body. It pushes the result of the *read you already
declared*, multiplexes every live read on the page onto one connection, and
reconnects on its own. `streams:` is for a feed that is not a read of current
state: a log tail, a progress sequence, an event history.

## Topics (actor-to-actor pub/sub)

An actor that changes something often needs to tell N interested actors
without knowing who they are. Declare the interest on the *subscriber* and
publish from anywhere on the server:

```ts
import { topic } from '@sigx/actors';

export const chatMessages = topic<{ from: string; text: string }>('chat-messages');
// per-room: topic('chat-messages', roomId)

// The subscriber declares its interest — nothing registers, nothing is stored.
export const RoomFeed = defineActor({
    type: 'RoomFeed',
    authorize: [requireSession],
    state: () => ({ recent: [] as { from: string; text: string }[] }),
    methods: (ctx) => ({
        async recent() {
            return ctx.snapshot().recent;
        }
    }),
    subscriptions: {
        // subscriber key = topic key, so RoomFeed/room-1 gets room-1's events
        'chat-messages': async (ctx, event) => {
            ctx.state.recent.push(event.payload as { from: string; text: string });
            await ctx.save();
        }
    }
});

// Publish from another actor's turn…
const report = await ctx.publish(topic('chat-messages', ctx.key), { from, text });
// …or from a serverFn / script via the running host:
await publishTopic(topic('chat-messages', roomId), { from, text });
```

Subscriptions are **implicit and declarative**: the subscriber set is a pure
function of the deploy — every registered type whose `subscriptions:` names
the topic. A publish **activates idle subscribers** exactly the way a
reminder delivery does, and each delivery is an ordinary dispatch of the
reserved `$sigx:topic` method through placement, so a subscriber owned by
another host is reached over the internal transport (HMAC, deadlines,
branded errors — all of it) with no topic-specific wire machinery. The cost
model is S dispatches per publish, S = subscribing *types*, not activations.

**Delivery is best-effort, at-most-once, and settled.** `publish()` resolves
when every subscriber's handler turn has settled and reports what happened:

```ts
const { subscribers, delivered, failures } = await ctx.publish(chatMessages, msg);
// failures: [{ type, key, message, kind? }] — a throwing handler, a dead
// host, a detected deadlock. The publisher NEVER throws for a subscriber.
```

Nothing is persisted or retried; a subscriber that was down missed the
event. Backpressure is intrinsic — the publisher awaits the turns, bounded
by its call deadline. FIFO holds per publisher→subscriber pair **only when
the publisher awaits its publishes sequentially**; concurrent publishes have
no relative order.

The details worth knowing:

- **Key mapping.** An entry may be `{ key: (topicKey) => subscriberKey,
  handle }` — `key: () => 'aggregate'` makes one singleton receive every
  key's events. The default is identity: topic key = subscriber key.
- **Cycles are deadlocks, not hangs.** `ctx.publish` carries the publishing
  turn's call chain, so a subscription that dispatches back into a
  non-reentrant publisher fails that delivery with `kind: 'deadlock'` in
  the report — the publisher is awaiting the fan-out, so an undetected
  cycle could never complete. `reentrant: true` delivers inline instead;
  `reentrant: 'always'` delivers as a concurrent turn of the publisher.
- **Handlers are turns.** They mutate state and `ctx.save()` like any
  method; a throwing handler fails only its own delivery and does not
  fault the activation. Handlers are not wire-callable and never appear on
  the client.
- **Pages observe topics through a projection.** A subscriber actor folds
  events into state; the page reads it with
  `useActorState(RoomFeed, roomId, 'recent', { live: true })` — the
  existing live channel pushes after every handler turn. No new wire.
- **Rolling deploys skew the subscriber set.** A host publishes to the
  subscribers *its* registry declares, so a newly-added subscribing type
  misses publishes from not-yet-rolled hosts until the deploy completes —
  consistent with best-effort delivery.
- **Hot topics pin subscribers active**: every delivery is activity, so a
  busy topic resets its subscribers' idle clocks (`idleAfterMs`).

## Tasks (long-running operations)

A method call holds the activation until it settles — fine for milliseconds,
wrong for a sync job or an AI workflow run. Declare that kind of work in the
`tasks:` factory and start it with `ctx.tasks.start(name, input?)`: the body
runs **detached, outside any turn**, so ordinary reads, streams and watches
keep answering while it works.

```ts
const Sync = defineActor({
    type: 'Sync',
    allowAnonymous: true,
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
  turn with the full context. Every mutation stays race-free, and
  everything downstream of a turn (change feeds, watches, write-behind) works
  unmodified. Reads in the body use `ctx.snapshot()` / `ctx.changes()`.
- **`ctx.abortSignal` in a task is the RUN's signal.** It fires on
  `ctx.tasks.cancel(name)` (reason `'cancelled'`) and on deactivation for
  any reason (reason: the `DeactivationReason`) — *before* the turn
  drain. Deactivation gives signalled tasks a bounded grace
  (`taskGraceMs`, default 10s) with turns still open, so a
  winding-down task can run one final `turn()` checkpoint.
- **`cancel` is a request, not a join.** It aborts and returns; the run
  leaves `ctx.tasks.list()` when its body settles. (Awaiting settlement from
  a method turn would deadlock with the task's own wind-down `turn()`.)
- **A running task keeps the actor alive** — the idle sweeper skips it, like
  an open stream.
- **`start` is single-flight per name** and resolves when the body is
  *launched*, not finished. A task that throws is terminal — no automatic
  retry; that policy belongs to the layer above.
- **No wire surface.** Start/cancel/status go through your own methods, so
  your guard chain governs them like any other call.

**Crash-resume is built in.** `start()` resolves only after the run is
durably recorded: a ledger entry in the reserved `$sigx:tasks` storage
record plus a liveness reminder armed under the same name. From then on:

- A run **interrupted by deactivation** (any reason but cancel) keeps its
  entry — the next activation of the actor restarts it, with
  `TaskInfo.restarts` bumped and the original `input` replayed through the
  state codec. Completion, a throw, and `cancel` all remove the entry (a
  thrown task is terminal; crash ≠ throw), and an empty ledger disarms the
  reminder.
- The **reminder is the crash driver**: when a host dies, the reminder
  shards are re-owned by the surviving hosts, the next tick delivers
  through placement, and the actor — tasks and all — re-activates wherever
  the cluster puts it, within roughly 60–90s. No client call needed.
- The contract is **at-least-once**: the runtime resumes the *function*;
  your code resumes the *work* from its own checkpointed state (the
  `ctx.turn()` + `save()` pattern above — resume by reading how far the
  last checkpoint got). A run that completes in the same instant its host
  stops may restart once more; make the last step idempotent or gate it on
  state.
- On the Cloudflare Durable Object backend this degrades gracefully: the
  ledger lives in the DO's own storage and the liveness reminder maps onto
  its alarm; a fiber does not survive eviction, so a task there is
  checkpoint-and-resume with short gaps rather than one continuous run —
  the same at-least-once contract, checkpoint aggressively.

## Jobs (`@sigx/actors/job`)

`defineJob` is the packaged experience on top of `tasks:` — one durable
long-running operation per actor, with the state machine, progress,
checkpoints and client surface already decided. Start a job from a request
handler and return immediately; check on it from anywhere in the cluster.

```ts
import { defineJob } from '@sigx/actors/job';

export const SecuritySync = defineJob({
    type: 'SecuritySync',
    authorize: [isAdmin],
    maxAttempts: 3,          // crash-resume attempts before 'failed'
    retainMs: 86_400_000,    // keep the terminal record a day, then forget
    run: async (job, input: { providerId: string }) => {
        const users = await loadUsers(input.providerId, { signal: job.signal });
        const from = (job.resumedFrom as { cursor: number } | undefined)?.cursor ?? 0;
        for (let i = from; i < users.length; i++) {
            job.signal.throwIfAborted();
            await syncOne(users[i]);
            await job.progress({ done: i + 1, total: users.length });
            if (i % 100 === 0) await job.checkpoint({ cursor: i + 1 });
        }
        return { synced: users.length };
    }
});

// A request handler — returns immediately, the job runs on the cluster:
const runId = crypto.randomUUID();
await actor(SecuritySync, runId).start({ providerId });
// Later, from anywhere:
await actor(SecuritySync, runId).status();   // JobInfo: status/progress/attempts
await actor(SecuritySync, runId).cancel();   // marked immediately, run aborted
await actor(SecuritySync, runId).result();   // the return value, once completed
for await (const info of actor(SecuritySync, runId).watch()) render(info);
```

What the layer decides for you:

- **State machine**: `pending → running → (paused ⇄ running) → completed |
  failed | cancelled`. `status()`/`watch()` return `JobInfo` — never the
  checkpoint (private) or the result (fetched once via `result()`).
- **One actor per run** — key = your run id. The directory's
  single-activation guarantee *is* the "exactly one runner" guarantee.
- **`start` is idempotent under retry**: a non-pending job returns its
  current info and never restarts.
- **Crash-resume counts, pause-resume is free**: a crash-resumed run
  arrives with `job.attempt` bumped and `job.resumedFrom` set to the last
  checkpoint; past `maxAttempts` the job is marked `failed`. `resume(data)`
  on a paused job re-runs with `job.resumeData` and no attempt cost.
- **`pause` parks durably**: `return job.pause(checkpoint)` writes the
  checkpoint, marks `paused`, and releases the task — the actor idles at
  zero cost until `resume()`. For a timeout, arm `job.reminders` before
  pausing and handle it in `onReminder(control, name)` — `control.resume()`
  / `control.cancel()` are internal, so no self-dispatch deadlock.
- **Progress rides the change feed, not storage**: `job.progress()` (and
  `job.update()` for your own `state:` extra fields) mutate state in a
  turn so `watch()` pushes them live, but nothing is persisted per tick —
  after a crash, progress honestly regresses to the last checkpoint.
- **`retainMs`** keeps the terminal record around for late `result()`
  readers, then a one-shot reminder clears the state and deactivates;
  `discard()` does it on demand.

A singleton queue-worker (strict ordering, bounded concurrency), a
cron-on-reminders scheduler, and the Cloudflare DO posture are recipes,
not API — see [`docs/job-recipes.md`](https://github.com/signalxjs/actors/blob/main/docs/job-recipes.md).

## Stateless workers (`defineWorker`)

Everything above assumes an actor **is** somebody — one identity, one
activation, turns in order. Pure compute (validation,
transformation, fan-out work) has none of that: two calls to the same
worker have no shared state to protect, so serializing them behind one
serialization is a bottleneck the semantics never asked for. `defineWorker`
declares a type whose activations are **interchangeable**:

```ts
import { defineWorker } from '@sigx/actors';

export const Resize = defineWorker({
    type: 'Resize',
    authorize: [requireUser],
    maxLocal: 8,             // pool cap; default: hardwareConcurrency (≤16)
    methods: () => ({
        async run(image: Uint8Array, width: number) {
            return transform(image, width);
        }
    })
});

await actor(Resize, 'any').run(img, 800);   // callers look exactly the same
```

The contract, stated loudly because it is the whole point:

- **Two calls to the same key may run concurrently, on different pool
  members.** The host keeps up to `maxLocal` activations per (type, key),
  spun up under load, and each dispatch rides the member with the fewest
  queued turns. `ctx.key` is still the key the caller addressed — it just no
  longer names a single runner.
- **Always local, zero directory traffic.** A worker activates on
  whichever host (or Cloudflare isolate) received the call: no directory
  claim, no lookup, no routing, no 421 redirect — and nothing for a
  cluster to fence, migrate or rebalance. (Both invariants are gated
  exactly in CI: `directory_ops == 0`, pool ≤ cap.)
- **No identity, so no identity-bound surface.** `state`, `persistence`,
  `reminders`, `tasks:`, `subscriptions:`, `placement` and `reentrant`
  do not exist on `WorkerOptions` — the option is a compile error, and
  `ctx.state` / `ctx.save()` etc. are typed away (`WorkerContext`) and
  throw if reached through a cast.
- What remains: authorization (`authorize`/`methodAuthorize`/`allowAnonymous`, same build gate),
  `reads:` (a pure read is the ideal cacheable GET), `streams:` (pure
  generators — an open stream pins its member against the sweep),
  `onActivate`/`onDeactivate` for per-member warm-up/teardown (load a
  model once per member, close it on the way out), and `ctx.timer` /
  `ctx.actor` / `ctx.publish`.
- **Pool members idle-collect individually** after `idleAfterMs` — a
  quiet worker shrinks back to zero footprint.
- **A same-key self-call is a deadlock, deterministically.** `reentrant`
  does not exist for workers, so `ctx.actor(Self, ctx.key)` throws
  `ActorDeadlockError` rather than working only when the pool happens to
  have a free member. A different key is a different pool and fine.
- **Watches are refused** — a watch is a state-change feed and a worker
  has no state.

Workers live in `*.actor.ts` files like every other definition (not
`*.worker.ts` — that suffix belongs to Vite's Web-Worker convention), and
the build swaps them for the same wire client, so a browser can call one
directly. In an app, `app.defineWorker` is the plugin-typed twin, exactly
like `app.defineActor`.

## Lifecycle

- `onActivate(ctx)` / `onDeactivate(ctx, reason)` hooks; an `onActivate`
  throw fails all parked callers and forgets the activation (nothing is
  remembered — the next call retries from scratch). `migrateState` runs
  before both, between the storage load and activation — see
  [Persistence](#persistence).
- Idle actors deactivate after `idleAfterMs` (default 20 min; per-actor
  override). `ctx.deactivate()`: finish the
  queue, then go.
- `defaults.maxActivations` (default 0 = unlimited) is a **soft cap**: when
  the sweep finds more active than this, it deactivates the
  least-recently-used idle, unheld ones with reason `'capacity'` — LRU
  pressure relief before memory pressure does it for us. Busy, queued, or
  kept-alive activations are never shed, so a genuinely loaded host may
  sit over the cap until it quiets; a shed actor re-activates on its next
  call, state intact. Rides the sweeper (`sweepIntervalMs > 0`), and
  `metrics().activations.byReason.capacity` says how often it fires.
- `host.stop()` drains every activation, flushes persistence, ends open streams
  and rejects new external calls.
  `attachSignalHandlers(host, { server, onStopBegin })`
  wires it to SIGTERM/SIGINT **and** drains the HTTP edge,
  which is the other half of a graceful shutdown. Both options matter:
  `onStopBegin` is what retires keep-alive sockets gracefully, while
  `server` alone only closes the listener at the end — see Clustering for
  the full recipe and why the order is what it is. A failed drain exits
  non-zero rather than vanishing as a clean stop.
- Deactivation fires `ctx.abortSignal` **first** — before draining the
  any turn — so a parked turn or a running task can observe it and wind down
  inside the drain window instead of holding it hostage.
- Calls that arrive during deactivation wait and land on a fresh activation.
- External calls get a deadline (`callTimeoutMs`, default 30s) — on expiry
  the **caller** gets `ActorCallTimeoutError`; the turn itself always runs
  to completion. Enforcement is never early but coarse when far: a deadline
  ≥ 10 s away shares one registry tick and may fire up to ~2 s late, while
  a short budget (a wire hop arriving nearly spent) gets an exact timer.

### Seeing which actors are live

`host.stats()` gives you the counts. `host.activations()` gives you the
actors themselves — bounded, sorted, and safe to poll:

```ts
host.activations({ sortBy: 'queued', limit: 20 });
// [{ type: 'Cart', key: 'user-42', queued: 7, ageMs: 812_004,
//    idleMs: 0, keptAlive: false, tasks: 0 }, …]
```

`tasks` is the running detached-task count — the actors hosting
long-running work, and the usual reason a `keptAlive` row is being skipped
by the idle sweeper. `sigx actors top` shows it as a TASKS column.

`sortBy` picks which end you care about: `'queued'` (default) is the hot
actors, `'age'` the long-lived ones, `'idle'` the next sweep's candidates.
`type` filters. Ties break on the actor id so the order is **stable between
polls** — a table that reshuffles equal rows at 1 Hz is unreadable.

It walks the directory, so it costs O(activations) and allocates a record
per candidate; `limit` defaults to 100 because this is a "top N" view and a
host can hold millions. Poll it at human rates, not per request.

`ageMs` is monotonic and `idleMs` is wall-clock — deliberately different
clocks. Age is a duration and must survive an NTP step; idle is compared
against `idleAfterMs` by the sweeper, which genuinely wants wall time.

`stats()` also reports `transitional: { activating, deactivating }`. Those
slots have no activation to read yet, so they are not in `activations` and
never were in the counts — which meant a host in the middle of an
activation storm read as **idle**, at exactly the moment you were looking
at it.

## Metrics

`metrics()` is a plugin that counts what the host is doing. Pull-based: no
exporter, no push pipeline, no metrics-library dependency — you read
`snapshot()` whenever you want, from a route, a health check, or a test.

```ts
import { defineActorApp, memoryStorage, metrics } from '@sigx/actors/host';

const m = metrics();
const app = defineActorApp({ actors, storage: memoryStorage() }).use(m);
const host = await app.start();

m.snapshot();
// {
//   windowMs: 60_000,
//   calls:       { total: 12_400, failed: 3, streams: 2 },
//   latencyMs:   { count, minMs, maxMs, meanMs, p50Ms, p90Ms, p99Ms },
//   queueMs:     { ... },   // waiting for a turn
//   turnMs:      { ... },   // holding the activation
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
| high `queueMs` | the actor is a hotspot — callers are waiting behind each other | shard the key, or reduce traffic to it |

A dispatch middleware only ever sees the sum, which is why `queueMs` is the
number people usually lack. `metrics()` gets it from `observeTurns`, the one
seam this needed (see below).

`conflicts` is worth an alert rather than a graph: each one is an etag
mismatch that discarded an activation.

### Per method, and why calls fail

`byType` tells you a type is slow; it never tells you *which of its methods*
is. `byMethod` carries the same five numbers keyed `Type#method`, and the
queue/turn split is most useful there — within one type, a hot actor and one
slow method look identical until you separate the methods.

```ts
m.snapshot().byMethod['Cart#checkout'];
// { calls: 4_100, failed: 2, latencyMs, queueMs, turnMs }
```

`errors.byKind` counts `ActorErrorKind` — `'call-timeout'`, `'wrong-host'`,
`'state-conflict'`, `'unreachable'`, `'deadlock'`, `'activation'`,
`'method-not-found'`, `'host-shutdown'` — plus `'(unknown)'` for anything an
actor method threw itself. `calls.failed` says a host is failing; this says
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
wait on whatever was behind them. Call-chain-reentrant `ctx.actor` calls run
inline against the caller's turn and are excluded too. Interleaved turns
(`reentrant: 'always'` / `methodReentrancy`) fire once per turn like any
other, but launch immediately: `queuedMs` is ~0 and `elapsedMs` is how long
the turn ran, not how long it held the activation — their intervals can
overlap. Throwing from an observer is swallowed and dev-logged — it can
never fail a turn.

Registering no observer leaves the hot path exactly as it was: the
timestamps are only taken when someone is listening.

**In a cluster, `metrics()` is per-host and the two halves of a cross-host
call land on different hosts.** `calls.total` and `latencyMs` are counted on
the host that *originated* the call; `queueMs`/`turnMs` and the activation
gauges on the host that *executed* it. That is a split, not a double count —
an inbound hop goes through the raw local dispatcher and never touches
`useDispatch` — so summing `calls.total` across hosts gives the true number
of calls the cluster served, once each.

## Health & readiness

`health()` adds two endpoints. It knows nothing about clustering: it serves
the aggregate of every plugin's `reportHealth()` contribution, and
`cluster()` contributes its own. So this is the whole wiring, clustered or
not:

```ts
import { defineActorApp, health } from '@sigx/actors/host';

export const app = defineActorApp({ actors, storage })
    .use(cluster({ providers, advertise, secret }))   // optional
    .use(health());
```

**`cluster({ secret })` is mandatory outside development**, for the same
reason `ops({ secret })` is, and with more at stake. The internal host-to-host
mount runs **no guards** — the public edge is supposed to have run them — and
it is contributed as an ordinary route, so it lands on the same listener as
your public actor endpoint. Omitting the secret therefore does not mean
"clustering without auth"; it means every registered type, key and method is
reachable by anyone who can address that listener, including the `$sigx:*`
symbols the public mount deliberately refuses. `cluster()` throws at
construction without one unless `__DEV__`, where it serves open and warns
once. Pass `secret: null` to opt out deliberately — for a mount genuinely
unreachable by an untrusted caller (an mTLS-terminated mesh, a private
network) — so the choice shows up in the diff rather than in a missing line.
An empty string throws in every build: `process.env.CLUSTER_SECRET ?? ''`
reads as configured while authenticating with an empty key.

| route | question | on failure |
|---|---|---|
| `GET /_sigx/health` | **liveness** — is this process worth keeping? | restart it |
| `GET /_sigx/health/ready` | **readiness** — should it be receiving traffic? | drain it |

**They must be allowed to disagree, and the drain is why.** A graceful
`host.stop()` announces `leaving` *before* activations hand off, so for the
whole handoff window the host is `200 live` and `503 not-ready`: take it out
of the load balancer, do **not** restart it — a restart would abort the
handoff that makes rolling deploys drop zero calls.

The cluster check fails for three states, each with a `detail` you can read
off the probe body:

- **`leaving`** — draining (post-`beginStop`). Not-ready, still live.
- **`fenced`** — this host lost its membership heartbeat and now refuses
  every activation. Its *published* status still reads `active`, so without
  this check a balancer would happily keep feeding a black hole. Fenced is
  also **fatal** (below): the fence is permanent for this host identity, so
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
routes only run on a started host, so serving at all *is* the liveness
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
import { metrics, health, ops } from '@sigx/actors/host';

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
one remote surface that did exist — the `$sigx:host#stats` symbol — is
cluster-only, carries no latency distributions, and disappears entirely in a
cluster of socket-only transports. A single-node host was unobservable from
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

`ops()` lives in `@sigx/actors/host` and `clusterStats` in
`@sigx/actors/cluster`, so a single-node host must not pay for the cluster
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
must: this is the endpoint you reach for when the host is *already* unwell,
and it must not be able to hang. A throwing provider is caught and its section
replaced with `{ error }`, leaving every other section intact — the one tool
that explains a broken host must not be broken by it. Names must be unique; a
clash throws at setup naming both plugins.

`ops().snapshot()` gives the same answer in process, for a test or for a tool
embedded in the host rather than polling it.

### Cluster-wide stats

`clusterStats()` fans out across the membership view and returns one report:

```ts
import { clusterStats } from '@sigx/actors/cluster';

const report = await clusterStats(plugin.placement, { timeoutMs: 2000 });
// {
//   view:    { version: 12, size: 3, active: 3 },
//   hosts:   [{ hostId, address, status, stats: { activations, queued, perType },
//               counters, reminderShards: ['p3','p7',…], uptimeMs,
//               metrics, health }],
//   totals:  { hosts, activations, queued, perType, counters, metrics, health },
//   reminderShards: { p0: ['s.ab12'], p1: ['s.cd34'], … },
//   unreachable: [{ hostId, address, reason: 'unreachable', message }],
//   partial: false
// }
```

**Behaviour is cluster-wide too, not just topology.** Each report carries a
mergeable metrics *digest* and that host's readiness, so one call answers for
the whole fleet — one reachable endpoint, one secret, and it works behind an
ingress where the peers are not individually reachable.

`totals.metrics` therefore has real cluster numbers: calls, failures,
streams, `errors.byKind`, storage operations, activation churn, per-type and
per-method call counts — and **latency merged properly**. A
`HistogramSnapshot` is p50/p90/p99 with no buckets, and the average of two
hosts' p99s is not the p99 of anything, so the digest carries the bucket
counts instead and the percentiles are re-derived from the summed
distribution. The buckets are sparse (a few dozen of 384 are ever occupied),
and a peer whose bucket LAYOUT differs has its counters merged and its
distribution dropped rather than silently mixed into a different axis.

```ts
report.totals.metrics?.hosts;          // how many hosts actually reported
report.totals.metrics?.turnMs?.p99Ms;  // from merged buckets, not an average
report.totals.health;                  // { ready, notReady, fatal, unknown }
```

**`totals.metrics.hosts` is the denominator, and it matters.** A host with no
`metrics()` attached — or one mid-rolling-deploy on a build that predates the
digest — contributes nothing, and totals that quietly cover two thirds of the
fleet look exactly like totals that cover all of it. It is `null`, rather than
a wall of zeroes, when nothing anywhere is instrumented: no instrumentation
and no traffic are very different findings.

**Drill-down is opt-in.** `detail` adds each host's live actor list and recent
failures, which the ordinary poll deliberately omits: the walk is
O(activations) on every host at once, and actor keys are the one field here
that can be personal data.

```ts
// Everything, for one host — the shape a dashboard drill-down wants.
await clusterStats(plugin.placement, { detail: { activations: 20, hosts: [id] } });
```

Over HTTP that is `GET /_sigx/ops/cluster?detail=1&activations=20&host=<id>`,
behind the same bearer as the rest of `ops()`. Requested limits are clamped by
the *responder*, not trusted from the caller: the wire is HMAC-guarded, but
that proves who is asking, not that `activations: 1e9` is a reasonable thing
to ask a host with millions of actors.

Adding all of this kept `HostReport.v` at `1`. Every field is optional, so a
peer on an older build simply answers without them — where a version bump
would have made a new collector classify every not-yet-deployed peer as
`unsupported`, blanking the report during exactly the deploy it exists to
explain.

It travels as a reserved symbol (`$sigx:host#stats`) on the **existing**
internal mount, so it inherits the per-request HMAC, the envelope, the codec
and the body cap — there is no second, unauthenticated way to read your
topology. It needs `secret` configured, like every other host-to-host call.

**It never throws because a peer is sick.** A host that times out, refuses
the secret, or predates this build lands in `unreachable` with a `reason`,
and `partial: true` marks the totals as a lower bound. A report you cannot
get during an incident is worthless. Peers are queried with bounded
concurrency (default 16), so a 100-host fan-out is waves rather than 100
simultaneous connections, and the collector answers for itself in process.

`reminderShards` maps each of the 16 shards to the hosts *claiming* it, built
from the reports rather than recomputed centrally — so **two** claimants means
views have diverged (safe: the per-shard etag CAS keeps reminders
at-most-once) and an **empty** list means nothing is ticking that shard. The
number of distinct hosts across that map is also how many hosts do reminder
work at all, which is otherwise invisible.

### Cluster counters

`placement.counters()` is the per-host, pull-based routing view — the same
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
| `directoryEvictions`, `hostSweeps`, `sweptEntries` | failover actually happening. Zero sweeps after a crash means dead entries are only being reclaimed lazily |
| `membershipChanges`, `membershipVersion` | store load. One join notifies every member, so this is the counter that makes membership cost visible |
| `selfFences` | this host was a black hole. Anything above zero needs investigating |
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
the read dispatches in-process through the host (guards and all), resolves
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

### Live reads (`{ live: true }`)

Everything above refreshes only the tab that wrote. Invalidation is local
bookkeeping, and no request/response call tells a *second* browser that
anything happened. `{ live: true }` is what closes that gap:

```ts
const messages = useActorState(RoomActor, room, 'recent', 20, { live: true });
const topic = useActorState(RoomActor, room, 'topic', { live: true });
```

The actor re-runs **the read you declared** after every turn that mutated
its state — whoever caused it — and pushes the result. That is why the feed
is per subscription rather than a state snapshot: `topic` is a method, and
only the actor can compute it.

What it costs and what it guarantees:

- **One connection for the whole page.** Every live read on the page rides a
  single held-open NDJSON response (`$live#subscribe`), multiplexed by
  subscription index, pinging every 30 s so proxies and mobile NATs leave it
  alone. Twelve live components do not open twelve connections.
- **The first paint is unchanged.** The ordinary read still seeds the cell,
  SSR still serializes it, and hydration still costs no request. `live` is
  additive.
- **A set change reopens the connection.** A `fetch` POST body is not duplex,
  so a newly mounted component cannot be pushed onto an open stream: the
  channel coalesces set changes (~20 ms), aborts, and reopens carrying the
  new set. Every subscription re-seeds on open, which is why a reconnect
  needs no resume token.
- **An unchanged value is dropped, not delivered.** Two things produce one
  routinely: the re-seed above (one widget mounting must not look like the
  whole page updating), and the fact that a mutating turn re-runs *every*
  subscription on that actor — change a room's topic and its `recent(20)`
  watch re-runs too, returning an identical list. These are views of current
  state, not an event log, so a subscriber cannot need to know that the value
  it already holds was recomputed.
- **It reconnects.** A long-lived response dies for reasons that are nobody's
  bug (a proxy timeout, a rolling restart, a laptop lid). Backoff doubles
  1 s → 30 s with jitter and resets on any healthy frame, and the re-seed
  doubles as the catch-up read.
- **A dead feed degrades to "not live", never to "broken".** One
  subscription's failure (a guard rejection, say) is delivered to that read
  alone and leaves the rest of the page live; a read whose feed cannot be
  established keeps working as a plain read.
- **Nothing subscribes during SSR.** The subscription lives in `onMounted`.
- **Security is the read's own.** A subscription runs the same guard chain as
  a unary call, at subscribe time, so it exposes nothing a polling client
  could not already read. There is deliberately no per-actor `live` opt-in.
- **The subscription array is capped**, at 256 by default. The mount fans out
  one watch per entry, all at once, and each can force a distinct activation
  that then sits pinned for `idleAfterMs` — and the guards above run *inside*
  that fan-out, so they do not bound it. Uncapped, one body-cap-sized request
  is the mount's largest amplification. An over-cap array is a `400` for the
  whole request rather than a per-index `e` frame: answering per index would
  mean doing the work first. Tune with
  `handleActorRequest({ maxLiveSubscriptions })` (also on `createFetchHandler`
  and `createAppHandler`); `0` disables the cap, which no public listener
  should do.

Cross-host works without configuration: each subscription dispatches through
placement, so watching an actor another host owns rides the host-to-host
transport. `$live` is never routed or redirected — one response fans out to
many actors, so no single actor can claim it.

Tuning, if you need it, is on the plugin — `actorsPlugin({ live: {
debounceMs, retryMs, maxRetryMs, onError } })`. A transport that brings its
own `live()` channel (a WebSocket transport, say) is used instead of this
one, with no call site changing.

## Dev & HMR

The vite plugin's dev host lives in the SSR module runner's graph and is
reachable through the `__SIGX_ACTOR_HOST__` seam. Editing a `*.actor.ts`
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
`handleServerFnRequest` with a host-backed resolver. `configureActors()`
(from `@sigx/actors/client`) points remote/native clients at another base,
independently of `configureServerFn`.

A method with a `reads:` declaration (below) also answers
`GET {base}/r/{token}/{Type}%23{method}?args=[key,...args]`, with the same
codec and the same envelope.

**The callable surface is the method table's OWN keys.** A name that is not
an own, callable key of `methods:` (or `streams:`) is a `404
method-not-found`, and inherited `Object.prototype` members — `toString`,
`constructor`, `valueOf`, `__proto__` — are therefore not callable. Declaring
a method that shadows one of those names is fine; it is an own key. What does
*not* work is a `methods:` factory returning a **class instance**, whose
methods live on a prototype: return an object literal (`__DEV__` warns if you
do not).

### The routing token

`{token}` is a stable per-actor routing hint, mirrored into the
`x-sigx-actor-route` header. It exists because the actor **key** rides in
the JSON body, and no load balancer will parse a body to route — so without
it the edge cannot tell which actor a request is for, and locality decays as
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
in the header would route the two carriers to different hosts. It also keeps
the header value safe ASCII for keys that are not. Hash-mode tokens are
`[0-9a-z]{7}`, so encoding is a no-op there; it matters for `'key'` mode and
custom functions. `actorRouteToken()` decodes either carrier back to the
real value.

```nginx
map $uri $actor { ~^/_sigx/actor/r/([^/]+)/ $1; }
upstream hosts { hash $actor consistent; server ...; }
# or, equivalently:  hash $http_x_sigx_actor_route consistent;
```

```
HAProxy   balance uri depth 4
Envoy     hash_policy: { header: { header_name: x-sigx-actor-route } }
```

**Pair it with `preferLocalPolicy()` — the token alone changes nothing.**
The composition is what produces locality: the LB hashes the token to host
X, the first call activates the actor *there*, and every later call for that
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
sets, so they disagree on most keys and guarantee a hop for every actor.
Deterministic placement does not help here — caller affinity does.

Two limits worth knowing:

- **`$live` carries no token** (nor does any `$`-reserved symbol). One
  held-open response fans out to *many* actors, so there is no single token
  and no single owner; sharding that connection would defeat the reason it
  exists. Live subscriptions still dispatch correctly through placement —
  they just keep paying the hop.
- **A migrated actor does not follow the LB.** `preferLocalPolicy()` applies
  to *new* activations, and the directory keeps a live actor where it is. So
  after a scale-out, already-hot actors stay misrouted until they deactivate.

The internal host-to-host mount carries no token: the caller has already
resolved the exact owner.

Renaming an actor's `type` or its methods is a **wire break** (and `type`
is also the storage identity).

The runtime reserves two symbol shapes on top of that, which is why
`defineActor` refuses a `type` starting with `$` or `@`:

| symbol | what it is |
|---|---|
| `$live#subscribe` | the public multiplexed subscribe mount — many live reads on one held-open NDJSON response |
| `$watch:{Type}#{method}` | INTERNAL, host-to-host only: the same read as `{Type}#{method}`, but opened as a subscription |

The second exists because a watch is an ordinary read dispatched in watch
mode, so the receiving host cannot tell which is meant from the method
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

`live()` is the one **optional** member. Leave it out and `@sigx/actors/app`
drives the `$live` mount over your `stream()` instead, which is how the
default transport gets live reads without carrying a line of push logic in
`./client` (that entry's bytes ride every bundle that touches an actor).
Implement it — as a WebSocket transport would — and the app uses yours,
unchanged call sites either way.

**Server-side**, a transport is a plugin: `PluginRegistry.route()` lets one
contribute its own mount, which `createAppHandler` / `createFetchHandler`
serve in dev and prod alike. (One current limit: `ActorRoute.handle` returns
a `Response`, which cannot express a Node WebSocket upgrade — that needs the
raw socket. Workers can express it.)

That limit applies to this **client-facing** transport. A *host-to-host*
transport is not bound by it, because it is free to bring its own listener
instead of a route — `httpTransport()`, the default, does contribute a route
like any other plugin, but a socket transport does not have to. See
[the host-to-host transport seam](#the-host-to-host-transport-is-pluggable).

### Routing is pluggable too

`ActorTransport` decides *how* a call travels; `ActorRouter` decides *where*
it goes. They sit side by side and compose, so a router and a custom
transport are one wrapper apart:

```ts
import {
    configureActors, fetchTransport, learningRouter, routedTransport
} from '@sigx/actors/client';

configureActors(routedTransport(fetchTransport({ endpoint }), learningRouter()));
```

| router | how it decides | good for |
|---|---|---|
| *(none — the default)* | always the configured endpoint | one origin, browsers |
| `learningRouter()` | caches what redirects teach it | any caller that can reach hosts directly |
| `staticRouter(url)` | always `url` | pinning over the build-time endpoint |
| `chainRouters(a, b)` | first non-null answer wins | composing a precise router with a learning one |
| yours | a service mesh, an existing sharding scheme, tenant affinity | whatever the deployment already does |

```ts
interface ActorRouter {
    readonly name: string;
    resolve(ref: ActorRef, ctx: ActorRouteContext): string | null;
    learn?(ref: ActorRef, endpoint: string): void;
    invalidate?(endpoint?: string): void;
    stats?(): ActorRouterStats;
    close?(): void;
}
```

`resolve` is **sync** on purpose: it runs on every call, and a router that
needs to await something should answer from cache and fill in the background
rather than put a round trip in front of every dispatch. Returning `null`
means "no opinion" and falls back to the configured endpoint.

**A router can never fail a call.** Every callback is wrapped: `resolve`
throwing falls back to the default, and `learn` or `invalidate` throwing is
ignored, because a cache that breaks while *remembering* an answer must not
destroy the answer. That is not politeness — it is what makes it safe to put
arbitrary user code on the dispatch path, and it holds because routing is an
optimization: a wrong endpoint costs a hop, never a wrong answer, since the
receiving host still forwards or redirects and the directory remains the
sole arbiter of who owns an actor.

Two limits worth knowing:

- **`$live` is never routed.** One held-open response fans out to many
  actors, so there is no single owner to pick; the router is bypassed and
  the server fans out through placement as usual.
- **A stream is routed on its FIRST pull only.** A redirect there re-routes
  cleanly; past the first value the endpoint is settled, and a later failure
  is a stream failure rather than a routing question.

`routedTransport(...).stats()` reports `routed` / `redirects` /
`invalidations` / `routerErrors` — `redirects` trending to ~0 as a learning
router warms is the signal that it is working.

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

That context carries the per-app state with a *lifetime*: the mounted-read
registry invalidation refreshes, and the live connection `{ live: true }`
subscribes on (closed with the app, so an unmounted page leaves nothing
open). Both are per app because a server rendering two requests at once has
two of them.

It deliberately does **not** register codec type handlers: the actor wire
reads core's `__SIGX_SERVERFN_CODEC__` seam directly, so one
`serverPlugin({ types })` already covers both wires.

## Clustering (multi-host)

One host per host, many hosts, one actor system — add the `cluster()`
plugin:

```ts
import { defineActorApp } from '@sigx/actors/host';
import { cluster } from '@sigx/actors/cluster';
import { redisCluster } from '@sigx/actors-redis';

export const app = defineActorApp({ actors, storage }).use(
    cluster({
        providers: redisCluster({ url: process.env.REDIS_URL }), // membership + directory
        advertise: 'http://10.0.4.7:7311',   // this host's peer-reachable origin
        secret: process.env.HOST_SECRET      // declared ONCE
    })
);
```

The plugin contributes the internal host-to-host mount as a route, so
`secret` and `internalBase` are stated once instead of being repeated at
the endpoint, and an adapter that mounts `app.routes` picks up
host-to-host traffic automatically:

```ts
import { createServer } from 'node:http';
import { createAppHandler, attachSignalHandlers } from '@sigx/actors/node';

// ONE handler for both mounts — public endpoint and internal route.
const handler = createAppHandler(app);

// Once shutdown begins, every response says `connection: close`, so client
// pools retire each socket after the response it is already receiving.
let stopping = false;
const server = createServer((req, res) => {
    if (stopping) res.setHeader('connection', 'close');
    handler(req, res);
});

// Listen BEFORE starting: `app.start()` joins membership, and from that
// moment peers may place actors here and call them. Bind first and there
// is no window where this host is routable but nothing is listening.
await new Promise<void>((resolve) => server.listen(7311, resolve));
const host = await app.start();

attachSignalHandlers(host, { server, onStopBegin: () => (stopping = true) });
```

**Pass the `server`, not just the host.** Stopping the actors is only half a
graceful shutdown. An orchestrator's preStop sleep and readiness-503 steer
*new* connections away, but connections already established survive endpoint
removal (conntrack) and ride into the exiting pod, where they are reset when
the process exits. On a real cluster that measured as 122 lost calls out of
~1.7M on a rolling restart — all connection-level, none of them visible from
the actor layer, which reported a clean hand-off.

The sequence is deliberately *not* the obvious one:

1. `onStopBegin()` — start answering `connection: close`. This is what
   actually drains the pools, one response at a time, interrupting nothing.
2. `host.stop()` — the actor drain. Pooled connections keep flowing; peers
   whose dials are refused see `unreachable`, which is retryable by design.
3. `server.close()` + `closeAllConnections()` — **last**.

Closing the listener first looks more decisive and is worse: on Node ≥ 19
`close()` also destroys idle connections, and "idle" from the server's side
includes a socket the client is at that instant writing its next request
onto. That produces exactly the reset the sequence exists to prevent.

On other runtimes, route `app.routes` yourself — each is a
`{ match(request), handle(request, host) }` pair. The lower-level
`clusterPlacement` / `handleHostRequest` / `matchesHostRequest` remain
exported for hand-rolled mounts.

#### Redirect on a miss, instead of proxying

When a call arrives at a host that does not own the actor, the mount
**proxies** it: one client round trip, one internal hop, every time. With
the routing token in front of an LB that hop should be rare — but for
callers that can reach hosts directly, it can be removed entirely.

```ts
handleActorRequest(request, { host, onMiss: 'redirect' });   // or 'auto'
```

| `onMiss` | behaviour |
|---|---|
| `'proxy'` *(default)* | forward and answer. Correct for browsers. |
| `'redirect'` | answer `421` naming the owner; the client retries there. |
| `'auto'` | redirect callers that advertise they can follow, proxy the rest. |

It goes on the **mount**, not the host, so one cluster can serve a browser
origin that proxies and an internal origin that redirects.

The client opts in, and remembers:

```ts
import { routedFetchTransport } from '@sigx/actors/client';

configureActors(routedFetchTransport({ endpoint, follow: true }));
```

Routing is opt-in **by import**: `configureActors({ endpoint })` on its own
ships none of it, which matters because the client entry rides every bundle
that touches an actor and a single-origin browser app cannot use redirects
at all.

Remembering is the point. Without it a redirect costs *two* client round
trips versus one trip plus an internal hop — strictly worse than proxying.
With it, only the first call to an actor pays: 2 requests once, then 1
forever, straight to the owner. A learned endpoint is dropped as soon as it
proves wrong (a connection failure or a 5xx), falling back to the configured
endpoint rather than stranding the actor.

**The owner must publish where clients can reach it:**

```ts
cluster({
    advertise: 'http://10.0.4.7:7311',        // INTERNAL, peer-to-peer
    publicAddress: 'https://host-3.example.com' // what a client can reach
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
  why the single-origin deployment (all hosts behind one public origin, the
  LB hashing the routing token) is usually the better answer there.
- **`$live` is never redirected.** One held-open response fans out to many
  actors, so there is no single owner to point at.
- **421 is safe to retry.** The endpoint resolves the owner *before*
  dispatching, so a redirect has provably run no code — which matters
  because actor calls are not idempotent and a 421 is a status user agents
  may retry on their own.

Redirect chains are bounded at both ends (default 2 hops): the client cap is
the useful one, the server cap is what makes a loop impossible even against
a client that ignores its own.

#### The host-to-host transport is pluggable

How hosts reach *each other* is a seam. `httpTransport()` is the default and
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
membership descriptor under `addresses[name]` — so a host can speak more
than one. A **list is a fallback chain**, tried in order:

```ts
transport: [tcpTransport({ port: 11111 }), httpTransport()]
```

That is the rolling-deploy story, and the reason `addresses` exists: while
the deploy is half-done, hosts that already advertise `tcp` talk over it and
the rest are still reached over HTTP, with no window in which any peer is
unreachable. A descriptor with no `addresses` at all — a host from a build
that predates the field — reads as HTTP-only, which is the safe direction.

A **single** transport is strict: a peer that advertises no address for it is
unreachable, loudly, rather than silently falling back. That is deliberate —
a silent fallback means you deploy a transport, benchmark it, and measure the
old one without ever knowing. Fallbacks that *do* happen are counted
(`counters().transportFallbacks`), reported (`HostReport.transports`), and
dev-warned once per peer.

Because the internal mount is now just a route a transport declares, a
cluster configured with only socket transports has **no internal HTTP
endpoint at all** — a smaller attack surface, and nothing to `curl`. The
public actor wire is unaffected either way.

#### Bound the connection pool on Node

Node's global `fetch` is undici with an **unbounded** pool and
`pipelining: 1`, which means one connection per in-flight request — measured
at *two* per in-flight request against a host (`benchmarks/BASELINES.md`,
Tier 2). At concurrency 64 across 99 peers that projects to ~12 600 sockets
per host, which is file descriptors, kernel buffers and a connection burst
every time a peer restarts.

The fix is four lines through the `fetch` seam:

```ts
import { Agent, fetch as undiciFetch } from 'undici';
import { cluster, httpTransport } from '@sigx/actors/cluster';

// One pool per host process, bounded per peer origin. Size it to your
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

#### Which placement policy should you use?

Measured, not argued — `pnpm bench:run locality-warm`, N=100, 240 actors, in
the **warm** steady state (actors already placed, which is where a running
cluster spends its life):

| edge × policy | local fraction | ownership spread |
|---|---:|---:|
| round-robin × `randomPlacementPolicy()` *(default)* | 0.02 | 2.92 |
| round-robin × `preferLocalPolicy()` | **0.00** | 1.25 |
| **hash the routing token × `preferLocalPolicy()`** | **1.00** | 2.50 |
| skewed LB × `randomPlacementPolicy()` | 0.00 | 2.92 |
| skewed LB × `preferLocalPolicy()` | 0.80 | **80.4** |

*Ownership spread is the most-loaded host's actor count over the mean: 1.0 is
perfectly even, and N means one host owns everything.*

**Use `preferLocalPolicy()` if — and only if — your edge hashes the routing
token.** That is the one row that wins, and it wins completely. Set it
per-type with `defineActor({ placement })` or cluster-wide with
`cluster({ policy })`, and configure the LB as shown in *Wire protocol*.

**The default stays `randomPlacementPolicy()`**, for the two rows that
explain why:

- Under a plain round-robin balancer, `preferLocalPolicy()` buys **nothing** —
  0.00 versus random's 0.02, both of them noise around 1/N. It pins each actor wherever its first call
  landed, and the balancer then sends the next call somewhere else anyway.
  A default that only helps once you have also configured your load
  balancer is not a default; it is a trap with a good outcome attached.
- Under a balancer that is *not* even — a rolling deploy, a bad health
  check, a scaled-down pool — it concentrates ownership catastrophically:
  **80×** at N=100, one host holding 80% of the actors. And they do not move
  back, because placement only applies to *new* activations. Random holds
  ~2.9 regardless of what the edge does, which is the property you want
  precisely when things are going wrong.

`consistentHashPolicy()` is a third option and mostly an anti-pattern under
edge hashing (see *Wire protocol*): the edge's hash and the cluster's
rendezvous hash are different functions over different sets, so they
disagree on most keys. It is worth having when a client routes with the
cluster's own rule rather than the LB's.

**`activationCountPolicy()` steers new activations toward the least-loaded
host** — the answer when the workload is uneven (hot types, lumpy keys) and
ownership spread is the number you are watching. It keeps a load view
refreshed out of band over the authenticated host-to-host ops channel
(`refreshMs`, default 5 s; a peer probe that misses its `timeoutMs` keeps
its stale entry), and `choose()` stays sync: it samples **two** random
active hosts and takes the less loaded — power-of-two-choices — plus a
local pending delta so a burst inside one refresh window sees its own
effect. Un-attached it keeps no state and IS `randomPlacementPolicy()`
behaviorally; attached but not yet refreshed it spreads random-or-better
until data lands. A host with no known load reads as cold, which is what
makes a freshly joined host attract work immediately.
Staleness is the design, not a defect: routing is an optimization, and a
decision made on old numbers costs a little balance, never correctness.

Stateful policies ride the `attach` seam: a policy may declare
`attach(runtime)` and the placement calls it at start (or on first
resolution, for a `defineActor({ placement })` declaration) with
`PolicyRuntime` — `{ hostId, view(), selfLoad(), peerLoad(target,
timeoutMs) }` — returning a teardown run at stop. A throwing `attach` is
contained; `choose()` must keep working un-attached.

Precedence, highest first: `defineActor({ placement })` →
`cluster({ typePolicies })` → `cluster({ policy })` → random.

#### Rebalancing

Placement only ever decides where a **new** activation goes; once a
workload has gone lumpy — a skewed LB, a scale-up that left old hosts
full, `preferLocalPolicy()` under the wrong edge — the actors do not move
back on their own. Rebalancing is the correction, and it is **off unless
configured**:

```ts
cluster({
    providers, advertise,
    policy: activationCountPolicy(),          // where shed actors land
    rebalance: { intervalMs: 60_000 }         // { threshold, maxMoves, minIdleMs, timeoutMs }
})
```

Each host runs `placement.rebalance()` on the cadence: probe peer loads
over the ops channel, and if this host is over `threshold × mean`
(default 1.2), `migrate()` a bounded batch (`maxMoves`, default 10) of
its **idlest** activations — skipping anything kept alive by a stream,
watch or task, anything with queued turns, and anything active within
`minIdleMs` (default 60 s). A migrated actor's claim is released and it
re-activates wherever placement puts it on its next call, state intact —
pair the loop with `activationCountPolicy()` and shed actors land on the
cold hosts.

The properties that make it safe to leave on:

- **A host sheds its own actors only, down to the mean, never past it** —
  the receivers' own rounds handle the rest, so the correction is slow,
  decentralized, and cannot oscillate (a two-host cluster will not trade
  one actor back and forth: rounds also require `own - mean ≥ 1`).
- **It never acts on missing data.** Unreachable peers are excluded from
  the mean, and a round with no answering peer does nothing — a
  partitioned host must not dump its actors on nobody.
- **One round is total and observable**: `rebalance()` reports
  `{ own, peers, mean, moved, reason? }` rather than throwing, ops
  tooling can invoke it directly, and the `rebalanceRounds` /
  `rebalanceMigrations` counters surface in `counters()` and
  `clusterStats()`.

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
an N-host cluster over your wire and every case runs against it. The rule it
enforces throughout is *assert on the error `kind`, never on an HTTP status*,
which is what makes the contract expressible off HTTP at all.

It is currently **contributor-facing only**: reachable inside this workspace
as `@sigx/actors/cluster/testing` through a tsconfig/vitest alias, but not in
the published `exports` map, so an out-of-repo package cannot import it yet.
Promoting the subpath is a deliberate step for whenever a transport ships
outside this repo.

How it works, in one paragraph: every activation writes a **claim** into a
distributed directory (create-if-absent; released on deactivation), so a
key activates on exactly one host; calls for actors placed elsewhere are
forwarded over the internal endpoint with the full call context (chain,
call id, deadline as *remaining* ms — clock-skew-proof) so deadlock
detection and timeouts work across hosts; a misdirected call answers
**421 wrong-host** with the owner and the caller re-routes (bounded, never
proxied); membership is TTL-heartbeat liveness in the shared store; the
reminder table is split into 16 hash shards, each ticked by exactly one
host via rendezvous hashing over the view (the per-shard etag CAS keeps
firing at-most-once even if views transiently diverge). Under all of it,
the storage etag CAS remains the integrity floor — a briefly-stale route
costs a rejected save and a fault-and-reload, never corrupted state.

Guards still run once, at the public edge — host-to-host hops are
intra-system, authenticated per request with an HMAC signature derived
from the shared `secret` (bound to the call, freshness-windowed; run
mTLS/VPC between hosts — transport encryption is deliberately out of
scope). Streams cross
hops with cancellation and keep-alive release intact. Placement is
pluggable per cluster and per type (`policy`, `typePolicies`:
`consistentHashPolicy()`, `preferLocalPolicy()`, uniform random by
default), a graceful `host.stop()` hands actors off (`'migrated'`
deactivations, claims released as they drain, callers retry through
routing — rolling deploys drop zero calls), and
`placement.migrate(ref)` moves one actor explicitly. For tests,
`memoryClusterHub()` gives an N-host in-process cluster with no external
store.

To see inside a running cluster — readiness that drains a `leaving` or
fenced host, `clusterStats()` across the view, and the routing/directory
counters — see [Health & readiness](#health--readiness) above.

## Entry points

| Entry | Contents |
|---|---|
| `@sigx/actors` | `defineActor`, `actor`, `useActor`, `actorKey`, errors, types — isomorphic, light |
| `@sigx/actors/host` | `defineActorApp`, `createHost`, `memoryStorage`, `metrics()`, `health()`, `ops()`, storage/placement/plugin seams — server-only |
| `@sigx/actors/server` | `handleActorRequest`, `matchesActorRequest`, `createActorResolver` — WinterCG-clean |
| `@sigx/actors/node` | `createAppHandler` (all mounts), `createActorHandler`, `attachSignalHandlers`, `fileStorage` |
| `@sigx/actors/client` | `__actorRef`, `configureActors`, `fetchTransport`, the `ActorTransport` seam — the build-swap target |
| `@sigx/actors/app` | `actorsPlugin()`, `useActorState`, `useActorAction` — the sigx app integration (the only entry that imports `@sigx/runtime-core`) |
| `@sigx/actors/job` | `defineJob` — durable long-running operations: state machine, progress, checkpoint/pause/resume, `watch()` (convention over `defineActor` + `tasks:`) |
| `@sigx/actors/cluster` | `cluster()` plugin, `clusterPlacement`, `clusterStats`, `handleHostRequest`, `memoryClusterHub`, provider seams — WinterCG-clean |
| `@sigx/actors/vite` | `sigxActors()`, `extractActors` |
| `@sigx/actors/vite-client` | ambient types for `virtual:sigx-actors` (types only) |

## Design notes & deliberate limits (v1)

- **One host per process; many processes via `./cluster`.** The
  `ActorDispatcher`/`ActorPlacement` seams remain the extension point for
  other distributed backends (Cloudflare Durable Objects map naturally);
  every call already flows through them, and dev-mode `devSerializeChecks`
  verifies your arguments would survive a remote hop.
- **String keys**; POST by default, with `GET` for methods that declare
  `reads:` (no form posts / 303 PRG yet); no WebSocket/SSE push layer —
  NDJSON covers server→client, per call (`streams:`) and multiplexed per page
  (`useActorState(…, { live: true })`).
- Reserved names: actor types starting with `$sigx:`, topic names starting
  with `$` or `@`, and the method names `$sigx:reminder` and `$sigx:topic`.
  `$sigx:host#stats` is the cluster's ops channel and is answered before any
  definition lookup, so an actor type named `$sigx:host` would simply be
  uncallable across hosts. The public endpoint refuses every
  `$sigx:`-prefixed method outright — those are the runtime's own deliveries,
  and they arrive over the authenticated internal mount only.
- **Topic delivery is best-effort, at-most-once.** No persistence, no retry,
  no replay — a durable mode is an explicit non-goal for v1 and has API room
  reserved (`PublishOptions`). Explicit runtime subscribe/unsubscribe is
  likewise deferred; `ctx.topics.*` stays free for it.
