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

One silo per host, many hosts, one actor system — plug `clusterPlacement`
into the placement seam and mount the internal silo-to-silo endpoint beside
the public one:

```ts
import { createSilo } from '@sigx/actors/silo';
import { clusterPlacement, handleSiloRequest, matchesSiloRequest } from '@sigx/actors/cluster';
import { redisCluster } from '@sigx/actors-redis';

const placement = clusterPlacement({
    ...redisCluster({ url: process.env.REDIS_URL }), // membership + directory + reminder lease
    advertise: 'http://10.0.4.7:7311',               // this silo's peer-reachable origin
    secret: process.env.SILO_SECRET                  // shared cluster secret
});
const silo = createSilo({ actors, storage, placement });

// On the same listener, before the public mount:
//   matchesSiloRequest(req) ? handleSiloRequest(req, { silo, placement, secret }) : …
```

How it works, in one paragraph: every activation writes a **claim** into a
distributed directory (create-if-absent; released on deactivation), so a
key activates on exactly one silo; calls for actors placed elsewhere are
forwarded over the internal endpoint with the full call context (chain,
call id, deadline as *remaining* ms — clock-skew-proof) so deadlock
detection and timeouts work across hosts; a misdirected call answers
**421 wrong-host** with the owner and the caller re-routes (bounded, never
proxied); membership is TTL-heartbeat liveness in the shared store; the
reminder table gets exactly one ticker via a leader lease. Under all of it,
the storage etag CAS remains the integrity floor — a briefly-stale route
costs a rejected save and a fault-and-reload, never corrupted state.

Guards still run once, at the public edge — silo-to-silo hops are
intra-system, authenticated by the shared `secret` (run mTLS/VPC between
hosts; transport encryption is deliberately out of scope). Streams cross
hops with cancellation and keep-alive release intact. For tests,
`memoryClusterHub()` gives an N-silo in-process cluster with no external
store.

## Entry points

| Entry | Contents |
|---|---|
| `@sigx/actors` | `defineActor`, `actor`, `useActor`, errors, types — isomorphic, light |
| `@sigx/actors/silo` | `createSilo`, `memoryStorage`, storage/placement seams — server-only |
| `@sigx/actors/server` | `handleActorRequest`, `matchesActorRequest`, `createActorResolver` — WinterCG-clean |
| `@sigx/actors/node` | `createActorHandler`, `attachSignalHandlers`, `fileStorage` |
| `@sigx/actors/client` | `__actorRef`, `configureActors` — the build-swap target |
| `@sigx/actors/cluster` | `clusterPlacement`, `handleSiloRequest`, `memoryClusterHub`, provider seams — WinterCG-clean |
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
