# chat example

Actors inside a **real SignalX app**: an actor read that renders on the
server, ships inside the document, hydrates with no second request — and
then keeps itself current in every open tab.

```sh
pnpm install && pnpm build
pnpm --filter chat-example dev
```

```
chat dev  http://localhost:5273
```

Open it, sign in from the footer, and say something. Then **open a second
tab and type in one of them** — that is the ten-second version of what this
example is for.

## What to look at when you open it

| | |
|---|---|
| **SSR that actually seeds** | `entry-server.tsx` renders `Room`, the actor reads dispatch **in-process**, and their results are serialized into the document. `entry-client.tsx` hydrates and finds them under the same canonical key, so the first paint costs no request. |
| **`{ live: true }`** | The three reads in `Room.tsx` ride **one** held-open connection for the whole page, each re-running server-side after any turn that mutated what it reads — whoever caused it. Without it, a write only ever refreshes the tab that made it. |
| **Topics → projection** | `room.actor.ts` publishes to `room-activity`; ONE singleton `ActivityFeed` (`key: () => 'all'`) folds every room's events; the page observes that projection. Post in `/r/other` and the first tab's "across all rooms" panel moves, though that page never heard of the other room. |
| **One policy, two transports** | See below — it is the lesson worth copying. |
| **`migrateState`** | `room.actor.ts` evolves a stored room between the storage read and activation, lazily, so a deploy adds no write amplification. |
| **Three plugins, one build** | `vite.config.ts`: `sigx()` + `sigxServer()` + `sigxActors()` compose with no coordination. |

**Non-goals.** No router, no styling, no clustering, no deployment. For
clustering, `examples/counter` runs three hosts with **no infrastructure at
all** and shows placement, failover and a stream consumed from a non-owner.
For the edge, see `examples/cf-workers`.

## The shape worth copying

Authentication and authorization look like one job and are three. Splitting
them is what lets an actor declare **nothing** and still be safe on both
transports:

```ts
// server-app.ts — the only place policy lives
export const app = createServerApp<ChatUser>({
    authenticate: (rq) => {            // who is this? null is a valid answer
        const name = currentUser(rq);
        return name === null ? null : { name };
    },
    codec: {                           // so identity can ride the envelope
        encode: (user) => user.name,
        decode: (encoded) => (encoded === '' ? null : { name: encoded })
    }
});
```

That is the whole configuration. The *may they?* half is core's default
`requireAuthenticated`, which every actor and serverFn inherits — the two
deliberately public surfaces opt out with `allowAnonymous: true`, and
grepping for it gives you this example's entire anonymous-reachable surface.
Propagation is not your problem at all: identity rides its own envelope slot,
so `ctx.principal` works inside every actor and across every `ctx.publish`
hop, which is how the activity feed attributes an entry to `ada` without any
payload carrying a user field.

Verify it in one command — the same actor method, with and without a session:

```sh
curl -X POST localhost:5290/_sigx/actor/Room/setTopic -H 'origin: http://localhost:5290' \
     -H 'content-type: application/json' -d '{"args":["general","hijacked"]}'
# {"error":{"message":"Authentication required","status":401}}
```

A guard could not have done the harder version of this. Guards are called
with `(rq, { symbol, name })` — **no key and no arguments** — so *"is this
caller signed in?"* was expressible and *"does this user own THIS room?"* was
not. A policy receives the resolved principal *and* the instance, so the
second question is ordinary:

```ts
authorize: (user, _rq, op) => op.resource!.key.startsWith(`${user.name}:`)
```

## Going multi-host from here

Nothing in the app changes. The app module gains a plugin and swaps its
storage:

```ts
// actors.app.ts — fileStorage is per-process; a second host sees none of it
storage: redisStorage({ url: process.env.REDIS_URL })

// server.mjs
composed = actorApp.use(cluster({
    providers: redisCluster({ url: process.env.REDIS_URL }),
    advertise: `http://${process.env.POD_IP}:${INTERNAL_PORT}`,
    secret: process.env.CLUSTER_SECRET
}));
```

Two things that are not optional once you do this: `cluster()`'s internal
mount runs **no guards at all** by design, so it must not share a listener
with your public port; and `SECURITY.md` is the file to read before the first
deploy, not after.

`examples/counter`'s `cluster-demo.mjs` shows the whole model working with
`memoryClusterHub()` and no Redis at all, which is the faster way to see it.

## Things that will bite you

**`createAppHandler` mounts every plugin route** — health, ops, the
host-to-host endpoint. Never put it on a public port. This example uses
`createActorHandler`, which mounts the actor route and nothing else.

**An unmounted `/_sigx/*` route falls through to the SSR document** and
answers **200 with a page**. So `/_sigx/health` reports healthy on a host
that never mounted health, and a probe believes it. `server.mjs` 404s the
whole reserved prefix for exactly this reason — check yours does too.

**`fileStorage` is per-process.** Two hosts do not see each other's rooms.
It is the right default for an example and wrong for a deployment.

**`AUTH_SECRET` is a boot failure in production, not a default.** Without it
every session would be forgeable, so `session.ts` throws rather than picking
one.

**Importing `@sigx/server` in the SSR entry is load-bearing.** It stamps the
request scope the document handler opens around each render; without it,
anything reading `rq.request` mid-render throws on a detached context.

**`.actors/` must be excluded from the Vite watcher.** A save is a temp-file
plus rename, and the HMR path loses that race — and chat state changing is
not a source edit anyway.

## Production, locally

```sh
pnpm --filter chat-example build
pnpm --filter chat-example start        # http://localhost:5290
```

Port **5290**, not 3000 — that one is contended on every developer's machine.

## Files

| File | |
|---|---|
| `src/server-app.ts` | the auth policy — **the thing to copy** |
| `src/room.actor.ts` | one actor per room: state, `ctx.save()`, `ctx.publish`, `migrateState` |
| `src/activity.actor.ts` | the topic and its singleton subscriber — the projection |
| `src/chat.server.ts` | serverFns beside actors; `postMessage` is the attributed write |
| `src/session.ts` | HMAC-signed HttpOnly cookies, timing-safe verify |
| `src/Room.tsx` | the page: three live reads, two writes, one serverFn read |
| `src/entry-server.tsx` | SSR entry; re-exports the actor app so both runtimes share one config |
| `src/entry-client.tsx` | browser entry; `actor()` here is the build-swapped client ref |
| `src/room-path.ts` | `/r/<name>` → room, shared by both entries |
| `src/static.ts` | resolve a request target inside `dist/client`, or refuse |
| `server.mjs` | the production chain: actors → serverFns → assets → document |
| `dev-server.mjs` | Vite middleware mode; the plugins mount their own endpoints |
| `vite.config.ts` | the three-plugin composition |
| `package.json` / `tsconfig.json` | JSX compiles to sigx's runtime, declared in both |
