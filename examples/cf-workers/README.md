# cf-workers example

A deployable `@sigx/actors` app on Cloudflare Workers: **one Durable Object
per actor**, a Worker at the edge that hosts nothing and routes everything.

```sh
pnpm install && pnpm build          # the example bundles the workspace source
pnpm --filter cf-workers-example dev

# second terminal
TARGET_URL=http://127.0.0.1:8787 pnpm --filter cf-workers-example verify
```

```json
{"mode":"verify","checks":6,"failures":[]}
```

Then open <http://127.0.0.1:8787> — the Worker serves a page as well as the
actors, from `createFetchHandler`'s `fallback`. Type any key to get a different
actor, increment it, and watch the number arrive over a `watch` stream rather
than from the button.

**Nothing on the page polls**, including the reminder count. `ctx.changes()`
emits after any turn that mutated state, and a reminder delivery *is* such a
turn — so an alarm firing pushes down the same stream as a button press. Arm
the reminder, leave the tab open, and watch the count move on its own.

It is one HTML string with an inline script (`src/page.ts`) — no build step and
no client bundle, because its job is to make the model visible, not to be an
app. For the real client story — `actor()` in the browser via the build-time
swap, `useActorState`, SSR — see `examples/chat`, which runs on Node.

## The shape worth copying

`examples/counter` builds its app **at module scope**, which is right for a
process that owns one host. On Workers it is wrong, and quietly so: storage
and reminders come from a Durable Object's own `ctx.storage`, so they exist
**per object**. A module-scope app binds whichever object constructed it first
and then serves every other object from those seams.

So the app is a factory (`src/actors.app.ts`), and the host hands the seams in:

```ts
export class ActorHost extends createHostDurableObject<Env>({
    actors,
    namespace: (env) => env.ACTORS,
    app: createApp          // receives the OBJECT's storage/reminders/defaults
}) {}

export default createWorkerHandler<Env>({
    actors,
    namespace: (env) => env.ACTORS,
    app: createApp,
    fetch: { origin: false }
});
```

Both exports need the actor registry: the Worker to run guards, tell a stream
method from a unary one and be the 404 authority; the object to activate. On
Cloudflare they are the **same bundle**, so it is a plain import.

The factory also does `.use(durableObjectsHosted())`. That plugin installs
nothing — the host claims the placement itself — but it narrows the
`defineActor` exported from `actors.app.ts` so `placement:` on an actor is a
compile error. A cluster policy means nothing on Durable Objects (a ref maps to
its object by name), and the placement throws at dispatch if one reaches it
anyway ([#362](https://github.com/signalxjs/actors/issues/362)).

## The client socket

`socket: {}` on `createWorkerHandler` mounts the Worker-terminated WebSocket
(`workerSocket()`, #157) at `/_sigx/socket`: one connection per page, one
`{i,sub}` frame per live subscription, every call fanned out through
placement to its actor's object. The page's counter rides it — the wire is
hand-written in `src/page.ts` on purpose, so all three frame shapes stay
visible; a real app uses `socketTransport()` from `@sigx/actors-ws` and
writes none of that.

Note the posture split: the HTTP mount here runs `origin: false` so `curl`
and `verify.mjs` work, but the socket keeps the default `'same-origin'` — a
socket upgrade arrives from a browser **with cookies attached and no
preflight**, and only the page dials it.

### The room pattern (object-terminated, #158)

When the shape is one actor and many clients — a chat room, a game lobby, a
shared document — terminate the socket **in the object** instead:

```ts
export class ActorHost extends createHostDurableObject<Env>({
    actors,
    namespace: (env) => env.ACTORS,
    app: createApp,
    socket: {}                              // session options live object-side
}) {}

export default createWorkerHandler<Env>({
    actors,
    namespace: (env) => env.ACTORS,
    socket: { terminate: 'object' }         // forwards {path}/{type}/{key}
});
```

The client dials one socket per actor
(`wss://…/_sigx/socket/Room/room-1`) and the session lives where the actor
lives — so a disconnect releases the room (see the #47 bite below, which
this mode is the answer to), and an idle page hibernates for free. The two
modes share a path prefix and differ by arity, so they compose on one
deployment. What eviction costs: the first message after a cold wake closes
`1012 'session evicted — reconnect'` and the client redials with current
cookies and re-seeds — the same contract as any drop.

## Five things that will bite you

**`new_sqlite_classes`, not `new_classes`.** The latter creates the legacy
key-value backed storage, which **cannot be migrated to SQLite in place** and
carries a far smaller per-value limit. It is the one irreversible line in
`wrangler.jsonc`.

**`__DEV__` must be defined.** The runtime ships a dev and a production dist
and expects the bundler to define its compile-time flag. Without
`"define": { "__DEV__": "false" }` the host throws `__DEV__ is not defined` on
the first request. `pnpm dev` overrides it to `true` so local warnings appear.

**`nodejs_compat` is required.** `@sigx/server` — the serverFn layer the wire
rides on, not the actor runtime itself — imports `node:async_hooks`. Without
the flag the Durable Object fails to boot on real Cloudflare and every call
comes back `unreachable`; `wrangler deploy` warns about it, and the warning is
not cosmetic. Local `wrangler dev` and the pool-based tests enable it
implicitly, so this only shows up on a real deployment.

**`origin: false`, or an explicit origin list.** Workers callers are not
browsers posting a form, and the public mount refuses a request with no
`Origin` by default. A real app with a browser front-end passes its own
origins here instead of switching the check off.

**A departed subscriber releases the actor only on the object-terminated
socket ([#47](https://github.com/signalxjs/actors/issues/47)).** The
Worker-terminated socket and the NDJSON stream change where the *connection*
ends, not where calls run: every subscription still crosses Worker→object
over `stub.fetch`, whose abort signal is swallowed at the boundary — so the
last tab leaving a room leaves the room's `keptAlive` set, resident and
billable. When those economics matter, use the room pattern above
([#158](https://github.com/signalxjs/actors/issues/158)): the session lives
in the object, teardown is local, and the empty room is released.

## Eviction is not deactivation

A Durable Object is evicted when idle, and eviction destroys the isolate, the
host and the activation **together** — so `onDeactivate` never runs. Anything
that must survive has to be written inside the turn (`ctx.save()`), which is
why `Counter` saves on every increment. Nothing already saved is lost;
nothing unsaved is kept, and there is no hook to save it in.

For the same reason there is no idle sweeper: the platform's eviction *is*
idle collection, one level down.

## verify.mjs

Correctness, not a benchmark. Six checks: exact counts under sequential
increments, no lost updates under 20 concurrent ones, K keys behaving as K
independent actors, a cross-actor call landing in the callee's object, a
`watch` stream delivering a frame, and **a reminder firing from a real alarm**
— the only one a plain request cannot stand in for.

One JSON line on stdout, everything else on stderr, so `| jq` works. Same
output contract as the counter example's load generator.

### MODE=load, and what it does *not* tell you

```sh
TARGET_URL=… MODE=load HOT=1 CONCURRENCY=16 DURATION_S=10 …   # one hot key
TARGET_URL=… MODE=load KEYS=32 CONCURRENCY=16 DURATION_S=10 … # spread
```

It is tempting to expect "one hot key saturates, K keys scale out". **Measured,
that is not what either environment shows**, so this example does not claim it.

Local `wrangler dev` (edge, every object and the load generator in one process
on one machine) is not even stable in direction:

| concurrency | 1 hot key | 32 keys |
|---|---|---|
| 8 | 1160 ops/s | 1483 ops/s |
| 32 | 1542 ops/s | 479 ops/s |
| 64 | 176 ops/s | 123 ops/s |

A real deployment, driven from one laptop, shows the hot key slightly *ahead*:

| concurrency | 1 hot key | 32 keys |
|---|---|---|
| 8 | 79 ops/s (p50 89ms) | 47 ops/s (p50 103ms) |
| 32 | 264 ops/s (p50 108ms) | 237 ops/s (p50 123ms) |

Both are dominated by round-trip latency to the edge and by cold-start cost —
spreading across 32 keys means creating 32 objects — and neither gets near one
object's turn-serialization ceiling. A hot key *is* bounded by that ceiling eventually;
showing it needs a load generator near the edge and far more concurrency than
a laptop can usefully drive.

So: `MODE=load` is a tool for measuring **your** deployment and your access
pattern, not a result this example ships. (The same caution the repo applies
to `benchmarks/` — see its "Trusting the numbers" section.)

## Deploying

See **[DEPLOY.md](DEPLOY.md)** — a runbook whose every step was run against a
real account before it was written, including the two failures worth knowing
about. Not automated here on purpose: it creates billable resources.

```sh
pnpm --filter cf-workers-example deploy
TARGET_URL=https://<name>.<subdomain>.workers.dev \
  pnpm --filter cf-workers-example verify
pnpm --filter cf-workers-example exec wrangler delete --force   # teardown
```

SQLite-backed Durable Objects are on the Workers **Free** plan; sustained
alarm testing and repeated deploys will want Workers Paid.

## Files

| File | |
|---|---|
| `src/actors.app.ts` | the app **factory** — the thing to copy |
| `src/counter.actor.ts` | state churn: one storage write per call |
| `src/ticker.actor.ts` | reminder-driven, reschedules from its own handler |
| `src/worker.ts` | the Durable Object class and the edge entry |
| `wrangler.jsonc` | binding, migration, `define`, `nodejs_compat`, `compatibility_date` |
| `src/page.ts` | the page served at `/` — dependency-free |
| `verify.mjs` | the six checks, plus `MODE=load` |
| `DEPLOY.md` | the runbook, with the failures included |
| `src/env.d.ts` | the `Env` bindings the Worker and the object share |
| `tsconfig.json` | its own program — Worker globals cannot join the root one |
| `package.json` | scripts, and wrangler as the only real dependency |
