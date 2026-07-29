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

## The shape worth copying

`examples/counter` builds its app **at module scope**, which is right for a
process that owns one silo. On Workers it is wrong, and quietly so: storage
and reminders come from a Durable Object's own `ctx.storage`, so they exist
**per object**. A module-scope app binds whichever object constructed it first
and then serves every other object from those seams.

So the app is a factory (`src/actors.app.ts`), and the host hands the seams in:

```ts
export class ActorSilo extends createSiloDurableObject<Env>({
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

## Four things that will bite you

**`new_sqlite_classes`, not `new_classes`.** The latter creates the legacy
key-value backed storage, which **cannot be migrated to SQLite in place** and
carries a far smaller per-value limit. It is the one irreversible line in
`wrangler.jsonc`.

**`__DEV__` must be defined.** The runtime ships a dev and a production dist
and expects the bundler to define its compile-time flag. Without
`"define": { "__DEV__": "false" }` the silo throws `__DEV__ is not defined` on
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

## Eviction is not deactivation

A Durable Object is evicted when idle, and eviction destroys the isolate, the
silo and the activation **together** — so `onDeactivate` never runs. Anything
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
output contract as `examples/aks-cluster/loadgen.mjs`.

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
object's mailbox ceiling. A hot key *is* bounded by that ceiling eventually;
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
| `verify.mjs` | the six checks, plus `MODE=load` |
| `DEPLOY.md` | the runbook, with the failures included |
