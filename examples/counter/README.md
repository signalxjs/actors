# counter example

The actor runtime with **no framework at all** — plain DOM, one file per
idea. Start here: everything below is `@sigx/actors` itself, with nothing
else in the way.

```sh
pnpm install && pnpm build
pnpm --filter counter-example dev
```

```
  ➜  Local:   http://localhost:5173/
```

Click the button. Then add `?counter=other` to the URL — a different key is
a different actor, with its own state, activated on first call. Reload: the
count is still there, because `fileStorage` wrote it to `.actors/`.

## What to look at when you open it

**The client swap** (`src/main.ts`). This line runs in the browser:

```ts
const client = actor(Counter, name);
await client.increment(1);
```

It is the same expression the server runs. `sigxActors()` swapped the actor
module for a typed client ref at build time, so in the browser each call
becomes a POST to `/_sigx/actor` — with the method names and argument types
still checked against the real actor.

**The stream is the whole live-update story** (`src/counter.actor.ts`):

```ts
streams: (ctx) => ({
    async *watch() {
        yield* ctx.changes({ initial: true });   // snapshot, then one per change
    }
})
```

The page consumes it with a `for await` and nothing polls. Open two tabs and
click in one.

**One app module, two runtimes** (`src/actors.app.ts`). The dev server gets
it through `sigxActors({ app })` and `server.mjs` imports it directly, so
storage and plugins are declared once. Note what is *not* in it: the actor
registry. Vite supplies the one it already builds, which is what keeps the
module loadable under plain Node.

**Shutdown is two halves** (`server.mjs`):

```ts
attachSignalHandlers(host, { server, onStopBegin: () => (stopping = true) });
```

Draining the actors is only one of them. Passing `server` alone closes the
listener at the end, without giving keep-alive clients a chance to retire
their pooled sockets first — which is what `onStopBegin` is for.

**Non-goals**: no SSR, no components, no auth, no styling. For those, see
`examples/chat`.

## Three hosts, one actor system

```sh
pnpm --filter counter-example cluster
```

No Redis, no Docker, nothing to install — `memoryClusterHub()` and a shared
`memoryStorage()` *are* the cluster. The three hosts talk over real
localhost HTTP sockets, so the wire is genuine even though the coordination
is in-process.

Six steps, each one an assertion that throws if it comes out wrong:

**1 — placement spreads.** Nine counters created through host 0 alone:

```
activations per host: s.juat5j00=5  s.jdd5n6di=3  s.qtd1r1k5=1
```

**2 — one activation, whoever you ask.** The same key hammered through all
three hosts at once:

```
6 concurrent increments via 3 hosts → [ 1, 2, 3, 4, 5, 6 ]
'cart' has exactly one owner: s.jdd5n6di (:5392)
```

No duplicates and no lost updates, because there is exactly one activation
and it takes one turn at a time.

**3 — a stream from a host that does not own the actor.**

```
  watch (via s.juat5j00): count=6
  watch (via s.juat5j00): count=16
  watch (via s.juat5j00): count=116
```

**4 — crash failover.** The owner is killed outright:

```
survivor s.juat5j00 serves 'cart' → { count: 116, lastVisit: … }
directory re-claimed by: s.qtd1r1k5
(state came back from shared storage — nothing was lost)
```

**5 — the ops surface**, including the distinction that matters during a
deploy:

```
GET /_sigx/health       -> 200 {"status":"live"}
GET /_sigx/health/ready -> 503 {"status":"not-ready", … "leaving — draining, take out of rotation"}
(live 200 but ready 503 — drain it, do NOT restart it)
```

**6 — graceful shutdown**, then `CLUSTER DEMO COMPLETE`.

### Watching it live

```sh
pnpm --filter counter-example cluster:serve                  # terminal 1

pnpm --filter counter-example exec sigx actors top \
    --url http://127.0.0.1:5392 --secret demo-ops-secret     # terminal 2
```

**Any surviving host is enough** — the fan-out reaches the rest — but *which*
one survives changes between runs. Step 4 kills the **owner of the `cart`
actor**, and placement decides who that is:

```js
step(`4. Crash failover — killing the owner ${entry.hostId}`);
```

`--serve` keeps that outcome, so one of the three ports will refuse
connections and it is not always the same one. If the CLI cannot connect, read
the `killing the owner` line in the demo's output, or just try another port.

For the same cluster in a browser, `examples/dashboard` renders it — including
the proxy that keeps this `--secret` off the client.

`--serve` skips the shutdown and keeps the cluster up under steady traffic.
The traffic is shaped on purpose — a hot actor that builds queue depth, a
spread of cold ones, a host left `leaving` after the drain, and one call in
seven aimed at a method that does not exist — so the latency split, the shard
map and the error panel have something in them instead of a screen of zeroes.

Run the CLI **from this package**: the `sigx` CLI discovers its plugins from
the dependencies of the project it runs in, and `@sigx/actors-cli` is a
devDependency here.

## A job that outlives the host running it

```sh
pnpm --filter counter-example job
```

`defineJob` (`src/crunch.job.ts`, 38 lines) is a state machine, idempotent
start, cancel, progress, checkpointing and crash-resume — the shape any real
import, sync or AI-workflow job has. The demo starts one, then kills the host
running it, mid-run:

```
=== 3. Kill the owner s.nmmunyta MID-RUN ===
owner is gone; the run was interrupted between checkpoints

=== 4. A survivor call re-places the actor — the task ledger resumes the run ===
status via s.i6jen6at: status=running attempts=2 (crash-resume counted)

result: { sum: 66, attempts: 2 }
final: status=completed attempts=2 — resumed from the last checkpoint
```

`attempts=2` is the crash. `sum: 66` is the proof that nothing past the last
checkpoint was lost — and the client-visible timeline never breaks, because
the same key keeps answering `status()` and `watch()` before, during and
after.

## Detached work, and a reminder that outlives the process

```sh
pnpm --filter counter-example runtime-demo
```

Two primitives no other example runs, on one host and no infrastructure.

**A task runs detached** (`src/importer.actor.ts`). A method call holds the
activation until it settles — right for milliseconds, wrong for an import. A
`tasks:` body runs outside any turn, so ordinary reads keep answering:

```
begin(10):  {"started":true,"total":10}
  status while running: phase=running done=1/10 running=[run]
  status while running: phase=running done=4/10 running=[run]
  status while running: phase=running done=8/10 running=[run]
(every one of those answered mid-import — a method call could not have)
```

The trade is that a task body gets no `ctx.state` and no `ctx.save()`. Every
mutation goes through `ctx.turn(fn)`, which enqueues `fn` as one ordinary
serialized turn — so the writes stay single-threaded and everything
downstream of a turn keeps working. `defineJob` is built on exactly this,
and adds the durable ledger, attempts and `watch()` on top.

Cancelling is a *request*, not a join: it aborts and returns, and the run
winds down through its own final turn.

```
stopped at done=4/50 phase=stopped
```

**A durable reminder survives the process** (`src/reminder.actor.ts`). This
is what separates `ctx.reminders` from `ctx.timer`: a timer is volatile and
dies with the activation, while a reminder is stored through `ActorStorage`,
fired by the *host's* scheduler, and **re-activates an actor that is no
longer there**.

The demo arms one, stops the host outright, starts a new host on the same
storage, and then deliberately does nothing:

```
=== 3. Arm a durable reminder, then kill the host that armed it ===
armed at 2026-08-05T18:05:27.905Z, due in 1500ms
pending: [wake]
host stopped — the activation is gone, and so is its scheduler

=== 4. A NEW host, same storage — the reminder fires with nobody calling ===
making NO calls to the actor — waiting 5s in silence
fired=1  lastFiredAt=2026-08-05T18:05:29.464Z
```

The silence is the point. Polling `status()` would itself activate the
actor, and then "the reminder woke it" and "I woke it and the reminder
happened to be due" would be indistinguishable — so the demo touches it
zero times and reads once at the end.

Two limits worth knowing: a repeating `period` has a **60s floor** (a
durable reminder is a storage write per tick; use `ctx.timer` for tighter
cadences), and delivery is coarse — "at or after", at-most-once per tick.
The demo drops `reminderTickMs` from its 30s default to 250ms so you are not
watching paint dry.

## Things that will bite you

**Run `pnpm build` first.** The example resolves `@sigx/actors` from the
built `dist/` through the workspace link, so a fresh checkout that skips it
fails at import.

**Node >= 22.18.** Both demos import `.ts` actor modules directly, relying on
native type stripping.

**Ports 5391-5393 and 5394-5396** for the cluster and job demos. Override
with `CLUSTER_DEMO_PORTS` / `JOB_DEMO_PORTS`.

**`memoryStorage()` is the demos' whole database.** It lives in the parent
process, which is what lets a killed host's state come back — and means all
of it is gone when the demo exits. `fileStorage` is what `dev` and `start`
use.

**`.actors/` must be excluded from the Vite watcher** (`vite.config.ts`).
A save is a temp-file plus rename, and the HMR path loses that race; actor
state changing is not a source edit anyway.

## Files

| File | |
|---|---|
| `src/counter.actor.ts` | the actor: state, `ctx.save()`, and a `watch` stream over `ctx.changes()` |
| `src/actors.app.ts` | the app — storage and plugins, shared by dev and prod |
| `src/main.ts` | the browser: the build-swapped `actor()` call and the stream loop |
| `src/crunch.job.ts` | `defineJob` — progress, checkpoint, resume |
| `src/importer.actor.ts` | `ctx.tasks` — detached work, mutating through `ctx.turn` |
| `src/reminder.actor.ts` | `ctx.reminders` — durable, and re-activates the actor |
| `src/static.ts` | resolve a request target inside `dist/`, or refuse |
| `server.mjs` | production entry: the app handler, then static, then a graceful drain |
| `cluster-demo.mjs` | three hosts over real sockets — spread, single activation, cross-host stream, failover, ops |
| `job-demo.mjs` | the same three hosts, one job, one deliberate crash |
| `runtime-demo.mjs` | one host: a detached task, then a reminder that survives a restart |
| `vite.config.ts` | `sigxActors({ app })` — the dev host and the client swap |
| `index.html` / `package.json` / `tsconfig.json` | the rest |
