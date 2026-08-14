# SignalX Actors

> **Virtual actors** for [SignalX](https://github.com/signalxjs):
> addressable, single-threaded, persistent server objects riding sigx's
> serverFn wire protocol. Lazy activation, turn-based concurrency, pluggable
> storage with optimistic concurrency, timers & durable reminders, NDJSON
> streams, and a Vite plugin that swaps actor modules for typed client stubs.

```ts
export const CartActor = defineActor({
    type: 'Cart',
    authorize: [requireUser],
    state: () => ({ items: [] as Item[] }),
    methods: (ctx) => ({
        async addItem(item: Item) {
            ctx.state.items.push(item); // single-threaded — no races
            await ctx.save();           // persisted via ActorStorage
            return ctx.state.items.length;
        }
    })
});

// browser, serverFn, or SSR — the same expression:
await actor(CartActor, cartId).addItem(item);
```

## Documentation

**https://sigx.dev/actors** — guides, API reference, clustering and deployment.
This repository holds the source, the runnable examples, and the internal
architecture notes in [`docs/`](docs).

## Packages

| Package | Description |
|---|---|
| [`@sigx/actors`](packages/actors) | The actor runtime, wire layer, and Vite plugin (ten runtime entries — `.`, `./host`, `./server`, `./node`, `./client`, `./app`, `./job`, `./cluster`, `./cluster/frames`, `./vite` — plus types-only `./vite-client`) |
| [`@sigx/actors-redis`](packages/actors-redis) | Redis providers — cluster membership, the actor directory, and etag-CAS actor storage |
| [`@sigx/actors-pg`](packages/actors-pg) | Postgres providers — etag-CAS actor storage, cluster membership, and the actor directory |
| [`@sigx/actors-surreal`](packages/actors-surreal) | SurrealDB 3 providers — etag-CAS actor storage, cluster membership, the actor directory, and durable reminders |
| [`@sigx/actors-k8s`](packages/actors-k8s) | Kubernetes membership provider — host liveness via coordination Leases, no extra store |
| [`@sigx/actors-tcp`](packages/actors-tcp) | Framed TCP transport — one multiplexed connection per peer |
| [`@sigx/actors-ws`](packages/actors-ws) | Client-facing WebSocket transport — browsers calling actors over one multiplexed socket |
| [`@sigx/actors-cloudflare`](packages/actors-cloudflare) | Cloudflare Durable Objects backend — run an actor app on Workers, one DO per actor |
| [`@sigx/actors-monitor`](packages/actors-monitor) | Renderer-free dashboard data layer — ops polling, rate derivation across resets, alerts and shard states |
| [`@sigx/actors-cli`](packages/actors-cli) | `sigx` CLI plugin — observe hosts, actors and clusters from the terminal |
| [`@sigx/actors-dashboard`](packages/actors-dashboard) | The web dashboard — the same five tabs as embeddable sigx components |
| [`@sigx/actors-otel`](packages/actors-otel) | Observability exporters — Prometheus text exposition, OpenTelemetry traces and a metrics bridge |
| [`benchmarks`](benchmarks) | Performance baselines — throughput, latency, heap footprint, leak detection. Run locally, and A/B'd on perf-sensitive PRs: timings inform, metrics marked `exact` gate (not published) |

Four runnable examples, in the order they are worth reading:

[`examples/counter`](examples/counter) — the runtime with **no framework at
all**: plain DOM, one file per idea. The build-time client swap, a `watch`
stream, file persistence — plus a 3-host cluster demo and a durable job that
survives its host being killed, both with **no infrastructure to install**.

```sh
pnpm install && pnpm build
pnpm --filter counter-example dev       # single-host dev server
pnpm --filter counter-example cluster   # 3-host cluster demo over real HTTP
```

[`examples/chat`](examples/chat) — actors inside a **real SignalX app**: SSR
that seeds `useActorState` into the document, one auth policy enforced on
both transports, serverFns beside the actor endpoint, topics feeding a
cross-room projection, and `{ live: true }` reads that keep every open tab
current over one connection. Open it twice and type in one.

```sh
pnpm --filter chat-example dev                                    # Vite dev host
pnpm --filter chat-example build && pnpm --filter chat-example start
```

[`examples/cf-workers`](examples/cf-workers) — the **Cloudflare** deployment:
one Durable Object per actor, a Worker that hosts nothing and routes
everything, and a `verify.mjs` that checks exact counts, per-key isolation,
streaming through the edge and a reminder firing from a real alarm. Shows the
app **factory** shape Workers needs — a module-scope app binds whichever
object constructs it first.

[`examples/dashboard`](examples/dashboard) — the **web dashboard** against a
real three-host cluster. Its subject is the ~60-line ops proxy: `ops()` sets
no CORS headers and refuses to construct without a bearer secret, so a
browser dashboard goes through a same-origin route of your own app that
attaches the token **server-side**. The browser half is nine lines with no
secret in it.

```sh
pnpm --filter counter-example cluster:serve   # terminal 1
pnpm --filter dashboard-example dev           # terminal 2 → http://localhost:5490
```

### Performance & deployment rig

[`perf/`](perf) is not an example — it is how the repo measures itself.
[`perf/aks`](perf/aks) is the **Kubernetes** scale-out test: N identical
host pods, cluster state in Redis, an in-cluster load generator, a Helm
chart, and a one-command driver
([`deploy/testenv.mjs`](perf/aks/deploy/testenv.mjs)) for the full
perf/failure runbook. Deployment identity is required environment — the
rig names nobody's infrastructure.

### Watching it happen

`cluster:serve` runs the same three hosts but keeps them up under steady
traffic, so there is something live to point the dashboard at:

```sh
pnpm build
pnpm --filter counter-example cluster:serve        # terminal 1

# terminal 2 — in the terminal…
pnpm --filter counter-example exec sigx actors top \
    --url http://127.0.0.1:5392 --secret demo-ops-secret
# …or in a browser, on http://localhost:5490
pnpm --filter dashboard-example dev
```

**5392, not 5391**: the demo kills the first host on its way past, to show
the survivors re-forming and reclaiming its reminder shards. Any surviving
host is enough — the fan-out reaches the rest.

The demo deliberately produces something worth looking at: a hot actor
that builds queue depth, a spread of cold ones, a host left `leaving`
after the drain step, and one call in seven aimed at a method that does
not exist — so the latency split, the shard map and the error panel are
populated rather than a screen of zeroes.

## Development

```sh
pnpm install
pnpm build       # build the package (dev + prod dists + d.ts)
pnpm test        # vitest (unit + wire integration + type contracts)
pnpm typecheck   # tsgo
pnpm lint        # oxlint
pnpm size        # size-limit bundle gates
```

This repo follows the sigx standard engineering setup
([`signalxjs/repo-template`](https://github.com/signalxjs/repo-template)):
issue → worktree (`pnpm wt new <N-slug>`) → PR → Copilot review →
squash-merge, with `main` protected. Agents: read [`AGENTS.md`](AGENTS.md).
Contributors: see [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Status

v0.3 — pre-1.0, and the public API may still move between minors.

Clustering has landed: `@sigx/actors/cluster` plus membership, directory and
storage providers for Redis, Postgres, SurrealDB and Kubernetes, framed TCP and
WebSocket transports, and Cloudflare Durable Objects as a whole-app backend. The
dispatch and placement seams that made those possible are the same ones a
future backend slots into — see
[Design notes](https://sigx.dev/actors/docs/design-notes/), and
[`docs/architecture`](docs/architecture) for how the seams fit together.

Read [`SECURITY.md`](SECURITY.md) before the first production deploy — the
trust boundaries and the limits you have to set yourself are stated there.

## License

MIT © Andreas Ekdahl
