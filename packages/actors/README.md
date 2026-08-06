# @sigx/actors

**Virtual actors** for SignalX: addressable, single-threaded, persistent server
objects riding the serverFn wire protocol.

```sh
pnpm add @sigx/actors
```

Peer dependencies: **`@sigx/reactivity` 0.15.3 or later** (the host's change
tracking uses an internals seam added in that release), `@sigx/runtime-core`,
`@sigx/serialize` and `@sigx/server` on 0.15, and `vite` 6 or later.

## Documentation

**https://sigx.dev/actors**

| | |
|---|---|
| Overview | https://sigx.dev/actors/docs/overview/ |
| Actors or server functions? | https://sigx.dev/actors/docs/actors-or-server-functions/ |
| Installation | https://sigx.dev/actors/docs/installation/ |
| Your first actor | https://sigx.dev/actors/docs/your-first-actor/ |
| The actor model | https://sigx.dev/actors/docs/actor-model/ |
| Clustering | https://sigx.dev/actors/docs/clustering/ |
| Entry points | https://sigx.dev/actors/docs/entry-points/ |

Backends, transports and tooling —
Redis, Postgres, SurrealDB, Kubernetes, TCP, WebSocket, Cloudflare Durable
Objects, the CLI and the OpenTelemetry exporters — are listed at
[sigx.dev/actors/packages](https://sigx.dev/actors/packages/).

Source, runnable examples and internal architecture notes:
https://github.com/signalxjs/actors

MIT © Andreas Ekdahl
