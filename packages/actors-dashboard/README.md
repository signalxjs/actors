# @sigx/actors-dashboard

The web dashboard for [`@sigx/actors`](https://sigx.dev/actors): hosts, actors,
latency, errors and cluster topology, as embeddable
[sigx](https://sigx.dev) components. The five tabs `sigx actors top` has, in a
browser.

```sh
pnpm add @sigx/actors-dashboard @sigx/actors-monitor
```

```tsx
import { ActorsDashboard } from '@sigx/actors-dashboard';
import { httpSource } from '@sigx/actors-monitor';

<ActorsDashboard source={httpSource({ url: '/admin/ops' })} />
```

Every panel is also exported on its own — `OverviewPanel`, `HostsPanel`,
`HostPanel`, `ActorsPanel`, `ClusterPanel`, `HealthPanel` — so a portal can
embed one table instead of the whole shell. `mountActorsDashboard(el, opts)`
renders it into a page that is not a sigx app.

Styling is self-contained and themed by `--sigx-actors-*` custom properties:
override the tokens on any ancestor and the whole dashboard follows.

## Two things to get right

**Never put the ops secret in the browser.** `/admin/ops` above is a route of
**your** app, not the host's. `ops()` sets no CORS headers and refuses to
construct without a bearer token outside dev — it reports your actor type
names, traffic shape and cluster topology, and actor keys are user data. Point
`httpSource` at a same-origin route you own, which authenticates the operator
however your app already does and forwards to `ops()` with the bearer attached
server-side.

**A counter going backwards is a gap, not a rate.** That rule, and the rest of
what makes these numbers honest, lives in
[`@sigx/actors-monitor`](https://www.npmjs.com/package/@sigx/actors-monitor) —
this package renders its verdicts and re-derives none of them, which is what
keeps it from disagreeing with the CLI.

Peer dependencies: `@sigx/actors` (≥ 0.9), plus `@sigx/runtime-core`,
`@sigx/runtime-dom` and `@sigx/reactivity` (all ≥ 0.15).

npm ≥ 7 and pnpm ≥ 8 install peers automatically, so the install line above is
usually all you need — but that is a package-manager default, not a
guarantee, and Yarn Classic and anyone running `auto-install-peers=false` will
not get them. If the app fails to start with `Cannot find package
'@sigx/actors'`, install it explicitly:

```sh
pnpm add @sigx/actors
```

It is needed because the host drill-down decodes that host's own latency
histogram with core's percentile walk rather than reimplementing it —
duplicating that walk is how a cluster-wide p99 quietly stops matching the
per-host one. `@sigx/actors` is WinterCG-clean and zero-dependency, so it
tree-shakes to the walk itself.

Requires **Node ^20.19.0 || >=22.12.0** to build, and any modern browser to
run.

## Documentation

**https://sigx.dev/actors/**

Source, examples and architecture notes:
https://github.com/signalxjs/actors

MIT © Andreas Ekdahl
