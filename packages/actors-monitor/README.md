# @sigx/actors-monitor

The renderer-free data layer every [`@sigx/actors`](https://sigx.dev/actors)
dashboard is built on: poll a host's `ops()` endpoint, normalise the embedded
and HTTP shapes, derive rates from cumulative counters, and say what is wrong.

```sh
pnpm add @sigx/actors-monitor
```

```ts
import { DashboardState, httpSource } from '@sigx/actors-monitor';

const state = new DashboardState({ source: httpSource({ url: '/ops' }) });
state.start();
// state.view.snapshot / .error / .partial — a signal your UI reads
```

Browser-safe: no renderer, no `node:` import, no DOM. `@sigx/actors` is an
**optional** peer (types only), so HTTP mode works with no actor runtime
installed. `@sigx/reactivity` (≥ 0.15.3) is the one runtime peer.

**One rule matters more than the rest.** A counter going backwards is a
`reset()` or a host restart, and it must produce a **gap** — not a negative
rate, and not an enormous positive one from treating the new total as the
delta. Getting it wrong draws a spike, not an error, which is why this is
implemented once here rather than in each dashboard.

Two renderers already consume it:
[`@sigx/actors-cli`](https://www.npmjs.com/package/@sigx/actors-cli) draws it
in a terminal, and
[`@sigx/actors-dashboard`](https://www.npmjs.com/package/@sigx/actors-dashboard)
draws it in a browser.

Requires **Node ^20.19.0 || >=22.12.0**, or any modern browser.

## Documentation

**https://sigx.dev/actors/**

The ops endpoint it polls: https://sigx.dev/actors/docs/ops-endpoint/

Source, examples and architecture notes:
https://github.com/signalxjs/actors

MIT © Andreas Ekdahl
