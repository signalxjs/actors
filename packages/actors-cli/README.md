# @sigx/actors-cli

A [`sigx` CLI](https://sigx.dev/cli/) plugin for observing `@sigx/actors`:
silos, grains, latency, errors and cluster topology, from the terminal.

```sh
pnpm add -D @sigx/actors-cli
```

That is the whole install. The `sigx` binary discovers plugins from your
dependencies, so the commands appear as soon as the package is there and
your project depends on `@sigx/actors`.

```sh
sigx actors                                        # the dashboard
sigx actors top --url http://silo:3000             # …against a running silo
sigx actors stats --json | jq '.cluster.totals'    # a snapshot, for piping
sigx actors health --url http://silo:3000          # exit code by readiness
```

## Two ways to reach a silo

**Embedded** (the default) loads your project's actor app module in-process
and reads it directly. Zero config, no secret, and it sees strictly more
than the wire can — full latency histograms, the whole activation list, the
cluster placement itself.

It also **starts a silo**. That silo is real: it joins membership, claims
actors and ticks reminders. Fine pointed at a dev app, wrong pointed at
production.

**HTTP** (`--url`) polls a running silo's `ops()` endpoint. It loads no user
code and holds no silo, so it is the mode for anything you did not just
start yourself.

```ts
// The silo you want to watch needs ops() mounted:
import { metrics, health, ops } from '@sigx/actors/silo';

export const collect = metrics();
export const app = defineActorApp({ actors })
    .use(collect)
    .use(health())
    .use(ops({ secret: process.env.OPS_SECRET }));
```

Pass the token as `--secret`, or set `SIGX_OPS_SECRET` so it stays out of
your shell history.

### What embedded mode needs from your app module

`defineActorApp` does not expose its plugins — they are contributions, not
members — so embedded mode reads them from named exports. Everything is
optional; an app that exports none of them still monitors, it just shows
fewer panels.

| Export | Gives you |
|---|---|
| `app` (or `default`) | **required** — the `defineActorApp` result |
| `metrics` | calls, latency, the queue/turn split, error kinds |
| `ops` or `health` | the readiness aggregate |
| `cluster` | the whole-cluster fan-out |

```ts
// src/actors.app.ts
export const metrics = actorMetrics();
export const cluster = actorCluster({ providers, secret });
export const app = defineActorApp({ storage }).use(metrics).use(cluster);
```

`--app <path>` names the module; without it, `src/actors.app.ts` and a few
neighbours are tried.

## Commands

| | |
|---|---|
| `sigx actors top` | The dashboard. The default, so bare `sigx actors` opens it. |
| `sigx actors stats` | One snapshot: cluster totals, per-silo state, latency, error kinds, slowest methods, hottest grains. `--json` for the raw shape. |
| `sigx actors health` | Liveness and readiness, **with the exit code as the answer**. |

### The dashboard

Five tabs — Overview, Silos, Grains, Cluster, Health — hosted by `runShell`
from `@sigx/cli/shell`, so the chrome, the palette and the teardown match
every other sigx tool.

| key | |
|---|---|
| `1`–`9` | switch tab |
| `j` / `k` | move the cursor in a table |
| `p` | pause polling |
| `r` | refresh now |
| `+` / `-` | slower / faster polling |
| `/reset` | clear the sparkline history |
| `Ctrl+C` | quit |

`--interval <ms>` sets the poll rate (default 1000, clamped to 200…60000).

Piped or in CI it degrades rather than hanging: one poll, one line, exit.

**A failed poll does not blank the screen.** The last good snapshot stays
up, labelled as such, with the status line showing `poll FAILING` and how
stale the numbers are — a dashboard that goes blank the moment a silo
hiccups destroys exactly the context you need to understand the hiccup.

### Exit codes for `health`

| | |
|---|---|
| `0` | ready |
| `1` | reachable but **not ready** — drain it, do not restart it |
| `2` | could not reach it at all |

`1` and `2` are deliberately distinct. A silo answering "not ready" is alive
and handing off its activations; a silo answering nothing is a different
incident with a different fix, and collapsing them is how a rolling deploy
becomes an outage.

## What the output is trying to tell you

- **`PARTIAL`** leads the output, never trails it. A member did not answer,
  so every total below is a lower bound — and they still look plausible,
  which is exactly why the caveat cannot be a footnote.
- **`queue` against `turn`.** They are the two halves of a call's latency
  and mean opposite things: high turn is a slow method, high queue is a hot
  grain. Printed adjacently because the comparison is the diagnosis.
- **An unclaimed reminder shard** means nothing is ticking it — those
  reminders are simply not firing, and nothing else surfaces that. Two
  claimants is safe (the per-shard etag CAS keeps delivery at-most-once) but
  means views have diverged.
- **`fenced`** is the status worth knowing. A fenced silo refuses every
  activation while its *published* status still says `active`, so a load
  balancer keeps feeding it.

## `@sigx/actors-cli/source`

The data layer is exported separately and imports nothing terminal-related:
`MonitorSource`, `MonitorSnapshot`, `httpSource`, `embeddedSource`, plus the
rate derivation (`RateTracker`, `Series`) and formatting.

It exists as a subpath because the awkward parts are worth sharing with any
other renderer. Core reports cumulative counters with no windowing, splits a
cross-silo call across two silos on purpose, and marks an incomplete fan-out
rather than failing it — every consumer needs the same handling of all
three, including the rule that a counter going **backwards** is a
`metrics().reset()` or a restart and must produce a *gap* rather than a
negative or an enormous positive rate.

## Licence

MIT
