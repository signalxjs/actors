# @sigx/actors-cli

A [`sigx` CLI](https://sigx.dev/cli/) plugin for observing `@sigx/actors`:
hosts, actors, latency, errors and cluster topology, from the terminal.

```sh
pnpm add -D @sigx/actors-cli
```

The `sigx` binary discovers plugins from the **dependencies of the project
you run it in** — it reads `./package.json`, looks for a `sigx-cli` field in
each dependency's manifest, and calls the plugin's `detect(cwd)`. So two
things have to be true in that directory:

1. `@sigx/actors-cli` is in its `dependencies` or `devDependencies`;
2. it depends on `@sigx/actors` — that is what `detect` looks for.

If either is missing the commands simply are not there:

```
$ sigx actors top
error: Unknown command 'actors'
```

That is not a broken install — it is the CLI correctly declining to offer
actor commands in a project that has no actors. `sigx --help` lists what it
did find. Run it from the package that owns the host, or in a monorepo:

```sh
pnpm --filter my-app exec sigx actors top
```

```sh
sigx actors                                        # the dashboard
sigx actors top --url http://host:3000             # …against a running host
sigx actors stats --json | jq '.cluster.totals'    # a snapshot, for piping
sigx actors health --url http://host:3000          # exit code by readiness
```

## Two ways to reach a host

**Embedded** (the default) loads your project's actor app module in-process
and reads it directly. Zero config, no secret, and it sees strictly more
than the wire can — full latency histograms, the whole activation list, the
cluster placement itself.

It also **starts a host**. That host is real: it joins membership, claims
actors and ticks reminders. Fine pointed at a dev app, wrong pointed at
production.

**HTTP** (`--url`) polls a running host's `ops()` endpoint. It loads no user
code and holds no host, so it is the mode for anything you did not just
start yourself.

```ts
// The host you want to watch needs ops() mounted:
import { metrics, health, ops } from '@sigx/actors/host';

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
| `sigx actors stats` | One snapshot: cluster totals, per-host state, latency, error kinds, slowest methods, hottest actors. `--json` for the raw shape. |
| `sigx actors health` | Liveness and readiness, **with the exit code as the answer**. |

### The dashboard

Five tabs — Overview, Hosts, Actors, Cluster, Health — hosted by `runShell`
from `@sigx/cli/shell`, so the chrome, the palette and the teardown match
every other sigx tool.

| key | |
|---|---|
| `1`–`9` | switch tab |
| `j` / `k` | move the cursor in a table |
| `enter` | open the selected host |
| `h` | back to the host list |
| `p` | pause polling |
| `r` | refresh now |
| `+` / `-` | slower / faster polling |
| `/reset` | clear the sparkline history |
| `Ctrl+C` | quit |

**Every panel says what its numbers are about.** Cluster-wide totals are
headed `cluster · N host(s)`; anything that is only the polled host's says
so. When some hosts report no metrics, the coverage is stated — totals
covering two thirds of a fleet look exactly like totals covering all of it.

`enter` on the Hosts tab opens that host in full: its calls, latency,
readiness, checks, error kinds and the actors living on it. That detail is
requested only while the panel is open, because a detail poll makes every
host walk its activation table.

`--interval <ms>` sets the poll rate (default 1000, clamped to 200…60000).

Piped or in CI it degrades rather than hanging: one poll, one line, exit.

**Every screen fits the pane it is given.** Tables window to the rows that
are left rather than running off the bottom, columns shrink from the right
so an identity is never the thing that gets truncated, and banners wrap
instead of being cut mid-sentence. `j`/`k` move the cursor on the tab you
are actually looking at, and the table scrolls to follow it.

Requires `@sigx/cli` >= 0.9, which is where a tab learns its pane size and
which tab is on screen (signalxjs/cli#88); the components come from
`@sigx/terminal` >= 0.11 (signalxjs/terminal#103).

**A failed poll does not blank the screen.** The last good snapshot stays
up, labelled as such, with the status line showing `poll FAILING` and how
stale the numbers are — a dashboard that goes blank the moment a host
hiccups destroys exactly the context you need to understand the hiccup.

### Exit codes for `health`

| | |
|---|---|
| `0` | ready |
| `1` | reachable but **not ready** — drain it, do not restart it |
| `2` | could not reach it at all |

`1` and `2` are deliberately distinct. A host answering "not ready" is alive
and handing off its activations; a host answering nothing is a different
incident with a different fix, and collapsing them is how a rolling deploy
becomes an outage.

## What the output is trying to tell you

- **`PARTIAL`** leads the output, never trails it. A member did not answer,
  so every total below is a lower bound — and they still look plausible,
  which is exactly why the caveat cannot be a footnote.
- **`queue` against `turn`.** They are the two halves of a call's latency
  and mean opposite things: high turn is a slow method, high queue is a hot
  actor. Printed adjacently because the comparison is the diagnosis.
- **An unclaimed reminder shard** means nothing is ticking it — those
  reminders are simply not firing, and nothing else surfaces that. Two
  claimants is safe (the per-shard etag CAS keeps delivery at-most-once) but
  means views have diverged.
- **`fenced`** is the status worth knowing. A fenced host refuses every
  activation while its *published* status still says `active`, so a load
  balancer keeps feeding it.

## `@sigx/actors-cli/source`

The data layer is exported separately and imports nothing terminal-related:
`MonitorSource`, `MonitorSnapshot`, `httpSource`, `embeddedSource`, plus the
rate derivation (`RateTracker`, `Series`) and formatting.

It exists as a subpath because the awkward parts are worth sharing with any
other renderer. Core reports cumulative counters with no windowing, splits a
cross-host call across two hosts on purpose, and marks an incomplete fan-out
rather than failing it — every consumer needs the same handling of all
three, including the rule that a counter going **backwards** is a
`metrics().reset()` or a restart and must produce a *gap* rather than a
negative or an enormous positive rate.

## Licence

MIT
