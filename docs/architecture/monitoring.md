# Monitoring: one data layer, many renderers

How a dashboard over `@sigx/actors` is put together, and which of its rules a
new renderer is not allowed to re-decide.

## The layering

```
   ops() on the host                    embeddedSource (Node only)
   ─────────────────                    ──────────────────────────
   GET /_sigx/ops                       dynamic import() of the app
   GET /_sigx/ops/cluster               module — STARTS A REAL HOST
          │                                        │
          └────────────► MonitorSource ◄───────────┘
                              │
                  @sigx/actors-monitor
                  ────────────────────
                  MonitorSnapshot   normalized, whichever source produced it
                  DashboardState    the poll loop, back-pressure, last-good
                  RateTracker       cumulative counters → rates, with GAPS
                  alertLines        what is wrong, worst first
                  scopeOf / …       what a number is ABOUT
                  shardStates       claimed / unclaimed / split
                              │
                ┌─────────────┴─────────────┐
                ▼                           ▼
      @sigx/actors-cli            @sigx/actors-dashboard
      terminal, @sigx/terminal    browser, pure sigx
```

**Nothing in `@sigx/actors-monitor` may import a renderer**, touch a DOM, or
import `node:` anything. Its one runtime peer is `@sigx/reactivity` — for the
signal `DashboardState` publishes its view through, which both renderers
observe because both re-export the same one (`@sigx/terminal` and `sigx` are
each `export * from '@sigx/reactivity'`).

`@sigx/actors` is an **optional, types-only** peer. That is load-bearing: HTTP
mode has to work in a project that does not depend on the actor runtime at all
(#116), and the ops payload is restated structurally in `http.ts` for the same
reason — a version skew between the tool and the host it watches should be a
missing field, not a build error.

## Why the layer exists at all

Not to save code. Every rule below fails **silently** when a second
implementation gets it wrong: none of them throws, and each one renders as a
plausible number.

**A counter going backwards is a gap, not a rate.** Core reports monotonic
totals since start with no windowing, so a rate has to come from diffing two
snapshots. When the second reading is *lower*, `metrics().reset()` ran, or the
host restarted, or a peer dropped out of the fan-out — and in every case the
previous total is meaningless. The answer is `null`. Subtracting anyway gives
a negative rate; treating the new value as the delta gives an enormous
positive one. Both draw as traffic that never happened.

**`partial` makes every total a lower bound.** A fan-out where one member did
not answer still returns numbers, and they look exactly like complete ones.
The snapshot carries `partial: true` all the way to the UI rather than
smoothing it over, and a renderer must say so — a dashboard that silently
under-reports during an outage is worse than one that admits it.

**A `null` digest is "said nothing", not "did nothing".** An uninstrumented
host and an idle one are different findings. Rendering the first as zeroes
claims the second. Same rule for an empty histogram: three zeroed bars assert
"we measured, and it was fast".

**A number needs its scope attached.** `scopeOf`, `polledLabel` and
`coverageNote` exist because the failure behind #121 was never a wrong number
— it was a right one under no label, sitting directly beneath one of a
different scope. Cluster-wide calls and this host's calls are both correct and
are not the same fact.

**Three shard states, three meanings.** One claimant is healthy. *None* means
nothing is ticking that shard, those reminders are not firing, and nothing
else in the system surfaces it. *Two or more* means membership views have
diverged — safe, because the per-shard etag CAS keeps delivery at-most-once,
but worth knowing. A renderer that collapses this to a claimant count loses
the distinction that matters.

## Adding a renderer

1. Take a `MonitorSource`, or a `DashboardState` built over one. Do not poll
   `ops()` yourself: the back-pressure (abandon the in-flight request rather
   than queue behind a slow host) and the keep-the-last-good-snapshot rule are
   not obvious and are already written.
2. Use `alertLines`, `scopeOf`, `polledLabel`, `coverageNote`, `shardStates`
   and `percentilePoints` as given. Map their severities into your own
   vocabulary — `@sigx/actors-cli/src/tui/bars.ts` is the worked example, and
   it is about ten lines.
3. Assert against `packages/actors-monitor/__tests__/fixture.ts`. Both
   renderers share it deliberately: the claim this layering makes is that a
   terminal and a browser over one data layer cannot disagree about what the
   cluster is doing, and two fixtures could not detect it if they did.
4. Anything you find yourself deriving that is about *actors* rather than
   about *drawing* belongs in the monitor, not in your renderer. That is how
   `alertLines` and `shardStates` got there (#239) — they were private
   functions inside the terminal screens until a second renderer needed them.

## Reaching `ops()` from a browser

`ops()` sets **no CORS headers** and refuses to construct without a bearer
secret outside `__DEV__`. Both are deliberate: the endpoint reports your actor
type names, traffic shape and cluster topology, and actor keys are user data.

So a browser dashboard does **not** call it directly — that would mean
shipping the secret to the client. It calls a **same-origin route of your own
app**, which authenticates the operator however your app already does and
forwards to `ops()` with the bearer attached server-side. `httpSource({ url:
'/your/route' })` with no `secret` is the browser half; `examples/dashboard`
is a working proxy.
