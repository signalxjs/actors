# Changelog

## [Unreleased]

### Added

- **A cluster to point it at** (#101). `examples/counter`'s cluster demo
  now mounts `metrics()` and `ops()` on all three silos, and
  `pnpm --filter counter-example cluster:serve` keeps them up under steady
  traffic instead of shutting down at the end.

  The traffic is shaped rather than uniform, because a dashboard rendering
  a screen of zeroes demonstrates nothing: a hot grain that builds queue
  depth, a spread of cold ones, a silo left `leaving` by the drain step, and
  one call in seven aimed at a method that does not exist — so the
  queue/turn split, the shard map and `errors.byKind` are all populated.

  The ops secret is separate from the cluster secret on purpose: they
  authenticate different things to different people. One is silo-to-silo,
  the other is an operator with a dashboard.

- **`sigx actors top` — the dashboard** (#101). Five tabs (Overview, Silos,
  Grains, Cluster, Health) hosted by `runShell` from `@sigx/cli/shell`,
  which exists for exactly this: a long-running plugin command that owns the
  screen. Using it rather than mounting our own app is what makes the tabs,
  status line, palette and teardown match every other sigx tool — and what
  lets other plugins' `tui:` contributions merge in alongside ours. It is
  the default verb, so bare `sigx actors` opens it.

  What each screen puts FIRST is the design work. A dashboard's job is not
  to show everything, it is to make the wrong thing impossible to miss, so
  every screen leads with a banner for whatever is currently wrong: a failed
  poll, a `partial` fan-out, an **unclaimed reminder shard** (nothing is
  ticking it, and nothing else surfaces that), a **fenced** silo (refusing
  activations while still published as `active`), or a non-zero
  `authFailures` (a secret rotation that has not reached every silo).

  **A failed poll does not blank the screen.** The last good snapshot stays
  up, labelled as stale, with `poll FAILING` and an age in the status bar. A
  dashboard that goes blank the moment a silo hiccups destroys exactly the
  context you need to understand the hiccup. The status bar also flags data
  older than three intervals, because stale numbers that still look live are
  the failure mode the whole line exists to prevent.

  Polling has back-pressure: an in-flight request is abandoned rather than
  queued behind, so a 5s silo on a 1s interval does not accumulate requests
  until something gives, and a late reply from an abandoned poll cannot
  clobber a newer answer. The timer is `unref`'d so Ctrl+C exits now rather
  than after the next interval.

  Piped or in CI it degrades rather than hanging: one poll, one line, exit.

- **`tui:` contributions** (#101) — an `actors` status chip and an
  `/actors` palette command, merged into any other plugin's shell.

  Deliberately a pointer rather than a live panel. Rendering one would mean
  opening a source, and the only zero-config source **starts a silo** — it
  joins membership, claims actors and ticks reminders. Doing that as a side
  effect of somebody else's dev server is a genuinely bad surprise, and "the
  monitor quietly became a cluster member" is not a sentence anyone should
  have to debug. `top` is an explicit command; that is where a source opens.

### Changed

- **The package has its own tsconfig, and the repo-root program excludes
  it** (#101). `@sigx/runtime-terminal` and `sigx` both augment the GLOBAL
  `JSX.IntrinsicElements` and both declare `text` — terminal's takes
  `color`, the DOM's is `SVGAttributes<SVGTextElement>` — so in one program
  containing both, `<text color="cyan">` cannot mean the terminal one. A
  per-file `@jsxImportSource` pragma does not help: the collision is in the
  namespace, not the factory.

  There are two configs rather than one, because CI runs `typecheck` before
  `build`: `tsconfig.json` emits declarations against the *published* shape
  of `@sigx/actors`, while `tsconfig.typecheck.json` resolves it from source
  (as the root config does for every other package) so a clean checkout
  typechecks. The root `typecheck` script now chains the package's own, so
  excluding it from the root program does not quietly drop it — and the
  typecheck config covers `__tests__` for the same reason.

- **Terminal building blocks** (#101) — `src/tui/`: `sparkline`, `meter`,
  `trend`, `layoutTable`/`fit`/`scrollOffset`/`moveCursor`/`sortRows`,
  `histogramRow`/`commonScale`, and `shardGrid`/`unclaimedShards`/
  `splitShards`.

  `@sigx/terminal` 0.9 ships `ProgressBar`, `Spinner`, `Card`, `Tabs`,
  `StatusBar`, `KeyHints`, `LogView`, `Badge` and `Divider` — nothing
  time-series, and a `Table` that is `columns: string[]` / `rows:
  string[][]`: static, no scrolling, no sorting, no selection. These are
  **intended to upstream to `@sigx/terminal-ui`**, and are written pure over
  plain data so that port is mostly re-styling.

  Three of the decisions are about not misleading:

  - The sparkline scales from **zero**, not from the series minimum. A
    min-anchored sparkline turns a flat line at 1000 req/s into a mountain
    range of noise, which is the commonest way these lie.
  - The lowest block is **reserved for exact zero**, so any non-zero value
    gets at least the second. It costs one level of resolution and buys the
    distinction that matters at a glance: 1 req/s against a scale of 1000
    otherwise draws exactly like silence.
  - A **gap is not a zero.** `null` renders as `·`, because a counter reset
    and an unreachable poll are not the same fact as an idle period, and
    drawing them at the baseline claims they are.

  `sortRows` breaks ties on identity and `moveCursor` clamps rather than
  wrapping, both for the same reason: a dashboard re-sorts every poll, and
  rows that swap places or a cursor that silently returns to the top read as
  activity when nothing has happened.

  Histogram rows take a **shared** scale (`commonScale`, on p99 rather than
  max so one outlier cannot flatten the panel), because the entire value of
  stacking `latency`, `queue` and `turn` is reading them against one axis.

- **`@sigx/actors-cli`** (#101) — a `@sigx/cli` plugin that observes actor
  silos, following the `@sigx/lynx-cli` precedent rather than shipping its
  own binary. `sigx` is already the command an app author runs, and the
  plugin model means these panes can merge into ANOTHER plugin's shell; a
  standalone binary could only ever be a separate window.

- **`sigx actors stats`** — one snapshot, printed and gone: cluster totals,
  per-silo state, the queue/turn latency split, error kinds, the slowest
  methods by p99 turn time, and the hottest grains. `--json` emits the whole
  normalized snapshot, on the principle that the point of `--json` is that
  something else does the summarising.

- **`sigx actors health`** — liveness and readiness with **the exit code as
  the answer**: `0` ready, `1` reachable but not ready, `2` unreachable.
  The last two are deliberately distinct — a silo answering "not ready" is
  alive and handing off its activations, a silo answering nothing is a
  different incident with a different fix, and collapsing them is how a
  rolling deploy becomes an outage.

- **Two sources behind one seam.** `embeddedSource` loads the project's app
  module in-process: zero config, no secret, and strictly more visible than
  the wire allows. `httpSource` polls a running silo's `ops()` endpoint,
  loading no user code. `--url` picks the second, and it WINS over a local
  module — passing a URL is an explicit statement about which silo you mean,
  and silently preferring a local one would monitor the wrong process while
  looking correct.

  Embedded mode reads plugin handles from named exports because
  `defineActorApp` does not expose its plugins — they are contributions, not
  members — and inventing a registry accessor in core for one consumer was
  the worse trade. An app exporting none of them still monitors; it shows
  fewer panels.

  It also never stops a silo it merely attached to: whether the app was
  already running is read BEFORE starting it, because that is the only
  moment the answer is knowable, and closing the monitor must not drain
  someone else's activations.

- **`@sigx/actors-cli/source`** — the data layer, as its own subpath and
  free of any terminal import, so a web dashboard can consume it unchanged.

  It carries the parts every renderer would otherwise get wrong. Core
  reports cumulative counters with no windowing, so rates come from diffing
  snapshots — and a counter going **backwards** is a `metrics().reset()`, a
  restart, or a peer dropping out of a fan-out. In all three the previous
  total is meaningless, so `rateBetween` reports a GAP: a negative rate and
  an enormous positive one both look like real traffic. `Series` records
  those gaps as holes rather than dropping the sample, because a straight
  line reads as "nothing happened" rather than "we lost this window".

  `partial` is carried all the way to the output and printed FIRST, never as
  a footnote: the totals under it are lower bounds and still look plausible.
