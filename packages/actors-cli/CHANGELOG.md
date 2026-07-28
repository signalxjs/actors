# Changelog

## [Unreleased]

### Added

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
