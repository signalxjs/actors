# Changelog

## [Unreleased]

### Added

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
