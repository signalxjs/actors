# Changelog

## [Unreleased]

### Added

- **Socket sessions in `stats` and `top` (#166).** A host that publishes the
  `sockets` ops section — `registry.reportOps('sockets', () =>
  socketStats().snapshot())` — gets a `sockets — host <id> ONLY` block in
  `sigx actors stats`: open sessions and calls in flight, live
  subscriptions (and how many were throttle-quantized), deliveries with
  their size, buffered bytes, connections opened/closed/refused, evictions
  (lifetime-cap and protocol-breach closes, when any), and the connection
  lifetime percentiles once anything has closed. `top` shows the same rows
  on the host drill-down and a `SOCKETS` column (open sessions) in the Hosts
  table — the column only once some host reported, so a fleet that never
  said does not read as "no sockets anywhere". The heading says `ONLY`
  because the fan-out carries no socket digest yet: the section is the
  polled host's own, never a cluster total, and a peer's `—` means it said
  nothing. Buffered bytes that no session could report draw as `—`, not
  `0 B` (#208). The embedded source reads the section from the app's
  `ops()` handle, the HTTP source from the ops body.

### Added

- **`locality` on the cluster screen (#52).** The header now reads
  `locality  80% local  (10 dispatches)`, derived from the placement's new
  per-request pair `dispatchesLocal` / `dispatchesRemote` — the fraction
  `routedLocal` never was, because it counts placement decisions and the
  warm local fast path bypasses it. Reads `—` until a host that reports the
  pair has dispatched anything, and never `NaN%` against a fleet on an older
  build. The reminder-shard grid below it now packs eight cells per row on
  any pane 48 columns or wider (it folded at seven on a 60-column pane, one
  row more than it needed), which is the line the new row takes.

- **The node each host runs on, and how many nodes the fleet spans (#51).**
  The Hosts table has a `NODE` column, the host drill-down a `node` row, the
  Overview a `nodes` row under `hosts`, and every `cluster · N host(s)` heading
  now reads `cluster · 3 host(s) / 1 node(s)` once any host reports where it
  runs. `sigx actors stats` prints `nodes` under `hosts` and `node <name>` on
  each host line; the non-TTY `top` summary uses the same spread. All of it
  comes from `HostView.meta.node`, which the perf charts now publish via the
  downward API — so three replicas packed onto one node is visible from the
  ops output instead of `kubectl top pods` joined against `kubectl get pods
  -o wide` by hand. A fleet that reports no node shows `—` in the column and
  no `nodes` row at all, rather than a count nobody measured. The `NODE` cell
  is the monitor's label — `…vmss000001`, the tail that differs — because
  the table shrinks from the right and two different AKS nodes cut to
  `aks-sigxacto…` would read as one; the full name is in the drill-down.

### Changed

- **HTTP mode no longer requires the cwd project to depend on
  `@sigx/actors` (#116).** The plugin's `detect` is now permissive — the
  `sigx actors` command group registers wherever `@sigx/actors-cli` is
  installed — and the project requirement moved to the verb path, where it
  belongs to the one mode that has it: without `--url` and without
  `@sigx/actors` in the cwd manifest, the command fails with
  "embedded mode needs a project that depends on `@sigx/actors`; pass
  `--url <origin>` to watch a running host instead" (exit code 2). With
  `--url`, HTTP mode runs unconditionally — it loads no user code and holds
  no host, so an ops box, a control plane watching tenant clusters, or a CI
  probe no longer needs a decoy dependency (or gets the misleading
  `Unknown command 'actors'`) to use it.

## [0.9.1] - 2026-08-16

### Changed

- **The data layer moved to `@sigx/actors-monitor` (#239).** `httpSource`, the
  `MonitorSnapshot` shape, `DashboardState`, rate derivation, alert derivation
  and the formatting helpers are now a package of their own, and this one
  depends on it. **`@sigx/actors-cli/source` re-exports all of it, so nothing
  breaks** — but a browser should import `@sigx/actors-monitor` directly.

  It moved because a web dashboard could not reach it here. This package
  hard-depends on `@sigx/terminal` and non-optionally peers `@sigx/cli`, so
  importing `/source` for `httpSource` cost a web app both — and that subpath
  re-exported `embeddedSource`, whose documented job is to dynamic-`import()`
  user code and start a real host. `embeddedSource` stays here and is now all
  that `src/source/` owns.

  Two internals went with it, because they were judgements rather than
  drawing: `alertLines` (with `scopeOf`, `polledLabel` and `coverageNote`) and
  the reminder-shard states. `unclaimedShards` / `splitShards` are no longer
  exported from `src/tui` — they are `@sigx/actors-monitor`'s; `shardCells`
  stays and maps the shared states onto `@sigx/terminal` tones.

  No user-visible behaviour change: the same commands print the same output.

## [0.5.0] - 2026-08-07

### Changed

- **Peers `@sigx/actors@^0.5.0`.** The family versions in lockstep, so the
  range moves with the release. 0.5.0 only ADDS `ctx.changes({ throttleMs })`
  and removes a snapshot a `$live` watch never read (#129) — no wire or API
  break, so a 0.4.x host interoperates fine.

## [0.4.0] - 2026-08-07

### Changed

- **Peers `@sigx/actors@^0.4.0`.** The family versions in lockstep, so the
  range moves with the release. Nothing else to do: 0.4.0 only ADDS
  `onSettled` to `defineJob` (#125), so unlike the 0.2.0 and 0.3.0 moves
  there is no wire or API break and a 0.3.x host interoperates fine.

- README trimmed to a pointer at https://sigx.dev/actors (#113): thesis,
  install, peer-dependency and minimum-version requirements, and links. The
  reference material is on the docs site; relative links (which npm does not
  resolve) are gone. No code or API change.

## [0.3.0] - 2026-08-05

### Added

- **One-way failures are visible.** `sigx actors stats` prints a
  `one-way fail` row under `calls`, and the dashboard's calls rows append
  `N one-way` — both only when the count is non-zero, and the dashboard row
  tones `warn` exactly as it does for `failed`.

### Changed

- **Peers `@sigx/actors@^0.3.0`.** The actor URL grammar is breaking
  (#96), so the whole family moves together — see the `@sigx/actors`
  changelog. **Upgrade every host before any client or peer starts
  emitting**: a host still on 0.2.x refuses calls from an upgraded one.

## [0.2.0] - 2026-08-05

### Changed

- **Realigned to the tier-1 line: `@sigx/terminal@^0.12`** (was `^0.11`),
  with `@sigx/cli` `^0.10` / peer `>=0.10.0` and `@sigx/args@^0.12`. On a
  0.x line a caret cannot cross the minor, so `^0.11.0` could never resolve
  the aligned `0.12.0` release — which pinned two copies of
  `@sigx/reactivity` into one graph and made `signal` a conflicting star
  export. These are not core packages, so they take literal specs rather
  than `catalog:` and did not move with the core bump.

- **Peers `@sigx/actors@^0.2.0`.** The guard split is breaking, so the
  whole family moves together — see the `@sigx/actors` changelog and core's
  [0.15 migration guide](https://github.com/signalxjs/core/blob/main/docs/migrations/0.15-guard-split.md).
  Actors, workers and jobs defined against this package declare access with
  `authorize` / `methodAuthorize` / `allowAnonymous` now, and the runtime is
  fail-closed: one that declares nothing, in a process with no server app,
  denies with 401.

## [0.1.0] - 2026-08-03

### Changed

- **silo → host, grain → actor** (#233): the dashboard tabs and stats
  headings now say hosts and actors, `SiloView`/`siloViewFromReport` are
  `HostView`/`hostViewFromReport`, and the ops HTTP source sends
  `?host=<id>` following the runtime's renamed ops contract.

### Added

- **The cursor leads somewhere: `enter` opens a host** (#121). Selecting a
  row in the Hosts table used to move a highlight and change nothing else,
  because a `HostReport` carried no metrics, no health and no actors for a
  peer. It carries all three now, so `enter` opens that host in full — its
  calls, latency, readiness, checks, error kinds and the actors living on
  it — and `h` returns to the list with the cursor where it was (the shell claims `esc` for its own palette and view stack).

  The detail is requested only while the panel is open. A detail poll makes
  every host walk its activation table, so a dashboard that always asked
  would make the cluster pay for a panel nobody is looking at.

- **Cluster-wide numbers, labelled as such** (#121). The Overview shows the
  merged `totals.metrics` when the fan-out produced them, headed
  `cluster · N host(s)`; the Actors and Health tabs are headed with the host
  they actually describe. `sigx actors stats` does the same —
  `calls — cluster-wide (2 of 3 hosts reporting)` versus
  `calls — host s.ab12 ONLY`.

  This is the second half of the issue's complaint. The numbers were not
  wrong; they were unlabelled, so one host's `calls total 382` printed
  directly beneath `cluster hosts 2` and read as a cluster figure.

- **A `READY` column on the Hosts table** (#121), because every host's
  readiness now travels in the fan-out rather than only the polled one's.
  `FATAL` is shown distinctly from `NO`: it means the host cannot recover
  and must be REPLACED, and reading it as "draining" is how a zombie pod
  sits there forever.

- **The coverage caveat** (#121). When only some hosts report metrics, the
  Overview and `stats` both say so — totals covering two thirds of a fleet
  look exactly like totals covering all of it.

### Changed

- **The component layer is upstream's now** (#121). `@sigx/terminal` 0.11
  ships everything offered in signalxjs/terminal#103 — `DataTable`,
  `Sparkline`, `Trend`, `Meter`, `BarChart`, `DetailList`, `StatusGrid` over
  `displayWidth` / `fitCell` / `layoutTable` / `scrollWindow` /
  `commonScale` / `statusGrid` — with our decisions kept as its defaults. So
  `src/tui/sparkline.ts` and `src/tui/table.ts` are **deleted** rather than
  wrapped, and `cellWidth` is gone exactly as its own comment promised it
  would be when `fitCell`/`padCell` landed. A second implementation of a
  sparkline is how two panels start disagreeing about what a flat line means.

  What stays local is only what is about actors: the percentile triple, the
  reminder-shard claim map, and `Line` (because `Text` is still a span).

  The assertions did NOT move upstream with the code. `tui.test.ts` now
  points at `@sigx/terminal` and states what this dashboard depends on — a
  gap is not a zero, a tiny non-zero value does not round away, truncation is
  marked, equal rows do not shuffle between polls. All of it passed on the
  first run against 0.11; keeping it means a future bump cannot quietly take
  any of it back.

- **Requires `@sigx/cli` >= 0.9 and `@sigx/terminal` >= 0.11** (#121), and
  `examples/counter` / `examples/aks-cluster` move with it so the demo
  instructions still run.

### Added

- **Every screen fits the pane it is given** (#121). `@sigx/cli` 0.9 hands a
  tab's `render` its content box (signalxjs/cli#88), so the tables window to
  the rows that are left instead of running off the bottom, the sparklines
  and bar groups size to the width, and banners WRAP rather than being cut
  mid-sentence — an alert truncated at the pane edge loses the half that says
  what to do about it. Before this nothing here called for a size at all:
  content clipped at the right edge while most of the screen sat empty.

  `layout.test.tsx` asserts every screen against a wide pane and a cramped
  one, in display cells; "fits at 100×30" says nothing about an ssh window.

- **A frame snapshot per screen** (`frames.test.tsx`, #121). The whole
  rendered frame, non-TTY, from a fixed fixture — so a rendering regression
  arrives as a diff in a pull request rather than as a screenshot months
  later. Reviewing the diff is reviewing the dashboard.

- **`fatal`, the locate miss rate, and watch counts are on screen** (#121).
  `HealthStatus.fatal` (#144) says a host cannot recover and must be
  REPLACED, which read as ordinary not-ready before; `locateRemote/locates`
  (#138) is the edge-routing miss rate; `remoteWatches`/`inboundWatches`
  (#119) matter next to an activation count, because a watch holds a
  keep-alive on its owner.

### Fixed

- **`j`/`k` moved every table's cursor at once** (#121). `ShellHandle` could
  not say which tab was showing, so the workaround was to move them all.
  `@sigx/cli` 0.9 answers it (`activeTab`), so only the visible tab's cursor
  moves — and the table scrolls to follow it, which is the first time the
  Hosts cursor has done anything observable at all.

- **The queue and activation sparklines were labelled as rates** (#121).
  Both are gauges — how many there are right now — so `33/s` claimed a
  throughput that was never measured. `gauge()` sits beside `rate()` in
  `model/format.ts` to keep the two apart.

- **`ellipsis()` cut by code units** (#121), so an emoji or an astral-plane
  character could be split into a replacement glyph. It counts code points
  now. (It is not display cells on purpose: `model/` is renderer-free, and
  cell-accurate fitting is `fitCell`'s job in the terminal layer.)

### Fixed (earlier, in this same cycle)

- **Every screen rendered as one concatenated line** (#121). `<text>` is a
  SPAN — terminal-zero calls it "deliberately INLINE, unlike every other
  component" — so consecutive `<text>` siblings share a line. Every
  component emitted a `<box>` wrapping N sibling `<text>` rows expecting one
  line each, so they all ran together: `2activations` was a value welded to
  the next row's label, and an entire host table rendered on a single line
  running into its own header.

  Rows are block elements now, via a named `Line` component so the intent is
  visible at the call site rather than implied by a bare `<box>`.

  **Why the tests passed.** `screens.test.tsx` joined the rendered lines
  with `renderNodeToLines(...).join(...)` before `toContain(...)`. Joining
  before substring-matching is structurally blind — it cannot tell twenty
  lines from one. The suite checked content and was mistaken for checking
  rendering. `lines()` and `rows()` now return the array, and the new
  `layout.test.tsx` asserts line counts, which row carries which text, that
  a value never shares a line with the next label, and that nothing exceeds
  the width it was given. Verified by reverting one component and watching
  them fail.

- **Table headers sat one column left of their values** (#121). Rows carry a
  cursor marker the header did not, so every column heading was off by one.

- **Column widths were measured with `.length`, not display cells** (#121).
  A wide glyph is two cells, so an ordinary actor key like `用户-42`
  (`.length` 5, width 7) under-padded its column and shifted everything to
  its right. Truncation now cuts by cells too, so a wide glyph is never
  split in half. Found by the `@sigx/terminal` maintainer while generalising
  these components upstream (signalxjs/terminal#103), where the same bug is
  documented in the existing `Table`; `cellWidth` goes away when their
  `fitCell`/`padCell` land.

- **`DeltaText` hardcoded `▲` as a warning** (#121). Right for latency,
  wrong for throughput — rising calls/s is good news drawn as a problem. It
  takes a `polarity` now (`lower-is-better` by default), so it stops lying
  half the time. Also from the upstream review.

### Added

- **A cluster to point it at** (#101). `examples/counter`'s cluster demo
  now mounts `metrics()` and `ops()` on all three hosts, and
  `pnpm --filter counter-example cluster:serve` keeps them up under steady
  traffic instead of shutting down at the end.

  The traffic is shaped rather than uniform, because a dashboard rendering
  a screen of zeroes demonstrates nothing: a hot actor that builds queue
  depth, a spread of cold ones, a host left `leaving` by the drain step, and
  one call in seven aimed at a method that does not exist — so the
  queue/turn split, the shard map and `errors.byKind` are all populated.

  The ops secret is separate from the cluster secret on purpose: they
  authenticate different things to different people. One is host-to-host,
  the other is an operator with a dashboard.

- **`sigx actors top` — the dashboard** (#101). Five tabs (Overview, Hosts,
  Actors, Cluster, Health) hosted by `runShell` from `@sigx/cli/shell`,
  which exists for exactly this: a long-running plugin command that owns the
  screen. Using it rather than mounting our own app is what makes the tabs,
  status line, palette and teardown match every other sigx tool — and what
  lets other plugins' `tui:` contributions merge in alongside ours. It is
  the default verb, so bare `sigx actors` opens it.

  What each screen puts FIRST is the design work. A dashboard's job is not
  to show everything, it is to make the wrong thing impossible to miss, so
  every screen leads with a banner for whatever is currently wrong: a failed
  poll, a `partial` fan-out, an **unclaimed reminder shard** (nothing is
  ticking it, and nothing else surfaces that), a **fenced** host (refusing
  activations while still published as `active`), or a non-zero
  `authFailures` (a secret rotation that has not reached every host).

  **A failed poll does not blank the screen.** The last good snapshot stays
  up, labelled as stale, with `poll FAILING` and an age in the status bar. A
  dashboard that goes blank the moment a host hiccups destroys exactly the
  context you need to understand the hiccup. The status bar also flags data
  older than three intervals, because stale numbers that still look live are
  the failure mode the whole line exists to prevent.

  Polling has back-pressure: an in-flight request is abandoned rather than
  queued behind, so a 5s host on a 1s interval does not accumulate requests
  until something gives, and a late reply from an abandoned poll cannot
  clobber a newer answer. The timer is `unref`'d so Ctrl+C exits now rather
  than after the next interval.

  Piped or in CI it degrades rather than hanging: one poll, one line, exit.

- **`tui:` contributions** (#101) — an `actors` status chip and an
  `/actors` palette command, merged into any other plugin's shell.

  Deliberately a pointer rather than a live panel. Rendering one would mean
  opening a source, and the only zero-config source **starts a host** — it
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
  hosts, following the `@sigx/lynx-cli` precedent rather than shipping its
  own binary. `sigx` is already the command an app author runs, and the
  plugin model means these panes can merge into ANOTHER plugin's shell; a
  standalone binary could only ever be a separate window.

- **`sigx actors stats`** — one snapshot, printed and gone: cluster totals,
  per-host state, the queue/turn latency split, error kinds, the slowest
  methods by p99 turn time, and the hottest actors. `--json` emits the whole
  normalized snapshot, on the principle that the point of `--json` is that
  something else does the summarising.

- **`sigx actors health`** — liveness and readiness with **the exit code as
  the answer**: `0` ready, `1` reachable but not ready, `2` unreachable.
  The last two are deliberately distinct — a host answering "not ready" is
  alive and handing off its activations, a host answering nothing is a
  different incident with a different fix, and collapsing them is how a
  rolling deploy becomes an outage.

- **Two sources behind one seam.** `embeddedSource` loads the project's app
  module in-process: zero config, no secret, and strictly more visible than
  the wire allows. `httpSource` polls a running host's `ops()` endpoint,
  loading no user code. `--url` picks the second, and it WINS over a local
  module — passing a URL is an explicit statement about which host you mean,
  and silently preferring a local one would monitor the wrong process while
  looking correct.

  Embedded mode reads plugin handles from named exports because
  `defineActorApp` does not expose its plugins — they are contributions, not
  members — and inventing a registry accessor in core for one consumer was
  the worse trade. An app exporting none of them still monitors; it shows
  fewer panels.

  It also never stops a host it merely attached to: whether the app was
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

### Fixed

- **`--json` emitted invalid JSON** (#117). The commands wrote their payload
  through `ctx.logger`, which prefixes every line with `[sigx] `, so
  `sigx actors stats --json | jq …` — the pipeline the README documents —
  received `[sigx] {` and could not parse it. The human tables carried the
  same seven characters of noise in front of every row, undoing the column
  alignment they work to produce.

  Payload now goes to stdout unprefixed; diagnostics keep the logger, and so
  keep going to stderr, which is what makes `2>/dev/null` do the useful
  thing.

  Nothing caught this because nothing observed what the *binary* prints: the
  unit tests called `renderStats()` directly and the end-to-end checks called
  `plugin.commands.actors.run()` with their own logger. There is now a test
  that asserts on `process.stdout`.

- **The example could not actually run the CLI** (#117). `examples/counter`
  demonstrates the dashboard but declared neither `@sigx/actors-cli` nor
  `@sigx/cli`, so plugin discovery found nothing and `sigx actors` was an
  unknown command. Both are dev dependencies of the example now, and the
  docs say which directory to run from — the CLI discovers plugins from the
  dependencies of the project it runs in, so the repo root was never going
  to work.
