# Changelog

## [Unreleased]

### Added

- **`HostView.sockets` — the host's socket sessions, when it published them
  (#166).** The `sockets` ops section an app registers from `socketStats()`
  (`registry.reportOps('sockets', () => stats.snapshot())`) now rides
  through `httpSource` onto the POLLED host's row: the only row it can
  belong to, because the cluster fan-out carries no socket digest yet. Every
  other host is `null`, which means "said nothing" — the same rule as
  `metrics` — and never "no sockets there". `withSockets(hosts, hostId,
  sockets)` is the one place that attaches it, shared with the CLI's
  embedded source so the rule is made once. `format.bytes()` renders byte
  counts (`950 B`, `12.4 kB`, `3.1 MB`) with `null` as an em dash, because a
  host whose sessions could not report their buffers has not said `0 B`
  (#208).

### Added

- **`HostView.meta` — each host's placement hints, and the node count derived
  from them (#51).** A `HostReport` has carried `PlacementOptions.meta` for a
  while; the monitor now keeps it (`null` when the host published none, as
  every non-cluster host does) instead of dropping it on the floor. The key
  with an agreed meaning is `node`: the machine the host runs on, which the
  perf charts now fill from `spec.nodeName`.

  Two helpers derive the one-glance version. `nodeCount(hosts)` is the number
  of DISTINCT `meta.node` values, or `null` when no host reports one — a fleet
  outside Kubernetes has not said where it runs, and guessing "one node" would
  claim a packed fleet while "one per host" would claim a spread one.
  `hostSpread(hosts)` renders `3 host(s) / 1 node(s)`, falling back to the
  plain `3 host(s)` when there is nothing to add, and `scopeOf` now uses it,
  so every panel heading that says `cluster · …` says how many nodes it spans.
  Derived here rather than in either renderer because the finding this exists
  for — three replicas on one 2-vCPU node capping a real cluster while three
  other nodes idled, with every replica readout saying `3/3` — is a count two
  renderers must not make differently.

  `nodeLabels(hosts)` is the third helper, for the table cell: full node
  name → the tail that tells it from the others (`…vmss000001`), with the
  prefix every reported node shares dropped at a separator. Real node names
  differ only in their tail and every table truncates from the right, so the
  raw name would show two different nodes as `aks-sigxacto…` twice — a
  spread fleet posing as a packed one. A lone node keeps its full name.

## [0.9.1] - 2026-08-16

### Fixed

- **The poll-failure alert no longer claims a last good snapshot when there is
  none (#256).** With a snapshot the numbers below the banner are real but
  stale and the caption says so; with none — a first poll that never landed —
  it now reads `poll failed — nothing has been read yet`, because the previous
  wording described a screen that does not exist. Both renderers inherit it.

### Added

- **`@sigx/actors-monitor` — the renderer-free data layer, extracted from
  `@sigx/actors-cli` (#239).** `httpSource`, the normalized `MonitorSnapshot`
  shape, `DashboardState` (the poll loop), rate derivation across resets,
  alert derivation, reminder-shard states, histogram percentiles and the
  formatting helpers, in a package with no renderer, no `node:` import and no
  DOM.

  It exists because a browser could not reach any of it. `@sigx/actors-cli`
  hard-depends on `@sigx/terminal` and non-optionally peers `@sigx/cli`, so a
  web app importing `@sigx/actors-cli/source` for `httpSource` paid for both —
  and that subpath re-exported `embeddedSource`, whose documented job is to
  dynamic-`import()` user code and start a real host. `embeddedSource` stays
  behind in the CLI; nothing here can reach it.

  Two things moved that were previously private to the terminal screens, and
  both are judgements a second renderer must not re-make: `alertLines` (what
  is wrong, worst first) with `scopeOf` / `polledLabel` / `coverageNote` (what
  a number is ABOUT — the #121 work), and `shardStates` (a reminder shard with
  no claimant is an incident; one with two is merely a divergence). Alerts now
  carry a severity (`'danger' | 'warn'`) rather than a `@sigx/terminal` theme
  colour.

  `@sigx/actors` is an **optional** peer, types only — HTTP mode still works
  with no actor runtime installed. `@sigx/reactivity` is the one runtime peer,
  for the signal `DashboardState` publishes its view through.

  No behaviour change: `@sigx/actors-cli/source` re-exports everything it did
  before, so existing importers are unaffected.
