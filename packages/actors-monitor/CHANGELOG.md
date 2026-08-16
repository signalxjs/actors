# Changelog

## [Unreleased]

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
