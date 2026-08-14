# Changelog

## [Unreleased]

### Added

- **`@sigx/actors-dashboard` — the first-party web dashboard (#241).** The
  five tabs `sigx actors top` has, in a browser: Overview, Hosts (with a
  per-host drill-down), Actors, Cluster, Health. Pure sigx — no CSS framework,
  no chart library, and no `sigx` umbrella import.

  `<ActorsDashboard source={httpSource({ url: '/admin/ops' })} />` is the whole
  setup. Every panel is exported standalone and takes `{ state }`, so a portal
  can embed one table rather than the shell; `mountActorsDashboard(el, opts)`
  covers a host page that is not a sigx app.

  It answers #117: a team building an admin page over `@sigx/actors-cli/source`
  should embed this rather than ship a near-duplicate that drifts. Nothing here
  decides what a number MEANS — what is wrong, what a figure is about, whether
  a counter moved backwards, whether a shard state is an incident, all of it is
  `@sigx/actors-monitor`'s, and both renderers are tested against the same
  fixture so a disagreement between them is detectable.

  **The ops secret never reaches the browser.** `ops()` sets no CORS headers
  and refuses to construct without a bearer token outside dev, so `httpSource`
  points at a same-origin route of the consuming app, which authenticates the
  operator and forwards with the bearer attached server-side.

  Styling is self-contained: one `<style>` injected at most once per document,
  every colour and metric a `--sigx-actors-*` custom property, dark via
  `prefers-color-scheme` or an explicit `theme`. `styles={false}` opts out and
  `actorsDashboardCss` is exported for anyone shipping the sheet themselves.

  Two browser-specific behaviours the terminal has no equivalent of: the poll
  loop stops on unmount (a single-page app that navigates away would otherwise
  poll the cluster for the lifetime of the tab), and the status line carries
  the AGE of the last successful poll on its own wall-clock ticker — a tab left
  open against a host that died at 03:00 renders a perfectly plausible cluster,
  and nothing else on screen would say the numbers had stopped moving.
