# Changelog

## [Unreleased]

## [0.9.1] - 2026-08-14

### Fixed

- **A failing first poll now says why, on the page (#256).** Every panel
  opened with `if (!snapshot) return <p>connecting…</p>`, and that early
  return sat ABOVE the alert banner — so in the one case where there is no
  snapshot *because the first poll failed*, the reason rendered nowhere except
  `poll FAILING` in small text in the status line. A dashboard pointed at an
  unreachable source showed "connecting…" indefinitely, which reads as "still
  working on it".

  The panels now render the banner first and then either `connecting…` or
  `no data yet — the first poll of <source> failed`, naming the source so a
  portal watching several deployments knows which one went quiet. The ordering
  lives in one `<Awaiting>` component rather than in six copies of the early
  return, because six copies is six chances to put it back the wrong way
  round.

- **`@sigx/actors` is declared as a peer dependency — 0.9.0 could not be
  loaded at all (#254).** `dist/index.js` imports `digestSnapshot` from
  `@sigx/actors/host` to decode the per-host latency histogram in the drill-
  down, and the manifest named `@sigx/actors` nowhere, so installing this
  package on its own produced:

  ```
  Error [ERR_MODULE_NOT_FOUND]: Cannot find package '@sigx/actors'
    imported from node_modules/@sigx/actors-dashboard/dist/index.js
  ```

  **If you installed 0.9.0, upgrade** — there is no workaround short of
  adding `@sigx/actors` to your own dependencies, which is what this release
  does for you. npm ≥7 installs peers automatically, so a fresh install needs
  no change on your side.

  A peer rather than a dependency because `@sigx/actors` is WinterCG-clean
  and zero-dependency — it is browser-safe and tree-shakes to the histogram
  walk — and a portal that already uses it keeps a single copy. Required
  rather than optional because the import is at module scope.

  It shipped because `scripts/verify-pack.js` installed all twelve tarballs
  into ONE sandbox, where a missing declaration always resolves. That script
  now asserts, per package, that every bare import in its packed `dist` is a
  package it declares.

## [0.9.0] - 2026-08-14

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
