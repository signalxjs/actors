# Changelog

## [Unreleased]

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

### Changed

- **Peers `@sigx/actors@^0.3.0`.** The actor URL grammar is breaking
  (#96), so the whole family moves together — see the `@sigx/actors`
  changelog. **Upgrade every host before any client or peer starts
  emitting**: a host still on 0.2.x refuses calls from an upgraded one.

  This package's own wire is unaffected: the frame transports carry the
  in-memory symbol, with no URL involved.

## [0.2.0] - 2026-08-05

### Changed

- **Peers `@sigx/actors@^0.2.0`.** The guard split is breaking, so the
  whole family moves together — see the `@sigx/actors` changelog and core's
  [0.15 migration guide](https://github.com/signalxjs/core/blob/main/docs/migrations/0.15-guard-split.md).
  Actors, workers and jobs defined against this package declare access with
  `authorize` / `methodAuthorize` / `allowAnonymous` now, and the runtime is
  fail-closed: one that declares nothing, in a process with no server app,
  denies with 401.

## [0.1.0] - 2026-08-03

### Changed

- **silo → host** (#233): `attachSiloUpgrade` → `attachHostUpgrade`,
  `WsSiloTransport` → `WsHostTransport`, and the default upgrade path
  moved from `/_sigx/silo-ws` to `/_sigx/host-ws`.

### Added

- Initial release: `wsTransport()` and `attachHostUpgrade()`, carrying
  host-to-host frames over WebSocket on the host's existing HTTP port. Shares
  the frame codec and connection state machine with `@sigx/actors-tcp`; all 18
  conformance cases pass, cross-host watches (`$watch:` symbols) included.
