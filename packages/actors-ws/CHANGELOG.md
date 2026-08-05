# Changelog

## [Unreleased]

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
