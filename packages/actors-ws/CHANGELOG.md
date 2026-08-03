# Changelog

## [Unreleased]

### Changed

- **silo → host** (#233): `attachSiloUpgrade` → `attachHostUpgrade`,
  `WsSiloTransport` → `WsHostTransport`, and the default upgrade path
  moved from `/_sigx/silo-ws` to `/_sigx/host-ws`.

### Added

- Initial release: `wsTransport()` and `attachHostUpgrade()`, carrying
  host-to-host frames over WebSocket on the host's existing HTTP port. Shares
  the frame codec and connection state machine with `@sigx/actors-tcp`; all 18
  conformance cases pass, cross-host watches (`$watch:` symbols) included.
