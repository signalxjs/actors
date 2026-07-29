# Changelog

## [Unreleased]

### Added

- Initial release: `wsTransport()` and `attachSiloUpgrade()`, carrying
  silo-to-silo frames over WebSocket on the silo's existing HTTP port. Shares
  the frame codec and connection state machine with `@sigx/actors-tcp`; all 18
  conformance cases pass, cross-silo watches (`$watch:` symbols) included.
