# Changelog

## [Unreleased]

### Added

- Initial release: `tcpTransport()`, an Orleans-style framed TCP transport for
  `@sigx/actors` silo-to-silo traffic. One multiplexed connection per peer,
  per-stream cancellation and credit-based backpressure, and the shared
  transport conformance suite passing in full — cross-silo watches
  (`$watch:` symbols) included — including the link-hygiene
  cases HTTP skips.
