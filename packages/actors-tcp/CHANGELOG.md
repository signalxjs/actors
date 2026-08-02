# Changelog

## [Unreleased]

### Changed

- **silo → host** (#233): the transport implements the renamed
  `HostTransport` seam; framing and the protocol are otherwise unchanged.

### Added

- Initial release: `tcpTransport()`, a framed TCP transport for
  `@sigx/actors` host-to-host traffic. One multiplexed connection per peer,
  per-stream cancellation and credit-based backpressure, and the shared
  transport conformance suite passing in full — cross-host watches
  (`$watch:` symbols) included — including the link-hygiene
  cases HTTP skips.
