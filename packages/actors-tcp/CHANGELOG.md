# Changelog

## [Unreleased]

### Changed

- **Security: inbound connections now have a handshake deadline and a
  cap** (#277). The listener adopted every socket into `pendingInbound`
  and answered WELCOME, and that set was drained only on HELLO, on close,
  or at `stop()` — with no timer anywhere on the path. A peer that
  connected and then said nothing held a file descriptor and a
  `maxFrameBytes`-capable read buffer for the life of the process, so a
  trickle of silent connections exhausted descriptors without ever
  authenticating: textbook slow-loris. New `handshakeTimeoutMs` (default
  10 000; `0` disables) closes a connection that has not named itself, and
  `maxPendingInbound` (default 256; `0` disables) refuses further sockets
  before adopting them, so a refused connection costs no `HostConnection`,
  no read buffer and no WELCOME write. The deadline is cancelled on HELLO,
  so an established peer link is never reaped. A cluster handshakes one
  connection per peer, so neither default is reachable by a busy cluster.
  Both options are validated at construction — a negative or non-finite
  value throws rather than quietly disabling the bound, since both are
  applied with a `> 0` test and a typo would otherwise reach exactly the
  state they exist to prevent. The README now also states plainly that this
  transport binds all interfaces by default, speaks no TLS, and belongs on
  a private network.

- **silo → host** (#233): the transport implements the renamed
  `HostTransport` seam; framing and the protocol are otherwise unchanged.

### Added

- Initial release: `tcpTransport()`, a framed TCP transport for
  `@sigx/actors` host-to-host traffic. One multiplexed connection per peer,
  per-stream cancellation and credit-based backpressure, and the shared
  transport conformance suite passing in full — cross-host watches
  (`$watch:` symbols) included — including the link-hygiene
  cases HTTP skips.
