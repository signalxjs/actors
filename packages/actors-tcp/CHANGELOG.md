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
