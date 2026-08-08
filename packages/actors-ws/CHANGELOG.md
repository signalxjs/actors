# Changelog

## [Unreleased]

### Added

- **`socketTransport()` on `./client`** (#99): browsers (and any WinterCG
  runtime) calling actors over one multiplexed WebSocket, speaking the
  published `@sigx/actors/socket-wire` vocabulary to
  `createActorSocketSession` on `@sigx/actors/server`. The connection seam
  is a link, not a URL (`connect(handlers): SocketLink`), so socket.io or
  any message channel can carry the traffic; `url` is sugar over the global
  `WebSocket`. Host-affine (per-call `endpoint` is ignored with a `__DEV__`
  warning — every call re-dispatches through placement), and in-flight
  calls fail un-retried when the socket drops: re-sending a non-idempotent
  actor method is a correctness bug, not a retry.
