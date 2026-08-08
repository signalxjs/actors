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
- **`live()`** (#99): the incremental live channel. Adding a subscription is
  one ~40-byte `{i,sub}` message and removing one is `{i,uns}` — no
  debounce, no restart, no generation counter, because nothing reopens.
  Subscriptions are declarative, so unlike calls they DO re-establish across
  a drop: the transport redials on its own and re-seeds the set, with the
  shared `fingerprint()` suppressing values that did not change. Wired
  through `delegateChannel()`, so `useActorState` and `actorsPlugin` need
  zero changes: `actorsPlugin({ transport: socketTransport({ url }) })` is
  the whole integration.
