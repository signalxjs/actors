# Changelog

## [Unreleased]

### Fixed

- **`socketTransport().close()` during the first dial no longer leaks the
  socket** (#175). `close()` can only release the link it holds, and inside
  the connect window there is none — so a handshake that completed after the
  close was adopted anyway and never released. The socket stayed open until
  TCP gave up, and server-side that is a live `createActorSocketSession`
  holding its pinned identity and any subscriptions it went on to establish,
  with nothing to bound it: `revalidateMs` and `maxConnectionMs` both
  default to off. An ordinary browser reaches this — the plugin closes the
  transport on teardown, so navigating away mid-connect was enough, and a
  slow network is what made the window wide. A caller parked on that dial
  also hung forever rather than failing; it now rejects with the usual
  closed error (`status: 0`). Found by the connection-scale rig in #172,
  which measured 300 such closes leaving all 300 sockets open on the host.

## [0.6.0] - 2026-08-09

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
- **`attachActorSocket()` + `toRequest()` on `./node`** (#99): the Node
  server adapter — an exact-path upgrade listener (never a prefix match;
  unmatched upgrades are destroyed only when no other listener exists) over
  a lazily-imported `ws` (optional peer, needed only by this entry).
  Messages that arrive before the session's async construction completes
  are buffered, not lost. The manual form stays first-class: a server you
  already run constructs `createActorSocketSession` in its own upgrade
  handler, with `toRequest()` building the WinterCG `Request` a Node
  upgrade doesn't have.
