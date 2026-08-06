# @sigx/actors-ws

**WebSocket transport** for [`@sigx/actors`](https://sigx.dev/actors)
host-to-host traffic — the same frames as
[`@sigx/actors-tcp`](https://www.npmjs.com/package/@sigx/actors-tcp), riding the
HTTP port the host already has.

- **`wsTransport(options)`** — a transport handle for the `cluster()` plugin.
  `advertiseUrl()` is **required**: the port is often unknown until the HTTP
  listener has bound, and the address must exist before the membership join.
- **`attachHostUpgrade()`** — registers the `upgrade` handler. A Node WebSocket
  upgrade needs the raw socket, which `ActorRoute.handle` cannot express, so
  this is the one piece the route seam cannot carry.

Pick this over `@sigx/actors-tcp` when one port matters, when traffic has to
cross a proxy or load balancer, or when a WinterCG runtime must dial in. Both
collapse the connection count the same way — one connection per peer.

```sh
pnpm add @sigx/actors-ws ws
```

[`ws`](https://github.com/websockets/ws) (≥ 8) is a peer dependency for the Node
server half, as is `@sigx/actors` itself. That half requires
**Node ^20.19.0 || >=22.12.0**; the client half is the standard `WebSocket` and
runs anywhere. HTTP remains the cluster default, because
`@sigx/actors/cluster` stays WinterCG-clean.

## Documentation

**https://sigx.dev/actors/packages/actors-ws/overview/**

Transport comparison and measured numbers:
https://sigx.dev/actors/docs/transports/

Source, examples and architecture notes:
https://github.com/signalxjs/actors

MIT © Andreas Ekdahl
