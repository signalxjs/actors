# @sigx/actors-tcp

Framed **TCP transport** for [`@sigx/actors`](https://sigx.dev/actors)
host-to-host traffic: one multiplexed connection per peer instead of HTTP's one
per in-flight request.

- **`tcpTransport()`** — a `HostTransportFactory` for the `cluster()` plugin.
  Chain it ahead of `httpTransport()` so a rolling deploy stays safe.

On Node this is the recommended transport, and the reason is **socket count**,
not latency: HTTP's pool sizes to `concurrency × peers`, which is file
descriptors, kernel buffers and conntrack entries. One connection per peer does
not change with RTT.

> **This transport belongs on a private network.** It binds **all interfaces**
> unless you set `host`, and it speaks no TLS — the cluster HMAC authenticates
> the peer, not the link. Put it on a pod network, a VPC or an mTLS-terminated
> mesh, never on a public interface.

```sh
pnpm add @sigx/actors-tcp
```

Node-only (it imports `node:net`), zero runtime dependencies. `@sigx/actors` is
a peer dependency. HTTP remains the cluster default, because
`@sigx/actors/cluster` stays WinterCG-clean.

## Documentation

**https://sigx.dev/actors/packages/actors-tcp/overview/**

Transport comparison and measured numbers:
https://sigx.dev/actors/docs/transports/

Source, examples and architecture notes:
https://github.com/signalxjs/actors

MIT © Andreas Ekdahl
