# `@sigx/actors-ws`

**WebSocket transport** for [`@sigx/actors`](https://www.npmjs.com/package/@sigx/actors)
host-to-host traffic — the same frames as
[`@sigx/actors-tcp`](../actors-tcp), over one port.

```sh
pnpm add @sigx/actors-ws ws
```

```ts
import { createServer } from 'node:http';
import { defineActorApp } from '@sigx/actors/host';
import { createAppHandler } from '@sigx/actors/node';
import { cluster, httpTransport } from '@sigx/actors/cluster';
import { wsTransport } from '@sigx/actors-ws';

const ws = wsTransport({
    advertiseUrl: () => `ws://10.0.4.7:7311/_sigx/host-ws`
});

const app = defineActorApp({ actors, storage }).use(
    cluster({ providers, advertise: 'http://10.0.4.7:7311', secret, transport: [ws, httpTransport()] })
);

const server = createServer(createAppHandler(app));
await ws.attach(server);          // the upgrade a route cannot express
await new Promise<void>((r) => server.listen(7311, r));
await app.start();
```

## Why this and not `@sigx/actors-tcp`

Raw TCP is the better wire where you control the network. WebSocket earns its
place in the cases TCP cannot reach:

- **One port.** Frames ride the HTTP listener the host already has — no second
  port in a security group, a Service, or a firewall rule.
- **Through proxies and load balancers.** They forward WebSocket; they will
  generally not forward an arbitrary TCP protocol.
- **Dialable from WinterCG runtimes**, since the client half is the standard
  `WebSocket`. `node:net` can never be.

The trade is a small framing tax over raw TCP, and a `ws` peer dependency for
the Node server half.

Both transports collapse the connection count the same way — **one connection
per peer** rather than HTTP's `concurrency × peers` — and both are far faster
than HTTP on loopback. Measured at concurrency 64 against a *tuned* HTTP
baseline (`benchmarks/BASELINES.md`):

| | connections per peer | ops/s | p99 | bytes/call |
|---|---:|---:|---:|---:|
| tuned HTTP | 64 | 14 287 | 9.6 ms | 640 |
| **WebSocket** | **1** | **63 495** | **1.58 ms** | **236** |

Read the two wins differently: the socket count holds at any RTT, while the
4.4× throughput is a *software* ratio that a real network largely absorbs
(~70 µs versus ~16 µs per call, against a LAN round trip of 200–1000 µs).

**HTTP remains the default** and must — `@sigx/actors/cluster` stays zero-dep
and WinterCG-clean so Workers keep working.

## Mounting: why `attachHostUpgrade` exists

`ActorRoute.handle` returns a `Response` and cannot express a Node WebSocket
upgrade — that needs the raw socket. So this is the one piece the plugin's
route seam cannot carry, and the package deliberately **does not own your
server**:

```ts
const detach = await ws.attach(server);
// or, with an explicit instance: attachHostUpgrade(server, { transport })
```

`cluster()` builds the transport from the factory internally, so `attach()`
resolves that instance for you rather than making you capture it. It registers
`server.on('upgrade')`, matches the configured path (default
`/_sigx/host-ws`), completes the handshake with `ws` in `noServer` mode, and
hands the socket to the shared frame layer. Upgrades on other paths are left
alone, so your own WebSocket endpoints keep working. Returns a detach function.

## Options

| option | default | |
|---|---|---|
| `advertiseUrl()` | — | **Required.** The URL peers dial. A function, because the port is often not known until the HTTP listener has bound, and the address must be produced before the membership join. |
| `path` | `/_sigx/host-ws` | Path the upgrade handler matches. |
| `connect(url)` | global `WebSocket` | Client factory. Supply `ws`'s implementation on runtimes without a global. |
| `maxFrameBytes` | 8 MiB | Frames larger than this are refused before being processed. |
| `credit` | 32 | Stream chunks a consumer accepts before it must extend credit. |
| `keepAliveMs` | 15 000 | Idle PING interval. `0` disables. |

## Notes

Frames are sent as **binary** messages using the shared header **minus** the
u32 length prefix — a WebSocket message already carries its own length, so
there is nothing to reassemble. That is the only difference from the TCP
transport; multiplexing, per-stream cancellation, credit-based backpressure,
the handshake and error mapping are all the shared code path.

A WebSocket exposes no synchronous backpressure signal, so the per-stream
credit window is the entire flow-control story here — which is why credit is
applied at the producing generator rather than at a buffer.

`ws` is a **peer dependency** rather than hand-rolled RFC 6455: masking,
fragmentation, close codes and deflate negotiation are exactly the parts that
go wrong, and `ws` is already present in most Node deployments.

## Conformance

Runs the shared transport conformance suite
(`@sigx/actors/cluster/testing`) — all 18 cases, identical to the TCP run,
including the link-hygiene cases HTTP skips.
