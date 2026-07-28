# `@sigx/actors-tcp`

Orleans-style **TCP transport** for [`@sigx/actors`](https://www.npmjs.com/package/@sigx/actors):
one multiplexed, framed connection per peer instead of one HTTP connection per
in-flight request.

```sh
pnpm add @sigx/actors-tcp
```

```ts
import { cluster, httpTransport } from '@sigx/actors/cluster';
import { tcpTransport } from '@sigx/actors-tcp';

cluster({
    providers,
    advertise: 'http://10.0.4.7:7311',
    secret: process.env.SILO_SECRET,
    // A CHAIN: TCP wherever the peer advertises it, HTTP everywhere else.
    // That is what makes a rolling deploy of this transport possible.
    transport: [tcpTransport({ port: 11111 }), httpTransport()]
});
```

## Why you would use it

**Not for latency.** The measurements in `benchmarks/BASELINES.md` are blunt
about this: per-call HMAC is worth 1.19× over a real socket (not the 3.35× the
in-process figures suggested), and cross-silo throughput plateaus at Node's
HTTP stack rather than at the runtime.

**For socket count.** HTTP's connection pool sizes to `concurrency × peers` —
measured at *two* connections per in-flight request, so ~12 600 sockets per
silo at concurrency 64 across 99 peers. That is file descriptors, kernel
buffers, conntrack entries, and a connection burst every time a peer restarts.
This transport holds **one connection per peer**, whatever the concurrency.

If your cluster is small or your concurrency is low, bounding the HTTP pool
(see the recipe in the main README) is simpler and gets you most of the way.
Reach for this when the connection count is the thing that hurts.

## Deploying it

`SiloDescriptor.addresses` carries a `tcp` entry per silo, so a mixed cluster
is expressible and the rollout is safe:

1. Deploy with `transport: [tcpTransport(), httpTransport()]` everywhere.
2. Silos that have the new build advertise `tcp` and use it with each other;
   silos that do not are still reached over HTTP.
3. Once every silo advertises `tcp`, drop `httpTransport()` from the chain if
   you want the internal HTTP mount gone entirely.

Step 3 is optional and has a consequence worth knowing: with no HTTP transport
in the chain there is **no internal `/_sigx/silo` mount at all** — a smaller
attack surface, but nothing to `curl`. The public actor wire is unaffected.

## Options

| option | default | |
|---|---|---|
| `port` | `0` | Listen port. `0` binds an ephemeral port, which is then what gets advertised. |
| `host` | all interfaces | Bind address. |
| `advertiseHost` | `127.0.0.1` | Host peers should dial. Set this on a multi-homed box. |
| `maxFrameBytes` | 8 MiB | Frames larger than this are refused **before** any payload is buffered. |
| `credit` | 32 | Stream chunks a consumer accepts before it must extend credit. |
| `keepAliveMs` | 15 000 | Idle PING interval. `0` disables. |

## How it works

Frames are `@sigx/actors/cluster/frames` — a 12-byte big-endian header
(`length`, `type`, `flags`, `status`, `corrId`) then a JSON payload through the
same wire codec the HTTP transport uses, so registered type handlers and the
prototype-pollution reviver apply identically.

Four things are load-bearing:

- **Cancellation is a frame, not a socket close.** With many streams
  multiplexed on one connection, closing it would cancel everything. A
  `CANCEL` frame targets one stream, and on receipt the callee both aborts
  *and* calls `generator.return()` — an async generator parked at `yield`
  never runs its `finally` from a signal alone.
- **Backpressure is applied at the generator.** Credit is checked *before*
  `next()` is pulled, so a slow consumer stops the producer rather than
  filling a buffer behind it.
- **A dropped connection fails its in-flight calls as `unreachable`, and
  never retries them.** The placement already evicts, refreshes and
  re-resolves; silently re-sending a non-idempotent actor method to a host
  that may no longer own the actor would be a correctness bug.
- **Simultaneous dial** is settled without an extra round trip: the
  lexicographically smaller `siloId` is the designated dialer and its
  outbound connection wins, so exactly one survives.

Authentication is the same per-call HMAC the HTTP transport uses, over the
same shared `secret`. Transport encryption is an operator concern — run mTLS
or a private network between silos.

## Conformance

This package runs the shared transport conformance suite
(`@sigx/actors/cluster/testing`), which was written against `httpTransport()`
*before* this transport existed — so it describes the contract rather than
this implementation's habits. All 16 cases pass, including the two link-hygiene
cases that HTTP skips because it holds no connections.

## Requirements

Node (or Bun/Deno) — it imports `node:net`. Not WinterCG-clean, which is
precisely why it is a separate package: `@sigx/actors/cluster` stays zero-dep
so Cloudflare Workers keep working, and HTTP stays the default.
