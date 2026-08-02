# `@sigx/actors-tcp`

Framed **TCP transport** for [`@sigx/actors`](https://www.npmjs.com/package/@sigx/actors):
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
    secret: process.env.HOST_SECRET,
    // A CHAIN: TCP wherever the peer advertises it, HTTP everywhere else.
    // That is what makes a rolling deploy of this transport possible.
    transport: [tcpTransport({ port: 11111 }), httpTransport()]
});
```

## Why you would use it

**On Node, this is the recommended transport.** Measured against a *tuned*
HTTP baseline (pool bounded to the concurrency) at concurrency 64 —
`benchmarks/BASELINES.md`:

| | connections per peer | ops/s | p99 |
|---|---:|---:|---:|
| tuned HTTP | 64 | 14 287 | 9.6 ms |
| **TCP** | **1** | **69 768** | **1.58 ms** |

Two separate wins, and they are worth different amounts:

**Socket count — real at any network.** HTTP's pool sizes to
`concurrency × peers` (measured at *two* connections per in-flight request, so
~12 600 per host at c=64 across 99 peers). That is file descriptors, kernel
buffers, conntrack entries and a connection burst on every peer restart. One
connection per peer does not change with RTT.

**Throughput — real, but mostly a loopback effect.** The 4.9× is a *software*
ratio: ~70 µs per call versus ~14 µs. On a LAN with a 200–1000 µs round trip
that difference is worth roughly 1.1×, not 4.9×. Take the socket property as
the reason to choose this; treat the throughput as a bonus that shrinks the
further apart your hosts are.

> An earlier version of this README said this transport was "not about
> latency". That was wrong. Per-call HMAC really is worth only 1.19× over a
> socket — but Node's HTTP *stack* is a separate and much larger cost, and a
> framed protocol on a persistent socket skips it. The Tier-2 rig caught it.

**HTTP remains the default**, and must: `@sigx/actors/cluster` stays zero-dep
and WinterCG-clean so Cloudflare Workers keep working, and HTTP is the only
transport that runs everywhere. With a bounded pool it is a perfectly
reasonable choice.

## Deploying it

`HostDescriptor.addresses` carries a `tcp` entry per host, so a mixed cluster
is expressible and the rollout is safe:

1. Deploy with `transport: [tcpTransport(), httpTransport()]` everywhere.
2. Hosts that have the new build advertise `tcp` and use it with each other;
   hosts that do not are still reached over HTTP.
3. Once every host advertises `tcp`, drop `httpTransport()` from the chain if
   you want the internal HTTP mount gone entirely.

Step 3 is optional and has a consequence worth knowing: with no HTTP transport
in the chain there is **no internal `/_sigx/host` mount at all** — a smaller
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
  lexicographically smaller `hostId` is the designated dialer and its
  outbound connection wins, so exactly one survives.

Authentication is the same per-call HMAC the HTTP transport uses, over the
same shared `secret`. Transport encryption is an operator concern — run mTLS
or a private network between hosts.

## Conformance

This package runs the shared transport conformance suite
(`@sigx/actors/cluster/testing`), which was written against `httpTransport()`
*before* this transport existed — so it describes the contract rather than
this implementation's habits. All 18 cases pass, including the two link-hygiene
cases that HTTP skips because it holds no connections.

## Requirements

Node (or Bun/Deno) — it imports `node:net`. Not WinterCG-clean, which is
precisely why it is a separate package: `@sigx/actors/cluster` stays zero-dep
so Cloudflare Workers keep working, and HTTP stays the default.
