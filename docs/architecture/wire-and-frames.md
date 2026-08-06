# The wire, the mounts, and the frame codec

What is actually on the network, and which surface answers it.

User-facing versions: [wire protocol](https://sigx.dev/actors/docs/wire-protocol/),
[locality routing](https://sigx.dev/actors/docs/locality-routing/),
[transports](https://sigx.dev/actors/docs/transports/). The trust boundaries are
in [`SECURITY.md`](../../SECURITY.md) and that file, not this one, is the
authority on posture.

## Two mounts, and they are not alike

| | Public actor endpoint | Internal host-to-host |
|---|---|---|
| Who calls it | browsers, service clients | peer hosts only |
| Auth | your guards, `ctx.principal` | shared-secret HMAC per call |
| Reserved methods | **refused outright** | this is where they arrive |
| Exists when | always | only if a transport declares a route |

The public endpoint refuses every `$sigx:`-prefixed method. Those are the
runtime's own deliveries — reminders, topic fan-out, `$sigx:host#stats` — and
they are legitimate only over the authenticated internal mount.

The internal mount is **not special-cased**. `httpTransport()` declares it
through `PluginRegistry.route()` like any other plugin route, which is why a
cluster configured with only `tcpTransport()` has no internal HTTP mount at
all: nothing to `curl`, smaller attack surface, and the public wire unaffected.

## The public request

```
POST {base}/r/{token}/{Type}/{method}      {"args": [key, ...args]}
  → {"data"} | {"error"}                   (NDJSON for streams)
```

**It is the serverFn protocol verbatim** — same origin policy, body caps,
prototype-pollution guards, error masking and codec — because
`handleActorRequest` *is* `handleServerFnRequest` with a host-backed resolver.
Changes to core's wire land here for free, and divergence is a bug.

The separator is a real path separator and encoding is per segment, so a normal
call spends no `%` at all. A type may contain slashes (`acme/greeter`), so the
symbol can span more than two segments and the **last** is always the method. A
type carrying an empty, `.` or `..` segment is refused by `defineActor`,
because `new URL()` would resolve it away and silently retarget the route.

A method declaring `reads:` also answers
`GET {base}/r/{token}/{Type}/{method}?a0={key}&a1=…`, same codec, same envelope.

**The callable surface is the method table's own keys.** Inherited
`Object.prototype` members — `toString`, `constructor`, `valueOf`, `__proto__`
— are therefore not callable. The corollary that bites: a `methods:` factory
returning a **class instance** exposes nothing, because those methods live on a
prototype. Return an object literal; `__DEV__` warns if you do not.

## The routing token

`{token}` is a stable per-actor routing hint, mirrored into the
`x-sigx-actor-route` header. It exists because the actor **key** rides in the
JSON body and no load balancer will parse a body to route — without it, edge
locality decays as 1/N (measured 1.00, 0.50, 0.12, 0.02, 0.01 for N = 1, 2, 10,
50, 100).

Two properties are load-bearing:

- **It is a middle segment.** The symbol is decoded as the *last* path segment,
  so the token slots in ahead of it and the endpoint neither parses nor
  validates it. `actorRouteToken()` is read-only for the same reason.
- **Routing is never load-bearing for correctness.** A stale, wrong or absent
  token costs a network hop, never a wrong answer. Any change that makes the
  token authoritative is a bug, not an optimization.

It is a hash, not the key — the same `hashRouteToken(type, key)` that
`@sigx/actors-otel` puts on spans, so spans join to routing tokens in access
logs. Treat that as log hygiene rather than privacy: an unkeyed hash of an
email is one dictionary lookup from plaintext.

## `@sigx/actors/cluster/frames`

The wire shared by every connection-oriented host transport. **It is a
published subpath on purpose** — `wire-shared.ts` is internal and an
out-of-repo package cannot reach it, so both `@sigx/actors-tcp` and
`@sigx/actors-ws` import the codec from here. One implementation beats a copy
per transport that drifts.

```
0  u32 length   bytes AFTER this field (>= 8)
4  u8  type
5  u8  flags    bit0 = stream call
6  u16 status   HTTP-shaped code; 0 except on ERROR/GOAWAY
8  u32 corrId
12 ..  payload  UTF-8 JSON, through the wire codec
```

Big-endian, because it reads correctly in a hexdump and `readUInt32BE` costs
nothing extra. WebSocket reuses the layout **minus the u32 length**, since a WS
message already carries its own. That `FrameLink` difference is the *only*
difference between the two transports — multiplexing, cancellation, credit and
error mapping are shared code.

Two fields that look like incidental detail and are not:

- **`status` is a header field deliberately.** `clusterStats` classifies peer
  failures by numeric status (403 → `unauthorized`, 404 → `unsupported`), so a
  transport that drops the number degrades every auth failure to a bare
  `'error'` in the ops surface.
- **The payload stays JSON.** `wire/endpoint-roundtrip` already measures
  ~115 k ops/s with the codec and JSON and no socket at all, so a second
  serialization format buys very little while forking the vocabulary
  `wire-shared.ts` pins — including its prototype-pollution reviver.

### Behaviours a transport must preserve

These are asserted by [`transportConformance`](conformance-suites.md), which
was written against `httpTransport()` *before* any connection-oriented
transport existed, so it describes the contract rather than one implementation:

- **Cancellation is a frame, not a socket close.** With many streams
  multiplexed on one connection, closing it would cancel everything. On
  `CANCEL` the callee both aborts *and* calls `generator.return()` — an async
  generator parked at `yield` never runs its `finally` from a signal alone.
- **Backpressure is applied at the generator**, before `next()` is pulled, so a
  slow consumer stops the producer rather than filling a buffer behind it.
- **A dropped connection fails its in-flight calls as `unreachable` and never
  retries them.** The placement already evicts, refreshes and re-resolves;
  silently re-sending a non-idempotent actor method to a host that may no
  longer own the actor would be a correctness bug.
