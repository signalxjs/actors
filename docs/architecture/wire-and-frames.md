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
| `internal: true` types | **refused as unregistered** (404) | served |
| Exists when | always | only if a transport declares a route |

The public endpoint refuses every `$sigx:`-prefixed method. Those are the
runtime's own deliveries — reminders, topic fan-out, `$sigx:host#stats` — and
they are legitimate only over the authenticated internal mount.

It also refuses every **`internal: true`** type (#74), and the refusal is the
unknown-type answer verbatim — same 404, same kind, same message — so a probe
cannot tell a hidden type from a missing one. The check is `isInternalActor`
in `guards.ts`, asked right after the definition lookup by each public entry
point that resolves a type on its own: `createActorResolver` (unary and GET
reads), `subscribeAll` (the `$live` mount resolves *before* the resolver's
definition lookup, so it repays the check per subscription) and the socket
session (calls and subscriptions). `resolveHostSymbol` never asks it, which is
what keeps the internal mount, `ctx.actor()` hops and remote reminder delivery
working. The build's half is in `vite/extract.ts`: an internal actor stays in
the extraction (the server registry is built from it) but its client export is
a `__serverOnly` stand-in, not an `__actorRef`.

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

Two kinds of call carry no token in any mode. `$`-prefixed mounts
(`$live#subscribe` fans one response out to many actors, so there is no single
owner), and **`defineWorker` types**: a worker is placed on whichever host the
call lands on, so a token would pin every call for one key to one pod and hand
back the fleet-wide spread that is the pool's whole point (#148 — measured as
`pool_spread` 3.03 → 1.12 on three hosts). The build stamps the worker flag
onto the client stub; a transport reads it from `ActorCallInit.worker`, and the
proxy strips it from `.with()` so a caller cannot drop a stateful actor's token.

The two differ at the edge, though. No token only restores the spread on a
load balancer that **falls back when the hash key is empty** — nginx core
`hash`, HAProxy `balance hdr(...)` and Envoy's ring hash do; ingress-nginx's Lua
consistent hash does not, it hashes `""` and pins every untokened request onto
whichever one pod that lands on. `$live` is safe on the k8s chart because it is
carved out of the hashed Ingress *by path*; a worker call has no distinguishing
path (`/_sigx/actor/Digest/summarize`), so a deployment hashing on the header
must either give the hash an empty-key fallback or carve its worker types out
by path as well. The k8s chart takes the fallback route (#342): it hashes
`$sigx_hash`, an nginx `map` that substitutes the per-request `$request_id`
for an empty `x-sigx-actor-route` and passes a real token through untouched.
A `map` is `http`-context config, which a chart cannot carry — it is the
controller ConfigMap's `http-snippet`, one step of the AKS RUNBOOK's §0 — and
that makes the step load-bearing rather than optional: a variable no `map`
defines is nil in the Lua balancer, which then hashes `""` for EVERY request,
tokened or not, and the whole actor path pins to one pod (read off the
balancer's code; not yet observed on a cluster). `infra.test.ts` asserts the
seam three ways — one token pins, distinct tokens spread, untokened calls
spread — and `testenv.mjs up` refuses to release the chart onto a controller
without the map.

It is a hash, not the key — the same `hashRouteToken(type, key)` that
`@sigx/actors-otel` puts on spans, so spans join to routing tokens in access
logs. Treat that as log hygiene rather than privacy: an unkeyed hash of an
email is one dictionary lookup from plaintext.

## `@sigx/actors/cluster/frames`

The wire shared by every connection-oriented host transport. **It is a
published subpath on purpose** — `wire-shared.ts` is internal and an
out-of-repo package cannot reach it, so `@sigx/actors-tcp` imports the codec
from here, and an out-of-repo transport can too. One implementation beats a
copy per transport that drifts.

```
0  u32 length   bytes AFTER this field (>= 8)
4  u8  type
5  u8  flags    bit0 = stream call
6  u16 status   HTTP-shaped code; 0 except on ERROR/GOAWAY
8  u32 corrId
12 ..  payload  UTF-8 JSON, through the wire codec
```

Big-endian, because it reads correctly in a hexdump and `readUInt32BE` costs
nothing extra. A message-oriented link (`messageOriented: true` on
`FrameLink`, e.g. a WebSocket) reuses the layout **minus the u32 length**,
since the message already carries its own. That `FrameLink` difference is the
*only* per-transport difference — multiplexing, cancellation, credit and
error mapping are shared code. (A host-to-host WebSocket transport built
exactly this way existed and was retired — #151.)

Two fields that look like incidental detail and are not:

- **`status` is a header field deliberately.** `clusterStats` classifies peer
  failures by numeric status (403 → `unauthorized`, 404 → `unsupported`), so a
  transport that drops the number degrades every auth failure to a bare
  `'error'` in the ops surface.
- **The payload stays JSON.** `wire/endpoint-roundtrip` already measures
  ~115 k ops/s with the codec and JSON and no socket at all, so a second
  serialization format buys very little while forking the vocabulary
  `wire-shared.ts` pins — including its prototype-pollution reviver.

## The client socket upgrade surface

`@sigx/actors/socket-wire` is the third wire — a browser's WebSocket into
`createActorSocketSession` — and deliberately **not** `cluster/frames`: text
JSON, no principal field, no envelope, no inbound-call direction. The session
core is WinterCG-clean (`Request` in, `send`/`close` callbacks out), so the
per-runtime difference is only who answers the upgrade:

- **Node** cannot answer an upgrade with a `Response`, which is why
  `attachActorSocket()` (`@sigx/actors-ws/node`) exists: it owns the
  `'upgrade'` event, buffers pre-session frames, and builds the WinterCG
  `Request` by hand.
- **Workers** answer an upgrade WITH a `Response` (`status: 101, webSocket`),
  so `workerSocket()` in `@sigx/actors-cloudflare` is an ordinary
  plugin-contributed route — no new seam, no pre-session buffer (the client
  end only exists inside the returned Response, so no frame can race
  construction), and a refused session answers with an honest HTTP status.
  Bun and Deno are the same shape with `server.upgrade` /
  `Deno.upgradeWebSocket` swapped in.

On Cloudflare the socket terminates in one of two places, and the difference
is the whole story:

- **Worker-terminated** (`workerSocket()`, exact path `{path}`) — one
  multiplexed socket per client, every call fanned out through placement.
  It changes **where the socket ends**, not where calls run: every call
  still crosses Worker→object over `stub.fetch`, whose abort signal is
  swallowed at the boundary (#47), so a departed live consumer leaves
  `keptAlive` set in the objects it watched.
- **Object-terminated** (`createHostDurableObject({ socket })` behind a
  forwarding route, path `{path}/{type}/{key}` — same prefix, disambiguated
  by arity, so both modes coexist) — one socket per actor, accepted with the
  hibernation API inside the object that owns it. Teardown is local, so a
  disconnect actually releases the activation; an idle page is free rather
  than resident-and-billable. The costs, stated plainly: a page watching N
  actors holds N sockets, and an evicted isolate loses the session — the
  first message after a cold wake closes `1012` and the client redials with
  current cookies and re-seeds, the same contract as any drop.
  `maxConnectionMs` survives eviction as a per-message-checked deadline in
  the socket attachment; the object's single alarm stays with reminders.
  The forwarding route (`objectSocketRoute`) is a public entry point that
  resolves the type **itself**, ahead of any session, so it repays the
  `internal: true` check (#74) at the upgrade: a flagged type gets the
  unknown-type 404 and never mints a Durable Object — a 101 there, where a
  missing type 404s, would tell a probe the type exists. The
  Worker-terminated mode needs no such line; it only wraps
  `createActorSocketSession`, which checks per frame.

The object-terminated mode fixes #47 only for the object's **own** actor's
watches — a session's watches on other actors go object→object over the
same swallowed-signal stub boundary, and the HTTP-stream shape leaks as
before, which is why #47 stays open.

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
