# @sigx/actors-ws

**Client-facing WebSocket transport** for [`@sigx/actors`](https://sigx.dev/actors):
browsers calling actors over **one multiplexed socket** — unary calls, streams,
cancellation, and (with the live layer) incremental subscriptions without the
`$live` reopen-and-reseed cost.

```sh
npm install @sigx/actors-ws @sigx/actors
```

```ts
import { socketTransport } from '@sigx/actors-ws/client';
import { configureActors } from '@sigx/actors/client';

configureActors(socketTransport({ url: 'wss://example.app/socket' }));
```

The server half is `createActorSocketSession()` on `@sigx/actors/server` — a
`Request` in, `send`/`close` callbacks out, so `ws`, socket.io, uWS, Bun, Deno
and Cloudflare all drive the same core. The wire vocabulary is published at
`@sigx/actors/socket-wire`. On Node, `attachActorSocket()` from
`@sigx/actors-ws/node` hooks the session into an HTTP server's upgrade event
(`ws` is an optional peer, needed only there); if you already run a WebSocket
server, construct the session in your own upgrade handler instead.

Reach for the socket on live-heavy pages and cross-origin apps; `fetchTransport()`
stays the default and stays correct. Per-call latency on a warm same-origin
HTTP/2 connection is **not** the win — and a socket drops on every network
change, so in-flight calls fail (and are never retried) when it does.

Host-to-host socket traffic is a different product with the opposite trust
model: [`@sigx/actors-tcp`](https://www.npmjs.com/package/@sigx/actors-tcp).

Docs: <https://sigx.dev/actors>.
