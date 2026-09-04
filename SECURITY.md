# Security Policy

## Reporting a vulnerability

**Please do not open a public GitHub issue for security problems.**

Use one of the following private channels:

1. **GitHub Security Advisories** — preferred. Open a private report at
   <https://github.com/signalxjs/actors/security/advisories/new>.
2. **Email** — contact the maintainer directly: **Andreas Ekdahl**
   <andy@ekdahls.net>.

Please include:

- A description of the issue and its impact.
- Steps to reproduce, ideally a minimal proof of concept.
- Affected package(s) and version(s).
- Any suggested mitigation, if you have one.

## Response

- We aim to acknowledge new reports within a few business days.
- Once a fix is ready, a patched version will be published to npm and a
  security advisory will be posted on GitHub crediting the reporter
  (unless they prefer to remain anonymous).

## Supported versions

Security fixes are applied to the latest released minor line.

---

# Threat model and hardening

`@sigx/actors` exposes server objects to a network. This section states what
the runtime guarantees, what it explicitly does not, and what you have to
configure. Read it before your first production deploy — several items below
are things no default can decide for you.

## The four surfaces

A host can expose four things, and they have four different postures. Confusing
them is the most likely way to get this wrong.

| Mount | Authentication | Guards | Exposure |
|---|---|---|---|
| **Public actor endpoint** (`/_sigx/actor`) | none of its own — same-origin policy + your guards | **yes**, `use:` / `methodUse:` | intended for untrusted clients |
| **Internal host-to-host** (`/_sigx/host`) | shared-secret HMAC per request | **no — none at all** | peers only |
| **Ops / metrics** (`/_sigx/ops`, Prometheus) | bearer token, mandatory outside dev | n/a | operators only |
| **Health** (`/_sigx/health`) | none, by necessity | n/a | load balancers |

**The internal mount runs no guards by design** — the public edge is supposed
to have run them before the call was forwarded. It is contributed as an
ordinary route, so it lands on *the same listener* as your public endpoint
unless you deliberately separate them. That is why `cluster()` refuses to
construct without a `secret` outside development: without one, every registered
type, key and method is reachable by anyone who can address that listener, with
your guard chains bypassed entirely.

Pass `cluster({ secret: null })` only when the mount is genuinely unreachable
by an untrusted caller — an mTLS-terminated mesh, a private pod network. If you
can, run the internal mount on a **separate listener** bound to a private
interface; the AKS-style deployment shape (public ingress for the actor and SSR
surface, an internal-only port for health, ops and host-to-host) is the one to
copy.

## The actor key is untrusted client input

This is the single most important thing to internalise.

The actor key is `args[0]` in the request body. It is validated as a non-empty
string and nothing more. **Any client can address any registered type and any
key** — that is the virtual-actor model working exactly as intended, the same
way Orleans grain ids work.

`actorRouteToken()` does **not** change this. It is a load-balancer affinity
hint, and the endpoint **never validates it** — it cannot, in general, because
`'key'` mode and custom token functions produce values the server has no way to
recompute. It reads like a capability and is not one.

**Know what a guard can and cannot see.** A guard is called as
`guard(rq, info)` — `rq` is the `ServerFnContext` (headers, cookies,
`rq.locals`) and `info` is `{ symbol, name }`. **The actor key is not passed to
guards.** It is `args[0]`, read by the endpoint after the guard chain has
already run. So a guard can answer *"is this caller authenticated, and may they
call this method at all?"* — it cannot answer *"may they call it on **this**
actor?"*

That second question has to be answered somewhere that can see the key. Two
patterns work:

**1. Front per-user actors with a serverFn that derives the key from the
session** (the recommended default; `perf/app/src/chat.server.ts` is a working
instance). The client never names the actor:

```ts
export const postMessage = serverFn({
    // No `use:` — the app's default policy already requires an authenticated
    // caller, and `authenticate` has already resolved them.
    handler: async (rq, input: { room: string; text: string }) => {
        // From the session, not the client. `requirePrincipal` throws 401
        // rather than returning a nullable, so `from` cannot be forged and
        // cannot silently be `undefined`.
        const { name: from } = await requirePrincipal<User>(rq);
        return actor(RoomActor, input.room).post(from, input.text);
    }
});
```

**2. Check ownership inside the method, using `ctx.key`**, when the actor must
stay directly addressable. The actor context does expose the key, so the method
can compare it against `ctx.principal` — identity rides its own envelope slot
and propagates across hops, so there is nothing to remember to stamp.

Never treat the key itself as proof of ownership, and never assume a guard on a
directly-addressable per-user actor is sufficient — by construction it did not
see which actor it was guarding. An actor marked `unguarded: true` is literally
reachable, on any key, by anyone who can send a same-origin-shaped POST.

The build-time gate (`sigxActors({ requireGuards })`, on by default) turns a
missing guard decision into a build error — but it only sees first-party source
and only applies if you use the Vite plugin. A plain `createHost` +
`createActorHandler` deployment gets a startup warning instead, and nothing
more.

## Routing tokens are hashed for log hygiene, not for privacy

`hashRouteToken()` is an **unkeyed FNV-1a** of the actor id, and it appears in
URL path segments and therefore in access logs. Quoting the source, because it
is exactly right:

> LOG HYGIENE, not privacy: an unkeyed hash of an email is one dictionary
> lookup from plaintext.

If your actor keys are personal data, do not rely on the hash to protect them.

## Cluster HMAC authenticates the peer, not the payload

The signed message is `proto \n symbol \n callId \n timestamp`. **The request
body is not covered.** Symbol-binding prevents method substitution, but since
the actor key travels in the body, an attacker positioned on the host-to-host
network can take a legitimately signed `Cart#addItem` and retarget it at a
*different cart*, or alter its arguments.

Treat the cluster HMAC as peer authentication only. **The host-to-host network
must be trusted or mTLS-terminated**; it is not a substitute for one.

Related: there is a **5-minute freshness window and no nonce store**, so a
captured request replays freely inside it. Narrow the window if your clocks are
tightly synchronised.

## Resource limits you must set yourself

The runtime caps what it can, but two of the important ones default to
unlimited because only you know the right number:

- **`maxActivations` defaults to `0` (unlimited).** Since keys are
  attacker-chosen, an unauthenticated caller reaching an `unguarded` actor can
  mint unbounded distinct activations, each pinned for `idleAfterMs` (20 min by
  default). **Set this in production.** The LRU shed is opt-in and rides this
  setting.
- **Turn queues are unbounded.** A single hot key can be flooded, and every
  queued turn holds its arguments alive. Depth is *observable* via `ops()` and
  `activations({ sortBy: 'queued' })`, but not enforced.

Bounded for you, with no configuration: request body (1 MiB), URL (8 KiB),
frame size (8 MiB, checked before a single payload byte is buffered), `$live`
subscriptions per connection (256), redirect hops, `$live` frame queue depth,
and metrics cardinality.

## A CPU-bound turn can fence its own host

This concerns clustered Node deployments — the hosts that run a membership
provider (`redisCluster`, `pgCluster`, `surrealCluster`, `k8sMembership`).
Such a host is one Node process with one JS thread, so actor turns, health
probes and the cluster **membership heartbeat all share one event loop**.
(The Cloudflare backend has no membership heartbeat to starve.) A turn
that computes synchronously past the membership TTL (`redisCluster`
defaults: `heartbeatMs: 5000`, `ttlMs: 15000`) starves the host's own
liveness signal. While the loop is held, peers age the host out and release
its directory claims; when the loop resumes, the host sees that its beat
landed past the TTL, cannot prove its own membership, and
[self-fences](docs/architecture/clustering.md#self-fencing) — fatal by
design, because a survivor may already be serving its actors. (The
pre-fencing behaviour — evicted yet still serving — was measured on a live
cluster and closed as #45; the fence is what makes the stall *loud* instead
of a split-brain.)

So a method that blocks long enough does not merely go slow: **it removes
its host from the cluster, and takes every other actor on that host down
with it.** Treat any method whose CPU cost scales with client input as a
denial-of-service primitive — a guard keeps anonymous callers out, but it
does not stop a signed-in one from wedging the host — and cap the work a
single call may ask for.

Mitigations, in the order to reach for them:

- **Chunk the work and yield between slices** — every few hundred
  iterations, `await new Promise((resolve) => setImmediate(resolve))`. The
  yield is the load-bearing detail: it is what lets the heartbeat, the
  probes and every other actor's turns run between slices.
  `perf/app/src/digest.actor.ts` is the worked example.
- **Move long work off the request turn with `ctx.tasks`**, so the caller is
  not held while it runs. A detached task still runs on the same loop,
  though — it must chunk and yield too.
- **Move genuinely heavy compute onto `worker_threads`**
  (`perf/app/src/digest-pool.ts`). A `defineWorker` pool does *not* do this:
  it multiplies activations — concurrency at `await` points — not threads,
  so a pool of synchronous hashers blocks exactly like one.

Development flags offenders: a turn that holds the loop longer than
`slowTurnMs` (default 5 s) gets a `__DEV__` console warning naming the actor
and method.

Packing several hosts on one machine — the way a multi-core node is used,
see ["One host per core"](docs/architecture/clustering.md#one-host-per-core)
— does not widen this surface: each host has its own loop and starves only
its own heartbeat. It adds one operational rule instead: size CPU requests
so the hosts cannot oversubscribe the cores, because a throttled host lands
its beat late without any single turn being slow.

## `reads.public` and shared caches

`reads: { method: { maxAge, public: true } }` emits `public, s-maxage=…` and
**drops `Vary: Cookie`**. A CDN will then serve one user's response to another.
Use it only for reads whose output depends solely on the actor key and
arguments — never for a per-user actor. Development warns when a guarded actor
is marked public; production does not.

## Transports

`@sigx/actors-tcp` carries host-to-host traffic on a
private network. `tcpTransport()` **binds all interfaces unless you set `host`,
and speaks no TLS.** It bounds what an unauthenticated connection can cost
(`handshakeTimeoutMs`, `maxPendingInbound`), but those are damage limits, not a
reason to expose the port.

## Operational secrets

- `ops()` and `prometheusOps()` **throw at construction without a secret**
  outside development. `401` covers both missing and wrong tokens, and auth
  runs before the path split so paths cannot be enumerated.
- Prefer the **`SIGX_OPS_SECRET` environment variable** over the
  `sigx actors --secret <token>` flag: a command-line argument is visible in
  `ps` and `/proc/*/cmdline` to any local user.
- Cluster and ops secrets are compared in constant time.
- An empty cluster secret is rejected at construction — `?? ''` is a
  configuration bug, not a configuration.

## What the runtime guarantees

Stated so you can rely on them, and so a regression is a bug rather than a
surprise:

- **Actor keys never become metric labels.** Prometheus labels are type and
  method only.
- **The unauthenticated health endpoint withholds `perType`** — it reports
  liveness and readiness, not your actor inventory.
- **`ops()` caps its activation list at 20 by default** specifically because
  that list carries actor keys.
- **Error samples carry the message only** — never arguments or state. A
  failing call's arguments are exactly where the secrets are.
- **421 redirects publish `publicAddress`, never the internal address.** Pod
  IPs are not disclosed to clients.
- **Public-mount errors are masked; peer errors are not.** The split is
  deliberate.
- **`$sigx:*` symbols are refused on the public mount**, answering identically
  to an unknown method so the reserved namespace cannot be probed.
- **Wire JSON is parsed with a prototype-pollution guard** on every wire-facing
  path.
- **`fileStorage()` percent-encodes both segments**, so an actor key cannot
  traverse out of its directory. It remains a development store regardless.

## A minimal production checklist

1. `cluster({ secret })` from the environment — and prefer a separate internal
   listener.
2. `ops({ secret })` from the environment.
3. Guards on every actor; `unguarded: true` only where you mean it, and never
   on anything that reads per-user state.
4. Per-user actors reached through a serverFn that derives the key from the
   session — or an in-method `ctx.key` ownership check. A guard alone cannot do
   this; it never sees the key.
5. `maxActivations` set to something your host can hold.
6. Host-to-host traffic on a private network or mTLS.
7. `reads.public` audited — no per-user actor marked public.
8. TLS terminated in front of the public mount.
