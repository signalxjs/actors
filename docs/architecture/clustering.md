# Clustering

How hosts find each other, agree on who owns an actor, and hand off cleanly.

User-facing versions: [clustering](https://sigx.dev/actors/docs/clustering/),
[placement policies](https://sigx.dev/actors/docs/placement-policies/),
[locality routing](https://sigx.dev/actors/docs/locality-routing/),
[rebalancing](https://sigx.dev/actors/docs/rebalancing/),
[Kubernetes](https://sigx.dev/actors/docs/kubernetes/).

## Two independent seams

**Membership** (who is alive) and the **directory** (who owns which actor) are
separate on purpose. `@sigx/actors-k8s` provides membership from
`coordination.k8s.io` Leases while the directory stays store-backed; a provider
package may implement either or both. `redisCluster()`, `pgCluster()` and
`surrealCluster()` are just bundles of the two.

## What is allowed to be wrong

**Everything here, transiently.** Membership views diverge, directory entries
go stale, a routing token points at yesterday's host. All of it is safe because
the [storage etag CAS](runtime-internals.md#persistence-and-the-conflict-path)
is the integrity floor: the worst outcome of a wrong answer is a rejected save
and a reload.

Hold onto that when reviewing changes here. A proposal that makes the directory
authoritative over storage, or that retries a non-idempotent dispatch to
"recover" from a stale route, is trading a cheap wrong answer for an expensive
one.

## Self-fencing

Every membership provider implements the same rule, and it is subtler than
"heartbeat failed".

A host fences — stops claiming actors, deactivates what it holds, withdraws —
when it cannot **prove** its own membership. That covers two cases:

- the beat *failed* past the TTL; and
- the beat merely *landed* past the TTL.

The second is the one that gets missed. A stalled event loop resumes and writes
successfully, so nothing errors — but the record expired while it was away,
peers dropped the host and released its claims, and a survivor may already be
serving those actors. Without the gap check the host looks healthy again and
two hosts believe they own the same actor.

TTL expiry is judged on the **database** clock wherever there is one
(`pgMembership`, `surrealMembership`), so host clock skew cannot fake a death
or a survival. The Kubernetes provider compares `renewTime` across hosts and
therefore assumes NTP-synced nodes, with `clockSkewMs` as the slack — the same
assumption kubelet node Leases make.

`k8sMembership` adds a third fencing case: a renewal that returns **404**. The
Lease *is* the membership token, so if something deleted it, peers have already
aged the host out. That case is deliberately not self-healing — recreating the
Lease would re-advertise a host whose actors a survivor may be serving — so the
host fences and the pod restart mints a fresh identity.

## Change detection

Providers compare **host signatures**, not just a version counter. A host that
dies silently expires on the store's clock without anyone bumping a version, so
a version-only comparison never converges. Push (pub/sub, LISTEN/NOTIFY, live
query, watch) is **best-effort in every provider**; the poll is the guarantee
and `pollMs` is the propagation bound.

## Placement resolution

`clusterPlacement` resolves a ref to a dispatcher by consulting the directory,
then the transport chain. Two things about its configuration:

- **It takes no `definition` option.** Per-type strategies are read from the
  bound host at call time (`host.definition(type)`), which is why
  `cluster()` registers the placement with a factory that ignores the setup
  context entirely: `registry.setPlacement(() => placement)`.
- **`setPlacement` is exclusive** — a second claim throws naming both plugins,
  which is what stops a Cloudflare object from being configured such that it
  can fetch itself.

The transport chain is tried in order, and `dispatcherFor(target)` returning
`null` means "this transport cannot reach that peer", not "failure" — that is
what makes a rolling deploy of a new transport safe, since hosts on the old
build simply keep being reached over HTTP.

## Registration-aware placement

A host is only ever **chosen to host** an actor type its app registers (#212).
The descriptor publishes the registered type names — `HostDescriptor.types`, a
typed field for the `publicAddress` reason (placement filters on it
default-deny, so it must not be indistinguishable from a free-form `meta`
label) — and eligibility is enforced at every decision point of
`#resolveTarget`, because the policy is not the only place a target comes from:

- **Route cache**: a cached hint naming a host that does not register the type
  is dropped, the existing dead-member treatment.
- **Directory**: a live owner that does not register the type is evicted and
  the actor re-placed — descriptors are immutable per incarnation (hostIds are
  minted per start), so such an entry is stale or poisoned and can only ever
  bounce callers.
- **Policy**: `choose()` is handed a view **narrowed** to the hosts registering
  `ref.type`, so every custom policy is registration-safe without doing
  anything. The narrowed view preserves object identity when nothing was
  filtered (homogeneous clusters pay nothing), `self` may not be in it (a host
  can place types it does not register — which is also why `preferLocalPolicy`
  falls through to rendezvous over the eligible view instead of answering
  `self` blindly), and a policy answering a host outside it — other than
  `self`, which means "local" and is guarded authoritatively there by the
  fence, the claim and the registry — fails the dispatch loudly, naming the
  policy.

An **empty** eligible set throws the branded `unplaceable` error rather than
silently widening to the full view — silently widening is how a type lands on
a host that never registered it. It is retried through the routing loop against
a refreshed view, because the one pod registering a type being mid-join *is* a
rolling deploy.

The receiving side enforces the same rule a second time: an inbound cluster
call for a type this host does not register answers **`wrong-host` with no
owner hint** (via `resolveClusterSymbol`, which resolves well-formed symbols
for unknown types precisely so the refusal can carry a kind), instead of the
old unbranded 404. A 404 is terminal to the caller; `wrong-host` makes it evict
its route and re-place — which is also what an **older** sender, with no
eligibility filtering of its own, already does with that kind. The Durable
Objects runtime keeps the 404 (`hostEndpointRuntime` — no cluster, nowhere to
re-place).

Mixed-version rule: a descriptor **without** `types` is an older build and
reads as "registers everything" — the legacy behavior and the only safe
direction, since absent-means-ineligible would empty every view mid-rolling-
deploy. Consequently the fix for "a rolling deploy places a brand-new type on
an old pod" is partial until the fleet republishes: old pods stay eligible for
the new type, but a new receiving pod refuses with `wrong-host` so the caller
re-places instead of caching a poisoned route.

Two deliberate non-changes: `ownsReminderShard` stays rendezvous over the FULL
active view (shards are host-level, not typed), and fencing semantics are
unchanged — for a volatile (`memoryStorage`) keyed actor the directory's
single-activation invariant, which fencing defends, is the *only* single-writer
guarantee there is, since volatile storage has no cross-host CAS floor. A host
registering only workers has nothing to fence and keeps serving them, which the
worker-cluster suite pins.

## Shutdown ordering

The sequence is deliberately not the obvious one:

1. **`onStopBegin()`** — start answering `connection: close`. This is what
   actually drains client pools, one response at a time, interrupting nothing.
2. **`host.stop()`** — the actor drain. The placement announces `leaving`
   *before* the drain begins so peers stop placing new actors here. Pooled
   connections keep flowing; peers whose dials are refused see `unreachable`,
   which is retryable by design.
3. **`server.close()` + `closeAllConnections()`** — **last**.

Closing the listener first looks more decisive and is worse: on Node ≥ 19
`close()` also destroys idle connections, and "idle" from the server's side
includes a socket the client is at that instant writing its next request onto.
That produces exactly the reset the sequence exists to prevent.

Why it matters in numbers: an orchestrator's preStop sleep and readiness-503
steer *new* connections away, but established connections survive endpoint
removal (conntrack) and ride into the exiting pod. On a real cluster that
measured as 122 lost calls out of ~1.7M on a rolling restart — all
connection-level, none visible from the actor layer, which reported a clean
hand-off.

Start-up is the mirror image: **listen before `app.start()`**, because
`start()` joins membership and from that moment peers may place actors here.

## Cloudflare is the degenerate case

`@sigx/actors-cloudflare` needs no membership, no directory and no HMAC —
the platform already guarantees one instance per object. But it **does** use
the internal mount: the Worker→object hop is `httpTransport()` with its `fetch`
swapped for a stub call, so envelope, NDJSON, deadlines and branded errors are
the runtime's own rather than a second implementation.

Two traps recorded there because both are silent:

- **The placement runs on both sides, with an `isSelf` predicate.** Giving the
  object's own host a plain local placement instead activates a callee *inside*
  the caller's object and writes its state into the wrong object's storage.
  Break `isSelf` and the suite OOMs — the object fetches itself forever.
- **A DO stub is never cached.** It is an I/O object bound to the request that
  created it, so reusing one across requests makes every later call
  "unreachable".
