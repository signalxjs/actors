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

The commonest way to hit the second case is not infrastructure — it is a
**CPU-bound actor turn**. The heartbeat is a timer on the same event loop
that runs turns, so a method that computes synchronously past the TTL
starves its host's own liveness signal and fences it, taking every other
actor on the host along. The authoring-side account — why that is a
denial-of-service surface, and the chunk-and-yield mitigation — is in
[SECURITY.md](../../SECURITY.md#a-cpu-bound-turn-can-fence-its-own-host).

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

### What a fence costs, and who pays it

Fencing defends the single-activation invariant, so its scope is exactly the
hosts that *have* claims:

- A host that registers **any stateful type** fences terminally. It stops
  claiming, deactivates what it holds, withdraws from membership, and reports
  `fatal` on its health check so the orchestrator restarts it — a fresh
  identity is the only safe way back, because peers may already be serving the
  actors it used to own.
- A host that registers **only workers** has nothing to fence. A
  `defineWorker` pool writes no directory entry by construction, so nothing
  anywhere names that host and nothing was re-placed when it vanished. It
  **rejoins under the same identity** instead (#272), retrying with capped
  backoff until the store takes it back, and serves its pools throughout. That
  identity reuse is safe *because* of the missing claim, not in spite of it.

Dispatch from a fenced host fails with the branded `fenced` error rather than
resolving to `local`. This matters more than it looks: a fenced host's
membership view is usually gone with the store that fenced it, and an empty
view reads as "solo" everywhere else in placement — so without the guard, every
call degraded into a local activation attempt and surfaced as *"unknown actor
type"* on a tier that only ever calls types other hosts own. The error is
terminal, not retried: re-resolving lands on the same refusal, and each attempt
would cost a directory lookup and a membership refresh against the store that
just failed. A *peer* calling into a fenced host still gets `unreachable`,
which is the answer it can act on.

## Change detection

Providers compare **host signatures**, not just a version counter. A host that
dies silently expires on the store's clock without anyone bumping a version, so
a version-only comparison never converges. Push (pub/sub, LISTEN/NOTIFY, live
query, watch) is **best-effort in every provider**; the poll is the guarantee
and `pollMs` is the propagation bound.

The same rule governs anything *derived* from a view: **cache on the view
object, never on `version`.** A provider builds a NEW `MembershipView` per
observed change — including a peer expiring on the store's clock, which need
not bump `version` (#267) — and hands back the same object until the next one,
so object identity is the invalidation key that is always right. Placement's
own derived data (`activeHostsCache` since #27, `eligibleCache` since #212)
is a `WeakMap` keyed on the view object; `membersMemo(placement, filter?)`
exports that pattern for consumers (#269), and `placement.onChange(cb)` passes
the provider's change stream through for anything the memo does not cover. A
consumer that memoized `members()` on `version` latched a stale member count
into a concurrency cap — the failure this exists to make unreachable.

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

## Targeted worker calls

`members()` and `dispatchOn()` on `ClusterPlacement` (#213) are the public
form of the mechanism `$sigx:host#stats` always used internally: enumerate the
view, then invoke something ON a chosen member rather than wherever placement
routes. `workerOn(placement, target, def, key?)` is the typed sugar.

Three rules keep it honest:

- **Workers only.** A worker executes on the host that receives the call —
  that is what a worker *is* — so targeting is meaningful. A stateful type is
  refused when the caller can resolve its definition: targeted delivery would
  fight placement over where the activation lives.
- **One attempt.** No retry, no route cache, no directory. The caller chose
  the host, so `unreachable` (departed) and `wrong-host` (does not register
  the type — the registration-aware refusal above) are *answers*, propagated
  branded, never consumed as a re-route.
- **Self-targets stay in-process** (the `clusterStats` precedent): a host
  often cannot reach its own advertised address, and a fenced host can still
  serve its own workers.

`targetedDispatches` counts these apart from `remoteDispatches`, and
`addCounters` sums with `?? 0` so a mixed-version peer's report missing newer
counters cannot NaN the `clusterStats` totals.

## Reading locality from the counters

`routedLocal` is a *placement* count, not a request count. `dispatcherFor`
hands back the local dispatcher *before* the routing loop whenever the host
already holds the claim, so in the warm steady state a local hit never reaches
the site that increments it, and `routedLocal / (routedLocal +
remoteDispatches)` reads near-zero for a perfectly local cluster — #52
measured `routedLocal` 33 → 81 against ~270k reads on a fleet whose CPU per
request had just halved from locality routing.

The pair to read instead is `dispatchesLocal` / `dispatchesRemote`: once per
call, stream or watch *subscriber*, on the warm fast path and the routed path
alike, decided at the call's first resolved target. So
`dispatchesLocal / (dispatchesLocal + dispatchesRemote)` is the per-request
locality fraction the locality-routing table promises, and it is what the CLI's
cluster screen and the dashboard render as `locality`. Three rules keep the
pair honest:

- **Once per call.** `remoteDispatches` stays per *attempt* (retries
  included), which is why the two differ even on the routed path. A re-route
  after `wrong-host` or `unreachable` is a `retries` event, not a second
  dispatch.
- **Per subscriber, not per stream.** A coalesced watch (#111) is one hop
  serving n subscribers; each attach counts `dispatchesRemote` once, and the
  shared pump behind it is told it has already been accounted for.
- **Workers and `dispatchOn()` count in neither.** Nothing placed them — a
  worker runs wherever it is called and a targeted call chose its host — so
  they have no locality to measure and would only dilute the fraction. Nor
  does a call whose target never resolves (`unplaceable` through every
  retry): with no first resolved target there is nothing to decide, so it
  lands only in `routingFailures`.

The pair counts calls this host *initiated*, which is more than the requests
it received: reminder and task-tick firings and topic deliveries go through
`Host.dispatch` → `dispatcherFor` like any other call, so `dispatchesLocal +
dispatchesRemote` runs ahead of an inbound request count on a host with
timers armed. Read the fraction, not the sum, against a request rate.

The warm fast path pays exactly this one increment and nothing else; the
`cluster/locality-warm` bench keeps deriving its `local_fraction` from
`remoteDispatches` against its own call count so its `exact` gates stay as
baselined.

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
