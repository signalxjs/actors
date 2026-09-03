# Changelog

## [Unreleased]

### Added

- **`durableObjectsHosted()`** (#362) — a plugin that installs nothing and
  only narrows: `ActorPlugin<Record<never, never>, never>`, so an `app`
  factory doing `defineActorApp(base).use(durableObjectsHosted())` gets the
  same `placement`-refusing `defineActor` a `.use(durableObjects(...))` app
  has (#351), without a namespace binding in hand. That is the device for
  the documented authoring path — `createHostDurableObject()` /
  `createWorkerHandler()` install `durableObjects()` themselves after the
  factory runs, so the factory could not — and for the type-only
  `export const { defineActor } = createApp({})` binding built from it.
  The host accepts it because it claims no placement seam.
  `examples/cf-workers` uses it.

### Changed

- **`durableObjects()` narrows `placement` to `never`** (#351). Type-only.
  The plugin is now `ActorPlugin<Record<never, never>, never>`, so the
  app-bound `defineActor` of an app that `.use(durableObjects(...))` refuses
  `placement` outright — the Durable Objects placement never reads it (a ref
  maps to its object by name), so a cluster `PlacementPolicy` on a DO-hosted
  actor compiled and was a silent no-op. The bare `defineActor` from
  `@sigx/actors` and apps without the plugin are unchanged; no runtime
  change. **Reach:** the compile error lands only where the app module
  itself calls `.use(durableObjects(...))`. On the supported authoring
  path — an `app` factory handed to `createHostDurableObject()` /
  `createWorkerHandler()`, which install the plugin themselves after the
  factory runs, and the type-only `export const { defineActor } =
  createApp({})` binding built from it — `placement` still has the wide
  type and is still a silent no-op. The runtime floor for that case (a
  `__DEV__` warning from the DO placement when a registered definition
  carries `placement`) and a type-level device the factory can use without
  `env` are tracked in #362.
- **`objectSocketRoute` refuses `internal: true` types at the upgrade**
  (#74). The object-terminated socket's forwarding route resolves the type
  itself, ahead of any session, so it now answers a server-internal type
  with the same 404 an unknown type gets — and never mints or wakes a
  Durable Object for it. Without this the upgrade answered 101 and only the
  first frame 404'd inside the object, which told a probe the type existed.
  The Worker-terminated `workerSocket()` needed no change: it wraps
  `createActorSocketSession`, which checks per frame.

### Fixed

- **A `placement` declared on a Durable Object-hosted actor is no longer a
  silent no-op** (#362). `durableObjectPlacement()` now reads
  `__sigxActor.placement` in the same per-type definition lookup it already
  makes for `stateless`, ahead of the `isSelf` branch, so an object's own
  actor is covered as well as a remote one; one lookup per type, then a
  memo hit. A strategy tagged `backend: 'cluster'` — every
  `@sigx/actors/cluster` policy — **throws at dispatch**, naming the type
  and the strategy: a cluster policy on a DO actor asks for a host to be
  chosen and none ever will be, the same posture the cluster placement
  takes with a tag it does not own (#350). Any other declared strategy
  logs one `__DEV__` warning per type (`[sigx actors-cloudflare] actor
  "<type>" declares placement "<name>" — Durable Objects ignore it; a ref
  maps to its object by name`). Before, the placement never read the
  declaration at all. A deployment that had a cluster policy on a DO actor
  was already not getting what it declared; it now fails loudly — remove
  the `placement`, or use `durableObjectsHosted()` to refuse it at compile
  time.
- **A reminder whose dispatch fails is retried one tick later instead of
  being lost, and counted** (#326). `durableObjectReminders` advances or
  deletes a due entry *before* `onAlarm()` delivers it, and a rejected
  `deliver()` (a call deadline, an `onReminder` that threw) was at most
  logged: the wake was gone, and `HostStats.remindersUndelivered` read `0`.
  The default `shardedReminders()` got the retry in #306; this is the same
  contract on a Durable Object. A rejected (or synchronously throwing)
  `deliver()` now re-arms its entry `reminderTickMs` out (a one-shot
  re-inserted, a periodic one pulled forward) in the alarm's final gated
  write — so the platform alarm is re-scheduled for it, never sooner than a
  tick, and a target that never answers costs one attempt per tick — and
  each failed attempt is reported through
  `ActorRemindersContext.undelivered`, so the host's counter, `ops()` and
  `metrics()` now say so here too. Same rules as the sharded table: an entry
  the actor set again meanwhile (from the very `onReminder` that then timed
  out, say) is left as the actor set it — a later decision wins — a periodic
  one it cleared stays cleared, and a one-shot it cleared while its dispatch
  was failing may be retried once, so `onReminder` should be idempotent.

## [0.7.0] - 2026-08-09

### Added

- **The object-terminated socket — `createHostDurableObject({ socket })` +
  `createWorkerHandler({ socket: { terminate: 'object' } })`** (#158): the
  upgrade at `/_sigx/socket/{type}/{key}` is forwarded — cookies, `Origin`
  and all — to that actor's Durable Object, which accepts it with the
  hibernation API (`state.acceptWebSocket`, tag `sigx:socket`). The session
  lives where the actor lives, so a disconnect tears down INSIDE the object:
  `iterator.return()` reaches the watch locally, `keptAlive` clears, and the
  empty room is released — the four skipped acceptance tests from the old
  tracker's #47 investigation are unskipped and green on real workerd.
  Hibernation contract, deliberately minimal: keepalive is
  `setWebSocketAutoResponse` (`{"p":1}` answered without waking the
  object; `pingMs` is not accepted), `maxConnectionMs` survives eviction as
  a per-message-checked deadline in the socket attachment (`{v, deadline?}`
  — nothing else rides it), and the first message after a cold wake closes
  `1012 'session evicted — reconnect'` — the client transport redials with
  the browser's current cookies and re-seeds, the same contract as any
  drop. One socket per actor/object ("the room pattern"); the fix covers
  the object's OWN actor's watches — a session's watches on other actors
  still cross the stub boundary, which with the remaining HTTP-stream shape
  is why #47 stays open. Also exported: `objectSocketRoute`,
  `parseSocketActorPath`, `durableObjectStubResolver` (the ref → stub
  derivation, extracted so the placement and the forwarding route cannot
  disagree about where an actor lives), `DurableWebSocketLike`, and the
  optional hibernation members on `DurableObjectStateLike`.

- **`workerSocket()` — the Worker-terminated route for the client socket**
  (#157): browsers speaking `@sigx/actors/socket-wire` over one WebSocket,
  upgraded in the Worker. On Workers a 101 upgrade IS a `Response`, so the
  route is an ordinary plugin contribution around
  `createActorSocketSession` — every call and subscription re-dispatches
  through placement to its actor's Durable Object, stub derived fresh per
  dispatch. `createWorkerHandler` gains a `socket` option as sugar. A
  refused construction (origin, auth) answers with an honest HTTP status
  instead of an accepted-then-closed socket, and unlike the Node adapter
  there is no pre-session buffer — the client end of a `WebSocketPair` only
  exists inside the returned Response, so no frame can race construction.
  **It does not fix #47**: a departed live consumer still leaves
  `keptAlive` set in the objects it watched, because cancellation dies at
  the `stub.fetch` boundary. When empty-room economics matter, wait for the
  object-terminated socket (#158).

## [0.5.0] - 2026-08-07

### Changed

- **Peers `@sigx/actors@^0.5.0`.** The family versions in lockstep, so the
  range moves with the release. 0.5.0 only ADDS `ctx.changes({ throttleMs })`
  and removes a snapshot a `$live` watch never read (#129) — no wire or API
  break, so a 0.4.x host interoperates fine.

## [0.4.0] - 2026-08-07

### Changed

- **Peers `@sigx/actors@^0.4.0`.** The family versions in lockstep, so the
  range moves with the release. Nothing else to do: 0.4.0 only ADDS
  `onSettled` to `defineJob` (#125), so unlike the 0.2.0 and 0.3.0 moves
  there is no wire or API break and a 0.3.x host interoperates fine.

- README trimmed to a pointer at https://sigx.dev/actors (#113): thesis,
  install, peer-dependency and minimum-version requirements, and links. The
  reference material is on the docs site; relative links (which npm does not
  resolve) are gone. No code or API change.

## [0.3.0] - 2026-08-05

### Changed

- **Peers `@sigx/actors@^0.3.0`.** The actor URL grammar is breaking
  (#96), so the whole family moves together — see the `@sigx/actors`
  changelog. **Upgrade every host before any client or peer starts
  emitting**: a host still on 0.2.x refuses calls from an upgraded one.

## [0.2.0] - 2026-08-05

### Fixed

- **Every actor call to a Durable Object 404'd** once core made the mount
  base load-bearing (signalxjs/core#563). The object's mount lives at
  `/_sigx/do`, but the base never reached `handleServerFnRequest`, so core
  matched the path against its own `/_sigx/fn` default. Latent until 0.15;
  fatal after.
- **Host-to-host traffic 401'd under the fail-closed runtime.** The internal
  mount authenticates with a per-request HMAC, not a principal, so its
  wrappers now declare anonymity — otherwise core's identity gate (which
  runs for every wire request) refused calls the platform had already
  guaranteed.

### Changed

- **Peers `@sigx/actors@^0.2.0`.** The guard split is breaking, so the
  whole family moves together — see the `@sigx/actors` changelog and core's
  [0.15 migration guide](https://github.com/signalxjs/core/blob/main/docs/migrations/0.15-guard-split.md).
  Actors, workers and jobs defined against this package declare access with
  `authorize` / `methodAuthorize` / `allowAnonymous` now, and the runtime is
  fail-closed: one that declares nothing, in a process with no server app,
  denies with 401.

## [0.1.0] - 2026-08-03

### Changed

- **silo → host** (#233): `createSiloDurableObject` →
  `createHostDurableObject` and the `SiloDurableObject*` types are
  `HostDurableObject*`. The Durable Object storage keys (`sigx:reminders`)
  are unchanged. A deployed Worker adopting the rename needs a
  `renamed_classes` migration for its exported class.

### Fixed

- **A Durable Object stub is no longer cached between dispatches** (#149).
  A stub is an I/O object owned by the request context that created it, and
  workerd refuses to use one from another request — so the cache turned every
  call after the first into `unreachable`. Found by running on real workerd;
  no fake could have shown it. Rebuilding costs one `idFromName` hash.
  `stubCacheSize` is gone.
- **`createWorkerHandler` resolves its namespace binding per request** (#149),
  for the same reason: the app is memoized across requests, but the binding
  it was built with is not reusable from a later one.

- **`onAlarm()` no longer holds `blockConcurrencyWhile` across delivery**
  (#139). It wrapped its whole body in the gate and then called
  `context.deliver()`, so a handler doing the documented thing —
  `ctx.reminders.set()` from inside `onReminder` — took the gate again.

  **Correction (#149):** the original entry said the real gate deadlocks on
  re-entry. Measured against workerd, it does not — it permits re-entry, and
  the fake that "proved" the deadlock was modelling a non-reentrant queue the
  platform does not implement. The split stays for the reason that survived
  measurement: the gate blocks the whole *object* until its callback settles,
  so holding it across an arbitrary user callback stalls every other event on
  that object for the duration of the handler.

  Now three phases — claim and persist the advance (gated), deliver
  (ungated), re-read and re-arm (gated) — which removes the re-entrancy
  instead of trying to detect it. A "gate already held" flag cannot work: it
  cannot distinguish a genuinely re-entrant call from a different concurrent
  one, and would let the latter skip the serialization the gate exists for.

  The existing tests missed this because the suite had both halves and never
  put them together: the reschedule test passed no gate at all, and the gate
  test used a depth counter that never blocks.

- **An expected reminder failure no longer throws through the gate** (#139).
  A wrong-owner `ctx.reminders.set()` raised its error inside
  `blockConcurrencyWhile`, and an exception escaping that **resets the
  Durable Object** — so the caller lost the message naming which actor the
  object actually hosts, and every other in-flight call on the object died
  with it. The failure now travels back as a value and is thrown once the
  gate has closed.

### Added

- **`createHostDurableObject()` and `createWorkerHandler()`** (#143) — an
  actor app now runs on Workers. The object hosts exactly the actor its id
  names; the Worker hosts nothing and routes everything.

  The object boots through a memoized promise rather than
  `blockConcurrencyWhile` in its constructor: a throw inside that gate
  *resets the object*, so a transient start failure would tear the isolate
  down and retry invisibly instead of surfacing an error. `alarm()` boots
  before delivering, because an alarm can be the first thing an evicted
  object sees and `onAlarm()` refuses to run unbound.

  Every inbound call is checked against the object's own id. That can never
  be a race here — `ref` to object id is a pure function — so a mismatch
  means the two sides disagree about naming or bindings, and it fails naming
  both rather than letting one actor exist in two objects.

  The `app` factory is never handed `env`, so it structurally cannot build a
  placement of its own; combined with `setPlacement` being exclusive, an app
  that tries fails naming both plugins instead of leaving the object able to
  fetch itself. `unhostedStorage()` backs the Worker's host, which never
  activates anything.

  **Eviction is not deactivation**: the platform destroys the isolate, the
  host and the activation together, so `onDeactivate` never runs. Actors that
  flush there must `ctx.save()` in the turn instead. Documented in the
  README, because nothing in the type system says it.

- **`durableObjectPlacement()` and the `durableObjects()` plugin** (#131) —
  a ref now routes to the Durable Object that holds it, via
  `idFromName` over the runtime's own actor id.

  **One placement runs on both sides**, distinguished only by an `isSelf`
  predicate. Giving the object's own host the plain local host instead looks
  obvious and silently corrupts state: `ctx.actor(Cart, 'x')` from inside
  `Counter/alice` would activate `Cart/x` *in alice's object* and write its
  record into the wrong object's storage — single activation violated, with
  nothing to point at. A self-call short-circuits to the local dispatcher
  before any stub is derived, so an object can never fetch itself.

  The hop reuses `httpTransport()` with its `fetch` swapped for a stub call,
  so the envelope, NDJSON framing, remaining-ms deadline re-anchoring and
  branded error re-creation are the runtime's own rather than a second
  implementation. No HMAC (a stub is not network-reachable — holding the
  binding is the capability grant, and guards run once at the public edge),
  no 421 wrong-host and no retry (`ref` to object id is a pure function and
  the platform guarantees one instance, so a mismatch is a configuration bug
  that must fail loudly).

- `DurableObjectStorageOptions` and `BlockConcurrencyWhile` are re-exported
  from the package root (#139). Both were declared and exported in
  `storage.ts` but never re-exported, so a consumer could not name either.
- A `.size-limit.json` budget for the package (#139) — now 4 KB against a
  current 2.75 KB. It was the only shipped dist with no budget, and it is
  the one runtime where bytes are billed as startup CPU.

- **Initial release** (#9): Cloudflare Durable Objects as the backend for
  `@sigx/actors`, on a **one-DO-per-actor** model.

  `durableObjectStorage(state.storage)` implements `ActorStorage`. DO
  storage is strongly consistent and single-threaded per object, so the
  runtime's etag compare-and-set holds without a transaction.

  `durableObjectReminders({ storage, alarms })` implements `ActorReminders`
  over the DO alarm. The default `shardedReminders()` splits one table into
  fixed hash shards and polls it, because a host hosts many actors and has
  to find whose reminder is due; a DO hosts exactly one, so there is
  nothing to search and nothing to poll. It also removes the cadence floor
  — `shardedReminders` can only promise "at or after `nextDue`, checked
  every `reminderTickMs`", whereas an alarm fires at the due time. Call
  `onAlarm()` from the object's `alarm()` handler.

  No membership, directory, or authenticated internal mount: Cloudflare
  already guarantees a single instance of a DO globally and serializes its
  requests, which is the guarantee `@sigx/actors/cluster` exists to rebuild
  elsewhere.
