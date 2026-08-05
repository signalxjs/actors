# Changelog

## [Unreleased]

## [0.3.0] - 2026-08-05

### Changed

- **Peers `@sigx/actors@^0.3.0`.** The actor URL grammar is breaking
  (#96), so the whole family moves together — see the `@sigx/actors`
  changelog. **Upgrade every host before any client or peer starts
  emitting**: a host still on 0.2.x refuses calls from an upgraded one.

## [0.2.0] - 2026-08-05

### Fixed

- **Every actor call to a Durable Object 404'd** once core made the mount
  base load-bearing (#563). The object's mount lives at `/_sigx/do`, but the
  base never reached `handleServerFnRequest`, so core matched the path
  against its own `/_sigx/fn` default. Latent until 0.15; fatal after.
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
