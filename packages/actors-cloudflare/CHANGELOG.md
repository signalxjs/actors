# Changelog

## [Unreleased]

### Fixed

- **`onAlarm()` no longer holds `blockConcurrencyWhile` across delivery**
  (#139). It wrapped its whole body in the gate and then called
  `context.deliver()`, so a handler doing the documented thing —
  `ctx.reminders.set()` from inside `onReminder` — took the gate again. The
  real `blockConcurrencyWhile` blocks the object until its callback settles
  and does not nest, so the inner call waited on a lock its own caller held:
  the object wedged, the alarm was never re-armed, and a periodic reminder
  was dead for good.

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

- **`durableObjectPlacement()` and the `durableObjects()` plugin** (#131) —
  a ref now routes to the Durable Object that holds it, via
  `idFromName` over the runtime's own actor id.

  **One placement runs on both sides**, distinguished only by an `isSelf`
  predicate. Giving the object's own silo the plain local host instead looks
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
- A `.size-limit.json` budget for the package (#139) — 2.5 KB against a
  current 1.9 KB. It was the only shipped dist with no budget, and it is
  the one runtime where bytes are billed as startup CPU.

- **Initial release** (#9): Cloudflare Durable Objects as the backend for
  `@sigx/actors`, on a **one-DO-per-actor** model.

  `durableObjectStorage(state.storage)` implements `ActorStorage`. DO
  storage is strongly consistent and single-threaded per object, so the
  runtime's etag compare-and-set holds without a transaction.

  `durableObjectReminders({ storage, alarms })` implements `ActorReminders`
  over the DO alarm. The default `shardedReminders()` splits one table into
  fixed hash shards and polls it, because a silo hosts many actors and has
  to find whose reminder is due; a DO hosts exactly one, so there is
  nothing to search and nothing to poll. It also removes the cadence floor
  — `shardedReminders` can only promise "at or after `nextDue`, checked
  every `reminderTickMs`", whereas an alarm fires at the due time. Call
  `onAlarm()` from the object's `alarm()` handler.

  No membership, directory, or authenticated internal mount: Cloudflare
  already guarantees a single instance of a DO globally and serializes its
  requests, which is the guarantee `@sigx/actors/cluster` exists to rebuild
  elsewhere.
