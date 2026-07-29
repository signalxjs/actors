# @sigx/actors-cloudflare

Cloudflare **Durable Objects** as the backend for
[`@sigx/actors`](../actors) — one DO per actor.

## Why it is small

Cloudflare already guarantees a single instance of a Durable Object
globally and serializes requests to it. That *is* the virtual-actor
contract, so this package needs none of the machinery
`@sigx/actors/cluster` uses to rebuild it: no membership heartbeats, no
activation directory, no HMAC-authenticated silo-to-silo mount.

Two seams remain.

## Storage

```ts
import { durableObjectStorage } from '@sigx/actors-cloudflare';

const storage = durableObjectStorage(state.storage);
```

DO storage is strongly consistent and single-threaded per object, so the
runtime's etag compare-and-set holds without a transaction.

## Reminders

```ts
import { durableObjectReminders } from '@sigx/actors-cloudflare';

const reminders = durableObjectReminders({
    storage: state.storage,
    alarms: state.storage,
    // Recommended: `onAlarm()` runs outside any actor turn (see below).
    blockConcurrencyWhile: (fn) => state.blockConcurrencyWhile(fn)
});

export class ActorHost {
    async alarm() {
        await reminders.onAlarm();   // fire what is due, re-arm the rest
    }
}
```

The default `shardedReminders()` splits one table into fixed hash shards
and polls it, because a silo hosts many actors and has to find whose
reminder is due. A DO hosts exactly one, so there is nothing to search and
nothing to poll: reminders live in the object's own storage and the
platform wakes it at the earliest due time.

That also removes the cadence floor — `shardedReminders` can only promise
"at or after `nextDue`, checked every `reminderTickMs`", whereas an alarm
fires at the due time.

## Concurrency

A Durable Object may deliver another event while your code awaits, so any
read-modify-write that is not already serialized needs a gate.

**Actor state does not need one.** Every `ctx.save()` runs inside a mailbox
turn, and one DO holds one actor, so its compare-and-set cannot interleave
through the normal dispatch path. `durableObjectStorage` still accepts
`blockConcurrencyWhile` for defence in depth, or if you drive it from
outside the actor runtime.

**Reminders do.** `onAlarm()` is invoked straight from the object's
`alarm()` handler, outside any turn, so its read-modify-write of the
reminder table can interleave with a `ctx.reminders.set/clear` from a
concurrent dispatch — and one of the two writes would be lost. Pass
`blockConcurrencyWhile` there.

Two things follow from what that gate actually is, and both are handled for
you:

- **It is never held across delivery.** `blockConcurrencyWhile` blocks the
  whole object until its callback settles, and does not nest. Since
  rescheduling from inside `onReminder` is a normal pattern — and takes the
  gate itself — holding it while delivering would deadlock the object. So
  `onAlarm()` runs in three phases: claim what is due and persist the
  advance (gated), deliver (**un**gated), then re-read and re-arm (gated).
- **Expected failures are raised outside it.** An exception escaping
  `blockConcurrencyWhile` *resets* the Durable Object, which would replace a
  diagnosable error with a generic one and kill every other in-flight call.
  A wrong-owner `ctx.reminders.set()` — the "this object hosts a different
  actor" case — therefore travels back as a value and is thrown once the
  gate has closed.

## Placement

```ts
import { durableObjectPlacement } from '@sigx/actors-cloudflare';

// In the Worker: nothing is local, so every call goes to an object.
const placement = durableObjectPlacement({ namespace: env.ACTORS });

// Inside a Durable Object: the SAME placement, plus a self predicate.
const placement = durableObjectPlacement({
    namespace: env.ACTORS,
    isSelf: (ref) => `${ref.type}\u0000${ref.key}` === state.id.name
});
```

A ref resolves to `idFromName("type\0key")` — the runtime's own actor id.
Not `type:key` or `type/key`: nothing forbids `:` or `/` in an actor key, so
neither is injective, which is exactly why the id uses a NUL separator.

**Why one placement on both sides.** Giving the object's own silo the plain
local host instead looks obvious and silently corrupts state:
`ctx.actor(Cart, 'x')` called from `Counter/alice` would resolve locally and
activate `Cart/x` *inside `Counter/alice`'s object*, writing that actor's
record into the wrong object's storage. Every calling object would get its own
copy — single activation violated, with nothing to point at. So a Durable
Object routes everything that is not its own actor back out to the object that
owns it. Self-recursion is impossible by construction: a self-call
short-circuits to the local dispatcher before any stub is derived.

The hop rides `httpTransport()` with its `fetch` swapped for a stub call, so
the envelope, the NDJSON framing, the remaining-ms deadline and the branded
error re-creation are the ones the rest of the runtime uses, not a second
implementation of them.

No HMAC: a stub is not network-reachable, and the only way to obtain one is to
hold the namespace binding — a Worker-level capability grant. Guards therefore
run once, at the public edge, where the request still carries real client
headers.

There is no 421 wrong-host and no retry either. That machinery exists because a
cluster directory and a caller's cached routing can disagree; here `ref` →
object id is a pure function and the platform guarantees one instance, so a
mismatch is a *configuration* bug — a wrong binding, or an `objectName` /
`jurisdiction` that differs between the Worker and the object — and fails
loudly rather than retrying.

`locationHint` only affects where a new object is first created and is safe to
change. `jurisdiction` and `objectName` are part of an actor's identity:
changing either repoints every actor at a different object, which is a state
migration.

## Status

Storage, reminders and the placement ship here. The `SiloDurableObject` host
class and the Worker front door are the next step; see
[#143](https://github.com/andtii/actors/issues/143). Until then you wire the
object class yourself.
