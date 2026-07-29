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

## Status

Storage and reminders ship here. The placement (`ref` → DO stub) and the
`SiloDurableObject` host class are the next step; see
[#9](https://github.com/andtii/actors/issues/9).
