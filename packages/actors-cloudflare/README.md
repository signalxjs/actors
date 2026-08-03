# @sigx/actors-cloudflare

Cloudflare **Durable Objects** as the backend for
[`@sigx/actors`](../actors) — one DO per actor.

## Why it is small

Cloudflare already guarantees a single instance of a Durable Object
globally and serializes requests to it. That *is* the virtual-actor
contract, so this package needs none of the machinery
`@sigx/actors/cluster` uses to rebuild it: no membership heartbeats, no
activation directory, no HMAC-authenticated host-to-host mount.

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
and polls it, because a host hosts many actors and has to find whose
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
outside the actor runtime. This holds for `reentrant: 'always'` /
`methodReentrancy` actors too: their turns interleave by design, but the
runtime single-flights their saves, so the compare-and-set stays serialized
within the one activation. (Interleaving needs `AsyncLocalStorage`, which
rides the `nodejs_compat` flag this package already requires.)

**Reminders do.** `onAlarm()` is invoked straight from the object's
`alarm()` handler, outside any turn, so its read-modify-write of the
reminder table can interleave with a `ctx.reminders.set/clear` from a
concurrent dispatch — and one of the two writes would be lost. Pass
`blockConcurrencyWhile` there.

Two things follow from what that gate actually is, and both are handled for
you:

- **It is never held across delivery.** `blockConcurrencyWhile` blocks the
  whole object until its callback settles, so holding it across an arbitrary
  user callback stalls every other event on that object for as long as the
  handler runs — and delivery is exactly the part whose duration you do not
  control. So `onAlarm()` runs in three phases: claim what is due and persist
  the advance (gated), deliver (**un**gated), then re-read and re-arm (gated).

  (The gate does *permit* re-entry, so rescheduling from inside `onReminder`
  would not deadlock. That is measured, not assumed — see
  `__tests__/workers/gate.test.ts`.)
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

**Why one placement on both sides.** Giving the object's own host the plain
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

**Stateless workers never map to an object.** A `defineWorker` type has no
identity, no storage record and no single-activation invariant, so the
placement short-circuits it to the local dispatcher before any object id is
derived: the pool runs in whichever isolate received the call — the Worker at
the edge, or the Durable Object whose `ctx.actor()` made the hop. One caveat
follows from `sweepIntervalMs: 0` on Workers: pools never shrink by idle
sweep here — they go away when the platform evicts the isolate, which loses
nothing for a stateless type.

## The object and the Worker

Two pieces, one bundle — on Cloudflare the Durable Object and the Worker are
the same script, so the actor registry is a plain import in the entry.

```ts
import { createHostDurableObject, createWorkerHandler } from '@sigx/actors-cloudflare';
import { Counter } from './counter.actor';

interface Env { ACTORS: DurableObjectNamespace }

// The object: hosts exactly the actor its id names.
export class ActorHost extends createHostDurableObject<Env>({
    actors: [Counter],
    namespace: (env) => env.ACTORS
}) {}

// The Worker: hosts nothing, routes everything.
export default createWorkerHandler<Env>({
    actors: [Counter],
    namespace: (env) => env.ACTORS,
    // Workers callers are not browsers posting a form; decide the policy.
    fetch: { origin: false }
});
```

`createHostDurableObject` returns a class rather than being one to extend,
because the object must own its seams — storage, reminders, placement and the
defaults all come from its own state, and a subclass wiring them itself would
be one `super()` away from silent corruption. Extending the returned class
still works for `webSocketMessage` and friends.

It boots through a memoized promise rather than `blockConcurrencyWhile` in the
constructor. A throw inside that gate *resets the object*, so a transient
start failure would tear the isolate down and retry invisibly instead of
surfacing an error that says what went wrong. A rejection is never cached, so
a failed start stays retryable.

`alarm()` boots first and then delivers, which is not defensive: an alarm can
be the **first** thing an evicted object sees, and `onAlarm()` refuses to run
before the host has bound its reminders.

Every inbound call is checked against the object's own id. Under Durable
Objects that can never be a race — `ref` → object id is a pure function — so a
mismatch means the Worker and the object disagree about `objectName`,
`jurisdiction`, or which namespace is bound, and it fails naming both sides
rather than letting one actor quietly exist in two objects.

An `app` factory can add plugins, and receives the object-derived options it
must pass on. It is deliberately not handed `env`: without a namespace binding
it cannot build a placement, and `setPlacement` being exclusive means an app
that tries anyway fails naming both plugins rather than leaving the object
able to fetch itself.

The Worker's host hosts nothing, so its storage is `unhostedStorage()` — every
operation throws saying so. In-memory storage there would be a silent lie.

## Eviction is not deactivation

A Durable Object is evicted when it goes idle, and eviction destroys the
isolate, the host and the activation together. **`onDeactivate` never runs.**

That is a real difference from every other backend. An actor that flushes in
`onDeactivate` must instead `ctx.save()` inside the turn, or use write-behind
with a short debounce. Nothing is lost that was already saved — storage is
durable — but nothing unsaved survives, and there is no hook to save it in.

For the same reason there is no idle sweeper (`sweepIntervalMs: 0`): the
platform's eviction *is* idle collection, one level down. On Workers
`keptAlive` is enforced by the request's lifetime rather than by a sweeper,
so an open `ctx.changes()` stream holds the object up exactly as long as its
response body is open.

## Status

Storage, reminders, the placement and the host all ship here, so an actor app
runs on Workers. Still to come: a real-`workerd` test suite (everything here is
proven against fakes, which is fast and honest about what it covers, but has
never met the platform) and a deployable example.
