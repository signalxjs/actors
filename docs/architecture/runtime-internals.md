# Runtime internals

Activation, turns, and the invariants that make single-threaded state safe.

User-facing versions of most of this are on the docs site —
[the actor model](https://sigx.dev/actors/docs/actor-model/),
[turns & concurrency](https://sigx.dev/actors/docs/turns/),
[reentrancy](https://sigx.dev/actors/docs/reentrancy/),
[lifecycle](https://sigx.dev/actors/docs/lifecycle/). What follows is the
maintainer's view.

## The turn

A **turn** is one method invocation on one activation, from the moment it
starts running to the moment its promise settles. Every activation runs its
turns one at a time, in arrival order.

That is the whole reason `ctx.state.count++` is safe with no lock: while your
turn is running, nothing else on that actor is. It is also why `await` inside a
turn costs more than in ordinary JS — the turn has not ended, so every later
call to that actor waits. **Slow I/O inside a turn is a queue for that actor**,
which is why `metrics()` splits the two halves: `queueMs` (waiting for a turn —
high means the actor is hot) against `turnMs` (holding the activation — high
means the turn itself is slow).

## Admission and overload

A turn queue is a promise chain with no cap of its own (`host/turns.ts`), so
before #384 the only thing that bounded it was the caller's deadline: a host
offered more than it could drain accepted every call, held each one for
`callTimeoutMs`, ran its body anyway when its turn came, and then failed it —
it *drowned* (the 500 runs/s row of `wf-local/drown-vs-shed` in
`BASELINES.md`: ~660 deadline failures and 979 publish timeouts while every
"the engine refused something" counter read zero). Three things changed.

**Two caps, both off by default.** `defineActor({ maxQueued })` (or
`HostDefaults.maxQueuedPerActor`, which it overrides — an explicit `0` on
either is unlimited) bounds one activation's queued-plus-running turns;
`HostDefaults.maxInflightTurns` bounds the host's, counting *every* turn on
it — timer ticks, task turns and watch reads included, because a loop
saturated by its actors' own work is just as full. A call that would pass
either is refused **before it is queued**, synchronously, with
`ActorOverloadedError` (`kind: 'overloaded'`, `scope: 'actor' | 'host'`,
`depth`, `limit`), and `HostStats.overloadRefusals` counts it. The check is
two integer compares on `Activation.enqueue`; with both caps at 0 the hot
path is byte for byte what it was.

**Only calls are refused.** The runtime's own turns never come through
admission: a watch loop's reads (`enqueueSystem`), the write-behind flush, a
conflict reload, a timer tick and a task's `ctx.turn` all schedule on
`Turns` directly. They *count* toward `maxInflightTurns` — they are what
fills the loop — but a cap exists to shed a caller's load, and refusing the
runtime's own progress would turn back-pressure into a stall. Where a
refusal lands on one of the runtime's delivery paths it takes that path's
existing failure branch: a reminder whose `deliver()` is refused is counted
`remindersUndelivered` and re-armed one tick out (#306), a topic delivery is
a `failures[]` entry and never a publisher exception, a one-way call rejects
its caller at acceptance rather than resolving `undefined` (the refusal is
pre-acceptance by construction — `enqueue` throws rather than returning a
rejection, so `#dispatchOneWay` cannot mistake it for a post-acceptance
failure). `retryQueuedOnConflict` is untouched: the reload keeps a queue
*alive* across a conflict, the cap keeps it *short*.

**Drop-on-dequeue.** At the head of every turn, a `call.deadline` already in
the past rejects with `ActorCallTimeoutError` (`skipped: true`) without
running the body: the caller already holds its timeout from the dispatcher's
race, so the body would be work for nobody, in front of calls that can still
be answered. A running turn is still never killed. This reuses the clock
read the turn already takes, so it too adds nothing to the hot path — and
it is why the control arm of `dispatch/overload-shed` runs ~80 of 400
bodies rather than all of them.

**Sizing.** `maxQueued ≈ callTimeoutMs / p50 turn ms`: never admit more than
the queue can drain inside the deadline, so that an admitted call completes
and a refused one fails in microseconds — the two outcomes a caller can act
on. The same arithmetic sizes `maxInflightTurns` against the loop as a
whole. Across hosts the kind crosses the wire as a 429 with its fields and
is **never re-placed** by the routing loop (`clustering.md`): the call
reached the owner, which is full, and retrying into the same queue is what
the refusal exists to prevent. `ClusterCounters.overloadedReplies` counts
them on the calling side.

## What runs outside a turn

Three things deliberately run outside the turn sequence, so they cannot block
it: **guards**, **task bodies** (`ctx.tasks`) and **stream iteration**.

They are not interchangeable, and this is easy to get wrong:

| | Gets | Touches state by |
|---|---|---|
| Guard | no `ctx` at all — an `ActorPolicy` is `(principal, rq, op)` | it doesn't |
| Task body | `ActorTaskContext` — `ActorContextBase` minus `state`/`save`, **plus `turn()`** | `ctx.turn(fn)`, the only re-entry point |
| Stream body | plain `ActorContext` — **no `turn()`** | `snapshot()` / `changes()` |

Only **task bodies** can re-enter. `turn()` is declared on `ActorTaskContext`
and nowhere else (`types.ts`; `host/activation.ts` defines it in exactly one
place). A guard never sees a context, and `#streamContext` derives from the
activation's context adding only `changes`/`state` accessors — a stream body
reads through a snapshot because a turn may mutate underneath it, and a stream
awaiting its own actor's next turn would deadlock against itself.

`reentrant: 'always'` is the other way out: it lets an actor's own turns
overlap. Interleaving needs `AsyncLocalStorage`, which is why the Cloudflare
package requires the `nodejs_compat` flag.

## Deadlock detection

Every call carries its chain. `A → B → A` into a non-reentrant actor throws
`ActorDeadlockError` **immediately**, with the full chain, rather than hanging
until a timeout.

This is a design commitment, not a diagnostic nicety: the alternative is a
30-second `callTimeoutMs` and no evidence. `reentrant: true` permits call-chain
re-entry — the cycle runs inline against your own turn — and
`reentrant: 'always'` makes the deadlock impossible by construction.

## Persistence and the conflict path

`ctx.save()` writes through `ActorStorage` with an etag. A conflicting writer
faults the stale activation with `ActorStateConflictError`; the next call loads
the winning state.

The rule underneath: **a briefly-wrong directory entry costs a rejected save,
never corruption.** Everything distributed in this codebase is allowed to be
transiently wrong because of that. Do not add a write path that bypasses the
CAS, and be suspicious of any change that makes placement authoritative over
storage.

Saves are single-flighted per activation, so even `reentrant: 'always'` actors
cannot interleave their compare-and-sets within one activation.

### Snapshot + log: `ctx.append` and the replay on load (#312)

A record is a **snapshot plus a log**. `ctx.save()` writes the snapshot —
the whole state, O(state) — and, on a storage with `appendText`, truncates
the log in the same write. `ctx.append(entry)` folds one entry into the live
state through the definition's `applyEntry(state, entry)` reducer and appends
the entry alone to the record's log under the record's etag — O(entry). Both
mint a new etag; a stale peer's later save conflicts either way. The
activation keeps **no in-memory log**: entries are folded at append time, so
the live state is exactly what the next full save writes, and the log's
truncation on save follows from that rather than needing bookkeeping.

Loading (`seedFromStorage`) is therefore *snapshot, then `migrateState`, then
replay*: the log's entries run through `applyEntry` in append order onto the
migrated shape, before `onActivate`. The reducer is the current definition's
and expects the current shape, which is why migration comes first — and why
an eager migration write-back persists the *folded* state: a full save
truncates the log it would otherwise have left behind. A record whose log is
non-empty under a definition without `applyEntry` fails activation, loudly
and without touching the record; it was written by a definition that had one.

The append rides the same single-flight slot as a save (`#gatedSave` with an
append argument, `#doSave` in its append shape), so on an interleaving
activation an append waits for an in-flight save and chains on the etag it
minted, and a save behind an append writes the fold and compacts. A conflict
on the append ends exactly as a save's: fault by default, or the parked
reload under `retryQueuedOnConflict`. The fast path needs two things — a
record to append to (`#etag !== null`) and the storage seam
(`ActivationHost.appendStateText`, present only when the storage implements
`appendText`); missing either, the append **is a full save**: same fold,
same result, the O(state) cost `save()` has always had. `fileStorage` and
`durableObjectStorage` decline the seam, so every append there is a save.

Version bookkeeping is honest about what an append makes durable: the entry.
`#savedVersion` advances to the append's version only when the version before
it was already saved; state written *directly* since the last full save is
not in the log, and claiming it would let a write-behind or eventual flush
that owes it find nothing to do. In the intended shape (append per step, no
unsaved direct writes) the flush finds nothing and the append costs exactly
one write; with a direct write ahead of it, the next full save carries both.

Two conflict paths exist, and they end differently:

- **Turn path** — `ctx.save()` inside a method loses the CAS. `#doSave` mints
  `ActorStateConflictError` and, by default, faults the activation: the losing
  turn rejects, every queued turn dies at `#turn`'s fault guard, and
  `#reportFault` hands the activation to the host (`deactivateOne(…,
  'conflict')`). With `retryQueuedOnConflict: true` (#368) the same conflict is
  *parked* instead (`#reloadPending`), the losing turn still rejects, and the
  next turn to enter the serial lane — `#turn`, a non-inline stream open, a
  task's `ctx.turn()`, a timer tick — runs `#reload` first: the same
  `seedFromStorage` load activation uses (so `migrateState` runs), the same
  in-place state reset `clearState` uses, the winning etag adopted, and the
  version bookkeeping set to "clean at the loaded record" so a change-feed
  subscriber sees the winning state as the next boundary — and only that: the
  losing turn's own `#afterTurn` boundary is suppressed while the reload is
  pending, which also keeps it from arming the write-behind debounce for
  writes about to be discarded (a debounce armed *before* the conflict finds
  nothing dirty, because the flush turn reloads at entry like every other
  serial turn). Queued turns then run
  in their original order against it; nothing is deactivated, re-activated or
  re-dispatched. A deactivation landing while the reload is pending skips the
  final flush, as the `'conflict'` reason does. If the reload itself fails, the
  parked conflict faults the activation and the default path takes over. The
  reload runs only at the entry of a *serial* turn, which is what makes it
  safe with no further locking: `Activation.create` refuses the option on an
  interleaving activation (`declaresInterleaving`: `reentrant: 'always'`, or a
  `methodReentrancy` map naming any method — an empty map interleaves nothing
  and is accepted) — such
  an activation queues no turns, so there is nothing to re-run, and a reload
  landing under an in-flight sibling would wipe that sibling's writes and let
  its save succeed on the new etag, a silent lost update where the default
  contract rejects the save. Call-chain reentrancy is unaffected: an in-chain
  call runs through `runInline` → `#invoke`, never `#turn`, so it never
  reloads under its caller.
- **Flush path** — the debounced write-behind / eventual save (#320) loses the
  CAS with no caller to throw to. `onStateError('flush')` hears about it, then
  the activation deactivates with `'conflict'` (#336) regardless of the option:
  the flush is a system turn, its writes belong to no caller, and re-running
  "whatever was dirty" has no order to preserve.

`ctx.save({ durability: 'eventual' })` (#320) is the same write, deferred: it
bumps the version and arms the write-behind debounce (`#scheduleWriteBehind`,
50 ms) instead of awaiting the CAS, so a burst of eventual saves is one write.
The explicit-persistence contract holds — no write happens that was not asked
for; like every save, the one that does happen stores the state as it stands
when it runs, so mutations made before the flush ride along — but
`deactivate()` now flushes an explicit actor too when an eventual save
is still ahead of `#savedVersion` (`#eventualWanted`), `clearState()` drops that
marker and cancels the armed debounce on an explicit actor so a deleted record
is not written back, and a debounce failure reports through `onStateError`
(`'flush'`) rather than to a caller — with no retry of its own: on an explicit
actor only the next `save()` or the deactivation flush carries it. `defineJob`'s
`checkpoint(cp, { durability })` is a pass-through to this.

## Change detection

Persistence and the change feed both need one bit — *did anything under
`ctx.state` change* — and both read it only at a boundary (`#afterTurn`, the
deactivation flush, `ctx.save()`). Nothing wants per-mutation granularity.

The shape that gives it (`activation.ts`, `#ensureChangeTracking`):

```ts
effect(() => deepTrack(this.#state), {
    scheduler: (run) => { this.#dirty = true; this.#retrack = run; }
});
```

Two halves, and they answer different questions.

**The `scheduler` decides how OFTEN the walk runs.** A write flips `#dirty`
synchronously — so out-of-turn and `onActivate` mutations are caught at write
time — and parks the effect's re-run instead of letting it fire. The one walk
happens when `#consumeDirty()` folds at the next boundary. The floor is one
walk per *dirty boundary*, not per mutation. This is why `watch(…, { deep:
true })` is not usable here: `WatchOptions` has no `scheduler`.

**Re-running the walk is what keeps NEW objects tracked**, and that is the
invariant to protect. An object added during turn N is unsubscribed until the
walk runs again, so `#consumeDirty()` must fold before the next turn's writes.
Get this wrong and a mutation to a recently-added object is silently missed —
under write-behind, that is lost data.
`packages/actors/__tests__/dirty-tracking.test.ts` pins it.

**What the walk COSTS is `@sigx/reactivity`'s to own.** `deepTrack` is core's
own `watch(deep)` traversal, exported on `@sigx/reactivity/internals` for this
caller (signalxjs/core#651) — hence the `^0.15.3` floor. It used to be a
private copy here, carrying a comment that divergence from upstream would be
divergence in what counts as a change; it diverged exactly that way, and one
mutating turn over 200-row state cost ~1.2 ms (#124). **Do not re-inline it.**
Calling core's walk instead of mirroring it is the whole point.

Two costs sit on this boundary, not one. The walk is the first; `#snapshot()`
— `cloneState(toRaw(state))`, a full encode+revive — is the second. Both scale
with total state size rather than with the size of the change, and since #128
made the walk ~9× cheaper, the snapshot is the larger of the two.

So the boundary builds a snapshot **lazily, and at most once**, and two kinds
of subscriber avoid it entirely (#129):

| subscriber | gets | pays for a snapshot |
|---|---|---|
| `ctx.changes()` | the state | yes, once per boundary, shared with every other such subscriber |
| `ctx.changes({ throttleMs })` | the state | only when its window is closed — a boundary inside an open window builds nothing |
| a `$live` watch (`openWatch`) | nothing | **never** |

The last row is the one that is easy to get wrong again. `ctx.changes()` yields
state, but a watch subscriber does not want state — it wants to know it should
re-invoke a read method. `createSharedWatch`'s pump reads `const { done } =
await iterator.next()` and never touches `value`, so it subscribes with the
internal `ticksOnly` flag and receives a shared frozen sentinel. **If you ever
make that pump read the value, you re-introduce a full clone of the whole state
per mutating turn per shared watch** — which is what it cost before #129.

Throttling is leading-plus-trailing, and the trailing emit takes a *fresh*
snapshot so a throttled consumer never receives a state older than the window
it waited out. It fires off the `ActorScheduler` seam, out of turn — the same
trade `#scheduleWriteBehind` already makes, and with the same caveat for
`reentrant: 'always'` actors, which have no between-turns state. A window still
owing an emit when the activation deactivates is flushed by `#closeSubs` before
the feed ends, because for a job's progress feed the final value is the one
that matters most.

## Reserved names

`defineActor` refuses **any** actor type starting with `$` or `@`
(`src/define.ts`), not merely `$sigx:`. Within that space:

| Reserved | Meaning |
|---|---|
| actor types starting with `$` or `@` | refused by `defineActor` |
| actor types starting with `$sigx:` | runtime-internal |
| topic names starting with `$` or `@` | refused by `topic()` |
| methods `$sigx:reminder`, `$sigx:topic` | runtime deliveries |
| `$sigx:host#stats` | the cluster's ops channel |

`$sigx:host#stats` is answered *before* any definition lookup, so an actor type
named `$sigx:host` would simply be uncallable across hosts. The public endpoint
refuses every `$sigx:`-prefixed method outright — those deliveries arrive over
the authenticated internal mount only.

## Background work

All of it runs through the [`ActorScheduler` seam](seams.md#actorscheduler--the-clock-seam):
the idle sweeper, the reminder tick, the task-roster adoption tick
([`ActorTaskLiveness`](seams.md#actortaskliveness), #310), `ctx.timer`,
write-behind flushes. Call
deadlines and the shutdown drain deliberately do not — they are scoped to an
in-flight request or to `stop()`, and a runtime that redirects them would break
the drain.

An activation deactivates after `idleAfterMs` (default 20 min) with no
activity; `ctx.timer` ticks do not count as activity unless `keepAlive` is set.
On Cloudflare there is no sweeper at all (`sweepIntervalMs: 0`) because
platform eviction *is* idle collection — and since eviction destroys isolate,
host and activation together, **`onDeactivate` never runs there**.

## Observing turns

`registry.observeTurns((ref, method, queuedMs, elapsedMs, failed, call) => …)`
covers **dispatched turns only**, including reminder delivery. Deliberately
excluded: volatile `ctx.timer` ticks, write-behind flushes, and reentrant
`ctx.actor` inline calls. When the last observer leaves, the runtime stops
timing turns entirely — which is what makes `metrics()` cheap to leave off.

`failed` means **the method invocation threw**, not "the caller saw an error"
(#53). The observer fires in the turn's `finally` *before* `#afterTurn` — the
post-turn bookkeeping that folds the turn's writes into the version, fans the
boundary out to `ctx.changes()` subscribers and schedules a write-behind flush
— so `elapsedMs` stops at the method too, and a bookkeeping failure (a
boundary snapshot whose codec throws) reaches the caller for a turn the
observer already reported with `failed: false`. That ordering is deliberate
and test-pinned (`turn-observer-failed.test.ts`). "The caller errored" is
measured at the dispatch seam, where a middleware sees every rejection and the
`metrics` plugin's `calls.failed` already counts it.

A snapshot that throws does not leave the turn half-bookkept (#338). The
fan-out runs to the end — the first snapshot error is held, the ticks-only
subscribers a shared watch holds (which never touch the codec) are still
delivered, the remaining value subscribers are skipped rather than asking
the codec again for the same state, and the throttle window the failing
subscriber had just opened is closed again — and it runs inside a
`try/finally`: the write-behind debounce is still armed, a prepared
whole-state snapshot nobody took is still released, and a pending fault
report or `ctx.deactivate()` request is still handed to the host before the
error is rethrown. The boundary itself is consumed *before* the fan-out and
stays consumed — retrying it from the next turn would make a read-only turn
reject for a write it never made — so the value subscribers that missed it
catch up on the next mutating boundary, whose snapshot is whole-state and
carries everything the missed one did.
