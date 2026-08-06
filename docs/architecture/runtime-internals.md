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
the idle sweeper, the reminder tick, `ctx.timer`, write-behind flushes. Call
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
