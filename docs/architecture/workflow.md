# The workflow engine

> `@sigx/actors-workflow` — how a durable run survives the host it started
> on, and why each piece is shaped the way it is.

A run is one actor. Its definition is data, its state is an append-only
log, and its progress is a chain of zero-delay hops rather than a loop.
Those three choices are the whole design, and each of them was forced by
something measured on the perf rig (`perf/aks/src/workflow/`, #297) —
which stays the pinned system under test and is deliberately NOT what
this package imports.

## Why not `defineJob`

`defineJob` is the right shape for a long operation that runs to
completion while somebody watches it: it holds a `tasks:` run, which
keeps the activation resident and keeps a watcher fed.

A workflow run is the opposite shape. It spends nearly all of its life
asleep — a `delay` node, a retry backoff, a wait for an external signal —
and the target is a million of them asleep at once. An activation per
sleeping run is a million activations. So a run **leaves memory** at every
durable sleep and comes back on a reminder, and the state that survives
that round trip is the record, not the closure.

## A definition is data, never code

A `branch` is a `{ var, op, value }` triple and a `task` names a handler
the deployment registered. Neither is a function, because a definition is
stored, versioned, sent over a wire, and read by a run that may start days
later on a host whose process never saw the code that wrote it.

A run **pins the version** it started on. Editing a workflow therefore
never changes a run already in flight, and a host may cache a version
forever. A run pinned to a version this deploy no longer carries fails
loudly rather than guessing at another one: its remaining path is
genuinely unknowable, and guessing would run the wrong nodes.

## The log is the state

Every transition is `ctx.append(entry)`, not `ctx.save()`. The seam is
`appendText` (#312, #376, #377): an append is O(entry) where a whole-state
save is O(state).

What that costs is a growth curve, not a constant. `jobs/checkpoint-growth`
measured a per-step whole-state checkpoint across a 300-step run:

| Point in the run | Cost of one checkpoint |
|---|---|
| head | 19.8 µs |
| tail | 113 µs |

The step did not get harder — the state did. A whole-state save re-encodes
everything accumulated so far, every time. A workflow run is exactly the
shape that punishes that: its variables only grow, and it takes a step per
node.

`applyRunEntry` is the only writer. Live state and replayed state cannot
diverge, because there is no second path that mutates. Two properties it
must hold, both of which have a test:

- **Total.** It runs during activation, so a throw does not fail one
  call — it stands the run up dead. An unknown entry tag is ignored and a
  malformed payload is guarded, because a run written by a newer build has
  to load on an older one mid-rollout.
- **Ignorant of the reader.** No entry holds a timestamp derived from the
  reading host, or anything else two hosts could interpret differently.

Full saves happen only where the record is rewritten anyway — at a durable
sleep and at the terminal state. Those are the compaction points; between
them the log is the truth. A finished run costs one read to load.

## Wakes are fenced, and fencing is what makes them cheap

Reminder firing is at-most-once, a volatile timer dies with its host, and
a migrated run can be woken by a survivor of its previous activation. So
every sleep mints a `seq`, and a wake carrying anything but the current
token does nothing.

That fence is what lets everything else be sloppy in the cheap direction:
a duplicate reminder tick, a timer re-armed after migration and a touch
racing a real wake are all no-ops rather than cases to design around.

Two details that are not obvious and both caused a bug while this was
written:

- **`ctx.reminders.set` takes `due` as ms FROM NOW, not an epoch.** The
  absolute `until` is kept in state anyway, because that is what a touch
  arriving on another host has to compare against. They are not
  interchangeable.
- **A wake carries `after`: where to go on waking.** Without it a wake
  returns to the same cursor, and a `delay` node sleeps again on arrival —
  an infinite nap that reads as progress from outside, since the status
  keeps flipping. It also separates the two reasons to sleep: finishing a
  delay MOVES, which resets the attempt counter, while a retry backoff
  must return to the same node with its attempts intact.

## Re-arming, and the livelock it can cause

Any touch re-arms what the run is owed, for every non-terminal status
(#409: a run whose volatile wake died with its host is `sleeping`, and
skipping that status is what stranded them).

Re-arming naively is a livelock. A touch that restarts an already-counting
wake lets a caller polling `status()` faster than the delay push the wake
back on every poll, and the run never advances — a run that stops making
progress *because* it is being watched. So a wake is armed once per
activation per `seq`, and a touch arms only what is genuinely missing.

Re-arming a **durable** wake then deactivates, for the same reason the
sleep path does. Otherwise a status poll pins in memory exactly the runs
that left it.

## Retries

Exponential with full jitter, capped: attempt N waits a random point in
`[0, base × 2^(N-1)]`. The perf rig's linear, unjittered policy
synchronised every retry across a fan-out, which is the thundering herd
the jitter exists to break.

The attempt record is written **before** the call. That makes task
execution at-least-once rather than at-most-once: a host that dies there
re-runs the attempt instead of silently skipping one it may have
completed. Handlers get an `idempotencyKey` stable across attempts of the
same node of the same run, and one with side effects is expected to use
it.

## What this package does not do yet

Fan-out and sub-workflows, external signals with a timeout edge, saga
compensation, cancel cascade, per-tenant quota, schedules and webhooks all
exist in the perf rig and are not here. Each is a new state that has to
survive a host dying mid-node, so each arrives with its own tests rather
than as a batch. The scale questions they depend on are tracked in
[`scaling-roadmap.md`](scaling-roadmap.md).
