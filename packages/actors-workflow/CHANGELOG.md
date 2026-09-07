# @sigx/actors-workflow

## [Unreleased]

### Added

- First release (#390). `defineWorkflow({ definitions, handlers })` returns
  the actor to register; one actor per run, keyed by run id, so a start is
  idempotent by construction — the id IS the idempotency key.
- `task`, `delay`, `branch` and `end` nodes. A definition is data: a branch
  is a `{ var, op, value }` triple and a task names a registered handler,
  because a definition is stored and read by a run that may resume on a
  host that never saw the process that wrote it.
- Runs are a **durable event log** folded through `applyEntry`, so a step
  costs O(entry) rather than O(state) — the shape a workflow punishes
  hardest, since its variables only grow and it takes a step per node
  (#312). A full save happens only where the record is rewritten anyway:
  at a durable sleep and at the terminal state.
- Task execution is **at-least-once**: the attempt is recorded before the
  call, so a host that dies mid-task re-runs it rather than skipping work
  it may not have done. `TaskContext.idempotencyKey` is stable per run and
  node.
- Retries are exponential with **full jitter** and a cap. Without jitter a
  fan-out that failed together retries together.
- A delay at or above `timerThresholdMs` (default 30 s) rides a durable
  reminder and the run leaves memory; below it, a volatile timer.
- Every touch — a `status()` read, an activation, a reminder — re-arms
  whatever the run is owed, for **every** non-terminal status. The engine
  this is derived from re-armed for only some, which left a run whose wake
  had gone missing invisible to every touch it had (#409); the wake fence
  is what makes the broader rule safe.
