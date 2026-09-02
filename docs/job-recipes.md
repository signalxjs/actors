# Job recipes

Patterns deliberately left out of the `@sigx/actors/job` API — each is a
few lines of composition over primitives that already exist, and baking
any of them in would have picked one consumer's policy for everyone.
Prerequisites: [Tasks](https://sigx.dev/actors/docs/tasks/) and
[Jobs](https://sigx.dev/actors/docs/jobs/).

## Cron on reminders — a scheduler that survives scale-out

An in-process cron library (`node-cron` and friends) breaks the moment you
run two replicas: every instance fires every schedule. Reminders already
solve this — they live in storage, fire through placement, and their
shards are re-owned when a host dies. A schedule is just an actor per
scheduled thing, re-arming a **one-shot** reminder per occurrence (which
sidesteps the 60s *period* floor; minute-resolution cron fits reminder
coarseness fine):

```ts
import { defineActor, type ReminderApi } from '@sigx/actors';
import { CronExpressionParser } from 'cron-parser'; // any cron lib
import { ExecutionJob } from './execution.job';

export const WorkflowSchedule = defineActor({
    type: 'WorkflowSchedule', // key = the workflow id
    use: [adminGuard],
    state: () => ({ cron: null as string | null, tz: 'UTC', lastRun: null as string | null }),
    methods: (ctx) => ({
        async enable(cron: string, tz = 'UTC') {
            ctx.state.cron = cron;
            ctx.state.tz = tz;
            await ctx.save();
            await arm(ctx);
        },
        async disable() {
            ctx.state.cron = null;
            await ctx.save();
            await ctx.reminders.clear('fire');
        }
    }),
    onReminder: async (ctx, name) => {
        if (name !== 'fire' || !ctx.state.cron) return;
        const runId = `${ctx.key}-${Date.now().toString(36)}`;
        // Fire-and-return: the job runs detached on whatever host placement
        // picks; this actor's only business is the calendar.
        await ctx.actor(ExecutionJob, runId).start({ workflowId: ctx.key });
        ctx.state.lastRun = runId;
        await ctx.save();
        await arm(ctx); // re-arm for the NEXT occurrence
    }
});

async function arm(ctx: { state: { cron: string | null; tz: string }; reminders: ReminderApi }) {
    if (!ctx.state.cron) return;
    const next = CronExpressionParser.parse(ctx.state.cron, { tz: ctx.state.tz }).next();
    await ctx.reminders.set('fire', { due: next.getTime() - Date.now() });
}
```

Properties you get for free: exactly one host fires each schedule (shard
ownership + the reminder CAS), a dead host's schedules move to survivors,
and the schedule state is queryable like any other actor. Reminder
resolution is coarse — "at or after due, within a tick" — which is the
right contract for cron; if you need sub-minute precision you want a
`ctx.timer` on a resident actor, not a reminder.

## The queue-worker actor — strict ordering, bounded concurrency

`defineJob`'s blessed shape is one actor per run: maximal parallelism,
per-run addressing. When you need the OPPOSITE — a queue that processes
items one at a time (or N at a time) in order — use a singleton actor
whose state is the queue, feeding per-run jobs and admitting new ones as
finished ones drain:

```ts
import { defineActor } from '@sigx/actors';
import { CrunchJob } from './crunch.job';

export const CrunchQueue = defineActor({
    type: 'CrunchQueue', // key = the queue name; one activation = the lock
    use: [adminGuard],
    state: () => ({
        pending: [] as { runId: string; input: unknown }[],
        active: [] as string[],
        limit: 2
    }),
    methods: (ctx) => ({
        async enqueue(runId: string, input: unknown) {
            ctx.state.pending.push({ runId, input });
            await ctx.save();
            await pump(ctx);
            return { position: ctx.state.pending.length, active: ctx.state.active.length };
        },
        /** Call this from the job's completion (or poll via a timer). */
        async done(runId: string) {
            ctx.state.active = ctx.state.active.filter((id) => id !== runId);
            await ctx.save();
            await pump(ctx);
        },
        stats: () => ctx.snapshot()
    }),
    // Re-activated after a crash? Reconcile: anything 'active' that has
    // reached a terminal status is done; then pump.
    onActivate: async (ctx) => {
        const still = [];
        for (const id of ctx.state.active) {
            const info = await ctx.actor(CrunchJob, id).status();
            if (info.status === 'running' || info.status === 'paused') still.push(id);
        }
        ctx.state.active = still;
        await ctx.save();
        await pump(ctx);
    }
});

async function pump(ctx) {
    while (ctx.state.active.length < ctx.state.limit && ctx.state.pending.length > 0) {
        const next = ctx.state.pending.shift();
        ctx.state.active.push(next.runId);
        await ctx.save(); // admit durably BEFORE starting (idempotent start heals a crash between)
        await ctx.actor(CrunchJob, next.runId).start(next.input);
    }
}
```

The single-activation guarantee is the lock: two callers cannot pump the
same queue concurrently, however many hosts take their calls. Completion
callbacks beat polling; the simplest wiring is a final line in the job's
`run()` that calls `job.update()` plus a queue-side `ctx.timer` reconcile —
or just have the job's caller invoke `done()`. Keep the queue actor's
turns small; the JOBS do the work.

## Cloudflare Durable Objects — checkpoint like you mean it

Jobs run on the DO backend unchanged (the task ledger lives in the
object's storage; the liveness reminder rides its alarm), but the physics
differ from a Node host:

- **Eviction is routine, not exceptional.** A DO can be evicted between any
  two awaits — far more often than a Node host dies. A job there is
  effectively *checkpoint-and-resume with short gaps*: `attempt > 1` is
  normal operation, not an incident. Checkpoint aggressively (every step,
  not every N) and size `maxAttempts` accordingly (think 20, not 3).
- **Eviction is not deactivation**: `onDeactivate` never runs and there is
  no grace window — the last durable thing wins. Never buffer more than
  one step's work between checkpoints.
- **`watch()` streams are cut on eviction.** Clients must resubscribe (the
  `$live` channel already retries); treat a dropped stream as routine.
- Progress regressing to the last checkpoint after a gap is *more visible*
  here — surface `attempts` in your UI so it reads as "resumed", not
  "restarted".

## Workflow throughput — where the ceilings are

For "many runs in flight" — thousands of jobs starting and finishing per
minute — the levers, in the order they pay:

- **A run costs five storage round trips**: the state load on activation,
  a state CAS on start and on finish, and a task-roster CAS on each side
  (#309 folded the ledger into job state; #310 made liveness a per-host
  roster with nothing periodic per running job). On a real store those are
  RTTs — the in-process floor is ~20–30 k runs/s per host, so the store's
  latency, not the runtime, is what a deployment divides by.
- **One host process per core.** A host is one JS thread, and mailboxes
  are not cores: the cluster scales linearly across processes (Tier 3: a
  pool's total went 867 → 1 863 ops/s for 3 → 7 hosts) where threads cap
  out. Run N processes per machine and let placement spread the runs.
- **`redisStorage({ url })` auto-pipelines** (#311): a host's same-tick
  CAS bursts coalesce into fewer socket writes. If you pass `client`,
  set `enableAutoPipelining: true` yourself.
- **Co-locate a queue actor with its jobs** — the `route` option's
  routing token plus an edge that hashes it keeps the enqueue→start hop
  local; `@sigx/actors-tcp` makes the hops that remain one multiplexed
  connection per peer instead of a pooled request each.
- **Throttle observers.** `job.watch({ throttleMs })` — an unthrottled
  watcher multiplies every checkpoint by the boundary snapshot.
- **Checkpoint size is the other axis**: a checkpoint re-encodes the whole
  job state, so cost grows with the run (`jobs/checkpoint-growth`,
  ~20 µs small → ~113 µs at 300 rows). Keep checkpoints to a cursor where
  you can; the O(delta) seam is #312.

## Which shape for which problem

| Problem | Shape |
|---|---|
| A run someone asks about ("how's my sync?") | `defineJob`, key = run id |
| Many independent runs in parallel | `defineJob`, one actor per run — placement spreads them |
| Strict ordering / bounded concurrency | queue-worker actor feeding jobs |
| Scheduled/recurring work | schedule actor + one-shot reminders |
| Sub-second/high-frequency ticking | `ctx.timer` on a resident actor — not reminders, not jobs |
| Human-in-the-loop wait with a deadline | `job.pause()` + `job.reminders` + `onReminder` control |
