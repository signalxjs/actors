/**
 * `engine/unit-costs` — what ONE workflow run costs the runtime, as
 * invariants (#379, the workflow axis's `directory-ops-per-activation`).
 *
 * Every `workflow/*` number is a Tier-3 measurement: Poisson arrivals,
 * random placement, a real clock, a reminder tick quantising every wake —
 * nothing there can be `exact`, so nothing there gates a merge. This is the
 * same engine (`perf/aks/src/workflow`, the pinned Tier-3 workload) in ONE
 * process on ONE host under `selfPolicy`, driven one run at a time, and it
 * counts what the runtime was asked to do per run: state saves, node
 * transitions, directory calls, reminder-shard writes, completion
 * deliveries. Those are pure functions of the definition and the engine's
 * code — a fixed loop over deterministic actors — which is the `Metric.exact`
 * contract, so every metric here is `exact` and every one gates.
 *
 * What makes the counts deterministic, and what would break it:
 *
 * - `selfPolicy` on a single host: a child run is placed locally, a cold
 *   activation costs the same directory calls every time. Random or
 *   consistent-hash placement would move `directory_ops` between runs.
 * - One run at a time, after a discarded warm-up run per arm: the
 *   definition read (cached per host per `name@version`), the singleton
 *   aggregator's first activation and the worker pool's growth all land in
 *   the warm-up, so a counted run is the STEADY-STATE cost.
 * - The engine's own retries are pushed out of reach — the notify-retry
 *   and join-watchdog wakes are armed minutes out, so a counted run never
 *   re-sends a `childDone` or re-issues a child start. Their counters are
 *   reported anyway: a non-zero value is a bug in the arm, not noise.
 * - Failure injection is off (`failureRate: 0`), so attempts are fixed.
 * - The durable-sleep arm ticks reminders every 50 ms of real time, and
 *   waits on the aggregator's completion event rather than polling the run:
 *   a `status()` poll is the touch that re-activates a deliberately
 *   deactivated run, which would make its directory count depend on when
 *   the poll landed relative to the tick.
 *
 * Named `engine/`, not `workflow/`: `wf-bench` selects `workflow/` by
 * substring, and a Tier-1 scenario under that prefix would run inside the
 * paid Tier-3 job.
 *
 * The engine reads its `WF_*` knobs at module load. They are set for the
 * duration of the dynamic import and restored after it, so the bench
 * process's environment is untouched for anything that runs later.
 */
import { memoryStorage } from '@sigx/actors/host';
import type { ActorStorage, Host, HostDefaults } from '@sigx/actors/host';
import { createCluster, selfPolicy } from '../cluster-harness.ts';
import type { Metric, RunContext, Scenario } from '../types.ts';

type Engine = typeof import('../../../perf/aks/src/workflow/index.ts');
type TemplateKnobs = Engine['DEFAULT_KNOBS'];

/**
 * The engine's shape for this scenario, in the knobs `config.ts` documents.
 * A 100 ms timer threshold is what lets the two wake kinds be reached with
 * millisecond delays; the retry cadences are minutes so no counted run
 * ever re-sends anything.
 */
const ENGINE_ENV: Record<string, string> = {
    WF_TIMER_THRESHOLD_MS: '100',
    WF_DEACTIVATE_ON_SLEEP: '1',
    WF_IDLE_AFTER_MS: '3600000',
    WF_NOTIFY_RETRY_MS: '600000',
    WF_CHILD_STALE_MS: '600000',
    WF_STALE_WAKE_MS: '600000',
    WF_STATS_SAVE_EVERY: '1'
};

/**
 * `actors.app.ts` (which the engine's actors are bound to) reads these at
 * load too. `REDIS_URL` would build the app on a Redis store the scenario
 * never uses; the two host knobs are only meaningful on that app. All
 * three are cleared for the import so a bench process that also runs the
 * `redis/*` scenarios still loads the engine on memory storage.
 */
const CLEARED_ENV = ['REDIS_URL', 'WF_REMINDER_TICK_MS', 'WF_CALL_TIMEOUT_MS'];

let enginePromise: Promise<Engine> | null = null;

function loadEngine(): Promise<Engine> {
    enginePromise ??= (async () => {
        const previous = new Map<string, string | undefined>();
        for (const name of [...Object.keys(ENGINE_ENV), ...CLEARED_ENV]) {
            previous.set(name, process.env[name]);
        }
        for (const [name, value] of Object.entries(ENGINE_ENV)) process.env[name] = value;
        for (const name of CLEARED_ENV) delete process.env[name];
        // The engine logs through `__DEV__` on its failure paths. The prod
        // dist has the flag compiled away; the engine's own source does
        // not, and an undefined global there would turn a real failure
        // into a ReferenceError that hides it.
        if (!('__DEV__' in globalThis)) Object.assign(globalThis, { __DEV__: false });
        try {
            return await import('../../../perf/aks/src/workflow/index.ts');
        } finally {
            for (const [name, value] of previous) {
                if (value === undefined) delete process.env[name];
                else process.env[name] = value;
            }
        }
    })();
    return enginePromise;
}

/** The reserved type the sharded reminder provider keeps its 16 shard records under. */
const REMINDER_TYPE = '$sigx:reminders';

/**
 * A storage whose successful WRITES are counted per record type — the
 * reminder-shard rewrites a durable sleep costs are the number this
 * scenario exists to pin, and they are invisible in the engine's own
 * counters (which count `ctx.save()` calls on the run actor). Counted on
 * settlement, so a rejected CAS is not a write the store never held.
 */
function writeCountingStorage(inner: ActorStorage): {
    storage: ActorStorage;
    writes(type: string): number;
    reset(): void;
} {
    const writes = new Map<string, number>();
    const bump = (type: string): void => {
        writes.set(type, (writes.get(type) ?? 0) + 1);
    };
    const storage: ActorStorage = {
        load: (type, key) => inner.load(type, key),
        save: (type, key, state, expected) =>
            inner.save(type, key, state, expected).then((etag) => {
                bump(type);
                return etag;
            }),
        clear: (type, key, expected) => inner.clear(type, key, expected)
    };
    // Forwarded only when the inner store has them: their presence routes
    // the host onto the single-walk and O(entry) paths, and a wrapper that
    // always declared them would change what is being counted.
    if (inner.saveText) {
        const saveText = inner.saveText.bind(inner);
        storage.saveText = (type, key, json, expected) =>
            saveText(type, key, json, expected).then((etag) => {
                bump(type);
                return etag;
            });
    }
    if (inner.appendText) {
        const appendText = inner.appendText.bind(inner);
        storage.appendText = (type, key, json, expected) =>
            appendText(type, key, json, expected).then((etag) => {
                bump(type);
                return etag;
            });
    }
    return {
        storage,
        writes: (type) => writes.get(type) ?? 0,
        reset: () => writes.clear()
    };
}

interface Arm {
    /** Metric prefix. */
    label: string;
    /** The template every counted run starts. */
    template: 'order' | 'etl';
    knobs: Partial<TemplateKnobs>;
    /** Host defaults over `quiet` — the durable arm needs a live reminder tick. */
    defaults?: HostDefaults;
}

/**
 * Three arms, each moving one thing:
 *
 *   order     a 10 ms delay under the threshold — a run whose wake rides a
 *             VOLATILE timer and never touches the reminder shards.
 *   etl/w=8   eight child runs and the durable join — the cross-actor
 *             shape: child starts, `childDone` calls, the watchdog reminder.
 *   sleep     `order` with a 150 ms delay OVER the threshold — the run arms
 *             a durable reminder, leaves memory, and is woken by the tick.
 */
const ARMS: readonly Arm[] = [
    { label: 'order', template: 'order', knobs: { delayMs: 10 } },
    { label: 'etl/w=8', template: 'etl', knobs: { delayMs: 10, fanoutWidth: 8, fanoutMode: 'children' } },
    {
        label: 'sleep',
        template: 'order',
        knobs: { delayMs: 150 },
        // Real time: the tick is what fires a durable wake, and a tick
        // every 50 ms means a 150 ms sleep wakes within one of them.
        defaults: { reminderTickMs: 50 }
    }
];

const sleepMs = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Wait for `runId`'s completion event under `tag` — the aggregator is the
 *  one actor a poll may touch without changing what is being counted. */
async function untilCompleted(host: Host, wf: Engine, tag: string, runId: string): Promise<void> {
    const stats = host.actor(wf.WorkflowStats, 'all');
    const deadline = Date.now() + 30_000;
    for (;;) {
        const { events } = await stats.drain(tag, 0, 100_000);
        const event = events.find((e) => e.runId === runId);
        if (event) {
            if (event.status !== 'completed') {
                throw new Error(`[engine/unit-costs] ${runId} ended ${event.status}`);
            }
            return;
        }
        if (Date.now() > deadline) throw new Error(`[engine/unit-costs] ${runId} did not complete in 30 s`);
        await sleepMs(5);
    }
}

const unitCosts: Scenario = {
    name: 'engine/unit-costs',
    description: 'saves, transitions, directory ops, shard writes and deliveries per workflow run — invariants',
    async run(ctx: RunContext): Promise<Metric[]> {
        const wf = await loadEngine();
        const runs = ctx.quick ? 3 : 10;
        const metrics: Metric[] = [];

        for (const arm of ARMS) {
            const knobs: TemplateKnobs = {
                ...wf.DEFAULT_KNOBS,
                taskMs: 1,
                failureRate: 0,
                retryBackoffMs: 1,
                ...arm.knobs
            };
            const version = wf.seedVersionFor(knobs);
            const counted = writeCountingStorage(memoryStorage());
            const harness = await createCluster(1, {
                actors: [...wf.workflowActors],
                policy: selfPolicy,
                storage: counted.storage,
                ...(arm.defaults ? { defaults: arm.defaults } : {})
            });
            try {
                const host = harness.hosts[0] as (typeof harness.hosts)[number];
                for (const def of wf.allDefinitions({ ...knobs, version })) {
                    await host.actor(wf.WorkflowDefinition, def.name).put(def);
                }
                const tag = `unit-${arm.label}`;
                const start = async (runId: string): Promise<void> => {
                    await host
                        .actor(wf.WorkflowRun, runId)
                        .start({ workflow: arm.template, version, template: arm.template, tag });
                    await untilCompleted(host, wf, tag, runId);
                };

                // The discarded run: definition read, aggregator activation,
                // pool growth. Everything after it is steady state.
                await start(`${arm.label}/warmup`);
                harness.counter.reset();
                counted.reset();
                wf.resetCounters();

                for (let i = 0; i < runs; i++) await start(`${arm.label}/${i}`);

                const c = wf.snapshotCounters();
                const per = (total: number): number => total / runs;
                const exact = (name: string, value: number, unit = 'count'): Metric => ({
                    name: `${arm.label}/${name}`,
                    value,
                    unit,
                    direction: 'lower',
                    // A fixed loop over deterministic actors under
                    // `selfPolicy`: one more save, one more directory call
                    // or one more shard write per run is a behavioural
                    // change, and this is where it fails. No `noiseFloor`:
                    // the comparer bypasses it for an exact metric, and
                    // naming one would suggest a tolerance there is not.
                    exact: true
                });

                metrics.push(
                    exact('saves_per_run', per(c.saves)),
                    exact('transitions_per_run', per(c.transitions)),
                    exact('task_attempts_per_run', per(c.taskAttempts)),
                    exact('directory_ops_per_run', per(harness.counter.sum('directory.'))),
                    exact('reminder_sets_per_run', per(c.remindersSet)),
                    exact('reminders_fired_per_run', per(c.remindersFired)),
                    exact('shard_writes_per_run', per(counted.writes(REMINDER_TYPE))),
                    exact('deliveries_per_run', per(c.statsEvents)),
                    exact('child_starts_per_run', per(c.childStarts)),
                    exact('child_done_calls_per_run', per(c.childDoneCalls)),
                    // Zero by construction in every arm. Reported so that a
                    // retry cadence creeping into a counted run reads as
                    // the regression it is rather than as extra saves.
                    exact('retries_per_run', per(c.childDoneRetries + c.joinRepairs + c.timersRearmed)),
                    exact('wakes_lost_per_run', per(c.wakesLost + c.wakesStale))
                );
            } finally {
                await harness.stop();
            }
        }
        return metrics;
    }
};

export const engineScenarios: Scenario[] = [unitCosts];
