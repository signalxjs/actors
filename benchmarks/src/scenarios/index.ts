import { clusterScenarios } from './cluster.ts';
import { computeScenarios } from './compute.ts';
import { tier2Scenarios } from './cluster2.ts';
import { TIER3_ENABLED, tier3Hint as tier3Reason, tier3Scenarios } from './infra.ts';
import { dispatchScenarios } from './dispatch.ts';
import { engineScenarios } from './engine.ts';
import { frameScenarios } from './frames.ts';
import { jobScenarios } from './jobs.ts';
import { lifecycleScenarios } from './lifecycle.ts';
import { memoryScenarios } from './memory.ts';
import { redisScenarios } from './redis.ts';
import { remindersRedisScenarios } from './reminders-redis.ts';
import { stateScenarios } from './state.ts';
import { statelessScenarios } from './stateless.ts';
import { liveFanoutScenarios } from './live-fanout.ts';
import { livePrincipalScenarios } from './live-principal.ts';
import { WS_ENABLED, wsHint as wsReason, socketScenarios } from './sockets.ts';
import { WF_ENABLED, wfHint as wfReason, workflowScenarios } from './workflow.ts';
import { WF_LOCAL_ENABLED, wfLocalHint as wfLocalReason, wfLocalScenarios } from './wf-local.ts';
import { topicScenarios } from './topics.ts';
import { wireScenarios } from './wire.ts';
import type { Scenario } from '../types.ts';

/**
 * Tier 2 forks a process per host and binds real ports, so it is slow and
 * touches the machine in ways the rest of the suite does not. Opt-in via
 * `BENCH_TIER2=1` (or `pnpm bench:tier2`) rather than a flag, so `pnpm bench`
 * stays exactly as fast and as side-effect-free as it was.
 */
export const TIER2_ENABLED = process.env.BENCH_TIER2 === '1';

/**
 * The `compute/*` scenarios spawn 8 OS threads, and — the real reason they
 * are gated — a thread-parallelism measurement on the ~2-core CI runner is
 * not merely noisy but structurally incapable of showing the effect. A
 * number that cannot be right on the machine recording it should not be
 * recorded there (#119).
 */
export const THREADS_ENABLED = process.env.BENCH_THREADS === '1';

/**
 * Order matters for reading the output: the dispatch ladder first, since
 * every later number is interpreted as a delta from it. Tier 2 comes last —
 * it is a different KIND of measurement (counts over real sockets, not
 * timings in one process) and must not be read as another rung of the ladder.
 */
export const ALL_SCENARIOS: Scenario[] = [
    ...dispatchScenarios,
    ...stateScenarios,
    // The next rung of the state ladder: the same codec costs, measured on
    // the `defineJob` path they were reported against (#124 / #227).
    ...jobScenarios,
    // The workflow engine's per-run invariants (#379): the Tier-1, `exact`
    // face of the `workflow/*` axis, so a runtime change on that path
    // gates here without a cluster.
    ...engineScenarios,
    ...wireScenarios,
    ...frameScenarios,
    ...lifecycleScenarios,
    ...memoryScenarios,
    ...clusterScenarios,
    ...statelessScenarios,
    ...topicScenarios,
    ...liveFanoutScenarios,
    ...livePrincipalScenarios,
    ...redisScenarios,
    // Same gate, two more questions about the sharded reminder table
    // against a real store (#382): `arm-fire` — the arm rate at which its
    // CAS starts failing; `table-size` — what a set and a tick cost with
    // P entries already asleep in the records.
    ...remindersRedisScenarios,
    ...(THREADS_ENABLED ? computeScenarios : []),
    ...(TIER2_ENABLED ? tier2Scenarios : []),
    // `wf-local/*` is Tier 2 as well — the workflow engine as N real host
    // processes on ONE box over a local Redis (#381). Counts are evidence,
    // the ratio between fleet sizes is the decision, timings are context.
    ...(WF_LOCAL_ENABLED ? wfLocalScenarios : []),
    // Tier 3 measures a DEPLOYMENT, not this process — a different kind of
    // number again, so it comes last and never reads as another rung.
    ...(TIER3_ENABLED ? tier3Scenarios : []),
    // And `sockets/*` is a different axis again WITHIN Tier 3: connections
    // held and messages delivered, driven from inside the cluster with no
    // ingress in the path. Never read as another rung of `infra/*`.
    ...(WS_ENABLED ? socketScenarios : []),
    // And `workflow/*` a third: an ENGINE on the cluster — runs per second,
    // wake lag, joins — driven from inside like `sockets/*` (#297).
    ...(WF_ENABLED ? workflowScenarios : [])
];

const TIER2_NAMES = tier2Scenarios.map((s) => s.name);
const COMPUTE_NAMES = computeScenarios.map((s) => s.name);

export function selectScenarios(filters: readonly string[]): Scenario[] {
    if (filters.length === 0) return ALL_SCENARIOS;
    return ALL_SCENARIOS.filter((s) => filters.some((f) => s.name.includes(f)));
}

/**
 * A filter matching only Tier-2 scenarios while Tier 2 is off would otherwise
 * report "no scenarios match", which reads as a typo rather than as a missing
 * opt-in. Returns the command to run, or null.
 */
export function tier2Hint(filters: readonly string[]): string | null {
    if (TIER2_ENABLED || filters.length === 0) return null;
    const wanted = filters.some((f) => TIER2_NAMES.some((name) => name.includes(f)));
    if (!wanted) return null;
    return (
        `Tier-2 scenarios are opt-in — they fork a process per host and bind real ports. Run:\n` +
        `  BENCH_TIER2=1 pnpm bench:run ${filters.join(' ')}`
    );
}

/**
 * The same courtesy for Tier 3, which is harder to diagnose: it is gated on
 * FIVE variables rather than one, so "no scenarios match" hides which of
 * them is missing.
 *
 * Matched against the real scenario NAMES, exactly as the Tier-2 hint is —
 * `tier3Scenarios` is always populated, only its inclusion in
 * `ALL_SCENARIOS` is gated, so the same test works. (Testing the filter
 * against the literal `'infra/'` instead gets it backwards: `'infra/'
 * .includes(f)` is true for any short prefix like `'i'`.)
 */
const TIER3_NAMES = tier3Scenarios.map((s) => s.name);
const WS_NAMES = socketScenarios.map((s) => s.name);
const WF_NAMES = workflowScenarios.map((s) => s.name);
const WF_LOCAL_NAMES = wfLocalScenarios.map((s) => s.name);

export function threadsHint(filters: readonly string[]): string | null {
    if (THREADS_ENABLED || filters.length === 0) return null;
    if (!filters.some((f) => COMPUTE_NAMES.some((name) => name.includes(f)))) return null;
    return (
        `compute/* scenarios are opt-in — they spawn OS threads, and the effect they ` +
        `measure cannot appear on a 2-core runner. On a multi-core box:\n` +
        `  BENCH_THREADS=1 pnpm bench:run ${filters.join(' ')}`
    );
}

export function tier3Hint(filters: readonly string[]): string | null {
    if (TIER3_ENABLED || filters.length === 0) return null;
    if (!filters.some((f) => TIER3_NAMES.some((name) => name.includes(f)))) return null;
    return tier3Reason();
}

/**
 * The same courtesy for `sockets/*`, which is gated on its own four
 * variables and — unlike `infra/*` — needs an IMAGE TAG as well as a
 * cluster: without one the Job renders against whatever `gitSha()` says,
 * which after any merge is an image the registry does not have.
 */
export function wsHint(filters: readonly string[]): string | null {
    if (WS_ENABLED || filters.length === 0) return null;
    if (!filters.some((f) => WS_NAMES.some((name) => name.includes(f)))) return null;
    const reason = wsReason();
    return reason === null ? null : `sockets/* are opt-in — ${reason}`;
}

/** `wf-local/*` — opt-in like Tier 2, plus a Redis to share (#381). */
export function wfLocalHint(filters: readonly string[]): string | null {
    if (WF_LOCAL_ENABLED || filters.length === 0) return null;
    if (!filters.some((f) => WF_LOCAL_NAMES.some((name) => name.includes(f)))) return null;
    return wfLocalReason();
}

/** `workflow/*` — gated exactly like `sockets/*`, on its own env (#297). */
export function wfHint(filters: readonly string[]): string | null {
    if (WF_ENABLED || filters.length === 0) return null;
    if (!filters.some((f) => WF_NAMES.some((name) => name.includes(f)))) return null;
    const reason = wfReason();
    return reason === null ? null : `workflow/* are opt-in — ${reason}`;
}
