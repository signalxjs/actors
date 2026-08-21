/**
 * Ladder control for `ws-loadgen.mjs` (#222) — when does a rung's connect
 * failures end the climb?
 *
 * The old rule was `connectFailures > 0`, and it cost a run: 4 failures at
 * n=100 against a cluster still converging after a rollover aborted the
 * whole ladder, and the re-run was clean at every rung. Worse than the
 * wasted run is what the abort LOOKS like — had it been the `ws-bench`
 * path, `sockets/principal-cliff` would have recorded a ceiling at the
 * first rung, a plausible wrong number rather than an error.
 *
 * So the rule is now three-valued, and it lives here — pure, no I/O —
 * because the arithmetic is the part a recorded number depends on and the
 * only part a unit test can hold still:
 *
 *  - a failure RATE within `maxFailureRate` **continues** (the bench
 *    report's `connect_failure_rate` carries a 1% noise floor for the same
 *    reason — a handful of failed dials at n=1000 is weather, not a wall);
 *  - a rate over it earns exactly ONE **retry** of the same rung, because
 *    the incident's signature is a transient that a second attempt clears;
 *  - a retry that is ALSO over the threshold **stops** the ladder —
 *    climbing past a rung that could not hold its connections measures
 *    nothing but the failure mode — and the reason string travels on the
 *    summary line as `ladderStopped`, so a truncated ladder is
 *    distinguishable from a completed one in the artifact.
 *
 * The denominator is ATTEMPTED dials (`connected + connectFailures`), the
 * same one `benchmarks/src/scenarios/sockets.ts` uses for
 * `connect_failure_rate` — the generator and the report must judge a rung
 * by the same fraction or they will disagree about which rung was the
 * ceiling.
 */

/** The per-pod outcome of one rung, as `runRung` counts it. */
export interface RungOutcome {
    /** Connections this rung was asked to dial. */
    n: number;
    /** Dials that completed the upgrade AND received their first value. */
    connected: number;
    /** Dials that did not. */
    connectFailures: number;
}

export type LadderDecision =
    | { action: 'continue' }
    | { action: 'retry'; reason: string }
    | { action: 'stop'; reason: string };

/** Failures over ATTEMPTED dials; 0 when nothing was attempted. */
export function connectFailureRate(outcome: Pick<RungOutcome, 'connected' | 'connectFailures'>) {
    const attempted = outcome.connected + outcome.connectFailures;
    return attempted > 0 ? outcome.connectFailures / attempted : 0;
}

const pct = (rate: number) => `${Math.round(rate * 1000) / 10}%`;

/**
 * Judge one rung. `retried` marks an outcome that IS already the retry —
 * the second strike is the one that stops the ladder.
 */
export function decideLadder(
    outcome: RungOutcome,
    { maxFailureRate, retried = false }: { maxFailureRate: number; retried?: boolean }
): LadderDecision {
    const rate = connectFailureRate(outcome);
    if (rate <= maxFailureRate) return { action: 'continue' };
    const attempted = outcome.connected + outcome.connectFailures;
    const what =
        `connect failures ${outcome.connectFailures}/${attempted} (${pct(rate)}) ` +
        `over the ${pct(maxFailureRate)} threshold at n=${outcome.n}`;
    return retried
        ? { action: 'stop', reason: `${what}, on the retry` }
        : { action: 'retry', reason: what };
}
