// @vitest-environment node
/**
 * The ladder-control arithmetic of #222 — the part of `ws-loadgen.mjs` a
 * recorded number depends on without any cluster being involved.
 *
 * The incident this pins: 4 connect failures at n=100 (a cluster still
 * converging after a rollover) aborted the whole ladder under the old
 * `> 0` rule, and had it been the `ws-bench` path the artifact would have
 * recorded a ceiling that was actually a transient. The re-run was clean at
 * every rung. So the rule is now: a rate within the threshold continues, a
 * rate over it earns exactly one retry, and only a retry that ALSO fails
 * stops the ladder — with the reason on the summary line, so a truncated
 * ladder is distinguishable from a completed one in the artifact.
 */
import { describe, expect, it } from 'vitest';
import { connectFailureRate, decideLadder } from '../src/loadgen/ladder.ts';

describe('connectFailureRate', () => {
    it('divides failures by ATTEMPTED dials — the same denominator the bench report uses', () => {
        // benchmarks/src/scenarios/sockets.ts `connect_failure_rate` is
        // failures / (connected + failures); the generator must agree or the
        // two would judge the same rung differently.
        expect(connectFailureRate({ connected: 96, connectFailures: 4 })).toBeCloseTo(0.04);
        expect(connectFailureRate({ connected: 216, connectFailures: 34 })).toBeCloseTo(0.136);
    });

    it('reads 0 when nothing was attempted, not NaN', () => {
        expect(connectFailureRate({ connected: 0, connectFailures: 0 })).toBe(0);
    });
});

describe('decideLadder (#222)', () => {
    const opts = { maxFailureRate: 0.01 };

    it('continues on a clean rung', () => {
        expect(decideLadder({ n: 100, connected: 100, connectFailures: 0 }, opts)).toEqual({
            action: 'continue'
        });
    });

    it('continues within the threshold — a blip under the noise floor is not a ceiling', () => {
        // 5/1000 = 0.5%: under the same 1% floor `connect_failure_rate`
        // carries in the bench report.
        expect(
            decideLadder({ n: 1000, connected: 995, connectFailures: 5 }, opts).action
        ).toBe('continue');
    });

    it('continues at EXACTLY the threshold — the rule is "over", not "at"', () => {
        expect(
            decideLadder({ n: 1000, connected: 990, connectFailures: 10 }, opts).action
        ).toBe('continue');
    });

    it('retries once over the threshold — the #222 incident, 4 failures at n=100', () => {
        const decision = decideLadder({ n: 100, connected: 96, connectFailures: 4 }, opts);
        expect(decision.action).toBe('retry');
        expect(decision).toMatchObject({ reason: expect.stringContaining('4/100') });
    });

    it('stops when the RETRY is also over the threshold, and the reason says so', () => {
        // 34/250 is a real ceiling as measured on HTTP — it must still stop.
        const decision = decideLadder(
            { n: 250, connected: 216, connectFailures: 34 },
            { ...opts, retried: true }
        );
        expect(decision.action).toBe('stop');
        if (decision.action !== 'stop') return;
        expect(decision.reason).toContain('34/250');
        expect(decision.reason).toContain('n=250');
        expect(decision.reason).toContain('retry');
    });

    it('a rung where nothing connected still gets its one retry, then stops', () => {
        const first = decideLadder({ n: 100, connected: 0, connectFailures: 100 }, opts);
        expect(first.action).toBe('retry');
        const second = decideLadder(
            { n: 100, connected: 0, connectFailures: 100 },
            { ...opts, retried: true }
        );
        expect(second.action).toBe('stop');
    });

    it('a clean retry continues the ladder — the transient is forgiven, not recorded', () => {
        expect(
            decideLadder({ n: 100, connected: 100, connectFailures: 0 }, { ...opts, retried: true })
                .action
        ).toBe('continue');
    });
});
