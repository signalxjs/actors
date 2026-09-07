// @vitest-environment node
/**
 * `awaitRollout` must outlast the rollout the chart actually describes.
 *
 * `ws-up` reported "UPGRADE FAILED: context deadline exceeded" for a
 * five-replica host-per-core arm that had converged moments later (#424).
 * Two timeouts were held against one rollout — helm's `--wait --timeout
 * 10m` and a `kubectl rollout status --timeout=420s` on the next line — so
 * the shorter decided, and neither could say whether the pods were slow,
 * crash-looping or unschedulable.
 *
 * The point of this test is that the replacement timeout is ARITHMETIC
 * over the chart, not a bigger guess: it reads the probe and grace values
 * out of `values.yaml`, so raising `failureThreshold` or the drain
 * deadline without revisiting the wait turns it red instead of costing
 * another paid dispatch.
 */
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const source = readFileSync(fileURLToPath(new URL('../deploy/testenv.mjs', import.meta.url)), 'utf8');
const values = readFileSync(fileURLToPath(new URL('../deploy/chart/values.yaml', import.meta.url)), 'utf8');

/** `awaitRollout`'s timeout expression, evaluated for a replica count. */
function timeoutFor(replicas: number): number {
    const expr = /const timeoutS = ([^;]+);/.exec(source)?.[1];
    if (!expr) throw new Error('awaitRollout timeout expression not found — did it move?');
    return Function('replicas', `return (${expr});`)(replicas) as number;
}

const num = (key: string, block?: string) =>
    Number(new RegExp(`${key}:\\s*(\\d+)`).exec(block ?? values)?.[1]);

describe('awaitRollout', () => {
    it('outlasts the worst case the chart itself describes', () => {
        const startup = /startup:\s*\n([\s\S]*?)\n  liveness:/.exec(values)?.[1] ?? '';
        const startupBudget = num('failureThreshold', startup) * num('periodSeconds', startup);
        const grace = num('terminationGracePeriodSeconds');
        expect(startupBudget).toBeGreaterThan(0);
        expect(grace).toBeGreaterThan(0);

        // pdb.maxUnavailable is 1, so pods are replaced roughly one at a
        // time: each costs its startup budget plus the grace period of the
        // pod it replaces.
        const perPod = startupBudget + grace;
        for (const replicas of [1, 3, 5, 7]) {
            expect(timeoutFor(replicas)).toBeGreaterThan(replicas * perPod);
        }
        // The arm that produced the bug: five replicas is 600s of chart,
        // which the retired 420s could never have covered and helm's 10m
        // only barely did.
        expect(5 * perPod).toBe(600);
        expect(timeoutFor(5)).toBeGreaterThan(600);
        expect(timeoutFor(5)).toBeGreaterThan(420);
    });

    it('leaves helm no second, shorter gate of its own', () => {
        const wsUp = /async function wsUp\(args\) \{([\s\S]*?)\n\}/.exec(source)?.[1];
        if (!wsUp) throw new Error('wsUp not found in testenv.mjs — did it move?');
        expect(wsUp).toContain('awaitRollout(');
        // The failure mode is a SECOND wait, so assert on the helm call
        // rather than on the whole function: `--wait` reappearing anywhere
        // in wsUp is the regression.
        expect(wsUp).not.toContain("'--wait'");
        expect(wsUp).not.toContain("'--timeout'");
    });
});
