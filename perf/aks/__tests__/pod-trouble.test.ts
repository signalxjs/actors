// @vitest-environment node
/**
 * `podTrouble` decides whether a pod that has not started is merely slow
 * or genuinely stuck — the difference between waiting and failing.
 *
 * It exists because both load verbs polled a Job until their timeout and
 * reported nothing else (#426). A generator pod that could never be
 * scheduled (#427) therefore cost a full hour per dispatch, three times
 * over, with the only output being "The operation was canceled."
 */
import { describe, expect, it } from 'vitest';
// @ts-expect-error — plain .mjs rig script, no types
import { podTrouble, BLOCKED_REASONS } from '../deploy/pod-trouble.mjs';

/** A `kube` that answers one `get pods -o json` with the given items. */
const kubeReturning = (items: unknown) => () => JSON.stringify({ items });

describe('podTrouble', () => {
    it('calls an unpullable image blocked, and says which image', () => {
        const kube = kubeReturning([{
            metadata: { name: 'gen-0-abc' },
            status: {
                phase: 'Pending',
                containerStatuses: [{
                    name: 'loadgen',
                    state: { waiting: { reason: 'ImagePullBackOff', message: 'manifest for r:e4d2136 not found' } }
                }]
            }
        }]);
        const { blocked, lines } = podTrouble(kube, 'ns', 'job-name=gen');
        expect(blocked).toBe(true);
        expect(lines[0]).toContain('gen-0-abc/loadgen');
        expect(lines[0]).toContain('ImagePullBackOff');
        // The message is the part that names the tag, which is what the
        // operator actually needs — #427 was a tag that had no image.
        expect(lines[0]).toContain('e4d2136');
    });

    it('reports an unschedulable pod WITHOUT calling it blocked', () => {
        // The distinction that matters: the cluster autoscaler may still
        // add a node, so this must not fail the run. It cost hours to
        // diagnose precisely because nothing ever printed it.
        const kube = kubeReturning([{
            metadata: { name: 'gen-0-def' },
            status: {
                phase: 'Pending',
                conditions: [{
                    type: 'PodScheduled', status: 'False', reason: 'Unschedulable',
                    message: '0/4 nodes are available: 2 node(s) had untolerated taint(s).'
                }]
            }
        }]);
        const { blocked, lines } = podTrouble(kube, 'ns', 'job-name=gen');
        expect(blocked).toBe(false);
        expect(lines[0]).toContain('Unschedulable');
        expect(lines[0]).toContain('untolerated taint');
    });

    it('says nothing about pods that are fine', () => {
        const kube = kubeReturning([
            { metadata: { name: 'a' }, status: { phase: 'Running' } },
            { metadata: { name: 'b' }, status: { phase: 'Succeeded' } }
        ]);
        expect(podTrouble(kube, 'ns', 's')).toEqual({ blocked: false, lines: [] });
    });

    it('treats a back-off as the steady state it is', () => {
        // Not "still retrying, give it time": kubelet retries an unpullable
        // image forever, so waiting longer cannot change the outcome.
        expect(BLOCKED_REASONS.has('ImagePullBackOff')).toBe(true);
        expect(BLOCKED_REASONS.has('CrashLoopBackOff')).toBe(true);
        // Whereas these resolve on their own and must never fail a run.
        expect(BLOCKED_REASONS.has('ContainerCreating')).toBe(false);
        expect(BLOCKED_REASONS.has('PodInitializing')).toBe(false);
    });

    it('survives a kubectl that fails or answers with nonsense', () => {
        // `allowFail` returns null, and a partial cluster read must not
        // take down the verb that was only trying to explain itself.
        expect(podTrouble(() => null, 'ns', 's')).toEqual({ blocked: false, lines: [] });
        expect(podTrouble(() => 'not json', 'ns', 's')).toEqual({ blocked: false, lines: [] });
    });
});
