// @vitest-environment node
/**
 * The TCP gate of a `ws-load` hand-run (#223): the one decision
 * `testenv.mjs` makes off `cluster/transportFallbacks` once the rows are
 * in, extracted so it can be pinned without an estate.
 *
 * The runbook's rule for a `TRANSPORT=tcp` measurement is that no link may
 * have fallen through to HTTP — `tcpTransport` routes to null for a peer
 * advertising no tcp address and the chain falls back PER LINK, so a
 * half-rolled-out fleet yields a clean HTTP number wearing the tcp label.
 * Until now the rig printed nothing that could check it; the verdict here
 * is what turns a silent fallback into a non-zero exit, the way
 * `protocolBreaches` already does.
 */
import { describe, expect, it } from 'vitest';
import { transportGate } from '../deploy/ws-load.mjs';

const TCP_SHAPE =
    'ws replicas=3 nodes=3 image=tag123 knobs=ENABLE_SOCKET=1,FETCH_CONNECTIONS=1024,TRANSPORT=tcp';
const HTTP_SHAPE =
    'ws replicas=3 nodes=3 image=tag123 knobs=ENABLE_SOCKET=1,FETCH_CONNECTIONS=1024';

describe('transportGate (#223)', () => {
    it('voids a TRANSPORT=tcp run on which any link fell back', () => {
        const verdict = transportGate(TCP_SHAPE, {
            'cluster/remoteWatches': 1324,
            'cluster/transportFallbacks': 2
        });
        expect(verdict).not.toBeNull();
        expect(verdict!.valid).toBe(false);
        // The count is in the message — one fallback and a fleet's worth
        // are different findings.
        expect(verdict!.message).toContain('2');
        expect(verdict!.message).toContain('not valid');
    });

    it('lets a clean tcp run stand', () => {
        expect(
            transportGate(TCP_SHAPE, {
                'cluster/remoteWatches': 1324,
                'cluster/transportFallbacks': 0
            })
        ).toBeNull();
    });

    it('is silent when the shape is not tcp — a fallback off HTTP is not a claim the run made', () => {
        // Over HTTP there is nothing to fall back FROM; a non-zero count
        // here would be another transport's story, and a shape without a
        // knobs block at all has no TRANSPORT to gate on.
        expect(transportGate(HTTP_SHAPE, { 'cluster/transportFallbacks': 3 })).toBeNull();
        expect(transportGate('ws replicas=3 nodes=3 image=tag123', { 'cluster/transportFallbacks': 3 })).toBeNull();
        expect(transportGate(undefined, { 'cluster/transportFallbacks': 3 })).toBeNull();
    });

    it('matches TRANSPORT as a whole knob, not a substring of another', () => {
        // A knob whose VALUE mentions tcp, or whose name merely ends in
        // TRANSPORT, must not turn the gate on.
        expect(
            transportGate(
                'ws replicas=3 nodes=3 image=t knobs=SOCKET_ORIGIN=tcp,X_TRANSPORT=tcp',
                { 'cluster/transportFallbacks': 3 }
            )
        ).toBeNull();
        // …and the knob's position in the list does not matter.
        expect(
            transportGate('ws replicas=3 nodes=3 image=t knobs=TRANSPORT=tcp,SOCKET_PING_MS=5000', {
                'cluster/transportFallbacks': 1
            })!.valid
        ).toBe(false);
    });

    it('says so, without failing the run, when the count is missing on a tcp run', () => {
        // `cluster/*` keys are omitted from the delta when a cluster-stats
        // fan-out missed a host at either end. A gate that silently passes
        // on a missing number is the "number nobody knows to distrust"
        // this rig exists to avoid — so the tcp run is told the gate went
        // unchecked, but not voided: the absence is a snapshot failure,
        // not evidence of a fallback.
        const verdict = transportGate(TCP_SHAPE, { open: 0, deliveries: 10 });
        expect(verdict).not.toBeNull();
        expect(verdict!.valid).toBe(true);
        expect(verdict!.message).toContain('unchecked');
    });
});
