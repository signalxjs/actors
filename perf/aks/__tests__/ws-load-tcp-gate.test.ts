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

/** A fleet on which the tcp chain is installed everywhere and every stats fan-out landed. */
const CLEAN_FLEET = { hosts: 3, tcpHosts: 3, watchesTrustworthy: true };

describe('transportGate (#223)', () => {
    it('voids a TRANSPORT=tcp run on which any link fell back', () => {
        const verdict = transportGate(
            TCP_SHAPE,
            {
                'cluster/remoteWatches': 1324,
                'cluster/transportFallbacks': 2
            },
            CLEAN_FLEET
        );
        expect(verdict).not.toBeNull();
        expect(verdict!.valid).toBe(false);
        // The count is in the message — one fallback and a fleet's worth
        // are different findings.
        expect(verdict!.message).toContain('2');
        expect(verdict!.message).toContain('not valid');
    });

    it('lets a clean tcp run stand', () => {
        expect(
            transportGate(
                TCP_SHAPE,
                {
                    'cluster/remoteWatches': 1324,
                    'cluster/transportFallbacks': 0
                },
                CLEAN_FLEET
            )
        ).toBeNull();
    });

    it('voids a TRANSPORT=tcp run whose chain is not installed on every host, whatever the delta says', () => {
        // The delta has a blind spot the runbook's own case walks into:
        // `transportDispatcher` counts a fallback once per peer at chain
        // resolution and then CACHES the transport it chose, so on a
        // stably mixed fleet every link that fell back before the `before`
        // snapshot — a previous arm in the same session, any pre-run
        // cross-host traffic — stays on HTTP for the whole run without
        // moving the counter. `tcpHosts < hosts` catches exactly that, and
        // needs no trustworthy snapshot to do it.
        const verdict = transportGate(
            TCP_SHAPE,
            { 'cluster/remoteWatches': 1324, 'cluster/transportFallbacks': 0 },
            { hosts: 3, tcpHosts: 2, watchesTrustworthy: true }
        );
        expect(verdict).not.toBeNull();
        expect(verdict!.valid).toBe(false);
        // Both numbers are in the message — "2 of 3" is the finding.
        expect(verdict!.message).toMatch(/2 of 3/);
        expect(verdict!.message).toContain('not valid');
        // …and a missing count does not soften it into a hint.
        expect(
            transportGate(TCP_SHAPE, { open: 0 }, { hosts: 3, tcpHosts: 1, watchesTrustworthy: false })!.valid
        ).toBe(false);
    });

    it('is silent when the shape is not tcp — a fallback off HTTP is not a claim the run made', () => {
        // Over HTTP there is nothing to fall back FROM; a non-zero count
        // here would be another transport's story, and a shape without a
        // knobs block at all has no TRANSPORT to gate on.
        // Nor is a fleet without tcp installed anywhere — over HTTP that is
        // the expected picture.
        const noTcp = { hosts: 3, tcpHosts: 0, watchesTrustworthy: true };
        expect(transportGate(HTTP_SHAPE, { 'cluster/transportFallbacks': 3 }, noTcp)).toBeNull();
        expect(
            transportGate('ws replicas=3 nodes=3 image=tag123', { 'cluster/transportFallbacks': 3 }, noTcp)
        ).toBeNull();
        expect(transportGate(undefined, { 'cluster/transportFallbacks': 3 }, noTcp)).toBeNull();
    });

    it('matches TRANSPORT as a whole knob, not a substring of another', () => {
        // A knob whose VALUE mentions tcp, or whose name merely ends in
        // TRANSPORT, must not turn the gate on.
        expect(
            transportGate(
                'ws replicas=3 nodes=3 image=t knobs=SOCKET_ORIGIN=tcp,X_TRANSPORT=tcp',
                { 'cluster/transportFallbacks': 3 },
                CLEAN_FLEET
            )
        ).toBeNull();
        // …and the knob's position in the list does not matter.
        expect(
            transportGate(
                'ws replicas=3 nodes=3 image=t knobs=TRANSPORT=tcp,SOCKET_PING_MS=5000',
                { 'cluster/transportFallbacks': 1 },
                CLEAN_FLEET
            )!.valid
        ).toBe(false);
    });

    it('says so, without failing the run, when the count is missing on a tcp run whose chain is installed everywhere', () => {
        // A gate that silently passes on a missing number is the "number
        // nobody knows to distrust" this rig exists to avoid — so the tcp
        // run is told the fallback check went unchecked, but not voided:
        // `tcpHosts` equals `hosts`, so the chain is installed, and the
        // absence is a collection failure, not evidence of a fallback.
        // Two causes, told apart by `watchesTrustworthy`: `cluster/*` keys
        // are omitted from the delta when a cluster-stats fan-out missed a
        // host at either end…
        const fanOut = transportGate(TCP_SHAPE, { open: 0, deliveries: 10 }, {
            hosts: 3,
            tcpHosts: 3,
            watchesTrustworthy: false
        });
        expect(fanOut).not.toBeNull();
        expect(fanOut!.valid).toBe(true);
        expect(fanOut!.message).toContain('unchecked');
        expect(fanOut!.message).toContain('missed a host');
        // …and a host image whose counters predate the field never sums
        // it at all, so the key is missing on a fully trustworthy delta.
        const oldImage = transportGate(TCP_SHAPE, { 'cluster/remoteWatches': 10 }, CLEAN_FLEET);
        expect(oldImage).not.toBeNull();
        expect(oldImage!.valid).toBe(true);
        expect(oldImage!.message).toContain('unchecked');
        expect(oldImage!.message).toContain('do not report');
        expect(oldImage!.message).not.toContain('missed a host');
    });
});
