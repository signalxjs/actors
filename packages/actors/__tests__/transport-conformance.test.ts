/**
 * The transport conformance suite, run against `httpTransport()`.
 *
 * The suite itself lives in `@sigx/actors/cluster/testing` and knows nothing
 * about HTTP or vitest: it is a list of cases plus the harness interface a
 * transport supplies. All this file does is adapt the existing
 * `createCluster` harness to that interface and hand each case to `it()`.
 *
 * Running it against the INCUMBENT, before a second transport exists, is the
 * point. A suite written alongside a new transport tends to describe that
 * transport's habits; one the shipped default already satisfies describes
 * the contract.
 */
import { describe, expect, it } from 'vitest';
import {
    transportConformance,
    type ConformanceClusterOptions,
    type TransportConformanceFactory,
    type TransportConformanceHarness
} from '@sigx/actors/cluster/testing';
import { encodeEnvelope, signAuth, HOST_AUTH_HEADER, HOST_CALL_HEADER } from '@sigx/actors/cluster';
import type { HostDescriptor } from '@sigx/actors/cluster';
import { createCluster, type ClusterHarness } from './harness';

/**
 * `httpTransport()` is connectionless, so `openLinks` is deliberately absent
 * and the link-leak / reaping cases report as skipped rather than passing
 * vacuously. `impostor` speaks the real internal wire with the wrong secret.
 */
const createHttpCluster: TransportConformanceFactory = async (
    options: ConformanceClusterOptions
): Promise<TransportConformanceHarness> => {
    const harness: ClusterHarness = await createCluster(options.hosts, {
        actors: options.actors,
        ...(options.secret === null ? {} : { secret: options.secret ?? 'conformance-secret' }),
        ...(options.policy ? { policy: options.policy } : {}),
        ...(options.retryBackoffMs !== undefined
            ? { retryBackoffMs: options.retryBackoffMs }
            : {})
    });

    return {
        placements: harness.placements,
        hosts: harness.hosts,
        unbind: harness.unbind,
        crash: harness.crash,
        impostor: async (target: HostDescriptor) => {
            const symbol = 'ConformanceEcho#increment';
            const callId = 'c1.forged';
            const response = await harness.fetch(
                `${target.address}/_sigx/host/${encodeURIComponent(symbol)}`,
                {
                    method: 'POST',
                    headers: {
                        'content-type': 'application/json',
                        [HOST_CALL_HEADER]: encodeEnvelope(
                            { callChain: [], callId },
                            's.attacker'
                        ),
                        [HOST_AUTH_HEADER]: await signAuth('wrong-secret', symbol, callId)
                    },
                    body: JSON.stringify({ args: ['forged', 1] })
                }
            );
            return { ok: response.ok, status: response.status };
        },
        stop: harness.stop
    };
};

describe('transport conformance: httpTransport()', () => {
    for (const testCase of transportConformance) {
        it(testCase.name, async (ctx) => {
            const outcome = await testCase.run(createHttpCluster);
            if (outcome && 'skipped' in outcome) {
                // Reported as SKIPPED, never as a pass. A case that cannot be
                // expressed by this transport must not read as one it satisfies
                // — that is how a suite quietly stops testing anything.
                ctx.skip(outcome.skipped);
            }
            expect(outcome).toBeUndefined();
        });
    }

    it('covers every case exactly once, with a stated reason', () => {
        const names = transportConformance.map((c) => c.name);
        expect(new Set(names).size).toBe(names.length);
        for (const c of transportConformance) {
            expect(c.why.length).toBeGreaterThan(20);
        }
    });
});
