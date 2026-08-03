/**
 * The frame codec on its own — no socket, no host.
 *
 * The binary transports (`actors-tcp`, `actors-ws`) carry none of HTTP's
 * `Request`/`Response`/webstreams overhead, so JSON parse is a far larger
 * fraction of what they do per frame than it is of `wire/endpoint-roundtrip`
 * (~5.7% there — see BASELINES.md "Where the time goes"). This is the rung
 * that shows what #218's guarded fast path (skip the pollution reviver when
 * no dangerous key can be present) is worth, and what a custom codec
 * reviver — which must always walk every node — costs relative to it.
 *
 * Timings only: parse speed is machine- and heap-dependent, so nothing here
 * is `exact`.
 */
import {
    decodeFrameBody,
    encodeFrameBody,
    FrameType,
    type Frame
} from '@sigx/actors/cluster/frames';
import { hostWireCodec } from '@sigx/actors/cluster';
import type { Metric, RunContext, Scenario } from '../types.ts';

/** A typical small unary reply — comparable to the issue's 90 B row. */
const SMALL_PAYLOAD = { data: { ok: true, count: 3, label: 'reply', values: [1, 2, 3] } };

/** ~9 KB: 200 rows, the shape a listing method returns. */
const ROWS_PAYLOAD = {
    data: Array.from({ length: 200 }, (_, i) => ({
        id: `row-${i}`,
        name: `item number ${i}`,
        value: i * 1.5,
        active: i % 2 === 0
    }))
};

function bodyFor(payload: unknown): Uint8Array {
    return encodeFrameBody({
        type: FrameType.REPLY,
        flags: 0,
        status: 0,
        corrId: 7,
        payload
    });
}

/** Drive a sync op in a timed loop; batches keep Date.now() off the hot path. */
function syncOpsPerSec(op: () => void, durationMs: number): number {
    const BATCH = 256;
    // Warmup: let the JIT tier the decode path before counting.
    for (let i = 0; i < BATCH * 4; i++) op();
    const start = performance.now();
    const deadline = start + durationMs;
    let ops = 0;
    let now = start;
    while (now < deadline) {
        for (let i = 0; i < BATCH; i++) op();
        ops += BATCH;
        now = performance.now();
    }
    return ops / ((now - start) / 1000);
}

const decode: Scenario = {
    name: 'frames/decode',
    description:
        'decodeFrameBody() alone — the guarded default reviver (fast path) vs a custom reviver (full walk)',
    async run(ctx: RunContext): Promise<Metric[]> {
        // A custom reviver is the pre-#218 cost: identity says "not the
        // default", so every node is walked.
        const customReviver = (_key: string, value: unknown): unknown => value;
        const metrics: Metric[] = [];
        for (const [size, payload] of [
            ['small', SMALL_PAYLOAD],
            ['rows-9kb', ROWS_PAYLOAD]
        ] as const) {
            const body = bodyFor(payload);
            // Sanity: both paths must decode to a frame carrying the payload.
            const check: Frame = decodeFrameBody(body, hostWireCodec.reviver);
            if (check.corrId !== 7 || check.payload === undefined) {
                throw new Error(`frames/decode: bad roundtrip for ${size}`);
            }
            for (const [variant, reviver] of [
                ['default', hostWireCodec.reviver],
                ['custom_reviver', customReviver]
            ] as const) {
                metrics.push({
                    name: `${size}/${variant}/ops_per_sec`,
                    value: syncOpsPerSec(
                        () => void decodeFrameBody(body, reviver),
                        ctx.quick ? 50 : ctx.durationMs
                    ),
                    unit: 'ops/s',
                    direction: 'higher'
                });
            }
        }
        return metrics;
    }
};

export const frameScenarios: Scenario[] = [decode];
