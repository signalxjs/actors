/**
 * `otelTraces()` on one host: the CLIENT span per dispatch, the SERVER span
 * per turn, parenting between them and from an external traceparent, the
 * error posture, and — the constraint the issue states outright — that with
 * no provider anywhere the middleware is a pass-through and no span exists.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { SpanKind, SpanStatusCode, trace } from '@opentelemetry/api';
import {
    BasicTracerProvider,
    InMemorySpanExporter,
    SimpleSpanProcessor,
    type ReadableSpan
} from '@opentelemetry/sdk-trace-base';
import { defineActor } from '@sigx/actors';
import { defineActorApp, type ActorApp } from '@sigx/actors/host';
import { otelTraces } from '@sigx/actors-otel';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };
const TP = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';

const Counter = defineActor({
    type: 'Counter',
    allowAnonymous: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        },
        async explode() {
            throw new Error('boom');
        }
    }),
    streams: () => ({
        async *counts() {
            yield 1;
            yield 2;
        }
    })
});

/** v2 renamed parentSpanId to parentSpanContext; accept either. */
function parentIdOf(span: ReadableSpan): string | undefined {
    const s = span as ReadableSpan & {
        parentSpanContext?: { spanId: string };
        parentSpanId?: string;
    };
    return s.parentSpanContext?.spanId ?? s.parentSpanId;
}

let running: ActorApp | null = null;
afterEach(async () => {
    await running?.stop();
    running = null;
});

async function startTraced() {
    const exporter = new InMemorySpanExporter();
    const provider = new BasicTracerProvider({
        spanProcessors: [new SimpleSpanProcessor(exporter)]
    });
    const app = defineActorApp({ actors: [Counter], defaults: quiet }).use(
        otelTraces({ tracerProvider: provider })
    );
    running = app;
    const host = await app.start();
    return { host, exporter };
}

describe('otelTraces', () => {
    it('creates a CLIENT dispatch span and a SERVER turn span, joined', async () => {
        const { host, exporter } = await startTraced();
        await host.actor(Counter, 'k1').increment(1);

        const spans = exporter.getFinishedSpans();
        expect(spans.map((s) => s.name)).toEqual(['Counter.increment', 'Counter.increment']);
        const server = spans.find((s) => s.kind === SpanKind.SERVER)!;
        const client = spans.find((s) => s.kind === SpanKind.CLIENT)!;
        // One trace; the turn is the child of the dispatch that caused it.
        expect(server.spanContext().traceId).toBe(client.spanContext().traceId);
        expect(parentIdOf(server)).toBe(client.spanContext().spanId);

        expect(client.attributes['sigx.actor.type']).toBe('Counter');
        expect(client.attributes['sigx.actor.method']).toBe('increment');
        expect(client.attributes['sigx.call.depth']).toBe(0);
        expect(typeof client.attributes['sigx.actor.key_hash']).toBe('string');
        expect(server.attributes['sigx.turn.queued_ms']).toBeGreaterThanOrEqual(0);
        expect(server.attributes['sigx.turn.failed']).toBe(false);
        // Never the raw key — as an attribute value or anywhere else.
        for (const span of spans) {
            expect(Object.values(span.attributes)).not.toContain('k1');
        }
    });

    it('parents the dispatch span to an external traceparent', async () => {
        const { host, exporter } = await startTraced();
        await host.dispatch({ type: 'Counter', key: 'k1' }, 'increment', [1], {
            callChain: [],
            callId: 'c-test',
            traceparent: TP
        });
        const client = exporter.getFinishedSpans().find((s) => s.kind === SpanKind.CLIENT)!;
        expect(client.spanContext().traceId).toBe('4bf92f3577b34da6a3ce929d0e0e4736');
        expect(parentIdOf(client)).toBe('00f067aa0ba902b7');
    });

    it('marks both spans on a throwing method and rethrows', async () => {
        const { host, exporter } = await startTraced();
        await expect(host.actor(Counter, 'k1').explode()).rejects.toThrow('boom');
        const spans = exporter.getFinishedSpans();
        const client = spans.find((s) => s.kind === SpanKind.CLIENT)!;
        const server = spans.find((s) => s.kind === SpanKind.SERVER)!;
        expect(client.status.code).toBe(SpanStatusCode.ERROR);
        expect(client.events.some((e) => e.name === 'exception')).toBe(true);
        expect(server.status.code).toBe(SpanStatusCode.ERROR);
        expect(server.attributes['sigx.turn.failed']).toBe(true);
    });

    it('forwards streams (untraced in v1) without the drop warning', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        try {
            const { host } = await startTraced();
            const seen: number[] = [];
            for await (const n of host.actor(Counter, 'k1').counts()) seen.push(n as number);
            expect(seen).toEqual([1, 2]);
            // The middleware must declare dispatchStream/dispatchWatch, or
            // the app dev-warns about a wrapper dropping them.
            expect(warn).not.toHaveBeenCalledWith(expect.stringContaining('dispatchStream'));
        } finally {
            warn.mockRestore();
        }
    });

    it('is a pass-through with no provider: calls work, zero spans, streams intact', async () => {
        // Observe the no-op path from the inside: a fake global provider
        // whose tracer RECORDS every startSpan but answers no-op spans (the
        // invalid all-zero context), exactly what the api's default provider
        // does. The plugin may probe — it must never span a dispatch.
        const started: string[] = [];
        const noop = trace.getTracerProvider().getTracer('noop');
        const spy = vi.spyOn(trace, 'getTracerProvider').mockReturnValue({
            getTracer: () => ({
                startSpan: (name: string) => {
                    started.push(name);
                    return noop.startSpan(name);
                }
            })
        } as never);
        try {
            const app = defineActorApp({ actors: [Counter], defaults: quiet }).use(otelTraces());
            running = app;
            const host = await app.start();
            await host.actor(Counter, 'k1').increment(1);
            const seen: number[] = [];
            for await (const n of host.actor(Counter, 'k1').counts()) seen.push(n as number);
            expect(seen).toEqual([1, 2]);
            // Probe spans only — never a dispatch or turn span.
            expect(started.every((name) => name === 'sigx.probe')).toBe(true);
            expect(started.length).toBeGreaterThan(0);
        } finally {
            spy.mockRestore();
        }
    });
});
