/**
 * The join test — issue #245's headline behaviour: a call crossing a
 * host-to-host hop appears as ONE trace, the caller's CLIENT span and the
 * owner host's SERVER span sharing a trace id, joined by the envelope's
 * `tp` field. Each host has its own provider and exporter, as two real
 * processes would.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { SpanKind } from '@opentelemetry/api';
import {
    BasicTracerProvider,
    InMemorySpanExporter,
    SimpleSpanProcessor,
    type ReadableSpan
} from '@opentelemetry/sdk-trace-base';
import { defineActor } from '@sigx/actors';
import { otelTraces } from '@sigx/actors-otel';
import { createCluster, selfPolicy, type ClusterHarness } from '../../actors/__tests__/harness';

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        }
    })
});

function parentIdOf(span: ReadableSpan): string | undefined {
    const s = span as ReadableSpan & {
        parentSpanContext?: { spanId: string };
        parentSpanId?: string;
    };
    return s.parentSpanContext?.spanId ?? s.parentSpanId;
}

let running: ClusterHarness | null = null;
afterEach(async () => {
    await running?.stop();
    running = null;
});

describe('cross-host trace join', () => {
    it('one trace across the wire: caller CLIENT span, owner SERVER span', async () => {
        const exporters = [new InMemorySpanExporter(), new InMemorySpanExporter()];
        const providers = exporters.map(
            (exporter) =>
                new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] })
        );
        const cluster = await createCluster(2, {
            actors: [Counter],
            policy: selfPolicy,
            plugins: (i) => [otelTraces({ tracerProvider: providers[i]! })]
        });
        running = cluster;

        // selfPolicy: host 1's first call makes it the owner...
        await cluster.hosts[1]!.actor(Counter, 'far').increment(1);
        exporters[1]!.reset();

        // ...so host 0's call crosses the wire.
        await cluster.hosts[0]!.actor(Counter, 'far').increment(1);

        const client = exporters[0]!
            .getFinishedSpans()
            .find((s) => s.kind === SpanKind.CLIENT && s.name === 'Counter.increment')!;
        const server = exporters[1]!
            .getFinishedSpans()
            .find((s) => s.kind === SpanKind.SERVER && s.name === 'Counter.increment')!;
        expect(client).toBeDefined();
        expect(server).toBeDefined();

        // The join: one trace id on both hosts, and the owner's turn span is
        // the child of the caller's dispatch span — carried by nothing but
        // the envelope's tp field.
        expect(server.spanContext().traceId).toBe(client.spanContext().traceId);
        expect(parentIdOf(server)).toBe(client.spanContext().spanId);
    });
});
