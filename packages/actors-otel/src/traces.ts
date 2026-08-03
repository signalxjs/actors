/**
 * OpenTelemetry traces for `@sigx/actors` — a `useDispatch` middleware
 * creating one CLIENT span per dispatch, and a turn observer creating the
 * matching SERVER span on the actor's OWNER host.
 *
 * Two spans because the runtime has two sides that never share a process by
 * guarantee: the middleware runs where the call is MADE (and never on an
 * inbound cluster hop — `dispatchInbound` deliberately bypasses middleware
 * to keep metrics single-counted), while the turn observer fires where the
 * actor LIVES. The propagated `call.traceparent` is what joins them into
 * one trace, across hosts or within one.
 *
 * Cost discipline: with no tracer provider registered, the wrapped dispatch
 * is one branch and (at most, every few seconds) one probe span — the
 * runtime's per-turn timestamps stay OFF because the turn observer is only
 * subscribed once a provider is detected.
 */
import {
    ROOT_CONTEXT,
    SpanKind,
    SpanStatusCode,
    isSpanContextValid,
    trace,
    type Attributes,
    type Tracer,
    type TracerProvider
} from '@opentelemetry/api';
import type { ActorCallContext, ActorRef, Host } from '@sigx/actors';
import type { ActorPlugin, PluginRegistry } from '@sigx/actors/host';
import { hashRouteToken } from '@sigx/actors/client';
import { formatTraceparent, parseTraceparent } from './traceparent';

export interface OtelTracesOptions {
    /**
     * The provider to trace against. Default: the global registration
     * (`trace.setGlobalTracerProvider` / `NodeSDK`), detected lazily so a
     * provider registered after `app.start()` is still picked up.
     */
    tracerProvider?: TracerProvider;
    /**
     * Create the callee-side SERVER span per turn (with the queued/turn
     * split as attributes). Default true. The subscription is what switches
     * the runtime's per-turn timestamps on, so turning this off removes
     * that cost too.
     */
    turnSpans?: boolean;
}

const TRACER_NAME = '@sigx/actors-otel';
/** How often to re-probe for a late-registered global provider. */
const PROBE_INTERVAL_MS = 5000;

/** Never the raw key: keys are frequently user ids or emails. The hash is
 *  the same one the `route` option mints, so a span is joinable to the
 *  routing token in an access log — log hygiene, not privacy. */
function attributesFor(ref: ActorRef, method: string): Attributes {
    return {
        'sigx.actor.type': ref.type,
        'sigx.actor.method': method,
        'sigx.actor.key_hash': hashRouteToken(ref.type, ref.key)
    };
}

function parentContext(traceparent: string | undefined) {
    const parsed = traceparent !== undefined ? parseTraceparent(traceparent) : null;
    return parsed === null
        ? ROOT_CONTEXT
        : trace.setSpanContext(ROOT_CONTEXT, { ...parsed, isRemote: true });
}

export function otelTraces(options: OtelTracesOptions = {}): ActorPlugin {
    const explicit = options.tracerProvider;
    const turnSpans = options.turnSpans !== false;

    let tracer: Tracer | null = null;
    let nextProbeAt = 0;
    let host: Host | null = null;
    let unsubscribeTurns: (() => void) | null = null;

    const onTurn = (
        ref: ActorRef,
        method: string,
        queuedMs: number,
        elapsedMs: number,
        failed: boolean,
        call?: ActorCallContext
    ): void => {
        if (!tracer) return;
        // The observer fires from the turn's `finally`, so the span is built
        // retroactively with explicit timestamps.
        const end = Date.now();
        const span = tracer.startSpan(
            `${ref.type}.${method}`,
            {
                kind: SpanKind.SERVER,
                startTime: end - elapsedMs,
                attributes: {
                    ...attributesFor(ref, method),
                    'sigx.turn.queued_ms': queuedMs,
                    'sigx.turn.failed': failed,
                    ...(call !== undefined ? { 'sigx.call.id': call.callId } : {})
                }
            },
            parentContext(call?.traceparent)
        );
        if (failed) span.setStatus({ code: SpanStatusCode.ERROR });
        span.end(end);
    };

    const ensureTurnObserver = (): void => {
        if (!turnSpans || unsubscribeTurns !== null || host === null) return;
        // The HOST seam, not registry.observeTurns: a registry subscription
        // attaches at start unconditionally, which would switch the per-turn
        // timestamps on even with no provider anywhere. Subscribing only
        // once a provider is detected keeps the untraced hot path exact.
        unsubscribeTurns = host.observeTurns(onTurn);
    };

    /**
     * Resolve (and cache) the tracer, or answer null while tracing is off.
     *
     * The api's global default cannot be interrogated directly — its proxy
     * hands back a no-op provider until an SDK registers. What CAN be told
     * apart is the span: a no-op tracer's spans carry the invalid all-zero
     * context, while a real SDK generates ids even for unsampled spans. So:
     * probe with one throwaway span, at most once per PROBE_INTERVAL_MS, and
     * cache only success (providers register once and never unregister).
     */
    const resolveTracer = (): Tracer | null => {
        if (tracer !== null) return tracer;
        if (explicit !== undefined) {
            tracer = explicit.getTracer(TRACER_NAME);
            ensureTurnObserver();
            return tracer;
        }
        const now = Date.now();
        if (now < nextProbeAt) return null;
        nextProbeAt = now + PROBE_INTERVAL_MS;
        const candidate = trace.getTracerProvider().getTracer(TRACER_NAME);
        const probe = candidate.startSpan('sigx.probe');
        probe.end();
        if (!isSpanContextValid(probe.spanContext())) return null;
        tracer = candidate;
        ensureTurnObserver();
        return tracer;
    };

    return {
        name: 'otel-traces',
        setup(registry: PluginRegistry): void {
            registry.onStart((started) => {
                host = started;
                // A provider registered before start subscribes immediately;
                // otherwise the dispatch-path probe subscribes later.
                if (tracer !== null || resolveTracer() !== null) ensureTurnObserver();
            });
            registry.onStop(() => {
                unsubscribeTurns?.();
                unsubscribeTurns = null;
                host = null;
            });
            registry.useDispatch((next) => ({
                // Deliberately not async: the inactive path must not grow a
                // promise hop — one branch, then straight through.
                dispatch(ref, method, args, call) {
                    const t = resolveTracer();
                    if (t === null) return next.dispatch(ref, method, args, call);
                    const span = t.startSpan(
                        `${ref.type}.${method}`,
                        {
                            kind: SpanKind.CLIENT,
                            attributes: {
                                ...attributesFor(ref, method),
                                'sigx.call.id': call.callId,
                                // Depth, never the chain itself — the chain
                                // contains raw keys.
                                'sigx.call.depth': call.callChain.length
                            }
                        },
                        parentContext(call.traceparent)
                    );
                    // The spread is the propagation: the envelope carries it
                    // on remote hops, the activation stores it for ctx.actor
                    // and the turn observer on local ones.
                    return next
                        .dispatch(ref, method, args, {
                            ...call,
                            traceparent: formatTraceparent(span.spanContext())
                        })
                        .then(
                            (result) => {
                                span.end();
                                return result;
                            },
                            (error: unknown) => {
                                span.recordException(error as Error);
                                span.setStatus({
                                    code: SpanStatusCode.ERROR,
                                    message: (error as Error)?.message ?? String(error)
                                });
                                span.end();
                                throw error;
                            }
                        );
                },
                // Streams and watches are forwarded, never traced, in v1 —
                // they are long-lived, so "one span per dispatch" is the
                // wrong shape (the metrics plugin makes the same call:
                // counter, no latency). Dropping them instead would take
                // $live down, which is the cautionary tale in metrics.ts.
                ...(next.dispatchStream && {
                    dispatchStream: (
                        ref: ActorRef,
                        method: string,
                        args: readonly unknown[],
                        call: ActorCallContext
                    ) => next.dispatchStream!(ref, method, args, call)
                }),
                ...(next.dispatchWatch && {
                    dispatchWatch: (
                        ref: ActorRef,
                        method: string,
                        args: readonly unknown[],
                        call: ActorCallContext,
                        watchOptions?: { throttleMs?: number }
                    ) => next.dispatchWatch!(ref, method, args, call, watchOptions)
                })
            }));
        }
    };
}
