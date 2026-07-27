/**
 * Node bridging for the actor endpoint — a one-liner over core's
 * connect-style adapter (which owns the IncomingMessage→Request bridge,
 * backpressure-aware body pumping, and disconnect→abort wiring), plus
 * process signal wiring for graceful shutdown.
 */
import {
    createServerFnHandler,
    type NodeRequestHandler,
    type ServerFnHandlerOptions
} from '@sigx/server/node';
import { createActorResolver } from '../server/actor-endpoint';
import type { Silo } from '../types';

export interface ActorHandlerOptions
    extends Omit<ServerFnHandlerOptions, 'resolve' | 'functions' | 'renderBoundaries' | 'base'> {
    /** The running silo — explicit, never ambient. */
    silo: Silo;
    /** URL prefix the handler owns. Default `/_sigx/actor`. */
    base?: string;
}

/**
 * Create the connect-style actor endpoint. Non-matching URLs call `next()`
 * — mount it beside the serverFn handler, before the document handler.
 */
export function createActorHandler(options: ActorHandlerOptions): NodeRequestHandler {
    const { silo, base, ...rest } = options;
    return createServerFnHandler({
        ...rest,
        base: base ?? '/_sigx/actor',
        resolve: createActorResolver(silo)
    });
}

/**
 * Wire SIGINT/SIGTERM to a graceful `silo.stop()` (drain mailboxes, flush
 * persistence) followed by process exit. Returns a detach function.
 */
export function attachSignalHandlers(
    silo: Silo,
    options?: { timeoutMs?: number; exit?: boolean }
): () => void {
    let stopping = false;
    const onSignal = (): void => {
        if (stopping) return;
        stopping = true;
        void silo
            .stop({ timeoutMs: options?.timeoutMs })
            .catch(() => {})
            .finally(() => {
                if (options?.exit !== false) process.exit(0);
            });
    };
    process.once('SIGINT', onSignal);
    process.once('SIGTERM', onSignal);
    return () => {
        process.removeListener('SIGINT', onSignal);
        process.removeListener('SIGTERM', onSignal);
    };
}
