/**
 * Node bridging for the actor endpoint — core's connect-style adapter
 * (which owns the IncomingMessage→Request bridge, backpressure-aware body
 * pumping, and disconnect→abort wiring) plus the one thing the actor wire
 * adds to it: taking the routing token off the path. Also the process
 * signal wiring for graceful shutdown.
 *
 * Anything this mount does beyond delegating has a twin in
 * `handleActorRequest`, and the two must not drift — a request served here
 * and the same request served on the WinterCG mount have to be decided
 * identically (#93).
 */
import {
    createServerFnHandler,
    type NodeRequestHandler,
    type ServerFnHandlerOptions
} from '@sigx/server/node';
import { createActorResolver } from '../server/actor-endpoint';
import type { ActorResolverOptions } from '../server/actor-endpoint';
import { stripRoutePath } from '../route';
import type { Host } from '../types';

export interface ActorHandlerOptions
    extends Omit<ServerFnHandlerOptions, 'resolve' | 'functions' | 'renderBoundaries' | 'base'>,
        ActorResolverOptions {
    /** The running host — explicit, never ambient. */
    host: Host;
    /** URL prefix the handler owns. Default `/_sigx/actor`. */
    base?: string;
}

/**
 * Create the connect-style actor endpoint. Non-matching URLs call `next()`
 * — mount it beside the serverFn handler, before the document handler.
 */
export function createActorHandler(options: ActorHandlerOptions): NodeRequestHandler {
    // Every ActorResolverOptions field has to be named here. They type-check
    // on this handler (the interface extends ActorResolverOptions), so one
    // left in `...rest` reaches the serverFn handler, which ignores it — the
    // option compiles and does nothing. `maxLiveSubscriptions` was exactly
    // that, and it is a security bound, so the Node deployment path silently
    // ran uncapped while the WinterCG one was capped.
    const { host, base, onMiss, maxHops, maxLiveSubscriptions, ...rest } = options;
    const mount = base ?? '/_sigx/actor';
    const inner = createServerFnHandler({
        ...rest,
        base: mount,
        resolve: createActorResolver(host, {
            base: mount,
            ...(onMiss !== undefined ? { onMiss } : {}),
            ...(maxHops !== undefined ? { maxHops } : {}),
            ...(maxLiveSubscriptions !== undefined ? { maxLiveSubscriptions } : {})
        })
    });
    return (req, res, next) => {
        // The routing token has to come off the path before CORE reads it,
        // and this mount hands core's adapter the request directly — so it
        // owns the strip, exactly as `handleActorRequest` does for the
        // WinterCG mount. Skipping it made core read
        // `r/{token}/{Type}#{method}` as the symbol: a 404 for an
        // authenticated caller, and a 401 for an `allowAnonymous` actor,
        // since the unknown-actor wrapper carries no `__sigxAnon` and core's
        // identity gate runs before the wrapper is ever invoked. `route:
        // 'hash'` is the CLIENT DEFAULT, so that was every anonymous actor
        // call on a Node deployment (#93).
        //
        // `req.url` is the RAW request target — path plus query — so only
        // the path is rewritten and the query rides along byte for byte
        // (a declared `reads:` GET carries its arguments there). Rewriting
        // it in place is the connect idiom, and it strands nothing
        // downstream: the rewritten target still starts with this mount's
        // base, so core answers it rather than calling `next()`.
        const target = req.url;
        if (target !== undefined) {
            const query = target.search(/[?#]/);
            const pathname = query === -1 ? target : target.slice(0, query);
            const stripped = stripRoutePath(pathname, mount);
            if (stripped !== null) {
                req.url = query === -1 ? stripped : stripped + target.slice(query);
            }
        }
        return inner(req, res, next);
    };
}

/**
 * An HTTP server this process should stop serving from before it exits.
 *
 * Structural rather than `http.Server` so `https`, `http2` and a test double
 * all satisfy it. `closeAllConnections` is optional and optional-called: it
 * arrived in Node 18.2, and a server without it degrades to `close()` alone
 * rather than throwing during shutdown — the worst possible moment for a
 * TypeError.
 */
export interface DrainableServer {
    close(callback?: (error?: Error) => void): unknown;
    /** Node >= 18.2. Destroys whatever is still connected. */
    closeAllConnections?(): void;
}

export interface SignalHandlerOptions {
    /** Passed to `host.stop()`. */
    timeoutMs?: number;
    /** `false` leaves the process running after the drain — for tests. */
    exit?: boolean;
    /**
     * The HTTP server this process is serving from.
     *
     * Strongly recommended for any server behind a load balancer. Stopping
     * the actors is only half a graceful shutdown: an orchestrator's
     * preStop sleep and readiness-503 only steer NEW connections away, while
     * connections that are already established survive endpoint removal
     * (conntrack) and ride into the exiting pod — where they are reset when
     * the process exits. That is a small but real error rate on every
     * rolling restart, and it is invisible from the actor layer, which
     * reports a clean hand-off.
     *
     * Closed at the END of shutdown, deliberately — see `onStopBegin` for
     * how connections are actually drained, and why closing early makes
     * things worse rather than better.
     *
     * Omit it and shutdown behaves exactly as it did before this option
     * existed.
     */
    server?: DrainableServer;
    /**
     * Called the moment shutdown starts, before the host drains.
     *
     * This is where a Node server marks responses `connection: close`, which
     * is THE mechanism that drains keep-alive pools safely: each client
     * retires its socket after the response it is already receiving, then
     * re-dials and lands on a live instance. Nothing is interrupted.
     *
     * ```ts
     * let stopping = false;
     * const server = createServer((req, res) => {
     *     if (stopping) res.setHeader('connection', 'close');
     *     handler(req, res);
     * });
     * attachSignalHandlers(host, { server, onStopBegin: () => (stopping = true) });
     * ```
     *
     * Why not just close the listener here instead? Because on Node >= 19
     * `server.close()` also destroys IDLE connections, and a socket that is
     * idle from the server's point of view may be one the client is at that
     * instant writing its next request onto. Destroying it produces exactly
     * the reset this whole sequence exists to prevent — measured, not
     * theorised. So the listener is closed only at the very end, by which
     * point anything still open either drained itself or is a genuine
     * straggler, and an idle-close is not an error to a client pool.
     */
    onStopBegin?: () => void;
    /**
     * Called if `host.stop()` rejects. Shutdown continues either way — a
     * reporting hook, not a veto.
     *
     * Exists so a failed drain can be logged before the process is gone.
     * Throwing from here is swallowed: an error reporter must not be able to
     * prevent the exit it was called to describe.
     */
    onError?: (error: unknown) => void;
}

/**
 * Wire SIGINT/SIGTERM to a graceful shutdown — drain the HTTP edge, then
 * `host.stop()` (drain turns, flush persistence) — followed by process
 * exit. Returns a detach function.
 */
export function attachSignalHandlers(host: Host, options?: SignalHandlerOptions): () => void {
    let stopping = false;
    const onSignal = (): void => {
        if (stopping) return;
        stopping = true;
        const server = options?.server;
        // The order here is load-bearing, and it is NOT the obvious one:
        //  1. onStopBegin() — the caller starts answering `connection: close`,
        //     so client pools retire each socket after the response it is
        //     already receiving. That is what actually drains them, and it
        //     interrupts nothing.
        //  2. host.stop()   — the actor drain. Pooled connections keep
        //     flowing throughout; peers whose dials are refused see
        //     `unreachable`, which is retryable by design.
        //  3. close() + closeAllConnections() — LAST. Closing the listener
        //     earlier is actively harmful: on Node >= 19 close() also
        //     destroys IDLE connections, and "idle" from the server's side
        //     includes a socket the client is at that instant writing its
        //     next request onto. That is a reset — the very failure this
        //     sequence exists to prevent.
        try {
            options?.onStopBegin?.();
        } catch {
            // A caller's own bookkeeping must not abort the shutdown.
        }
        let code = 0;
        void host
            .stop({ timeoutMs: options?.timeoutMs })
            .catch((error: unknown) => {
                // A failed drain must not exit 0 and vanish: on a terminated
                // pod the exit code is often the only diagnostic left behind.
                code = 1;
                try {
                    options?.onError?.(error);
                } catch {
                    // An error reporter cannot be allowed to prevent the exit
                    // it was called to describe.
                }
            })
            .finally(() => {
                try {
                    server?.close();
                    server?.closeAllConnections?.();
                } catch {
                    // `DrainableServer` is structural, so this is somebody
                    // else's implementation and may throw. Tearing down the
                    // edge is best-effort; failing to do it must not strand
                    // the process by skipping the exit below.
                }
                if (options?.exit !== false) process.exit(code);
            });
    };
    process.once('SIGINT', onSignal);
    process.once('SIGTERM', onSignal);
    return () => {
        process.removeListener('SIGINT', onSignal);
        process.removeListener('SIGTERM', onSignal);
    };
}
