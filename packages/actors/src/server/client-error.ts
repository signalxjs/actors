/**
 * Branded actor error → the status a CLIENT sees, in one place.
 *
 * Shared by the unary endpoint and `$live`, deliberately: a subscription
 * that reports `method-not-found` as a 500 while the same call over the
 * unary path reports 404 sends the client chasing a server fault that is
 * really its own typo. The two surfaces must not drift.
 *
 * Distinct from `cluster/wire-errors.ts`, which maps the SAME errors for a
 * trusted peer and therefore masks nothing. This one is the public edge.
 */
import { ServerFnError } from '@sigx/server';
import { isActorError } from '../errors';

export function toClientError(error: unknown): unknown {
    if (!isActorError(error)) return error;
    switch (error.kind) {
        case 'method-not-found':
            return new ServerFnError(404, error.message, { kind: error.kind });
        case 'state-conflict':
            return new ServerFnError(409, error.message, { kind: error.kind });
        case 'host-shutdown':
            return new ServerFnError(503, error.message, { kind: error.kind });
        case 'call-timeout':
            return new ServerFnError(504, error.message, { kind: error.kind });
        case 'wrong-host':
        case 'unreachable':
            // Cluster routing errors are resolved host-to-host and should
            // never surface here; if one does, it's retryable — 503.
            return new ServerFnError(503, error.message, { kind: error.kind });
        case 'deadlock':
        case 'activation':
        default:
            // Server-side bugs/config problems: mask in prod (plain 500).
            return error;
    }
}
