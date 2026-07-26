/**
 * Actor error taxonomy — every cross-boundary error this package throws.
 *
 * Identified by BRAND, never `instanceof`: in dev the Vite module runner and
 * Node can hold two copies of this module (the module-graph split documented
 * in core's `packages/vite/src/ssr.ts`), and the wire client re-creates
 * errors from the wire without these classes. Same posture as core's
 * `ServerFnError`.
 */

/** The discriminant carried by every branded actor error. */
export type ActorErrorKind =
    | 'deadlock'
    | 'activation'
    | 'state-conflict'
    | 'method-not-found'
    | 'silo-shutdown'
    | 'call-timeout';

export interface ActorErrorShape extends Error {
    readonly __sigxActorError: true;
    readonly kind: ActorErrorKind;
}

export class ActorError extends Error implements ActorErrorShape {
    readonly __sigxActorError = true as const;
    readonly kind: ActorErrorKind;

    constructor(kind: ActorErrorKind, message: string, options?: { cause?: unknown }) {
        super(message, options);
        this.name = 'ActorError';
        this.kind = kind;
    }
}

/** Brand check matching every actor error, local or wire-recreated. */
export function isActorError(error: unknown): error is ActorErrorShape {
    return (
        typeof error === 'object' &&
        error !== null &&
        (error as { __sigxActorError?: unknown }).__sigxActorError === true
    );
}

/**
 * A → B → A into a non-reentrant actor. Thrown at DISPATCH time with the
 * full chain — the single-process advantage over Orleans, where the same
 * cycle hangs until a timeout.
 */
export class ActorDeadlockError extends ActorError {
    /** `type/key` hops from the external caller to the cycle, inclusive. */
    readonly chain: readonly string[];

    constructor(chain: readonly string[]) {
        super(
            'deadlock',
            `[sigx actors] call-chain deadlock: ${chain.join(' → ')}. ` +
                `The target is already processing a turn in this chain and is not ` +
                `reentrant; declare \`reentrant: true\` on it to allow call-chain re-entry.`
        );
        this.name = 'ActorDeadlockError';
        this.chain = chain;
    }
}

export function isActorErrorKind(error: unknown, kind: ActorErrorKind): boolean {
    return isActorError(error) && error.kind === kind;
}

/** `onActivate`/`storage.load`/factory threw: every parked caller gets this. */
export class ActorActivationError extends ActorError {
    constructor(ref: string, options?: { cause?: unknown }) {
        super('activation', `[sigx actors] activation of ${ref} failed.`, options);
        this.name = 'ActorActivationError';
    }
}

/**
 * `ctx.save()` found the stored etag ahead of this activation's — someone
 * else wrote this actor's state. The activation is stale: the current turn
 * and every queued turn reject with this, the activation is forgotten, and
 * the next call loads the winning state (Orleans InconsistentStateException).
 */
export class ActorStateConflictError extends ActorError {
    constructor(ref: string) {
        super(
            'state-conflict',
            `[sigx actors] stale state for ${ref}: the stored record changed under ` +
                `this activation. The activation is discarded; retry the call.`
        );
        this.name = 'ActorStateConflictError';
    }
}

export class ActorMethodNotFoundError extends ActorError {
    constructor(type: string, method: string) {
        super('method-not-found', `[sigx actors] actor type "${type}" has no method "${method}".`);
        this.name = 'ActorMethodNotFoundError';
    }
}

export class SiloShutdownError extends ActorError {
    constructor() {
        super('silo-shutdown', '[sigx actors] the silo is shutting down; new calls are rejected.');
        this.name = 'SiloShutdownError';
    }
}

/** The CALLER's deadline passed. The turn itself is never killed. */
export class ActorCallTimeoutError extends ActorError {
    constructor(ref: string, method: string, ms: number) {
        super(
            'call-timeout',
            `[sigx actors] call to ${ref}.${method}() exceeded its ${ms}ms deadline. ` +
                `The turn keeps running; only this caller gave up.`
        );
        this.name = 'ActorCallTimeoutError';
    }
}

/**
 * Storage-provider conflict brand — thrown by `ActorStorage.save/clear` on an
 * etag mismatch. Its own brand (not ActorStateConflictError) because storage
 * implementations may live in a different module graph than the runtime that
 * classifies them.
 */
export class ActorStorageConflict extends Error {
    readonly __sigxActorStorageConflict = true as const;

    constructor(type: string, key: string) {
        super(`[sigx actors] optimistic-concurrency conflict writing ${type}/${key}.`);
        this.name = 'ActorStorageConflict';
    }
}

export function isStorageConflict(error: unknown): boolean {
    return (
        typeof error === 'object' &&
        error !== null &&
        (error as { __sigxActorStorageConflict?: unknown }).__sigxActorStorageConflict === true
    );
}
