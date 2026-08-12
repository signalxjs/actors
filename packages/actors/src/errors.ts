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
    | 'host-shutdown'
    | 'call-timeout'
    | 'wrong-host'
    | 'unreachable'
    | 'unplaceable'
    | 'watch-declaration';

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
 * A → B → A into a non-reentrant actor (or a non-interleaving method of
 * one). Thrown at DISPATCH time with the full chain — the cycle fails fast
 * instead of hanging until a timeout. A `reentrant: 'always'` target (or an
 * in-chain call to a `methodReentrancy`-mapped method) never throws this:
 * the cycle completes as a concurrent turn.
 */
export class ActorDeadlockError extends ActorError {
    /** `type/key` hops from the external caller to the cycle, inclusive. */
    readonly chain: readonly string[];

    constructor(chain: readonly string[]) {
        super(
            'deadlock',
            `[sigx actors] call-chain deadlock: ${chain.join(' → ')}. ` +
                `The target is already processing a turn in this chain and is not ` +
                `reentrant; declare \`reentrant: true\` on it to allow call-chain ` +
                `re-entry, \`reentrant: 'always'\` for full interleaving, or map ` +
                `just this method in \`methodReentrancy\`.`
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
 * the next call loads the winning state. Interleaved turns already in
 * flight when the fault lands run to completion (their results were
 * computed on the stale state), but any save they attempt rejects.
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

/**
 * A method declared `watches: { m: { principalIndependent: true } }` was
 * observed consulting `ctx.principal` inside its watch read (#138).
 *
 * Thrown in EVERY build, not just dev. By the time it fires the relay may
 * already have merged distinct identities onto one coalesced stream, and the
 * owner — which sees a single subscriber per stream — cannot repair that. The
 * defect it prevents is serving one user's view to another, which is the
 * exact bug the per-principal split (#121) exists to stop.
 *
 * It does NOT heal: the same read fails the same way on re-subscribe and on a
 * fresh activation after failover, because the declaration is source code
 * while the discovery it contradicts is per activation. Remove the flag or
 * the `ctx.principal` read.
 */
export class ActorWatchDeclarationError extends ActorError {
    constructor(type: string, method: string) {
        super(
            'watch-declaration',
            `[sigx actors] "${type}.${method}" declares \`watches: { '${method}': ` +
                `{ principalIndependent: true } }\` but consulted \`ctx.principal\` while ` +
                `serving a watch. Identity is an input to this read, so its subscribers ` +
                `cannot share one loop. Remove the declaration — or, if the touch was ` +
                `only to AUTHORIZE, move it to \`authorize\`/\`methodAuthorize\`, which ` +
                `run per subscriber at the entry point, outside any turn.`
        );
        this.name = 'ActorWatchDeclarationError';
    }
}

export class ActorMethodNotFoundError extends ActorError {
    constructor(type: string, method: string) {
        super('method-not-found', `[sigx actors] actor type "${type}" has no method "${method}".`);
        this.name = 'ActorMethodNotFoundError';
    }
}

export class HostShutdownError extends ActorError {
    constructor() {
        super('host-shutdown', '[sigx actors] the host is shutting down; new calls are rejected.');
        this.name = 'HostShutdownError';
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

/** The owner hint a wrong-host error carries — enough to re-route. */
export interface ActorOwnerHint {
    readonly hostId: string;
    /**
     * INTERNAL origin — the peer-reachable address of the owner's
     * host-to-host endpoint, typically a private pod IP.
     *
     * Never send this to a client. It is unreachable from outside the
     * cluster, and publishing it hands out internal topology to anyone who
     * can reach the public mount. The public endpoint's redirect carries
     * `publicAddress` instead, and asserts this field's absence.
     */
    readonly address?: string;
    /**
     * PUBLIC origin — where a client can reach the owner's actor mount,
     * e.g. `https://host-3.example.com`. Set from `cluster({ publicAddress })`,
     * absent unless an operator configured it.
     *
     * Absent means "do not redirect": guessing from `address` would be a
     * silent assumption about both the mount path and whether the internal
     * origin is client-reachable.
     */
    readonly publicAddress?: string;
}

/**
 * This host is not (or no longer) the actor's owner — a distributed
 * placement answers a misdirected dispatch with this instead of proxying.
 * Internal to host-to-host traffic: the calling placement consumes the
 * `owner` hint and re-dispatches; it should never reach an application.
 */
export class ActorWrongHostError extends ActorError {
    /** Where the actor actually lives, if this host knows. */
    readonly owner?: ActorOwnerHint;

    constructor(ref: string, owner?: ActorOwnerHint) {
        super(
            'wrong-host',
            `[sigx actors] ${ref} is not placed on this host` +
                (owner ? ` (owner: ${owner.hostId})` : '') +
                `; the caller should re-resolve placement and retry.`
        );
        this.name = 'ActorWrongHostError';
        this.owner = owner;
    }
}

/**
 * No ACTIVE host registers this actor type (#212), so placement has nowhere
 * to put it — a registering host that is still `joining` (or already
 * `leaving`) does not count. Retried against a refreshed membership view —
 * a rolling deploy whose only pod registering the type is mid-join looks
 * exactly like this — and surfaced as the `cause` of the final
 * `ActorActivationError` once the retries are spent.
 */
export class ActorUnplaceableError extends ActorError {
    constructor(type: string, view: { hosts: number; active: number }) {
        super(
            'unplaceable',
            `[sigx actors] actor type "${type}" is registered by no ACTIVE host ` +
                `(view: ${view.hosts} host${view.hosts === 1 ? '' : 's'}, ${view.active} ` +
                `active). Add "${type}" to some host's defineActorApp({ actors }), or check ` +
                `that the pods registering it are up.`
        );
        this.name = 'ActorUnplaceableError';
    }
}

/** A peer host's address did not respond. Retryable by the caller. */
export class ActorUnreachableError extends ActorError {
    constructor(target: string, options?: { cause?: unknown }) {
        super(
            'unreachable',
            `[sigx actors] host ${target} is unreachable; the call may be retried.`,
            options
        );
        this.name = 'ActorUnreachableError';
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
