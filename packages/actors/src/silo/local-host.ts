/**
 * The single-node host: directory (type+key → activation slot), lazy
 * activation, deadlock detection, and deactivation orchestration. This is
 * the v1 `ActorDispatcher`; a Durable Objects or cluster placement replaces
 * exactly this class with a fetch-forwarding stub.
 */
import {
    ActorActivationError,
    ActorCallTimeoutError,
    ActorDeadlockError,
    ActorError,
    SiloShutdownError
} from '../errors';
import {
    actorId,
    actorLabel,
    type ActorCallContext,
    type ActorDispatcher,
    type ActorRef,
    type AnyActorDefinition,
    type DeactivationReason,
    type PlacementBindings,
    type SiloStats
} from '../types';
import { Activation, type ActivationHost } from './activation';
import { Mailbox } from './mailbox';

type Slot =
    | { phase: 'activating'; promise: Promise<Activation> }
    | { phase: 'active'; activation: Activation }
    | { phase: 'deactivating'; drained: Promise<void> };

export class LocalHost implements ActorDispatcher {
    #directory = new Map<string, Slot>();
    #host: ActivationHost;
    #resolveDefinition: (type: string) => AnyActorDefinition | Promise<AnyActorDefinition | null> | null;
    #shuttingDown = false;
    /** Read per use, not captured: `placement.bind()` runs after construction. */
    #bindings: () => PlacementBindings | undefined;

    constructor(
        host: ActivationHost,
        resolveDefinition: (
            type: string
        ) => AnyActorDefinition | Promise<AnyActorDefinition | null> | null,
        bindings: () => PlacementBindings | undefined = () => undefined
    ) {
        this.#host = host;
        this.#resolveDefinition = resolveDefinition;
        this.#bindings = bindings;
    }

    async dispatch(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): Promise<unknown> {
        const work = this.#dispatchInner(ref, method, args, call);
        return call.deadline === undefined
            ? work
            : raceDeadline(work, call.deadline, ref, method);
    }

    dispatchStream(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): AsyncIterable<unknown> {
        // Must return synchronously; resolve the activation on first pull.
        const open = async (): Promise<AsyncIterator<unknown>> => {
            this.#checkShutdown(call);
            await this.#checkReentrancy(ref, call);
            const activation = await this.#activationFor(ref);
            return activation.openStream(method, args, call)[Symbol.asyncIterator]();
        };
        let inner: Promise<AsyncIterator<unknown>> | null = null;
        return {
            [Symbol.asyncIterator]: () => ({
                next: async () => {
                    inner ??= open();
                    return (await inner).next();
                },
                return: async () => {
                    if (inner) {
                        const it = await inner;
                        if (it.return) await it.return(undefined);
                    }
                    return { value: undefined, done: true as const };
                }
            })
        };
    }

    async #dispatchInner(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): Promise<unknown> {
        this.#checkShutdown(call);
        const inline = await this.#checkReentrancy(ref, call);
        if (inline) return inline.runInline(method, args, call);
        const activation = await this.#activationFor(ref);
        return activation.enqueue(method, args, call);
    }

    #checkShutdown(call: ActorCallContext): void {
        // In-chain dispatches still pass — a draining turn must be able to
        // finish its actor-to-actor calls.
        if (this.#shuttingDown && call.callChain.length === 0) throw new SiloShutdownError();
    }

    /**
     * The target is already in this call chain → its turn is up-stack
     * awaiting us. Reentrant: run inline against that turn. Non-reentrant:
     * throw now with the full chain (Orleans would hang until timeout).
     */
    async #checkReentrancy(ref: ActorRef, call: ActorCallContext): Promise<Activation | null> {
        const id = actorId(ref);
        if (!call.callChain.includes(id)) return null;
        const slot = this.#directory.get(id);
        const active = slot?.phase === 'active' ? slot.activation : null;
        const reentrant = active
            ? active.def.__sigxActor.reentrant === true
            : (await this.#definition(ref.type)).__sigxActor.reentrant === true;
        if (!reentrant) {
            throw new ActorDeadlockError([...call.callChain, id]);
        }
        if (!active) {
            // Chain says it's up-stack but the slot is gone. Single-node
            // this shouldn't happen; fall back to a normal dispatch. In a
            // cluster it means the turn runs on ANOTHER host — activating a
            // second copy here would break single-activation, so a strict
            // placement makes it a loud, retryable error instead.
            if (this.#bindings()?.strictChainPresence) {
                throw new ActorError(
                    'deadlock',
                    `[sigx actors] ${actorLabel(ref)} is mid-turn in this call chain but has ` +
                        `no activation on this host — the activation moved mid-call. Retry.`
                );
            }
            return null;
        }
        return active;
    }

    async #definition(type: string): Promise<AnyActorDefinition> {
        const def = await this.#resolveDefinition(type);
        if (!def) {
            throw new ActorActivationError(`${type}/?`, {
                cause: new Error(
                    `[sigx actors] unknown actor type "${type}" — is it registered with createSilo({ actors })?`
                )
            });
        }
        return def;
    }

    async #activationFor(ref: ActorRef): Promise<Activation> {
        const id = actorId(ref);
        for (;;) {
            const slot = this.#directory.get(id);
            if (!slot) {
                // Reserve SYNCHRONOUSLY so a racing second dispatch joins
                // this promise — the single-activation invariant. (`reserved`
                // is only read after the first await inside the closure.)
                const mailbox = new Mailbox();
                let reserved!: Slot;
                const promise = (async () => {
                    const def = await this.#definition(ref.type);
                    // The distributed directory's claim point: a throw here
                    // (wrong-host) rejects every parked caller and the slot
                    // is dropped below — nothing activates.
                    await this.#bindings()?.beforeActivate?.(ref);
                    const activation = await Activation.create(ref, def, this.#host, mailbox);
                    const current = this.#directory.get(id);
                    if (current === reserved) {
                        this.#directory.set(id, { phase: 'active', activation });
                    }
                    return activation;
                })();
                reserved = { phase: 'activating', promise };
                this.#directory.set(id, reserved);
                promise.catch(() => {
                    // Activation failed: every parked caller rejects with the
                    // ActorActivationError; nothing is remembered.
                    if (this.#directory.get(id) === reserved) this.#directory.delete(id);
                });
                return promise;
            }
            if (slot.phase === 'activating') return slot.promise;
            if (slot.phase === 'active') return slot.activation;
            // Deactivating: park until drained, then re-activate fresh
            // (Orleans behavior — calls during deactivation wait, not fail).
            await slot.drained;
        }
    }

    // -----------------------------------------------------------------------
    // Lifecycle orchestration (silo-facing)

    /** Begin (or join) graceful deactivation of one activation. */
    deactivate(ref: ActorRef, reason: DeactivationReason): Promise<void> {
        const id = actorId(ref);
        const slot = this.#directory.get(id);
        if (!slot) return Promise.resolve();
        if (slot.phase === 'deactivating') return slot.drained;
        if (slot.phase === 'activating') {
            // Let the activation finish, then deactivate it.
            return slot.promise
                .then(() => this.deactivate(ref, reason))
                .catch(() => {});
        }
        const activation = slot.activation;
        let next!: Slot & { phase: 'deactivating' };
        const drained = (async () => {
            try {
                await activation.deactivate(reason);
            } finally {
                if (this.#directory.get(id) === next) this.#directory.delete(id);
                // Directory-claim release. Swallowed: a failed release must
                // not reject callers parked on `drained` — a stale remote
                // entry is reclaimed lazily by its owner's liveness.
                try {
                    await this.#bindings()?.afterDeactivate?.(activation.ref, reason);
                } catch (error) {
                    if (__DEV__) {
                        console.error(
                            `[sigx actors] afterDeactivate hook for ` +
                                `${actorLabel(activation.ref)} failed:`,
                            error
                        );
                    }
                }
            }
        })();
        next = { phase: 'deactivating', drained };
        this.#directory.set(id, next);
        return drained;
    }

    /** Idle sweep: deactivate every activation past its collection age. */
    sweep(now: number, defaultIdleMs: number): void {
        for (const [, slot] of this.#directory) {
            if (slot.phase !== 'active') continue;
            const a = slot.activation;
            const idleAfter = a.def.__sigxActor.idleAfterMs ?? defaultIdleMs;
            if (a.idle && !a.keptAlive && now - a.lastActivityMs >= idleAfter) {
                void this.deactivate(a.ref, 'idle');
            }
        }
    }

    /** Deactivate every activation of one type (dev/HMR). */
    async deactivateType(type: string): Promise<void> {
        const waits: Promise<void>[] = [];
        for (const [, slot] of this.#directory) {
            if (slot.phase === 'active' && slot.activation.ref.type === type) {
                waits.push(this.deactivate(slot.activation.ref, 'explicit'));
            }
        }
        await Promise.all(waits);
    }

    /**
     * Shutdown: close the front door, drain everyone (reason 'shutdown'),
     * force-drop stragglers at the deadline.
     */
    async stopAll(timeoutMs: number): Promise<void> {
        this.#shuttingDown = true;
        const waits: Promise<void>[] = [];
        for (const [, slot] of this.#directory) {
            if (slot.phase === 'active') {
                waits.push(this.deactivate(slot.activation.ref, 'shutdown'));
            } else if (slot.phase === 'deactivating') {
                waits.push(slot.drained);
            } else {
                waits.push(slot.promise.then(
                    (a) => this.deactivate(a.ref, 'shutdown'),
                    () => {}
                ));
            }
        }
        let timer: ReturnType<typeof setTimeout> | undefined;
        const deadline = new Promise<'timeout'>((r) => {
            timer = setTimeout(() => r('timeout'), timeoutMs);
            (timer as { unref?: () => void }).unref?.();
        });
        const outcome = await Promise.race([
            Promise.allSettled(waits).then(() => 'done' as const),
            deadline
        ]);
        clearTimeout(timer);
        if (outcome === 'timeout') {
            for (const [id, slot] of this.#directory) {
                if (slot.phase === 'active') {
                    if (__DEV__) {
                        console.warn(
                            `[sigx actors] force-dropping ${actorLabel(slot.activation.ref)} at ` +
                                `the shutdown deadline (${timeoutMs}ms).`
                        );
                    }
                    slot.activation.forceStop();
                }
                this.#directory.delete(id);
            }
        }
    }

    stats(): SiloStats {
        let activations = 0;
        let queued = 0;
        const perType: Record<string, number> = {};
        for (const [, slot] of this.#directory) {
            if (slot.phase !== 'active') continue;
            activations++;
            queued += slot.activation.mailbox.depth;
            perType[slot.activation.ref.type] = (perType[slot.activation.ref.type] ?? 0) + 1;
        }
        return { activations, queued, perType };
    }
}

/** Race a dispatch against the caller's deadline. The turn is never killed. */
async function raceDeadline(
    work: Promise<unknown>,
    deadline: number,
    ref: ActorRef,
    method: string
): Promise<unknown> {
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw new ActorCallTimeoutError(actorLabel(ref), method, 0);
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
        return await Promise.race([
            work,
            new Promise((_r, reject) => {
                timer = setTimeout(
                    () => reject(new ActorCallTimeoutError(actorLabel(ref), method, remaining)),
                    remaining
                );
                (timer as { unref?: () => void }).unref?.();
            })
        ]);
    } finally {
        clearTimeout(timer);
        // The losing work promise must not surface as unhandled.
        work.catch(() => {});
    }
}
