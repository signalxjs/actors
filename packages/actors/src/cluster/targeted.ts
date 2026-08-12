/**
 * `workerOn()` — the typed sugar over `ClusterPlacement.dispatchOn()`
 * (#213): a proxy whose methods invoke a stateless worker ON a chosen
 * member. Unary only in v1 — a targeted call is an ops-shaped delivery
 * (fan-out to every web pod, deliver to the socket-owning host), and none
 * of those stream; streams stay future work rather than a half-supported
 * mode.
 */
import type { ActorDefinition, AnyActorDefinition } from '../types';
import type { ClusterPlacement, TargetedCallOptions } from './placement';
import type { HostDescriptor } from './types';

/** Unary methods only — the `ActorClient` shape minus streams. */
export type TargetedWorkerClient<D> = D extends ActorDefinition<infer _S, infer M, infer _St>
    ? {
          [K in keyof M]: M[K] extends (...a: infer A) => infer R
              ? (...a: A) => Promise<Awaited<R>>
              : never;
      }
    : never;

/**
 * A typed client for `def`'s methods, every call delivered to `target`
 * through `placement.dispatchOn()` — one attempt, no retry, worker executes
 * on the targeted host. `key` defaults to `'targeted'`; pass one when the
 * worker keys its pools.
 */
export function workerOn<D extends AnyActorDefinition>(
    placement: ClusterPlacement,
    target: HostDescriptor | string,
    def: D,
    key = 'targeted',
    options?: TargetedCallOptions
): TargetedWorkerClient<D> {
    const dispatchOn = placement.dispatchOn?.bind(placement);
    if (!dispatchOn) {
        throw new Error(
            `[sigx actors] workerOn() needs a placement with dispatchOn() — this one ` +
                `predates it (#213).`
        );
    }
    const ref = { type: def.type, key };
    return new Proxy(Object.create(null) as object, {
        get(_t, prop) {
            if (typeof prop !== 'string') return undefined;
            return (...args: unknown[]) => dispatchOn(target, ref, prop, args, options);
        }
    }) as TargetedWorkerClient<D>;
}
