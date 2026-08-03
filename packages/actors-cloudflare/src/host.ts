/**
 * `createHostDurableObject` — a Durable Object that IS an actor host.
 *
 * A factory returning a class rather than a base class to extend, because
 * the object must own its seams: storage, reminders, placement and the
 * defaults all come from the Durable Object's own state, and a subclass that
 * wired them itself would be one `super()` away from silent corruption.
 * Extending the returned class still works for `webSocketMessage` and
 * friends, so there is one surface rather than two.
 *
 * `cloudflare:workers` is deliberately NOT imported. Extending Cloudflare's
 * `DurableObject` base is only needed for RPC entrypoints and `ctx.props`;
 * `fetch` and `alarm` work on a plain class. Importing it would make this
 * package unloadable outside workerd and take the fake-driven tests with it.
 */
import type { ActorRef, ActorStorage, Host } from '@sigx/actors';
import {
    defineActorApp,
    type ActorApp,
    type ActorAppOptions,
    type HostDefaults
} from '@sigx/actors/host';
import {
    handleHostRequestForRuntime,
    matchesHostRequest,
    hostEndpointRuntime,
    type HostEndpointOptions,
    type HostEndpointRuntime
} from '@sigx/actors/cluster';
import { durableObjectReminders, type DurableObjectReminders } from './reminders';
import { durableObjectStorage } from './storage';
import {
    durableObjectName,
    durableObjects,
    type DurableObjectPlacementOptions
} from './placement';
import type { DurableAlarms } from './reminders';
import type { DurableStorage } from './storage';
import type { DurableObjectIdLike, DurableObjectNamespaceLike } from './types';

/** The slice of `DurableObjectState` a host needs. */
export interface DurableObjectStateLike {
    readonly id: DurableObjectIdLike;
    readonly storage: DurableStorage & DurableAlarms;
    blockConcurrencyWhile<T>(fn: () => Promise<T>): Promise<T>;
}

/**
 * The seams the object derived from its own state. An `app` factory MUST
 * pass these to `defineActorApp` — they are what make the app this object's
 * rather than a generic one.
 */
export interface DurableAppOptions extends ActorAppOptions {
    storage: ActorStorage;
    defaults: HostDefaults;
}

export interface HostDurableObjectOptions<Env = unknown> {
    /** Registry, unless the `app` factory supplies one via `withActors`. */
    actors?: ActorAppOptions['actors'];
    /**
     * THE binding, for actor-to-actor calls. A call from this object to a
     * DIFFERENT actor must reach that actor's object rather than activating
     * a second copy here.
     */
    namespace(env: Env): DurableObjectNamespaceLike;
    /**
     * Add plugins. Receives the object-derived options it must pass on, and
     * deliberately NOT `env`: without a namespace binding it cannot build a
     * placement, which is what keeps `setPlacement` ours.
     */
    app?(base: DurableAppOptions): ActorApp;
    /** Must match the Worker's, byte for byte. */
    placement?: Pick<
        DurableObjectPlacementOptions,
        'objectName' | 'locationHint' | 'jurisdiction' | 'base'
    >;
    /** Forwarded to the internal mount (body caps, `onError`, `timeoutMs`). */
    endpoint?: HostEndpointOptions;
}

export interface HostDurableObjectInstance {
    fetch(request: Request): Promise<Response>;
    alarm(): Promise<void>;
    /** The running host, for subclasses adding their own handlers. */
    host(): Promise<Host>;
}

interface Started {
    host: Host;
    app: ActorApp;
    reminders: DurableObjectReminders;
    runtime: HostEndpointRuntime;
}

export function createHostDurableObject<Env = unknown>(
    options: HostDurableObjectOptions<Env>
): new (state: DurableObjectStateLike, env: Env) => HostDurableObjectInstance {
    const base = options.placement?.base ?? '/_sigx/do';
    const objectName = options.placement?.objectName ?? durableObjectName;

    return class HostDurableObject implements HostDurableObjectInstance {
        readonly #state: DurableObjectStateLike;
        readonly #env: Env;
        #starting: Promise<Started> | null = null;

        constructor(state: DurableObjectStateLike, env: Env) {
            this.#state = state;
            this.#env = env;
            // Nothing async here on purpose. `blockConcurrencyWhile` in a
            // constructor looks like the idiomatic place to boot, but a throw
            // inside it RESETS the object — so a transient start failure
            // would tear the isolate down and retry invisibly instead of
            // surfacing a 503 that says what went wrong.
        }

        /** Boot once per instance; never cache a rejection. */
        #ready(): Promise<Started> {
            return (this.#starting ??= this.#boot().catch((error: unknown) => {
                this.#starting = null;
                throw error;
            }));
        }

        async #boot(): Promise<Started> {
            const state = this.#state;
            if (state.id.name === undefined) {
                // Without a name there is nothing to compare a ref against, so
                // `isSelf` would answer false for EVERY ref — including this
                // object's own actor. The object would then fetch itself,
                // forever. Refuse to start rather than run a host that is
                // guaranteed to recurse.
                //
                // `name` is populated for ids from `idFromName`, which is what
                // the placement uses; an id parsed with `idFromString` has
                // none, and cannot host an actor here.
                throw new Error(
                    '[sigx actors-cloudflare] this Durable Object id has no name, so it ' +
                        'cannot tell which actor it hosts — every call would be forwarded ' +
                        'back out and the object would fetch itself. Route through ' +
                        'idFromName() (which the placement does), not idFromString().'
                );
            }
            const gate = <T,>(fn: () => Promise<T>): Promise<T> =>
                state.blockConcurrencyWhile(fn);

            const storage = durableObjectStorage(state.storage, {
                blockConcurrencyWhile: gate
            });
            const reminders = durableObjectReminders({
                storage: state.storage,
                alarms: state.storage,
                blockConcurrencyWhile: gate
            });

            const appBase: DurableAppOptions = {
                storage,
                reminders,
                // No idle sweeper: the platform evicts this whole object when
                // it goes idle, so a sweeper could never usefully fire — and
                // a pending interval would hold the object in memory, and
                // billable, for as long as the actor exists.
                defaults: { sweepIntervalMs: 0 }
            };
            const app = options.app?.(appBase) ?? defineActorApp(appBase);
            if (!app.hasActors) {
                if (!options.actors) {
                    throw new Error(
                        '[sigx actors-cloudflare] no actors registered — pass `actors` to ' +
                            'createHostDurableObject(), or call withActors() in the `app` factory.'
                    );
                }
                app.withActors(options.actors);
            }

            // The SAME placement the Worker uses, plus the self predicate.
            // `setPlacement` is exclusive, so an app factory that already
            // installed `durableObjects()` throws here naming both plugins —
            // the core's own guard IS the recursion guard.
            app.use(
                durableObjects({
                    namespace: options.namespace(this.#env),
                    isSelf: (ref) => objectName(ref) === state.id.name,
                    hostId: state.id.name ?? state.id.toString(),
                    ...options.placement
                })
            );

            const host = await app.start();
            return {
                host,
                app,
                reminders,
                runtime: this.#guardIdentity(hostEndpointRuntime(host))
            };
        }

        /**
         * Refuse a call for an actor this object does not host.
         *
         * Under Durable Objects `ref` → object id is a pure function, so this
         * cannot be a race the way a cluster's wrong-host can: it means the
         * Worker and this object disagree about `objectName`, `jurisdiction`
         * or which namespace is bound. Failing here names both sides, rather
         * than letting one actor quietly exist in two objects.
         *
         */
        #guardIdentity(runtime: HostEndpointRuntime): HostEndpointRuntime {
            // Always present: `#boot()` refuses to start without it.
            const own = this.#state.id.name as string;
            const check = (ref: ActorRef): void => {
                const expected = objectName(ref);
                if (expected === own) return;
                throw new Error(
                    `[sigx actors-cloudflare] this Durable Object is "${own}", but a call ` +
                        `for ${ref.type}/${ref.key} (which derives "${expected}") arrived. ` +
                        `The Worker and this object disagree about objectName/jurisdiction, ` +
                        `or the wrong namespace is bound.`
                );
            };
            return {
                resolve: (symbol) => runtime.resolve(symbol),
                dispatch: (ref, method, args, call) => {
                    check(ref);
                    return runtime.dispatch(ref, method, args, call);
                },
                dispatchStream: (ref, method, args, call) => {
                    check(ref);
                    return runtime.dispatchStream(ref, method, args, call);
                },
                dispatchWatch: (ref, method, args, call, watch) => {
                    check(ref);
                    return runtime.dispatchWatch(ref, method, args, call, watch);
                }
            };
        }

        async host(): Promise<Host> {
            return (await this.#ready()).host;
        }

        async fetch(request: Request): Promise<Response> {
            const { runtime } = await this.#ready();
            if (!matchesHostRequest(request, base)) {
                return new Response(
                    JSON.stringify({
                        error: {
                            message:
                                `[sigx actors-cloudflare] this Durable Object serves the ` +
                                `actor mount at ${base}/ only.`,
                            status: 404
                        }
                    }),
                    { status: 404, headers: { 'content-type': 'application/json' } }
                );
            }
            // No `secret`: a stub is not network-reachable, and holding the
            // namespace binding is the capability grant.
            return handleHostRequestForRuntime(request, {
                ...options.endpoint,
                runtime
            });
        }

        async alarm(): Promise<void> {
            // Booting first is required, not defensive: `onAlarm()` throws if
            // the host has not bound its reminders, and an alarm can be the
            // FIRST thing an evicted object sees.
            const { reminders } = await this.#ready();
            await reminders.onAlarm();
        }
    };
}
