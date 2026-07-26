/**
 * `clusterPlacement` — the distributed `ActorPlacement`. Ownership lives in
 * the directory (claim-on-activate, release-on-deactivate through the
 * `PlacementBindings` seam); routing resolves local-first, then the route
 * cache, then the directory, then the placement policy. Misdirected calls
 * come back as wrong-host redirects (never proxied onward) and the loop
 * here re-resolves and retries, bounded.
 *
 * Correctness never rests on this file being right: the storage etag CAS
 * remains the integrity floor — a briefly-wrong route or a split-brain
 * window costs a rejected save and a fault-and-reload, not corruption.
 */
import {
    ActorActivationError,
    ActorUnreachableError,
    ActorWrongHostError,
    isActorError
} from '../errors';
import {
    actorId,
    actorLabel,
    type ActorCallContext,
    type ActorDispatcher,
    type ActorPlacement,
    type ActorRef,
    type PlacementBindings,
    type Silo
} from '../types';
import { createSiloTransport, type SiloTransport } from './transport';
import type {
    ClusterProviders,
    DirectoryEntry,
    MembershipView,
    PlacementPolicy,
    SiloDescriptor,
    SiloIdentity
} from './types';

export interface ClusterPlacementOptions extends ClusterProviders {
    /** Peer-reachable origin of this silo's HTTP listener. */
    advertise: string;
    /** Shared cluster secret for the internal mount. */
    secret?: string;
    /** Path prefix of the internal mount. Default `/_sigx/silo`. */
    internalBase?: string;
    /** Fetch implementation (tests pipe it straight into peers' handlers). */
    fetch?: typeof globalThis.fetch;
    /** Placement policy for NEW activations. Default: uniform random. */
    policy?: PlacementPolicy;
    /** Wrong-host / unreachable re-resolve attempts. Default 3. */
    retries?: number;
    /** Free-form placement hints published in the membership descriptor. */
    meta?: Record<string, string>;
}

export interface ClusterPlacement extends ActorPlacement {
    readonly identity: SiloIdentity;
    /** This silo's current membership descriptor. */
    descriptor(): SiloDescriptor;
    /**
     * Inbound side, consumed by `handleSiloRequest`: dispatch LOCALLY or
     * throw wrong-host — never forward (redirect-not-proxy).
     */
    dispatchInbound(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): Promise<unknown>;
    dispatchInboundStream(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): AsyncIterable<unknown>;
}

const ROUTE_CACHE_MAX = 10_000;

function randBase36(length: number): string {
    const cryptoObj = (globalThis as { crypto?: Crypto }).crypto;
    if (cryptoObj?.getRandomValues) {
        const bytes = new Uint8Array(length);
        cryptoObj.getRandomValues(bytes);
        return Array.from(bytes, (b) => (b % 36).toString(36)).join('');
    }
    let out = '';
    while (out.length < length) out += Math.random().toString(36).slice(2);
    return out.slice(0, length);
}

/** Uniform random over active silos — the Orleans default posture. */
const randomPolicy: PlacementPolicy = {
    choose(_ref, view, self) {
        const active = view.silos.filter((s) => s.status === 'active');
        if (active.length === 0) return self;
        return active[Math.floor(Math.random() * active.length)]!;
    }
};

export function clusterPlacement(options: ClusterPlacementOptions): ClusterPlacement {
    return new ClusterPlacementImpl(options);
}

class ClusterPlacementImpl implements ClusterPlacement {
    readonly identity: SiloIdentity;
    #options: ClusterPlacementOptions;
    #policy: PlacementPolicy;
    #retries: number;
    #transport: SiloTransport;
    #local: ActorDispatcher | null = null;
    #silo: Silo | null = null;
    /** Our live directory claims: actorId → the entry we wrote. */
    #claimed = new Map<string, DirectoryEntry>();
    /** actorId → siloId hint. Insertion-ordered Map as a cheap LRU. */
    #routeCache = new Map<string, string>();
    #seq = 0;
    #fenced = false;
    #status: SiloDescriptor['status'] = 'joining';
    #unsubscribe: (() => void)[] = [];

    constructor(options: ClusterPlacementOptions) {
        this.identity = { siloId: `s.${randBase36(8)}`, epoch: Date.now() };
        this.#options = options;
        this.#policy = options.policy ?? randomPolicy;
        this.#retries = options.retries ?? 3;
        this.#transport = createSiloTransport({
            siloId: this.identity.siloId,
            internalBase: options.internalBase ?? '/_sigx/silo',
            ...(options.secret !== undefined ? { secret: options.secret } : {}),
            ...(options.fetch ? { fetch: options.fetch } : {})
        });
    }

    descriptor(): SiloDescriptor {
        return {
            ...this.identity,
            address: this.#options.advertise,
            status: this.#status,
            ...(this.#options.meta ? { meta: this.#options.meta } : {})
        };
    }

    // -----------------------------------------------------------------------
    // ActorPlacement

    bind(local: ActorDispatcher, silo: Silo): PlacementBindings {
        this.#local = local;
        this.#silo = silo;
        return {
            beforeActivate: async (ref) => {
                if (this.#fenced) {
                    throw new ActorUnreachableError(
                        `${this.identity.siloId} (self, fenced — membership lost)`
                    );
                }
                const id = actorId(ref);
                const mine: DirectoryEntry = {
                    siloId: this.identity.siloId,
                    activationId: `${this.identity.siloId}/${this.identity.epoch}/${++this.#seq}`
                };
                let winner = await this.#options.directory.claim(id, mine);
                if (
                    winner.activationId !== mine.activationId &&
                    winner.siloId === this.identity.siloId &&
                    !this.#claimed.has(id)
                ) {
                    // A stale entry naming US without a live claim behind it
                    // (leftover from a lost release) — reclaim, don't bounce
                    // callers off our own ghost.
                    await this.#options.directory.evict(id, winner);
                    winner = await this.#options.directory.claim(id, mine);
                }
                if (winner.activationId !== mine.activationId) {
                    throw new ActorWrongHostError(actorLabel(ref), {
                        siloId: winner.siloId,
                        ...(this.#address(winner.siloId) !== undefined
                            ? { address: this.#address(winner.siloId)! }
                            : {})
                    });
                }
                this.#claimed.set(id, mine);
            },
            afterDeactivate: async (ref) => {
                const id = actorId(ref);
                const entry = this.#claimed.get(id);
                if (!entry) return;
                this.#claimed.delete(id);
                await this.#options.directory.release(id, entry);
            },
            strictChainPresence: true,
            shouldTickReminders: () =>
                this.#options.reminderLease
                    ? this.#options.reminderLease.tryHold(this.identity.siloId)
                    : true
        };
    }

    async start(): Promise<void> {
        this.#status = 'active';
        await this.#options.membership.join(this.descriptor());
        this.#unsubscribe.push(
            this.#options.membership.onChange((view) => this.#pruneRoutes(view)),
            this.#options.membership.onSelfSuspect(() => void this.#fence())
        );
    }

    async stop(): Promise<void> {
        // silo.stop() has already drained activations (releasing claims)
        // before placement.stop() runs.
        for (const unsub of this.#unsubscribe) unsub();
        this.#unsubscribe = [];
        this.#status = 'leaving';
        try {
            await this.#options.membership.setStatus('leaving');
        } catch {
            // Leaving is best-effort; the heartbeat TTL is the backstop.
        }
        if (this.#options.reminderLease) {
            try {
                await this.#options.reminderLease.release(this.identity.siloId);
            } catch {
                // Lease expiry is the backstop.
            }
        }
        await this.#options.membership.leave();
    }

    dispatcherFor(ref: ActorRef): ActorDispatcher {
        // Local fast path: we hold the claim, no store reads, no routing.
        if (this.#claimed.has(actorId(ref))) return this.#local!;
        return this.#routing;
    }

    // -----------------------------------------------------------------------
    // Inbound (redirect-not-proxy: local or wrong-host, never forward)

    dispatchInbound(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): Promise<unknown> {
        return this.#local!.dispatch(ref, method, args, call);
    }

    dispatchInboundStream(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): AsyncIterable<unknown> {
        return this.#local!.dispatchStream!(ref, method, args, call);
    }

    // -----------------------------------------------------------------------
    // Routing

    /** One shared dispatcher that resolves + retries per call. */
    #routing: ActorDispatcher = {
        dispatch: (ref, method, args, call) => this.#routedDispatch(ref, method, args, call),
        dispatchStream: (ref, method, args, call) =>
            this.#routedStream(ref, method, args, call)
    };

    #address(siloId: string): string | undefined {
        return this.#options.membership
            .view()
            .silos.find((s) => s.siloId === siloId)?.address;
    }

    #member(siloId: string): SiloDescriptor | undefined {
        return this.#options.membership.view().silos.find((s) => s.siloId === siloId);
    }

    #cacheRoute(id: string, siloId: string): void {
        if (this.#routeCache.size >= ROUTE_CACHE_MAX) {
            const oldest = this.#routeCache.keys().next().value;
            if (oldest !== undefined) this.#routeCache.delete(oldest);
        }
        this.#routeCache.delete(id);
        this.#routeCache.set(id, siloId);
    }

    #pruneRoutes(view: MembershipView): void {
        const live = new Set(view.silos.map((s) => s.siloId));
        for (const [id, siloId] of this.#routeCache) {
            if (!live.has(siloId)) this.#routeCache.delete(id);
        }
    }

    /** Self-fence: membership lost — stop claiming, drop what we hold. */
    async #fence(): Promise<void> {
        if (this.#fenced) return;
        this.#fenced = true;
        const types = new Set<string>();
        for (const id of this.#claimed.keys()) {
            const nul = id.indexOf('\u0000');
            if (nul > 0) types.add(id.slice(0, nul));
        }
        for (const type of types) {
            await this.#silo?.deactivateType(type).catch(() => {});
        }
    }

    /** Resolve who should serve `ref` right now: us, or a peer. */
    async #resolveTarget(ref: ActorRef): Promise<'local' | SiloDescriptor> {
        const id = actorId(ref);
        if (this.#claimed.has(id)) return 'local';

        const cached = this.#routeCache.get(id);
        if (cached !== undefined) {
            if (cached === this.identity.siloId) return 'local';
            const member = this.#member(cached);
            if (member) return member;
            this.#routeCache.delete(id);
        }

        const entry = await this.#options.directory.lookup(id);
        if (entry) {
            if (entry.siloId === this.identity.siloId) return 'local';
            const member = this.#member(entry.siloId);
            if (member) {
                this.#cacheRoute(id, entry.siloId);
                return member;
            }
            if (!(await this.#options.membership.isAlive(entry.siloId))) {
                // Dead owner: reclaim lazily and place fresh below.
                await this.#options.directory.evict(id, entry);
            }
        }

        const view = this.#options.membership.view();
        if (view.silos.length === 0) return 'local'; // not started / solo
        const chosen = this.#policy.choose(ref, view, this.descriptor());
        // Sticky: concurrent activations of one key must agree on a target
        // so racing dispatches join one claim instead of splitting.
        this.#cacheRoute(id, chosen.siloId);
        return chosen.siloId === this.identity.siloId ? 'local' : chosen;
    }

    /** Consume a routing failure; true = re-resolve and retry. */
    async #noteFailure(id: string, error: unknown): Promise<boolean> {
        if (isActorError(error) && error.kind === 'wrong-host') {
            this.#routeCache.delete(id);
            const owner = (error as ActorWrongHostError).owner;
            if (owner?.siloId && owner.siloId !== this.identity.siloId) {
                this.#cacheRoute(id, owner.siloId);
            }
            return true;
        }
        if (isActorError(error) && error.kind === 'unreachable') {
            this.#routeCache.delete(id);
            const entry = await this.#options.directory.lookup(id);
            if (entry && !(await this.#options.membership.isAlive(entry.siloId))) {
                await this.#options.directory.evict(id, entry);
            }
            await this.#options.membership.refresh();
            return true;
        }
        return false;
    }

    async #routedDispatch(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): Promise<unknown> {
        const id = actorId(ref);
        let lastError: unknown;
        for (let attempt = 0; attempt <= this.#retries; attempt++) {
            const target = await this.#resolveTarget(ref);
            try {
                if (target === 'local') {
                    return await this.#local!.dispatch(ref, method, args, call);
                }
                return await this.#transport
                    .dispatcherFor(target)
                    .dispatch(ref, method, args, call);
            } catch (error) {
                if (!(await this.#noteFailure(id, error))) throw error;
                lastError = error;
            }
        }
        throw new ActorActivationError(actorLabel(ref), {
            cause:
                lastError ??
                new Error(`placement did not converge after ${this.#retries + 1} attempts`)
        });
    }

    #routedStream(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): AsyncIterable<unknown> {
        const id = actorId(ref);
        const resolveTarget = (): Promise<'local' | SiloDescriptor> => this.#resolveTarget(ref);
        const noteFailure = (error: unknown): Promise<boolean> => this.#noteFailure(id, error);
        const local = this.#local!;
        const transport = this.#transport;
        const retries = this.#retries;
        // Placement errors (wrong-host, unreachable) surface on the FIRST
        // pull — the endpoint pump pulls the first chunk before responding,
        // so buffering one chunk here matches established stream semantics.
        async function* run(): AsyncGenerator<unknown> {
            let lastError: unknown;
            for (let attempt = 0; attempt <= retries; attempt++) {
                const target = await resolveTarget();
                const iterable =
                    target === 'local'
                        ? local.dispatchStream!(ref, method, args, call)
                        : transport
                              .dispatcherFor(target)
                              .dispatchStream!(ref, method, args, call);
                const iterator = iterable[Symbol.asyncIterator]();
                let first: IteratorResult<unknown>;
                try {
                    first = await iterator.next();
                } catch (error) {
                    if (!(await noteFailure(error))) throw error;
                    lastError = error;
                    continue;
                }
                let finished = false;
                try {
                    if (first.done) {
                        finished = true;
                        return;
                    }
                    yield first.value;
                    for (;;) {
                        const next = await iterator.next();
                        if (next.done) {
                            finished = true;
                            return;
                        }
                        yield next.value;
                    }
                } finally {
                    if (!finished && iterator.return) await iterator.return(undefined);
                }
            }
            throw new ActorActivationError(actorLabel(ref), {
                cause:
                    lastError ??
                    new Error(`placement did not converge after ${retries + 1} attempts`)
            });
        }
        return run();
    }
}
