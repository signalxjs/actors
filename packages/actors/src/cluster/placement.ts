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
    type AnyActorDefinition,
    type PlacementBindings,
    type Silo
} from '../types';
import { mintCallId } from '../call-id';
import { fnv1a, reminderShardKeys } from '../silo/reminder-shards';
import {
    createCounters,
    type ClusterCounters,
    type ClusterCounterTotals
} from './counters';
import { SILO_STATS_METHOD, SILO_STATS_TYPE, type SiloReport } from './stats';
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
    /** Per-actor-type policy overrides (e.g. pin hot session types local). */
    typePolicies?: Record<string, PlacementPolicy>;
    /** Wrong-host / unreachable re-resolve attempts. Default 3. */
    retries?: number;
    /**
     * Backoff between UNREACHABLE retries, ms (linear: n × this) — rides
     * out transient network blips without burning every attempt at once.
     * Wrong-host redirects retry immediately. Default 100.
     */
    retryBackoffMs?: number;
    /** Free-form placement hints published in the membership descriptor. */
    meta?: Record<string, string>;
}

export interface ClusterPlacement extends ActorPlacement {
    readonly identity: SiloIdentity;
    /** This silo's current membership descriptor. */
    descriptor(): SiloDescriptor;
    /** Whether THIS silo owns a reminder shard under the current view. */
    ownsReminderShard(shard: string): boolean;
    /**
     * Explicit rebalance primitive: gracefully deactivate ONE locally-owned
     * activation with reason `'migrated'` and release its claim — the next
     * call re-places it under the current policy. No-op if the actor is
     * not active on this silo. Automatic load-driven rebalancing composes
     * on top of this.
     */
    migrate(ref: ActorRef): Promise<void>;
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
    /** The membership view this silo currently holds. */
    view(): MembershipView;
    /** Pull-based routing/directory counters for THIS silo. Fresh object. */
    counters(): ClusterCounters;
    /**
     * This silo's operational snapshot — the local half of `clusterStats()`,
     * and exactly what a peer's `$sigx:silo#stats` answers. Safe before
     * `start()`: reports `'joining'` and zeroes.
     */
    report(): SiloReport;
    /**
     * Fetch a peer's report over the internal transport.
     *
     * @internal — the fan-out helper behind `clusterStats()`. It goes
     * straight to the transport, never through the routing loop, so reading
     * the counters cannot move them.
     */
    peerReport(
        target: SiloDescriptor,
        timeoutMs: number,
        signal?: AbortSignal
    ): Promise<SiloReport>;
    /**
     * @internal — the internal mount reports HMAC rejections here. Optional
     * on the interface so a hand-rolled placement passed to
     * `handleSiloRequest` need not implement it.
     */
    noteAuthFailure?(): void;
}

/**
 * Highest-random-weight (rendezvous) selection: every silo independently
 * picks the SAME member for a key, with a lexical tie-break so equal hashes
 * cannot split the answer. Shared by new-activation placement and reminder
 * shard ownership — two uses of one rule, not two rules that agree.
 */
function rendezvous(key: string, silos: readonly SiloDescriptor[]): SiloDescriptor | null {
    let best: SiloDescriptor | null = null;
    let bestScore = -1;
    for (const silo of silos) {
        const score = fnv1a(`${key}|${silo.siloId}`);
        if (score > bestScore || (score === bestScore && best !== null && silo.siloId < best.siloId)) {
            bestScore = score;
            best = silo;
        }
    }
    return best;
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
    name: 'random',
    choose(_ref, view, self) {
        const active = view.silos.filter((s) => s.status === 'active');
        if (active.length === 0) return self;
        return active[Math.floor(Math.random() * active.length)]!;
    }
};

/** Uniform random over active silos (the default). Exported for
 *  `typePolicies` maps that override only some types. */
export function randomPlacementPolicy(): PlacementPolicy {
    return randomPolicy;
}

/**
 * Rendezvous (highest-random-weight) placement: every silo deterministically
 * picks the SAME target for a new key, so racing activations agree without
 * a directory round-trip and misses stay rare. The directory remains the
 * arbiter; membership changes only re-home keys that hashed to the departed
 * silo.
 */
export function consistentHashPolicy(): PlacementPolicy {
    return {
        name: 'consistent-hash',
        choose(ref, view, self) {
            const active = view.silos.filter((s) => s.status === 'active');
            if (active.length === 0) return self;
            return rendezvous(actorId(ref), active) ?? self;
        }
    };
}

/** Pin new activations to the calling silo — sticky-LB-friendly for hot
 *  session-shaped types. */
export function preferLocalPolicy(): PlacementPolicy {
    return {
        name: 'prefer-local',
        choose: (_ref, _view, self) => self
    };
}

export function clusterPlacement(options: ClusterPlacementOptions): ClusterPlacement {
    return new ClusterPlacementImpl(options);
}

class ClusterPlacementImpl implements ClusterPlacement {
    readonly identity: SiloIdentity;
    #options: ClusterPlacementOptions;
    #policy: PlacementPolicy;
    #retries: number;
    #retryBackoffMs: number;
    /** Silo ids seen in a membership view — the departure diff base. */
    #seenSilos = new Set<string>();
    #transport: SiloTransport;
    #local: ActorDispatcher | null = null;
    #silo: Silo | null = null;
    /** type → its `defineActor({ placement })` policy, or null if none. */
    #declaredPolicies = new Map<string, PlacementPolicy | null>();
    /** Our live directory claims: actorId → the entry we wrote. */
    #claimed = new Map<string, DirectoryEntry>();
    /** actorId → siloId hint. Insertion-ordered Map as a cheap LRU. */
    #routeCache = new Map<string, string>();
    #seq = 0;
    #fenced = false;
    #status: SiloDescriptor['status'] = 'joining';
    #unsubscribe: (() => void)[] = [];
    /** Mutable counters — never handed out; `counters()` copies. */
    #counters: ClusterCounterTotals = createCounters();
    /** `performance.now()` at `start()`; 0 before. */
    #startedAt = 0;

    constructor(options: ClusterPlacementOptions) {
        this.identity = { siloId: `s.${randBase36(8)}`, epoch: Date.now() };
        this.#options = options;
        this.#policy = options.policy ?? randomPolicy;
        this.#retries = options.retries ?? 3;
        this.#retryBackoffMs = options.retryBackoffMs ?? 100;
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
                this.#counters.directoryClaims++;
                let winner = await this.#options.directory.claim(id, mine);
                if (
                    winner.activationId !== mine.activationId &&
                    winner.siloId === this.identity.siloId &&
                    !this.#claimed.has(id)
                ) {
                    // A stale entry naming US without a live claim behind it
                    // (leftover from a lost release) — reclaim, don't bounce
                    // callers off our own ghost.
                    this.#counters.directoryEvictions++;
                    await this.#options.directory.evict(id, winner);
                    this.#counters.directoryClaims++;
                    winner = await this.#options.directory.claim(id, mine);
                }
                if (winner.activationId !== mine.activationId) {
                    this.#counters.claimConflicts++;
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
                this.#counters.directoryReleases++;
                await this.#options.directory.release(id, entry);
            },
            strictChainPresence: true,
            ownsReminderShard: (shard) => this.ownsReminderShard(shard),
            stopReason: 'migrated'
        };
    }

    async migrate(ref: ActorRef): Promise<void> {
        if (!this.#claimed.has(actorId(ref))) return;
        await this.#silo?.deactivate(ref, 'migrated');
    }

    /**
     * Rendezvous hashing over the ACTIVE membership view: for each shard,
     * the silo with the highest hash(shard, siloId) owns it. Deterministic
     * per view — no stored assignment, no lease. Transient view divergence
     * (two silos both claiming a shard) is safe: the reminder shard's etag
     * CAS keeps firing at-most-once regardless.
     */
    ownsReminderShard(shard: string): boolean {
        const active = this.#options.membership
            .view()
            .silos.filter((s) => s.status === 'active');
        if (active.length === 0) return true; // solo / not started
        return rendezvous(shard, active)?.siloId === this.identity.siloId;
    }

    async start(): Promise<void> {
        this.#status = 'active';
        await this.#options.membership.join(this.descriptor());
        this.#startedAt = performance.now();
        this.#seenSilos = new Set(
            this.#options.membership.view().silos.map((s) => s.siloId)
        );
        this.#unsubscribe.push(
            this.#options.membership.onChange((view) => {
                this.#counters.membershipChanges++;
                this.#pruneRoutes(view);
                void this.#sweepDeparted(view);
            }),
            this.#options.membership.onSelfSuspect(() => void this.#fence())
        );
    }

    async beginStop(): Promise<void> {
        // Runs BEFORE the drain: peers must stop placing new actors here
        // while activations hand off (their policies filter on 'active').
        this.#status = 'leaving';
        try {
            await this.#options.membership.setStatus('leaving');
        } catch {
            // Leaving is best-effort; the heartbeat TTL is the backstop.
        }
    }

    async stop(): Promise<void> {
        // silo.stop() has already drained activations (releasing claims,
        // reason 'migrated') between beginStop() and here.
        for (const unsub of this.#unsubscribe) unsub();
        this.#unsubscribe = [];
        this.#status = 'leaving';
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
        // Counted separately from `remoteDispatches` and NEVER summed with
        // it: this is the same logical call the caller already counted on
        // its own side. Note that `#local` is the RAW local dispatcher, so
        // an inbound call does not pass through `useDispatch` middleware —
        // `metrics()` counts a cross-silo call once, on the caller.
        this.#counters.inboundDispatches++;
        return this.#local!.dispatch(ref, method, args, call);
    }

    dispatchInboundStream(
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ): AsyncIterable<unknown> {
        this.#counters.inboundStreams++;
        return this.#local!.dispatchStream!(ref, method, args, call);
    }

    // -----------------------------------------------------------------------
    // Ops

    view(): MembershipView {
        return this.#options.membership.view();
    }

    /** @internal — the internal mount reports HMAC rejections here. */
    noteAuthFailure(): void {
        this.#counters.authFailures++;
    }

    counters(): ClusterCounters {
        return {
            ...this.#counters,
            // Gauges are read here rather than tracked, so they cannot drift.
            claimed: this.#claimed.size,
            routeCacheSize: this.#routeCache.size,
            membershipVersion: this.#options.membership.view().version,
            status: this.#fenced ? 'fenced' : this.#status
        };
    }

    report(): SiloReport {
        const shards = reminderShardKeys().filter((shard) => this.ownsReminderShard(shard));
        return {
            v: 1,
            siloId: this.identity.siloId,
            epoch: this.identity.epoch,
            address: this.#options.advertise,
            status: this.#fenced ? 'fenced' : this.#status,
            stats: this.#silo?.stats() ?? { activations: 0, queued: 0, perType: {} },
            counters: this.counters(),
            reminderShards: shards,
            uptimeMs: this.#startedAt === 0 ? 0 : Math.round(performance.now() - this.#startedAt),
            ...(this.#options.meta ? { meta: this.#options.meta } : {})
        };
    }

    /** @internal — straight to the transport, never through the routing loop,
     *  so reading the counters cannot move them. */
    async peerReport(
        target: SiloDescriptor,
        timeoutMs: number,
        signal?: AbortSignal
    ): Promise<SiloReport> {
        const timeout = AbortSignal.timeout(timeoutMs);
        const call: ActorCallContext = {
            callChain: [],
            callId: mintCallId(),
            // Bounded from both ends: the abort stops a hung socket here,
            // the deadline lets the peer give up on its own clock.
            deadline: Date.now() + timeoutMs,
            abortSignal: signal ? AbortSignal.any([timeout, signal]) : timeout
        };
        const report = (await this.#transport
            .dispatcherFor(target)
            .dispatch(
                { type: SILO_STATS_TYPE, key: SILO_STATS_METHOD },
                SILO_STATS_METHOD,
                [],
                call
            )) as SiloReport | undefined;
        if (!report || report.v !== 1) {
            // A rolling deploy IS a mixed-version cluster, and the ops tool
            // has to keep working during the deploy that broke things.
            throw Object.assign(
                new Error(
                    `[sigx actors] ${target.siloId} answered an unsupported stats payload ` +
                        `(v${String(report?.v)})`
                ),
                { status: 404 }
            );
        }
        return report;
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

    /**
     * Proactive directory hygiene: when a silo we have seen disappears
     * from the view AND the store confirms it dead (a graceful leaver
     * already released its claims), sweep its entries so callers never
     * trip over them. Racing survivors are fine — eviction is idempotent.
     * Lazy eviction on lookup remains the backstop.
     */
    async #sweepDeparted(view: MembershipView): Promise<void> {
        const live = new Set(view.silos.map((s) => s.siloId));
        const departed = [...this.#seenSilos].filter(
            (id) => !live.has(id) && id !== this.identity.siloId
        );
        for (const id of live) this.#seenSilos.add(id);
        if (departed.length === 0 || !this.#options.directory.evictSilo) return;
        for (const id of departed) {
            // A silo only stops being "seen" once its sweep completed (or
            // the store says it is in fact alive-and-absent-from-view for
            // now) — a transient view drop or a failed sweep keeps it on
            // the list, so the next membership change tries again.
            try {
                if (await this.#options.membership.isAlive(id)) continue;
                this.#counters.siloSweeps++;
                this.#counters.sweptEntries += await this.#options.directory.evictSilo(id);
                this.#seenSilos.delete(id);
            } catch (error) {
                if (__DEV__) {
                    console.error(`[sigx actors] directory sweep for ${id} failed:`, error);
                }
            }
        }
    }

    /** Self-fence: membership lost — stop claiming, drop what we hold. */
    async #fence(): Promise<void> {
        if (this.#fenced) return;
        this.#fenced = true;
        this.#counters.selfFences++;
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
            if (cached === this.identity.siloId) {
                this.#counters.routeCacheHits++;
                return 'local';
            }
            const member = this.#member(cached);
            if (member) {
                this.#counters.routeCacheHits++;
                return member;
            }
            this.#routeCache.delete(id);
        }
        this.#counters.routeCacheMisses++;

        this.#counters.directoryLookups++;
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
                this.#counters.directoryEvictions++;
                await this.#options.directory.evict(id, entry);
            }
        }

        const view = this.#options.membership.view();
        if (view.silos.length === 0) return 'local'; // not started / solo
        const policy =
            (await this.#declaredPolicy(ref.type)) ??
            this.#options.typePolicies?.[ref.type] ??
            this.#policy;
        const chosen = policy.choose(ref, view, this.descriptor());
        // Sticky: concurrent activations of one key must agree on a target
        // so racing dispatches join one claim instead of splitting.
        this.#cacheRoute(id, chosen.siloId);
        return chosen.siloId === this.identity.siloId ? 'local' : chosen;
    }

    /**
     * The strategy an actor declared with `defineActor({ placement })` —
     * Orleans's placement attribute, and the highest-precedence answer.
     *
     * Resolved lazily and memoized per TYPE: a `virtual:sigx-actors`
     * registry only loads a type's module on demand, so this cannot be
     * gathered up front. Types with no declaration memoize `null`, making
     * this one map lookup per dispatch after the first.
     */
    async #declaredPolicy(type: string): Promise<PlacementPolicy | undefined> {
        const memo = this.#declaredPolicies.get(type);
        if (memo !== undefined) return memo ?? undefined;

        const silo = this.#silo;
        if (!silo) return undefined;
        let def: AnyActorDefinition | null;
        try {
            def = (await silo.definition(type)) ?? null;
        } catch {
            // A failed module load is the dispatch path's problem to report,
            // not the placement's — fall through to the configured policy.
            return undefined;
        }
        const declared = def?.__sigxActor.placement;
        const policy =
            declared && typeof (declared as PlacementPolicy).choose === 'function'
                ? (declared as PlacementPolicy)
                : null;
        if (__DEV__ && declared && !policy) {
            console.warn(
                `[sigx actors] actor "${type}" declares placement ` +
                    `"${declared.name ?? 'unnamed'}", which is ` +
                    `not a cluster PlacementPolicy (no choose()) — ignored. It is probably a ` +
                    `strategy for a different placement backend.`
            );
        }
        this.#declaredPolicies.set(type, policy);
        return policy ?? undefined;
    }

    /**
     * Consume a routing failure. Returns how to retry: wrong-host retries
     * immediately (the redirect told us where); unreachable and a REMOTE
     * peer's shutdown retry after a backoff (a blip, or a rolling deploy
     * releasing its claims as it drains); null = not ours, rethrow. A
     * LOCAL shutdown is never retried — this silo really is stopping.
     */
    async #noteFailure(
        id: string,
        error: unknown,
        remote: boolean
    ): Promise<'wrong-host' | 'unreachable' | 'draining' | null> {
        if (isActorError(error) && error.kind === 'wrong-host') {
            this.#counters.wrongHostRedirects++;
            this.#routeCache.delete(id);
            const owner = (error as ActorWrongHostError).owner;
            if (owner?.siloId && owner.siloId !== this.identity.siloId) {
                this.#cacheRoute(id, owner.siloId);
            }
            return 'wrong-host';
        }
        if (isActorError(error) && error.kind === 'unreachable') {
            this.#counters.unreachableRetries++;
            this.#routeCache.delete(id);
            this.#counters.directoryLookups++;
            const entry = await this.#options.directory.lookup(id);
            if (entry && !(await this.#options.membership.isAlive(entry.siloId))) {
                this.#counters.directoryEvictions++;
                await this.#options.directory.evict(id, entry);
            }
            await this.#options.membership.refresh();
            return 'unreachable';
        }
        if (remote && isActorError(error) && error.kind === 'silo-shutdown') {
            this.#counters.drainingRetries++;
            // The owner is handing off: its claim releases as the actor
            // drains — don't evict, just re-resolve after a backoff (the
            // refreshed view excludes the leaver from placement).
            this.#routeCache.delete(id);
            await this.#options.membership.refresh();
            return 'draining';
        }
        return null;
    }

    #backoff(attempt: number): Promise<void> {
        const ms = this.#retryBackoffMs * (attempt + 1);
        if (ms <= 0) return Promise.resolve();
        return new Promise((r) => setTimeout(r, ms));
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
            if (attempt > 0) this.#counters.retries++;
            const target = await this.#resolveTarget(ref);
            try {
                if (target === 'local') {
                    this.#counters.routedLocal++;
                    return await this.#local!.dispatch(ref, method, args, call);
                }
                this.#counters.remoteDispatches++;
                return await this.#transport
                    .dispatcherFor(target)
                    .dispatch(ref, method, args, call);
            } catch (error) {
                const failure = await this.#noteFailure(id, error, target !== 'local');
                if (!failure) throw error;
                if (failure !== 'wrong-host' && attempt < this.#retries) {
                    await this.#backoff(attempt);
                }
                lastError = error;
            }
        }
        this.#counters.routingFailures++;
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
        const noteFailure = (
            error: unknown,
            remote: boolean
        ): Promise<'wrong-host' | 'unreachable' | 'draining' | null> =>
            this.#noteFailure(id, error, remote);
        const backoff = (attempt: number): Promise<void> => this.#backoff(attempt);
        const local = this.#local!;
        const transport = this.#transport;
        const retries = this.#retries;
        // Captured like `local`/`transport`: `run()` is a free generator, so
        // `this` must not leak into it.
        const counters = this.#counters;
        // Placement errors (wrong-host, unreachable) surface on the FIRST
        // pull — the endpoint pump pulls the first chunk before responding,
        // so buffering one chunk here matches established stream semantics.
        async function* run(): AsyncGenerator<unknown> {
            let lastError: unknown;
            for (let attempt = 0; attempt <= retries; attempt++) {
                if (attempt > 0) counters.retries++;
                const target = await resolveTarget();
                let iterable: AsyncIterable<unknown>;
                if (target === 'local') {
                    counters.routedLocal++;
                    iterable = local.dispatchStream!(ref, method, args, call);
                } else {
                    counters.remoteStreams++;
                    iterable = transport
                        .dispatcherFor(target)
                        .dispatchStream!(ref, method, args, call);
                }
                const iterator = iterable[Symbol.asyncIterator]();
                let first: IteratorResult<unknown>;
                try {
                    first = await iterator.next();
                } catch (error) {
                    const failure = await noteFailure(error, target !== 'local');
                    if (!failure) throw error;
                    if (failure !== 'wrong-host' && attempt < retries) await backoff(attempt);
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
            counters.routingFailures++;
            throw new ActorActivationError(actorLabel(ref), {
                cause:
                    lastError ??
                    new Error(`placement did not converge after ${retries + 1} attempts`)
            });
        }
        return run();
    }
}
