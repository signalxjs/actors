/**
 * Task liveness — how a detached run survives its host (#310).
 *
 * A running task is durable in the actor's own state (a job) or in its
 * `$sigx:tasks` ledger; what neither can do is get the actor RE-ACTIVATED
 * after the host holding it dies with nobody calling. That is the one job
 * of this seam: remember which actors have runs in flight on which host,
 * and touch a dead host's actors so a survivor resumes them.
 *
 * ## `rosterTaskLiveness()` — the default
 *
 * One roster per host, under the reserved type `$sigx:tasks-roster`:
 *
 *   {hostId}/p0..p15   { [actorId]: sinceMs }   — sub-sharded by the reminder
 *                                                 hash, so a busy host's record
 *                                                 stays small
 *   $hosts             { [hostId]: sinceMs }     — the index adoption reads
 *
 * The host is the SOLE WRITER of its own roster, which is what makes it
 * cheap: the table and etag live in memory, `track`/`untrack` are one CAS
 * each with no load, and every mutation that lands while a write is in
 * flight rides the next one (group commit per sub-shard). Nothing is
 * periodic per running task.
 *
 * Adoption rides the reminder tick. The host owning `reminderShardOf(hostId)`
 * (rendezvous over the membership view — one adopter per dead host, no
 * stampede) loads the index, and for every host `isHostLive` says is gone
 * it loads that host's sub-shards, touches each actor — the same
 * `$sigx:reminder`/`TASK_REMINDER` delivery the liveness reminder used, so
 * activation resumes the run and the adopting host's own roster tracks it —
 * drops what it touched, and deletes the drained roster and index entry.
 * Detection latency is the membership TTL plus a tick, as before.
 *
 * ## `reminderTaskLiveness()` — the previous mechanism, kept
 *
 * One durable reminder per actor with a run in flight (`TASK_REMINDER`,
 * period 60 s), armed on start and cleared when the ledger empties. Right
 * where a reminder IS the platform's wake-up — a Durable Object's alarm —
 * and wrong where reminders are a shared table: every start and finish
 * rewrote a 16th of the cluster's reminders, and every running task cost a
 * dispatch per minute (`jobs/many-running`, #307).
 */
import { isStorageConflict } from '../errors';
import type {
    ActorRef,
    ActorStorage,
    ActorTaskLiveness,
    ActorTaskLivenessContext
} from '../types';
import { reminderShardKeys, reminderShardOf } from './reminder-shards';
import { TASK_REMINDER, TASK_REMINDER_MS } from './tasks';

export const ROSTER_TYPE = '$sigx:tasks-roster';
/**
 * The index record's key. RESERVED: `bind()` refuses a host id that starts
 * with `$`, which is what keeps the index and the rosters apart — a host id
 * is otherwise an unconstrained string from the placement.
 */
export const ROSTER_INDEX_KEY = '$hosts';

type Roster = Record<string, number>;
type HostIndex = Record<string, number>;

/** Etag-conflict retries on the records that DO have several writers. */
const CAS_ATTEMPTS = 8;

interface Pending {
    edit: (table: Roster) => void;
    resolve: () => void;
    reject: (error: unknown) => void;
}

interface Shard {
    readonly key: string;
    table: Roster;
    etag: string | null;
    pending: Pending[];
    flushing: boolean;
}

function actorIdOf(ref: ActorRef): string {
    return `${ref.type}\u0000${ref.key}`;
}

function refOf(actorId: string): ActorRef | null {
    const nul = actorId.indexOf('\u0000');
    return nul < 0 ? null : { type: actorId.slice(0, nul), key: actorId.slice(nul + 1) };
}

export function rosterTaskLiveness(): ActorTaskLiveness {
    return new RosterTaskLiveness();
}

export function reminderTaskLiveness(): ActorTaskLiveness {
    return new ReminderTaskLiveness();
}

class RosterTaskLiveness implements ActorTaskLiveness {
    #context: ActorTaskLivenessContext | null = null;
    #shards = new Map<string, Shard>();
    #registered: Promise<void> | null = null;
    #stopTick: (() => void) | null = null;
    #adopting = false;

    bind(context: ActorTaskLivenessContext): void {
        if (this.#context) {
            throw new Error(
                '[sigx actors] this task-liveness instance is already bound to a host — ' +
                    'construct a new one per host.'
            );
        }
        if (context.hostId.length === 0 || context.hostId.startsWith('$')) {
            // The roster's records are keyed by host id; `$` is the reserved
            // prefix the index lives under, so such an id would alias it.
            throw new Error(
                `[sigx actors] host id "${context.hostId}" cannot be used for task liveness: ` +
                    'ids starting with "$" are reserved for the roster index.'
            );
        }
        this.#context = context;
    }

    #require(): ActorTaskLivenessContext {
        if (!this.#context) {
            throw new Error('[sigx actors] task liveness used before bind() — this is a host bug.');
        }
        return this.#context;
    }

    get #storage(): ActorStorage {
        return this.#require().storage;
    }

    start(): void {
        if (this.#stopTick) return;
        const context = this.#require();
        // Same switch as the reminder tick: a runtime with no background
        // execution disables both.
        if (context.tickMs <= 0) return;
        this.#stopTick = context.scheduler.every(context.tickMs, () => {
            void this.#adopt().catch((error) => {
                if (__DEV__) console.error('[sigx actors] task-roster adoption failed:', error);
            });
        });
    }

    stop(): void {
        this.#stopTick?.();
        this.#stopTick = null;
    }

    track(ref: ActorRef): Promise<void> {
        const id = actorIdOf(ref);
        // The index entry first, once per host: a roster nobody can find is
        // not durable. Awaited inside the mutation so `track` resolves only
        // when both are written.
        return this.#mutate(id, (table) => {
            table[id] = Date.now();
        });
    }

    untrack(ref: ActorRef): Promise<void> {
        const id = actorIdOf(ref);
        return this.#mutate(id, (table) => {
            delete table[id];
        });
    }

    // -----------------------------------------------------------------------
    // Our own roster: sole writer, cached, group-committed

    #shardFor(actorId: string): Shard {
        const key = `${this.#require().hostId}/${reminderShardOf(actorId)}`;
        let shard = this.#shards.get(key);
        if (!shard) {
            // A host id is minted per host INSTANCE and never reused, so its
            // records cannot exist yet: the cache starts empty with no etag
            // and never loads.
            shard = { key, table: {}, etag: null, pending: [], flushing: false };
            this.#shards.set(key, shard);
        }
        return shard;
    }

    #mutate(actorId: string, edit: (table: Roster) => void): Promise<void> {
        const shard = this.#shardFor(actorId);
        return new Promise<void>((resolve, reject) => {
            shard.pending.push({ edit, resolve, reject });
            void this.#flush(shard);
        });
    }

    async #flush(shard: Shard): Promise<void> {
        if (shard.flushing) return;
        shard.flushing = true;
        try {
            await (this.#registered ??= this.#register().catch((error) => {
                // A transient index failure must not wedge every later
                // track(): drop the memo so the next flush retries the
                // registration instead of re-awaiting a dead promise.
                this.#registered = null;
                throw error;
            }));
            while (shard.pending.length > 0) {
                // Everything queued so far rides ONE write; what lands during
                // the await forms the next batch.
                const batch = shard.pending;
                shard.pending = [];
                // Edits land on a COPY and become the table only once the
                // write is durable: a caller that sees a rejection must not
                // find its edit persisted by the next batch's write.
                const next: Roster = { ...shard.table };
                try {
                    for (const p of batch) p.edit(next);
                    await this.#write(shard, next);
                    shard.table = next;
                    for (const p of batch) p.resolve();
                } catch (error) {
                    for (const p of batch) p.reject(error);
                }
            }
        } catch (error) {
            // The index write failed: nothing queued can be durable.
            const batch = shard.pending;
            shard.pending = [];
            for (const p of batch) p.reject(error);
        } finally {
            shard.flushing = false;
        }
    }

    async #write(shard: Shard, table: Roster): Promise<void> {
        // Nothing stored and nothing to store — a track and its untrack
        // that coalesced into one batch — is not a write.
        if (shard.etag === null && Object.keys(table).length === 0) return;
        for (let attempt = 1; ; attempt++) {
            try {
                shard.etag = await this.#save(shard.key, table, shard.etag);
                return;
            } catch (error) {
                if (!isStorageConflict(error) || attempt >= CAS_ATTEMPTS) throw error;
                // Someone else wrote our record — an adopter that took this
                // host for dead. Our table is the truth for the runs that
                // are live HERE, so re-anchor on the stored etag and write it
                // back whole; the adopter's touches were idempotent.
                const record = await this.#storage.load(ROSTER_TYPE, shard.key);
                shard.etag = record?.etag ?? null;
            }
        }
    }

    #save(key: string, table: Roster, etag: string | null): Promise<string> {
        // Rosters are JSON-native — stored unencoded — so hand the store the
        // string outright where it takes one (#238).
        const storage = this.#storage;
        return storage.saveText
            ? storage.saveText(ROSTER_TYPE, key, JSON.stringify(table), etag)
            : storage.save(ROSTER_TYPE, key, table, etag);
    }

    async #register(): Promise<void> {
        const { hostId } = this.#require();
        for (let attempt = 1; ; attempt++) {
            const record = await this.#storage.load(ROSTER_TYPE, ROSTER_INDEX_KEY);
            const index = (record?.state as HostIndex | undefined) ?? {};
            if (hostId in index) return;
            index[hostId] = Date.now();
            try {
                await this.#save(ROSTER_INDEX_KEY, index, record?.etag ?? null);
                return;
            } catch (error) {
                if (!isStorageConflict(error) || attempt >= CAS_ATTEMPTS) throw error;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Everyone else's roster: adoption

    async #adopt(): Promise<void> {
        if (this.#adopting) return;
        this.#adopting = true;
        try {
            const context = this.#require();
            const record = await this.#storage.load(ROSTER_TYPE, ROSTER_INDEX_KEY);
            if (!record) return;
            const index = record.state as HostIndex;
            for (const hostId of Object.keys(index)) {
                if (hostId === context.hostId) continue;
                if (await context.isHostLive(hostId)) continue;
                // One adopter per dead host: the reminder-shard owner of the
                // host id, so N survivors split N dead hosts and never race.
                if (!(await context.ownsShard(reminderShardOf(hostId)))) continue;
                if (await this.#adoptHost(hostId)) await this.#forgetHost(hostId);
            }
        } finally {
            this.#adopting = false;
        }
    }

    /** Touch every actor in `hostId`'s roster; true once the roster is gone. */
    async #adoptHost(hostId: string): Promise<boolean> {
        const context = this.#require();
        let drained = true;
        for (const shard of reminderShardKeys()) {
            const key = `${hostId}/${shard}`;
            const record = await this.#storage.load(ROSTER_TYPE, key);
            if (!record) continue;
            const table = record.state as Roster;
            const ids = Object.keys(table);
            const outcomes = await Promise.allSettled(
                ids.map(async (id) => {
                    const ref = refOf(id);
                    if (!ref) return; // not ours to understand — drop it
                    await context.touch(ref);
                })
            );
            const remaining: Roster = {};
            outcomes.forEach((outcome, i) => {
                const id = ids[i]!;
                if (outcome.status === 'fulfilled') return;
                remaining[id] = table[id]!;
                if (__DEV__) {
                    console.error(
                        `[sigx actors] adopting ${id.replace('\u0000', '/')} from dead host ` +
                            `${hostId} failed — will retry next tick:`,
                        outcome.reason
                    );
                }
            });
            try {
                if (Object.keys(remaining).length === 0) {
                    await this.#storage.clear(ROSTER_TYPE, key, record.etag);
                } else {
                    drained = false;
                    await this.#save(key, remaining, record.etag);
                }
            } catch (error) {
                // A conflict means the host is writing after all, or another
                // adopter got here first; either way, not ours this tick.
                if (!isStorageConflict(error)) throw error;
                drained = false;
            }
        }
        return drained;
    }

    async #forgetHost(hostId: string): Promise<void> {
        for (let attempt = 1; ; attempt++) {
            const record = await this.#storage.load(ROSTER_TYPE, ROSTER_INDEX_KEY);
            if (!record) return;
            const index = record.state as HostIndex;
            if (!(hostId in index)) return;
            delete index[hostId];
            try {
                await this.#save(ROSTER_INDEX_KEY, index, record.etag);
                return;
            } catch (error) {
                if (!isStorageConflict(error) || attempt >= CAS_ATTEMPTS) throw error;
            }
        }
    }
}

class ReminderTaskLiveness implements ActorTaskLiveness {
    #context: ActorTaskLivenessContext | null = null;

    bind(context: ActorTaskLivenessContext): void {
        if (this.#context) {
            // Same rule as the roster: one instance per host. Re-binding
            // would route the previous host's track/untrack to this one.
            throw new Error(
                '[sigx actors] this task-liveness instance is already bound to a host — ' +
                    'construct a new one per host.'
            );
        }
        this.#context = context;
    }

    start(): void {}
    stop(): void {}

    track(ref: ActorRef): Promise<void> {
        return this.#require()
            .reminders(ref)
            .set(TASK_REMINDER, { due: TASK_REMINDER_MS, period: TASK_REMINDER_MS });
    }

    untrack(ref: ActorRef): Promise<void> {
        return this.#require().reminders(ref).clear(TASK_REMINDER);
    }

    #require(): ActorTaskLivenessContext {
        if (!this.#context) {
            throw new Error('[sigx actors] task liveness used before bind() — this is a host bug.');
        }
        return this.#context;
    }
}
