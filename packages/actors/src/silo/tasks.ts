/**
 * The task ledger — what makes a detached task survive its silo.
 *
 * One reserved storage record per actor (`$sigx:tasks` / actorId) listing
 * the runs currently in flight: `{ [name]: { input?, startedAt, restarts } }`.
 * A liveness reminder under the same reserved name is armed while the
 * ledger is non-empty; when the owning silo dies, the reminder shards are
 * re-owned by the survivors, the next tick delivers through placement, the
 * grain re-activates wherever the cluster puts it, and activation restarts
 * every ledgered run. At-least-once by contract: the runtime resumes the
 * FUNCTION, user code resumes the WORK from its own checkpointed state.
 *
 * Same CAS recipe as the reminder table (reload-and-reapply on etag
 * conflict, no-op edits write nothing), but per-actor rather than sharded:
 * the record has exactly one legitimate writer at a time — the activation
 * holding the directory claim — so conflicts are rare races, not steady
 * state.
 */
import { isStorageConflict } from '../errors';
import type { ActorStorage } from '../types';

export const TASKS_TYPE = '$sigx:tasks';
/** The liveness reminder's name (also reserved — never reaches onReminder). */
export const TASK_REMINDER = '$sigx:tasks';
/**
 * Liveness cadence, ms. Bounds crash-recovery latency at roughly
 * `TASK_REMINDER_MS + reminderTickMs`; 60s is the reminder period floor.
 */
export const TASK_REMINDER_MS = 60_000;

export interface TaskLedgerEntry {
    /** Codec-encoded start input, replayed on resume. */
    input?: unknown;
    /** Epoch-ms the run FIRST started (not this attempt). */
    startedAt: number;
    /** Times the runtime re-started the run after deactivation or crash. */
    restarts: number;
}
export type TaskLedger = Record<string, TaskLedgerEntry>;

export interface TaskLedgerApi {
    load(): Promise<TaskLedger>;
    /**
     * Reload-and-reapply CAS edit, serialized through one writer chain.
     * Resolves with the ledger as written (or as loaded, for a no-op
     * edit) — callers use it to see emptiness without a second read.
     */
    mutate(edit: (ledger: TaskLedger) => void): Promise<TaskLedger>;
}

const MUTATE_ATTEMPTS = 3;

/** The ledger over one actor's reserved record. `codec` is the silo's
 *  state codec, so `input` survives the same vocabulary state does. */
export function taskLedger(
    storage: ActorStorage,
    actorId: string,
    codec: { encode(value: unknown): unknown; revive(value: unknown): unknown }
): TaskLedgerApi {
    let chain: Promise<unknown> = Promise.resolve();

    async function load(): Promise<{ ledger: TaskLedger; etag: string | null }> {
        const record = await storage.load(TASKS_TYPE, actorId);
        return {
            ledger: record ? (codec.revive(record.state) as TaskLedger) : {},
            etag: record?.etag ?? null
        };
    }

    async function mutateNow(edit: (ledger: TaskLedger) => void): Promise<TaskLedger> {
        for (let attempt = 1; ; attempt++) {
            const { ledger, etag } = await load();
            const before = JSON.stringify(codec.encode(ledger));
            edit(ledger);
            const encoded = codec.encode(ledger);
            // A no-op edit must not write (nor bump the etag).
            if (JSON.stringify(encoded) === before) return ledger;
            try {
                if (Object.keys(ledger).length === 0) {
                    // An empty ledger is a DELETED record, not `{}` — the
                    // storage stays free of one tombstone per actor that
                    // ever ran a task.
                    if (etag !== null) await storage.clear(TASKS_TYPE, actorId, etag);
                } else {
                    await storage.save(TASKS_TYPE, actorId, encoded, etag);
                }
                return ledger;
            } catch (error) {
                if (!isStorageConflict(error) || attempt >= MUTATE_ATTEMPTS) throw error;
            }
        }
    }

    return {
        load: async () => (await load()).ledger,
        mutate(edit) {
            const work = () => mutateNow(edit);
            const run = chain.then(work, work);
            chain = run.catch(() => {});
            return run;
        }
    };
}
