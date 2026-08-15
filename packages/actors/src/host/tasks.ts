/**
 * The task ledger — what makes a detached task survive its host.
 *
 * One reserved storage record per actor (`$sigx:tasks` / actorId) listing
 * the runs currently in flight: `{ [name]: { input?, startedAt, restarts } }`.
 * A liveness reminder under the same reserved name is armed while the
 * ledger is non-empty; when the owning host dies, the reminder shards are
 * re-owned by the survivors, the next tick delivers through placement, the
 * actor re-activates wherever the cluster puts it, and activation restarts
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

/** The ledger over one actor's reserved record. `codec` is the host's
 *  state codec, so `input` survives the same vocabulary state does.
 *
 *  `codec.stringify` is optional and is `JSON.stringify(encode(value))` in
 *  ONE walk (#238). It collapses the walks a mutation costs from five —
 *  encode + stringify for the before-image, encode + stringify for the
 *  after, and the adapter's own — down to two, because the same string
 *  serves both the no-op compare and (via `saveText`) the write. */
export function taskLedger(
    storage: ActorStorage,
    actorId: string,
    codec: {
        encode(value: unknown): unknown;
        revive(value: unknown): unknown;
        stringify?(value: unknown): string | undefined;
    }
): TaskLedgerApi {
    let chain: Promise<unknown> = Promise.resolve();

    async function load(): Promise<{ ledger: TaskLedger; etag: string | null }> {
        const record = await storage.load(TASKS_TYPE, actorId);
        return {
            ledger: record ? (codec.revive(record.state) as TaskLedger) : {},
            etag: record?.etag ?? null
        };
    }

    // One walk to the JSON when the host supplied the fused emitter, two
    // otherwise. Identical bytes either way — that equality is the contract
    // `stringifyWithHandlers` is held to — so the compare below means the
    // same thing on both paths.
    //
    // An `undefined` from the fused emitter falls through to the pair rather
    // than to a substituted default: it is unreachable for a ledger (always
    // an object; only a top-level symbol or function returns `undefined`),
    // and inventing bytes would be worse than unreachable code — the no-op
    // compare cannot tell a stand-in value from a real one, so a wrong
    // string here silently turns a real edit into a skipped write.
    const toJson = (value: unknown): string =>
        codec.stringify?.(value) ?? JSON.stringify(codec.encode(value));

    async function mutateNow(edit: (ledger: TaskLedger) => void): Promise<TaskLedger> {
        for (let attempt = 1; ; attempt++) {
            const { ledger, etag } = await load();
            const before = toJson(ledger);
            edit(ledger);
            const json = toJson(ledger);
            // A no-op edit must not write (nor bump the etag).
            if (json === before) return ledger;
            try {
                if (Object.keys(ledger).length === 0) {
                    // An empty ledger is a DELETED record, not `{}` — the
                    // storage stays free of one tombstone per actor that
                    // ever ran a task.
                    if (etag !== null) await storage.clear(TASKS_TYPE, actorId, etag);
                } else if (storage.saveText) {
                    // The compare already produced exactly what the store
                    // wants; handing over the tree instead would make the
                    // adapter walk it a third time.
                    await storage.saveText(TASKS_TYPE, actorId, json, etag);
                } else {
                    await storage.save(TASKS_TYPE, actorId, codec.encode(ledger), etag);
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
