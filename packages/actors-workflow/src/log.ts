/**
 * The run's durable event log, and the reducer that folds it back.
 *
 * A run's state is written as a SEQUENCE OF ENTRIES rather than a series
 * of whole-state saves. `ctx.append(entry)` is O(entry) where `ctx.save()`
 * is O(state) — measured at 19.8 µs at the head of a 300-step run against
 * 113 µs at its tail, because a whole-state save re-encodes everything the
 * run has accumulated every time it takes a step (`seams.md`, #312).
 *
 * A workflow run is exactly the shape that punishes: its variables only
 * grow, and it takes a step per node. So every transition is an append,
 * and a full save happens only where the record is being rewritten
 * anyway — at a durable sleep and at the terminal state. Those are the
 * compaction points; between them the log is the truth.
 *
 * The entries are a closed union, and deliberately small: an entry is
 * replayed by every host that ever loads the run, so it must mean the same
 * thing to a build that has not been deployed yet. Nothing here holds a
 * timestamp derived from the reading host, a reference to a definition, or
 * anything else that could be interpreted differently later.
 */
import type { Json, RunStatus } from './types';

export type RunEntry =
    /** The run began: the pinned definition and the starting variables. */
    | { t: 'start'; def: string; version: number; at: number; vars: Record<string, Json> }
    /** The cursor moved. `seq` fences wakes; see `run.ts`. */
    | { t: 'move'; to: string; seq: number }
    /** A task produced a value. Separate from `move` because a task that
     *  assigns and then fails must not look like a step that happened. */
    | { t: 'vars'; set: Record<string, Json> }
    /** An attempt is about to be made. Written BEFORE the call, which is
     *  what makes task execution at-least-once rather than at-most-once:
     *  a host that dies here re-runs the attempt, and never silently
     *  skips one it may have completed. */
    | { t: 'attempt'; node: string; n: number }
    /**
     * The run is asleep until `until`, woken by `wake`.
     *
     * `after` is what to do on waking: a node id to move to, or `null` to
     * re-enter the current node. Without it a wake returns to the same
     * cursor, and a `delay` node sleeps again on arrival — an infinite
     * nap that looks exactly like progress from the outside, since the
     * status keeps flipping. It also distinguishes the two reasons to
     * sleep: finishing a delay MOVES (which resets the attempt counter),
     * while a retry backoff must come back to the same node with its
     * attempts intact.
     */
    | { t: 'sleep'; until: number; seq: number; durable: boolean; after: string | null }
    /** Awake again — the wake fired or a touch recovered it. */
    | { t: 'wake'; seq: number }
    /** Terminal. */
    | { t: 'end'; status: 'completed' | 'failed'; at: number; error?: string };

/** The persisted shape. Only ever built by `applyRunEntry`. */
export interface RunState {
    def: string;
    version: number;
    status: RunStatus;
    cursor: string;
    vars: Record<string, Json>;
    startedAt: number;
    endedAt?: number;
    error?: string;
    transitions: number;
    /** Attempts made at the CURRENT node — reset by a move. */
    attempt: number;
    /**
     * The single outstanding wake, or null. `seq` is the fence: a wake
     * whose token is not this one is stale and ignored, which is what
     * makes a duplicate reminder tick, a timer re-armed after migration,
     * and a touch racing a real wake all harmless.
     */
    wake: { until: number; seq: number; durable: boolean; after: string | null } | null;
    /** Monotonic, minted for every wake. */
    seq: number;
}

/** The empty run — what `state()` returns before anything is appended. */
export const emptyRun = (): RunState => ({
    def: '',
    version: 0,
    status: 'running',
    cursor: '',
    vars: {},
    startedAt: 0,
    transitions: 0,
    attempt: 0,
    wake: null,
    seq: 0
});

/**
 * Fold one entry into the state. Pure, total, and the ONLY writer: every
 * mutation the engine makes goes through an entry, so a replayed log and a
 * live run cannot diverge. An unknown entry is ignored rather than thrown
 * on — a run written by a newer build must still load on an older one
 * during a rollout, and refusing would strand it.
 */
export function applyRunEntry(state: RunState, entry: unknown): void {
    const e = entry as RunEntry;
    if (!e || typeof e !== 'object' || typeof (e as { t?: unknown }).t !== 'string') return;
    switch (e.t) {
        case 'start':
            state.def = e.def;
            state.version = e.version;
            state.startedAt = e.at;
            state.vars = { ...e.vars };
            state.status = 'running';
            break;
        case 'move':
            state.cursor = e.to;
            state.seq = e.seq;
            state.transitions += 1;
            // A new node has made no attempts yet. Kept here rather than in
            // the engine so a replay reconstructs it identically.
            state.attempt = 0;
            state.wake = null;
            state.status = 'running';
            break;
        case 'vars':
            Object.assign(state.vars, e.set);
            break;
        case 'attempt':
            state.attempt = e.n;
            break;
        case 'sleep':
            state.status = 'sleeping';
            state.seq = e.seq;
            state.wake = { until: e.until, seq: e.seq, durable: e.durable, after: e.after ?? null };
            break;
        case 'wake':
            // Only the fenced wake clears the sleep; a stale one changes
            // nothing, which is the whole point of carrying `seq`.
            if (state.wake && state.wake.seq === e.seq) {
                state.wake = null;
                state.status = 'running';
            }
            break;
        case 'end':
            state.status = e.status;
            state.endedAt = e.at;
            state.wake = null;
            if (e.error !== undefined) state.error = e.error;
            break;
    }
}
