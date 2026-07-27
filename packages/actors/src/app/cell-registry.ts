/**
 * The mounted-read registry — what makes a write refresh the reads it
 * staled.
 *
 * Per app, not page-global: a server rendering two requests concurrently
 * has two of these, and both die with their app. `actorsPlugin()` owns it
 * and hands it out through `ActorsContext`.
 *
 * A read registers a KEY GETTER rather than a key. `useActorState`'s key can
 * be reactive, and a cell registered under the key it happened to have at
 * mount would stop matching the moment that key changed — silently, and in
 * exactly the case (a list whose selection moves) where a stale row is most
 * visible.
 */
import { invalidateRestored } from '@sigx/runtime-core/internals';
import { keyMatches } from './key-match';
import type { ActorKeyArg } from '../actor-key';

/**
 * An exact string key, or a tuple PREFIX. Tuple elements are JSON
 * primitives — the same constraint `actorKey()` puts on its arguments, and
 * for the same reason: anything else either throws in `JSON.stringify`
 * (`bigint`) or silently collapses to `null` (`undefined`, symbols,
 * functions) and would then match keys it has nothing to do with.
 */
export type InvalidationPattern = string | readonly ActorKeyArg[];

export interface ActorCellRegistry {
    /**
     * Register a mounted read. Returns the unregister function — call it
     * from `onUnmounted`, or the registry keeps the component's `refresh`
     * alive after it is gone.
     */
    add(key: () => string | null, refresh: () => void): () => void;
    /**
     * Drop matching entries from the page payload and refresh matching
     * mounted reads. Returns how many reads were refreshed, which is what
     * the tests assert on.
     */
    invalidate(patterns: readonly InvalidationPattern[]): number;
}

interface Cell {
    key: () => string | null;
    refresh: () => void;
}

export function createCellRegistry(): ActorCellRegistry {
    const cells = new Set<Cell>();

    return {
        add(key, refresh) {
            const cell: Cell = { key, refresh };
            cells.add(cell);
            return () => void cells.delete(cell);
        },

        invalidate(patterns) {
            if (patterns.length === 0) return 0;
            let refreshed = 0;
            for (const cell of cells) {
                const key = cell.key();
                if (key === null) continue; // an idle read has no key to stale
                if (!patterns.some((pattern) => keyMatches(key, pattern))) continue;
                // Drop the page-payload entry as well as refetching. Without
                // this a component that unmounts and remounts restores the
                // value SSR put in the document — correct at first paint,
                // wrong ever after a write.
                invalidateRestored(key);
                refreshed++;
                cell.refresh();
            }
            return refreshed;
        }
    };
}
