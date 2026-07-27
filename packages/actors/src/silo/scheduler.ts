/**
 * The clock seam for BACKGROUND work — the idle sweeper, the reminder tick,
 * `ctx.timer`, the write-behind flush — which goes through an
 * `ActorScheduler` instead of reaching for `setInterval` directly.
 *
 * Call deadlines and the shutdown drain stay on host timers on purpose:
 * they are scoped to an in-flight request or stop, so they run wherever
 * that does.
 *
 * Two reasons, and the second is the load-bearing one:
 *
 * 1. Tests can drive time exactly, instead of the suite-wide
 *    `{ sweepIntervalMs: 60_000, reminderTickMs: 60_000 }` trick that just
 *    pushes background work beyond the end of the test.
 * 2. Some runtimes have no background execution at all. A Cloudflare Worker
 *    only runs while it is handling a request, so a `setInterval` registered
 *    at startup simply never fires; a Durable Object drives the same work
 *    from alarms. That is an architectural difference, not something a
 *    polyfill can paper over — so it is a seam.
 */
import type { ActorScheduler } from '../types';

/**
 * The default: `setTimeout`/`setInterval` from the host, `unref`'d where
 * that exists so a silo never holds a Node process open on its own.
 *
 * WinterCG-clean — both globals are standard, and `unref` is
 * optional-chained. It is the right choice anywhere with a long-lived
 * process (Node, Deno, Bun); it is the WRONG choice on Workers, which is
 * what the seam is for.
 */
export function timerScheduler(): ActorScheduler {
    return {
        every(intervalMs: number, tick: () => void): () => void {
            const handle = setInterval(tick, intervalMs);
            (handle as { unref?: () => void }).unref?.();
            return () => clearInterval(handle);
        },
        after(delayMs: number, run: () => void): () => void {
            const handle = setTimeout(run, delayMs);
            (handle as { unref?: () => void }).unref?.();
            return () => clearTimeout(handle);
        }
    };
}

interface ManualEntry {
    due: number;
    /** null = one-shot. */
    intervalMs: number | null;
    run: () => void;
}

export interface ManualScheduler extends ActorScheduler {
    /** Fire everything due within the next `ms`, in due order. */
    advance(ms: number): void;
    /** Current virtual time, ms since this scheduler was created. */
    readonly now: number;
    /** Jobs still registered — a leak check for tests. */
    readonly pending: number;
}

/**
 * A scheduler driven by hand. `advance(ms)` fires due jobs in order,
 * re-arming repeats as it goes, so a test can say "an hour passes" without
 * waiting or guessing.
 */
export function manualScheduler(): ManualScheduler {
    let now = 0;
    const entries = new Set<ManualEntry>();

    const add = (delayMs: number, intervalMs: number | null, run: () => void): (() => void) => {
        const entry: ManualEntry = { due: now + Math.max(0, delayMs), intervalMs, run };
        entries.add(entry);
        return () => entries.delete(entry);
    };

    return {
        every: (intervalMs, tick) => add(intervalMs, Math.max(1, intervalMs), tick),
        after: (delayMs, run) => add(delayMs, null, run),
        advance(ms: number): void {
            if (ms < 0) {
                // Rewinding would leave already-due jobs "in the future" and
                // silently skip them. In a test helper that is a bug in the
                // test, so say so rather than absorb it.
                throw new Error(
                    `[sigx actors] manualScheduler.advance() cannot go backwards (got ${ms}).`
                );
            }
            const target = now + ms;
            // Re-scan each round: a job may register more work when it runs.
            for (;;) {
                let next: ManualEntry | null = null;
                for (const entry of entries) {
                    if (entry.due <= target && (next === null || entry.due < next.due)) {
                        next = entry;
                    }
                }
                if (!next) break;
                now = next.due;
                if (next.intervalMs === null) entries.delete(next);
                else next.due = now + next.intervalMs;
                next.run();
            }
            now = target;
        },
        get now(): number {
            return now;
        },
        get pending(): number {
            return entries.size;
        }
    };
}
