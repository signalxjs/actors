let callCounter = 0;

// Per-process random component so ids from different hosts and restarts are
// distinguishable (collisions are possible but vanishingly unlikely) — a
// forwarded call keeps one correlation id cluster-wide. Format opaque.
const PROC = Math.random().toString(36).slice(2, 8);

/** Mint a correlation id — unique within the process, cheap. */
export function mintCallId(): string {
    return `c${(++callCounter).toString(36)}.${PROC}.${Date.now().toString(36)}`;
}
