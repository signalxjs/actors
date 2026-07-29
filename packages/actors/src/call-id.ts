let callCounter = 0;

// Per-process random component so ids from different hosts and restarts are
// distinguishable (collisions are possible but vanishingly unlikely) — a
// forwarded call keeps one correlation id cluster-wide. Format opaque.
//
// Drawn LAZILY: workerd forbids generating random values at module scope, the
// same rule the lazy AbortController in `context.ts` works around. Seeding at
// import is either refused outright or served from a deterministic startup
// seed — and in that case every isolate of one bundle shares a seed while its
// `callCounter` restarts at zero, so two Durable Objects mint identical ids
// and the id stops correlating anything.
let proc: string | undefined;

/** Mint a correlation id — unique within the process, cheap. */
export function mintCallId(): string {
    proc ??= Math.random().toString(36).slice(2, 8);
    return `c${(++callCounter).toString(36)}.${proc}.${Date.now().toString(36)}`;
}
