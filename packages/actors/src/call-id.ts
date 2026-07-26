let callCounter = 0;

/** Mint a correlation id — unique within the process, cheap. */
export function mintCallId(): string {
    return `c${(++callCounter).toString(36)}.${Date.now().toString(36)}`;
}
