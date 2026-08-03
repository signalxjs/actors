/**
 * W3C `traceparent` codec — ~20 lines, kept here rather than depending on
 * `@opentelemetry/core` for its propagator: the format is four dash-joined
 * hex fields, and this package's only need is envelope-field <-> span-context
 * conversion.
 *
 * Deliberately independent of `@opentelemetry/api` types (plain shapes) so
 * importing it never drags the peer in.
 */

export interface TraceparentFields {
    traceId: string;
    spanId: string;
    /** The flags byte, numeric — bit 0 is `sampled`. */
    traceFlags: number;
}

const TRACEPARENT_RE = /^[0-9a-f]{2}-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/;

/** Parse a traceparent header value; null for anything invalid. */
export function parseTraceparent(header: string): TraceparentFields | null {
    const match = TRACEPARENT_RE.exec(header);
    if (!match) return null;
    const [, traceId, spanId, flags] = match as unknown as [string, string, string, string];
    // All-zero ids are the spec's "no trace" — invalid, not a parent.
    if (traceId === '00000000000000000000000000000000') return null;
    if (spanId === '0000000000000000') return null;
    if (header.startsWith('ff-')) return null;
    return { traceId, spanId, traceFlags: parseInt(flags, 16) };
}

/** Format span-context fields as a version-00 traceparent header value. */
export function formatTraceparent(fields: TraceparentFields): string {
    const flags = (fields.traceFlags & 0xff).toString(16).padStart(2, '0');
    return `00-${fields.traceId}-${fields.spanId}-${flags}`;
}
