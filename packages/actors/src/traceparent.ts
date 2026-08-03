/**
 * W3C `traceparent` shape check, shared by the two places that accept one
 * from outside: the public endpoint (HTTP header) and the host-to-host
 * envelope (`tp` field). It lives here rather than in either caller because
 * `server/` must not import `cluster/`.
 *
 * Shape only — `00-<32 hex>-<16 hex>-<2 hex>` with any version byte, per the
 * spec's forward-compat rule (a receiver must accept versions it does not
 * know). The runtime never interprets the field: it is unauthenticated
 * observability metadata relayed for tracing middleware, never control flow,
 * so a malformed value costs the trace, never the call.
 */
const TRACEPARENT_RE = /^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/;

/** True iff `value` is a plausibly-valid W3C traceparent header value. */
export function isTraceparent(value: unknown): value is string {
    if (typeof value !== 'string' || !TRACEPARENT_RE.test(value)) return false;
    // Shapes the spec marks INVALID rather than unknown: version ff is
    // reserved, and an all-zero trace-id / parent-id means "no trace".
    if (value.startsWith('ff-')) return false;
    if (value.slice(3, 35) === '00000000000000000000000000000000') return false;
    if (value.slice(36, 52) === '0000000000000000') return false;
    return true;
}
