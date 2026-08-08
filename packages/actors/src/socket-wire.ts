/**
 * `@sigx/actors/socket-wire` — the CLIENT socket vocabulary: what a browser
 * (or any untrusted caller) speaks to `createActorSocketSession` over a
 * WebSocket or any other message-oriented link.
 *
 * It is a published subpath for the same reason `./cluster/frames` is — an
 * out-of-repo adapter must be able to reach the vocabulary and the codec
 * without copying either (#99).
 *
 * ## This is NOT the cluster wire, on purpose
 *
 * `./cluster/frames` is built for a peer inside the deployment perimeter:
 * its envelope carries the encoded principal and the request bag, it
 * resolves against the internal mount's guard-free symbol table, and corrId
 * parity exists so both ends originate calls. None of that may face a
 * browser. This vocabulary has no field for an identity, no envelope and no
 * inbound-call direction: **a client cannot forge a principal because the
 * protocol has no place to put one** — the security argument expressed as a
 * shape, which is the form that cannot rot. Identity is pinned server-side
 * at upgrade; authorization runs per message.
 *
 * ## Shape notes
 *
 * - Text JSON, not binary. The codec stringifies anyway, so binary buys
 *   nothing here (unlike host-to-host, where it avoids a UTF-8 round trip)
 *   and costs DevTools readability.
 * - `{i,v}` / `{i,e}` are deliberately the `LiveFrame` shapes of the `$live`
 *   endpoint — a live value over the socket is byte-identical to one over
 *   `$live`; only the meaning of `i` shifts from "index in the sent array"
 *   to "id the client chose".
 * - `{p:1}` is the same keepalive both directions; a quiet connection must
 *   move bytes or a proxy reaps it (see `DEFAULT_LIVE_PING_MS`).
 * - Cancelling one call must not close the socket — with many calls
 *   multiplexed on one connection, closing it is not the cancel signal.
 */
import type { LiveSubscription, WireError } from './wire-shared';

// The codec and the branded-error helper, so an adapter or client package
// encodes arguments and revives values exactly as `fetchTransport` does —
// one `serverPlugin({ types })` registration covers every wire. `parseWire`
// is the pollution-safe parse; raw `JSON.parse` on socket input is how a
// prototype-pollution payload gets revived.
export { parseWire } from './wire-parse';
export { encodeWire, reviveWire, wireFail } from './wire-shared';
export type { LiveSubscription, WireError };

/**
 * Client → session. `i` is a client-chosen id, unique among that client's
 * in-flight calls (and, separately, among its open subscriptions — the two
 * namespaces do not collide because a message's kind picks the table).
 */
export type SocketRequest =
    /** Call `s` (`Type#method`) with `a`; `k: 1` = stream, `w: 1` = one-way. */
    | { i: number; s: string; a: unknown[]; k?: 1; w?: 1 }
    /** Cancel call `i`. Cancels that call only — never the connection. */
    | { i: number; c: 1 }
    /** Open live subscription `i`. */
    | { i: number; sub: LiveSubscription }
    /** Close live subscription `i`. */
    | { i: number; uns: 1 }
    /** Keepalive. Carries nothing; answered with a `{p:1}` reply. */
    | { p: 1 };

/**
 * Session → client. For a unary call: one `{i,v}` then `{i,d}`. For a
 * stream: any number of `{i,v}` then `{i,d}` or `{i,e}`. For a
 * subscription: `{i,v}` per value until `{i,uns}` or a terminal `{i,e}`.
 * A one-way call (`w: 1`) is answered by nothing at all.
 */
export type SocketReply =
    /** A unary result, a stream chunk, or a live value for `i`. */
    | { i: number; v: unknown }
    /** Done — call `i` completed. */
    | { i: number; d: 1 }
    /** Failed — call or subscription `i` is over; classified and masked. */
    | { i: number; e: WireError }
    /** Keepalive. */
    | { p: 1 };
