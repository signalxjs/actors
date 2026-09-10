/**
 * `nodeHmac()` — the synchronous `HostHmac` for a Node host (#440).
 *
 * The cluster's default, `webCryptoHmac`, is correct everywhere and slow on
 * Node for a reason that has nothing to do with the arithmetic:
 * `crypto.subtle.sign` is asynchronous by contract and Node runs it on the
 * libuv threadpool, so every sign and every verify is a thread hop and a
 * promise — measured at ~27 µs per call sequentially, ~6 µs amortized at
 * 64 in flight. `node:crypto`'s `createHmac` computes the same
 * HMAC-SHA-256 on the calling thread in ~2 µs and returns a string, which
 * the envelope's callers branch on instead of awaiting. Both a sign and a
 * verify sit on every secured cross-host call's critical path.
 *
 * The output is byte-identical to the default, so a fleet can carry both
 * at once: `cluster({ secret, hmac: nodeHmac() })` on the hosts that have
 * rolled, the default on the rest.
 */
import { createHmac } from 'node:crypto';
import type { HostHmac } from '../cluster/envelope';

export function nodeHmac(): HostHmac {
    return {
        hex: (secret, message) => createHmac('sha256', secret).update(message).digest('hex')
    };
}
