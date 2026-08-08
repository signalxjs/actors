/**
 * The parent↔child IPC protocol for the Tier-2 rig.
 *
 * Deliberately small and explicit: every message is a discriminated union
 * member, so a child that receives something it does not understand fails
 * loudly instead of hanging the parent on a reply that never comes.
 *
 * Two conversations share the channel. The parent DRIVES the child
 * (init/start/drive/stats/stop), and the child calls BACK into the parent
 * for membership and directory operations — the cluster store lives in the
 * parent, so those are RPCs in the opposite direction.
 */
import type { MembershipView } from '@sigx/actors/cluster';

/** Every cluster-store operation a host can ask of the parent. */
export type StoreOp =
    | 'membership.join'
    | 'membership.setStatus'
    | 'membership.leave'
    | 'membership.refresh'
    | 'membership.isAlive'
    | 'directory.lookup'
    | 'directory.claim'
    | 'directory.release'
    | 'directory.evict'
    | 'directory.evictHost';

export interface DriveRequest {
    /** Actor refs to call, round-robined across the loop. */
    targets: readonly { type: string; key: string }[];
    concurrency: number;
    durationMs: number;
    /** Timestamp every op. Costs a `performance.now()` per call, so
     *  throughput and latency are never measured in the same pass. */
    latency: boolean;
}

export interface ChildStats {
    hostId: string;
    /** Connections this host ACCEPTED. Every accept is a peer's open, which
     *  is how outbound sockets get counted without hooking the transport. */
    accepted: number;
    /** Highest simultaneous accepted connections. */
    concurrentPeak: number;
    /** Still open right now. */
    concurrentNow: number;
    /** Bytes read/written across every accepted connection, headers included. */
    bytesIn: number;
    bytesOut: number;
    /** Requests served on the internal host mount. */
    inboundRequests: number;
    /** Store RPCs this child made — the "did we measure the store?" guard. */
    storeOps: number;
    /** Calls this child issued as a driver. */
    callsIssued: number;
    /** libuv TCP handles, from `process.report` — portable across darwin/linux. */
    tcpHandles: number;
    rssBytes: number;
}

// --- parent → child --------------------------------------------------------

/**
 * Which outbound HTTP client the host dispatches with.
 *
 * `default` is what ships today: the global `fetch`, whose undici agent has
 * `connections: null` (unbounded) and `pipelining: 1`. `bounded` and `h2`
 * settle the dispatcher candidates without shipping a dependency — `undici`
 * is a benchmarks-only devDependency for exactly that reason.
 *
 * `tcp` is not a fetch variant at all: it replaces the transport outright
 * with `@sigx/actors-tcp`, which is what makes the default-transport
 * decision a measurement rather than an argument.
 */
export type DispatcherKind = 'default' | 'bounded' | 'h2' | 'tcp';

export type ToChild =
    | {
          t: 'init';
          index: number;
          secret: string | null;
          dispatcher: DispatcherKind;
          /** Per-origin connection cap for `bounded` and `h2`. */
          connections: number;
      }
    | { t: 'start' }
    | { t: 'view'; view: MembershipView }
    | { t: 'selfSuspect' }
    | { t: 'drive'; id: number; request: DriveRequest }
    | { t: 'warm'; id: number; targets: readonly { type: string; key: string }[] }
    | { t: 'stats'; id: number }
    | { t: 'resetStats'; id: number }
    | { t: 'stop'; id: number }
    | { t: 'storeReply'; id: number; ok: true; value: unknown }
    | { t: 'storeReply'; id: number; ok: false; error: string };

// --- child → parent --------------------------------------------------------

export type ToParent =
    | { t: 'ready'; index: number; port: number }
    | { t: 'started'; hostId: string }
    | { t: 'store'; id: number; op: StoreOp; args: readonly unknown[] }
    | { t: 'drove'; id: number; ops: number; opsPerSec: number; percentiles?: unknown }
    | { t: 'warmed'; id: number }
    | { t: 'stats'; id: number; stats: ChildStats }
    | { t: 'reset'; id: number }
    | { t: 'stopped'; id: number }
    | { t: 'failed'; message: string };
