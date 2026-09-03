/**
 * One multiplexed host-to-host connection.
 *
 * Both directions of a link share this: the dialer and the acceptor run the
 * same demultiplexer, so a connection carries calls in either direction and
 * the socket count stays at one per peer rather than one per in-flight
 * request. That collapse is the entire reason this package exists — after
 * #96 and #98, per-call HMAC turned out to be worth 1.19× over a real
 * socket, so latency is *not* the argument; file descriptors are.
 *
 * Three things here are easy to get wrong and are called out where they
 * happen: cancelling a stream without closing the socket, applying
 * backpressure at the generator rather than at the buffer, and failing
 * in-flight calls when the connection drops instead of retrying them.
 */
import {
    decodeFrameBody,
    encodeFrame,
    encodeFrameBody,
    FLAG_STREAM,
    FrameReader,
    FrameType,
    type Frame
} from './frames';
import { decodeEnvelope, verifyAuth } from './envelope';
import type { HostCallMode, HostTransportConfig, HostTransportRuntime } from './seam';
import { parseWatchOptions } from './watch-symbol';
import type { ActorCallContext } from '../types';

/**
 * The byte-level link a `HostConnection` runs over — the ONLY thing that
 * differs between the TCP and WebSocket transports.
 *
 * `messageOriented` is the whole distinction: a WebSocket already delivers
 * whole messages with their own length, so frames ride it without the u32
 * prefix; a TCP stream needs the prefix and an incremental reader to find
 * boundaries. Everything above this interface — multiplexing, cancellation,
 * credit, the handshake, error mapping — is identical for both.
 */
export interface FrameLink {
    readonly messageOriented: boolean;
    /** Returns false when the peer is backpressured. */
    send(bytes: Uint8Array): boolean;
    close(): void;
    onData(cb: (bytes: Uint8Array) => void): void;
    onClose(cb: () => void): void;
    onDrain(cb: () => void): void;
}

/** Default ceiling on a single frame. Checked before any payload is buffered. */
export const DEFAULT_MAX_FRAME_BYTES = 8 * 1024 * 1024;
/** Chunks a stream consumer will accept before it must extend credit. */
export const DEFAULT_CREDIT = 32;

export interface ConnectionOptions {
    config: HostTransportConfig;
    link: FrameLink;
    /** True on the side that opened the socket — decides corrId parity. */
    dialer: boolean;
    maxFrameBytes: number;
    credit: number;
    /** Present once the host is running; inbound calls need it. */
    runtime(): HostTransportRuntime | null;
    /** The peer's id, once the handshake has established it. */
    onPeer(hostId: string, epoch: number): void;
    onClose(): void;
}

interface PendingUnary {
    resolve(value: unknown): void;
    reject(error: unknown): void;
}

interface PendingStream {
    push(frame: Frame): void;
    fail(error: unknown): void;
    finish(): void;
}

interface InboundStream {
    controller: AbortController;
    generator?: AsyncGenerator<unknown>;
    /** Chunks the consumer has said it will take. */
    credit: number;
    /** Resolves when credit becomes available again. */
    wake?: () => void;
}

/**
 * The ONE failure the placement may re-dispatch on, and therefore the one
 * this connection may only raise for a call it can PROVE never left this
 * process: nothing was written, so nothing can have run (#353).
 */
function unreachable(reason: string): Error {
    return Object.assign(new Error(`[sigx actors] host connection unavailable (${reason})`), {
        __sigxActorError: true as const,
        kind: 'unreachable' as const
    });
}

export class HostConnection {
    readonly #o: ConnectionOptions;
    readonly #reader: FrameReader;
    #nextCorr: number;
    #closed = false;
    /** Set by `retire()`: no new outbound calls; closes once both ends are done. */
    #retiring: string | null = null;
    /** We told the peer our outbound work on this connection is finished. */
    #sentDone = false;
    /** The peer told us the same (GOAWAY 0). */
    #peerDone = false;
    #peerHostId = '';
    /** Backpressure at the connection level: stop pumping until `drain`. */
    #writable = true;
    readonly #drainWaiters = new Set<() => void>();

    readonly #unary = new Map<number, PendingUnary>();
    readonly #streams = new Map<number, PendingStream>();
    readonly #inbound = new Map<number, InboundStream>();

    constructor(options: ConnectionOptions) {
        this.#o = options;
        this.#reader = new FrameReader(options.maxFrameBytes, options.config.codec.reviver);
        // Split the id space so both ends can open calls on one connection
        // without ever colliding: dialer even, acceptor odd.
        this.#nextCorr = options.dialer ? 2 : 3;

        options.link.onData((bytes) => this.#onData(bytes));
        options.link.onClose(() => this.close('link closed'));
        options.link.onDrain(() => {
            this.#writable = true;
            for (const wake of this.#drainWaiters) wake();
            this.#drainWaiters.clear();
        });
    }

    get peerHostId(): string {
        return this.#peerHostId;
    }

    get closed(): boolean {
        return this.#closed;
    }

    /** Taken out of service by `retire()`: the transport must not route on it. */
    get retiring(): boolean {
        return this.#retiring !== null;
    }

    // -----------------------------------------------------------------------
    // Outbound

    async dispatch(
        symbol: string,
        args: unknown,
        envelope: string,
        auth: string | undefined,
        call: ActorCallContext
    ): Promise<unknown> {
        // Refused BEFORE anything is written, so `unreachable` is honest: the
        // placement re-dispatches on it, and a re-dispatch of a call that
        // was on the wire is the double execution of #353. Closed and
        // retiring both mean "the transport has (or will dial) a replacement".
        if (this.#closed) throw unreachable('closed');
        if (this.#retiring) throw unreachable(this.#retiring);
        const signal = call.abortSignal;
        // A signal that already fired never fires again, so the listener
        // below would never run: the call would go out and could not be
        // cancelled. Refuse it here, with the caller's own reason, unsent.
        if (signal?.aborted) throw signal.reason ?? new Error('aborted');
        const corrId = this.#allocate();
        return await new Promise<unknown>((resolve, reject) => {
            // The listener MUST come off when the call settles. Left attached
            // it leaks on a long-lived signal, and — worse — a later abort
            // would send a CANCEL for a corrId that has already completed and
            // may by then belong to a different call.
            const settle = (fn: (v: never) => void) => (value: never): void => {
                if (signal) signal.removeEventListener('abort', onAbort);
                fn(value);
            };
            const done = settle(resolve as (v: never) => void);
            const fail = settle(reject as (v: never) => void);
            const onAbort = (): void => {
                if (!this.#forgetUnary(corrId)) return;
                this.#send({
                    type: FrameType.CANCEL,
                    flags: 0,
                    status: 0,
                    corrId,
                    payload: { r: 'caller-abort' }
                });
                fail((signal?.reason ?? new Error('aborted')) as never);
            };
            signal?.addEventListener('abort', onAbort, { once: true });
            // Written FIRST, registered second: a write that fails closes the
            // connection, which rejects everything registered as "in flight,
            // outcome unknown" — and this call, provably unsent, is not that.
            const sent = this.#send({
                type: FrameType.CALL,
                flags: 0,
                status: 0,
                corrId,
                payload: { s: symbol, e: envelope, a: args, ...(auth ? { h: auth } : {}) }
            });
            if (!sent) {
                fail(unreachable('frame not written') as never);
                return;
            }
            this.#unary.set(corrId, {
                resolve: done as (v: unknown) => void,
                reject: fail as (e: unknown) => void
            });
        });
    }

    dispatchStream(
        symbol: string,
        args: unknown,
        envelope: string,
        auth: string | undefined,
        call: ActorCallContext
    ): AsyncGenerator<unknown> {
        // Same rule as `dispatch`: refused before the CALL is written, so the
        // placement's re-route (it retries a stream only before its first
        // chunk) never re-issues one that may already be producing.
        const refusal: unknown = this.#closed
            ? unreachable('closed')
            : this.#retiring !== null
              ? unreachable(this.#retiring)
              : call.abortSignal?.aborted
                ? (call.abortSignal.reason ?? new Error('aborted'))
                : undefined;
        const corrId = this.#allocate();
        const queue: unknown[] = [];
        let done = false;
        let failure: unknown;
        let wake: (() => void) | undefined;
        let taken = 0;

        const bump = (): void => {
            const w = wake;
            wake = undefined;
            w?.();
        };

        this.#streams.set(corrId, {
            push: (frame) => {
                queue.push(frame.payload);
                bump();
            },
            fail: (error) => {
                failure = error;
                done = true;
                bump();
            },
            finish: () => {
                done = true;
                bump();
            }
        });

        const sent =
            refusal === undefined &&
            this.#send({
                type: FrameType.CALL,
                flags: FLAG_STREAM,
                status: 0,
                corrId,
                payload: {
                    s: symbol,
                    e: envelope,
                    a: args,
                    c: this.#o.credit,
                    ...(auth ? { h: auth } : {})
                }
            });
        if (!sent) {
            // Never registered as in flight, so `close()` cannot re-classify
            // it: the first pull throws `unreachable`, and nothing was sent.
            this.#streams.delete(corrId);
            failure = refusal ?? unreachable('frame not written');
            done = true;
        }

        // Captured rather than aliasing `this`: `run()` is a free generator,
        // so the instance must not leak into it.
        const send = (frame: Frame): void => void this.#send(frame);
        const forget = (): boolean => this.#forgetStream(corrId);
        const isClosed = (): boolean => this.#closed;
        const window = this.#o.credit;

        // An aborting CALLER must cancel the stream too, not just a consumer
        // that stops pulling — otherwise a deadline blown on this side leaves
        // the producer running on the other.
        const onAbort = (): void => {
            if (forget() && !isClosed()) {
                send({
                    type: FrameType.CANCEL,
                    flags: 0,
                    status: 0,
                    corrId,
                    payload: { r: 'caller-abort' }
                });
            }
            failure ??= call.abortSignal?.reason ?? new Error('aborted');
            done = true;
            bump();
        };
        call.abortSignal?.addEventListener('abort', onAbort, { once: true });

        async function* run(): AsyncGenerator<unknown> {
            try {
                for (;;) {
                    while (queue.length > 0) {
                        const value = queue.shift();
                        taken++;
                        // Top the producer back up once it has spent half
                        // its allowance, so the pipe never fully stalls.
                        if (taken >= window / 2) {
                            send({
                                type: FrameType.CREDIT,
                                flags: 0,
                                status: 0,
                                corrId,
                                payload: { n: taken }
                            });
                            taken = 0;
                        }
                        yield value;
                    }
                    if (failure !== undefined) throw failure;
                    if (done) return;
                    await new Promise<void>((resolve) => {
                        wake = resolve;
                    });
                }
            } finally {
                // THE case a multiplexed transport gets wrong. N streams
                // share this socket, so closing it is not the cancel signal
                // — the peer only learns the consumer gave up if we say so.
                call.abortSignal?.removeEventListener('abort', onAbort);
                if (forget() && !isClosed()) {
                    send({
                        type: FrameType.CANCEL,
                        flags: 0,
                        status: 0,
                        corrId,
                        payload: { r: 'consumer-return' }
                    });
                }
            }
        }
        return run();
    }

    // -----------------------------------------------------------------------
    // Framing

    #allocate(): number {
        const id = this.#nextCorr;
        // Wrap keeping parity; 0 and 1 are reserved for connection frames.
        this.#nextCorr = this.#nextCorr + 2 > 0xffffffff ? (this.#o.dialer ? 2 : 3) : this.#nextCorr + 2;
        return id;
    }

    send(frame: Frame): void {
        this.#send(frame);
    }

    /** True once the link has taken the bytes; false means nothing was written. */
    #send(frame: Frame): boolean {
        if (this.#closed) return false;
        try {
            const bytes = this.#o.link.messageOriented
                ? encodeFrameBody(frame)
                : encodeFrame(frame);
            this.#writable = this.#o.link.send(bytes);
            return true;
        } catch {
            this.close('write failed');
            return false;
        }
    }

    #forgetUnary(corrId: number): PendingUnary | undefined {
        const pending = this.#unary.get(corrId);
        if (pending) {
            this.#unary.delete(corrId);
            this.#settleRetirement();
        }
        return pending;
    }

    #forgetStream(corrId: number): boolean {
        const had = this.#streams.delete(corrId);
        if (had) this.#settleRetirement();
        return had;
    }

    /** Wait for the socket to drain. Connection-level, on top of per-stream
     *  credit: credit counts chunks, and one chunk can be a megabyte. */
    async #awaitWritable(): Promise<void> {
        if (this.#writable || this.#closed) return;
        await new Promise<void>((resolve) => this.#drainWaiters.add(resolve));
    }

    #onData(bytes: Uint8Array): void {
        let frames: Frame[];
        try {
            if (this.#o.link.messageOriented) {
                // One message is exactly one frame; the transport already
                // found the boundary, so there is nothing to reassemble.
                if (bytes.length > this.#o.maxFrameBytes) {
                    throw new Error(
                        `[sigx actors] frame of ${bytes.length} bytes exceeds the ` +
                            `${this.#o.maxFrameBytes}-byte cap`
                    );
                }
                frames = [decodeFrameBody(bytes, this.#o.config.codec.reviver)];
            } else {
                this.#reader.push(bytes);
                frames = [...this.#reader.drain()];
            }
        } catch (error) {
            // Either way the stream is unrecoverable — there is no
            // resynchronising past a frame we could not parse — but the two
            // causes are different and the peer deserves to be told which.
            // 413 means "your frame was too big"; 400 means "your bytes were
            // not a frame", which is a protocol bug rather than a size limit.
            const oversized = (error as { code?: string }).code === 'FRAME_TOO_LARGE';
            this.#send({
                type: FrameType.GOAWAY,
                flags: 0,
                status: oversized ? 413 : 400,
                corrId: 0,
                payload: { m: (error as Error).message }
            });
            this.close(oversized ? 'frame too large' : 'malformed frame');
            return;
        }
        // HELLO and the first CALLs usually share one `data` event. A frame
        // in this batch may close the connection (a GOAWAY, a handshake
        // refusal); the CALLs behind it must then NOT run — their REPLY
        // could never be delivered, and the caller's "outcome unknown" would
        // have been a silent execution.
        for (const frame of frames) {
            if (this.#closed) return;
            void this.#onFrame(frame);
        }
    }

    async #onFrame(frame: Frame): Promise<void> {
        switch (frame.type) {
            case FrameType.HELLO:
            case FrameType.WELCOME: {
                const p = frame.payload as { hostId?: string; epoch?: number };
                if (typeof p?.hostId === 'string') {
                    this.#peerHostId = p.hostId;
                    this.#o.onPeer(p.hostId, p.epoch ?? 0);
                }
                return;
            }
            case FrameType.PING:
                this.#send({ type: FrameType.PONG, flags: 0, status: 0, corrId: frame.corrId });
                return;
            case FrameType.PONG:
                return;
            case FrameType.GOAWAY:
                // Status 0 is HTTP/2's NO_ERROR: the peer has retired this
                // connection and finished its outbound work on it, not an
                // error. Anything else is a refusal, effective at once.
                if (frame.status === 0) {
                    this.#peerDone = true;
                    this.retire('peer retired it');
                    this.#settleRetirement();
                    return;
                }
                this.close(`peer sent GOAWAY ${frame.status}`);
                return;
            case FrameType.CALL:
                await this.#onCall(frame);
                return;
            case FrameType.REPLY:
                this.#forgetUnary(frame.corrId)?.resolve(frame.payload);
                return;
            case FrameType.ERROR: {
                const wire = frame.payload as { message?: string; status?: number; data?: unknown };
                const error = this.#o.config.fromWireError(
                    frame.status || 500,
                    wire,
                    wire?.message ?? '[sigx actors] host call failed'
                );
                const unary = this.#forgetUnary(frame.corrId);
                if (unary) {
                    unary.reject(error);
                    return;
                }
                this.#streams.get(frame.corrId)?.fail(error);
                return;
            }
            case FrameType.CHUNK:
                this.#streams.get(frame.corrId)?.push(frame);
                return;
            case FrameType.END:
                this.#streams.get(frame.corrId)?.finish();
                return;
            case FrameType.CANCEL: {
                await this.#cancelInbound(frame.corrId);
                return;
            }
            case FrameType.CREDIT: {
                const inbound = this.#inbound.get(frame.corrId);
                if (!inbound) return;
                inbound.credit += (frame.payload as { n?: number })?.n ?? 0;
                const wake = inbound.wake;
                inbound.wake = undefined;
                wake?.();
                return;
            }
            default:
                return;
        }
    }

    /**
     * Cancel an inbound stream. BOTH steps are required: an async generator
     * parked at `yield` never runs its `finally` from an abort signal alone,
     * because only `next()`/`return()` resumes it. Aborting alone leaks the
     * activation; returning alone skips any signal-aware cleanup.
     */
    async #cancelInbound(corrId: number): Promise<void> {
        const inbound = this.#inbound.get(corrId);
        if (!inbound) return;
        this.#inbound.delete(corrId);
        inbound.controller.abort();
        inbound.wake?.();
        try {
            await inbound.generator?.return(undefined);
        } catch {
            // The generator's own cleanup threw; nothing useful to do.
        }
    }

    async #onCall(frame: Frame): Promise<void> {
        const runtime = this.#o.runtime();
        const corrId = frame.corrId;
        const p = frame.payload as { s?: string; e?: string; a?: unknown; c?: number; h?: string };
        if (!runtime) {
            this.#fail(corrId, 503, '[sigx actors] the host is not started', {
                kind: 'host-shutdown'
            });
            return;
        }
        try {
            const symbol = p?.s ?? '';
            const auth = await this.#authorize(symbol, p);
            if (!auth) {
                runtime.noteAuthFailure();
                this.#fail(corrId, 403, '[sigx actors] cluster authentication failed');
                return;
            }
            const target = await runtime.resolve(symbol);
            if (!target) {
                this.#fail(corrId, 404, `[sigx actors] no such method "${symbol}"`, {
                    kind: 'method-not-found'
                });
                return;
            }
            const { call, ref, args } = this.#prepare(target, p);
            // Resolving awaited; the connection may have closed meanwhile.
            // A call this connection can no longer answer must not run:
            // the caller was told "outcome unknown", and the one outcome
            // this side can still make true is "it did not execute".
            if (this.#closed) return;
            if ((frame.flags & FLAG_STREAM) !== 0) {
                // The flag says the REPLY is a chunk stream; the symbol says
                // what produced it. A watch is a stream on the wire and
                // neither a `streams:` method nor a unary call in the
                // runtime, which is exactly why the mode has to travel.
                await this.#serveStream(
                    corrId,
                    runtime,
                    ref,
                    target.method,
                    args,
                    call,
                    p?.c ?? this.#o.credit,
                    target.mode,
                    symbol
                );
            } else {
                const value = await runtime.dispatch(ref, target.method, args, call);
                this.#send({
                    type: FrameType.REPLY,
                    flags: 0,
                    status: 0,
                    corrId,
                    payload: this.#o.config.codec.encode(value)
                });
            }
        } catch (error) {
            const wire = this.#o.config.toWireError(error);
            this.#fail(corrId, wire.status ?? 500, wire.message ?? 'error', wire.data);
        }
    }

    async #authorize(symbol: string, p: { e?: string; h?: string }): Promise<boolean> {
        const secret = this.#o.config.secret;
        if (secret === undefined) return true;
        let callId = '';
        try {
            callId = p?.e ? decodeEnvelope(p.e).call.callId : '';
        } catch {
            return false;
        }
        if (callId === '') return false;
        return await verifyAuth(secret, p?.h ?? null, symbol, callId);
    }

    #prepare(
        target: { type: string; method: string },
        p: { e?: string; a?: unknown }
    ): { call: ActorCallContext; ref: { type: string; key: string }; args: unknown[] } {
        const decoded = decodeEnvelope(p?.e ?? '');
        const decodedArgs = this.#o.config.codec.decode(p?.a) as unknown[];
        const [key, ...rest] = decodedArgs;
        if (typeof key !== 'string' || key.length === 0) {
            throw new Error('[sigx actors] host call needs a non-empty string key');
        }
        return {
            call: { ...decoded.call },
            ref: { type: target.type, key },
            args: rest
        };
    }

    async #serveStream(
        corrId: number,
        runtime: HostTransportRuntime,
        ref: { type: string; key: string },
        method: string,
        args: unknown[],
        call: ActorCallContext,
        initialCredit: number,
        mode: HostCallMode,
        /** The WIRE symbol, so a rejection names what the caller actually
         *  sent (`$watch:Counter#read`) rather than the bare method. */
        symbol: string
    ): Promise<void> {
        const controller = new AbortController();
        const state: InboundStream = { controller, credit: initialCredit };
        this.#inbound.set(corrId, state);
        const signal = call.abortSignal
            ? AbortSignal.any([call.abortSignal, controller.signal])
            : controller.signal;
        const scoped = { ...call, abortSignal: signal };
        // A watch leads its args with its options, after the key that
        // `#prepare` already peeled off.
        const iterable =
            mode === 'watch'
                ? runtime.dispatchWatch(
                      ref,
                      method,
                      args.slice(1),
                      scoped,
                      parseWatchOptions(args[0], symbol)
                  )
                : runtime.dispatchStream(ref, method, args, scoped);
        const generator = iterable[Symbol.asyncIterator]() as AsyncGenerator<unknown>;
        state.generator = generator;
        try {
            for (;;) {
                // Credit is checked BEFORE pulling: the generator is never
                // advanced past what the consumer will take, so no
                // producer-side buffer can form in the first place.
                while (state.credit <= 0 && !controller.signal.aborted) {
                    await new Promise<void>((resolve) => {
                        state.wake = resolve;
                    });
                }
                if (controller.signal.aborted) return;
                await this.#awaitWritable();
                const next = await generator.next();
                if (next.done) break;
                state.credit--;
                this.#send({
                    type: FrameType.CHUNK,
                    flags: 0,
                    status: 0,
                    corrId,
                    payload: this.#o.config.codec.encode(next.value)
                });
            }
            if (!controller.signal.aborted) {
                this.#send({ type: FrameType.END, flags: 0, status: 0, corrId });
            }
        } catch (error) {
            if (!controller.signal.aborted) {
                const wire = this.#o.config.toWireError(error);
                this.#fail(corrId, wire.status ?? 500, wire.message ?? 'error', wire.data);
            }
        } finally {
            this.#inbound.delete(corrId);
        }
    }

    #fail(corrId: number, status: number, message: string, data?: unknown): void {
        this.#send({
            type: FrameType.ERROR,
            flags: 0,
            status,
            corrId,
            payload: { message, status, ...(data !== undefined ? { data } : {}) }
        });
    }

    /**
     * Take this connection out of service without cutting what is on it —
     * the simultaneous-dial loser's exit (#353). New outbound calls are
     * refused as `unreachable` (provably unsent; the transport routes them
     * over the surviving link), the calls already on the wire run to their
     * answers, and the socket closes only once BOTH ends have said they are
     * finished: each sends GOAWAY 0 when its own outbound work has drained,
     * and closes when it has both sent and received one. A CALL the peer
     * wrote before it learned of the retirement is ordered before its
     * GOAWAY, so it is always answered. A peer that never says it is done
     * holds the socket until its calls settle or the transport stops —
     * which is what the calls' own deadlines already bound.
     */
    retire(reason: string): void {
        if (this.#closed || this.#retiring !== null) return;
        this.#retiring = reason;
        this.#settleRetirement();
    }

    #settleRetirement(): void {
        if (this.#closed || this.#retiring === null) return;
        if (!this.#sentDone && this.#unary.size === 0 && this.#streams.size === 0) {
            this.#sentDone = true;
            this.#send({ type: FrameType.GOAWAY, flags: 0, status: 0, corrId: 0 });
        }
        if (this.#sentDone && this.#peerDone) this.close(`retired: ${this.#retiring}`);
    }

    /**
     * The retry contract, stated once: `unreachable` means PROVABLY NOT
     * EXECUTED, and the placement re-dispatches on it. A unary call whose
     * CALL frame was written is not that — the peer may have run it and the
     * REPLY died with the socket — so it fails with an ordinary error the
     * placement does not classify, and the caller decides. Re-sending a
     * non-idempotent actor method on a guess is the double execution of
     * #353, not a retry. Streams keep `unreachable`: the placement re-issues
     * a stream only before its first chunk, and a watch is a read.
     */
    close(reason: string): void {
        if (this.#closed) return;
        this.#closed = true;
        const lost = new Error(
            `[sigx actors] host connection closed with this call in flight (${reason}): ` +
                `outcome unknown, not retried`
        );
        for (const pending of this.#unary.values()) pending.reject(lost);
        this.#unary.clear();
        const error = unreachable(reason);
        for (const stream of this.#streams.values()) stream.fail(error);
        this.#streams.clear();
        // Snapshot first: #cancelInbound deletes from #inbound, so iterating
        // the live map would mutate it mid-iteration.
        const inFlight = Array.from(this.#inbound.keys());
        for (const corrId of inFlight) void this.#cancelInbound(corrId);
        for (const wake of this.#drainWaiters) wake();
        this.#drainWaiters.clear();
        this.#o.link.close();
        this.#o.onClose();
    }
}
