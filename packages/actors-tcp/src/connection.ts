/**
 * One multiplexed silo-to-silo connection.
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
import type { Socket } from 'node:net';
import {
    encodeFrame,
    FLAG_STREAM,
    FrameReader,
    FrameType,
    type Frame
} from '@sigx/actors/cluster/frames';
import {
    decodeEnvelope,
    verifyAuth,
    type SiloTransportConfig,
    type SiloTransportRuntime
} from '@sigx/actors/cluster';
import type { ActorCallContext } from '@sigx/actors';

/** Default ceiling on a single frame. Checked before any payload is buffered. */
export const DEFAULT_MAX_FRAME_BYTES = 8 * 1024 * 1024;
/** Chunks a stream consumer will accept before it must extend credit. */
export const DEFAULT_CREDIT = 32;

export interface ConnectionOptions {
    config: SiloTransportConfig;
    socket: Socket;
    /** True on the side that opened the socket — decides corrId parity. */
    dialer: boolean;
    maxFrameBytes: number;
    credit: number;
    /** Present once the silo is running; inbound calls need it. */
    runtime(): SiloTransportRuntime | null;
    /** The peer's id, once the handshake has established it. */
    onPeer(siloId: string, epoch: number): void;
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

export class SiloConnection {
    readonly #o: ConnectionOptions;
    readonly #reader: FrameReader;
    #nextCorr: number;
    #closed = false;
    #peerSiloId = '';
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

        const socket = options.socket;
        socket.setNoDelay(true);
        socket.on('data', (chunk: Buffer) => this.#onData(chunk));
        socket.on('error', () => this.close('socket error'));
        socket.on('close', () => this.close('socket closed'));
        socket.on('drain', () => {
            this.#writable = true;
            for (const wake of this.#drainWaiters) wake();
            this.#drainWaiters.clear();
        });
    }

    get peerSiloId(): string {
        return this.#peerSiloId;
    }

    get closed(): boolean {
        return this.#closed;
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
        const corrId = this.#allocate();
        const signal = call.abortSignal;
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
                if (!this.#unary.delete(corrId)) return;
                this.#send({
                    type: FrameType.CANCEL,
                    flags: 0,
                    status: 0,
                    corrId,
                    payload: { r: 'caller-abort' }
                });
                fail((signal?.reason ?? new Error('aborted')) as never);
            };
            this.#unary.set(corrId, {
                resolve: done as (v: unknown) => void,
                reject: fail as (e: unknown) => void
            });
            signal?.addEventListener('abort', onAbort, { once: true });
            this.#send({
                type: FrameType.CALL,
                flags: 0,
                status: 0,
                corrId,
                payload: { s: symbol, e: envelope, a: args, ...(auth ? { h: auth } : {}) }
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

        const connection = this;
        async function* run(): AsyncGenerator<unknown> {
            try {
                for (;;) {
                    while (queue.length > 0) {
                        const value = queue.shift();
                        taken++;
                        // Top the producer back up once it has spent half
                        // its allowance, so the pipe never fully stalls.
                        if (taken >= connection.#o.credit / 2) {
                            connection.#send({
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
                if (connection.#streams.delete(corrId) && !connection.#closed) {
                    connection.#send({
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

    #send(frame: Frame): void {
        if (this.#closed) return;
        try {
            this.#writable = this.#o.socket.write(encodeFrame(frame));
        } catch {
            this.close('write failed');
        }
    }

    /** Wait for the socket to drain. Connection-level, on top of per-stream
     *  credit: credit counts chunks, and one chunk can be a megabyte. */
    async #awaitWritable(): Promise<void> {
        if (this.#writable || this.#closed) return;
        await new Promise<void>((resolve) => this.#drainWaiters.add(resolve));
    }

    #onData(chunk: Buffer): void {
        this.#reader.push(new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength));
        let frames: Frame[];
        try {
            frames = [...this.#reader.drain()];
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
        for (const frame of frames) void this.#onFrame(frame);
    }

    async #onFrame(frame: Frame): Promise<void> {
        switch (frame.type) {
            case FrameType.HELLO:
            case FrameType.WELCOME: {
                const p = frame.payload as { siloId?: string; epoch?: number };
                if (typeof p?.siloId === 'string') {
                    this.#peerSiloId = p.siloId;
                    this.#o.onPeer(p.siloId, p.epoch ?? 0);
                }
                return;
            }
            case FrameType.PING:
                this.#send({ type: FrameType.PONG, flags: 0, status: 0, corrId: frame.corrId });
                return;
            case FrameType.PONG:
                return;
            case FrameType.GOAWAY:
                this.close(`peer sent GOAWAY ${frame.status}`);
                return;
            case FrameType.CALL:
                await this.#onCall(frame);
                return;
            case FrameType.REPLY: {
                const pending = this.#unary.get(frame.corrId);
                this.#unary.delete(frame.corrId);
                pending?.resolve(frame.payload);
                return;
            }
            case FrameType.ERROR: {
                const wire = frame.payload as { message?: string; status?: number; data?: unknown };
                const error = this.#o.config.fromWireError(
                    frame.status || 500,
                    wire,
                    wire?.message ?? '[sigx actors] silo call failed'
                );
                const unary = this.#unary.get(frame.corrId);
                if (unary) {
                    this.#unary.delete(frame.corrId);
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
            this.#fail(corrId, 503, '[sigx actors] the silo is not started', {
                kind: 'silo-shutdown'
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
            if ((frame.flags & FLAG_STREAM) !== 0) {
                await this.#serveStream(corrId, runtime, ref, target.method, args, call, p?.c ?? this.#o.credit);
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
            throw new Error('[sigx actors] silo call needs a non-empty string key');
        }
        return {
            call: { ...decoded.call },
            ref: { type: target.type, key },
            args: rest
        };
    }

    async #serveStream(
        corrId: number,
        runtime: SiloTransportRuntime,
        ref: { type: string; key: string },
        method: string,
        args: unknown[],
        call: ActorCallContext,
        initialCredit: number
    ): Promise<void> {
        const controller = new AbortController();
        const state: InboundStream = { controller, credit: initialCredit };
        this.#inbound.set(corrId, state);
        const signal = call.abortSignal
            ? AbortSignal.any([call.abortSignal, controller.signal])
            : controller.signal;
        const iterable = runtime.dispatchStream(ref, method, args, { ...call, abortSignal: signal });
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

    close(reason: string): void {
        if (this.#closed) return;
        this.#closed = true;
        // Every in-flight call fails as UNREACHABLE, and is never retried
        // here: the placement already evicts, refreshes and re-resolves, and
        // silently re-sending a non-idempotent actor method to a host that
        // may no longer own the actor is a correctness bug, not a retry.
        const error = Object.assign(
            new Error(`[sigx actors] silo connection lost (${reason})`),
            { __sigxActorError: true as const, kind: 'unreachable' as const }
        );
        for (const pending of this.#unary.values()) pending.reject(error);
        this.#unary.clear();
        for (const stream of this.#streams.values()) stream.fail(error);
        this.#streams.clear();
        for (const corrId of [...this.#inbound.keys()]) void this.#cancelInbound(corrId);
        for (const wake of this.#drainWaiters) wake();
        this.#drainWaiters.clear();
        this.#o.socket.destroy();
        this.#o.onClose();
    }
}
