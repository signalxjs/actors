/**
 * Keepalive on the per-actor stream path (#178).
 *
 * `$live` has pinged since it shipped, because a page's live connection is
 * mostly quiet and every intermediary between a browser and a host reaps an
 * idle response. `actor(X, k).watch()` is the same shape and had nothing: a
 * quiet room's stream is bytes-silent until the next mutation, so ingress
 * (60 s by default), cloud load balancers (~4 min) and mobile NATs all close
 * it, and the client sees "ended without a done/error terminator".
 *
 * The keepalive is a TRANSPORT-level line — `{"ping":1}`, beside
 * `chunk`/`done`/`error` — deliberately not a chunk: a chunk is user data on
 * every stream method, so any sentinel put there could collide with something
 * an actor legitimately yields.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { actor, defineActor } from '@sigx/actors';
import { createHost, type Host } from '@sigx/actors/host';
import { handleActorRequest } from '@sigx/actors/server';
import { __actorRef, configureActors } from '@sigx/actors/client';

const ENDPOINT = 'http://actors.test/_sigx/actor';
const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const Room = defineActor({
    type: 'Room',
    allowAnonymous: true,
    state: () => ({ topic: 'hello' }),
    methods: (ctx) => ({
        async topic() {
            return ctx.state.topic;
        }
    }),
    streams: (ctx) => ({
        /**
         * The canonical live-state body — one seed value, then silence until
         * something mutates. Nothing mutates in this file, which is the whole
         * point: it is the shape that dies at a proxy.
         */
        async *watch() {
            for await (const state of ctx.changes({ initial: true })) yield state.topic;
        },
        /** The opposite shape: never silent long enough to need a keepalive. */
        async *chatter() {
            for (let i = 0; ; i++) {
                yield `${i}`;
                await new Promise((r) => setTimeout(r, 5));
            }
        }
    })
});

const RoomRef = __actorRef('Room', ENDPOINT, ['watch', 'chatter']) as typeof Room;
const running: Host[] = [];

function host(): Host {
    const s = createHost({ actors: [Room], defaults: quiet });
    running.push(s);
    return s;
}

/** Open the stream through the real endpoint and return the raw response. */
async function open(s: Host, streamPingMs?: number, signal?: AbortSignal): Promise<Response> {
    return handleActorRequest(
        new Request(`${ENDPOINT}/${encodeURIComponent('Room#watch')}`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args: ['r1'] }),
            ...(signal ? { signal } : {})
        }),
        { host: s, origin: false, ...(streamPingMs !== undefined ? { streamPingMs } : {}) }
    );
}

/** Collect raw NDJSON lines until `want` of them have arrived (or EOF). */
async function readLines(response: Response, want: number): Promise<string[]> {
    const reader = response.body!.getReader();
    const decoder = new TextDecoder();
    const lines: string[] = [];
    let buffer = '';
    try {
        while (lines.length < want) {
            const { value, done } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            let nl: number;
            while ((nl = buffer.indexOf('\n')) >= 0) {
                const line = buffer.slice(0, nl).trim();
                buffer = buffer.slice(nl + 1);
                if (line) lines.push(line);
            }
        }
    } finally {
        await reader.cancel().catch(() => {});
    }
    return lines;
}

afterEach(async () => {
    configureActors(null);
    for (const s of running.splice(0)) await s.stop({ timeoutMs: 1000 });
});

describe('per-actor stream keepalive', () => {
    it('pings a quiet stream so it survives an idle proxy', async () => {
        const s = host();
        await s.start();

        const response = await open(s, 20);
        // One real value, then nothing an actor will ever send: whatever
        // arrives next can only be the keepalive.
        const lines = await readLines(response, 3);

        expect(lines[0]).toContain('"chunk"');
        expect(lines.slice(1)).toEqual(['{"ping":1}', '{"ping":1}']);
    });

    it('sends no ping when the option is 0', async () => {
        const s = host();
        await s.start();

        const response = await open(s, 0);
        const lines = await Promise.race([
            readLines(response, 2),
            new Promise<string[]>((resolve) => setTimeout(() => resolve(['timed-out']), 80))
        ]);

        // 80 ms of silence: with pinging off the only line is the first value.
        expect(lines).toEqual(['timed-out']);
    });

    it('does not ping a stream that is already producing bytes', async () => {
        // The ping marks SILENCE, not wall clock. A stream sending values
        // faster than the interval is demonstrably alive, so pinging it too
        // would be payload and parse work for nothing.
        //
        // Driven on FAKE timers (#108): under real time, "yields every 5 ms
        // against a 25 ms interval" is only a 5× margin over the scheduler,
        // and one routine stall on a loaded runner produces 25 ms of genuine
        // silence — the keepalive then fires exactly as specified and the
        // assertion, not the behaviour, breaks. Faking `setTimeout` makes
        // "faster than the interval" true by construction: both the actor's
        // 5 ms yields and the keepalive clock live on the same faked heap,
        // which is every timer this path arms (`quiet` disables call
        // deadlines, and the 60 s sweep/reminder intervals are out of reach).
        vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
        try {
            const s = createHost({ actors: [Room], defaults: quiet });
            running.push(s);
            await s.start();

            const response = await handleActorRequest(
                new Request(`${ENDPOINT}/${encodeURIComponent('Room#chatter')}`, {
                    method: 'POST',
                    headers: { 'content-type': 'application/json' },
                    body: JSON.stringify({ args: ['r2'] })
                }),
                { host: s, origin: false, streamPingMs: 25 }
            );
            const reading = readLines(response, 8);
            // Eight lines need 35 fake-ms of 5 ms yields; walk 45 in 1 ms
            // steps so every yield's microtask chain (generator → NDJSON →
            // keepalive wrapper → reader) drains before the clock moves on.
            // A wall-clock keepalive — one that ignored the writes — would
            // fire at 25 and land a ping among these lines.
            for (let tick = 0; tick < 45; tick++) await vi.advanceTimersByTimeAsync(1);
            const lines = await reading;

            expect(lines).toHaveLength(8);
            expect(lines.filter((line) => line.includes('ping'))).toEqual([]);
        } finally {
            vi.useRealTimers();
        }
    });

    it('does not pile up pings behind a consumer that stopped reading', async () => {
        // Backpressure: a stalled consumer leaves the stream unable to accept
        // more, and a keepalive that enqueues anyway grows the internal queue
        // by one line per interval for as long as the stall lasts. It is also
        // pointless — bytes are already pending on the socket, so there is no
        // idle for a ping to interrupt.
        const s = host();
        await s.start();

        const response = await open(s, 10);
        // Deliberately not reading for many intervals, then drain only what is
        // ALREADY buffered — anything the stream produces once reading resumes
        // is a legitimate keepalive, not the backlog under test.
        await new Promise((r) => setTimeout(r, 150));
        const reader = response.body!.getReader();
        const decoder = new TextDecoder();
        let buffered = '';
        try {
            for (;;) {
                const next = await Promise.race([
                    reader.read(),
                    new Promise<null>((r) => setTimeout(() => r(null), 5))
                ]);
                if (!next || next.done) break;
                buffered += decoder.decode(next.value, { stream: true });
            }
        } finally {
            await reader.cancel().catch(() => {});
        }
        const lines = buffered.split('\n').filter(Boolean);
        const pings = lines.filter((line) => line.includes('ping')).length;

        expect(lines[0]).toContain('"chunk"');
        // At most the one ping that fitted the queue — not the ~15 that 150 ms
        // at a 10 ms interval would otherwise have piled up behind the stall.
        expect(pings).toBeLessThanOrEqual(1);
    });

    it('stops arming for a response nobody reads and nobody cancels', async () => {
        // The timer's whole lifetime: it exists only while somebody is waiting
        // for bytes. A tick that cannot write does not re-arm — otherwise an
        // abandoned response (no reader, no cancel, so neither teardown path
        // ever runs) keeps a timer alive for the life of the process. The next
        // `pull` starts the clock again, which is exactly what a consumer that
        // resumed reading produces.
        const s = host();
        await s.start();
        const arming = vi.spyOn(globalThis, 'setTimeout');
        const PING = 13; // distinctive, so only this option's timers are counted

        await open(s, PING);
        await new Promise((resolve) => setTimeout(resolve, 100));

        const armings = arming.mock.calls.filter((call) => call[1] === PING).length;
        arming.mockRestore();
        // The initial arm, and the one the first `pull` makes. Not the eight a
        // self-re-arming tick would have produced over 100 ms.
        expect(armings).toBeLessThanOrEqual(3);
    });

    it('the reader skips pings, so they are invisible to the consumer', async () => {
        const s = host();
        await s.start();
        configureActors({
            endpoint: ENDPOINT,
            fetch: async (input, init) => {
                const request = new Request(input, init);
                const response = await handleActorRequest(request, {
                    host: s,
                    origin: false,
                    streamPingMs: 10
                });
                if (!response.body || !init?.signal) return response;
                // Real fetch cancels the body when its signal aborts; an
                // in-memory handler call has no such link, so the stream
                // would outlive the consumer's `break`.
                const reader = response.body.getReader();
                init.signal.addEventListener('abort', () => void reader.cancel().catch(() => {}));
                return new Response(
                    new ReadableStream<Uint8Array>({
                        async pull(controller) {
                            const { value, done } = await reader.read();
                            if (done) controller.close();
                            else controller.enqueue(value);
                        },
                        cancel: (reason) => reader.cancel(reason)
                    }),
                    { status: response.status, headers: response.headers }
                );
            }
        });

        const seen: unknown[] = [];
        const controller = new AbortController();
        const stream = actor(RoomRef, 'r1').with({ signal: controller.signal }).watch();
        const reading = (async () => {
            for await (const value of stream) seen.push(value);
        })();
        // Long enough for several pings to cross a reader that must ignore
        // them: a ping surfacing as a value would show up here, and a ping
        // mis-parsed as a terminator would end the stream.
        await new Promise((r) => setTimeout(r, 60));
        controller.abort();
        await reading.catch(() => {});

        expect(seen).toEqual(['hello']);
    });
});
