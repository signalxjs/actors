/**
 * The object-terminated socket's branches real workerd cannot exercise:
 * cold-wake recovery, the attachment deadline, tag scoping, and the named
 * refusals — driven through fakes, which is the reason every socket-facing
 * type in `host.ts` is structural.
 */
import { describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import {
    createHostDurableObject,
    type DurableObjectStateLike,
    type DurableWebSocketLike
} from '../src/index';

const SEP = '\u0000';

const Counter = defineActor({
    type: 'Counter',
    allowAnonymous: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async read() {
            return ctx.state.count;
        }
    })
});

interface FakeSocket extends DurableWebSocketLike {
    sent: string[];
    closedWith: { code?: number; reason?: string } | null;
    attachment: unknown;
}

function fakeSocket(attachment: unknown = undefined): FakeSocket {
    const socket: FakeSocket = {
        sent: [],
        closedWith: null,
        attachment,
        send(message) {
            socket.sent.push(message);
        },
        close(code, reason) {
            socket.closedWith = { code, reason };
        },
        serializeAttachment(value) {
            socket.attachment = value;
        },
        deserializeAttachment() {
            return socket.attachment;
        }
    };
    return socket;
}

function fakeState(options: {
    name?: string;
    tagged?: DurableWebSocketLike[];
    acceptWebSocket?: boolean;
}): DurableObjectStateLike {
    return {
        id: { name: options.name, toString: () => options.name ?? 'anon' },
        storage: {} as DurableObjectStateLike['storage'],
        blockConcurrencyWhile: (fn) => fn(),
        ...(options.acceptWebSocket === false
            ? {}
            : {
                  acceptWebSocket: () => {}
              }),
        getWebSockets: () => options.tagged ?? []
    };
}

function instanceFor(state: DurableObjectStateLike) {
    const HostClass = createHostDurableObject({
        actors: [Counter],
        namespace: () => {
            throw new Error('namespace touched — these paths must not dispatch');
        },
        socket: { origin: false }
    });
    return new HostClass(state, {});
}

/** `Upgrade` is a forbidden header for Node's Request constructor, so the
 *  request is structural — `fetch` reads only `headers` and `url`. */
function upgrade(url: string): Request {
    return { url, headers: new Headers({ upgrade: 'websocket' }) } as unknown as Request;
}

describe('object-terminated socket, faked', () => {
    it('refuses an upgrade for an actor this object does not host, naming both sides', async () => {
        const state = fakeState({ name: `Counter${SEP}right` });
        const res = await instanceFor(state).fetch(upgrade('https://x.test/_sigx/socket/Counter/wrong'));
        expect(res.status).toBe(403);
        const body = (await res.json()) as { error: { message: string } };
        expect(body.error.message).toContain('Counter/wrong');
        expect(body.error.message).toContain('right');
    });

    it('refuses with a named reason when the runtime has no hibernation API', async () => {
        const state = fakeState({ name: `Counter${SEP}k`, acceptWebSocket: false });
        const res = await instanceFor(state).fetch(upgrade('https://x.test/_sigx/socket/Counter/k'));
        expect(res.status).toBe(500);
        const body = (await res.json()) as { error: { message: string } };
        expect(body.error.message).toContain('acceptWebSocket');
    });

    it('closes 1012 on a message after a cold wake', async () => {
        // The socket survived hibernation; the session did not. The client
        // transport treats 1012 like any drop: redial, re-seed.
        const ws = fakeSocket({ v: 1 });
        const state = fakeState({ name: `Counter${SEP}k`, tagged: [ws] });
        await instanceFor(state).webSocketMessage(ws, JSON.stringify({ i: 1, s: 'Counter#read', a: ['k'] }));
        expect(ws.closedWith).toEqual({ code: 1012, reason: 'session evicted — reconnect' });
    });

    it('closes 1003 on a binary frame', async () => {
        const ws = fakeSocket();
        const state = fakeState({ name: `Counter${SEP}k`, tagged: [ws] });
        await instanceFor(state).webSocketMessage(ws, new Uint8Array([1]));
        expect(ws.closedWith?.code).toBe(1003);
    });

    it('enforces the attachment deadline lazily — maxConnectionMs survives eviction', async () => {
        const ws = fakeSocket({ v: 1, deadline: Date.now() - 1_000 });
        const state = fakeState({ name: `Counter${SEP}k`, tagged: [ws] });
        await instanceFor(state).webSocketMessage(ws, '{"p":1}');
        expect(ws.closedWith).toEqual({ code: 1008, reason: 'connection lifetime exceeded' });
    });

    it('leaves a socket it did not accept alone', async () => {
        // A subclass's own socket (a different tag) must pass through
        // untouched — tag scoping is what makes one class own two surfaces.
        const foreign = fakeSocket();
        const state = fakeState({ name: `Counter${SEP}k`, tagged: [] });
        const instance = instanceFor(state);
        await instance.webSocketMessage(foreign, 'anything');
        await instance.webSocketClose(foreign);
        await instance.webSocketError(foreign);
        expect(foreign.closedWith).toBeNull();
    });
});
