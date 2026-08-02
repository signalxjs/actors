/**
 * Runtime-reserved methods (#240) — the public mount must not dispatch them.
 *
 * `$sigx:reminder` (and every future `$sigx:*` delivery) is invoked by the
 * runtime itself: the reminder service delivers it, and remote hosts reach it
 * over the HMAC-authenticated internal mount. The PUBLIC resolver synthesizes
 * a wrapper for whatever method name it is handed, so without a refusal an
 * unauthenticated caller can invoke `onReminder` with a name of its choosing —
 * a reminder firing the reminder table never scheduled.
 *
 * The refusal must be indistinguishable from a method that does not exist:
 * same status, same kind, same message shape. Anything else confirms the
 * runtime's internals to whoever is probing.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor, type Host } from '@sigx/actors';
import { createHost } from '@sigx/actors/host';
import { handleActorRequest } from '@sigx/actors/server';
import { resolveHostSymbol } from '@sigx/actors/cluster';

const ENDPOINT = 'http://actors.test/_sigx/actor';
const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

let reminderRuns = 0;

/** Unguarded on purpose: the refusal must hold with no guard in the way. */
const Inbox = defineActor({
    type: 'Inbox',
    unguarded: true,
    state: () => ({ notes: [] as string[] }),
    methods: (ctx) => ({
        async count() {
            return ctx.state.notes.length;
        }
    }),
    onReminder: (_ctx, name) => {
        reminderRuns += 1;
        void name;
    }
});

const running: Host[] = [];

async function startHost(): Promise<Host> {
    const host = createHost({ actors: [Inbox], defaults: quiet });
    await host.start();
    running.push(host);
    return host;
}

function post(host: Host, symbol: string, args: readonly unknown[]): Promise<Response> {
    return handleActorRequest(
        new Request(`${ENDPOINT}/${encodeURIComponent(symbol)}`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args })
        }),
        { host, origin: false }
    );
}

afterEach(async () => {
    reminderRuns = 0;
    for (const host of running.splice(0)) await host.stop({ timeoutMs: 1000 });
});

describe('reserved methods on the public mount', () => {
    it('refuses $sigx:reminder without running the handler', async () => {
        const host = await startHost();
        const response = await post(host, 'Inbox#$sigx:reminder', ['user-1', 'cleanup']);
        expect(response.status).toBe(404);
        expect(reminderRuns).toBe(0);
    });

    it('is indistinguishable from a method that does not exist', async () => {
        const host = await startHost();
        const reserved = await post(host, 'Inbox#$sigx:reminder', ['user-1', 'x']);
        const missing = await post(host, 'Inbox#definitelyNot', ['user-1']);
        expect(reserved.status).toBe(missing.status);
        const reservedBody = (await reserved.json()) as { error: { kind: string; message: string } };
        const missingBody = (await missing.json()) as { error: { kind: string; message: string } };
        expect(reservedBody.error.kind).toBe(missingBody.error.kind);
        // Same wording, differing only in the method name it names.
        expect(reservedBody.error.message.replace('$sigx:reminder', 'definitelyNot')).toBe(
            missingBody.error.message
        );
    });

    it('refuses every $sigx:-prefixed name, not just the ones that exist today', async () => {
        const host = await startHost();
        const response = await post(host, 'Inbox#$sigx:topic', ['user-1', { forged: true }]);
        expect(response.status).toBe(404);
    });

    it('still answers ordinary methods', async () => {
        const host = await startHost();
        const response = await post(host, 'Inbox#count', ['user-1']);
        expect(response.status).toBe(200);
        expect(((await response.json()) as { data: number }).data).toBe(0);
    });

    it('keeps resolving reserved symbols on the internal mount', async () => {
        const host = await startHost();
        // Remote reminder deliveries travel as `Type#$sigx:reminder` over the
        // HMAC-authenticated host-to-host mount — the refusal is the public
        // resolver's, never the shared symbol rule's.
        const target = await resolveHostSymbol(host, 'Inbox#$sigx:reminder');
        expect(target).toEqual({ type: 'Inbox', method: '$sigx:reminder', mode: 'unary' });
    });
});
