import { env, runInDurableObject } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { encodeEnvelope, SILO_CALL_HEADER } from '@sigx/actors/cluster';
import type { Env } from './fixture-worker';

declare module 'cloudflare:test' {
    interface ProvidedEnv extends Env {}
}

const SEP = '\u0000';

/**
 * Does aborting a Durable Object stub fetch reach the object?
 *
 * This is the assumption the whole Cloudflare disconnect chain rests on, and
 * it goes straight to the object's internal mount — no Worker, no placement,
 * no public endpoint — so nothing else can be blamed.
 */
// SKIPPED because it FAILS and documents why: the abort never reaches the
// object. Kept as the evidence behind #187 rather than deleted — it is the
// narrowest reproduction there is, and the first thing to re-run against any
// proposed fix (or against a future workerd that closes the gap).
describe.skip('stub.fetch honours its signal', () => {
    it('releases the activation when the caller aborts', async () => {
        const key = 'stubsig';
        const name = `Counter${SEP}${key}`;
        const stub = env.ACTORS.get(env.ACTORS.idFromName(name));

        const ac = new AbortController();
        const res = await stub.fetch(
            `https://sigx.invalid/_sigx/do/${encodeURIComponent('Counter#watch')}`,
            {
                method: 'POST',
                headers: {
                    'content-type': 'application/json',
                    [SILO_CALL_HEADER]: encodeEnvelope(
                        { callChain: [], callId: 'c1.sig.0' },
                        'probe'
                    )
                },
                body: JSON.stringify({ args: [key] }),
                signal: ac.signal
            }
        );
        expect(res.ok).toBe(true);
        const reader = res.body!.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        while (!buffer.includes('\n')) {
            const { value, done } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
        }

        const held = async (): Promise<boolean | undefined> =>
            runInDurableObject(stub, async (instance) => {
                const silo = await (
                    instance as { silo(): Promise<import('@sigx/actors').Silo> }
                ).silo();
                return silo.activations().find((a) => a.key === key)?.keptAlive;
            });

        console.log('STUBSIG before abort keptAlive=', await held());
        ac.abort();
        let seen: boolean | undefined;
        for (let i = 0; i < 40; i++) {
            seen = await held();
            if (seen === false) break;
            await scheduler.wait(50);
        }
        console.log('STUBSIG after abort  keptAlive=', seen);
        expect(seen).toBe(false);
    });
});
