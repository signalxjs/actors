/**
 * The append path on a REAL Durable Object (#375): `ctx.append` writes one
 * key per entry and never the snapshot, a full save compacts the log back
 * into the snapshot, and a load replays the log through `applyEntry` — the
 * `storage.list({ prefix })` order is the platform's, not a fake's.
 */
import { env, runInDurableObject, SELF } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { durableObjectStorage } from '@sigx/actors-cloudflare';
import { encodeSymbolPath } from '../../../actors/src/wire-url';
import type { Env } from './fixture-worker';

declare module 'cloudflare:test' {
    interface ProvidedEnv extends Env {}
}

const SEP = '\u0000';

async function invoke(symbol: string, args: readonly unknown[]): Promise<unknown> {
    const res = await SELF.fetch(`https://edge.test/_sigx/actor/${encodeSymbolPath(symbol)}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ args })
    });
    const body = (await res.json()) as { data?: unknown; error?: { message?: string } };
    if (!res.ok || body.error) throw new Error(body.error?.message ?? `HTTP ${res.status}`);
    return body.data;
}

const stubFor = (key: string) => env.ACTORS.get(env.ACTORS.idFromName(`Journal${SEP}${key}`));

/** Every storage key of the object, and what `durableObjectStorage` loads for the journal — read from inside it. */
async function stored(key: string): Promise<{ keys: string[]; record: unknown }> {
    return runInDurableObject(stubFor(key), async (_instance, state) => {
        const keys = [...(await state.storage.list()).keys()].filter((k) => k.startsWith('sigx:state'));
        const record = await durableObjectStorage(state.storage).load('Journal', key);
        return { keys, record };
    });
}

const logKeys = (keys: string[]) => keys.filter((k) => k.includes(`${SEP}log${SEP}`));

describe('ctx.append on a real Durable Object', () => {
    it('writes one key per entry beside the snapshot, and the load replays them in order', async () => {
        await invoke('Journal#checkpoint', ['a']);
        for (let i = 1; i <= 12; i++) await invoke('Journal#step', ['a', `s${i}`]);
        const { keys, record } = await stored('a');
        expect(logKeys(keys)).toHaveLength(12);
        expect(record).toMatchObject({ state: { steps: [], checkpoints: 1 }, log: Array.from({ length: 12 }, (_, i) => ({ step: `s${i + 1}` })) });
        await expect(invoke('Journal#read', ['a'])).resolves.toEqual(Array.from({ length: 12 }, (_, i) => `s${i + 1}`));
    });

    it('a full save compacts the log into the snapshot', async () => {
        await invoke('Journal#checkpoint', ['b']);
        await invoke('Journal#step', ['b', 'one']);
        await invoke('Journal#step', ['b', 'two']);
        await invoke('Journal#checkpoint', ['b']);
        const { keys, record } = await stored('b');
        expect(logKeys(keys)).toHaveLength(0);
        expect(record).toMatchObject({ state: { steps: ['one', 'two'], checkpoints: 2 }, log: [] });
    });
});
