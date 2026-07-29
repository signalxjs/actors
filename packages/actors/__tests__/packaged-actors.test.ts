/**
 * Actors shipped from an npm package.
 *
 * The runtime already allows it — `withActors([...])` takes definitions from
 * anywhere. What needs proving is the BUILD half: a package cannot rely on
 * `vite/extract.ts`, which only sees first-party source (`node_modules` is
 * excluded), so it performs the client swap itself through its exports map.
 * The fixture in `fixtures/actors-package` is that shape.
 *
 * The security property under test: the browser entry carries typed REFS and
 * no implementation.
 */
import { readFileSync } from 'node:fs';
import { describe, expect, it, vi } from 'vitest';
import { actor, type ActorClientWith } from '@sigx/actors';
import { defineActorApp } from '@sigx/actors/silo';
import { configureActors } from '@sigx/actors/client';
import { handleActorRequest, matchesActorRequest } from '@sigx/actors/server';
import { Greeter, Presence } from './fixtures/actors-package/server';
import { Greeter as GreeterRef, Presence as PresenceRef } from './fixtures/actors-package/client';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };
const ENDPOINT = 'http://packaged.test/_sigx/actor';

describe('a package can ship actors the app registers', () => {
    it('registers several definitions from one package', async () => {
        const app = defineActorApp({ defaults: quiet }).withActors([Greeter, Presence]);
        const silo = await app.start();
        try {
            await expect(silo.actor(Greeter, 'a').greet('world')).resolves.toBe('hello world');
            await expect(silo.actor(Presence, 'a').setOnline(true)).resolves.toBe(true);
            // Two actors, two types, one package.
            expect(silo.stats().perType).toMatchObject({
                'acme/greeter': 1,
                'acme/presence': 1
            });
        } finally {
            await app.stop();
        }
    });

    it('refuses two different actors claiming one type', async () => {
        // `type` is the wire, directory AND storage key, so the loser's
        // callers would silently reach the winner's state. Caught where the
        // rest of registration is validated: when the silo is built.
        const clash = { ...Presence, type: 'acme/greeter' } as typeof Presence;
        const app = defineActorApp({ defaults: quiet }).withActors([Greeter, clash]);
        await expect(app.start()).rejects.toThrow(/registered as "acme\/greeter"/);
    });

    it('allows the same definition to be registered twice', async () => {
        // Re-exported through two barrels is not a conflict.
        const app = defineActorApp({ defaults: quiet }).withActors([Greeter, Greeter]);
        const silo = await app.start();
        try {
            await expect(silo.actor(Greeter, 'x').greet('again')).resolves.toBe('hello again');
        } finally {
            await app.stop();
        }
    });

    it('warns when a packaged actor declares no guard decision', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        // The consuming app's `requireGuards` build gate never sees package
        // source, so this is the only place the omission can be caught.
        const ungated = {
            ...Presence,
            type: 'acme/ungated',
            __sigxActor: { ...Presence.__sigxActor, unguarded: undefined }
        } as unknown as typeof Presence;
        const app = defineActorApp({ defaults: quiet }).withActors([ungated]);
        try {
            await app.start();
            expect(warn).toHaveBeenCalledWith(
                expect.stringContaining('declares neither a `use` guard chain')
            );
        } finally {
            await app.stop();
            warn.mockRestore();
        }
    });
});

describe('the package does its own client swap', () => {
    it('ships refs, not implementations, in the browser entry', () => {
        // Read via a repo-relative path: the DOM-ish test environment does
        // not give `import.meta.url` a file: scheme.
        const client = readFileSync(
            'packages/actors/__tests__/fixtures/actors-package/client.ts',
            'utf8'
        );
        // The ONLY import of ./server is a type import, which is erased —
        // so no method body can reach the browser bundle. Assert that of
        // EVERY import form, not just the named one: a namespace
        // (`import * as server`), default or side-effect import carries the
        // implementation just as surely.
        expect(client).toContain("import type { Greeter as GreeterDef");
        // `[^;]` keeps a match inside ONE statement (imports carry no `;`)
        // while still spanning a multi-line import list.
        const serverImports = client.match(/^import[^;]*['"]\.\/server['"];/gm) ?? [];
        expect(serverImports).toHaveLength(1);
        for (const statement of serverImports) {
            expect(statement.startsWith('import type ')).toBe(true);
        }
        // ...and no runtime form that an import statement scan would miss.
        expect(client).not.toMatch(/\b(?:require|import)\s*\(\s*['"]\.\/server['"]/);
        // ...and none of the implementation is present.
        expect(client).not.toContain('ctx.save()');
        expect(client).not.toContain('hello ${name}');
    });

    it('drives a real call over the wire through the packaged ref', async () => {
        const app = defineActorApp({ defaults: quiet }).withActors([Greeter, Presence]);
        const silo = await app.start();
        configureActors({
            endpoint: ENDPOINT,
            fetch: async (input, init) => {
                const request = new Request(input, init);
                expect(matchesActorRequest(request)).toBe(true);
                return handleActorRequest(request, { silo, origin: false });
            }
        });
        try {
            // `actor()` on the REF takes the wire path — the same expression
            // a consumer writes in the browser.
            const client = actor(GreeterRef, 'wire') as ActorClientWith<typeof Greeter>;
            await expect(client.greet('packaged')).resolves.toBe('hello packaged');
            // ...and the call really landed on the silo's activation.
            await expect(silo.actor(Greeter, 'wire').count()).resolves.toBe(1);
        } finally {
            configureActors(null);
            await app.stop();
        }
    });

    it('keeps the ref and the definition agreed on type and streams', () => {
        const brand = (value: unknown): { __sigxActor?: string } =>
            value as { __sigxActor?: string };
        expect(brand(GreeterRef).__sigxActor).toBe(Greeter.type);
        expect(brand(PresenceRef).__sigxActor).toBe(Presence.type);
        // A ref that forgot a stream name would route it as a plain method.
        const streaming = actor(GreeterRef, 'k') as Record<string, unknown>;
        expect(typeof streaming['watch']).toBe('function');
    });
});
