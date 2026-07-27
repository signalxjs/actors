/**
 * The build-time story: extraction, the client module swap, the
 * requireGuards gate, mid-edit refusal, and the prod registry.
 */
import { describe, expect, it } from 'vitest';
import { extractActors, mayDefineActors, sigxActors } from '@sigx/actors/vite';

const CART = `
import { defineActor } from '@sigx/actors';
import { requireUser } from './guards';

export const CartActor = defineActor({
    type: 'Cart',
    use: [requireUser],
    state: () => ({ items: [] }),
    methods: (ctx) => ({
        async add(item) { ctx.state.items.push(item); await ctx.save(); }
    }),
    streams: (ctx) => ({
        async *watch() { yield* ctx.changes(); },
        async *tail() { yield 1; }
    })
});

export const helper = () => 42;
`;

function opts(requireGuards: boolean | 'warn' = true) {
    return { endpoint: '/_sigx/actor', requireGuards };
}

describe('extractActors', () => {
    it('reads type, streams, and guardedness statically', () => {
        const result = extractActors(CART, 'src/cart.actor.ts', opts());
        expect(result.errors).toEqual([]);
        expect(result.actors).toHaveLength(1);
        expect(result.actors[0]).toMatchObject({
            exportName: 'CartActor',
            type: 'Cart',
            streams: ['watch', 'tail'],
            guarded: true
        });
        expect(result.otherExports).toEqual(['helper']);
    });

    it('emits the swapped client module: __actorRef + __serverOnly, nothing else', () => {
        const result = extractActors(CART, 'src/cart.actor.ts', opts());
        expect(result.clientModule).toContain(
            `export const CartActor = __actorRef("Cart", "/_sigx/actor", ["watch","tail"]);`
        );
        expect(result.clientModule).toContain(
            `export const helper = __serverOnly("helper", "src/cart.actor.ts");`
        );
        // The real implementation must not leak.
        expect(result.clientModule).not.toContain('items.push');
        expect(result.clientModule).not.toContain('requireUser');
    });

    it('requires a literal type', () => {
        const code = `
import { defineActor } from '@sigx/actors';
const name = 'Cart';
export const CartActor = defineActor({ type: name, unguarded: true, state: () => ({}), methods: () => ({}) });
`;
        const result = extractActors(code, 'x.actor.ts', opts());
        expect(result.errors[0]?.message).toMatch(/string literal/);
        expect(result.clientModule).toBeNull();
    });

    it('requireGuards: an unguarded actor without the explicit word is a build error', () => {
        const code = `
import { defineActor } from '@sigx/actors';
export const Open = defineActor({ type: 'Open', state: () => ({}), methods: () => ({}) });
`;
        const strict = extractActors(code, 'x.actor.ts', opts(true));
        expect(strict.errors[0]?.message).toMatch(/no \`use:\` guard chain/);

        const warn = extractActors(code, 'x.actor.ts', opts('warn'));
        expect(warn.errors).toEqual([]);
        expect(warn.warnings[0]).toMatch(/no \`use:\` guard chain/);

        const explicit = extractActors(
            code.replace(`type: 'Open',`, `type: 'Open', unguarded: true,`),
            'x.actor.ts',
            opts(true)
        );
        expect(explicit.errors).toEqual([]);
    });

    it('requires a statically readable streams table', () => {
        const code = `
import { defineActor } from '@sigx/actors';
const table = {};
export const X = defineActor({ type: 'X', unguarded: true, state: () => ({}), methods: () => ({}), streams: () => table });
`;
        const result = extractActors(code, 'x.actor.ts', opts());
        expect(result.errors[0]?.message).toMatch(/inline object literal/);
    });
});

// ---------------------------------------------------------------------------

interface TransformCtx {
    environment?: { name?: string };
    warnings: string[];
    warn(msg: string): void;
    error(msg: string): never;
}

function makeCtx(env: 'client' | 'ssr'): TransformCtx {
    return {
        environment: { name: env },
        warnings: [],
        warn(msg: string) {
            this.warnings.push(msg);
        },
        error(msg: string): never {
            throw new Error(msg);
        }
    };
}

function makePlugin(pluginOptions?: Parameters<typeof sigxActors>[0]) {
    const plugin = sigxActors(pluginOptions) as {
        configResolved?: (config: unknown) => void;
        transform?: (this: TransformCtx, code: string, id: string) => { code: string } | null;
        load?: (this: TransformCtx, id: string) => string | undefined;
        resolveId?: (id: string) => string | undefined;
    };
    // Point the plugin at a root with no actor files; transforms feed code in.
    (plugin.configResolved as (c: unknown) => void)({
        root: '/tmp/sigx-actors-test-root',
        command: 'serve',
        plugins: []
    });
    return plugin;
}

describe('sigxActors() transform', () => {
    it('swaps actor modules in the client environment only', () => {
        const plugin = makePlugin();
        const client = makeCtx('client');
        const swapped = plugin.transform!.call(client, CART, '/app/src/cart.actor.ts');
        expect(swapped?.code).toContain(`__actorRef("Cart"`);
        expect(swapped?.code).not.toContain('items.push');

        const ssr = makeCtx('ssr');
        const kept = plugin.transform!.call(ssr, CART, '/app/src/cart.actor.ts');
        expect(kept).toBeNull(); // real module untouched
    });

    it('never serves a mid-edit broken actor module to the browser', () => {
        const plugin = makePlugin();
        const client = makeCtx('client');
        // Good first pass caches a stub…
        plugin.transform!.call(client, CART, '/app/src/cart.actor.ts');
        // …then a syntax-broken edit arrives: last good stub is served.
        const broken = plugin.transform!.call(client, 'export const {{{', '/app/src/cart.actor.ts');
        expect(broken?.code).toContain(`__actorRef("Cart"`);
        // A file with no last-good stub gets the loud refusal.
        const refusal = plugin.transform!.call(
            client,
            'export const {{{',
            '/app/src/fresh.actor.ts'
        );
        expect(refusal?.code).toMatch(/refusing to serve/);
    });

    it('warns when defineActor appears outside *.actor.ts on a client build', () => {
        const plugin = makePlugin();
        const client = makeCtx('client');
        const result = plugin.transform!.call(
            client,
            `import { defineActor } from '@sigx/actors'; export const X = defineActor({});`,
            '/app/src/rogue.ts'
        );
        expect(result).toBeNull();
        expect(client.warnings[0]).toMatch(/matches none of the actor-module patterns/);
        // The message names the patterns actually in force, not a hard-coded
        // "*.actor.ts" — the defaults include .actor.tsx as well.
        expect(client.warnings[0]).toContain("**/*.actor.tsx");
    });

    it('does NOT warn about EXCLUDED files — a dependency is not yours to move', () => {
        const plugin = makePlugin();
        const client = makeCtx('client');
        // `@sigx/actors`' own bundled dist mentions defineActor and is
        // excluded by `**/dist/**`. Before the fix this warned on every
        // client build of an app that links the package from a workspace,
        // telling the developer to move code they do not own.
        const result = plugin.transform!.call(
            client,
            `import { defineActor } from '@sigx/actors'; export const X = defineActor({});`,
            '/repo/packages/actors/dist/seam-abc123.prod.js'
        );
        expect(result).toBeNull();
        expect(client.warnings).toEqual([]);

        const inNodeModules = plugin.transform!.call(
            client,
            `import { defineActor } from '@sigx/actors'; export const X = defineActor({});`,
            '/app/node_modules/@sigx/actors/dist/index.js'
        );
        expect(inNodeModules).toBeNull();
        expect(client.warnings).toEqual([]);
    });

    it('the requireGuards gate fails the client transform loudly', () => {
        const plugin = makePlugin();
        const client = makeCtx('client');
        const code = `
import { defineActor } from '@sigx/actors';
export const Open = defineActor({ type: 'Open', state: () => ({}), methods: () => ({}) });
`;
        expect(() => plugin.transform!.call(client, code, '/app/src/open.actor.ts')).toThrow(
            /no `use:` guard chain/
        );
    });

    it('serves the prod registry from virtual:sigx-actors', () => {
        const plugin = makePlugin();
        const ssr = makeCtx('ssr');
        plugin.transform!.call(ssr, CART, '/app/src/cart.actor.ts');
        const resolved = plugin.resolveId!('virtual:sigx-actors');
        expect(resolved).toBe('\0virtual:sigx-actors');
        const registry = plugin.load!.call(makeCtx('ssr'), '\0virtual:sigx-actors');
        expect(registry).toContain(`"Cart": () => import(`);
        expect(registry).toContain(`m["CartActor"]`);
    });
});

// ---------------------------------------------------------------------------
// The app-bound `defineActor` (sigxActors({ app })).
//
// This is the security-critical half of M3: an actor module that is NOT
// extracted is never client-swapped, so its implementation — guards, secrets,
// storage calls — would be bundled into the browser build. "Not extracted"
// fails silently, so it is asserted explicitly here.

const APP_BOUND = `
import { defineActor } from '../actors.app';

export const Session = defineActor({
    type: 'Session',
    unguarded: true,
    state: () => ({ hits: 0 }),
    methods: (ctx) => ({
        async touch() { return ++ctx.state.hits; }
    })
});
`;

/** Mirrors the plugin: does the specifier resolve to the app module? */
function appSource(fromDir: string, appPath: string) {
    return (source: string): boolean => {
        if (!source.startsWith('.')) return false;
        const parts = `${fromDir}/${source}`.split('/');
        const out: string[] = [];
        for (const part of parts) {
            if (part === '.' || part === '') continue;
            if (part === '..') out.pop();
            else out.push(part);
        }
        return out.join('/').replace(/\.[cm]?[jt]sx?$/, '') === appPath.replace(/\.[cm]?[jt]sx?$/, '');
    };
}

describe('app-bound defineActor extraction', () => {
    it('extracts an actor whose defineActor comes from the app module', () => {
        const result = extractActors(APP_BOUND, 'src/actors/session.actor.ts', {
            ...opts(),
            isDefineSource: appSource('src/actors', 'src/actors.app.ts')
        });
        expect(result.errors).toEqual([]);
        expect(result.actors.map((a) => a.type)).toEqual(['Session']);
        // ...and it is still swapped for the browser.
        expect(result.clientModule).toContain('__actorRef');
        expect(result.clientModule).not.toContain('ctx.state.hits');
    });

    it('does NOT treat an unrelated relative import as a defineActor source', () => {
        const result = extractActors(APP_BOUND, 'src/actors/session.actor.ts', {
            ...opts(),
            isDefineSource: appSource('src/actors', 'src/somewhere-else.ts')
        });
        // Nothing extracted — the local `defineActor` is some other function.
        expect(result.actors).toEqual([]);
    });

    it('still enforces the guard gate on app-bound actors', () => {
        const unguarded = APP_BOUND.replace('unguarded: true,', '');
        const result = extractActors(unguarded, 'src/actors/session.actor.ts', {
            ...opts(true),
            isDefineSource: appSource('src/actors', 'src/actors.app.ts')
        });
        expect(result.errors.map((e) => e.message).join(' ')).toMatch(/guard|use|unguarded/i);
    });

    it('pre-filters app-module importers only when hinted', () => {
        // The module never mentions '@sigx/actors', so without the hint the
        // cheap filter would skip it and it would never be swapped.
        expect(mayDefineActors(APP_BOUND)).toBe(false);
        expect(mayDefineActors(APP_BOUND, ['actors.app'])).toBe(true);
    });
});

describe('app-module import spellings', () => {
    // Both spellings Vite accepts must be recognized. A miss here is silent
    // and severe: the module is not extracted, so it is never client-swapped
    // and its implementation reaches the browser.
    const ROOT_RELATIVE = APP_BOUND.replace("'../actors.app'", "'/src/actors.app.ts'");

    it('recognizes a root-relative app import', () => {
        const result = extractActors(ROOT_RELATIVE, 'src/actors/session.actor.ts', {
            ...opts(),
            // The plugin's predicate resolves '/x' against the Vite root.
            isDefineSource: (source) =>
                source.replace(/^\//, '').replace(/\.[cm]?[jt]sx?$/, '') === 'src/actors.app'
        });
        expect(result.errors).toEqual([]);
        expect(result.actors.map((a) => a.type)).toEqual(['Session']);
        expect(result.clientModule).toContain('__actorRef');
    });
});
