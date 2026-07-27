/**
 * Static extraction for `*.actor.ts` modules — the build-time half of the
 * actor system. Reads each exported `defineActor({...})`:
 *
 *   - `type`  — must be a string LITERAL (it is the wire/storage identity)
 *   - `streams` — the returned object literal's keys (for the client proxy)
 *   - `use` / `unguarded` — for the `requireGuards` build gate
 *
 * and produces the swapped CLIENT module (`__actorRef` per actor,
 * `__serverOnly` for every other value export). The real module never
 * reaches the browser.
 */
import { parseAst } from 'vite';

// Minimal structural AST typing — enough for the shapes we read.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Node = Record<string, any>;

export interface ExtractedActor {
    exportName: string;
    type: string;
    streams: string[];
    guarded: boolean;
    unguarded: boolean;
    /** Offset of the defineActor call (for error locations). */
    offset: number;
}

export interface ActorExtraction {
    actors: ExtractedActor[];
    /** Non-actor VALUE exports that need `__serverOnly` stand-ins. */
    otherExports: string[];
    errors: { message: string; offset: number }[];
    /** The generated client module (null when there are errors). */
    clientModule: string | null;
}

const CLIENT_HEADER = `import { __actorRef } from '@sigx/actors/client';`;

/** Marker test: is this code our own generated client output? */
export function isGeneratedClientModule(code: string): boolean {
    return code.startsWith(CLIENT_HEADER);
}

/**
 * Cheap pre-filter: can this module define actors at all?
 *
 * `hints` carries extra substrings that imply a `defineActor` import —
 * with `sigxActors({ app })` an actor module imports the APP module
 * (`../actors.app`) and so never mentions `@sigx/actors` at all. Missing
 * one here is not a soft failure: an unextracted actor module is never
 * client-swapped, so its implementation would ship to the browser.
 */
export function mayDefineActors(code: string, hints: readonly string[] = []): boolean {
    if (!code.includes('defineActor')) return false;
    return code.includes('@sigx/actors') || hints.some((hint) => code.includes(hint));
}

export interface ExtractOptions {
    /** Fetch target baked into the emitted client refs. */
    endpoint: string;
    /** The guard build gate. */
    requireGuards: boolean | 'warn';
    /**
     * Does this import specifier also export `defineActor`? Used for the
     * app module under `sigxActors({ app })`; relative specifiers are
     * resolved by the caller, which knows the importer's directory.
     */
    isDefineSource?: (source: string) => boolean;
}

const LANG_BY_EXT: Record<string, 'ts' | 'tsx' | 'js' | 'jsx'> = {
    '.ts': 'ts',
    '.tsx': 'tsx',
    '.js': 'js',
    '.jsx': 'jsx'
};

export function extractActors(
    code: string,
    file: string,
    options: ExtractOptions
): ActorExtraction & { warnings: string[] } {
    const ext = file.slice(file.lastIndexOf('.'));
    const program = parseAst(code, { lang: LANG_BY_EXT[ext] ?? 'tsx' }, file) as unknown as Node;
    const actors: ExtractedActor[] = [];
    const otherExports: string[] = [];
    const errors: { message: string; offset: number }[] = [];
    const warnings: string[] = [];

    /** Local names `defineActor` is imported under. */
    const defineNames = new Set<string>();
    for (const node of program.body as Node[]) {
        if (node.type !== 'ImportDeclaration') continue;
        const source = node.source?.value as string | undefined;
        if (source === undefined) continue;
        // `@sigx/actors` itself, or the app module re-exporting a bound
        // `defineActor`. The caller supplies that predicate because only it
        // can resolve a relative specifier against the importing file.
        if (source !== '@sigx/actors' && !options.isDefineSource?.(source)) continue;
        if (node.importKind === 'type') continue;
        for (const spec of (node.specifiers ?? []) as Node[]) {
            if (
                spec.type === 'ImportSpecifier' &&
                spec.importKind !== 'type' &&
                (spec.imported?.name ?? spec.imported?.value) === 'defineActor'
            ) {
                defineNames.add(spec.local.name as string);
            }
        }
    }

    const readActor = (exportName: string, init: Node): void => {
        const arg = (init.arguments ?? [])[0] as Node | undefined;
        if (!arg || arg.type !== 'ObjectExpression') {
            errors.push({
                message: `export "${exportName}": defineActor needs an inline options object`,
                offset: init.start ?? 0
            });
            return;
        }
        const props = new Map<string, Node>();
        for (const prop of (arg.properties ?? []) as Node[]) {
            if (prop.type !== 'Property' && prop.type !== 'ObjectProperty') continue;
            const key = prop.key?.name ?? prop.key?.value;
            if (typeof key === 'string') props.set(key, prop);
        }
        const typeProp = props.get('type');
        const typeValue = typeProp?.value;
        if (!typeValue || typeValue.type !== 'Literal' || typeof typeValue.value !== 'string') {
            errors.push({
                message:
                    `export "${exportName}": \`type\` must be a string literal — it is the ` +
                    `actor's wire and storage identity and the build reads it statically`,
                offset: (typeProp?.start ?? init.start ?? 0) as number
            });
            return;
        }
        const useProp = props.get('use');
        const guarded =
            useProp !== undefined &&
            !(useProp.value?.type === 'ArrayExpression' && useProp.value.elements.length === 0);
        const unguarded =
            props.get('unguarded')?.value?.type === 'Literal' &&
            props.get('unguarded')!.value.value === true;

        const streams: string[] = [];
        const streamsProp = props.get('streams');
        if (streamsProp) {
            const table = streamTableExpression(streamsProp.value as Node);
            if (!table) {
                errors.push({
                    message:
                        `export "${exportName}": \`streams\` must be a factory returning an ` +
                        `inline object literal, so stream method names are statically readable`,
                    offset: (streamsProp.start ?? 0) as number
                });
                return;
            }
            for (const prop of (table.properties ?? []) as Node[]) {
                const key = prop.key?.name ?? prop.key?.value;
                if (typeof key === 'string') streams.push(key);
            }
        }

        actors.push({
            exportName,
            type: typeValue.value as string,
            streams,
            guarded,
            unguarded,
            offset: (init.start ?? 0) as number
        });
    };

    for (const node of program.body as Node[]) {
        if (node.type !== 'ExportNamedDeclaration') {
            if (node.type === 'ExportDefaultDeclaration') {
                errors.push({
                    message:
                        'default exports are not supported in actor modules — use a named ' +
                        '`export const`',
                    offset: (node.start ?? 0) as number
                });
            }
            continue;
        }
        if (node.exportKind === 'type') continue;
        const decl = node.declaration as Node | undefined;
        if (decl?.type === 'VariableDeclaration') {
            for (const declarator of (decl.declarations ?? []) as Node[]) {
                const name = declarator.id?.name as string | undefined;
                if (!name) continue;
                const init = declarator.init as Node | undefined;
                if (
                    init?.type === 'CallExpression' &&
                    init.callee?.type === 'Identifier' &&
                    defineNames.has(init.callee.name as string)
                ) {
                    readActor(name, init);
                } else {
                    otherExports.push(name);
                }
            }
        } else if (decl?.type === 'FunctionDeclaration' || decl?.type === 'ClassDeclaration') {
            if (decl.id?.name) otherExports.push(decl.id.name as string);
        } else if (!decl) {
            for (const spec of (node.specifiers ?? []) as Node[]) {
                if (spec.exportKind === 'type') continue;
                const exported = spec.exported?.name ?? spec.exported?.value;
                if (typeof exported === 'string') otherExports.push(exported);
            }
        }
    }

    // The requireGuards build gate (rfc-server-v3 §1.4 posture, default ON:
    // actors have no unguarded installed base to migrate).
    if (options.requireGuards !== false) {
        for (const actor of actors) {
            if (actor.guarded || actor.unguarded) continue;
            const message =
                `actor "${actor.type}" declares no \`use:\` guard chain. Add one, or mark it ` +
                `\`unguarded: true\` if it is deliberately public. ` +
                `(requireGuards is on by default; set requireGuards: 'warn' | false on ` +
                `sigxActors() to downgrade.)`;
            if (options.requireGuards === 'warn') warnings.push(message);
            else errors.push({ message, offset: actor.offset });
        }
    }

    let clientModule: string | null = null;
    if (errors.length === 0) {
        const lines = [CLIENT_HEADER];
        if (otherExports.length > 0) {
            lines.push(`import { __serverOnly } from '@sigx/server/client';`);
        }
        for (const actor of actors) {
            lines.push(
                `export const ${actor.exportName} = __actorRef(` +
                    `${JSON.stringify(actor.type)}, ${JSON.stringify(options.endpoint)}` +
                    `${actor.streams.length ? `, ${JSON.stringify(actor.streams)}` : ''});`
            );
        }
        for (const name of otherExports) {
            lines.push(
                `export const ${name} = __serverOnly(${JSON.stringify(name)}, ${JSON.stringify(file)});`
            );
        }
        clientModule = lines.join('\n') + '\n';
    }

    return { actors, otherExports, errors, warnings, clientModule };
}

/** The object literal a `streams:` factory returns, if statically visible. */
function streamTableExpression(value: Node | undefined): Node | null {
    if (!value) return null;
    if (value.type === 'ArrowFunctionExpression' || value.type === 'FunctionExpression') {
        const body = value.body as Node;
        if (body.type === 'ObjectExpression') return body;
        if (body.type === 'BlockStatement') {
            for (const statement of (body.body ?? []) as Node[]) {
                if (statement.type === 'ReturnStatement') {
                    const arg = statement.argument as Node | undefined;
                    return arg?.type === 'ObjectExpression' ? arg : null;
                }
            }
        }
    }
    return null;
}
