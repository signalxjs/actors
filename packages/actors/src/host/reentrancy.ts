/**
 * Reentrancy-mode resolution and the async-context loader behind
 * `reentrant: 'always'` / `methodReentrancy` (full interleaving).
 *
 * Interleaved turns need a per-turn call context: the activation's single
 * `#currentCall` field is wrong by construction once two turns overlap, so
 * an interleaving activation carries an `AsyncLocalStorage` instead. The
 * import is lazy and only ever paid by definitions that DECLARE
 * interleaving — the default (serial) path never touches `node:async_hooks`.
 */

/** The options subset this module reads (see `ActorOptions`). */
export interface ReentrancyOptions {
    readonly reentrant?: boolean | 'call-chain' | 'always';
    readonly methodReentrancy?: Record<string, 'always'>;
}

export type ReentrancyMode = 'none' | 'call-chain' | 'always';

/**
 * The mode governing ONE method: the per-method map wins (own keys only —
 * same rule as `methodUse`), then the actor-level setting. `true` is the
 * v1 spelling of `'call-chain'`.
 */
export function effectiveReentrancy(opts: ReentrancyOptions, method: string): ReentrancyMode {
    const map = opts.methodReentrancy;
    if (map && Object.hasOwn(map, method)) return 'always';
    const r = opts.reentrant;
    return r === 'always' ? 'always' : r === true || r === 'call-chain' ? 'call-chain' : 'none';
}

/** Does this definition ever interleave (and therefore need the store)? */
export function declaresInterleaving(opts: ReentrancyOptions): boolean {
    if (opts.reentrant === 'always') return true;
    const map = opts.methodReentrancy;
    return map !== undefined && Object.keys(map).length > 0;
}

/**
 * Check the `reentrant` / `methodReentrancy` declarations. Runs at
 * ACTIVATION rather than definition time — the root entry's size gate
 * pushed it here, and the first activation of the type still fails loudly
 * (as `ActorActivationError`) in every build, before any turn runs.
 * `reentrant` was a boolean in v1, so an unknown string would otherwise
 * silently read as "not reentrant"; the per-method map is a dispatch
 * commitment (an in-chain call to a mapped method is admitted instead of
 * deadlocking), so a malformed entry must not wait for the first cycle to
 * surface.
 *
 * A map key cannot be checked against real method names even here-adjacent
 * at define time (the `methods` factory needs a live ctx) — the activation
 * dev-warns on a key its built method table does not have.
 */
export function validateReentrancy(
    type: string,
    opts: ReentrancyOptions,
    streamNames: readonly string[]
): void {
    const at = `[sigx actors] actor "${type}"`;
    const r = opts.reentrant as unknown;
    if (r !== undefined && typeof r !== 'boolean' && r !== 'call-chain' && r !== 'always') {
        throw new Error(`${at} \`reentrant\` must be false, true/'call-chain' or 'always'.`);
    }
    const map = opts.methodReentrancy as unknown;
    if (map === undefined) return;
    if (typeof map !== 'object' || map === null || Array.isArray(map)) {
        throw new Error(
            `${at} \`methodReentrancy\` must be an object mapping method names to 'always'.`
        );
    }
    // PRESENCE is the contradiction, empty map included — a present-but-
    // empty map on an 'always' actor is a declaration that says nothing
    // and usually means a half-finished edit.
    if (r === 'always') {
        throw new Error(
            `${at}: \`reentrant: 'always'\` makes \`methodReentrancy\` redundant — pick one.`
        );
    }
    for (const method of Object.keys(map)) {
        const where = `${at} methodReentrancy "${method}"`;
        if ((map as Record<string, unknown>)[method] !== 'always') {
            throw new Error(`${where} must map to 'always'.`);
        }
        if (streamNames.includes(method)) {
            // Stream iteration already runs detached from the mailbox —
            // there is no turn exclusivity to exempt it from.
            throw new Error(`${where} is a \`streams:\` method — streams take no turn.`);
        }
        if (method.startsWith('$')) {
            // The runtime's own deliveries ($sigx:reminder, $sigx:topic)
            // follow the actor-level setting only.
            throw new Error(`${where} is a reserved name — map \`methods:\` entries only.`);
        }
    }
}

/**
 * The slice of `AsyncLocalStorage` the activation uses — structural, so the
 * host entry does not depend on Node typings.
 */
export interface CallStore<T> {
    getStore(): T | undefined;
    run<R>(store: T, callback: () => R): R;
}

export type CallStoreCtor = new <T>() => CallStore<T>;

let alsCtor: CallStoreCtor | null | undefined;

/**
 * Load `AsyncLocalStorage`, once. Node, Deno, Bun and workerd (under the
 * `nodejs_compat`/`nodejs_als` flag) all provide it; anywhere else the
 * first activation of an interleaving type fails loudly instead of running
 * with corrupted call chains.
 */
export async function loadCallStore(): Promise<CallStoreCtor> {
    if (alsCtor === undefined) {
        try {
            // The specifier rides a variable so this file typechecks and
            // bundles without Node typings — the host entry is WinterCG,
            // and only an interleaving definition ever reaches this line.
            const specifier = 'node:async_hooks';
            const mod = (await import(/* @vite-ignore */ specifier)) as {
                AsyncLocalStorage?: CallStoreCtor;
            };
            alsCtor = mod.AsyncLocalStorage ?? null;
        } catch {
            alsCtor = null;
        }
    }
    if (!alsCtor) {
        throw new Error(
            `[sigx actors] reentrant interleaving needs AsyncLocalStorage (node:async_hooks), ` +
                `which this runtime does not provide. On Cloudflare Workers enable the ` +
                `\`nodejs_compat\` (or \`nodejs_als\`) compatibility flag.`
        );
    }
    return alsCtor;
}
