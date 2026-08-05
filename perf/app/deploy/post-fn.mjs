/**
 * The `postMessage` serverFn id, read from the BUILD rather than pasted.
 *
 * The id is `postMessage_fn_<hash>`, and the hash moves whenever the build
 * decides it should. `edge-ladder.mjs` used to carry one literally, and by
 * the time anyone looked it had rotted from `…_6c5508cb` to `…_2b42ef63` —
 * silently, because a wrong id 404s and a 404 is CHEAPER than a real write.
 * `infra/write-mix` therefore reported the breakage as extra throughput.
 *
 * So: one derivation, three callers (`testenv.mjs`, the Tier-3 scenarios,
 * and the assertion suite, which additionally proves the id is live before
 * any VM budget is spent). Throwing beats returning a guess — an id nobody
 * can resolve must stop the run, not quietly become a 404 ladder.
 */
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

/** The emitted registry — `pnpm --filter chat-example build` writes it. */
const REGISTRY = new URL('../dist/server/sigx-server-fns.js', import.meta.url);

/**
 * @param {string} name serverFn export name, e.g. `postMessage`
 * @returns {string} the hashed wire id
 */
export function serverFnId(name) {
    let source;
    try {
        source = readFileSync(fileURLToPath(REGISTRY), 'utf8');
    } catch {
        throw new Error(
            `[chat] cannot read ${fileURLToPath(REGISTRY)} — run \`pnpm --filter chat-example build\` first`
        );
    }
    // The registry also holds a stable `<pkg>/src/…#name` alias, but that
    // one contains slashes: as a URL segment it becomes %2F, which proxies
    // are entitled to normalize. The hashed id is the form the browser
    // actually sends, so it is the form to measure.
    const found = new RegExp(`"(${name}_fn_[0-9a-f]+)"`).exec(source);
    if (!found) {
        throw new Error(`[chat] no serverFn id for '${name}' in the built registry`);
    }
    return found[1];
}

/** The write half of every Tier-3 ladder. */
export const postMessageFnId = () => serverFnId('postMessage');
