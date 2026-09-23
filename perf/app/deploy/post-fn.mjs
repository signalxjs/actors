/**
 * The `postMessage` serverFn id, read from the BUILD rather than pasted.
 *
 * Since core 1.0 (rfc-server-v5, #450) the id is the function's stable KEY,
 * `<id>/<name>` — `sigx-perf-app/src/chat.server.ts/postMessage` — and the
 * route is `{base}/<key>`. Before it, the id was `postMessage_fn_<hash>`, and
 * the hash moved whenever the build decided it should: `edge-ladder.mjs`
 * used to carry one literally, and by the time anyone looked it had rotted
 * from `…_6c5508cb` to `…_2b42ef63` — silently, because a wrong id 404s and
 * a 404 is CHEAPER than a real write. `infra/write-mix` therefore reported
 * the breakage as extra throughput. The key moves far less, but it still
 * moves with the file path, so it is still derived.
 *
 * So: one derivation, three callers (`testenv.mjs`, the Tier-3 scenarios,
 * and the assertion suite, which additionally proves the id is live before
 * any VM budget is spent). Throwing beats returning a guess — an id nobody
 * can resolve must stop the run, not quietly become a 404 ladder.
 */
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

/** The emitted registry — `pnpm --filter sigx-perf-app build` writes it. */
const REGISTRY = new URL('../dist/server/sigx-server-fns.js', import.meta.url);

/**
 * @param {string} name serverFn export name, e.g. `postMessage`
 * @returns {string} the stable key, which is also the wire path
 */
export function serverFnId(name) {
    let source;
    try {
        source = readFileSync(fileURLToPath(REGISTRY), 'utf8');
    } catch {
        throw new Error(
            `[chat] cannot read ${fileURLToPath(REGISTRY)} — run \`pnpm --filter sigx-perf-app build\` first`
        );
    }
    // The registry is keyed by `"<id>/<name>"`, and the slashes are real
    // path separators on the wire — `/_sigx/fn/<key>` with nothing encoded.
    const found = new RegExp(`\\["([^"]+/${name})"\\]`).exec(source);
    if (!found) {
        throw new Error(`[chat] no serverFn id for '${name}' in the built registry`);
    }
    return found[1];
}

/** The write half of every Tier-3 ladder. */
export const postMessageFnId = () => serverFnId('postMessage');
