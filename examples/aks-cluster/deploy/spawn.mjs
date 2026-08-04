/**
 * Make `execFileSync`/`spawn` able to reach a `.cmd` shim on Windows.
 *
 * The Azure CLI ships as `az.cmd` there, and pnpm as `pnpm.CMD`. Both of
 * this repo's Tier-3 entry points spawn them WITHOUT a shell — deliberately,
 * because a shell would re-parse arguments like a jsonpath expression or a
 * helm `--set` value — and that is exactly the combination Node refuses:
 *
 *   execFileSync('az', …)          → ENOENT   (no PATHEXT without a shell)
 *   execFileSync('az.cmd', …)      → EINVAL
 *   execFileSync('C:\…\az.cmd', …) → EINVAL
 *
 * The EINVAL is upstream and intentional: since Node 18.20 / 20.12,
 * `child_process` refuses `.bat`/`.cmd` unless `shell: true`
 * (CVE-2024-27980). So there is no name by which a shim can be spawned
 * directly, and the only route is through `cmd.exe`.
 *
 * Going through `cmd.exe` normally means handing your arguments to a shell,
 * which is the thing we were avoiding. It does not have to: with
 * `windowsVerbatimArguments` the command line is passed through untouched,
 * so we can escape each argument ourselves — first for the target's CRT
 * parser, then for cmd's own metacharacters — and the callee receives
 * exactly the argv it would have received on Linux. That two-layer escape is
 * the approach `cross-spawn` settled on; it is reproduced here rather than
 * taken as a dependency because these are unpublished example scripts, and
 * because the comment explaining WHY is the valuable half.
 *
 * On POSIX, and for real `.exe`s on Windows, nothing changes.
 */
import { spawnSync } from 'node:child_process';
import { extname } from 'node:path';

const WINDOWS = process.platform === 'win32';

/** `where` costs a subprocess; one per command name is plenty. */
const resolved = new Map();

/**
 * The first thing on PATH that Windows can actually execute.
 *
 * `where az` reports BOTH `…\wbin\az` (a bash script, useless here) and
 * `…\wbin\az.cmd`, in that order — so "first match" is the wrong pick and
 * the extension is what decides. A real `.exe` is always preferable: it
 * needs no cmd.exe hop at all.
 *
 * @param {string} cmd
 * @returns {string}
 */
function resolveOnWindows(cmd) {
    if (extname(cmd)) return cmd;
    const hit = resolved.get(cmd);
    if (hit !== undefined) return hit;
    let picked = cmd;
    // `where.exe` is a real executable, so it needs none of this itself.
    const found = spawnSync('where', [cmd], { encoding: 'utf8' });
    if (found.status === 0) {
        const lines = String(found.stdout)
            .split(/\r?\n/)
            .map((l) => l.trim())
            .filter(Boolean);
        picked =
            lines.find((l) => /\.exe$/i.test(l)) ??
            lines.find((l) => /\.(cmd|bat)$/i.test(l)) ??
            lines[0] ??
            cmd;
    }
    resolved.set(cmd, picked);
    return picked;
}

/**
 * Quote one argument so the callee's CRT parses it back to this exact
 * string, then escape it again so cmd.exe hands that quoting through
 * untouched rather than expanding `%VAR%` or acting on `&`, `|`, `<`, `>`.
 *
 * @param {string} arg
 * @returns {string}
 */
function escapeArgument(arg) {
    let value = String(arg);
    // A backslash is only special immediately before a quote (or at the very
    // end, where the closing quote makes it one) — so double runs there and
    // leave every other backslash alone. A Windows path is mostly
    // backslashes; escaping them all would corrupt it.
    value = value.replace(/(\\*)"/g, '$1$1\\"').replace(/(\\*)$/, '$1$1');
    value = `"${value}"`;
    // Now the cmd layer. The quotes we just added get escaped too: cmd
    // strips the `^`s and the CRT then sees the quoting above.
    return value.replace(/[()%!^"<>&|]/g, '^$&');
}

/** The program path, escaped for cmd — spaces included (`Program Files`). */
function escapeCommand(command) {
    return String(command).replace(/([()\][%!^"`<>&|;, *?])/g, '^$1');
}

/**
 * Rewrite `(command, args)` into a form `execFileSync`/`spawn` can run on
 * this platform. Spread the result's `options` into your own.
 *
 * @param {string} command
 * @param {readonly string[]} args
 * @returns {{ command: string, args: string[], options: object }}
 */
export function spawnable(command, args = []) {
    if (!WINDOWS) return { command, args: [...args], options: {} };
    const target = resolveOnWindows(command);
    if (!/\.(cmd|bat)$/i.test(target)) return { command: target, args: [...args], options: {} };
    const line = [escapeCommand(target), ...args.map((a) => escapeArgument(a))].join(' ');
    return {
        command: 'cmd.exe',
        // /d skips AutoRun scripts (a machine-specific one would otherwise
        // run before every single call); /s makes cmd strip exactly the
        // outer quote pair and leave the rest of the line alone.
        args: ['/d', '/s', '/c', `"${line}"`],
        options: { windowsVerbatimArguments: true }
    };
}
