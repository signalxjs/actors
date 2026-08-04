/**
 * Types for `post-fn.mjs`. Hand-written because the module is plain ESM
 * shared by three TypeScript-checked callers (the Tier-3 scenarios, the
 * assertion suite) and one plain-JS one (`testenv.mjs`) — which is exactly
 * why it is not a `.ts` file to begin with: the VM-side and script-side
 * callers run it with no build step.
 */

/** The hashed wire id for a serverFn export, read from the built registry. */
export function serverFnId(name: string): string;

/** The write half of every Tier-3 ladder: `postMessage_fn_<hash>`. */
export function postMessageFnId(): string;
