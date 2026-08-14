/**
 * Constants the BROWSER may see. Nothing else may live in this file.
 *
 * It exists as a file of its own so that the example's one guarantee — the
 * ops bearer token never reaches the client — is structural rather than a
 * bundler optimisation. `src/config.server.ts` holds the secret and reads
 * `process.env`; `src/main.tsx` imports only this, so there is no module path
 * from browser code to either.
 *
 * Tree-shaking would in fact drop them today (checked: the built bundle
 * contains neither the secret nor a `process.env` access). That is not good
 * enough for a security boundary. A guarantee that depends on an optimiser
 * staying clever is one that breaks quietly the first time somebody adds a
 * side effect, a `console.log` of the config object, or a re-export — and
 * `__tests__/no-secret-in-browser.test.ts` pins the split so it cannot.
 */

/**
 * The same-origin path the browser calls.
 *
 * Deliberately not `/_sigx/ops`: it is a route of THIS app, and giving it the
 * host's path invites the belief that the browser is talking to the host.
 */
export const OPS_MOUNT = '/ops';
