/**
 * The knobs, in one place, so `vite.config.ts` and `server.mjs` cannot drift.
 *
 * Defaults match `pnpm --filter counter-example cluster:serve`, which is what
 * this example is pointed at: three hosts on 5391–5393, ops behind
 * `demo-ops-secret`.
 *
 * The secret is read from the environment with a demo fallback — appropriate
 * for a demo cluster whose secret is printed in its own README, and NOT the
 * shape to copy. In a real deployment it comes from your secret store and has
 * no default at all.
 */
export const PORT = Number(process.env.DASHBOARD_PORT ?? 5490);

/**
 * Any ONE host of the cluster — it fans out to the rest for us, which is the
 * whole reason a dashboard needs one address and not a list.
 *
 * 5392 rather than 5391, and that is not arbitrary: `cluster:serve` KILLS the
 * first host on its way past, to show the survivors re-forming and re-claiming
 * its reminder shards. Pointing at 5391 gives you a dashboard that cannot
 * connect, which reads as a broken example rather than as the demo working.
 */
export const OPS_HOST = process.env.OPS_HOST ?? 'http://127.0.0.1:5392';

export const OPS_SECRET = process.env.OPS_SECRET ?? 'demo-ops-secret';

/**
 * The same-origin path the browser calls.
 *
 * Deliberately not `/_sigx/ops`: it is a route of THIS app, and giving it the
 * host's path invites the belief that the browser is talking to the host.
 */
export const OPS_MOUNT = '/ops';
