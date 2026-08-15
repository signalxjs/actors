/**
 * Server-side configuration — including the ops bearer token.
 *
 * **Nothing in `src/main.tsx`'s import graph may reach this file.** That is
 * the example's whole guarantee, it is enforced by
 * `__tests__/no-secret-in-browser.test.ts`, and the public half lives in
 * `src/config.public.ts`.
 *
 * Defaults match `pnpm --filter counter-example cluster:serve`, which is what
 * this example is pointed at: three hosts on 5391–5393, ops behind
 * `demo-ops-secret`.
 */
export const PORT = Number(process.env.DASHBOARD_PORT ?? 5490);

/** The three hosts `pnpm --filter counter-example cluster:serve` starts. */
const DEFAULT_HOSTS = 'http://127.0.0.1:5391,http://127.0.0.1:5392,http://127.0.0.1:5393';

/**
 * Candidate hosts, tried in order until one answers.
 *
 * A dashboard needs any ONE surviving host — the fan-out reaches the rest —
 * and that is the invariant worth coding to, because **you cannot know which
 * host will be alive**. `cluster:serve` kills the owner of the `cart` actor:
 *
 *     step(`4. Crash failover — killing the owner ${entry.hostId}`);
 *
 * Which host that is comes out of the placement policy, so the casualty
 * varies from run to run — 5391 one time, 5392 the next. An earlier version
 * of this file hardcoded a single port on the strength of one observation,
 * which gave a one-in-three chance of aiming the example squarely at the
 * corpse and a dashboard that read as broken (#256).
 *
 * `OPS_HOST` (singular) still works for the ordinary case of pointing this at
 * one real deployment.
 */
export const OPS_HOSTS = (process.env.OPS_HOSTS ?? process.env.OPS_HOST ?? DEFAULT_HOSTS)
    .split(',')
    .map((host) => host.trim().replace(/\/+$/, ''))
    .filter(Boolean);

/**
 * The `ops({ secret })` bearer token.
 *
 * Read from the environment with a demo fallback — appropriate for a demo
 * cluster whose secret is printed in its own README, and **not** the shape to
 * copy. In a real deployment it comes from your secret store and has no
 * default at all, so a misconfiguration fails loudly instead of serving your
 * cluster topology under a guessable token.
 */
export const OPS_SECRET = process.env.OPS_SECRET ?? 'demo-ops-secret';
