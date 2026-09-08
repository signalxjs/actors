/**
 * The `--set` flags that point ANY workload at this estate's node pool.
 *
 * The pool is labelled AND tainted with the workload name, so every pod
 * that belongs on it needs both halves: the node selector to choose those
 * nodes, and the toleration to be allowed onto them. A pod carrying only
 * the selector picks precisely the nodes whose taint it cannot tolerate and
 * stays Pending forever.
 *
 * That is not hypothetical — it is #427. `testenv.mjs` had this function
 * and used it for the helm releases (#407), while `wf-load.mjs` and
 * `ws-load.mjs`, which are deliberately standalone of `testenv.mjs`, each
 * passed the selector alone. So the generator Job could never be scheduled
 * on a second estate, and every load dispatch against one waited out its
 * whole timeout on a pod that would never start. The host-per-core recipe
 * in RUNBOOK (u) had therefore never run.
 *
 * It lives here, imported by all three, so they cannot drift apart again.
 * Pure and dependency-free, which is what lets the standalone verbs take it
 * without taking `testenv.mjs`'s module-scope config and side effects.
 *
 * The toleration is written WHOLE, and that is not verbosity: helm merges
 * maps but REPLACES list elements, so `--set tolerations[0].value=x` alone
 * discards the key, operator and effect that `values.yaml` gives the same
 * element, and the API server rejects the pod — "operator must be Exists
 * when `key` is empty". `nodeSelector.workload` is a map key and merges,
 * which is why only its sibling needed fixing (#406).
 */
export const workloadSets = (workload) => [
    '--set', `nodeSelector.workload=${workload}`,
    '--set', 'tolerations[0].key=workload',
    '--set', 'tolerations[0].operator=Equal',
    '--set', `tolerations[0].value=${workload}`,
    '--set', 'tolerations[0].effect=NoSchedule'
];
