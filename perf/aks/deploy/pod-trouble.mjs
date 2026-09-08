/**
 * Why is this pod not running yet?
 *
 * The load verbs poll a Job until it succeeds or their timeout expires. A
 * pod that can never start — a tag that was never built, a config error —
 * is not "failed" from the Job's point of view, it is *retrying*, so the
 * poll waits out the whole budget and reports the cancellation and nothing
 * else. That cost an hour of cluster time twice in one session (#426): the
 * generator's image tag defaults to the runner's git HEAD, and merging to
 * `main` mid-session silently repointed it at a commit that had no image.
 *
 * Shared by `wf-load.mjs` and `ws-load.mjs` deliberately. Both are
 * standalone of `testenv.mjs` — which has config and side effects at module
 * scope — but this is a pure function taking `kube` as a parameter, so the
 * two cannot drift into disagreeing about what counts as stuck.
 */

/**
 * Container waiting-reasons that never resolve on their own. Back-off
 * reasons are included because that IS the steady state: kubelet retries
 * an unpullable image forever, so waiting longer changes nothing.
 */
export const BLOCKED_REASONS = new Set([
    'ImagePullBackOff',
    'ErrImagePull',
    'InvalidImageName',
    'CreateContainerConfigError',
    'CreateContainerError',
    'CrashLoopBackOff'
]);

/**
 * Inspect the pods matching `selector`.
 *
 * Returns `{ blocked, lines }`: `lines` always describes what was found (so
 * a caller can print it on any stall), while `blocked` is true only when a
 * pod is in a state that will not fix itself — which is what justifies
 * failing early rather than waiting out the timeout.
 */
export function podTrouble(kube, namespace, selector) {
    const raw = kube(['-n', namespace, 'get', 'pods', '-l', selector, '-o', 'json'],
        { quiet: true, allowFail: true });
    if (!raw) return { blocked: false, lines: [] };
    let items;
    try {
        items = JSON.parse(raw).items ?? [];
    } catch {
        return { blocked: false, lines: [] };
    }
    const lines = [];
    let blocked = false;
    for (const pod of items) {
        const name = pod.metadata?.name ?? '(unnamed)';
        const phase = pod.status?.phase ?? '?';
        if (phase === 'Running' || phase === 'Succeeded') continue;
        const containers = [
            ...(pod.status?.containerStatuses ?? []),
            ...(pod.status?.initContainerStatuses ?? [])
        ];
        for (const c of containers) {
            const w = c.state?.waiting;
            if (!w) continue;
            lines.push(`${name}/${c.name}: ${w.reason ?? '?'}${w.message ? ` — ${w.message}` : ''}`);
            if (BLOCKED_REASONS.has(w.reason)) blocked = true;
        }
        // Pending with no container status at all is the scheduler's, not
        // the kubelet's: the reason lives on the pod condition.
        if (containers.length === 0) {
            const unsched = (pod.status?.conditions ?? [])
                .find((c) => c.type === 'PodScheduled' && c.status === 'False');
            if (unsched) {
                lines.push(`${name}: ${unsched.reason ?? 'NotScheduled'}${unsched.message ? ` — ${unsched.message}` : ''}`);
                // Unschedulable can resolve — the autoscaler may add a node —
                // so it is reported, never fatal.
            } else {
                lines.push(`${name}: ${phase}`);
            }
        }
    }
    return { blocked, lines };
}
