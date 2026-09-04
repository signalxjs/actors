// @vitest-environment node
// Node, NOT the repo-default happy-dom: its `fetch` enforces browser CORS
// and refuses the host-to-host hop — the same reason `infra.test.ts` says so.
/**
 * The workflow engine under host loss (#297), memory-backed: three
 * in-process hosts on a `memoryClusterHub` over one shared `memoryStorage`.
 * The cases live in `helpers/wf-cluster.ts` and run again over Redis in
 * `workflow-cluster-redis.test.ts` (#383).
 *
 * The shape is set through env BEFORE the engine is imported (the knobs
 * are read at module load). `WF_DEACTIVATE_ON_SLEEP=0` deliberately: a run
 * that deactivates the moment it sleeps has no owner to kill. The unit
 * suite covers the deactivating shape.
 */
import { memoryStorage } from '@sigx/actors/host';
import { workflowClusterSuite } from './helpers/wf-cluster.ts';

process.env.WF_TIMER_THRESHOLD_MS = '100';
process.env.WF_REMINDER_TICK_MS = '50';
process.env.WF_STALE_WAKE_MS = '300';
process.env.WF_CHILD_STALE_MS = '200';
process.env.WF_DEACTIVATE_ON_SLEEP = '0';
process.env.WF_IDLE_AFTER_MS = '600000';
process.env.WF_STATS_SAVE_EVERY = '1';
process.env.WF_NOTIFY_RETRY_MS = '300';

workflowClusterSuite('memory', async () => ({ storage: memoryStorage() }));
