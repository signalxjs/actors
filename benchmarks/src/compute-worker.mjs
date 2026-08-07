/**
 * The other side of the thread crossing (#119).
 *
 * Plain `.mjs`, not `.ts`: the pool spawns this with `new Worker(url)`, and a
 * worker that has to be type-stripped before it can answer its first message
 * would fold the loader's cost into the crossing this benchmark exists to
 * price. `.mjs` also matches the repo's convention for non-linted entries.
 *
 * The body MUST be identical to `hashIterations` in `compute-actors.ts` — the
 * comparison is "the same work, here or there", and a worker that computes a
 * slightly different thing measures nothing.
 */
import { parentPort } from 'node:worker_threads';

/** Keep in step with `hashIterations` in `compute-actors.ts`. */
function hashIterations(payload, iterations) {
    let h = 0;
    for (let i = 0; i < iterations; i++) {
        h = (h << 5) - h + (payload.charCodeAt(i % payload.length) | 0);
        h |= 0;
    }
    return h;
}

parentPort.on('message', (msg) => {
    parentPort.postMessage({ id: msg.id, value: hashIterations(msg.payload, msg.iterations) });
});
