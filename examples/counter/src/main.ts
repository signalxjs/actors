/**
 * Browser side. `counter.actor.ts` is build-swapped: the import below
 * resolves to a typed client ref, and `actor()` turns each call into a
 * POST to /_sigx/actor — same expression that dispatches in-process on
 * the server.
 */
import { actor } from '@sigx/actors';
import { Counter } from './counter.actor';

const name = new URLSearchParams(location.search).get('counter') ?? 'demo';
const client = actor(Counter, name);

const countEl = document.querySelector('#count')!;
const visitEl = document.querySelector('#visit')!;

function render(state: { count: number; lastVisit: Date | null }): void {
    countEl.textContent = String(state.count);
    visitEl.textContent = state.lastVisit ? state.lastVisit.toLocaleTimeString() : '–';
}

document.querySelector('#inc')!.addEventListener('click', () => {
    void client.increment(1);
});

// Live view: the watch stream re-renders on every actor state change —
// open the page twice and watch both update.
void (async () => {
    for await (const state of client.watch()) {
        render(state as { count: number; lastVisit: Date | null });
    }
})();
