/**
 * Two runtime primitives that no other example runs: detached tasks, and
 * durable reminders that outlive the process.
 *
 *     pnpm --filter counter-example runtime-demo    # after `pnpm build`
 *
 * (`runtime-demo`, not `runtime` — pnpm resolves the latter to a builtin
 * and never reaches this package's script.)
 *
 * What each part proves is in README.md. Every step throws on a wrong
 * result, so this file is an assertion suite that narrates itself.
 */
import { rm } from 'node:fs/promises';
import { join } from 'node:path';
import { defineActorApp } from '@sigx/actors/host';
import { fileStorage } from '@sigx/actors/node';
import { Importer } from './src/importer.actor.ts';
import { Reminder } from './src/reminder.actor.ts';

const DIR = join(import.meta.dirname, '.actors-runtime-demo');
const log = (...args) => console.log(...args);
const step = (title) => log(`\n=== ${title} ===`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// A clean slate, so `fired` counts from zero on a re-run.
await rm(DIR, { recursive: true, force: true });

/**
 * ON DISK, deliberately: the reminder half of this demo restarts the
 * process's host and needs the second one to find what the first wrote.
 *
 * `reminderTickMs` is 30s by default — the scheduler scans for due
 * reminders on that cadence. The demo drops it to 250ms so you are not
 * watching paint dry; nothing else about the mechanism changes, and a real
 * deployment wants the default (a scan is a storage read per shard).
 */
const newHost = async () => {
    const app = defineActorApp({
        actors: [Importer, Reminder],
        storage: fileStorage({ dir: DIR }),
        defaults: { reminderTickMs: 250 }
    });
    return { app, host: await app.start() };
};

let { host } = await newHost();

// ---------------------------------------------------------------------------

step('1. A task runs DETACHED — reads keep answering while it works');

const importer = host.actor(Importer, 'nightly');
log(`begin(10):  ${JSON.stringify(await importer.begin(10))}`);

// If the work were a plain method, every one of these would queue behind it
// and the first answer would arrive after the import finished.
const seen = [];
for (let i = 0; i < 6; i++) {
    await sleep(200);
    const s = await importer.status();
    seen.push(s.done);
    log(`  status while running: phase=${s.phase} done=${s.done}/${s.total} running=[${s.running}]`);
}

if (!seen.some((d) => d > 0 && d < 10)) {
    throw new Error(`status never observed partial progress: ${seen.join(',')}`);
}
log('(every one of those answered mid-import — a method call could not have)');

while ((await importer.status()).phase === 'running') await sleep(150);
const finished = await importer.status();
log(`finished:   phase=${finished.phase} done=${finished.done}/${finished.total}`);
if (finished.phase !== 'done' || finished.done !== 10) {
    throw new Error(`import did not complete: ${JSON.stringify(finished)}`);
}

step('2. Cancelling is a request, not a join');

const cancelled = host.actor(Importer, 'aborted');
await cancelled.begin(50);
await sleep(400);
await cancelled.stop();
while ((await cancelled.status()).phase === 'running') await sleep(100);
const stopped = await cancelled.status();
log(`stopped at done=${stopped.done}/${stopped.total} phase=${stopped.phase}`);
if (stopped.phase !== 'stopped' || stopped.done === 0 || stopped.done >= 50) {
    throw new Error(`cancel did not stop it partway: ${JSON.stringify(stopped)}`);
}
log('(the run wound down through its own final turn — the checkpoint landed)');

// ---------------------------------------------------------------------------

step('3. Arm a durable reminder, then kill the host that armed it');

const armed = await host.actor(Reminder, 'nightly-sweep').arm(1500);
log(`armed at ${armed.toISOString()}, due in 1500ms`);
log(`pending: [${(await host.actor(Reminder, 'nightly-sweep').status()).pending}]`);

await host.stop({ timeoutMs: 5000 });
log('host stopped — the activation is gone, and so is its scheduler');

step('4. A NEW host, same storage — the reminder fires with nobody calling');

({ host } = await newHost());

/**
 * NOT a poll. Reading `status()` would itself activate the actor, and then
 * "the reminder woke it" and "I woke it and the reminder happened to be
 * due" are indistinguishable — the demo would prove nothing.
 *
 * So: touch it zero times, wait past the due time, and read ONCE at the
 * end. Nothing in this process addressed the actor during the window, so
 * the only thing that can have activated it is the host's reminder scan.
 */
log('making NO calls to the actor — waiting 5s in silence');
await sleep(5000);

const woke = await host.actor(Reminder, 'nightly-sweep').status();
log(`fired=${woke.fired}  lastFiredAt=${woke.lastFiredAt?.toISOString()}`);
if (woke.fired !== 1) {
    throw new Error(`reminder did not fire on the new host: ${JSON.stringify(woke)}`);
}
if (woke.lastFiredAt.getTime() < armed.getTime()) {
    throw new Error('lastFiredAt predates the arm — stale state, not a delivery');
}
log('(the reminder outlived the process that armed it and re-activated the actor)');

await host.stop({ timeoutMs: 5000 });
await rm(DIR, { recursive: true, force: true });

log('\nRUNTIME DEMO COMPLETE — detached tasks, and a reminder that survived a restart.');
