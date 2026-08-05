/**
 * A page, so the deployed URL is something you can open.
 *
 * Served from `createFetchHandler`'s `fallback`: anything that is not an actor
 * call falls through to here, so one Worker serves both. Deliberately one
 * string with an inline script — no build step, no client bundle, no
 * framework. The point is to make three things visible, not to be an app:
 *
 *  - a key is an actor, and a different key is a different actor with its own
 *    Durable Object and its own state;
 *  - a `watch` stream updates the number as it changes, rather than being
 *    inferred from a curl;
 *  - a reminder fires with nothing driving it, which is the one capability a
 *    request cannot stand in for.
 *
 * For the real client story — `actor()` in the browser via the build-time
 * swap, `useActorState`, SSR — see `examples/chat`, which runs on Node.
 */
const HTML = String.raw`<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>@sigx/actors on Durable Objects</title>
<style>
  :root { color-scheme: light dark; --line: color-mix(in oklab, currentColor 15%, transparent); }
  * { box-sizing: border-box; }
  body {
    margin: 0; padding: 2rem 1.25rem; display: grid; gap: 2rem;
    justify-content: center; grid-template-columns: minmax(0, 34rem);
    font: 15px/1.55 ui-sans-serif, system-ui, -apple-system, sans-serif;
  }
  h1 { font-size: 1.15rem; margin: 0 0 .35rem; }
  h2 { font-size: .8rem; margin: 0 0 .75rem; text-transform: uppercase;
       letter-spacing: .07em; opacity: .6; font-weight: 600; }
  p { margin: 0; opacity: .75; }
  section { border: 1px solid var(--line); border-radius: 10px; padding: 1.25rem; }
  .row { display: flex; gap: .5rem; align-items: center; flex-wrap: wrap; }
  input {
    font: inherit; padding: .45rem .6rem; border-radius: 7px; min-width: 0; flex: 1 1 8rem;
    border: 1px solid var(--line); background: transparent; color: inherit;
  }
  button {
    font: inherit; padding: .45rem .85rem; border-radius: 7px; cursor: pointer;
    border: 1px solid var(--line); background: transparent; color: inherit;
  }
  button:hover:not(:disabled) { background: color-mix(in oklab, currentColor 8%, transparent); }
  button:disabled { opacity: .45; cursor: default; }
  output {
    font: 600 2.4rem/1 ui-monospace, monospace; font-variant-numeric: tabular-nums;
    display: block; margin: 1rem 0;
  }
  .note { font-size: .82rem; opacity: .6; margin-top: .75rem; }
  .live::before {
    content: ''; width: .5rem; height: .5rem; border-radius: 50%;
    background: currentColor; display: inline-block; margin-right: .4rem;
    vertical-align: .08em; opacity: .5;
  }
  .live.on::before { opacity: 1; background: #22c55e; }
  code { font-family: ui-monospace, monospace; font-size: .85em;
         background: color-mix(in oklab, currentColor 9%, transparent);
         padding: .1em .35em; border-radius: 4px; }
</style>

<header>
  <h1><code>@sigx/actors</code> on Cloudflare Durable Objects</h1>
  <p>One Durable Object per actor. Every key below is its own actor, with its
     own storage, anywhere in the world.</p>
</header>

<section>
  <h2>Counter</h2>
  <div class="row">
    <input id="key" value="demo" spellcheck="false" aria-label="actor key">
    <button id="load">Load</button>
  </div>
  <output id="count">–</output>
  <div class="row">
    <button id="inc">+1</button>
    <button id="inc10">+10</button>
    <span id="stream" class="live">stream idle</span>
  </div>
  <p class="note">Change the key and press Load — a different key is a
     different actor. The number updates from a <code>watch</code> stream, not
     from the button, so incrementing in another tab moves it here too.</p>
</section>

<section>
  <h2>Reminder</h2>
  <div class="row">
    <button id="arm">Arm for 20s</button>
    <span id="ticks">ticks: –</span>
  </div>
  <p class="note">A durable alarm. Leave this tab open and the count will
     tick up on its own — the platform woke the object and its turn pushed the
     change down the same stream. Nothing here polls. Close the tab and come
     back and it will still have fired, which is the one thing a request
     cannot fake.</p>
</section>

<script type="module">
const $ = (id) => document.getElementById(id);

async function call(type, method, args) {
    const res = await fetch('/_sigx/actor/' + type + '/' + method, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ args })
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok || body.error) throw new Error(body.error?.message ?? 'HTTP ' + res.status);
    return body.data;
}

let streams = [];

function stopStreams() {
    for (const ac of streams) ac.abort();
    streams = [];
}

/**
 * Follow one actor's changes and hand every frame to onChunk.
 *
 * NDJSON carries three kinds of line: a chunk, a terminating done, and an
 * in-band error. Handling only chunks would leave the badge claiming
 * "streaming" forever after the stream ended or failed.
 */
async function follow(type, key, onChunk) {
    const ac = new AbortController();
    streams.push(ac);
    const badge = $('stream');
    try {
        const res = await fetch('/_sigx/actor/' + type + '/watch', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args: [key] }),
            signal: ac.signal
        });
        if (!res.ok || !res.body) throw new Error('HTTP ' + res.status);
        badge.classList.add('on');
        badge.textContent = 'streaming ' + key;
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        reading: for (;;) {
            const { value, done } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            let nl;
            while ((nl = buffer.indexOf('\n')) >= 0) {
                const line = buffer.slice(0, nl);
                buffer = buffer.slice(nl + 1);
                if (!line) continue;
                const frame = JSON.parse(line);
                if ('error' in frame) throw new Error(frame.error?.message ?? 'stream failed');
                if ('done' in frame) break reading;
                if ('chunk' in frame) onChunk(frame.chunk);
            }
        }
        badge.classList.remove('on');
        badge.textContent = 'stream ended';
    } catch (error) {
        if (ac.signal.aborted) return;
        badge.classList.remove('on');
        badge.textContent = 'stream: ' + error.message;
    }
}

const keyOf = () => $('key').value.trim() || 'demo';

function load() {
    stopStreams();
    const key = keyOf();
    // Both numbers arrive on streams. Nothing on this page polls — including
    // the reminder count, because a reminder delivery is a turn that mutates
    // state, so it notifies watchers like any other turn.
    void follow('Counter', key, (s) => ($('count').textContent = s.count));
    void follow('Ticker', key, (s) => ($('ticks').textContent = 'ticks: ' + s.ticks));
}

const bump = (by) => async () => {
    // No optimistic update: the number arrives on the stream, which is the
    // point being demonstrated.
    await call('Counter', 'increment', [keyOf(), by]);
};

$('load').onclick = load;
$('inc').onclick = bump(1);
$('inc10').onclick = bump(10);
$('key').onkeydown = (e) => { if (e.key === 'Enter') load(); };
$('arm').onclick = async () => {
    await call('Ticker', 'once', [keyOf(), 20_000]);
    $('arm').textContent = 'armed — due in 20s';
    setTimeout(() => ($('arm').textContent = 'Arm for 20s'), 20_000);
};

load();
</script>
`;

/** The page, with caching left off so a redeploy is visible immediately. */
export function servePage(request: Request): Response | undefined {
    const url = new URL(request.url);
    if (request.method !== 'GET' || (url.pathname !== '/' && url.pathname !== '/index.html')) {
        return undefined; // not ours — let the 404 happen
    }
    return new Response(HTML, {
        headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' }
    });
}
