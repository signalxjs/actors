/**
 * The component the whole example is really about: an actor read that
 * renders on the server, ships inside the document, and hydrates in the
 * browser without a second request.
 */
import { component, errorScope, onMounted, onUnmounted, signal, useAction, useData } from 'sigx';
import { useActorAction, useActorState, useActorsContext } from '@sigx/actors/app';
import { actor, actorKey } from '@sigx/actors';
import { RoomActor } from './room.actor';
import { me, postMessage, signIn, signOut } from './chat.server';
import type { Message } from './room.actor';

export const Room = component<{ room?: string }>((ctx) => {
    const room = ctx.props.room ?? 'general';
    const draft = signal({ text: '', name: '' });
    const { cells } = useActorsContext();

    errorScope({
        fallback: (error, retry) => (
            <p class="error">
                {error.message} <button onClick={retry}>retry</button>
            </p>
        )
    });

    // Guarded actor reads. On the server these dispatch in-process and
    // serialize into the page; in the browser they restore from it.
    const messages = useActorState(RoomActor, room, 'recent', 20);
    const topic = useActorState(RoomActor, room, 'topic');

    // Unattributed write — straight to the actor.
    const setTopic = useActorAction(RoomActor, room, 'setTopic');
    // Attributed write — through the serverFn that knows the session. A
    // serverFn is outside `useActorAction`'s reach, so the reads it stales
    // are declared here by hand.
    const post = useAction(async (text: string) => {
        const count = await postMessage({ room, text });
        cells.invalidate([actorKey(RoomActor, room)]);
        return count;
    });
    // A plain serverFn read beside the actor reads: `me` carries core's
    // `__sigxKey`, so useData keys and SSR-serializes it the same way.
    const session = useData(me);

    // The OTHER tabs. Everything above refreshes only the tab that wrote:
    // `useActorAction` and the `cells.invalidate()` in `post` are local
    // bookkeeping, and nothing in a request/response call ever tells a
    // second browser that something happened. `watch` is the actor's push
    // side — one long-lived NDJSON response per tab, yielding after every
    // turn that mutated the room, whoever caused it.
    //
    // What arrives is a state snapshot, and it is deliberately ignored: the
    // cells above stay the single definition of what this page reads (a
    // `recent(20)` slice, a topic), so the stream only has to say "stale"
    // and let them re-read. That is the same `invalidate` call `post`
    // already makes — the difference is what triggers it.
    //
    // The feed opens with `{ initial: true }`, so the first value lands as
    // soon as the subscription does and costs one catch-up read. That is
    // NOT the hydration refetch this example avoids — the first paint still
    // comes from the document alone. It is the price of the window between
    // the SSR read and this subscription, which no amount of seeding can
    // observe from here.
    //
    // Client-only, so `onMounted`: during SSR there is no tab to update.
    //
    // The loop RECONNECTS. A long-lived NDJSON response dies for reasons
    // that are nobody's bug — a proxy idle timeout, a rolling restart, a
    // laptop lid — and a page that goes permanently stale on the first
    // drop is broken in production. Backoff doubles 1s → 30s with jitter
    // and resets after any healthy stream; `{ initial: true }` inside
    // `watch()` doubles as the catch-up read after every reconnect, so
    // nothing that happened during the gap is missed.
    const live = new AbortController();
    const wait = (ms: number): Promise<void> =>
        new Promise((resolve) => {
            // Listener BEFORE arming the timer, and short-circuit an
            // already-aborted signal — otherwise an unmount racing this
            // setup waits out the whole backoff.
            if (live.signal.aborted) return resolve();
            let t: ReturnType<typeof setTimeout> | undefined;
            const done = (): void => {
                if (t !== undefined) clearTimeout(t);
                live.signal.removeEventListener('abort', done);
                resolve();
            };
            live.signal.addEventListener('abort', done);
            t = setTimeout(done, ms);
        });
    onMounted(() => {
        void (async () => {
            let delay = 1_000;
            while (!live.signal.aborted) {
                try {
                    const watching = actor(RoomActor, room).with({ signal: live.signal }).watch();
                    for await (const _ of watching) {
                        delay = 1_000; // a healthy stream resets the backoff
                        cells.invalidate([actorKey(RoomActor, room)]);
                    }
                } catch (error) {
                    // An aborted stream is this component unmounting, not
                    // a failure — bail silently. Anything else gets logged
                    // once per attempt and retried.
                    if (live.signal.aborted) return;
                    console.warn('[chat] live stream dropped, reconnecting:', error);
                }
                if (live.signal.aborted) return;
                await wait(delay + Math.random() * 250);
                delay = Math.min(delay * 2, 30_000);
            }
        })();
    });
    // Breaking the iteration aborts the underlying fetch, so the server
    // sees the disconnect and drops its `ctx.changes()` subscription.
    onUnmounted(() => live.abort());

    const send = async (): Promise<void> => {
        const text = draft.text;
        if (!text.trim()) return;
        draft.text = '';
        // No refresh() here: `postMessage` writes through the actor, and
        // `useActorAction` invalidates every read of it. This one goes
        // through a serverFn, so it declares the same thing explicitly —
        // see `post` above.
        await post.run(text);
    };

    return () => (
        <main>
            <h1>#{room}</h1>
            <p class="topic">
                {topic.match({
                    ready: (t) => t,
                    pending: () => 'loading topic…',
                    // A guard rejection arrives HERE, as state, not as a
                    // thrown error — errorScope covers render failures,
                    // while AsyncState reports its own.
                    error: (e) => <span class="error">{e.message}</span>
                })}
                <button
                    disabled={setTopic.loading}
                    // `useActorAction` invalidates every read of this
                    // actor on success, so `topic` refreshes itself.
                    onClick={() => void setTopic.run(['random thoughts'])}
                >
                    change topic
                </button>
            </p>

            <ul class="messages">
                {messages.match({
                    pending: () => <li>loading…</li>,
                    error: (e, retry) => (
                        <li class="error">
                            {e.message} <button onClick={retry}>retry</button>
                        </li>
                    ),
                    ready: (list: Message[]) =>
                        list.length === 0 ? (
                            <li class="empty">no messages yet</li>
                        ) : (
                            list.map((m) => (
                                <li>
                                    <b>{m.from}</b> {m.text}{' '}
                                    <time>{m.at.toLocaleTimeString()}</time>
                                </li>
                            ))
                        )
                })}
            </ul>

            <form onSubmit={(e: Event) => (e.preventDefault(), void send())}>
                <input
                    value={draft.text}
                    onInput={(e: Event) => (draft.text = (e.target as HTMLInputElement).value)}
                    placeholder="say something"
                />
                <button disabled={post.loading}>send</button>
            </form>

            <footer>
                {session.match({
                    pending: () => <span>…</span>,
                    ready: (u) =>
                        u === null ? (
                            // No session: a real sign-in — the serverFn
                            // mints an HMAC-signed HttpOnly cookie, then a
                            // reload re-SSRs the page with it (honest and
                            // tiny for an example; a router would refresh
                            // in place).
                            <form
                                onSubmit={(e: Event) => (
                                    e.preventDefault(),
                                    void (async () => {
                                        try {
                                            await signIn({ name: draft.name });
                                            location.reload();
                                        } catch (error) {
                                            console.warn('[chat] sign-in failed:', error);
                                        }
                                    })()
                                )}
                            >
                                <input
                                    value={draft.name}
                                    onInput={(e: Event) =>
                                        (draft.name = (e.target as HTMLInputElement).value)
                                    }
                                    placeholder="pick a name"
                                />
                                <button>sign in</button>
                            </form>
                        ) : (
                            <span>
                                signed in as <b>{u}</b>{' '}
                                <button
                                    onClick={() =>
                                        void signOut().then(() => location.reload())
                                    }
                                >
                                    sign out
                                </button>
                            </span>
                        )
                })}
            </footer>
        </main>
    );
});
