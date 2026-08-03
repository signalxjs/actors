/**
 * The component the whole example is really about: an actor read that
 * renders on the server, ships inside the document, and hydrates in the
 * browser without a second request.
 */
import { component, errorScope, signal, useAction, useData } from 'sigx';
import { useActorAction, useActorState, useActorsContext } from '@sigx/actors/app';
import { actorKey } from '@sigx/actors';
import { RoomActor } from './room.actor';
import { ActivityFeed } from './activity.actor';
import { me, postMessage, signIn, signOut } from './chat.server';
import type { Message } from './room.actor';
import type { ActivityEntry } from './activity.actor';

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
    //
    // `{ live: true }` adds the OTHER tabs. Everything else here only ever
    // refreshes the tab that wrote — `useActorAction` and the
    // `cells.invalidate()` below are local bookkeeping, and nothing in a
    // request/response call tells a second browser that anything happened.
    // With it, both reads ride ONE held-open connection for the whole page
    // (the `$live` mount multiplexes them and pings every 30 s so proxies
    // leave it alone), each re-running server-side after any turn that
    // mutated the room, whoever caused it — and the first paint still comes
    // from the document alone.
    const messages = useActorState(RoomActor, room, 'recent', 20, { live: true });
    const topic = useActorState(RoomActor, room, 'topic', { live: true });
    // The topics projection: every room publishes to `room-activity`, ONE
    // singleton ActivityFeed folds those events, and this read observes it —
    // so activity in OTHER rooms shows up here without this page knowing
    // those rooms exist. Same live channel as the two reads above.
    const activity = useActorState(ActivityFeed, 'all', 'recent', 8, { live: true });

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

            <aside class="activity">
                <h2>across all rooms</h2>
                <ul>
                    {activity.match({
                        pending: () => <li>…</li>,
                        error: (e) => <li class="error">{e.message}</li>,
                        ready: (list: ActivityEntry[]) =>
                            list.length === 0 ? (
                                <li class="empty">nothing yet</li>
                            ) : (
                                [...list].reverse().map((a) => (
                                    <li>
                                        <b>#{a.room}</b>{' '}
                                        {a.what === 'message' ? 'a new message' : 'topic changed'}
                                        {a.who ? ` by ${a.who}` : ''}{' '}
                                        <time>{a.at.toLocaleTimeString()}</time>
                                    </li>
                                ))
                            )
                    })}
                </ul>
            </aside>

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
