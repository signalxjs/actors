/**
 * The component the whole example is really about: an actor read that
 * renders on the server, ships inside the document, and hydrates in the
 * browser without a second request.
 */
import { component, errorScope, signal, useAction, useData } from 'sigx';
import { useActorAction, useActorState, useActorsContext } from '@sigx/actors/app';
import { actorKey } from '@sigx/actors';
import { RoomActor } from './room.actor';
import { me, postMessage } from './chat.server';
import type { Message } from './room.actor';

export const Room = component(() => {
    const room = 'general';
    const draft = signal({ text: '' });
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
                signed in as{' '}
                {session.match({
                    ready: (u) => u ?? 'nobody',
                    pending: () => '…'
                })}
            </footer>
        </main>
    );
});
