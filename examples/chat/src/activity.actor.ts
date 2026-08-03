/**
 * The cross-room activity feed — the topics projection pattern, live.
 *
 * Every room publishes to the `room-activity` topic (keyed by room name);
 * ONE singleton subscriber (`key: () => 'all'`) folds every room's events
 * into a bounded recent list; and the page observes that list with
 * `useActorState(…, { live: true })`, which pushes after every handler
 * turn. Topics is what makes the cross-actor half possible at all: a room
 * cannot know who is interested, and this feed cannot know which rooms
 * exist — the subscription IS the wiring, declared where the interest
 * lives.
 */
import { topic } from '@sigx/actors';
import { defineActor } from './actors.app';
import { requireUser } from './guards';

/** What a room announces. The room name rides the topic KEY. */
export interface RoomActivity {
    readonly what: 'message' | 'topic';
}

export const roomActivity = (room: string) => topic<RoomActivity>('room-activity', room);

export interface ActivityEntry {
    readonly room: string;
    readonly what: 'message' | 'topic';
    readonly at: Date;
    /** Who caused it — the session user from the context bag, which the
     *  publishing room's turn inherited from the edge guard and
     *  `ctx.publish` handed to this subscriber. Absent when the publish
     *  came from an unstamped context (a script, an ops probe). */
    readonly who?: string;
}

/** How much history the feed keeps. A projection, not a log. */
const KEEP = 50;

export const ActivityFeed = defineActor({
    type: 'ActivityFeed',
    use: [requireUser],
    state: () => ({ recent: [] as ActivityEntry[] }),
    methods: (ctx) => ({
        /** The read the page seeds from and watches live. */
        async recent(limit: number): Promise<ActivityEntry[]> {
            return ctx.state.recent.slice(-limit);
        }
    }),
    subscriptions: {
        'room-activity': {
            // Every room's events land on ONE instance — the aggregator
            // shape of the key mapping.
            key: () => 'all',
            handle: async (ctx, event) => {
                const { what } = event.payload as RoomActivity;
                // ctx.bag here is the PUBLISHING call's bag: the guard
                // stamped it at the edge, the room's turn carried it, and
                // ctx.publish handed it to this handler — attribution with
                // no field threaded through any payload.
                const who = ctx.bag.user;
                ctx.state.recent.push({
                    room: event.topic.key,
                    what,
                    at: new Date(event.at),
                    ...(who ? { who } : {})
                });
                if (ctx.state.recent.length > KEEP) {
                    ctx.state.recent.splice(0, ctx.state.recent.length - KEEP);
                }
                await ctx.save();
            }
        }
    }
});
