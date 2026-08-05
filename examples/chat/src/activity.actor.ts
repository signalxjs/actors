/**
 * The cross-room activity feed — the topics projection pattern.
 *
 * Every room publishes to `room-activity`, keyed by room name. ONE singleton
 * subscriber (`key: () => 'all'`) folds every room's events into a bounded
 * recent list, and the page observes that list live.
 *
 * Topics is what makes this possible at all: a room cannot know who is
 * interested, and this feed cannot know which rooms exist. The subscription
 * IS the wiring, declared where the interest lives.
 */
import { topic } from '@sigx/actors';
import { defineActor } from './actors.app';

/** What a room announces. The room name rides the topic KEY. */
export interface RoomActivity {
    readonly what: 'message' | 'topic';
}

export const roomActivity = (room: string) => topic<RoomActivity>('room-activity', room);

export interface ActivityEntry {
    readonly room: string;
    readonly what: 'message' | 'topic';
    readonly at: Date;
    /** Who caused it. Absent when the publish came from an unauthenticated
     *  context — a script, an ops probe. */
    readonly who?: string;
}

/** How much history the feed keeps. A projection, not a log. */
const KEEP = 50;

export const ActivityFeed = defineActor({
    type: 'ActivityFeed',
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
                // `ctx.principal` here is the PUBLISHING call's identity:
                // the edge authenticated it, the room's turn carried it, and
                // `ctx.publish` handed it to this handler. Attribution with
                // no field threaded through the payload.
                const who = (ctx.principal as { name: string } | null)?.name;
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
