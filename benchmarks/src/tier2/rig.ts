/**
 * The Tier-2 rig: N silos as real forked processes, talking over real
 * loopback sockets.
 *
 * `fork` rather than `spawn` because it inherits `execArgv` — so
 * `--conditions=production` propagates and the children measure the BUILT
 * prod dist, exactly like every other scenario here. A spawned `node` would
 * silently benchmark the dev dist, which is the failure mode most likely to
 * go unnoticed because everything would still work.
 *
 * The parent hosts the cluster store (one `memoryClusterHub`) and answers
 * children's membership/directory RPCs. That is deliberate: this rig exists
 * to isolate the TRANSPORT, and a real Redis would inject its own latency,
 * sockets and file descriptors into every comparison — #87 already owns
 * that axis. The `ipc_ops_per_call` guard is what keeps the choice honest:
 * if the route cache is not doing its job, the run measured the store and
 * says so rather than reporting a transport number.
 */
import { fork, type ChildProcess } from 'node:child_process';
import { memoryClusterHub } from '@sigx/actors/cluster';
import type { MembershipView } from '@sigx/actors/cluster';
import type { Percentiles } from '../histogram.ts';
import type {
    ChildStats,
    DispatcherKind,
    DriveRequest,
    StoreOp,
    ToChild,
    ToParent
} from './protocol.ts';

const CHILD_URL = new URL('./child.ts', import.meta.url);
/** A child that has not answered in this long is wedged; kill the run. */
const REPLY_TIMEOUT_MS = 30_000;

export interface RigOptions {
    silos: number;
    /** `null` runs the cluster unauthenticated — the HMAC-off arm. */
    secret: string | null;
    /** Outbound HTTP client. Default `'default'` = the global fetch, i.e.
     *  exactly what ships. */
    dispatcher?: DispatcherKind;
    /** Per-origin connection cap for the tuned arms. */
    connections?: number;
}

export interface DriveOutcome {
    ops: number;
    opsPerSec: number;
    percentiles?: Percentiles;
}

interface Member {
    index: number;
    child: ChildProcess;
    port: number;
    siloId: string;
    providers: ReturnType<ReturnType<typeof memoryClusterHub>['providers']>;
    /** Keyed `<replyType>:<id>`, or the bare type for id-less replies. */
    waiters: Map<string, { resolve(m: ToParent): void; reject(e: Error): void }>;
}

export interface Rig {
    readonly members: readonly { index: number; siloId: string; port: number }[];
    /** Activate `keys` on the silo that will OWN them, so a later drive from
     *  another silo is guaranteed to cross the wire. */
    warm(index: number, targets: readonly { type: string; key: string }[]): Promise<void>;
    drive(index: number, request: DriveRequest): Promise<DriveOutcome>;
    stats(index: number): Promise<ChildStats>;
    resetStats(): Promise<void>;
    stop(): Promise<void>;
}

/** The actor type the children host. */
export const BENCH_TYPE = 'Tier2Bench';

export async function startRig(options: RigOptions): Promise<Rig> {
    const hub = memoryClusterHub();
    const members: Member[] = [];
    let stopped = false;

    /** Kill everything, however we got here. A stranded child holds a port. */
    const killAll = (): void => {
        for (const m of members) {
            if (!m.child.killed) m.child.kill('SIGKILL');
        }
    };
    const onExit = (): void => killAll();
    /**
     * Ctrl-C must still stop the benchmark. Installing ANY `SIGINT` listener
     * suppresses Node's default exit, so reaping the children here without
     * exiting would leave the parent hung on a run it can no longer finish.
     * 130 is the conventional "terminated by SIGINT" status.
     */
    const onSigint = (): void => {
        killAll();
        process.exit(130);
    };
    process.once('exit', onExit);
    process.once('SIGINT', onSigint);

    try {
        for (let index = 0; index < options.silos; index++) {
            members.push(await startChild(index));
        }
        // Push the joined view to everyone once the whole cluster is up, so
        // no silo starts routing against a half-formed membership.
        broadcastView();
        return rig();
    } catch (error) {
        killAll();
        process.off('exit', onExit);
        process.off('SIGINT', onSigint);
        throw error;
    }

    // -----------------------------------------------------------------------

    async function startChild(index: number): Promise<Member> {
        const child = fork(CHILD_URL, { stdio: ['ignore', 'inherit', 'inherit', 'ipc'] });
        const providers = hub.providers();
        const member: Member = {
            index,
            child,
            port: 0,
            siloId: '',
            providers,
            waiters: new Map()
        };

        // A child failure must REJECT whatever the parent is waiting on.
        // Throwing from an event handler only produces an uncaught
        // exception, which strands the run instead of failing it.
        const abort = (reason: string): void => {
            const error = new Error(`[tier2] silo ${index} ${reason}`);
            for (const waiter of member.waiters.values()) waiter.reject(error);
            member.waiters.clear();
        };

        child.on('message', (message: ToParent) => {
            if (message.t === 'store') {
                void answerStore(child, member, message.id, message.op, message.args);
                return;
            }
            if (message.t === 'failed') {
                abort(`failed: ${message.message}`);
                return;
            }
            member.waiters.get(replyKey(message))?.resolve(message);
        });
        child.on('exit', (code, signal) => {
            if (!stopped) abort(`exited early (code ${code}, signal ${signal})`);
        });

        const ready = (await request(
            member,
            {
                t: 'init',
                index,
                secret: options.secret,
                dispatcher: options.dispatcher ?? 'default',
                connections: options.connections ?? 64
            },
            'ready'
        )) as { port: number };
        member.port = ready.port;
        const started = (await request(member, { t: 'start' }, 'started')) as { siloId: string };
        member.siloId = started.siloId;
        // Each child's membership handle lives here; the child's `join` RPC
        // has already run through it by the time `started` comes back.
        broadcastView();
        return member;
    }

    function replyKey(message: ToParent): string {
        return 'id' in message ? `${message.t}:${message.id}` : message.t;
    }

    function request(member: Member, message: ToChild, expect: ToParent['t']): Promise<ToParent> {
        const key = 'id' in message ? `${expect}:${(message as { id: number }).id}` : expect;
        return new Promise<ToParent>((resolve, reject) => {
            const timer = setTimeout(() => {
                member.waiters.delete(key);
                // A wedged child is killed rather than waited on: the run is
                // already invalid, and a hung fork outlives the parent.
                member.child.kill('SIGKILL');
                reject(
                    new Error(
                        `[tier2] silo ${member.index} did not answer "${expect}" in ` +
                            `${REPLY_TIMEOUT_MS}ms — killed`
                    )
                );
            }, REPLY_TIMEOUT_MS);
            member.waiters.set(key, {
                resolve: (reply) => {
                    clearTimeout(timer);
                    member.waiters.delete(key);
                    resolve(reply);
                },
                reject: (error) => {
                    clearTimeout(timer);
                    reject(error);
                }
            });
            member.child.send(message);
        });
    }

    async function answerStore(
        child: ChildProcess,
        member: Member,
        id: number,
        op: StoreOp,
        args: readonly unknown[]
    ): Promise<void> {
        try {
            const value = await invoke(member, op, args);
            const reply: ToChild = { t: 'storeReply', id, ok: true, value };
            child.send(reply);
            // Membership mutations change what every OTHER silo can see.
            if (op.startsWith('membership.')) broadcastView();
        } catch (error) {
            const reply: ToChild = {
                t: 'storeReply',
                id,
                ok: false,
                error: error instanceof Error ? error.message : String(error)
            };
            child.send(reply);
        }
    }

    function invoke(member: Member, op: StoreOp, args: readonly unknown[]): Promise<unknown> {
        const { membership, directory } = member.providers;
        switch (op) {
            case 'membership.join':
                return membership.join(args[0] as never);
            case 'membership.setStatus':
                return membership.setStatus(args[0] as never);
            case 'membership.leave':
                return membership.leave();
            case 'membership.refresh':
                return membership.refresh();
            case 'membership.isAlive':
                return membership.isAlive(args[0] as string);
            case 'directory.lookup':
                return directory.lookup(args[0] as string);
            case 'directory.claim':
                return directory.claim(args[0] as string, args[1] as never);
            case 'directory.release':
                return directory.release(args[0] as string, args[1] as never);
            case 'directory.evict':
                return directory.evict(args[0] as string, args[1] as never);
            case 'directory.evictSilo':
                return Promise.resolve(directory.evictSilo?.(args[0] as string) ?? 0);
        }
    }

    /** The store's view, pushed to every child — `view()` is sync on the
     *  provider interface, so children cannot pull it on the hot path. */
    function broadcastView(): void {
        if (members.length === 0) return;
        const view: MembershipView = members[0]!.providers.membership.view();
        const message: ToChild = { t: 'view', view };
        for (const m of members) {
            if (!m.child.killed && m.child.connected) m.child.send(message);
        }
    }

    function rig(): Rig {
        let nextId = 1;
        return {
            members: members.map((m) => ({ index: m.index, siloId: m.siloId, port: m.port })),
            async warm(index, targets) {
                await request(members[index]!, { t: 'warm', id: nextId++, targets }, 'warmed');
            },
            async drive(index, driveRequest) {
                const reply = (await request(
                    members[index]!,
                    { t: 'drive', id: nextId++, request: driveRequest },
                    'drove'
                )) as { ops: number; opsPerSec: number; percentiles?: Percentiles };
                return {
                    ops: reply.ops,
                    opsPerSec: reply.opsPerSec,
                    ...(reply.percentiles ? { percentiles: reply.percentiles } : {})
                };
            },
            async stats(index) {
                const reply = (await request(
                    members[index]!,
                    { t: 'stats', id: nextId++ },
                    'stats'
                )) as { stats: ChildStats };
                return reply.stats;
            },
            async resetStats() {
                await Promise.all(
                    members.map((m) => request(m, { t: 'resetStats', id: nextId++ }, 'reset'))
                );
            },
            async stop() {
                stopped = true;
                await Promise.allSettled(
                    members.map((m) => request(m, { t: 'stop', id: nextId++ }, 'stopped'))
                );
                killAll();
                process.off('exit', onExit);
                process.off('SIGINT', onSigint);
            }
        };
    }
}
