#!/usr/bin/env node
/**
 * The Azure test environment, as one command per verb.
 *
 *   node testenv.mjs up            # node pool + both releases + DNS
 *   node testenv.mjs status        # what exists, what it costs
 *   node testenv.mjs test [args]   # assertions, then perf, then one verdict
 *   node testenv.mjs baseline      # record the perf baseline for THIS shape
 *   node testenv.mjs bench [args]  # the raw Tier-3 run (args to the harness)
 *   node testenv.mjs load [args]   # run the edge ladder from a same-region VM
 *   node testenv.mjs ws-up [vals]  # mount the socket endpoint on the hosts
 *   node testenv.mjs ws-load [vals]# the WebSocket connection-scale Job
 *   node testenv.mjs migrate-check # the two-deploy migrateState assertion
 *   node testenv.mjs down          # delete everything this script created
 *
 * Why a script and not a README section: every measurement in scenario (l)
 * of the RUNBOOK depends on a specific shape (hosts spread one-per-node, a
 * load client in the same region, an ingress that does not buffer), and a
 * shape assembled by hand is a shape nobody can reproduce or bill
 * correctly. `down` is as important as `up` — this pool costs money idle.
 *
 * Shape is defaulted; identity is not. The pool, sizes and namespaces below
 * carry generic defaults, but every name that identifies an Azure estate
 * (RG, CLUSTER, ACR, …) must come from the environment — this file is
 * public, so it names nobody's infrastructure. Locally, export them once;
 * in CI they arrive as Actions secrets, which GitHub then masks in every
 * log line. A verb fails fast naming exactly what it is missing. `up` is
 * idempotent, so re-running it converges rather than duplicating. Requires
 * az (logged in), kubectl and helm on PATH.
 */
import { execFileSync, spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { createHmac } from 'node:crypto';
import { postMessageFnId } from '../../app/deploy/post-fn.mjs';
import { spawnable } from '../../../benchmarks/src/spawn.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(here, '..', '..', '..');

const cfg = {
    subscription: process.env.SUBSCRIPTION ?? '',          // '' = az default
    // Identity — no defaults, see the header. Validated per verb below.
    rg: process.env.RG ?? '',
    cluster: process.env.CLUSTER ?? '',
    acr: process.env.ACR ?? '',
    location: process.env.LOCATION ?? '',
    chatHost: process.env.CHAT_HOST ?? '',
    dnsZone: process.env.DNS_ZONE ?? '',
    dnsRg: process.env.DNS_RG ?? '',
    // The load client: its own resource group so `down` is one delete.
    loadRg: process.env.LOAD_RG ?? '',
    loadVm: process.env.LOAD_VM ?? '',
    // Shape — generic, so defaults are fine.
    pool: process.env.POOL ?? 'sigxactors',
    poolSize: process.env.POOL_SIZE ?? 'Standard_D2ls_v6',
    poolCount: process.env.POOL_COUNT ?? '4',
    poolMax: process.env.POOL_MAX ?? '8',
    // The taint/label pair every chart in here selects on.
    workload: process.env.WORKLOAD ?? 'sigx-actors-test',
    actorsNs: process.env.ACTORS_NS ?? 'sigx-actors-test',
    chatNs: process.env.CHAT_NS ?? 'sigx-chat',
    loadVmSize: process.env.LOAD_VM_SIZE ?? 'Standard_D4s_v5',
    ingressNs: process.env.INGRESS_NS ?? 'ingress-nginx',
    // Mirrors the chart's ports.internal — where health and ops live.
    ports: { internal: Number(process.env.INTERNAL_PORT ?? 7311) }
};

// Suffix-anchored, and fatal if it does not match: a loose replace() on a
// host that merely CONTAINS the zone would compute the wrong record-set
// name, and ensureDns() would then happily rewrite somebody else's record.
// Only judged once both are present — requiredness itself is the per-verb
// check's job, and half an identity should fail as "missing", not as
// "mismatched".
const dnsSuffix = `.${cfg.dnsZone}`;
if (cfg.chatHost && cfg.dnsZone && !cfg.chatHost.endsWith(dnsSuffix)) {
    console.error(`CHAT_HOST (${cfg.chatHost}) must be a name inside DNS_ZONE (${cfg.dnsZone})`);
    process.exit(1);
}
const dnsName = cfg.dnsZone && cfg.chatHost.endsWith(dnsSuffix)
    ? cfg.chatHost.slice(0, -dnsSuffix.length)
    : '';

// ---------------------------------------------------------------------------

const log = (...a) => console.log(...a);
const step = (s) => log(`\n▸ ${s}`);

/**
 * Best-effort temp cleanup. Retried because Windows holds a directory open
 * for a moment after a process that touched it exits, and NEVER fatal: a
 * leftover temp dir is a far better outcome than failing a build that
 * already succeeded, which is exactly what an unguarded `rmSync` did.
 */
function discard(dir) {
    try {
        rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 200 });
    } catch (error) {
        log(`  (left ${dir} behind: ${error.code ?? error.message})`);
    }
}

function sh(cmd, args, { quiet = false, allowFail = false, cwd = repoRoot } = {}) {
    // `spawnable` is what lets this reach `az` and `pnpm` on Windows, where
    // both are `.cmd` shims that `execFileSync` refuses outright. It is a
    // rewrite of the command, never a shell: arguments still arrive at the
    // callee exactly as written here.
    const run = spawnable(cmd, args);
    try {
        const out = execFileSync(run.command, run.args, {
            encoding: 'utf8',
            stdio: quiet ? ['ignore', 'pipe', 'pipe'] : ['ignore', 'pipe', 'inherit'],
            cwd,
            ...run.options
        });
        return out.trim();
    } catch (error) {
        if (allowFail) return null;
        log(`✗ ${cmd} ${args.slice(0, 4).join(' ')} …`);
        throw error;
    }
}

const az = (args, opts) => sh('az', [...args, ...(cfg.subscription ? ['--subscription', cfg.subscription] : [])], opts);
// Always pin the context: a stray `az aks get-credentials` elsewhere can
// repoint current-context, and a teardown aimed at the wrong cluster is
// not a mistake you get to undo.
const kube = (args, opts) => sh('kubectl', ['--context', cfg.cluster, ...args], opts);
const helm = (args, opts) => sh('helm', ['--kube-context', cfg.cluster, ...args], opts);

const gitSha = () => sh('git', ['rev-parse', '--short', 'HEAD'], { quiet: true });

// ---------------------------------------------------------------------------

function ensureCredentials() {
    const has = sh('kubectl', ['config', 'get-contexts', '-o', 'name'], { quiet: true, allowFail: true });
    if (!has?.split('\n').includes(cfg.cluster)) {
        step(`fetching credentials for ${cfg.cluster}`);
        az(['aks', 'get-credentials', '-g', cfg.rg, '-n', cfg.cluster, '--overwrite-existing']);
        // An AAD-enabled cluster hands back an exec-auth kubeconfig that
        // wants an interactive device-code login — dead on a CI runner.
        // kubelogin flips it to ride the ambient az session (in CI, the
        // OIDC login that just happened). Best-effort on purpose: no
        // kubelogin on PATH or a local-accounts kubeconfig both mean
        // there is nothing to convert.
        sh('kubelogin', ['convert-kubeconfig', '-l', 'azurecli'], { quiet: true, allowFail: true });
    }
}

/**
 * In Actions, GitHub masks the VALUES of secrets wherever they appear in a
 * log — which covers every name this script is given. Values it DERIVES
 * (the ingress IP, for one) are not secrets, so they are registered for
 * masking explicitly the moment they are first known. A no-op outside CI.
 */
function maskInCi(value) {
    if (process.env.GITHUB_ACTIONS && value) log(`::add-mask::${value}`);
    return value;
}

function ensurePool() {
    const pools = az(['aks', 'nodepool', 'list', '-g', cfg.rg, '--cluster-name', cfg.cluster,
        '--query', '[].name', '-o', 'tsv'], { quiet: true }) ?? '';
    if (pools.split(/\s+/).includes(cfg.pool)) {
        log(`  pool ${cfg.pool} exists`);
        return;
    }
    step(`creating node pool ${cfg.pool} (${cfg.poolCount}× ${cfg.poolSize}, autoscale 0-${cfg.poolMax})`);
    az(['aks', 'nodepool', 'add', '-g', cfg.rg, '--cluster-name', cfg.cluster,
        '--name', cfg.pool, '--mode', 'User',
        '--node-vm-size', cfg.poolSize, '--node-count', cfg.poolCount,
        '--enable-cluster-autoscaler', '--min-count', '0', '--max-count', cfg.poolMax,
        '--labels', `workload=${cfg.workload}`,
        '--node-taints', `workload=${cfg.workload}:NoSchedule`]);
}

/**
 * Build in ACR from a clean export of the COMMIT, not from the checkout.
 *
 * `.dockerignore` already says what belongs in the upload — "dependencies
 * are installed and dists are rebuilt INSIDE the container" — and
 * `git archive` enforces that instead of hoping the ignore list stays ahead
 * of every artifact a local build drops. Two things follow, both wanted:
 *
 * - The image matches its tag. The tag IS the git sha, so building a dirty
 *   tree under it produces an image nobody can reproduce from that sha.
 *   Uncommitted work is now excluded, and warned about, rather than
 *   silently shipped.
 * - It works at all. Uploading the checkout as context fails against ACR
 *   with `failed to download context` — reproducibly, from either checkout,
 *   while the same tree exported clean builds fine. The difference is the
 *   ~50k ignored files under `node_modules` that the CLI still walks.
 *
 * `--no-logs` because the CLI cannot stream a build log containing
 * non-ASCII on a Windows console: colorama writes through cp1252 and dies
 * with UnicodeEncodeError partway through OUR build output, failing a build
 * that actually succeeded. Logs are fetched explicitly when it fails, which
 * is when anyone wants them.
 */
function buildImage(name, dockerfile, tag) {
    step(`building ${name}:${tag} in ${cfg.acr}`);
    const dirty = sh('git', ['status', '--porcelain'], { quiet: true, allowFail: true });
    if (dirty) {
        log(`  ! ${dirty.split('\n').length} uncommitted change(s) — NOT in this image (tag is ${tag})`);
    }
    const dir = mkdtempSync(join(tmpdir(), 'sigx-ctx-'));
    try {
        // `git archive` cannot write a directory tree, so: tar out, tar in.
        const tar = spawnable('git', ['archive', '--format=tar', '-o', join(dir, 'ctx.tar'), 'HEAD']);
        execFileSync(tar.command, tar.args, { cwd: repoRoot, stdio: 'ignore', ...tar.options });
        // RELATIVE filename with cwd, never an absolute one: the `tar` first
        // on PATH may be msys GNU tar, which reads `C:\…` as `host:path` and
        // tries to resolve a machine called `C`. No drive letter, no problem.
        const untar = spawnable('tar', ['-xf', 'ctx.tar']);
        execFileSync(untar.command, untar.args, { cwd: dir, stdio: 'ignore', ...untar.options });
        rmSync(join(dir, 'ctx.tar'), { force: true });
        // The context is the POSITIONAL argument, not `.` with cwd set to
        // it: the CLI leaves a background telemetry process behind that
        // inherits its cwd, and on Windows that holds the directory open
        // long enough to make cleanup fail after a successful build.
        const out = sh('az', ['acr', 'build', '--registry', cfg.acr, '--image', `${name}:${tag}`,
            '--platform', 'linux/amd64', '--file', dockerfile, dir, '--no-logs',
            ...(cfg.subscription ? ['--subscription', cfg.subscription] : [])],
            { quiet: true, allowFail: true });
        if (out === null) {
            log(`✗ ${name}:${tag} failed to build — its logs:`);
            const run = az(['acr', 'task', 'list-runs', '-r', cfg.acr, '--top', '1',
                '--query', '[0].runId', '-o', 'tsv'], { quiet: true, allowFail: true });
            if (run) az(['acr', 'task', 'logs', '-r', cfg.acr, '--run-id', run], { allowFail: true });
            throw new Error(`az acr build failed for ${name}:${tag}`);
        }
        log(`  built ${name}:${tag}`);
    } finally {
        discard(dir);
    }
}

/**
 * Helm's `major.minor`, once per run.
 *
 * Two flags this script wants are recent, and on an older helm each is a
 * hard parse error rather than a fallback — so they get checked rather
 * than assumed. Reported once, naming the version, because "invalid
 * argument \"server\" for \"--dry-run\"" is not a message anyone connects
 * to their helm being from 2022.
 */
let helmVersion = null;
function helmAtLeast(major, minor) {
    if (helmVersion === null) {
        const short = sh('helm', ['version', '--short'], { quiet: true, allowFail: true }) ?? '';
        const [, gotMajor, gotMinor] = /v(\d+)\.(\d+)\./.exec(short) ?? [];
        helmVersion = { major: Number(gotMajor) || 0, minor: Number(gotMinor) || 0, short: short.trim() };
        if (helmVersion.major < 3 || (helmVersion.major === 3 && helmVersion.minor < 14)) {
            log(`  ! helm ${helmVersion.short || '(unknown)'} is old — needs >= 3.13 for a`);
            log('    server-side preflight and >= 3.14 for --reset-then-reuse-values.');
            log('    Falling back; upgrade helm to get the stronger checks back.');
        }
    }
    return (
        helmVersion.major > major || (helmVersion.major === major && helmVersion.minor >= minor)
    );
}

function release(name, chart, ns, extra) {
    const exists = helm(['list', '-n', ns, '-q'], { quiet: true, allowFail: true })?.split('\n').includes(name);
    kube(['create', 'namespace', ns], { quiet: true, allowFail: true });
    // Preflight against the REAL API server: admission webhooks reject
    // things `helm lint` and `helm template` render happily (an nginx
    // Ingress path containing `$` under pathType Prefix, for one). On an
    // older helm this degrades to a client-side render, which still catches
    // a broken template — just not a webhook.
    const dryFlag = helmAtLeast(3, 13) ? ['--dry-run=server'] : ['--dry-run'];
    const dry = helm(['upgrade', '--install', name, chart, '-n', ns, ...dryFlag, ...extra],
        { quiet: true, allowFail: true });
    if (dry === null) {
        log(`✗ ${name}: the chart was rejected — running it again for the error:`);
        helm(['upgrade', '--install', name, chart, '-n', ns, ...dryFlag, ...extra]);
    }
    step(`${exists ? 'upgrading' : 'installing'} ${name} in ${ns}`);
    helm([exists ? 'upgrade' : 'install', name, chart, '-n', ns,
        // reset-then-reuse (not --reuse-values): replaying old values breaks
        // the moment a chart gains a key, and every value we care about is
        // passed explicitly below anyway. Before helm 3.14 that flag does
        // not exist, so fall back to plain `--reset-values` — which is the
        // same idea minus the "keep what the operator set by hand" half,
        // and is if anything the more reproducible of the two.
        ...(exists ? [helmAtLeast(3, 14) ? '--reset-then-reuse-values' : '--reset-values'] : []),
        ...extra]);
}

function ensureDns(ip) {
    const existing = az(['network', 'dns', 'record-set', 'a', 'show', '-g', cfg.dnsRg,
        '-z', cfg.dnsZone, '-n', dnsName, '--query', 'ARecords[0].ipv4Address', '-o', 'tsv'],
        { quiet: true, allowFail: true });
    if (existing === ip) return log(`  dns ${cfg.chatHost} → ${ip} already set`);
    if (existing) {
        az(['network', 'dns', 'record-set', 'a', 'delete', '-g', cfg.dnsRg,
            '-z', cfg.dnsZone, '-n', dnsName, '-y'], { quiet: true, allowFail: true });
    }
    step(`pointing ${cfg.chatHost} at ${ip}`);
    az(['network', 'dns', 'record-set', 'a', 'add-record', '-g', cfg.dnsRg,
        '-z', cfg.dnsZone, '-n', dnsName, '-a', ip, '--ttl', '300'], { quiet: true });
}

const ingressIp = () =>
    maskInCi(kube(['-n', cfg.ingressNs, 'get', 'svc', 'ingress-nginx-controller',
        '-o', 'jsonpath={.status.loadBalancer.ingress[0].ip}'], { quiet: true }));

// ---------------------------------------------------------------------------

async function up() {
    const tag = gitSha();
    ensurePool();
    buildImage('sigx-actors-test', 'perf/aks/Dockerfile', tag);
    buildImage('sigx-chat', 'perf/app/Dockerfile', tag);

    release('sigx', 'perf/aks/deploy/chart', cfg.actorsNs, [
        '--set', `image.repository=${cfg.acr}.azurecr.io/sigx-actors-test`,
        '--set', `image.tag=${tag}`,
        '--set', `nodeSelector.workload=${cfg.workload}`
    ]);
    release('chat', 'perf/app/deploy/chart', cfg.chatNs, [
        '--set', `image.repository=${cfg.acr}.azurecr.io/sigx-chat`,
        '--set', `image.tag=${tag}`,
        '--set', `ingress.host=${cfg.chatHost}`,
        '--set', `nodeSelector.workload=${cfg.workload}`
    ]);

    step('waiting for rollouts');
    kube(['-n', cfg.actorsNs, 'rollout', 'status', 'deploy/sigx-host', '--timeout=420s']);
    kube(['-n', cfg.chatNs, 'rollout', 'status', 'deploy/chat-host', '--timeout=420s']);
    ensureDns(ingressIp());
    await status();
    log(`\n✓ up. https://${cfg.chatHost} — tear down with: node ${'testenv.mjs'} down`);
}

async function status() {
    step('cluster');
    const nodes = kube(['get', 'nodes', '-l', `workload=${cfg.workload}`, '--no-headers'],
        { quiet: true, allowFail: true }) ?? '';
    log(`  pool nodes: ${nodes ? nodes.split('\n').length : 0}`);
    for (const [ns, dep] of [[cfg.actorsNs, 'sigx-host'], [cfg.chatNs, 'chat-host']]) {
        const ready = kube(['-n', ns, 'get', 'deploy', dep, '-o',
            'jsonpath={.status.readyReplicas}/{.spec.replicas}'], { quiet: true, allowFail: true });
        log(`  ${ns}/${dep}: ${ready ?? '(absent)'}`);
        // Spread is the difference between N replicas and N× capacity.
        const spread = kube(['-n', ns, 'get', 'pods', '-l', 'app.kubernetes.io/component=host',
            '-o', 'jsonpath={range .items[*]}{.spec.nodeName}{"\\n"}{end}'], { quiet: true, allowFail: true });
        if (spread) {
            const distinct = new Set(spread.split('\n').filter(Boolean)).size;
            log(`     across ${distinct} node(s)${distinct < spread.split('\n').filter(Boolean).length ? ' — sharing nodes, so replicas add less than they look like' : ''}`);
        }
    }
    const vm = az(['vm', 'show', '-g', cfg.loadRg, '-n', cfg.loadVm, '--query', 'name', '-o', 'tsv'],
        { quiet: true, allowFail: true });
    log(`  load vm: ${vm ? `${cfg.loadVm} (${cfg.loadVmSize}) — running, billable` : '(absent)'}`);
    const code = sh('curl', ['-s', '-o', '/dev/null', '-w', '%{http_code}', '--max-time', '8',
        `https://${cfg.chatHost}/`], { quiet: true, allowFail: true });
    log(`  https://${cfg.chatHost}: ${code ?? 'unreachable'}`);
}

async function loadVmUp() {
    const exists = az(['vm', 'show', '-g', cfg.loadRg, '-n', cfg.loadVm, '--query', 'name', '-o', 'tsv'],
        { quiet: true, allowFail: true });
    if (exists) return log(`  load vm ${cfg.loadVm} exists`);
    step(`creating load vm ${cfg.loadVm} in ${cfg.location} (same region as the cluster — a laptop measures its own latency, not the endpoint)`);
    az(['group', 'create', '-n', cfg.loadRg, '-l', cfg.location], { quiet: true });
    az(['vm', 'create', '-g', cfg.loadRg, '-n', cfg.loadVm, '--image', 'Ubuntu2404',
        '--size', cfg.loadVmSize, '--location', cfg.location, '--generate-ssh-keys',
        '--public-ip-sku', 'Standard', '--nsg-rule', 'NONE', '--os-disk-size-gb', '32']);
    step('installing node on the vm');
    runOnVm(`
V=$(curl -fsSL https://nodejs.org/dist/index.json | grep -o '"version":"v22[^"]*"' | head -1 | cut -d'"' -f4)
curl -fsSL "https://nodejs.org/dist/$V/node-$V-linux-x64.tar.xz" -o /tmp/n.txz
mkdir -p /opt/n && tar xJf /tmp/n.txz -C /opt/n --strip-components=1
/opt/n/bin/node --version`);
}

/**
 * The script goes to `az` as `@file`, not inline.
 *
 * Inline was fine until it wasn't: a Windows command line cannot carry a
 * raw newline, so a multi-line `--scripts` argument is unrepresentable
 * there — and this one also base64s a whole ladder into it, which is a lot
 * of command line on any platform. A file sidesteps both, and `az` has
 * taken `@path` for any argument value all along.
 */
function runOnVm(script) {
    const dir = mkdtempSync(join(tmpdir(), 'sigx-vm-'));
    const file = join(dir, 'script.sh');
    try {
        writeFileSync(file, script);
        return az(['vm', 'run-command', 'invoke', '-g', cfg.loadRg, '-n', cfg.loadVm,
            '--command-id', 'RunShellScript', '--scripts', `@${file}`,
            '--query', 'value[0].message', '-o', 'tsv'], { quiet: true });
    } finally {
        discard(dir);
    }
}

/**
 * Run the edge ladder from the VM. Extra args become env for the ladder,
 * e.g. `load LADDER=64,128,256 MIX=0.2 DURATION_MS=30000`.
 */
async function load(args) {
    await loadVmUp();
    const cookie = mintCookie();
    const b64 = Buffer.from(readFileSync(join(here, 'edge-ladder.mjs'))).toString('base64');
    // POST_FN comes from the BUILD — the id is hashed and moves, and a
    // stale one 404s every write while looking like extra throughput.
    const env = ['ROOMS=64', 'WORKERS=4', 'DURATION_MS=20000', 'LADDER=32,64,128,256,512',
        `POST_FN=${postMessageFnId()}`,
        ...args].map((kv) => `export ${kv}`).join('\n');
    step('running the ladder from the vm');
    const out = runOnVm(`
set -e
echo '${b64}' | base64 -d > /tmp/ladder.mjs
ulimit -n 65535
export COOKIE='${cookie}'
export TARGET_URL='https://${cfg.chatHost}'
${env}
/opt/n/bin/node /tmp/ladder.mjs`);
    log(out);
}

// ---------------------------------------------------------------------------
// The WebSocket scale test (#172)

/** The actors release's host pods, oldest name first for stable output. */
const hostPods = () =>
    kube(['-n', cfg.actorsNs, 'get', 'pod', '-l', 'app.kubernetes.io/component=host',
        '-o', 'jsonpath={range .items[*]}{.metadata.name}{"\\n"}{end}'], { quiet: true })
        ?.split('\n').filter(Boolean).sort() ?? [];

/**
 * `ops.sockets` summed across every host, read from INSIDE the pods.
 *
 * `kubectl exec` rather than a port-forward because the counters are
 * PER HOST and the Service would load-balance a single poll to whichever
 * pod it liked — giving one host's numbers and calling them the fleet's.
 * The bearer secret is already in each pod's environment, so it never has
 * to leave the cluster or land in this process's argv.
 */
function socketTotals() {
    const read = (pod) => {
        const out = kube(['-n', cfg.actorsNs, 'exec', pod, '--', 'node', '-e',
            "fetch('http://127.0.0.1:' + (process.env.PORT || 7311) + '/_sigx/ops', " +
            "{ headers: { authorization: 'Bearer ' + process.env.OPS_SECRET } })" +
            '.then((r) => r.text()).then((t) => console.log(t))'
        ], { quiet: true, allowFail: true });
        if (!out) return null;
        try {
            return JSON.parse(out)?.ops?.sockets ?? null;
        } catch {
            return null;
        }
    };
    const totals = {};
    let hosts = 0;
    for (const pod of hostPods()) {
        const section = read(pod);
        // A section that is missing or is an `{ error }` from a throwing
        // provider must not be silently added as zeros — that would read as
        // "this host served nothing", which is a different claim.
        if (!section || section.error) continue;
        hosts++;
        for (const [key, value] of Object.entries(section)) {
            if (typeof value === 'number') totals[key] = (totals[key] ?? 0) + value;
        }
    }
    return { hosts, totals };
}

/**
 * Enable (and configure) the socket endpoint on the running actors release.
 *
 * Separate from `ws-load` on purpose: turning the socket on is a ROLLOUT,
 * and a rollout in the middle of a load verb would silently re-measure a
 * different deployment than the one the previous rung ran against. Extra
 * args are chart values, e.g.
 *   ws-up socket.env.SOCKET_MAX_SUBSCRIPTIONS=1024 socket.sessions=true
 */
async function wsUp(args) {
    step('enabling the socket endpoint on the actors release');
    const sets = ['socket.enabled=true', ...args].flatMap((kv) => ['--set', kv]);
    helm(['upgrade', '--install', 'sigx', join(here, 'chart'), '-n', cfg.actorsNs,
        '--reset-then-reuse-values', '--wait', '--timeout', '10m', ...sets]);
    kube(['-n', cfg.actorsNs, 'rollout', 'status', 'deploy/sigx-host', '--timeout=420s']);
    log(`  socket mounted on ${hostPods().length} host pod(s)`);
}

/**
 * Run the WebSocket load Job and report the merged rows.
 *
 * Extra args are `wsLoadgen.*` chart values without the prefix, e.g.
 *   ws-load mode=hot ladder=1000,5000,10000 parallelism=2 durationS=60
 *
 * Commas are escaped for `--set` here rather than by the caller: `helm
 * --set` splits values on commas, so an unescaped ladder fails with
 * `key "5000" has no value` — a confusing error a long way from its cause.
 */
async function wsLoad(args) {
    const enabled = kube(['-n', cfg.actorsNs, 'get', 'deploy', 'sigx-host', '-o',
        'jsonpath={.spec.template.spec.containers[0].env[?(@.name=="ENABLE_SOCKET")].value}'],
        { quiet: true, allowFail: true });
    if (enabled !== '1') {
        console.error('the host has no socket endpoint — run `testenv.mjs ws-up` first');
        process.exit(1);
    }

    const suffix = `${Date.now()}-${Math.floor(Math.random() * 10000)}`;
    const job = `sigx-wsloadgen-${suffix}`;
    const sets = ['wsLoadgen.enabled=true', `wsLoadgen.nameSuffix=${suffix}`, ...args]
        .flatMap((kv) => {
            const [key, ...rest] = kv.split('=');
            const value = rest.join('=');
            const name = key.startsWith('wsLoadgen.') || key.startsWith('image.')
                ? key
                : `wsLoadgen.${key}`;
            return ['--set-string', `${name}=${value.replaceAll(',', '\\,')}`];
        });

    step(`rendering ${job}`);
    const manifest = helm(['template', 'sigx', join(here, 'chart'), '-n', cfg.actorsNs,
        '-s', 'templates/wsloadgen-job.yaml',
        '--set', `image.repository=${cfg.acr}.azurecr.io/sigx-actors-test`,
        '--set', `image.tag=${gitSha()}`,
        '--set', `nodeSelector.workload=${cfg.workload}`,
        ...sets], { quiet: true });

    // BEFORE the apply. Taken after it, the first rung's connections are
    // already opening, and the delta comes out with more closes than opens
    // — which reads like lost connections rather than a racing baseline.
    const before = socketTotals();

    const dir = mkdtempSync(join(tmpdir(), 'sigx-wsload-'));
    const file = join(dir, 'job.yaml');
    try {
        writeFileSync(file, manifest);
        kube(['-n', cfg.actorsNs, 'apply', '-f', file]);
    } finally {
        discard(dir);
    }

    // Read the completion count back off the Job rather than trusting the
    // args: `parallelism` may have come from values, from an arg, or from
    // the chart default, and the wait below has to know when it is done.
    const wanted = Number(kube(['-n', cfg.actorsNs, 'get', 'job', job, '-o',
        'jsonpath={.spec.completions}'], { quiet: true, allowFail: true })) || 1;

    step(`waiting for ${job} (${wanted} pod(s))`);
    /**
     * Poll rather than `kubectl wait`, because the wait has to do two jobs.
     *
     * The second one is the point: `open` is a GAUGE, so it only exists
     * while the connections do. Reading it after the Job has finished
     * reports zero, and the monotonic totals cannot substitute — they count
     * every connection ever opened across every rung, not how many were up
     * at once. The peak of this sample IS the "how many clients were
     * connected at the same time" number, and it comes from the SERVERS
     * rather than from the thing generating the load.
     *
     * It is a sample, so it under-reports by however much the true peak
     * fell between polls; the generator's own per-pod `connected` stays the
     * authority for what each pod achieved.
     */
    let peakOpen = 0;
    let peakSubscriptions = 0;
    let samples = 0;
    const deadline = Date.now() + 3600_000;
    for (;;) {
        const live = socketTotals();
        if (live.hosts > 0) {
            samples++;
            peakOpen = Math.max(peakOpen, live.totals.open ?? 0);
            peakSubscriptions = Math.max(peakSubscriptions, live.totals.subscriptions ?? 0);
        }
        const state = kube(['-n', cfg.actorsNs, 'get', 'job', job, '-o',
            'jsonpath={.status.succeeded}|{.status.failed}'], { quiet: true, allowFail: true }) ?? '';
        const [succeeded, failed] = state.split('|').map((v) => Number(v) || 0);
        if (succeeded >= wanted || failed > 0) {
            if (failed > 0) log(`  ✗ ${job}: ${failed} pod(s) failed`);
            break;
        }
        if (Date.now() > deadline) {
            log(`  ✗ ${job} did not complete within the deadline`);
            break;
        }
        await new Promise((resolve) => setTimeout(resolve, 5000));
    }
    const after = socketTotals();

    /**
     * Logs PER POD, via the label — `kubectl logs job/<name>` picks ONE pod
     * and says nothing about the rest, so a parallel run silently reported
     * a single pod's numbers as the whole run's. The rows are summed, so
     * that is not a display bug: it under-reports the measurement by the
     * pod count.
     */
    const pods = kube(['-n', cfg.actorsNs, 'get', 'pod', '-l', `job-name=${job}`,
        '-o', 'jsonpath={range .items[*]}{.metadata.name}{"\\n"}{end}'],
        { quiet: true, allowFail: true })?.split('\n').filter(Boolean) ?? [];
    log(`  collecting logs from ${pods.length} pod(s)`);
    const rows = [];
    for (const pod of pods) {
        const out = kube(['-n', cfg.actorsNs, 'logs', pod, '--tail=-1'],
            { quiet: true, allowFail: true }) ?? '';
        for (const line of out.split('\n')) {
            const trimmed = line.trim();
            if (!trimmed.startsWith('{')) continue;
            try {
                rows.push(JSON.parse(trimmed));
            } catch {
                // a progress line that merely begins with a brace
            }
        }
    }

    if (rows.length === 0) {
        for (const pod of pods.slice(0, 2)) {
            log(`--- ${pod} ---`);
            log((kube(['-n', cfg.actorsNs, 'logs', pod, '--tail=40'],
                { quiet: true, allowFail: true }) ?? '(no logs)'));
        }
        console.error(`${job} produced no result rows`);
        process.exitCode = 1;
        return;
    }

    step('merged rows');
    for (const row of mergeRows(rows)) log(JSON.stringify(row));

    step('peak concurrency, observed on the HOSTS');
    log(JSON.stringify({
        hosts: after.hosts,
        peakOpen,
        peakSubscriptions,
        samples,
        note: 'gauge sampled every ~5s while the job ran; a true peak between samples is missed'
    }));

    step('ops.sockets, summed across hosts');
    const delta = {};
    for (const [key, value] of Object.entries(after.totals)) {
        if (key === 'open' || key === 'inFlight' || key === 'subscriptions') continue;
        delta[key] = value - (before.totals[key] ?? 0);
    }
    log(JSON.stringify({ hosts: after.hosts, delta }));
    if ((delta.protocolBreaches ?? 0) > 0) {
        log('  ✗ protocol breaches — the client and the session disagree; the run is not valid');
        process.exitCode = 1;
    }
}

/**
 * One row per rung, summed across the Job's pods.
 *
 * COUNTS sum; PERCENTILES DO NOT. Merging p99s across pods produces a
 * number that is the p99 of nothing, so latency is taken from the single
 * publishing pod (the only one that carries probes) and labelled with the
 * pod count it does NOT represent.
 */
function mergeRows(rows) {
    const byRung = new Map();
    for (const row of rows) {
        const merged = byRung.get(row.n) ?? {
            n: row.n,
            pods: 0,
            mode: row.mode,
            read: row.read,
            principal: row.principal,
            connected: 0,
            socketsExpected: 0,
            connectFailures: 0,
            deliveries: 0,
            durationMs: 0,
            publishes: 0,
            publishFailures: 0,
            subscriptionErrors: 0,
            drops: 0,
            seqMismatches: 0,
            maxBufferedBytes: 0,
            clientRssMb: 0,
            latencyMs: null,
            latencyFromPods: 0
        };
        merged.pods++;
        for (const key of ['connected', 'socketsExpected', 'connectFailures', 'deliveries',
            'publishes', 'publishFailures', 'subscriptionErrors', 'drops', 'seqMismatches']) {
            merged[key] += row[key] ?? 0;
        }
        // Rates are NOT summed. Each pod already divided by its own window,
        // and pods do not start or finish together — adding their rates
        // overstates throughput by however much the windows differ. The
        // merged rate is recomputed below from summed deliveries over the
        // longest window, which is the one they all overlap within.
        merged.durationMs = Math.max(merged.durationMs, row.durationMs ?? 0);
        merged.maxBufferedBytes = Math.max(merged.maxBufferedBytes, row.maxBufferedBytes ?? 0);
        merged.clientRssMb = Math.max(merged.clientRssMb, row.clientRssMb ?? 0);
        if (row.latencyMs) {
            merged.latencyMs = row.latencyMs;
            merged.latencyFromPods++;
        }
        byRung.set(row.n, merged);
    }
    return [...byRung.values()]
        .sort((a, b) => a.n - b.n)
        .map((row) => ({
            ...row,
            // `n` is PER POD, so the number a reader wants is this one.
            totalConnections: row.connected,
            deliveriesPerSec: row.durationMs > 0
                ? Math.round((row.deliveries / (row.durationMs / 1000)) * 1000) / 1000
                : 0,
            deliveriesPerPublish: row.publishes > 0
                ? Math.round((row.deliveries / row.publishes) * 1000) / 1000
                : 0
        }));
}

/**
 * The runtime knobs, read back off the LIVE Deployment.
 *
 * Every one of these changes the perf curve, and none of them changes the
 * image tag — so without them in the shape, `bench:infra:compare` happily
 * compares a rebalancing, activation-count deployment against a baseline
 * taken from a prefer-local one and reports the difference as a code
 * regression. Read from the container's env rather than from values.yaml
 * because a `helm --set` is exactly the case that would otherwise slip
 * through.
 */
const KNOBS = [
    'PLACEMENT',
    'PLACEMENT_REFRESH_MS',
    'REBALANCE',
    'REBALANCE_INTERVAL_MS',
    'REBALANCE_THRESHOLD',
    'REBALANCE_MIN_IDLE_MS',
    'REBALANCE_MAX_MOVES',
    'MAX_ACTIVATIONS',
    'SWEEP_INTERVAL_MS',
    'DIGEST_MAX_LOCAL',
    'DIGEST_ITERS',
    'MIGRATE_PERSIST'
];

/** `{ NAME: value }` for the knobs the live chat Deployment actually sets. */
function liveKnobs() {
    const raw = kube(['-n', cfg.chatNs, 'get', 'deploy', 'chat-host', '-o',
        'jsonpath={range .spec.template.spec.containers[0].env[*]}{.name}={.value}{"\\n"}{end}'],
        { quiet: true, allowFail: true }) ?? '';
    const out = {};
    for (const line of raw.split('\n')) {
        const eq = line.indexOf('=');
        if (eq <= 0) continue;
        const name = line.slice(0, eq);
        if (KNOBS.includes(name)) out[name] = line.slice(eq + 1);
    }
    return out;
}

/**
 * The deployment's identity for baseline purposes. Node spread is IN it on
 * purpose: replicas sharing a node add almost no capacity, and a report
 * cannot tell the two apart (#183). So are the runtime knobs — see above.
 */
function infraShape() {
    const replicas = kube(['-n', cfg.chatNs, 'get', 'deploy', 'chat-host',
        '-o', 'jsonpath={.spec.replicas}'], { quiet: true });
    const nodes = kube(['-n', cfg.chatNs, 'get', 'pods', '-l', 'app.kubernetes.io/component=host',
        '-o', 'jsonpath={range .items[*]}{.spec.nodeName}{"\\n"}{end}'], { quiet: true });
    const distinct = new Set(nodes.split('\n').filter(Boolean)).size;
    const image = kube(['-n', cfg.chatNs, 'get', 'deploy', 'chat-host',
        '-o', 'jsonpath={.spec.template.spec.containers[0].image}'], { quiet: true });
    // Sorted, so two identically-configured deployments produce the same
    // string whatever order the manifest happened to list the env in.
    const knobs = Object.entries(liveKnobs())
        .sort(([a], [b]) => (a < b ? -1 : 1))
        .map(([k, v]) => `${k}=${v}`)
        .join(',');
    return (
        `replicas=${replicas} nodes=${distinct} image=${image.split(':').pop()}` +
        (knobs ? ` knobs=${knobs}` : '')
    );
}

const opsSecret = () =>
    Buffer.from(
        kube(['-n', cfg.chatNs, 'get', 'secret', 'chat-secrets',
            '-o', 'jsonpath={.data.opsSecret}'], { quiet: true }),
        'base64'
    ).toString('utf8');

const authSecret = () =>
    Buffer.from(
        kube(['-n', cfg.chatNs, 'get', 'secret', 'chat-secrets',
            '-o', 'jsonpath={.data.authSecret}'], { quiet: true }),
        'base64'
    ).toString('utf8');

/**
 * Everything `test` and `bench` share: the same-region VM, the built dist,
 * the ops port-forward, and the `INFRA_*` environment read off the live
 * release. Returns `{ env, done }` — call `done()` when finished — or null
 * (with exitCode set) when the forward never came up.
 */
async function infraSession() {
    // Tier 3 drives its load from the VM (a laptop's WAN jitter is +/-50%,
    // which cannot detect a regression), so the VM is part of the session.
    await loadVmUp();
    // The harness measures the BUILT dist; without it the scenario registry
    // cannot even load (it imports every tier).
    if (!existsSync(join(repoRoot, 'packages/actors/dist/index.prod.js'))) {
        step('building (the bench harness needs dist/*.prod.js)');
        sh('pnpm', ['build'], { quiet: true });
    }
    // The ops checks read the INTERNAL listener, which is deliberately not
    // routable from outside — so do what an operator does: port-forward it.
    const forwardPort = Number(process.env.INFRA_OPS_PORT ?? 7399);
    // kubectl is a real executable everywhere, so this needs no rewriting
    // today — routed through `spawnable` anyway so a machine that installs
    // it as a shim (scoop, npm, a wrapper) does not silently break `test`
    // while every other verb keeps working.
    const fwd = spawnable('kubectl', ['--context', cfg.cluster, '-n', cfg.chatNs,
        'port-forward', `svc/${'chat'}-host`, `${forwardPort}:${cfg.ports.internal}`]);
    const forward = spawn(fwd.command, fwd.args, { stdio: 'ignore', ...fwd.options });
    // VERIFY it, rather than sleeping and hoping. A forward binds to one
    // pod, so it dies whenever that pod is replaced — which a rolling
    // restart guarantees, and a leftover forward from an earlier session
    // then holds the port while refusing every connection. Unchecked, all
    // of that surfaces as a handful of ops-reading FEATURE tests failing,
    // which is the most misleading shape this suite can fail in.
    const opsUp = await (async () => {
        for (let i = 0; i < 30; i++) {
            await new Promise((r) => setTimeout(r, 1_000));
            const res = await fetch(`http://127.0.0.1:${forwardPort}/_sigx/health`)
                .catch(() => null);
            if (res?.ok) return true;
        }
        return false;
    })();
    if (!opsUp) {
        forward.kill();
        log(`✗ the ops port-forward never came up on :${forwardPort}.`);
        log(`  Something else may be holding it — check for a stray`);
        log(`  \`kubectl port-forward\` from an earlier run, or set INFRA_OPS_PORT.`);
        process.exitCode = 1;
        return null;
    }
    const env = {
        ...process.env,
        INFRA_URL: `https://${cfg.chatHost}`,
        INFRA_AUTH_SECRET: authSecret(),
        INFRA_OPS_SECRET: opsSecret(),
        INFRA_NS: cfg.chatNs,
        INFRA_CONTEXT: cfg.cluster,
        INFRA_SHAPE: infraShape(),
        INFRA_OPS_URL: `http://127.0.0.1:${forwardPort}`,
        INFRA_VM_RG: cfg.loadRg,
        INFRA_VM_NAME: cfg.loadVm,
        // The knobs the suites gate on. Each is read back off the live
        // Deployment, so a suite asserting a feature runs exactly when the
        // deployment was actually brought up with it.
        ...knobEnv()
    };
    log(`  shape: ${env.INFRA_SHAPE}`);
    return { env, done: () => forward.kill() };
}

/**
 * The whole point of the exercise: assert, then measure, then say one
 * thing. Extra args go to vitest (`test -t "sealed"`), and `INFRA_CHAOS=1`
 * adds the destructive scenarios.
 */
async function test(args) {
    const session = await infraSession();
    if (!session) return;
    const { env, done } = session;
    step('assertions (vitest)');
    const assertions = spawnInherit('pnpm', ['test:infra', ...args], env);
    // Perf costs a VM and ~10 minutes of it. Measuring a deployment whose
    // assertions just failed spends that on a number nobody can trust, so
    // stop — unless the operator is specifically chasing the perf half.
    const forcePerf = process.env.INFRA_FORCE_PERF === '1';
    let perf = 0;
    if (assertions !== 0 && !forcePerf) {
        log('\n✗ assertions failed — skipping perf (INFRA_FORCE_PERF=1 runs it anyway)');
        perf = 1;
    } else {
        step('perf (bench tier 3, compared against the recorded baseline)');
        perf = spawnInherit('pnpm', ['bench:infra:compare'], env);
    }
    done();
    log(`\n${assertions === 0 ? '✓' : '✗'} assertions   ${perf === 0 ? '✓' : '✗'} perf`);
    if (assertions !== 0 || perf !== 0) process.exitCode = 1;
}

/**
 * The raw Tier-3 run alone: same environment as `test`, but the extra args
 * go straight to the bench harness (`--json=…`, `--runs=…`, a scenario
 * filter) and no baseline is consulted. This is the verb CI dispatches —
 * `test`'s comparison is a judgement against a locally recorded baseline,
 * where a JSON artifact is a measurement anyone can diff later.
 */
async function bench(args) {
    const session = await infraSession();
    if (!session) return;
    const { env, done } = session;
    step('perf (bench tier 3, raw run)');
    const perf = spawnInherit('pnpm', ['bench:infra', ...args], env);
    done();
    if (perf !== 0) process.exitCode = 1;
}

/** The live knobs, as the `INFRA_*` names the suites and scenarios read. */
function knobEnv() {
    const live = liveKnobs();
    const out = {};
    for (const [name, value] of Object.entries(live)) {
        if (value !== '') out[`INFRA_${name}`] = value;
    }
    return out;
}

/** Record the perf baseline FOR THE CURRENT SHAPE. */
async function baseline() {
    await loadVmUp();
    const env = {
        ...process.env,
        INFRA_URL: `https://${cfg.chatHost}`,
        INFRA_AUTH_SECRET: authSecret(),
        INFRA_SHAPE: infraShape(),
        INFRA_VM_RG: cfg.loadRg,
        INFRA_VM_NAME: cfg.loadVm
    };
    log(`  shape: ${env.INFRA_SHAPE}`);
    // Propagate failure: automation that reads exit 0 as "baseline recorded"
    // would compare against a stale one forever.
    if (spawnInherit('pnpm', ['bench:infra:baseline'], env) !== 0) {
        log('✗ baseline FAILED — nothing was recorded');
        process.exitCode = 1;
    }
}

/**
 * The one assertion that structurally needs TWO deploys: `migrateState`
 * only runs on a record the PREVIOUS image wrote.
 *
 *   node testenv.mjs migrate-check [<old-tag>]
 *
 * Write a room against whatever is deployed now, roll the release forward
 * to HEAD, then assert the room reads back whole at the new version. No
 * fixture and no test-only endpoint: a v1 record is simply what the older
 * image writes, which is the same thing that happens to every real room
 * during the deploy this is imitating.
 *
 * Default `<old-tag>` is the tag already running, so the normal sequence is
 * `up` on an older commit, then `migrate-check` on the newer one.
 */
async function migrateCheck(args) {
    const target = `https://${cfg.chatHost}`;
    const secret = authSecret();
    const name = 'migrator';
    const sig = createHmac('sha256', secret).update(name).digest('hex');
    const cookie = `user=${encodeURIComponent(`${name}.${sig}`)}`;
    const room = `migrated-${Date.now().toString(36)}`;

    const before = kube(['-n', cfg.chatNs, 'get', 'deploy', 'chat-host',
        '-o', 'jsonpath={.spec.template.spec.containers[0].image}'], { quiet: true });
    const oldTag = args[0] ?? before.split(':').pop();
    step(`writing ${room} against the OLD image (${oldTag})`);
    if (args[0]) {
        release('chat', 'perf/app/deploy/chart', cfg.chatNs, [
            '--set', `image.repository=${cfg.acr}.azurecr.io/sigx-chat`,
            '--set', `image.tag=${oldTag}`,
            '--set', `ingress.host=${cfg.chatHost}`,
            '--set', `nodeSelector.workload=${cfg.workload}`
        ]);
        kube(['-n', cfg.chatNs, 'rollout', 'status', 'deploy/chat-host', '--timeout=420s']);
    }
    for (let i = 0; i < 3; i++) {
        const res = await fetch(`${target}/_sigx/fn/${postMessageFnId()}`, {
            method: 'POST',
            headers: { 'content-type': 'application/json', origin: target, cookie },
            body: JSON.stringify({ args: [{ room, text: `pre-migration ${i}` }] })
        });
        if (!res.ok) {
            log(`✗ could not seed ${room}: ${res.status}`);
            process.exitCode = 1;
            return;
        }
    }

    const tag = gitSha();
    step(`rolling forward to ${tag}`);
    buildImage('sigx-chat', 'perf/app/Dockerfile', tag);
    release('chat', 'perf/app/deploy/chart', cfg.chatNs, [
        '--set', `image.repository=${cfg.acr}.azurecr.io/sigx-chat`,
        '--set', `image.tag=${tag}`,
        '--set', `ingress.host=${cfg.chatHost}`,
        '--set', `nodeSelector.workload=${cfg.workload}`
    ]);
    kube(['-n', cfg.chatNs, 'rollout', 'status', 'deploy/chat-host', '--timeout=420s']);

    step('asserting the migration');
    const code = spawnInherit('pnpm', ['test:infra', '-t', 'migrateState'], {
        ...process.env,
        INFRA_URL: target,
        INFRA_AUTH_SECRET: secret,
        INFRA_MIGRATED_ROOM: room
    });
    log(`\n${code === 0 ? '✓' : '✗'} migrateState (${room}, written by ${oldTag})`);
    if (code !== 0) process.exitCode = 1;
}

function spawnInherit(cmd, args, env) {
    const run = spawnable(cmd, args);
    try {
        execFileSync(run.command, run.args, {
            stdio: 'inherit',
            cwd: repoRoot,
            env,
            ...run.options
        });
        return 0;
    } catch {
        return 1;
    }
}

/** A signed session cookie, minted the way the chat app's guard verifies. */
function mintCookie() {
    const secret = Buffer.from(
        kube(['-n', cfg.chatNs, 'get', 'secret', 'chat-secrets',
            '-o', 'jsonpath={.data.authSecret}'], { quiet: true }),
        'base64'
    ).toString('utf8');
    const name = process.env.LOAD_USER ?? 'loadtest';
    const sig = createHmac('sha256', secret).update(name).digest('hex');
    return `user=${encodeURIComponent(`${name}.${sig}`)}`;
}

async function down() {
    step('deleting releases');
    for (const [name, ns] of [['chat', cfg.chatNs], ['sigx', cfg.actorsNs]]) {
        helm(['uninstall', name, '-n', ns], { quiet: true, allowFail: true });
        kube(['delete', 'namespace', ns, '--wait=false'], { quiet: true, allowFail: true });
    }
    step(`removing dns ${cfg.chatHost}`);
    az(['network', 'dns', 'record-set', 'a', 'delete', '-g', cfg.dnsRg, '-z', cfg.dnsZone,
        '-n', dnsName, '-y'], { quiet: true, allowFail: true });
    step(`deleting load vm resource group ${cfg.loadRg}`);
    az(['group', 'delete', '-n', cfg.loadRg, '-y', '--no-wait'], { quiet: true, allowFail: true });
    step(`deleting node pool ${cfg.pool}`);
    az(['aks', 'nodepool', 'delete', '-g', cfg.rg, '--cluster-name', cfg.cluster,
        '--name', cfg.pool, '--no-wait'], { allowFail: true });
    log('\n✓ down (pool and vm deletions continue in the background)');
}

const [verb = 'status', ...rest] = process.argv.slice(2);
const verbs = {
    up,
    down,
    status,
    test: () => test(rest),
    baseline,
    bench: () => bench(rest),
    load: () => load(rest),
    'ws-up': () => wsUp(rest),
    'ws-load': () => wsLoad(rest),
    'migrate-check': () => migrateCheck(rest),
    'vm-up': loadVmUp
};

/**
 * Which identity env vars each verb actually reaches for — checked before
 * anything runs, so a missing name fails here with its env var named
 * rather than three az calls in with a 404 on somebody's default.
 */
const ENV_KEY = {
    RG: 'rg', CLUSTER: 'cluster', ACR: 'acr', LOCATION: 'location',
    CHAT_HOST: 'chatHost', DNS_ZONE: 'dnsZone', DNS_RG: 'dnsRg',
    LOAD_RG: 'loadRg', LOAD_VM: 'loadVm'
};
const NEEDS = {
    up: ['RG', 'CLUSTER', 'ACR', 'CHAT_HOST', 'DNS_ZONE', 'DNS_RG'],
    status: ['RG', 'CLUSTER', 'CHAT_HOST', 'LOAD_RG', 'LOAD_VM'],
    test: ['RG', 'CLUSTER', 'CHAT_HOST', 'LOCATION', 'LOAD_RG', 'LOAD_VM'],
    baseline: ['RG', 'CLUSTER', 'CHAT_HOST', 'LOCATION', 'LOAD_RG', 'LOAD_VM'],
    bench: ['RG', 'CLUSTER', 'CHAT_HOST', 'LOCATION', 'LOAD_RG', 'LOAD_VM'],
    load: ['RG', 'CLUSTER', 'CHAT_HOST', 'LOCATION', 'LOAD_RG', 'LOAD_VM'],
    // In-cluster only: the WebSocket Job talks to the actors Service, so
    // it needs neither the public chat host nor the load VM.
    'ws-up': ['RG', 'CLUSTER'],
    'ws-load': ['RG', 'CLUSTER', 'ACR'],
    'migrate-check': ['RG', 'CLUSTER', 'ACR', 'CHAT_HOST'],
    down: ['RG', 'CLUSTER', 'CHAT_HOST', 'DNS_ZONE', 'DNS_RG', 'LOAD_RG'],
    'vm-up': ['LOCATION', 'LOAD_RG', 'LOAD_VM']
};
/** Verbs that reach kubectl/helm, and so need a kubeconfig first. */
const KUBE_VERBS = new Set(['up', 'status', 'test', 'baseline', 'bench', 'load', 'ws-up', 'ws-load', 'migrate-check', 'down']);

if (!verbs[verb]) {
    log(`usage: node testenv.mjs <${Object.keys(verbs).join('|')}>`);
    process.exit(1);
}
const missing = NEEDS[verb].filter((name) => !cfg[ENV_KEY[name]]);
if (missing.length) {
    console.error(`${verb} needs ${missing.join(', ')} set in the environment.`);
    console.error('These name your Azure estate and are deliberately not defaulted');
    console.error('here — see deploy/RUNBOOK.md. Export them once locally; in CI');
    console.error('they come from Actions secrets.');
    process.exit(1);
}
// A fresh CI runner has no kubeconfig at all, so every kube-touching verb
// fetches credentials — not just `up`.
if (KUBE_VERBS.has(verb)) ensureCredentials();
await verbs[verb]();
