#!/usr/bin/env node
/**
 * The Azure test environment, as one command per verb.
 *
 *   node testenv.mjs up            # node pool + both releases + DNS
 *   node testenv.mjs status        # what exists, what it costs
 *   node testenv.mjs load [args]   # run the edge ladder from a same-region VM
 *   node testenv.mjs down          # delete everything this script created
 *
 * Why a script and not a README section: every measurement in scenario (l)
 * of the RUNBOOK depends on a specific shape (silos spread one-per-node, a
 * load client in the same region, an ingress that does not buffer), and a
 * shape assembled by hand is a shape nobody can reproduce or bill
 * correctly. `down` is as important as `up` — this pool costs money idle.
 *
 * Nothing is hardcoded: every value below is an env var with a default,
 * and `up` is idempotent, so re-running it converges rather than
 * duplicating. Requires az (logged in), kubectl and helm on PATH.
 */
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { readFileSync } from 'node:fs';
import { createHmac } from 'node:crypto';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(here, '..', '..', '..');

const cfg = {
    subscription: process.env.SUBSCRIPTION ?? '',          // '' = az default
    rg: process.env.RG ?? 'OmniaFlowCore',
    cluster: process.env.CLUSTER ?? 'omniaflow-core',
    acr: process.env.ACR ?? 'omniaflowtestacr',
    location: process.env.LOCATION ?? 'westeurope',
    pool: process.env.POOL ?? 'sigxactors',
    poolSize: process.env.POOL_SIZE ?? 'Standard_D2ls_v6',
    poolCount: process.env.POOL_COUNT ?? '4',
    poolMax: process.env.POOL_MAX ?? '8',
    // The taint/label pair every chart in here selects on.
    workload: process.env.WORKLOAD ?? 'sigx-actors-test',
    actorsNs: process.env.ACTORS_NS ?? 'sigx-actors-test',
    chatNs: process.env.CHAT_NS ?? 'sigx-chat',
    chatHost: process.env.CHAT_HOST ?? 'chat.omniaflowtest.net',
    dnsZone: process.env.DNS_ZONE ?? 'omniaflowtest.net',
    dnsRg: process.env.DNS_RG ?? 'omniaflowcore',
    // The load client: its own resource group so `down` is one delete.
    loadRg: process.env.LOAD_RG ?? 'sigx-loadtest',
    loadVm: process.env.LOAD_VM ?? 'loadvm',
    loadVmSize: process.env.LOAD_VM_SIZE ?? 'Standard_D4s_v5',
    ingressNs: process.env.INGRESS_NS ?? 'ingress-nginx'
};

// Suffix-anchored, and fatal if it does not match: a loose replace() on a
// host that merely CONTAINS the zone would compute the wrong record-set
// name, and ensureDns() would then happily rewrite somebody else's record.
const dnsSuffix = `.${cfg.dnsZone}`;
if (!cfg.chatHost.endsWith(dnsSuffix)) {
    console.error(`CHAT_HOST (${cfg.chatHost}) must be a name inside DNS_ZONE (${cfg.dnsZone})`);
    process.exit(1);
}
const dnsName = cfg.chatHost.slice(0, -dnsSuffix.length);

// ---------------------------------------------------------------------------

const log = (...a) => console.log(...a);
const step = (s) => log(`\n▸ ${s}`);

function sh(cmd, args, { quiet = false, allowFail = false } = {}) {
    try {
        const out = execFileSync(cmd, args, {
            encoding: 'utf8',
            stdio: quiet ? ['ignore', 'pipe', 'pipe'] : ['ignore', 'pipe', 'inherit'],
            cwd: repoRoot
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
    }
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

function buildImage(name, dockerfile, tag) {
    step(`building ${name}:${tag} in ${cfg.acr}`);
    az(['acr', 'build', '--registry', cfg.acr, '--image', `${name}:${tag}`,
        '--platform', 'linux/amd64', '--file', dockerfile, '.']);
}

function release(name, chart, ns, extra) {
    const exists = helm(['list', '-n', ns, '-q'], { quiet: true, allowFail: true })?.split('\n').includes(name);
    kube(['create', 'namespace', ns], { quiet: true, allowFail: true });
    step(`${exists ? 'upgrading' : 'installing'} ${name} in ${ns}`);
    helm([exists ? 'upgrade' : 'install', name, chart, '-n', ns,
        // reset-then-reuse (not --reuse-values): replaying old values breaks
        // the moment a chart gains a key, and every value we care about is
        // passed explicitly below anyway.
        ...(exists ? ['--reset-then-reuse-values'] : []), ...extra]);
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
    kube(['-n', cfg.ingressNs, 'get', 'svc', 'ingress-nginx-controller',
        '-o', 'jsonpath={.status.loadBalancer.ingress[0].ip}'], { quiet: true });

// ---------------------------------------------------------------------------

async function up() {
    const tag = gitSha();
    ensureCredentials();
    ensurePool();
    buildImage('sigx-actors-test', 'examples/aks-cluster/Dockerfile', tag);
    buildImage('sigx-chat', 'examples/chat/Dockerfile', tag);

    release('sigx', 'examples/aks-cluster/deploy/chart', cfg.actorsNs, [
        '--set', `image.tag=${tag}`,
        '--set', `nodeSelector.workload=${cfg.workload}`
    ]);
    release('chat', 'examples/chat/deploy/chart', cfg.chatNs, [
        '--set', `image.tag=${tag}`,
        '--set', `ingress.host=${cfg.chatHost}`,
        '--set', `nodeSelector.workload=${cfg.workload}`
    ]);

    step('waiting for rollouts');
    kube(['-n', cfg.actorsNs, 'rollout', 'status', 'deploy/sigx-silo', '--timeout=420s']);
    kube(['-n', cfg.chatNs, 'rollout', 'status', 'deploy/chat-silo', '--timeout=420s']);
    ensureDns(ingressIp());
    await status();
    log(`\n✓ up. https://${cfg.chatHost} — tear down with: node ${'testenv.mjs'} down`);
}

async function status() {
    step('cluster');
    const nodes = kube(['get', 'nodes', '-l', `workload=${cfg.workload}`, '--no-headers'],
        { quiet: true, allowFail: true }) ?? '';
    log(`  pool nodes: ${nodes ? nodes.split('\n').length : 0}`);
    for (const [ns, dep] of [[cfg.actorsNs, 'sigx-silo'], [cfg.chatNs, 'chat-silo']]) {
        const ready = kube(['-n', ns, 'get', 'deploy', dep, '-o',
            'jsonpath={.status.readyReplicas}/{.spec.replicas}'], { quiet: true, allowFail: true });
        log(`  ${ns}/${dep}: ${ready ?? '(absent)'}`);
        // Spread is the difference between N replicas and N× capacity.
        const spread = kube(['-n', ns, 'get', 'pods', '-l', 'app.kubernetes.io/component=silo',
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

function runOnVm(script) {
    return az(['vm', 'run-command', 'invoke', '-g', cfg.loadRg, '-n', cfg.loadVm,
        '--command-id', 'RunShellScript', '--scripts', script,
        '--query', 'value[0].message', '-o', 'tsv'], { quiet: true });
}

/**
 * Run the edge ladder from the VM. Extra args become env for the ladder,
 * e.g. `load LADDER=64,128,256 MIX=0.2 DURATION_MS=30000`.
 */
async function load(args) {
    await loadVmUp();
    const cookie = mintCookie();
    const b64 = Buffer.from(readFileSync(join(here, 'edge-ladder.mjs'))).toString('base64');
    const env = ['ROOMS=64', 'WORKERS=4', 'DURATION_MS=20000', 'LADDER=32,64,128,256,512',
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
const verbs = { up, down, status, load: () => load(rest), 'vm-up': loadVmUp };
if (!verbs[verb]) {
    log(`usage: node testenv.mjs <${Object.keys(verbs).join('|')}>`);
    process.exit(1);
}
await verbs[verb]();
